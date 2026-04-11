"""
nl_query_agent.py — Natural Language to SQL query interface.

Translates natural language questions into SQLite SELECT queries using Claude Haiku.
Heavy guardrails: SELECT-only, read-only connection, timeout, rate limit, keyword blocklist.

V1: Query + results. V2: Suggested questions, charts, voice input.
"""
import json
import logging
import os
import re
import sqlite3
import time
import threading

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = logging.getLogger("reytech.nl_query")

_MODEL = "claude-haiku-4-5-20251001"
_API_TIMEOUT = 10

# V2: Suggested queries for the UI
SUGGESTED_QUERIES = [
    {"text": "Top 5 agencies by total spend", "category": "spend"},
    {"text": "Win rate this month", "category": "performance"},
    {"text": "Quotes sent this week", "category": "activity"},
    {"text": "Average markup % by agency", "category": "pricing"},
    {"text": "Items quoted most frequently", "category": "catalog"},
    {"text": "Quotes expiring in next 7 days", "category": "urgency"},
    {"text": "How many PCs are priced but not sent?", "category": "pipeline"},
    {"text": "Top 10 products by revenue", "category": "revenue"},
]
_QUERY_TIMEOUT = 5  # seconds for SQL execution
_MAX_ROWS = 100
_RATE_LIMIT = 10  # queries per minute

# Rate limiting state
_rate_lock = threading.Lock()
_rate_timestamps = []

# Schema cache
_schema_cache = None
_schema_cache_ts = 0
_SCHEMA_TTL = 300  # 5 minutes

# Tables safe to expose (no secrets, no internal state)
SAFE_TABLES = [
    "quotes", "contacts", "products", "product_catalog",
    "orders", "rfqs", "price_history", "price_checks",
    "scprs_awards", "won_quotes_kb", "vendor_intel",
    "buyer_intel", "competitors", "compliance_matrices",
    "parsed_documents", "nl_query_log",
]

# Keywords that must NEVER appear in generated SQL
BANNED_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "ATTACH", "DETACH", "VACUUM", "REINDEX", "REPLACE",
    "PRAGMA",  # blocked in execution, allowed in schema builder only
}


# ═══════════════════════════════════════════════════════════════════════════
# GUARDRAILS
# ═══════════════════════════════════════════════════════════════════════════

def _check_rate_limit() -> bool:
    """Return True if under rate limit, False if blocked."""
    with _rate_lock:
        now = time.time()
        # Remove timestamps older than 60 seconds
        _rate_timestamps[:] = [ts for ts in _rate_timestamps if now - ts < 60]
        if len(_rate_timestamps) >= _RATE_LIMIT:
            return False
        _rate_timestamps.append(now)
        return True


def _validate_sql(sql: str) -> tuple:
    """Validate generated SQL. Returns (ok: bool, error: str).

    Guardrails:
    1. Must start with SELECT
    2. No banned keywords
    3. No multiple statements
    """
    if not sql or not sql.strip():
        return False, "Empty SQL"

    # Strip comments
    cleaned = re.sub(r'--[^\n]*', '', sql)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()

    # Must start with SELECT
    if not cleaned.upper().startswith("SELECT"):
        return False, "Only SELECT queries are allowed"

    # No multiple statements (semicolons followed by non-whitespace)
    parts = [p.strip() for p in cleaned.split(";") if p.strip()]
    if len(parts) > 1:
        return False, "Multiple SQL statements not allowed"

    # Check for banned keywords (as whole words, case-insensitive)
    upper = cleaned.upper()
    for keyword in BANNED_KEYWORDS:
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, upper):
            return False, f"Keyword '{keyword}' is not allowed"

    return True, ""


def _inject_limit(sql: str) -> str:
    """Ensure LIMIT clause exists. Add LIMIT 100 if missing."""
    upper = sql.upper().strip().rstrip(";")
    if "LIMIT" not in upper:
        return sql.rstrip().rstrip(";") + f" LIMIT {_MAX_ROWS}"
    return sql


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA CONTEXT
# ═══════════════════════════════════════════════════════════════════════════

def _build_schema_context() -> str:
    """Generate DB schema description for the system prompt. Cached 5 minutes."""
    global _schema_cache, _schema_cache_ts

    if _schema_cache and (time.time() - _schema_cache_ts < _SCHEMA_TTL):
        return _schema_cache

    try:
        from src.core.db import get_db
        lines = ["Available SQLite tables and their columns:\n"]

        with get_db() as conn:
            for table in SAFE_TABLES:
                try:
                    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
                    if not cols:
                        continue
                    col_descs = []
                    for col in cols:
                        name = col["name"] if isinstance(col, dict) else col[1]
                        ctype = col["type"] if isinstance(col, dict) else col[2]
                        col_descs.append(f"  {name} ({ctype})")
                    lines.append(f"\n{table}:")
                    lines.extend(col_descs)

                    # Sample row count
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    row_count = count[0] if count else 0
                    lines.append(f"  -- {row_count} rows")
                except Exception:
                    continue  # Table may not exist yet

        _schema_cache = "\n".join(lines)
        _schema_cache_ts = time.time()
        return _schema_cache

    except Exception as e:
        log.error("Failed to build schema context: %s", e)
        return "Schema unavailable"


# ═══════════════════════════════════════════════════════════════════════════
# CLAUDE API
# ═══════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT_TEMPLATE = """You are a SQL query generator for a procurement/RFQ business application.
Given a natural language question, generate a single SQLite SELECT statement.

{schema}

Rules:
- Return ONLY valid JSON: {{"sql": "SELECT ...", "explanation": "brief description"}}
- Always include LIMIT 100 unless the user asks for a specific count
- Use appropriate JOINs when the question spans multiple tables
- For date filtering, dates are stored as ISO text (e.g., '2026-04-10')
- Use LIKE for fuzzy text matching, exact = for IDs
- For aggregations, always alias computed columns (e.g., COUNT(*) AS total)
- Never use subqueries when a JOIN suffices
- If the question is ambiguous, pick the most likely interpretation"""


def _get_api_key() -> str:
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


def _generate_sql(query_text: str) -> dict:
    """Call Claude Haiku to generate SQL from natural language."""
    api_key = _get_api_key()
    if not api_key or not HAS_REQUESTS:
        return {"sql": "", "explanation": "API not available"}

    schema = _build_schema_context()
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(schema=schema)

    try:
        request_body = {
            "model": _MODEL,
            "max_tokens": 512,
            "system": [{"type": "text", "text": system_prompt,
                        "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": query_text[:2000]}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=request_body, timeout=_API_TIMEOUT,
        )

        if resp.status_code != 200:
            log.warning("NL query API error: %d", resp.status_code)
            return {"sql": "", "explanation": f"API error {resp.status_code}"}

        data = resp.json()
        full_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                full_text += block.get("text", "")

        # Parse JSON from response
        text = full_text.strip()
        # Try to find JSON object
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end])
            return {
                "sql": parsed.get("sql", ""),
                "explanation": parsed.get("explanation", ""),
            }

        return {"sql": "", "explanation": "Could not parse response"}

    except requests.exceptions.Timeout:
        log.debug("NL query: API timeout")
        return {"sql": "", "explanation": "API timeout"}
    except Exception as e:
        log.error("NL query API error: %s", e, exc_info=True)
        return {"sql": "", "explanation": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# SQL EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

class QueryTimeoutError(Exception):
    pass


def _execute_safe(sql: str) -> dict:
    """Execute SQL with read-only connection and timeout.

    Returns: {"ok", "results", "columns", "row_count"}
    """
    from src.core.db import get_db

    start = time.time()

    try:
        with get_db() as conn:
            # CRITICAL: read-only mode
            conn.execute("PRAGMA query_only = ON")

            # Set up timeout via progress handler
            deadline = start + _QUERY_TIMEOUT

            def _progress_check():
                if time.time() > deadline:
                    return 1  # non-zero cancels the query
                return 0

            conn.set_progress_handler(_progress_check, 1000)

            try:
                cursor = conn.execute(sql)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()

                # Convert sqlite3.Row to dicts
                results = []
                for row in rows:
                    if isinstance(row, sqlite3.Row):
                        results.append(dict(row))
                    elif isinstance(row, (tuple, list)):
                        results.append(dict(zip(columns, row)))
                    else:
                        results.append(dict(row))

                duration_ms = int((time.time() - start) * 1000)
                return {
                    "ok": True,
                    "results": results,
                    "columns": columns,
                    "row_count": len(results),
                    "duration_ms": duration_ms,
                }

            finally:
                conn.set_progress_handler(None, 0)
                conn.execute("PRAGMA query_only = OFF")

    except sqlite3.OperationalError as e:
        if "interrupted" in str(e).lower():
            return {"ok": False, "error": f"Query timed out after {_QUERY_TIMEOUT}s"}
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# QUERY LOGGING
# ═══════════════════════════════════════════════════════════════════════════

def _log_query(query_text: str, sql: str, result_count: int,
               duration_ms: int, success: bool, error: str = ""):
    """Write query to nl_query_log table for audit."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                """INSERT INTO nl_query_log
                   (query_text, generated_sql, result_count, duration_ms, success, error)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (query_text, sql, result_count, duration_ms,
                 1 if success else 0, error)
            )
    except Exception as e:
        log.debug("Failed to log NL query: %s", e)


def get_query_history(limit: int = 20) -> list:
    """Get recent NL query history."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT query_text, generated_sql, result_count, duration_ms,
                          success, error, queried_at
                   FROM nl_query_log ORDER BY queried_at DESC LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("Failed to get query history: %s", e)
        return []


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════

def nl_query(query_text: str) -> dict:
    """Translate natural language to SQL, execute, and return results.

    Returns: {"ok", "sql", "explanation", "results", "columns", "row_count", "duration_ms"}
    """
    if not query_text or not query_text.strip():
        return {"ok": False, "error": "Empty query"}

    # Rate limit check
    if not _check_rate_limit():
        log.warning("NL query rate limited: %s", query_text[:80])
        return {"ok": False, "error": "Rate limit exceeded (max 10 queries/minute)"}

    start = time.time()

    # Step 1: Generate SQL
    gen_result = _generate_sql(query_text)
    sql = gen_result.get("sql", "")
    explanation = gen_result.get("explanation", "")

    if not sql:
        _log_query(query_text, "", 0, 0, False, "No SQL generated")
        return {"ok": False, "error": "Could not generate a query", "explanation": explanation}

    # Step 2: Validate SQL
    valid, err = _validate_sql(sql)
    if not valid:
        log.warning("NL query blocked: %s → %s (reason: %s)", query_text[:80], sql[:100], err)
        _log_query(query_text, sql, 0, 0, False, f"Blocked: {err}")
        return {"ok": False, "error": f"Query blocked: {err}", "sql": sql}

    # Step 3: Inject LIMIT if missing
    sql = _inject_limit(sql)

    # Step 4: Execute with guardrails
    exec_result = _execute_safe(sql)
    total_ms = int((time.time() - start) * 1000)

    if not exec_result.get("ok"):
        error = exec_result.get("error", "Unknown error")
        log.warning("NL query failed: %s → %s (error: %s)", query_text[:80], sql[:100], error)
        _log_query(query_text, sql, 0, total_ms, False, error)
        return {"ok": False, "error": error, "sql": sql, "explanation": explanation}

    result_count = exec_result.get("row_count", 0)
    _log_query(query_text, sql, result_count, total_ms, True)

    log.info("NL query: '%s' → %d rows in %dms", query_text[:60], result_count, total_ms)

    return {
        "ok": True,
        "sql": sql,
        "explanation": explanation,
        "results": exec_result.get("results", []),
        "columns": exec_result.get("columns", []),
        "row_count": result_count,
        "duration_ms": total_ms,
    }
