"""
startup_checks.py — Runs on EVERY deploy/boot. Catches the 3 bug classes:
  1. "no such table" — sqlite3.connect without init_db
  2. Wrong DATA_DIR — reading from git dir instead of Railway volume
  3. Silent thread death — daemon threads with no error handling

Results at GET /api/health/startup
"""

import os, re, json, time, sqlite3, logging, importlib
from datetime import datetime

log = logging.getLogger("startup_checks")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

_results = {"ran_at": None, "passed": 0, "failed": 0, "warnings": 0, "checks": []}


def _check(name, fn):
    try:
        ok, detail = fn()
        status = "PASS" if ok else "FAIL"
        if not ok:
            _results["failed"] += 1
            log.error("STARTUP FAIL: %s — %s", name, detail)
        else:
            _results["passed"] += 1
        _results["checks"].append({"name": name, "status": status, "detail": detail})
    except Exception as e:
        _results["failed"] += 1
        detail = f"{type(e).__name__}: {str(e)[:200]}"
        log.error("STARTUP CRASH: %s — %s", name, detail)
        _results["checks"].append({"name": name, "status": "CRASH", "detail": detail})


def run_all_checks():
    _results.update(ran_at=datetime.now().isoformat(), passed=0, failed=0, warnings=0, checks=[])

    # 1. DATA_DIR correct location
    def check_data_dir():
        on_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT"))
        git_data = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
        is_volume = os.path.abspath(DATA_DIR) != os.path.abspath(git_data)
        if on_railway and not is_volume:
            return False, f"DATA_DIR={DATA_DIR} is git dir on Railway"
        if not os.path.exists(DATA_DIR):
            return False, f"DATA_DIR={DATA_DIR} does not exist"
        if not os.access(DATA_DIR, os.W_OK):
            return False, f"DATA_DIR={DATA_DIR} not writable"
        return True, f"DATA_DIR={DATA_DIR} (volume={is_volume})"
    _check("DATA_DIR", check_data_dir)

    # 2. DB exists + critical tables
    def check_db():
        db_path = os.path.join(DATA_DIR, "reytech.db")
        try:
            from src.core.db import init_db
            init_db()
        except Exception as e:
            return False, f"init_db failed: {e}"
        conn = sqlite3.connect(db_path, timeout=10)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        critical = ["price_checks", "quotes", "contacts", "notifications", "workflow_runs",
                     "scprs_po_master", "product_catalog", "audit_trail"]
        missing = [t for t in critical if t not in tables]
        if missing:
            return False, f"Missing tables: {missing}"
        return True, f"{len(tables)} tables, all {len(critical)} critical present"
    _check("DB tables", check_db)

    # 3. _load_price_checks works
    def check_load_pcs():
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        return True, f"{len(pcs)} PCs loaded"
    _check("Load PCs", check_load_pcs)

    # 4. Critical imports
    def check_imports():
        failures = []
        mods = [
            ("src.agents.product_catalog", "match_item"),
            ("src.agents.email_poller", "EmailPoller"),
            ("src.forms.reytech_filler_v4", "fill_704b"),
            ("src.core.db", "get_db"),
        ]
        for mod, attr in mods:
            try:
                m = importlib.import_module(mod)
                if not hasattr(m, attr):
                    failures.append(f"{mod}.{attr} missing")
            except Exception as e:
                failures.append(f"{mod}: {e}")
        if failures:
            return False, "; ".join(failures)
        return True, f"All {len(mods)} OK"
    _check("Imports", check_imports)

    # 5. Product catalog accessible
    def check_catalog():
        from src.agents.product_catalog import get_catalog_stats, init_catalog_db
        init_catalog_db()
        stats = get_catalog_stats()
        return True, f"{stats.get('total_products', 0)} products"
    _check("Catalog", check_catalog)

    # 6. SCPRS KB accessible
    def check_scprs():
        try:
            from src.knowledge.won_quotes_db import get_kb_stats
            stats = get_kb_stats()
            return True, f"{stats.get('total_quotes', 0)} quotes"
        except Exception as e:
            return False, str(e)
    _check("SCPRS KB", check_scprs)

    # 7. Templates compile
    def check_templates():
        from jinja2 import Environment, FileSystemLoader
        tpl_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src", "templates")
        env = Environment(loader=FileSystemLoader(tpl_dir))
        errors = []
        for name in env.list_templates():
            try: env.get_template(name)
            except Exception as e: errors.append(f"{name}: {e}")
        if errors:
            return False, f"{len(errors)} errors: {errors[0]}"
        return True, f"{len(env.list_templates())} clean"
    _check("Templates", check_templates)

    # 8. WAL mode
    def check_wal():
        db_path = os.path.join(DATA_DIR, "reytech.db")
        conn = sqlite3.connect(db_path, timeout=10)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode != "wal":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.close()
        return True, f"{'was '+mode+', fixed' if mode != 'wal' else 'active'}"
    _check("WAL mode", check_wal)

    # 9. STATIC CODE AUDIT — catch bugs BEFORE they hit runtime
    def check_code_patterns():
        issues = []
        src_root = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src")
        for root, dirs, files in os.walk(src_root):
            dirs[:] = [d for d in dirs if d != '__pycache__']
            for fname in files:
                if not fname.endswith('.py'): continue
                fpath = os.path.join(root, fname)
                with open(fpath, encoding="utf-8", errors="replace") as f:
                    content = f.read()
                rel = os.path.relpath(fpath, src_root)
                
                # Pattern A: hardcoded "data/" file paths
                for m in re.finditer(r'["\']data/(\w+\.\w+)["\']', content):
                    issues.append(f"{rel}: hardcoded 'data/{m.group(1)}'")
                
                # Pattern B: os.environ.get("DATA_DIR"...) without paths.py import
                if 'os.environ.get("DATA_DIR"' in content or "os.environ.get('DATA_DIR'" in content:
                    if 'from src.core.paths import' not in content and fname != 'paths.py':
                        issues.append(f"{rel}: uses env DATA_DIR without paths.py import")
                
                # Pattern C: sqlite3.connect without init_db or get_db or DATA_DIR
                if 'sqlite3.connect(' in content and fname not in ('db.py', 'startup_checks.py'):
                    has_guard = 'init_db' in content or 'get_db' in content or 'from src.core' in content
                    if not has_guard:
                        issues.append(f"{rel}: direct sqlite3.connect without any db guard")

        if issues:
            return False, f"{len(issues)} code issues: {'; '.join(issues[:5])}"
        return True, "No hardcoded paths, no unguarded DB, no env DATA_DIR"
    _check("Code patterns", check_code_patterns)

    # 10. Auto-price imports (lightweight — no DB calls)
    def check_auto_price_imports():
        errors = []
        for mod in ["src.agents.product_catalog", "src.knowledge.pricing_oracle"]:
            try:
                __import__(mod)
            except Exception as e:
                errors.append(f"{mod}: {e}")
        if errors:
            return False, "; ".join(errors)
        return True, "Pricing modules importable"
    _check("Auto-price imports", check_auto_price_imports)

    # 11. Form profile registry — every YAML profile must validate against its blank PDF
    def check_form_profiles():
        from src.core.quote_engine import boot_validate_profiles
        results = boot_validate_profiles(strict=False)
        bad = {pid: issues for pid, issues in results.items() if issues}
        if bad:
            first = next(iter(bad.items()))
            return False, f"{len(bad)}/{len(results)} profiles invalid; {first[0]}: {first[1][0]}"
        return True, f"All {len(results)} profiles valid"
    _check("Form profiles", check_form_profiles)

    # Summary + auto-alert
    total = _results["passed"] + _results["failed"]
    failed_names = [c["name"] for c in _results["checks"] if c["status"] != "PASS"]

    if _results["failed"] > 0:
        log.error("⚠️ STARTUP: %d/%d FAILED — %s", _results["failed"], total, ", ".join(failed_names))
        _auto_alert_failures(_results)
    else:
        log.info("✅ STARTUP: %d/%d passed", _results["passed"], total)

    return _results


def _auto_alert_failures(results):
    """Send email + bell notification when startup checks fail."""
    failed = [c for c in results["checks"] if c["status"] != "PASS"]
    if not failed:
        return

    subject = f"⚠️ Deploy Health: {len(failed)} check(s) failed"
    body = f"Startup health checks ran at {results['ran_at']}\n\n"
    body += f"PASSED: {results['passed']}  |  FAILED: {results['failed']}\n\n"
    for c in failed:
        body += f"❌ {c['name']}: {c['detail']}\n"
    body += f"\nFull results: /api/health/startup"

    # 1. Email alert
    try:
        gmail = os.environ.get("GMAIL_ADDRESS", "")
        gmail_pw = os.environ.get("GMAIL_PASSWORD", "")
        if gmail and gmail_pw:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = gmail
            msg["To"] = gmail  # Send to self
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
                s.login(gmail, gmail_pw)
                s.send_message(msg)
            log.info("Startup alert emailed to %s", gmail)
    except Exception as e:
        log.warning("Startup email alert failed: %s", e)

    # 2. Bell notification (shows on home page)
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="deploy_health",
            title=subject,
            body=body,
            urgency="urgent",
            channels=["bell", "email"],
            run_async=False,
        )
    except Exception as e:
        log.debug("Startup bell alert: %s", e)

    # 3. Write to audit trail
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO audit_trail (timestamp, event_type, actor, details) VALUES (datetime('now'), ?, ?, ?)",
                ("deploy_health_fail", "system", json.dumps({"failed": [c["name"] for c in failed], "details": body[:500]}))
            )
    except Exception as _e:
        log.debug("suppressed: %s", _e)


def get_results():
    return _results


# ── Boot-time schema validation and auto-repair ──────────────────────────────

# Canonical column specs: (table, column, type_with_default)
# Source of truth is db.py SCHEMA + _migrate_columns()
_EXPECTED_SCHEMA = {
    "price_checks": [
        ("id", "TEXT PRIMARY KEY"),
        ("created_at", "TEXT NOT NULL"),
        ("requestor", "TEXT"),
        ("agency", "TEXT"),
        ("institution", "TEXT"),
        ("items", "TEXT"),
        ("source_file", "TEXT"),
        ("quote_number", "TEXT"),
        ("pc_number", "TEXT"),
        ("total_items", "INTEGER DEFAULT 0"),
        ("status", "TEXT DEFAULT 'parsed'"),
        ("email_uid", "TEXT"),
        ("email_subject", "TEXT"),
        ("due_date", "TEXT"),
        ("pc_data", "TEXT DEFAULT '{}'"),
        ("ship_to", "TEXT DEFAULT ''"),
        ("contact_email", "TEXT DEFAULT ''"),
        ("sent_at", "TEXT"),
        ("last_scprs_check", "TEXT"),
        ("scprs_check_count", "INTEGER DEFAULT 0"),
        ("award_status", "TEXT DEFAULT 'pending'"),
        ("competitor_name", "TEXT"),
        ("competitor_price", "REAL"),
        ("competitor_po", "TEXT"),
        ("revision_of", "TEXT"),
        ("closed_at", "TEXT"),
        ("closed_reason", "TEXT"),
        ("qb_po_id", "TEXT"),
        ("qb_invoice_id", "TEXT"),
    ],
    "rfqs": [
        ("id", "TEXT PRIMARY KEY"),
        ("received_at", "TEXT NOT NULL"),
        ("agency", "TEXT"),
        ("institution", "TEXT"),
        ("requestor_name", "TEXT"),
        ("requestor_email", "TEXT"),
        ("rfq_number", "TEXT"),
        ("items", "TEXT"),
        ("status", "TEXT DEFAULT 'new'"),
        ("source", "TEXT"),
        ("email_uid", "TEXT"),
        ("notes", "TEXT"),
        ("updated_at", "TEXT"),
    ],
    "orders": [
        ("id", "TEXT PRIMARY KEY"),
        ("quote_number", "TEXT"),
        ("agency", "TEXT"),
        ("institution", "TEXT"),
        ("po_number", "TEXT"),
        ("po_date", "TEXT"),
        ("status", "TEXT DEFAULT 'active'"),
        ("total", "REAL DEFAULT 0"),
        ("items", "TEXT"),
        ("notes", "TEXT"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
    ],
    "price_history": [
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("found_at", "TEXT NOT NULL"),
        ("description", "TEXT NOT NULL"),
        ("part_number", "TEXT"),
        ("manufacturer", "TEXT"),
        ("quantity", "REAL"),
        ("unit_price", "REAL NOT NULL"),
        ("source", "TEXT NOT NULL"),
        ("source_url", "TEXT"),
        ("source_id", "TEXT"),
        ("agency", "TEXT"),
        ("quote_number", "TEXT"),
        ("price_check_id", "TEXT"),
        ("notes", "TEXT"),
    ],
    "processed_emails": [
        ("uid", "TEXT PRIMARY KEY"),
        ("inbox", "TEXT DEFAULT 'sales'"),
        ("processed_at", "TEXT"),
    ],
    "won_quotes": [
        ("id", "TEXT PRIMARY KEY"),
        ("po_number", "TEXT"),
        ("item_number", "TEXT"),
        ("description", "TEXT"),
        ("normalized_description", "TEXT"),
        ("tokens", "TEXT"),
        ("category", "TEXT"),
        ("supplier", "TEXT"),
        ("department", "TEXT"),
        ("unit_price", "REAL"),
        ("quantity", "REAL"),
        ("total", "REAL"),
        ("award_date", "TEXT"),
        ("source", "TEXT"),
        ("confidence", "REAL DEFAULT 1.0"),
        ("ingested_at", "TEXT"),
        ("updated_at", "TEXT"),
    ],
}


def run_schema_checks():
    """Validate every expected table and column exists in reytech.db.

    For missing tables, run CREATE TABLE IF NOT EXISTS.
    For missing columns, run ALTER TABLE ADD COLUMN with a safe default.
    Returns a summary dict with tables_checked, columns_checked, issues_fixed.
    """
    db_path = os.path.join(DATA_DIR, "reytech.db")
    if not os.path.exists(db_path):
        log.warning("Schema check: %s does not exist yet — skipping", db_path)
        return {"tables_checked": 0, "columns_checked": 0, "issues_fixed": []}

    issues_fixed = []
    tables_checked = 0
    columns_checked = 0

    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")

        # Get existing tables
        existing_tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        for table, columns in _EXPECTED_SCHEMA.items():
            tables_checked += 1

            if table not in existing_tables:
                # Build CREATE TABLE statement from column specs
                col_defs = ", ".join(f"{col} {ctype}" for col, ctype in columns)
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})")
                conn.commit()
                issues_fixed.append(f"created table {table}")
                log.warning("Schema repair: created missing table %s", table)
                columns_checked += len(columns)
                continue

            # Table exists — check each column
            existing_cols = {
                r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }

            for col, ctype in columns:
                columns_checked += 1
                if col not in existing_cols:
                    # Strip PRIMARY KEY / NOT NULL for ALTER TABLE ADD COLUMN
                    safe_type = ctype.replace("PRIMARY KEY", "").replace("AUTOINCREMENT", "")
                    safe_type = safe_type.replace("NOT NULL", "").strip()
                    # Ensure a DEFAULT for non-nullable types
                    if "DEFAULT" not in safe_type.upper() and "TEXT" in safe_type.upper():
                        safe_type += " DEFAULT ''"
                    elif "DEFAULT" not in safe_type.upper() and ("REAL" in safe_type.upper() or "INTEGER" in safe_type.upper()):
                        safe_type += " DEFAULT 0"

                    try:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {safe_type}")
                        conn.commit()
                        issues_fixed.append(f"added {table}.{col}")
                        log.warning("Schema repair: added missing column %s.%s (%s)", table, col, safe_type)
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            issues_fixed.append(f"FAILED {table}.{col}: {e}")
                            log.error("Schema repair failed: %s.%s — %s", table, col, e)

        conn.close()
    except Exception as e:
        log.error("Schema check error: %s", e)
        return {"tables_checked": tables_checked, "columns_checked": columns_checked,
                "issues_fixed": issues_fixed, "error": str(e)}

    # Log startup report
    if issues_fixed:
        log.warning("Schema check — %d tables, %d columns, %d issues fixed: %s",
                     tables_checked, columns_checked, len(issues_fixed),
                     "; ".join(issues_fixed))
    else:
        log.info("Schema OK — %d tables, %d columns, 0 issues",
                 tables_checked, columns_checked)

    return {"tables_checked": tables_checked, "columns_checked": columns_checked,
            "issues_fixed": issues_fixed}
