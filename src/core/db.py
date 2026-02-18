"""
src/core/db.py — Persistent SQLite Database Layer

WHY THIS EXISTS:
  Railway containers have ephemeral filesystems. Every git push triggers a redeploy
  which resets /app/data/*.json to whatever was last committed. Any runtime writes
  (new quotes, CRM contacts, SCPRS prices found) are LOST on redeploy.

SOLUTION:
  1. Railway Volume mounted at /data (set RAILWAY_VOLUME_MOUNT_PATH=/data in Railway UI)
     → Files in /data survive every redeploy, restart, and crash
  2. SQLite database at /data/reytech.db for all structured data
     → Single file, zero dependencies, full SQL, WAL mode for concurrent workers
  3. JSON files in /data/*.json as secondary write path (keeps existing code working)
     → Every write goes to BOTH SQLite AND JSON files on the volume

SETUP (one-time in Railway UI):
  1. railway.app → your project → your service → Storage → Add Volume
     Mount Path: /data
  2. Add env var: REYTECH_DATA_DIR=/data
  3. Redeploy → data now persists forever

TABLES:
  quotes         — every quote generated with full line-item pricing
  price_history  — every price found (Amazon, SCPRS, GSA) per item description
  contacts       — CRM contacts with all fields
  activity_log   — every interaction logged against a contact
  orders         — won quotes → POs
  rfqs           — inbound email RFQs
  revenue_log    — manual + QB revenue entries toward $2M goal
  intel_pulls    — SCPRS deep pull runs with stats
"""

import os
import json
import sqlite3
import logging
import threading
from datetime import datetime
from contextlib import contextmanager

log = logging.getLogger("reytech.db")

# ── Path resolution ───────────────────────────────────────────────────────────
# Priority: REYTECH_DATA_DIR env var (Railway volume) → project /data directory
def _resolve_data_dir() -> str:
    """Return the persistent data directory path."""
    env_path = os.environ.get("REYTECH_DATA_DIR", "")
    if env_path and os.path.isdir(env_path):
        return env_path
    # Railway volume not configured — fall back to project data dir
    _here = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(_here)))
    return os.path.join(project_root, "data")

DATA_DIR = _resolve_data_dir()
DB_PATH = os.path.join(DATA_DIR, "reytech.db")
os.makedirs(DATA_DIR, exist_ok=True)

_db_lock = threading.Lock()

# ── Connection factory ────────────────────────────────────────────────────────
@contextmanager
def get_db():
    """Thread-safe SQLite connection with WAL mode for 2-worker gunicorn."""
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_number    TEXT UNIQUE NOT NULL,
    created_at      TEXT NOT NULL,
    agency          TEXT,
    institution     TEXT,
    requestor       TEXT,
    contact_email   TEXT,
    contact_phone   TEXT,
    rfq_number      TEXT,
    ship_to_name    TEXT,
    ship_to_address TEXT,
    subtotal        REAL DEFAULT 0,
    tax             REAL DEFAULT 0,
    total           REAL DEFAULT 0,
    items_count     INTEGER DEFAULT 0,
    items_text      TEXT,
    items_detail    TEXT,           -- JSON array of line items with full pricing
    status          TEXT DEFAULT 'pending',
    pdf_path        TEXT,
    source_pc_id    TEXT,
    source_rfq_id   TEXT,
    po_number       TEXT,
    status_notes    TEXT,
    is_test         INTEGER DEFAULT 0,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    found_at        TEXT NOT NULL,
    description     TEXT NOT NULL,
    part_number     TEXT,
    manufacturer    TEXT,
    quantity        REAL,
    unit_price      REAL NOT NULL,
    source          TEXT NOT NULL,  -- amazon|scprs|gsa|manual|won_quote
    source_url      TEXT,
    source_id       TEXT,           -- ASIN, PO number, etc
    agency          TEXT,           -- which agency this price was for
    quote_number    TEXT,           -- quote it was used in
    price_check_id  TEXT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_price_desc ON price_history(description);
CREATE INDEX IF NOT EXISTS idx_price_pn ON price_history(part_number);
CREATE INDEX IF NOT EXISTS idx_price_src ON price_history(source);

CREATE TABLE IF NOT EXISTS contacts (
    id              TEXT PRIMARY KEY,  -- matches crm_contacts.json keys
    created_at      TEXT NOT NULL,
    buyer_name      TEXT,
    buyer_email     TEXT,
    buyer_phone     TEXT,
    agency          TEXT,
    title           TEXT,
    department      TEXT,
    linkedin        TEXT,
    notes           TEXT,
    tags            TEXT,           -- JSON array
    total_spend     REAL DEFAULT 0,
    po_count        INTEGER DEFAULT 0,
    categories      TEXT,           -- JSON object
    items_purchased TEXT,           -- JSON array
    purchase_orders TEXT,           -- JSON array
    last_purchase   TEXT,
    score           REAL DEFAULT 0,
    opportunity_score INTEGER DEFAULT 0,
    is_reytech_customer INTEGER DEFAULT 0,
    outreach_status TEXT DEFAULT 'new',
    source          TEXT DEFAULT 'manual',
    intel_synced_at TEXT,
    updated_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_contact_email ON contacts(buyer_email);
CREATE INDEX IF NOT EXISTS idx_contact_agency ON contacts(agency);

CREATE TABLE IF NOT EXISTS activity_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id      TEXT NOT NULL,
    logged_at       TEXT NOT NULL,
    event_type      TEXT NOT NULL,  -- email_sent|email_received|voice_called|note|meeting
    subject         TEXT,
    body            TEXT,
    outcome         TEXT,
    actor           TEXT DEFAULT 'user',
    metadata        TEXT            -- JSON for extra fields
);

CREATE INDEX IF NOT EXISTS idx_activity_contact ON activity_log(contact_id);

CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    quote_number    TEXT,
    agency          TEXT,
    institution     TEXT,
    po_number       TEXT,
    po_date         TEXT,
    status          TEXT DEFAULT 'active',
    total           REAL DEFAULT 0,
    items           TEXT,           -- JSON
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS rfqs (
    id              TEXT PRIMARY KEY,
    received_at     TEXT NOT NULL,
    agency          TEXT,
    institution     TEXT,
    requestor_name  TEXT,
    requestor_email TEXT,
    rfq_number      TEXT,
    items           TEXT,           -- JSON array of items
    status          TEXT DEFAULT 'new',
    source          TEXT,           -- email|manual
    email_uid       TEXT,
    notes           TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS revenue_log (
    id              TEXT PRIMARY KEY,
    logged_at       TEXT NOT NULL,
    amount          REAL NOT NULL,
    description     TEXT NOT NULL,
    source          TEXT DEFAULT 'manual', -- manual|quote_won|qb
    quote_number    TEXT,
    po_number       TEXT,
    agency          TEXT,
    date            TEXT
);

CREATE TABLE IF NOT EXISTS intel_pulls (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT DEFAULT 'running',
    queries_run     INTEGER DEFAULT 0,
    pos_scanned     INTEGER DEFAULT 0,
    buyers_found    INTEGER DEFAULT 0,
    agencies_found  INTEGER DEFAULT 0,
    error           TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS price_checks (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    requestor       TEXT,
    agency          TEXT,
    items           TEXT,           -- JSON with full pricing per item
    source_file     TEXT,
    quote_number    TEXT,
    total_items     INTEGER DEFAULT 0
);
"""

def init_db():
    """Create all tables if they don't exist. Safe to call multiple times."""
    with get_db() as conn:
        conn.executescript(SCHEMA)
    log.info("DB initialized at %s", DB_PATH)
    return True

# ── Quote operations ──────────────────────────────────────────────────────────
def upsert_quote(q: dict) -> bool:
    """Insert or update a quote record. Called from _log_quote()."""
    now = datetime.now().isoformat()
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO quotes
                  (quote_number, created_at, agency, institution, requestor,
                   contact_email, contact_phone, rfq_number, ship_to_name,
                   ship_to_address, subtotal, tax, total, items_count,
                   items_text, items_detail, status, pdf_path,
                   source_pc_id, source_rfq_id, po_number, is_test, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(quote_number) DO UPDATE SET
                  agency=excluded.agency, institution=excluded.institution,
                  total=excluded.total, subtotal=excluded.subtotal,
                  items_detail=excluded.items_detail, items_text=excluded.items_text,
                  status=excluded.status, pdf_path=excluded.pdf_path,
                  po_number=excluded.po_number, updated_at=excluded.updated_at
            """, (
                q.get("quote_number"), q.get("created_at", now),
                q.get("agency"), q.get("institution"),
                q.get("requestor") or q.get("contact_name"),
                q.get("email") or q.get("requestor_email") or q.get("contact_email"),
                q.get("phone") or q.get("contact_phone"),
                q.get("rfq_number"),
                q.get("ship_to_name"),
                json.dumps(q.get("ship_to_address", [])),
                q.get("subtotal", 0), q.get("tax", 0), q.get("total", 0),
                q.get("items_count", 0),
                q.get("items_text", ""),
                json.dumps(q.get("items_detail", [])),
                q.get("status", "pending"),
                q.get("pdf_path") or q.get("path"),
                q.get("source_pc_id"), q.get("source_rfq_id"),
                q.get("po_number"),
                1 if q.get("is_test") else 0,
                now,
            ))
        return True
    except Exception as e:
        log.error("upsert_quote %s: %s", q.get("quote_number"), e)
        return False


def get_quote(quote_number: str) -> dict | None:
    """Fetch a quote by number."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM quotes WHERE quote_number=?",
                           (quote_number,)).fetchone()
    if not row:
        return None
    d = dict(row)
    for field in ("items_detail", "ship_to_address"):
        if d.get(field):
            try: d[field] = json.loads(d[field])
            except: pass
    return d


def get_all_quotes_db(status: str = None, limit: int = 500) -> list:
    """Get all quotes from SQLite, newest first."""
    with get_db() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM quotes WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM quotes ORDER BY created_at DESC LIMIT ?",
                (limit,)).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        for field in ("items_detail", "ship_to_address"):
            if d.get(field):
                try: d[field] = json.loads(d[field])
                except: pass
        result.append(d)
    return result


# ── Price history operations ──────────────────────────────────────────────────
def record_price(description: str, unit_price: float, source: str,
                 part_number: str = "", manufacturer: str = "",
                 quantity: float = 1, source_url: str = "",
                 source_id: str = "", agency: str = "",
                 quote_number: str = "", price_check_id: str = "",
                 notes: str = "") -> int | None:
    """Record a price observation. Called every time a price is found."""
    if not description or not unit_price or unit_price <= 0:
        return None
    try:
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO price_history
                  (found_at, description, part_number, manufacturer, quantity,
                   unit_price, source, source_url, source_id, agency,
                   quote_number, price_check_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                datetime.now().isoformat(),
                description[:500], part_number[:100], manufacturer[:200],
                quantity, unit_price, source, source_url[:500],
                source_id[:100], agency, quote_number, price_check_id,
                notes[:500],
            ))
            return cur.lastrowid
    except Exception as e:
        log.error("record_price '%s': %s", description[:40], e)
        return None


def get_price_history_db(description: str = "", part_number: str = "",
                          source: str = "", limit: int = 50) -> list:
    """Look up historical prices for an item. Returns newest first."""
    conditions = []
    params = []
    if description:
        conditions.append("LOWER(description) LIKE ?")
        params.append(f"%{description.lower()[:100]}%")
    if part_number:
        conditions.append("LOWER(part_number) LIKE ?")
        params.append(f"%{part_number.lower()}%")
    if source:
        conditions.append("source=?")
        params.append(source)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM price_history {where} ORDER BY found_at DESC LIMIT ?",
            params).fetchall()
    return [dict(r) for r in rows]


def get_price_stats() -> dict:
    """Summary stats for the price history database."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        by_source = conn.execute(
            "SELECT source, COUNT(*) as cnt, AVG(unit_price) as avg_price "
            "FROM price_history GROUP BY source").fetchall()
        recent = conn.execute(
            "SELECT description, unit_price, source, found_at "
            "FROM price_history ORDER BY found_at DESC LIMIT 10").fetchall()
    return {
        "total_prices": total,
        "by_source": [dict(r) for r in by_source],
        "recent": [dict(r) for r in recent],
    }


# ── Contact operations ────────────────────────────────────────────────────────
def upsert_contact(c: dict) -> bool:
    """Insert or update a CRM contact."""
    now = datetime.now().isoformat()
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO contacts
                  (id, created_at, buyer_name, buyer_email, buyer_phone,
                   agency, title, department, linkedin, notes, tags,
                   total_spend, po_count, categories, items_purchased,
                   purchase_orders, last_purchase, score, opportunity_score,
                   is_reytech_customer, outreach_status, source,
                   intel_synced_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  buyer_name=COALESCE(excluded.buyer_name, buyer_name),
                  buyer_email=COALESCE(excluded.buyer_email, buyer_email),
                  buyer_phone=COALESCE(excluded.buyer_phone, buyer_phone),
                  agency=COALESCE(excluded.agency, agency),
                  title=COALESCE(NULLIF(excluded.title,''), title),
                  department=COALESCE(NULLIF(excluded.department,''), department),
                  linkedin=COALESCE(NULLIF(excluded.linkedin,''), linkedin),
                  notes=COALESCE(NULLIF(excluded.notes,''), notes),
                  total_spend=MAX(excluded.total_spend, total_spend),
                  po_count=MAX(excluded.po_count, po_count),
                  categories=excluded.categories,
                  items_purchased=excluded.items_purchased,
                  outreach_status=excluded.outreach_status,
                  intel_synced_at=excluded.intel_synced_at,
                  updated_at=excluded.updated_at
            """, (
                c.get("id"), c.get("created_at", now),
                c.get("buyer_name"), c.get("buyer_email"),
                c.get("buyer_phone"), c.get("agency"),
                c.get("title"), c.get("department"),
                c.get("linkedin"), c.get("notes"),
                json.dumps(c.get("tags", [])),
                c.get("total_spend", 0), c.get("po_count", 0),
                json.dumps(c.get("categories", {})),
                json.dumps(c.get("items_purchased", [])),
                json.dumps(c.get("purchase_orders", [])),
                c.get("last_purchase"),
                c.get("score", 0), c.get("opportunity_score", 0),
                1 if c.get("is_reytech_customer") else 0,
                c.get("outreach_status", "new"),
                c.get("source", "manual"),
                c.get("intel_synced_at"),
                now,
            ))
        return True
    except Exception as e:
        log.error("upsert_contact %s: %s", c.get("id"), e)
        return False


def log_activity(contact_id: str, event_type: str, subject: str = "",
                  body: str = "", outcome: str = "", actor: str = "user",
                  metadata: dict = None) -> int | None:
    """Log an interaction against a contact."""
    try:
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO activity_log
                  (contact_id, logged_at, event_type, subject, body, outcome, actor, metadata)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                contact_id, datetime.now().isoformat(),
                event_type, subject[:500], body[:5000],
                outcome[:200], actor,
                json.dumps(metadata or {}),
            ))
            return cur.lastrowid
    except Exception as e:
        log.error("log_activity %s: %s", contact_id, e)
        return None


def get_contact_activity(contact_id: str, limit: int = 100) -> list:
    """Get activity log for a contact, newest first."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM activity_log WHERE contact_id=? ORDER BY logged_at DESC LIMIT ?",
            (contact_id, limit)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("metadata"):
            try: d["metadata"] = json.loads(d["metadata"])
            except: pass
        result.append(d)
    return result


# ── Revenue operations ────────────────────────────────────────────────────────
def log_revenue(amount: float, description: str, source: str = "manual",
                quote_number: str = "", po_number: str = "",
                agency: str = "", date: str = "") -> str | None:
    """Record a revenue entry."""
    rid = f"REV-{datetime.now().strftime('%Y%m%d')}-{os.urandom(3).hex()}"
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO revenue_log
                  (id, logged_at, amount, description, source,
                   quote_number, po_number, agency, date)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (rid, datetime.now().isoformat(), amount, description,
                  source, quote_number, po_number, agency,
                  date or datetime.now().strftime("%Y-%m-%d")))
        return rid
    except Exception as e:
        log.error("log_revenue: %s", e)
        return None


def get_revenue_total(year: int = None) -> dict:
    """Get revenue totals for a year (default current year)."""
    year = year or datetime.now().year
    with get_db() as conn:
        rows = conn.execute(
            "SELECT source, SUM(amount) as total, COUNT(*) as count "
            "FROM revenue_log WHERE date LIKE ? GROUP BY source",
            (f"{year}%",)).fetchall()
        total_row = conn.execute(
            "SELECT SUM(amount) FROM revenue_log WHERE date LIKE ?",
            (f"{year}%",)).fetchone()
    by_source = {r["source"]: {"total": r["total"], "count": r["count"]}
                 for r in rows}
    return {
        "year": year,
        "total": float(total_row[0] or 0),
        "by_source": by_source,
    }


# ── DB stats ─────────────────────────────────────────────────────────────────
def get_db_stats() -> dict:
    """Return row counts for all tables — used in /api/metrics."""
    tables = ["quotes", "price_history", "contacts", "activity_log",
              "orders", "rfqs", "revenue_log", "intel_pulls", "price_checks"]
    stats = {"db_path": DB_PATH, "db_size_kb": 0}
    try:
        stats["db_size_kb"] = round(os.path.getsize(DB_PATH) / 1024, 1)
    except FileNotFoundError:
        pass
    with get_db() as conn:
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[table] = count
            except Exception:
                stats[table] = 0
    return stats


# ── Migration: JSON → SQLite ──────────────────────────────────────────────────
def migrate_json_to_db() -> dict:
    """One-time migration of all existing JSON files into SQLite.
    Safe to call multiple times — uses INSERT OR IGNORE.
    """
    counts = {"quotes": 0, "contacts": 0, "revenue": 0, "errors": []}

    # Quotes
    quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
    try:
        with open(quotes_path) as f:
            quotes = json.load(f)
        if isinstance(quotes, list):
            for q in quotes:
                if upsert_quote(q):
                    counts["quotes"] += 1
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    except Exception as e:
        counts["errors"].append(f"quotes: {e}")

    # CRM contacts
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            contacts = json.load(f)
        if isinstance(contacts, dict):
            for cid, c in contacts.items():
                c["id"] = cid
                if upsert_contact(c):
                    counts["contacts"] += 1
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    except Exception as e:
        counts["errors"].append(f"contacts: {e}")

    # Revenue entries
    revenue_path = os.path.join(DATA_DIR, "intel_revenue.json")
    try:
        with open(revenue_path) as f:
            rev = json.load(f)
        if isinstance(rev, dict):
            for entry in rev.get("manual_entries", []):
                try:
                    log_revenue(
                        amount=entry.get("amount", 0),
                        description=entry.get("description", ""),
                        date=entry.get("date", ""),
                        source="manual",
                    )
                    counts["revenue"] += 1
                except Exception:
                    pass
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    except Exception as e:
        counts["errors"].append(f"revenue: {e}")

    log.info("JSON→DB migration: %s", counts)
    return counts


# ── Startup ───────────────────────────────────────────────────────────────────
def startup() -> dict:
    """Initialize DB and migrate existing data. Call once at app start."""
    init_db()
    stats_before = get_db_stats()
    if stats_before.get("quotes", 0) == 0:
        # First run — migrate from JSON
        migrated = migrate_json_to_db()
        log.info("First-run migration complete: %s", migrated)
    stats = get_db_stats()
    log.info("DB ready: %s", {k: v for k, v in stats.items() if k != "db_path"})
    return {"ok": True, "db_path": DB_PATH, "stats": stats,
            "is_volume": DATA_DIR == os.environ.get("REYTECH_DATA_DIR", "")}
