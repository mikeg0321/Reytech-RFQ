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
    # Explicit override (optional)
    env_path = os.environ.get("REYTECH_DATA_DIR", "")
    if env_path and os.path.isdir(env_path):
        return env_path
    # Railway auto-mounts volume — check both common paths
    for candidate in ("/app/data", "/data"):
        if os.path.isdir(candidate):
            # Confirm it's a real volume mount (not just the git data dir baked into image)
            # Railway sets RAILWAY_VOLUME_NAME or RAILWAY_VOLUME_MOUNT_PATH when a volume is attached
            if (os.environ.get("RAILWAY_VOLUME_NAME") or
                os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or
                os.environ.get("RAILWAY_ENVIRONMENT")):
                return candidate
    # Fall back to project /data directory (local dev)
    _here = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(_here)))
    return os.path.join(project_root, "data")

def _is_railway_volume() -> bool:
    """True when running on Railway with a volume actually mounted."""
    return bool(
        os.environ.get("RAILWAY_VOLUME_NAME") or
        os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    )

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

CREATE TABLE IF NOT EXISTS notifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    urgency         TEXT DEFAULT 'info',
    title           TEXT NOT NULL,
    body            TEXT,
    context_json    TEXT,
    deep_link       TEXT,
    is_read         INTEGER DEFAULT 0,
    sms_sent        INTEGER DEFAULT 0,
    email_sent      INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_notif_unread ON notifications(is_read, created_at);
CREATE INDEX IF NOT EXISTS idx_notif_type ON notifications(event_type);

CREATE TABLE IF NOT EXISTS email_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at       TEXT NOT NULL,
    direction       TEXT NOT NULL,
    sender          TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    subject         TEXT,
    body_preview    TEXT,
    full_body       TEXT,
    attachments_json TEXT,
    quote_number    TEXT,
    po_number       TEXT,
    rfq_id          TEXT,
    contact_id      TEXT,
    intent          TEXT,
    status          TEXT DEFAULT 'sent',
    message_id      TEXT,
    thread_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_email_log_contact ON email_log(contact_id);
CREATE INDEX IF NOT EXISTS idx_email_log_quote ON email_log(quote_number);
CREATE INDEX IF NOT EXISTS idx_email_log_po ON email_log(po_number);
CREATE INDEX IF NOT EXISTS idx_email_log_direction ON email_log(direction, logged_at);
CREATE INDEX IF NOT EXISTS idx_email_log_sender ON email_log(sender);

CREATE TABLE IF NOT EXISTS vendor_orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    submitted_at    TEXT NOT NULL,
    updated_at      TEXT,
    vendor_key      TEXT NOT NULL,     -- grainger|amazon_business|curbell_medical|etc
    vendor_name     TEXT NOT NULL,
    po_number       TEXT NOT NULL,     -- Our internal PO (R26Q4-PO-GRAI)
    order_number    TEXT,              -- Vendor's confirmation/order number
    quote_number    TEXT,              -- The Reytech quote this PO is for
    items_json      TEXT,              -- JSON array of ordered items
    total           REAL DEFAULT 0,
    status          TEXT DEFAULT 'submitted',  -- submitted|confirmed|shipped|delivered|failed|po_emailed
    tracking        TEXT,              -- tracking number when shipped
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_vendor_orders_quote ON vendor_orders(quote_number);
CREATE INDEX IF NOT EXISTS idx_vendor_orders_status ON vendor_orders(status);
CREATE INDEX IF NOT EXISTS idx_vendor_orders_vendor ON vendor_orders(vendor_key);
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
    Safe to call multiple times — uses INSERT OR IGNORE / upsert.

    Source priority:
      1. DATA_DIR (volume /app/data — runtime writes live here)
      2. src/seed_data/ (baked into the image at /app/src/seed_data/ — NEVER
         shadowed by the volume mount, so always readable on first boot)

    The root-level data/ directory is intentionally NOT used as a fallback
    because on Railway it resolves to /app/data — the same path as the volume
    mount — meaning it is equally empty on a fresh volume.
    """
    counts = {"quotes": 0, "contacts": 0, "revenue": 0, "errors": []}

    # src/seed_data/ is two levels up from this file (src/core/db.py → src/ → seed_data/)
    _here = os.path.abspath(__file__)
    _seed_data = os.path.join(os.path.dirname(os.path.dirname(_here)), "seed_data")

    # DATA_DIR first (picks up any runtime JSON already written to volume),
    # then seed_data as guaranteed fallback.
    source_dirs = list(dict.fromkeys([DATA_DIR, _seed_data]))  # dedup, volume never == seed_data
    log.info("migrate_json_to_db: source_dirs=%s", source_dirs)

    def _try_load(fname):
        for d in source_dirs:
            p = os.path.join(d, fname)
            if os.path.exists(p) and os.path.getsize(p) > 2:
                try:
                    with open(p) as f:
                        return json.load(f)
                except Exception:
                    continue
        return None

    # Quotes
    quotes = _try_load("quotes_log.json")
    if isinstance(quotes, list):
        for q in quotes:
            if upsert_quote(q):
                counts["quotes"] += 1

    # CRM contacts
    contacts = _try_load("crm_contacts.json")
    if isinstance(contacts, dict):
        for cid, c in contacts.items():
            c["id"] = cid
            if upsert_contact(c):
                counts["contacts"] += 1

    # Revenue entries
    rev = _try_load("intel_revenue.json")
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

    log.info("JSON→DB migration: %s", counts)
    return counts


# ── Startup ───────────────────────────────────────────────────────────────────
def _seed_volume_json() -> dict:
    """On first boot with a fresh volume, copy seed JSON files from src/seed_data/
    into DATA_DIR so the live app can read them (not just SQLite).
    Files that already exist in DATA_DIR are never overwritten.
    Returns dict of {filename: 'copied'|'exists'|'error'}.
    """
    _here = os.path.abspath(__file__)
    _seed_dir = os.path.join(os.path.dirname(os.path.dirname(_here)), "seed_data")
    if not os.path.isdir(_seed_dir):
        return {}
    results = {}
    for fname in os.listdir(_seed_dir):
        if not fname.endswith(".json"):
            continue
        src = os.path.join(_seed_dir, fname)
        dst = os.path.join(DATA_DIR, fname)
        if os.path.exists(dst) and os.path.getsize(dst) > 10:
            results[fname] = "exists"
            continue
        try:
            import shutil
            shutil.copy2(src, dst)
            results[fname] = "copied"
        except Exception as e:
            results[fname] = f"error: {e}"
    copied = [k for k, v in results.items() if v == "copied"]
    if copied:
        log.info("Seeded %d JSON files into volume: %s", len(copied), copied)
    return results


def startup() -> dict:
    """Initialize DB and migrate existing data. Call once at app start."""
    # Step 1: If volume is fresh, copy seed JSON into it so the app can read them
    if _is_railway_volume():
        seed_results = _seed_volume_json()
        copied = [k for k, v in seed_results.items() if v == "copied"]
        if copied:
            log.info("Volume first-boot seed: copied %d files", len(copied))

    init_db()
    stats_before = get_db_stats()
    if stats_before.get("quotes", 0) == 0 and stats_before.get("contacts", 0) == 0:
        # First run — migrate from JSON seed files
        migrated = migrate_json_to_db()
        log.info("First-run migration complete: %s", migrated)
    stats = get_db_stats()
    is_vol = _is_railway_volume()
    log.info("DB ready [volume=%s]: %s", is_vol,
             {k: v for k, v in stats.items() if k not in ("db_path", "db_size_kb")})
    return {"ok": True, "db_path": DB_PATH, "stats": stats, "is_volume": is_vol}


# ══════════════════════════════════════════════════════════════════════════════
# FULL DATA ACCESS LAYER  (Phase 32c — JSON elimination)
# Every function here is a drop-in replacement for a JSON file read/write.
# All consumers should import from here instead of touching .json files.
# ══════════════════════════════════════════════════════════════════════════════

def _row_to_dict(row) -> dict:
    """Convert sqlite3.Row or tuple+description to dict."""
    if row is None:
        return {}
    if hasattr(row, 'keys'):
        return dict(row)
    return dict(row)


def _jl(val, default=None):
    """JSON-load a DB column value safely."""
    if val is None:
        return default if default is not None else []
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return default if default is not None else []


def _jd(val) -> str:
    """JSON-dump a value for DB storage."""
    if val is None:
        return '[]'
    if isinstance(val, str):
        return val
    return json.dumps(val, default=str)


# ── CUSTOMERS ─────────────────────────────────────────────────────────────────

def get_all_customers(agency: str = None) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    if agency:
        rows = conn.execute(
            "SELECT * FROM customers WHERE agency=? ORDER BY display_name", (agency,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM customers ORDER BY agency, display_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_customer(c: dict) -> bool:
    """Insert or update a customer record."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO customers
              (qb_name, display_name, company, parent, agency, address, city, state, zip,
               bill_to, bill_to_city, bill_to_state, bill_to_zip, phone, email,
               open_balance, abbreviation, source, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(qb_name) DO UPDATE SET
              display_name=excluded.display_name, company=excluded.company,
              parent=excluded.parent, agency=excluded.agency, address=excluded.address,
              city=excluded.city, state=excluded.state, zip=excluded.zip,
              phone=excluded.phone, email=excluded.email,
              open_balance=excluded.open_balance, updated_at=excluded.updated_at
        """, (c.get('qb_name',''), c.get('display_name',''), c.get('company',''),
              c.get('parent',''), c.get('agency','DEFAULT'),
              c.get('address',''), c.get('city',''), c.get('state',''), c.get('zip',''),
              c.get('bill_to',''), c.get('bill_to_city',''), c.get('bill_to_state',''),
              c.get('bill_to_zip',''), c.get('phone',''), c.get('email',''),
              float(c.get('open_balance',0) or 0), c.get('abbreviation',''),
              c.get('source','quickbooks'), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_customer: %s", e)
        return False
    finally:
        conn.close()


def get_customer(qb_name: str) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM customers WHERE qb_name=?", (qb_name,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_customers_by_agency() -> dict:
    """Return {agency: [customers]} grouped dict."""
    rows = get_all_customers()
    out = {}
    for r in rows:
        ag = r.get('agency','DEFAULT')
        out.setdefault(ag, []).append(r)
    return out


# ── VENDORS ───────────────────────────────────────────────────────────────────

def get_all_vendors() -> list:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_vendor(v: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO vendors (name, company, address, city, state, zip, phone, email,
                                  open_balance, source, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
              company=excluded.company, phone=excluded.phone, email=excluded.email,
              open_balance=excluded.open_balance, updated_at=excluded.updated_at
        """, (v.get('name',''), v.get('company',''), v.get('address',''),
              v.get('city',''), v.get('state',''), v.get('zip',''),
              v.get('phone',''), v.get('email',''),
              float(v.get('open_balance',0) or 0), v.get('source','quickbooks'), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_vendor: %s", e)
        return False
    finally:
        conn.close()


# ── PRICE CHECKS ──────────────────────────────────────────────────────────────

def get_all_price_checks(include_test: bool = False) -> dict:
    """Return {pc_id: pc_dict} matching the old price_checks.json format."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    q = "SELECT * FROM price_checks ORDER BY created_at DESC"
    rows = conn.execute(q).fetchall()
    conn.close()
    result = {}
    for r in rows:
        d = dict(r)
        if not include_test and d.get('is_test'):
            continue
        pid = d.get('id') or d.get('pc_number') or str(d.get('rowid',''))
        d['items'] = _jl(d.get('items'), [])
        d['status_history'] = _jl(d.get('status_history'), [])
        d['ship_to'] = _jl(d.get('ship_to'), {})
        result[pid] = d
    return result


def upsert_price_check(pc_id: str, pc: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO price_checks
              (id, created_at, pc_number, requestor, agency, institution, due_date,
               ship_to, items, source_file, quote_number, total_items, status,
               status_history, parsed, reytech_quote_number, is_test, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              status=excluded.status, status_history=excluded.status_history,
              items=excluded.items, quote_number=excluded.quote_number,
              reytech_quote_number=excluded.reytech_quote_number,
              parsed=excluded.parsed, updated_at=excluded.updated_at
        """, (pc_id, pc.get('created_at', now), pc.get('pc_number',''),
              pc.get('requestor',''), pc.get('agency',''), pc.get('institution',''),
              pc.get('due_date',''), _jd(pc.get('ship_to',{})),
              _jd(pc.get('items',[])), pc.get('source_pdf', pc.get('source_file','')),
              pc.get('reytech_quote_number', pc.get('quote_number','')),
              len(pc.get('items',[])), pc.get('status','parsed'),
              _jd(pc.get('status_history',[])), 1 if pc.get('parsed') else 0,
              pc.get('reytech_quote_number',''), 1 if pc.get('is_test') else 0, now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_price_check %s: %s", pc_id, e)
        return False
    finally:
        conn.close()


def get_price_check(pc_id: str) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM price_checks WHERE id=?", (pc_id,)).fetchone()
    conn.close()
    if not row:
        return {}
    d = dict(row)
    d['items'] = _jl(d.get('items'), [])
    d['status_history'] = _jl(d.get('status_history'), [])
    d['ship_to'] = _jl(d.get('ship_to'), {})
    return d


# ── EMAIL OUTBOX ──────────────────────────────────────────────────────────────

def get_outbox(status: str = None, limit: int = 200) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    if status:
        rows = conn.execute(
            "SELECT * FROM email_outbox WHERE status=? ORDER BY created_at DESC LIMIT ?",
            (status, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM email_outbox ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['entities_resolved'] = _jl(d.pop('entities', None), {})
        d['to'] = d.pop('to_address', '')
        meta = _jl(d.pop('metadata', None), {})
        d.update({k: v for k, v in meta.items() if k not in d})
        result.append(d)
    return result


def upsert_outbox_email(em: dict) -> str:
    """Insert or update an outbox email. Returns the id."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    eid = em.get('id') or f"em-{__import__('uuid').uuid4().hex[:12]}"
    try:
        conn.execute("""
            INSERT INTO email_outbox
              (id, created_at, status, type, to_address, subject, body, intent,
               entities, approved_at, sent_at, metadata)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              status=excluded.status, body=excluded.body,
              approved_at=excluded.approved_at, sent_at=excluded.sent_at
        """, (eid, em.get('created_at', now), em.get('status','draft'),
              em.get('type',''), em.get('to', em.get('to_address','')),
              em.get('subject',''), em.get('body',''), em.get('intent',''),
              _jd(em.get('entities_resolved', em.get('entities',{}))),
              em.get('approved_at',''), em.get('sent_at',''),
              _jd({k: v for k, v in em.items()
                   if k not in ('id','created_at','status','type','to','to_address',
                                'subject','body','intent','entities_resolved',
                                'entities','approved_at','sent_at')})))
        conn.commit()
    except Exception as e:
        log.error("upsert_outbox_email: %s", e)
    finally:
        conn.close()
    return eid


def update_outbox_status(email_id: str, status: str, **kwargs):
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    updates = {'status': status}
    if status == 'approved':
        updates['approved_at'] = now
    elif status == 'sent':
        updates['sent_at'] = kwargs.get('sent_at', now)
    sets = ', '.join(f"{k}=?" for k in updates)
    conn.execute(f"UPDATE email_outbox SET {sets} WHERE id=?",
                 list(updates.values()) + [email_id])
    conn.commit()
    conn.close()


# ── QA REPORTS ────────────────────────────────────────────────────────────────

def save_qa_report(report: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = report.get('timestamp', datetime.now(timezone.utc).isoformat())
    try:
        conn.execute("""
            INSERT INTO qa_reports (timestamp, health_score, grade, summary, critical_count, checks)
            VALUES (?,?,?,?,?,?)
        """, (now, report.get('health_score',0), report.get('grade','?'),
              _jd(report.get('summary',{})), report.get('critical_count',0),
              _jd(report.get('checks',[]))))
        conn.commit()
        return True
    except Exception as e:
        log.error("save_qa_report: %s", e)
        return False
    finally:
        conn.close()


def get_qa_reports(limit: int = 50) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM qa_reports ORDER BY timestamp DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['summary'] = _jl(d.get('summary'), {})
        d['checks'] = _jl(d.get('checks'), [])
        result.append(d)
    return result


def get_latest_qa_report() -> dict:
    reports = get_qa_reports(limit=1)
    return reports[0] if reports else {}


# ── EMAIL TEMPLATES ───────────────────────────────────────────────────────────

def get_email_templates() -> dict:
    """Return {id: template_dict} matching old email_templates.json['templates']."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM email_templates").fetchall()
    conn.close()
    return {r['id']: dict(r) for r in rows}


def upsert_email_template(tid: str, tmpl: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO email_templates (id, name, subject, body, category, updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name, subject=excluded.subject,
              body=excluded.body, updated_at=excluded.updated_at
        """, (tid, tmpl.get('name', tid), tmpl.get('subject',''),
              tmpl.get('body', tmpl.get('template','')), tmpl.get('category',''), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_email_template: %s", e)
        return False
    finally:
        conn.close()


# ── VENDOR REGISTRATION ───────────────────────────────────────────────────────

def get_vendor_registrations() -> dict:
    """Return {vendor_key: data} matching old vendor_registration.json."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM vendor_registration").fetchall()
    conn.close()
    result = {}
    for r in rows:
        d = dict(r)
        meta = _jl(d.pop('metadata', None), {})
        d.update({k: v for k, v in meta.items() if k not in d})
        result[d['vendor_key']] = d
    return result


def upsert_vendor_registration(key: str, data: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO vendor_registration
              (vendor_key, vendor_name, status, account_number, rep_name,
               rep_email, rep_phone, notes, metadata, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(vendor_key) DO UPDATE SET
              status=excluded.status, account_number=excluded.account_number,
              rep_name=excluded.rep_name, rep_email=excluded.rep_email,
              notes=excluded.notes, updated_at=excluded.updated_at
        """, (key, data.get('name', key), data.get('status','pending'),
              data.get('account_number',''), data.get('rep_name',''),
              data.get('rep_email',''), data.get('rep_phone',''),
              data.get('notes',''), _jd(data), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_vendor_registration: %s", e)
        return False
    finally:
        conn.close()


# ── MARKET INTELLIGENCE ───────────────────────────────────────────────────────

def get_market_intelligence() -> dict:
    """Return the full market intelligence dict (all sections)."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM market_intelligence").fetchall()
    conn.close()
    return {r['section']: _jl(r['data']) for r in rows}


def upsert_market_intelligence(section: str, data) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO market_intelligence (section, data, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(section) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
        """, (section, _jd(data), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_market_intelligence: %s", e)
        return False
    finally:
        conn.close()


# ── INTEL AGENCIES ────────────────────────────────────────────────────────────

def get_intel_agencies() -> list:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM intel_agencies ORDER BY total_spend DESC").fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['buyers'] = _jl(d.get('buyers'), [])
        d['categories'] = _jl(d.get('categories'), [])
        result.append(d)
    return result


def upsert_intel_agency(ag: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO intel_agencies
              (dept_code, dept_name, total_spend, buyers, categories,
               is_customer, opportunity_score, updated_at)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(dept_code) DO UPDATE SET
              total_spend=excluded.total_spend, buyers=excluded.buyers,
              categories=excluded.categories, opportunity_score=excluded.opportunity_score,
              updated_at=excluded.updated_at
        """, (ag.get('dept_code',''), ag.get('dept_name', ag.get('agency','')),
              float(ag.get('total_spend',0) or 0),
              _jd(ag.get('buyers',[])), _jd(ag.get('categories',[])),
              1 if ag.get('is_customer') else 0,
              float(ag.get('opportunity_score',0) or 0), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_intel_agency: %s", e)
        return False
    finally:
        conn.close()


# ── GROWTH OUTREACH ───────────────────────────────────────────────────────────

def get_growth_outreach() -> dict:
    """Return {'campaigns': [...]} matching old growth_outreach.json format."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM growth_outreach ORDER BY created_at DESC").fetchall()
    conn.close()
    campaigns = []
    for r in rows:
        d = dict(r)
        d['context_summary'] = _jl(d.pop('context', None), {})
        meta = _jl(d.pop('metadata', None), {})
        d.update({k: v for k, v in meta.items() if k not in d})
        campaigns.append(d)
    return {'campaigns': campaigns, 'total_sent': sum(c.get('sent_count',0) for c in campaigns)}


def save_growth_campaign(camp: dict) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    now = datetime.now(timezone.utc).isoformat()
    cid = camp.get('id') or f"camp-{__import__('uuid').uuid4().hex[:8]}"
    try:
        conn.execute("""
            INSERT INTO growth_outreach
              (id, created_at, type, dry_run, template, context, status, sent_count, metadata)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              status=excluded.status, sent_count=excluded.sent_count,
              metadata=excluded.metadata
        """, (cid, camp.get('created_at', now), camp.get('type',''),
              1 if camp.get('dry_run') else 0, camp.get('template',''),
              _jd(camp.get('context_summary', camp.get('context',{}))),
              camp.get('status','draft'), len(camp.get('outreach',[])),
              _jd({k: v for k, v in camp.items()
                   if k not in ('id','created_at','type','dry_run','template',
                                'context_summary','context','status','outreach')})))
        conn.commit()
        return True
    except Exception as e:
        log.error("save_growth_campaign: %s", e)
        return False
    finally:
        conn.close()


# ── JSON COMPATIBILITY SHIMS ──────────────────────────────────────────────────
# These functions write-through to SQLite AND keep JSON in sync during transition.
# Once all consumers are migrated, the JSON writes can be removed.

def sync_customers_to_json():
    """Write customers table back to customers.json (for QB sync backwards compat)."""
    customers = get_all_customers()
    path = os.path.join(DATA_DIR, 'customers.json')
    try:
        with open(path, 'w') as f:
            json.dump(customers, f, indent=2, default=str)
    except Exception as e:
        log.warning("sync_customers_to_json: %s", e)


def sync_outbox_to_json():
    """Write email_outbox table back to email_outbox.json."""
    emails = get_outbox(limit=500)
    path = os.path.join(DATA_DIR, 'email_outbox.json')
    try:
        with open(path, 'w') as f:
            json.dump(emails, f, indent=2, default=str)
    except Exception as e:
        log.warning("sync_outbox_to_json: %s", e)

