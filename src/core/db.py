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
  (intel_pulls removed — migration 16)
"""

import os
import json
import re
import sqlite3
import logging

def _safe_identifier(name):
    """Validate SQL identifier (table/column name) to prevent injection."""
    clean = re.sub(r'[^a-zA-Z0-9_]', '', str(name))
    if not clean or clean != str(name).strip():
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return clean

import threading
from datetime import datetime, timezone
from contextlib import contextmanager

log = logging.getLogger("reytech.db")

# ─── Skip ledger ──────────────────────────────────────────────────────────────
# Per-row JSON column decoding (`items_detail`, `ship_to_address`, `metadata`,
# `tags`, `categories`, `purchase_orders`, …) previously fell back to `[]`/
# `{}`/`None` with `try: …; except: pass`, silently dropping the structured
# data of any row whose JSON had been corrupted (truncated by a crashed
# writer, double-encoded, etc.). Callers got an "empty-but-valid" row and
# downstream loss-analysis / restore flows then operated on missing items.
#
# The ledger lets routes/orchestrator drain skips after a row-loading call
# and surface the corruption count via the standard 3-channel envelope.
# Severity is INFO — one corrupt row is graceful degradation, not an outage.
from src.core.dependency_check import Severity, SkipReason  # noqa: E402

_SKIP_LEDGER: list[SkipReason] = []

# JSON columns whose decoded value should be a list — used to pick the right
# typed-empty fallback so callers iterating it never crash on a dict/None.
_LIST_TYPED_JSON_FIELDS = frozenset({
    "items_detail", "line_items", "tags", "items_purchased",
    "purchase_orders", "items",
})


def _record_skip(skip: SkipReason) -> None:
    """Append a skip to the module ledger; routes/orchestrator drain later."""
    _SKIP_LEDGER.append(skip)


def drain_skips() -> list[SkipReason]:
    """Pop and return every skip recorded since the last drain. Destructive
    so two consecutive calls do not double-warn."""
    drained = list(_SKIP_LEDGER)
    _SKIP_LEDGER.clear()
    return drained


def _decode_json_field(value, *, field: str, where: str):
    """Decode a JSON-typed SQLite column to its native Python type.

    - None / empty string → return as-is (newly-created rows; not corruption).
    - Already a list/dict → passthrough (joined queries pre-decode).
    - Valid JSON string → decoded value.
    - Malformed JSON → typed empty (`[]` for list-typed fields, `{}` else)
      plus an INFO skip recorded to the module ledger.

    The single seam means every row decoder (`_row_to_quote`, the batch
    loaders, the metadata loaders, the contact-list builder) emits the
    same corruption telemetry.
    """
    if value is None or value == "":
        return value
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        # Unexpected type — treat as corruption so callers don't crash.
        _record_skip(SkipReason(
            name=f"{field}_json",
            reason=f"unsupported raw type {type(value).__name__}",
            severity=Severity.INFO,
            where=where,
        ))
        return [] if field in _LIST_TYPED_JSON_FIELDS else {}
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        _record_skip(SkipReason(
            name=f"{field}_json",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.INFO,
            where=where,
        ))
        return [] if field in _LIST_TYPED_JSON_FIELDS else {}


# ── Path resolution ───────────────────────────────────────────────────────────
# Use the centralized DATA_DIR from paths.py (handles Railway volume detection)
from src.core.paths import DATA_DIR, _USING_VOLUME

def _is_railway_volume() -> bool:
    """True when running on Railway with a volume actually mounted."""
    return _USING_VOLUME

DB_PATH = os.path.join(DATA_DIR, "reytech.db")
os.makedirs(DATA_DIR, exist_ok=True)

_db_lock = threading.RLock()   # RLock: allows same-thread reentry (boot sync → upsert)

# ── Thread-local connection pool ─────────────────────────────────────────────
_local = threading.local()


def _make_connection():
    """Create and configure a new SQLite connection."""
    conn = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def close_thread_db():
    """Close the thread-local DB connection. Call on thread exit."""
    conn = getattr(_local, 'conn', None)
    if conn:
        try:
            conn.close()
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        _local.conn = None


# ── Connection factory ────────────────────────────────────────────────────────
@contextmanager
def get_db():
    """Get a thread-local SQLite connection. WAL mode allows concurrent reads.

    Reuses a per-thread connection instead of creating a new one each call.
    This removes the global RLock serialization — threads read concurrently.
    Writes are still serialized by SQLite's internal WAL locking.
    """
    conn = getattr(_local, 'conn', None)
    if conn is None:
        conn = _make_connection()
        _local.conn = conn
    # Defensive reset: a previous caller on this thread may have set
    # `conn.row_factory = None` (or to something custom). Because the
    # connection is shared per-thread, that mutation persists and the next
    # caller's `dict(row)` blows up with "dictionary update sequence element
    # #0 has length N; 2 is required". Incident 2026-04-19: this took down
    # the 1-click banner smoke check for two PRs (#213, #215, both wrong-
    # theory fixes). Restore Row before every yield so each caller gets the
    # documented contract regardless of what siblings did.
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            # Connection is broken — discard it so next call gets a fresh one
            close_thread_db()
        raise


def db_retry(fn, max_retries=3, delay=1.0):
    """Retry a DB operation that may hit 'database is locked'.
    fn should be a callable that performs the DB work (using get_db() inside).
    Fires Slack alert if all retries exhausted."""
    import time as _time
    last_err = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                last_err = e
                log.warning("DB locked (attempt %d/%d): %s", attempt + 1, max_retries, e)
                _time.sleep(delay * (attempt + 1))
            else:
                # Final failure — alert
                if "database is locked" in str(e):
                    try:
                        from src.core.webhooks import fire_event
                        fire_event("db_lock_timeout", {
                            "function": getattr(fn, "__name__", "unknown"),
                            "attempts": max_retries,
                            "error": str(e)[:200],
                        })
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
                raise
    raise last_err

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_number    TEXT UNIQUE NOT NULL,
    created_at      TEXT NOT NULL,
    agency          TEXT,
    institution     TEXT,
    requestor       TEXT,
    contact_name    TEXT,
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
    line_items      TEXT,           -- JSON array (alias for items_detail, used by upsert)
    status          TEXT DEFAULT 'pending',
    pdf_path        TEXT,
    source_pc_id    TEXT,
    source_rfq_id   TEXT,
    source          TEXT DEFAULT '',
    sent_at         TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    status_history  TEXT DEFAULT '[]',
    po_number       TEXT,
    status_notes    TEXT,
    is_test         INTEGER DEFAULT 0,
    total_cost      REAL DEFAULT 0,
    gross_profit    REAL DEFAULT 0,
    margin_pct      REAL DEFAULT 0,
    items_costed    INTEGER DEFAULT 0,
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
    updated_at      TEXT,
    is_test         INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    TEXT NOT NULL,
    action      TEXT NOT NULL,
    field       TEXT,
    old_value   TEXT,
    new_value   TEXT,
    actor       TEXT DEFAULT 'system',
    details     TEXT,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oal_order ON order_audit_log(order_id);
CREATE INDEX IF NOT EXISTS idx_oal_action ON order_audit_log(action);

CREATE TABLE IF NOT EXISTS order_attachments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    TEXT NOT NULL,
    line_id     TEXT,
    file_type   TEXT NOT NULL,
    file_name   TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    uploaded_by TEXT DEFAULT 'user',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oa_order ON order_attachments(order_id);

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
    updated_at      TEXT,
    data_json       TEXT
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
    date            TEXT,
    is_test         INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS quote_number_ledger (
    quote_number    TEXT PRIMARY KEY,
    assigned_at     TEXT NOT NULL DEFAULT (datetime('now')),
    source_type     TEXT DEFAULT '',
    source_id       TEXT DEFAULT '',
    status          TEXT DEFAULT 'active',
    voided_at       TEXT DEFAULT '',
    void_reason     TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS price_checks (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    requestor       TEXT,
    agency          TEXT,
    institution     TEXT,
    items           TEXT,           -- JSON with full pricing per item
    source_file     TEXT,
    quote_number    TEXT,
    pc_number       TEXT,
    total_items     INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'parsed',
    email_uid       TEXT,
    email_subject   TEXT,
    due_date        TEXT,
    pc_data         TEXT DEFAULT '{}',
    ship_to         TEXT DEFAULT '',
    data_json       TEXT
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

CREATE TABLE IF NOT EXISTS email_outbox (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    status          TEXT DEFAULT 'draft',
    type            TEXT DEFAULT '',
    to_address      TEXT,
    subject         TEXT,
    body            TEXT,
    intent          TEXT DEFAULT '',
    entities        TEXT DEFAULT '{}',
    approved_at     TEXT DEFAULT '',
    sent_at         TEXT DEFAULT '',
    metadata        TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_email_outbox_status ON email_outbox(status);
CREATE INDEX IF NOT EXISTS idx_email_outbox_created ON email_outbox(created_at);

CREATE TABLE IF NOT EXISTS growth_outreach (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    type            TEXT DEFAULT '',
    dry_run         INTEGER DEFAULT 0,
    template        TEXT DEFAULT '',
    context         TEXT DEFAULT '{}',
    status          TEXT DEFAULT 'draft',
    sent_count      INTEGER DEFAULT 0,
    metadata        TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_growth_outreach_status ON growth_outreach(status);
CREATE INDEX IF NOT EXISTS idx_growth_outreach_created ON growth_outreach(created_at);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at        TEXT,
    finished_at       TEXT,
    type              TEXT DEFAULT 'workflow',
    status            TEXT DEFAULT 'completed',
    run_at            TEXT,
    score             INTEGER,
    grade             TEXT,
    passed            INTEGER,
    failed            INTEGER,
    warned            INTEGER,
    critical_failures TEXT,
    full_report       TEXT
);

CREATE TABLE IF NOT EXISTS competitor_intel (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    found_at          TEXT NOT NULL,
    pc_id             TEXT,
    quote_number      TEXT,
    our_price         REAL DEFAULT 0,
    competitor_name   TEXT NOT NULL,
    competitor_price  REAL DEFAULT 0,
    price_delta       REAL DEFAULT 0,
    price_delta_pct   REAL DEFAULT 0,
    po_number         TEXT,
    agency            TEXT,
    institution       TEXT,
    item_summary      TEXT,
    items_detail      TEXT,
    solicitation      TEXT,
    outcome           TEXT DEFAULT 'lost',
    notes             TEXT,
    loss_reason_class TEXT DEFAULT '',
    our_cost          REAL DEFAULT 0,
    our_margin_pct    REAL DEFAULT 0,
    margin_too_high   INTEGER DEFAULT 0,
    category          TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_competitor_name ON competitor_intel(competitor_name);
CREATE INDEX IF NOT EXISTS idx_competitor_agency ON competitor_intel(agency);
CREATE INDEX IF NOT EXISTS idx_competitor_pc ON competitor_intel(pc_id);
CREATE INDEX IF NOT EXISTS idx_ci_loss_class ON competitor_intel(loss_reason_class);
CREATE INDEX IF NOT EXISTS idx_ci_margin ON competitor_intel(margin_too_high);

-- Award intelligence: loss pattern tracking
CREATE TABLE IF NOT EXISTS loss_patterns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at     TEXT NOT NULL,
    pattern_type    TEXT NOT NULL,
    category        TEXT DEFAULT '',
    agency          TEXT DEFAULT '',
    competitor      TEXT DEFAULT '',
    description     TEXT NOT NULL,
    severity        TEXT DEFAULT 'info',
    recommendation  TEXT DEFAULT '',
    data_json       TEXT DEFAULT '{}',
    acknowledged    INTEGER DEFAULT 0,
    acknowledged_at TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_lp_type ON loss_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_lp_severity ON loss_patterns(severity);
CREATE INDEX IF NOT EXISTS idx_lp_unack ON loss_patterns(acknowledged);

-- Action items from competitive loss analysis
CREATE TABLE IF NOT EXISTS action_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    source_quote TEXT,
    action_type TEXT,
    description TEXT NOT NULL,
    priority TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'pending',
    completed_at TEXT,
    notes TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_ai_status ON action_items(status);
CREATE INDEX IF NOT EXISTS idx_ai_type ON action_items(action_type);

-- Pricing recommendation audit — tracks oracle accuracy
CREATE TABLE IF NOT EXISTS recommendation_audit (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at     TEXT NOT NULL,
    pc_id           TEXT,
    quote_number    TEXT,
    item_index      INTEGER,
    description     TEXT,
    item_number     TEXT,
    oracle_price    REAL,
    oracle_source   TEXT,
    oracle_confidence TEXT,
    user_price      REAL,
    delta_pct       REAL,
    followed        INTEGER DEFAULT 0,
    outcome         TEXT DEFAULT 'pending',
    outcome_price   REAL,
    updated_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_ra_pc ON recommendation_audit(pc_id);
CREATE INDEX IF NOT EXISTS idx_ra_outcome ON recommendation_audit(outcome);

-- PRD-28 WI-1: Quote revision history
CREATE TABLE IF NOT EXISTS quote_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_number    TEXT NOT NULL,
    revision_num    INTEGER NOT NULL,
    revised_at      TEXT NOT NULL,
    reason          TEXT,
    snapshot_json   TEXT,
    changed_by      TEXT DEFAULT 'user'
);
CREATE INDEX IF NOT EXISTS idx_qrev_qn ON quote_revisions(quote_number);

-- PRD-28 WI-2: Email engagement tracking
CREATE TABLE IF NOT EXISTS email_engagement (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id        TEXT NOT NULL,
    event_type      TEXT NOT NULL,  -- open|click
    event_at        TEXT NOT NULL,
    ip_address      TEXT,
    user_agent      TEXT,
    link_url        TEXT
);
CREATE INDEX IF NOT EXISTS idx_engage_email ON email_engagement(email_id);

-- PRD-28 WI-3: Lead nurture sequences
CREATE TABLE IF NOT EXISTS lead_nurture (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         TEXT NOT NULL,
    step_num        INTEGER NOT NULL,
    scheduled_at    TEXT NOT NULL,
    sent_at         TEXT,
    status          TEXT DEFAULT 'pending',  -- pending|sent|paused|skipped
    email_id        TEXT,
    template_key    TEXT
);
CREATE INDEX IF NOT EXISTS idx_nurture_lead ON lead_nurture(lead_id);
CREATE INDEX IF NOT EXISTS idx_nurture_sched ON lead_nurture(status, scheduled_at);

-- PRD-28 WI-5: Vendor scores
CREATE TABLE IF NOT EXISTS vendor_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_name     TEXT NOT NULL,
    scored_at       TEXT NOT NULL,
    price_score     REAL DEFAULT 0,
    reliability_score REAL DEFAULT 0,
    speed_score     REAL DEFAULT 0,
    breadth_score   REAL DEFAULT 0,
    overall_score   REAL DEFAULT 0,
    categories      TEXT,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_vscore_name ON vendor_scores(vendor_name);

-- ═══ JSON→DB Migration: leads, customers, vendors ═══

CREATE TABLE IF NOT EXISTS leads (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    updated_at      TEXT,
    status          TEXT DEFAULT 'new',
    agency          TEXT,
    institution     TEXT,
    buyer_name      TEXT,
    buyer_email     TEXT,
    buyer_phone     TEXT,
    category        TEXT,
    score           REAL DEFAULT 0,
    score_breakdown TEXT,
    score_history   TEXT,
    score_updated_at TEXT,
    po_number       TEXT,
    po_value        REAL DEFAULT 0,
    po_date         TEXT,
    due_date        TEXT,
    items_count     INTEGER DEFAULT 0,
    match_type      TEXT,
    matched_items   TEXT,
    our_historical_price REAL DEFAULT 0,
    scprs_listed_price REAL DEFAULT 0,
    estimated_savings_pct REAL DEFAULT 0,
    outreach_draft  TEXT,
    outreach_sent_at TEXT,
    response_received_at TEXT,
    notes           TEXT,
    source          TEXT DEFAULT 'scprs',
    nurture_active  INTEGER DEFAULT 0,
    nurture_sequence TEXT,
    nurture_steps   TEXT,
    nurture_started_at TEXT,
    nurture_paused_at TEXT,
    nurture_pause_reason TEXT,
    converted_at    TEXT,
    converted_contact_id TEXT,
    extra_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_agency ON leads(agency);
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(buyer_email);
CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(score);

CREATE TABLE IF NOT EXISTS customers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    qb_name         TEXT,
    display_name    TEXT,
    company         TEXT,
    parent          TEXT,
    agency          TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    phone           TEXT,
    email           TEXT,
    open_balance    REAL DEFAULT 0,
    source          TEXT DEFAULT 'quickbooks',
    is_parent_org   INTEGER DEFAULT 0,
    bill_to         TEXT,
    bill_to_city    TEXT,
    bill_to_state   TEXT,
    bill_to_zip     TEXT,
    abbreviation    TEXT,
    child_count     INTEGER DEFAULT 0,
    created_at      TEXT,
    updated_at      TEXT,
    extra_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_cust_name ON customers(display_name);
CREATE INDEX IF NOT EXISTS idx_cust_agency ON customers(agency);
CREATE INDEX IF NOT EXISTS idx_cust_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_cust_qb ON customers(qb_name);

CREATE TABLE IF NOT EXISTS vendors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    company         TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    phone           TEXT,
    email           TEXT,
    website         TEXT,
    source          TEXT DEFAULT 'quickbooks',
    open_balance    REAL DEFAULT 0,
    price_score     REAL DEFAULT 0,
    reliability_score REAL DEFAULT 0,
    speed_score     REAL DEFAULT 0,
    breadth_score   REAL DEFAULT 0,
    overall_score   REAL DEFAULT 0,
    scored_at       TEXT,
    categories_served TEXT,
    gsa_contract    TEXT,
    notes           TEXT,
    created_at      TEXT,
    updated_at      TEXT,
    extra_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_vendor_name ON vendors(name);
CREATE INDEX IF NOT EXISTS idx_vendor_email ON vendors(email);
CREATE INDEX IF NOT EXISTS idx_vendor_score ON vendors(overall_score);

-- Sent document versions: tracks every PDF revision for a price check
CREATE TABLE IF NOT EXISTS sent_documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pc_id           TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    filename        TEXT NOT NULL,
    filepath        TEXT NOT NULL,
    file_size       INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'current',   -- current, superseded, draft
    notes           TEXT,
    created_by      TEXT DEFAULT 'user',
    items_json      TEXT,                     -- snapshot of line items at this version
    header_json     TEXT,                     -- snapshot of header fields
    change_summary  TEXT                      -- what changed from previous version
);
CREATE INDEX IF NOT EXISTS idx_sentdoc_pcid ON sent_documents(pc_id, version);

-- ── Tables added by ghost-data audit (were used in db.py but never created) ──

CREATE TABLE IF NOT EXISTS qa_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL,
    health_score    REAL DEFAULT 0,
    grade           TEXT DEFAULT '?',
    summary         TEXT,
    critical_count  INTEGER DEFAULT 0,
    checks          TEXT
);

CREATE TABLE IF NOT EXISTS vendor_registration (
    vendor_key      TEXT PRIMARY KEY,
    vendor_name     TEXT,
    status          TEXT DEFAULT 'pending',
    account_number  TEXT DEFAULT '',
    rep_name        TEXT DEFAULT '',
    rep_email       TEXT DEFAULT '',
    rep_phone       TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    metadata        TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS market_intelligence (
    section         TEXT PRIMARY KEY,
    data            TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS intel_agencies (
    dept_code       TEXT PRIMARY KEY,
    dept_name       TEXT,
    total_spend     REAL DEFAULT 0,
    buyers          TEXT,
    categories      TEXT,
    is_customer     INTEGER DEFAULT 0,
    opportunity_score REAL DEFAULT 0,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS email_sent_log (
    id              TEXT PRIMARY KEY,
    sent_at         TEXT NOT NULL,
    to_address      TEXT DEFAULT '',
    subject         TEXT DEFAULT '',
    body            TEXT DEFAULT '',
    type            TEXT DEFAULT '',
    ref_id          TEXT DEFAULT '',
    success         INTEGER DEFAULT 1,
    error           TEXT DEFAULT ''
);

-- ── SCPRS Intelligence Tables (core to CRM, catalog, growth) ──

CREATE TABLE IF NOT EXISTS scprs_po_master (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pulled_at       TEXT,
    po_number       TEXT UNIQUE,
    dept_code       TEXT,
    dept_name       TEXT,
    institution     TEXT,
    supplier        TEXT,
    supplier_id     TEXT,
    status          TEXT,
    start_date      TEXT,
    end_date        TEXT,
    acq_type        TEXT,
    acq_method      TEXT,
    merch_amount    REAL,
    grand_total     REAL,
    buyer_name      TEXT,
    buyer_email     TEXT,
    buyer_phone     TEXT,
    search_term     TEXT,
    agency_key      TEXT
);
CREATE INDEX IF NOT EXISTS idx_po_institution ON scprs_po_master(institution);
CREATE INDEX IF NOT EXISTS idx_po_buyer ON scprs_po_master(buyer_email);
CREATE INDEX IF NOT EXISTS idx_po_supplier ON scprs_po_master(supplier);

CREATE TABLE IF NOT EXISTS scprs_po_lines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id           INTEGER REFERENCES scprs_po_master(id),
    po_number       TEXT,
    line_num        INTEGER,
    item_id         TEXT,
    description     TEXT,
    unspsc          TEXT,
    uom             TEXT,
    quantity        REAL,
    unit_price      REAL,
    line_total      REAL,
    line_status     TEXT,
    category        TEXT,
    reytech_sells   INTEGER DEFAULT 0,
    reytech_sku     TEXT,
    opportunity_flag TEXT
);

CREATE TABLE IF NOT EXISTS scprs_pull_schedule (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    agency_key      TEXT UNIQUE,
    priority        TEXT,
    pull_interval_hours INTEGER DEFAULT 24,
    last_pull       TEXT,
    next_pull       TEXT,
    enabled         INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS scprs_pull_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pulled_at       TEXT,
    search_term     TEXT,
    dept_filter     TEXT,
    results_found   INTEGER DEFAULT 0,
    lines_parsed    INTEGER DEFAULT 0,
    new_pos         INTEGER DEFAULT 0,
    error           TEXT,
    duration_sec    REAL
);

CREATE TABLE IF NOT EXISTS won_quotes (
    id              TEXT PRIMARY KEY,
    po_number       TEXT,
    item_number     TEXT,
    description     TEXT,
    normalized_description TEXT,
    tokens          TEXT,
    category        TEXT,
    supplier        TEXT,
    department      TEXT,
    unit_price      REAL,
    quantity        REAL,
    total           REAL,
    award_date      TEXT,
    source          TEXT,
    confidence      REAL DEFAULT 1.0,
    ingested_at     TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS buyer_intelligence (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_email     TEXT,
    buyer_name      TEXT,
    agency          TEXT,
    institution     TEXT,
    total_spend     REAL DEFAULT 0,
    po_count        INTEGER DEFAULT 0,
    categories      TEXT,
    items_purchased TEXT,
    last_po_date    TEXT,
    opportunity_score REAL DEFAULT 0,
    is_reytech_customer INTEGER DEFAULT 0,
    source          TEXT DEFAULT 'scprs',
    created_at      TEXT,
    updated_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_bi_email ON buyer_intelligence(buyer_email);
CREATE INDEX IF NOT EXISTS idx_bi_agency ON buyer_intelligence(agency);

CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    updated_at  TEXT,
    updated_by  TEXT DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS audit_trail (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    item_description TEXT,
    field_changed   TEXT,
    old_value       TEXT,
    new_value       TEXT,
    source          TEXT DEFAULT 'manual',
    rfq_id          TEXT,
    part_number     TEXT,
    actor           TEXT DEFAULT 'system',
    notes           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_audit_desc ON audit_trail(item_description);

CREATE TABLE IF NOT EXISTS api_keys (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash    TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    last_used   TEXT,
    is_active   INTEGER DEFAULT 1,
    created_by  TEXT DEFAULT 'system',
    scopes      TEXT DEFAULT '["read","write"]'
);

CREATE TABLE IF NOT EXISTS scprs_catalog (
    description     TEXT PRIMARY KEY,
    unspsc          TEXT DEFAULT '',
    last_unit_price REAL,
    last_quantity   REAL,
    last_uom        TEXT DEFAULT '',
    last_supplier   TEXT DEFAULT '',
    last_department TEXT DEFAULT '',
    last_po_number  TEXT DEFAULT '',
    last_date       TEXT DEFAULT '',
    times_seen      INTEGER DEFAULT 1,
    product_image_path TEXT DEFAULT '',
    product_image_url  TEXT DEFAULT '',
    updated_at      TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS scprs_buyers (
    buyer_email         TEXT PRIMARY KEY,
    buyer_name          TEXT DEFAULT '',
    department          TEXT DEFAULT '',
    dept_code           TEXT DEFAULT '',
    total_pos           INTEGER DEFAULT 0,
    total_spend         REAL DEFAULT 0,
    total_line_items    INTEGER DEFAULT 0,
    first_po_date       TEXT DEFAULT '',
    last_po_date        TEXT DEFAULT '',
    top_categories      TEXT DEFAULT '',
    buys_from_reytech   INTEGER DEFAULT 0,
    reytech_spend       REAL DEFAULT 0,
    reytech_last_date   TEXT DEFAULT '',
    relationship_status TEXT DEFAULT 'unknown',
    prospect_score      REAL DEFAULT 0,
    outreach_status     TEXT DEFAULT 'none',
    outreach_last_date  TEXT DEFAULT '',
    outreach_response   TEXT DEFAULT '',
    notes               TEXT DEFAULT '',
    updated_at          TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS scprs_buyer_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_email     TEXT NOT NULL,
    po_number       TEXT DEFAULT '',
    description     TEXT DEFAULT '',
    unit_price      TEXT DEFAULT '',
    quantity        TEXT DEFAULT '',
    supplier        TEXT DEFAULT '',
    date            TEXT DEFAULT '',
    UNIQUE(buyer_email, po_number, description)
);

CREATE TABLE IF NOT EXISTS item_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_description TEXT NOT NULL,
    original_item_number TEXT DEFAULT '',
    canonical_description TEXT DEFAULT '',
    canonical_item_number TEXT DEFAULT '',
    mfg_number TEXT DEFAULT '',
    mfg_name TEXT DEFAULT '',
    upc TEXT DEFAULT '',
    asin TEXT DEFAULT '',
    product_url TEXT DEFAULT '',
    supplier TEXT DEFAULT '',
    last_cost REAL DEFAULT 0,
    confidence REAL DEFAULT 0.5,
    confirmed INTEGER DEFAULT 0,
    times_confirmed INTEGER DEFAULT 0,
    first_seen TEXT DEFAULT (datetime('now')),
    last_confirmed TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(original_description, original_item_number)
);
CREATE INDEX IF NOT EXISTS idx_item_mappings_desc ON item_mappings(original_description);

CREATE TABLE IF NOT EXISTS supplier_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    description TEXT NOT NULL,
    item_number TEXT DEFAULT '',
    cost REAL NOT NULL,
    supplier TEXT DEFAULT '',
    source TEXT DEFAULT '',
    source_url TEXT DEFAULT '',
    confirmed_at TEXT DEFAULT (datetime('now')),
    expires_at TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    UNIQUE(description, supplier)
);

CREATE TABLE IF NOT EXISTS match_feedback (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    pc_id            TEXT NOT NULL,
    item_index       INTEGER NOT NULL,
    item_description TEXT NOT NULL,
    match_source     TEXT NOT NULL,
    match_id         TEXT DEFAULT '',
    match_description TEXT DEFAULT '',
    match_confidence REAL DEFAULT 0,
    feedback_type    TEXT NOT NULL,
    user_price       REAL DEFAULT 0,
    match_price      REAL DEFAULT 0,
    reason           TEXT DEFAULT '',
    normalized_query TEXT DEFAULT '',
    normalized_match TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_mf_query ON match_feedback(normalized_query);
CREATE INDEX IF NOT EXISTS idx_mf_match ON match_feedback(normalized_match);
CREATE INDEX IF NOT EXISTS idx_mf_source ON match_feedback(match_source);

CREATE TABLE IF NOT EXISTS parse_gaps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rfq_id          TEXT,
    field_name      TEXT,
    was_empty       INTEGER DEFAULT 1,
    user_filled_value TEXT DEFAULT '',
    source_type     TEXT DEFAULT '',
    email_subject   TEXT DEFAULT '',
    requestor_email TEXT DEFAULT '',
    agency          TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ═══════════════════════════════════════════════════════════════════
-- PACKAGE AUDIT TRAIL (added 2026-03-18)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS package_manifest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rfq_id          TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    created_by      TEXT DEFAULT 'system',
    agency_key      TEXT,
    agency_name     TEXT,
    required_forms  TEXT NOT NULL DEFAULT '[]',
    generated_forms TEXT NOT NULL DEFAULT '[]',
    missing_forms   TEXT DEFAULT '[]',
    source_validation TEXT,
    field_audit     TEXT,
    overall_status  TEXT DEFAULT 'draft',
    package_filename TEXT,
    package_size    INTEGER,
    total_forms     INTEGER,
    total_pages     INTEGER,
    quote_number    TEXT,
    quote_total     REAL,
    item_count      INTEGER,
    items_snapshot  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pm_rfq ON package_manifest(rfq_id);
CREATE INDEX IF NOT EXISTS idx_pm_status ON package_manifest(overall_status);

CREATE TABLE IF NOT EXISTS package_review (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id     INTEGER NOT NULL,
    form_id         TEXT NOT NULL,
    form_filename   TEXT,
    reviewed_at     TEXT,
    reviewed_by     TEXT DEFAULT 'user',
    verdict         TEXT DEFAULT 'pending',
    notes           TEXT,
    field_warnings  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pr_manifest ON package_review(manifest_id);

CREATE TABLE IF NOT EXISTS package_delivery (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id     INTEGER NOT NULL,
    rfq_id          TEXT NOT NULL,
    delivered_at    TEXT NOT NULL,
    delivery_method TEXT DEFAULT 'email',
    recipient_email TEXT,
    recipient_name  TEXT,
    email_subject   TEXT,
    email_log_id    INTEGER,
    package_hash    TEXT
);
CREATE INDEX IF NOT EXISTS idx_pd_rfq ON package_delivery(rfq_id);
CREATE INDEX IF NOT EXISTS idx_pd_manifest ON package_delivery(manifest_id);

CREATE TABLE IF NOT EXISTS buyer_preferences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_email     TEXT NOT NULL,
    buyer_name      TEXT,
    agency_key      TEXT,
    preference_key  TEXT NOT NULL,
    preference_value TEXT NOT NULL,
    source          TEXT,
    learned_at      TEXT NOT NULL,
    notes           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bp_email_key ON buyer_preferences(buyer_email, preference_key);

CREATE TABLE IF NOT EXISTS form_templates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    form_id         TEXT NOT NULL UNIQUE,
    form_name       TEXT NOT NULL,
    template_path   TEXT NOT NULL,
    revision_date   TEXT,
    field_count     INTEGER,
    field_names     TEXT,
    vendor_fields   TEXT,
    buyer_fields    TEXT,
    last_verified   TEXT,
    sha256          TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS lifecycle_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    occurred_at     TEXT NOT NULL,
    actor           TEXT DEFAULT 'system',
    summary         TEXT,
    detail_json     TEXT,
    metadata        TEXT
);
CREATE INDEX IF NOT EXISTS idx_le_entity ON lifecycle_events(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_le_type ON lifecycle_events(event_type);
CREATE INDEX IF NOT EXISTS idx_le_time ON lifecycle_events(occurred_at);

-- ═══════════════════════════════════════════════════════════════════════
-- Orders V2: Normalized line items (exploded from orders.data_json blob)
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS order_line_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT NOT NULL,
    line_number     INTEGER NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    part_number     TEXT DEFAULT '',
    mfg_number      TEXT DEFAULT '',
    asin            TEXT DEFAULT '',
    uom             TEXT DEFAULT 'EA',
    qty_ordered     INTEGER DEFAULT 0,
    qty_backordered INTEGER DEFAULT 0,
    unit_price      REAL DEFAULT 0,
    unit_cost       REAL DEFAULT 0,
    extended_price  REAL DEFAULT 0,
    extended_cost   REAL DEFAULT 0,
    sourcing_status TEXT DEFAULT 'pending',
    supplier_name   TEXT DEFAULT '',
    supplier_url    TEXT DEFAULT '',
    vendor_order_id INTEGER,
    vendor_order_ref TEXT DEFAULT '',
    tracking_number TEXT DEFAULT '',
    carrier         TEXT DEFAULT '',
    ship_date       TEXT DEFAULT '',
    expected_delivery TEXT DEFAULT '',
    delivery_date   TEXT DEFAULT '',
    fulfillment_type TEXT DEFAULT 'dropship',
    invoice_status  TEXT DEFAULT 'pending',
    invoice_number  TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (vendor_order_id) REFERENCES vendor_orders(id)
);
CREATE INDEX IF NOT EXISTS idx_oli_order ON order_line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_oli_vendor ON order_line_items(vendor_order_id);
CREATE INDEX IF NOT EXISTS idx_oli_status ON order_line_items(sourcing_status);

-- Orders V2: Delivery confirmation log (dropship model — no warehouse)
CREATE TABLE IF NOT EXISTS delivery_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT NOT NULL,
    line_item_id    INTEGER NOT NULL,
    confirmed_at    TEXT NOT NULL,
    delivery_date   TEXT NOT NULL,
    confirmation_source TEXT DEFAULT 'manual',
    tracking_number TEXT DEFAULT '',
    carrier         TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    confirmed_by    TEXT DEFAULT 'user',
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (line_item_id) REFERENCES order_line_items(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_dl_order ON delivery_log(order_id);
CREATE INDEX IF NOT EXISTS idx_dl_line ON delivery_log(line_item_id);

-- Orders V2: Migration tracking
CREATE TABLE IF NOT EXISTS migrations_applied (
    name            TEXT PRIMARY KEY,
    applied_at      TEXT NOT NULL
);

-- ═══════════════════════════════════════════════════════════════════
-- SUPPLIER PROFILES (landed cost: tax exemption + shipping estimates)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS supplier_profiles (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name           TEXT NOT NULL UNIQUE,
    tax_exempt_status       TEXT NOT NULL DEFAULT 'unknown',
    free_shipping_threshold REAL DEFAULT 0,
    default_shipping_pct    REAL DEFAULT 0,
    drop_ship               INTEGER DEFAULT 0,
    notes                   TEXT DEFAULT '',
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT DEFAULT (datetime('now'))
);

-- PRICING ORACLE V2 TABLES (moved from runtime creation to schema)

CREATE TABLE IF NOT EXISTS oracle_calibration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    agency TEXT DEFAULT '',
    sample_size INTEGER DEFAULT 0,
    win_count INTEGER DEFAULT 0,
    loss_on_price INTEGER DEFAULT 0,
    loss_on_other INTEGER DEFAULT 0,
    avg_winning_margin REAL DEFAULT 25,
    avg_losing_delta REAL DEFAULT 0,
    recommended_max_markup REAL DEFAULT 30,
    competitor_floor REAL DEFAULT 0,
    last_updated TEXT,
    UNIQUE(category, agency)
);

CREATE TABLE IF NOT EXISTS institution_pricing_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    institution TEXT NOT NULL,
    category TEXT DEFAULT 'general',
    avg_winning_markup REAL DEFAULT 25,
    avg_losing_markup REAL DEFAULT 0,
    win_count INTEGER DEFAULT 0,
    loss_count INTEGER DEFAULT 0,
    price_sensitivity TEXT DEFAULT 'normal',
    preferred_suppliers TEXT DEFAULT '',
    last_updated TEXT,
    UNIQUE(institution, category)
);

CREATE TABLE IF NOT EXISTS winning_quote_shapes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    institution TEXT DEFAULT '',
    agency TEXT DEFAULT '',
    category_mix TEXT,
    total_items INTEGER,
    avg_markup REAL,
    markup_stddev REAL,
    markup_distribution TEXT,
    outcome TEXT,
    recorded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_wqs_institution ON winning_quote_shapes(institution);
CREATE INDEX IF NOT EXISTS idx_wqs_outcome ON winning_quote_shapes(outcome);
-- idx_wqs_agency is created by _migrate_columns AFTER the agency column is
-- back-filled via ALTER TABLE — creating it here would fail on prod DBs
-- that predate the column.

CREATE TABLE IF NOT EXISTS winning_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    quote_number TEXT,
    po_number TEXT,
    order_id TEXT,
    agency TEXT,
    institution TEXT,
    description TEXT NOT NULL,
    part_number TEXT,
    sku TEXT,
    qty REAL DEFAULT 1,
    sell_price REAL NOT NULL,
    cost REAL DEFAULT 0,
    margin_pct REAL DEFAULT 0,
    supplier TEXT,
    category TEXT,
    catalog_product_id INTEGER,
    fingerprint TEXT
);
CREATE INDEX IF NOT EXISTS idx_wp_fingerprint ON winning_prices(fingerprint);
CREATE INDEX IF NOT EXISTS idx_wp_part ON winning_prices(part_number);
CREATE INDEX IF NOT EXISTS idx_wp_institution ON winning_prices(institution);
CREATE INDEX IF NOT EXISTS idx_wp_recorded ON winning_prices(recorded_at);

CREATE TABLE IF NOT EXISTS template_strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    strategy TEXT NOT NULL,
    score INTEGER NOT NULL,
    source_type TEXT DEFAULT '',
    pc_id TEXT DEFAULT '',
    buyer_agency TEXT DEFAULT '',
    event_type TEXT DEFAULT 'generation',
    detail TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ts_fingerprint ON template_strategies(fingerprint);
CREATE INDEX IF NOT EXISTS idx_ts_strategy ON template_strategies(strategy);

-- ═════════════════════════════════════════════════════════════════════
-- Phase 2 Tier-2 lookup denormalization (2026-04-25)
-- Every operator-saved cost gets one row here, keyed on (mfg_number, upc).
-- Phase 2's `find_recent_quote_cost` reads this for "the last time we
-- accepted a real cost for this exact item" — Tier 2 of the cost cascade.
-- We do NOT read quotes.items_detail JSON because:
--   (a) JSON column requires per-row decode, O(quotes) per lookup
--   (b) Pre-2026-04-25 rows contain Amazon-poisoned costs from the bug
--       PR #524 fixed — provenance filter would skip them anyway
-- This table only ever receives operator-confirmed costs (cost_source
-- column), so it cannot resurrect Amazon ghosts.
-- ═════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS quote_line_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mfg_number TEXT,
    upc TEXT,
    description TEXT,
    cost REAL NOT NULL,
    cost_source TEXT NOT NULL,
    cost_source_url TEXT DEFAULT '',
    quote_number TEXT DEFAULT '',
    pc_id TEXT DEFAULT '',
    rfq_id TEXT DEFAULT '',
    supplier_name TEXT DEFAULT '',
    accepted_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_qlc_mfg ON quote_line_costs(mfg_number);
CREATE INDEX IF NOT EXISTS idx_qlc_upc ON quote_line_costs(upc);
CREATE INDEX IF NOT EXISTS idx_qlc_accepted ON quote_line_costs(accepted_at);

-- ═════════════════════════════════════════════════════════════════════
-- Runtime feature flags (Item C of P0 resilience backlog)
-- Threshold + constant hotfixes that should NOT require a deploy.
-- Read via src/core/flags.py get_flag(key, default) which layers a
-- 60s in-memory cache on top of this table.
-- ═════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS feature_flags (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')),
    updated_by TEXT DEFAULT '',
    description TEXT DEFAULT ''
);

-- ═════════════════════════════════════════════════════════════════════
-- Utilization events (Phase 4 of PC↔RFQ refactor)
-- Feature-use tracking so the internal dashboard knows which
-- parts of the app are hot, dead, or error-prone.
-- ═════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS utilization_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feature TEXT NOT NULL,
    context TEXT DEFAULT '',
    user TEXT DEFAULT '',
    duration_ms INTEGER DEFAULT 0,
    ok INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_util_feature ON utilization_events(feature);
CREATE INDEX IF NOT EXISTS idx_util_created ON utilization_events(created_at);

-- ═════════════════════════════════════════════════════════════════════
-- Agency Rules (Phase C — Gmail-derived per-buyer guidance)
-- Claude-extracted rules from 2y of buyer emails. Consumed by
-- Form QA gate to produce agency-specific warnings before send.
-- rule_type: forms, delivery, packaging, signature, contact,
--            quote_format, rejection_reason, misc
-- confidence: 0-1 Claude self-scored + n-sample-reinforced
-- ═════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS agency_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agency TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    rule_text TEXT NOT NULL,
    source_email_ids TEXT DEFAULT '[]',
    confidence REAL DEFAULT 0.5,
    sample_count INTEGER DEFAULT 1,
    first_seen TEXT DEFAULT (datetime('now')),
    last_seen TEXT DEFAULT (datetime('now')),
    active INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_agency_rules_agency ON agency_rules(agency, active);
CREATE INDEX IF NOT EXISTS idx_agency_rules_type ON agency_rules(rule_type);
"""

def init_db():
    """Create all tables if they don't exist. Safe to call multiple times.
    NOTE: DAL migration (JSON→DB for leads/customers/vendors) is deferred
    to background thread for faster startup. See app.py _deferred_init()."""
    print("[BOOT:DB] init_db: creating schema...", flush=True)
    with get_db() as conn:
        conn.executescript(SCHEMA)
    print("[BOOT:DB] init_db: migrating columns...", flush=True)
    _migrate_columns()
    _migrate_feature_flags_from_app_settings()
    _seed_supplier_profiles()
    # Usage tracking
    try:
        from src.core.usage_tracker import init_usage_tracking
        with get_db() as _uconn:
            init_usage_tracking(_uconn)
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    print("[BOOT:DB] init_db: complete", flush=True)
    log.info("DB initialized at %s", DB_PATH)
    return True


def init_db_deferred():
    """Run deferred DB init tasks (DAL migration). Called from background thread."""
    try:
        from src.core.dal import migrate_json_to_db
        migrate_json_to_db()
    except Exception as e:
        log.warning("DAL migration: %s", e)


def _migrate_columns():
    """Add missing columns to existing tables. Safe to call repeatedly."""
    migrations = [
        # (table, column, type+default)
        ("quotes", "contact_name", "TEXT"),
        ("quotes", "line_items", "TEXT"),
        ("quotes", "source", "TEXT DEFAULT ''"),
        ("quotes", "sent_at", "TEXT DEFAULT ''"),
        ("quotes", "notes", "TEXT DEFAULT ''"),
        ("quotes", "status_history", "TEXT DEFAULT '[]'"),
        ("quotes", "status_notes", "TEXT"),
        ("quotes", "total_cost", "REAL DEFAULT 0"),
        ("quotes", "gross_profit", "REAL DEFAULT 0"),
        ("quotes", "margin_pct", "REAL DEFAULT 0"),
        ("quotes", "items_costed", "INTEGER DEFAULT 0"),
        ("workflow_runs", "started_at", "TEXT"),
        ("workflow_runs", "finished_at", "TEXT"),
        ("workflow_runs", "type", "TEXT DEFAULT 'workflow'"),
        ("workflow_runs", "status", "TEXT DEFAULT 'completed'"),
        # PC award tracking (Phase: Option C workflow)
        ("price_checks", "status", "TEXT DEFAULT 'new'"),
        ("price_checks", "sent_at", "TEXT"),
        ("price_checks", "last_scprs_check", "TEXT"),
        ("price_checks", "scprs_check_count", "INTEGER DEFAULT 0"),
        ("price_checks", "award_status", "TEXT DEFAULT 'pending'"),
        ("price_checks", "competitor_name", "TEXT"),
        ("price_checks", "competitor_price", "REAL"),
        ("price_checks", "competitor_po", "TEXT"),
        ("price_checks", "revision_of", "TEXT"),
        ("price_checks", "closed_at", "TEXT"),
        ("price_checks", "closed_reason", "TEXT"),
        # PC persistence columns (email import + SQLite roundtrip)
        ("price_checks", "pc_number", "TEXT DEFAULT ''"),
        ("price_checks", "institution", "TEXT DEFAULT ''"),
        ("price_checks", "email_uid", "TEXT DEFAULT ''"),
        ("price_checks", "email_subject", "TEXT DEFAULT ''"),
        ("price_checks", "due_date", "TEXT DEFAULT ''"),
        # ── PRD-28 Work Item 1: Quote Lifecycle ──
        ("quotes", "expires_at", "TEXT"),
        ("quotes", "closed_by_agent", "TEXT"),
        ("quotes", "close_reason", "TEXT"),
        ("quotes", "revision_count", "INTEGER DEFAULT 0"),
        ("quotes", "win_probability", "REAL DEFAULT 0"),
        ("quotes", "last_follow_up", "TEXT"),
        ("quotes", "follow_up_count", "INTEGER DEFAULT 0"),
        # ── PRD-28 Work Item 2: Email Outbox Overhaul ──
        ("email_outbox", "retry_count", "INTEGER DEFAULT 0"),
        ("email_outbox", "last_error", "TEXT"),
        ("email_outbox", "retry_at", "TEXT"),
        ("email_outbox", "open_count", "INTEGER DEFAULT 0"),
        ("email_outbox", "click_count", "INTEGER DEFAULT 0"),
        ("email_outbox", "last_opened", "TEXT"),
        ("email_outbox", "last_clicked", "TEXT"),
        ("email_outbox", "tracking_id", "TEXT"),
        # ── PRD-28 Work Item 4: Revenue Dashboard ──
        ("revenue_log", "margin_pct", "REAL DEFAULT 0"),
        ("revenue_log", "cost", "REAL DEFAULT 0"),
        ("revenue_log", "category", "TEXT"),
        ("revenue_log", "institution", "TEXT"),
        # ── PRD-28 Work Item 5: Vendor Intelligence ──
        ("contacts", "converted_from_lead", "TEXT"),
        ("contacts", "last_contacted", "TEXT"),
        # ── SCPRS Intelligence ──
        ("scprs_po_master", "agency_key", "TEXT"),
        ("scprs_pull_schedule", "enabled", "INTEGER DEFAULT 1"),
        # ── Missing columns found by product audit ──
        ("contacts", "phone", "TEXT DEFAULT ''"),
        ("price_checks", "contact_email", "TEXT DEFAULT ''"),
        # ── QuickBooks integration ──
        ("price_checks", "pc_data", "TEXT DEFAULT '{}'"),
        ("price_checks", "qb_po_id", "TEXT"),
        ("price_checks", "qb_invoice_id", "TEXT"),
        ("vendors", "qb_vendor_id", "TEXT"),
        ("customers", "qb_customer_id", "TEXT"),
        # ── Lead nurture ──
        ("contacts", "nurture_sequence", "TEXT"),
        ("contacts", "nurture_step", "INTEGER DEFAULT 0"),
        ("contacts", "lead_score", "REAL DEFAULT 0"),
        # ── Multi-state, multi-source procurement ──
        ("scprs_po_master", "data_quality_flag", "TEXT"),
        ("scprs_po_master", "state", "TEXT DEFAULT 'CA'"),
        ("scprs_po_master", "jurisdiction", "TEXT DEFAULT 'state'"),
        ("scprs_po_master", "source_system", "TEXT DEFAULT 'scprs'"),
        ("scprs_po_lines", "state", "TEXT DEFAULT 'CA'"),
        ("scprs_awards", "state", "TEXT DEFAULT 'CA'"),
        ("scprs_awards", "source_system", "TEXT DEFAULT 'scprs'"),
        ("vendor_intel", "state", "TEXT DEFAULT 'CA'"),
        ("vendor_intel", "source_system", "TEXT DEFAULT 'scprs'"),
        ("buyer_intel", "state", "TEXT DEFAULT 'CA'"),
        ("buyer_intel", "source_system", "TEXT DEFAULT 'scprs'"),
        ("won_quotes_kb", "state", "TEXT DEFAULT 'CA'"),
        ("won_quotes_kb", "source_system", "TEXT DEFAULT 'scprs'"),
        ("competitors", "states", "TEXT DEFAULT 'CA'"),
        # ── Exhaustive scrape columns ──
        ("scprs_po_master", "screenshot_path", "TEXT DEFAULT ''"),
        ("scprs_po_master", "scraped_at", "TEXT DEFAULT ''"),
        # ── Catalog item enrichment ──
        ("scprs_catalog", "mfg_number", "TEXT DEFAULT ''"),
        ("scprs_catalog", "mfg_name", "TEXT DEFAULT ''"),
        ("scprs_catalog", "upc", "TEXT DEFAULT ''"),
        ("scprs_catalog", "asin", "TEXT DEFAULT ''"),
        ("scprs_catalog", "nsn", "TEXT DEFAULT ''"),
        ("scprs_catalog", "sku", "TEXT DEFAULT ''"),
        ("scprs_catalog", "product_url", "TEXT DEFAULT ''"),
        ("scprs_catalog", "product_url_verified", "INTEGER DEFAULT 0"),
        ("scprs_catalog", "identifiers_json", "TEXT DEFAULT ''"),
        ("scprs_catalog", "enriched_description", "TEXT DEFAULT ''"),
        ("scprs_catalog", "enrichment_status", "TEXT DEFAULT 'raw'"),
        # ── Quote speed clock ──
        ("price_checks", "received_at", "TEXT DEFAULT ''"),
        ("price_checks", "first_opened_at", "TEXT DEFAULT ''"),
        ("price_checks", "priced_at", "TEXT DEFAULT ''"),
        ("price_checks", "generated_at", "TEXT DEFAULT ''"),
        ("price_checks", "time_to_price_mins", "INTEGER DEFAULT 0"),
        ("price_checks", "time_to_send_mins", "INTEGER DEFAULT 0"),
        ("quotes", "received_at", "TEXT DEFAULT ''"),
        ("quotes", "first_opened_at", "TEXT DEFAULT ''"),
        ("quotes", "priced_at", "TEXT DEFAULT ''"),
        ("quotes", "generated_at", "TEXT DEFAULT ''"),
        ("quotes", "time_to_price_mins", "INTEGER DEFAULT 0"),
        ("quotes", "time_to_send_mins", "INTEGER DEFAULT 0"),
        # ── Item memory full fields ──
        ("item_mappings", "last_sell_price", "REAL DEFAULT 0"),
        ("item_mappings", "uom", "TEXT DEFAULT ''"),
        ("item_mappings", "supplier_url", "TEXT DEFAULT ''"),
        ("item_mappings", "notes", "TEXT DEFAULT ''"),
        # ── RFQ metadata columns ──
        ("rfqs", "solicitation_number", "TEXT DEFAULT ''"),
        ("rfqs", "due_date", "TEXT DEFAULT ''"),
        ("rfqs", "email_subject", "TEXT DEFAULT ''"),
        ("rfqs", "body_text", "TEXT DEFAULT ''"),
        ("rfqs", "form_type", "TEXT DEFAULT ''"),
        # ── Quote number persistence (prevents duplicate allocation) ──
        ("rfqs", "reytech_quote_number", "TEXT DEFAULT ''"),
        ("rfqs", "shipping_option", "TEXT DEFAULT 'included'"),
        ("rfqs", "shipping_amount", "REAL DEFAULT 0"),
        ("rfqs", "delivery_location", "TEXT DEFAULT ''"),
        # ── Package manifest (items_snapshot added after table existed on Railway) ──
        ("package_manifest", "items_snapshot", "TEXT"),
        # ── data_json blob: stores full dict for lossless round-trip ──
        ("rfqs", "data_json", "TEXT"),
        ("price_checks", "data_json", "TEXT"),
        ("orders", "data_json", "TEXT"),
        # ── Email threading (reply-in-thread + forward handling) ──
        ("price_checks", "email_message_id", "TEXT DEFAULT ''"),
        ("price_checks", "original_sender", "TEXT DEFAULT ''"),
        ("rfqs", "email_message_id", "TEXT DEFAULT ''"),
        ("rfqs", "original_sender", "TEXT DEFAULT ''"),
        # ── Orders V2: structured columns on orders (currently only in data_json blob) ──
        ("orders", "buyer_name", "TEXT DEFAULT ''"),
        ("orders", "buyer_email", "TEXT DEFAULT ''"),
        ("orders", "ship_to", "TEXT DEFAULT ''"),
        ("orders", "ship_to_address", "TEXT DEFAULT ''"),
        ("orders", "total_cost", "REAL DEFAULT 0"),
        ("orders", "margin_pct", "REAL DEFAULT 0"),
        ("orders", "po_pdf_path", "TEXT DEFAULT ''"),
        ("orders", "fulfillment_type", "TEXT DEFAULT 'dropship'"),
        # ── Orders V2: link vendor_orders to orders ──
        ("vendor_orders", "order_id", "TEXT DEFAULT ''"),
        # ── Multi-PC Bundle support ──
        ("price_checks", "bundle_id", "TEXT DEFAULT ''"),
        # ── Match Feedback ──
        ("item_mappings", "rejected", "INTEGER DEFAULT 0"),
        ("item_mappings", "reject_count", "INTEGER DEFAULT 0"),
        # ── UPC identifier matching ──
        ("won_quotes", "upc", "TEXT DEFAULT ''"),
        ("product_catalog", "upc", "TEXT DEFAULT ''"),
        # ── Email requirements extraction ──
        ("rfqs", "requirements_json", "TEXT DEFAULT '{}'"),
        ("price_checks", "requirements_json", "TEXT DEFAULT '{}'"),
        # ── IN-10: winning_quote_shapes.agency ──
        # Insert in pricing_oracle_v2.calibrate_from_outcome has always bound
        # the agency code into the `institution` slot (see 2026-04-21 audit).
        # Add a dedicated `agency` column so future consumers can disambiguate
        # without re-deriving from institution. Legacy `institution` kept for
        # read compatibility — already-written rows carry agency data there.
        ("winning_quote_shapes", "agency", "TEXT DEFAULT ''"),
        # ── BUILD-10: is_test on orders + revenue_log ──
        # quotes.is_test gates every BI / analytics aggregate; orders and
        # revenue_log were the two remaining revenue sources without it, so
        # a test quote converted to an order would count toward headline
        # won_revenue. Mirror the flag from the linked quote on INSERT and
        # filter aggregates with AND is_test=0.
        ("orders", "is_test", "INTEGER DEFAULT 0"),
        ("revenue_log", "is_test", "INTEGER DEFAULT 0"),
    ]
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        for table, col, col_type in migrations:
            try:
                conn.execute("ALTER TABLE " + _safe_identifier(table) + " ADD COLUMN " + _safe_identifier(col) + " " + col_type)
                log.info("Migration: added %s.%s", table, col)
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    pass  # Already exists — expected on repeat runs
                elif "no such table" in str(e).lower():
                    pass  # Table doesn't exist yet — CREATE TABLE will handle it
                else:
                    log.warning("Migration %s.%s failed: %s", table, col, e)
        # UPC indexes for fast barcode lookups
        for _idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_wq_upc ON won_quotes(upc)",
            "CREATE INDEX IF NOT EXISTS idx_pc_upc ON product_catalog(upc)",
            "CREATE INDEX IF NOT EXISTS idx_ps_sku ON product_suppliers(sku)",
            # IN-10: winning_quote_shapes.agency index — created AFTER the
            # column migration above so prod DBs don't fail on the index
            # build before ALTER TABLE has landed the column.
            "CREATE INDEX IF NOT EXISTS idx_wqs_agency ON winning_quote_shapes(agency)",
        ]:
            try:
                conn.execute(_idx_sql)
            except Exception as _e:
                log.debug("suppressed: %s", _e)

        # IN-10: backfill agency from institution for rows written before the
        # column existed. institution slot has always carried the agency code
        # in pricing_oracle_v2.calibrate_from_outcome — preserve that data.
        try:
            conn.execute(
                "UPDATE winning_quote_shapes "
                "SET agency = institution "
                "WHERE (agency IS NULL OR agency = '') AND institution != ''"
            )
        except sqlite3.OperationalError as _e:
            # Table may not exist on a fresh install where agency column lands
            # via CREATE TABLE (not ALTER). Nothing to backfill in that case.
            log.debug("winning_quote_shapes backfill skipped: %s", _e)

        # ── SCPRS ingest idempotency (audit P0 — 2026-04-19) ──
        # Dedupe existing (po_id, line_num) collisions before creating the
        # UNIQUE index. Keep the lowest id per collision; earlier rows win
        # because they're from the first successful ingest.
        try:
            conn.execute("""
                DELETE FROM scprs_po_lines
                WHERE id NOT IN (
                    SELECT MIN(id) FROM scprs_po_lines
                    WHERE po_id IS NOT NULL AND line_num IS NOT NULL
                    GROUP BY po_id, line_num
                )
                AND po_id IS NOT NULL AND line_num IS NOT NULL
            """)
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        # Hot-path indexes + idempotency UNIQUE. UNIQUE enables INSERT OR
        # REPLACE at the ingest sites so re-pulls refresh stale rows instead
        # of silently skipping them (prior SELECT-then-skip lost updates).
        for _idx_sql in [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_po_lines_uniq ON scprs_po_lines(po_id, line_num)",
            "CREATE INDEX IF NOT EXISTS idx_po_lines_opp ON scprs_po_lines(opportunity_flag)",
            "CREATE INDEX IF NOT EXISTS idx_po_lines_cat ON scprs_po_lines(category)",
            "CREATE INDEX IF NOT EXISTS idx_po_master_start ON scprs_po_master(start_date)",
            "CREATE INDEX IF NOT EXISTS idx_po_master_supplier_lc ON scprs_po_master(LOWER(supplier))",
        ]:
            try:
                conn.execute(_idx_sql)
            except Exception as _e:
                log.debug("suppressed: %s", _e)
        # Copy agency_code → agency_key for existing SCPRS data
        try:
            conn.execute("""
                UPDATE scprs_po_master SET agency_key = agency_code
                WHERE (agency_key IS NULL OR agency_key = '') AND agency_code IS NOT NULL
            """)
        except Exception as _e:
            log.debug("suppressed: %s", _e)  # agency_code column may not exist on fresh installs
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Column migration failed: %s", e)

    # ── Cleanup: remove test orders/revenue from all sources ──
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        r1 = conn.execute("DELETE FROM orders WHERE po_number LIKE '%TEST%'")
        r2 = conn.execute("DELETE FROM revenue_log WHERE po_number LIKE '%TEST%'")
        r3 = conn.execute("DELETE FROM revenue_log WHERE id LIKE 'rev-R26Q%' AND source = 'quote_won'")
        r4 = conn.execute("UPDATE quotes SET is_test = 1 WHERE quote_number IN (SELECT quote_number FROM quotes WHERE quote_number LIKE 'R26Q%' AND status = 'won' AND is_test = 0 AND total < 700)")
        total_cleaned = r1.rowcount + r2.rowcount + r3.rowcount + r4.rowcount
        if total_cleaned > 0:
            log.info("Test data cleanup: removed %d test orders, %d revenue entries, %d orphan entries, marked %d test quotes",
                     r1.rowcount, r2.rowcount, r3.rowcount, r4.rowcount)
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug("Test data cleanup: %s", e)

    # (orders.json cleanup removed — SQLite is single source of truth)


def _migrate_feature_flags_from_app_settings():
    """One-shot copy of legacy `app_settings WHERE key LIKE 'flag:%'`
    rows into the canonical `feature_flags` table so admin-API writes
    and legacy reads converge. Idempotent — `INSERT OR IGNORE` keeps
    values already written via the new path untouched.

    Legacy values were stored JSON-encoded (`true`, `5`, `"foo"`).
    Strings come back double-quoted; strip one layer so the unified
    coercion in `flags._coerce` sees the same shape as a value written
    through the new API.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT key, value FROM app_settings WHERE key LIKE 'flag:%'"
        ).fetchall()
        copied = 0
        for r in rows:
            name = r["key"].replace("flag:", "", 1)
            if not name:
                continue
            raw = r["value"]
            if raw is None:
                continue
            raw = str(raw)
            if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
                raw = raw[1:-1]
            cur = conn.execute(
                """INSERT OR IGNORE INTO feature_flags
                       (key, value, updated_by, description, updated_at)
                   VALUES (?, ?, 'migration', 'copied from app_settings', datetime('now'))""",
                (name, raw),
            )
            if cur.rowcount:
                copied += 1
        conn.commit()
        conn.close()
        if copied:
            log.info("feature_flags migration: copied %d legacy flag(s) from app_settings", copied)
    except Exception as e:
        log.warning("feature_flags migration failed: %s", e)


# ── Supplier Profiles: seed + lookup ─────────────────────────────────────────

_SEED_SUPPLIERS = [
    # (name, tax_exempt_status, free_shipping_threshold, default_shipping_pct, notes)
    ("Amazon",              "exempt_on_file", 35,   0,  "ATEP resale cert on file. Prime free shipping."),
    ("Grainger",            "exempt_on_file", 50,   0,  "Resale cert on file. Free ship >$50."),
    ("Uline",              "exempt_on_file", 250,  5,  "Resale cert on file. Free ship >$250."),
    ("Staples",            "exempt_on_file", 50,   0,  "Business account. Free ship >$50."),
    ("Office Depot",       "exempt_on_file", 50,   0,  "Business account. Free ship >$50."),
    ("S&S Worldwide",      "exempt_on_file", 75,   0,  "Resale cert on file. Free ship >$75."),
    ("McMaster-Carr",      "not_accepted",   0,    8,  "Does NOT honor resale certs. Always charges tax. Shipping varies."),
    ("Dollar Tree",        "not_accepted",   0,    0,  "Retail only. Tax charged. Case-only online orders."),
    ("Target",             "not_accepted",   35,   5,  "Retail. No resale cert online. Free ship >$35."),
    ("Home Depot",         "exempt_on_file", 0,    0,  "Pro account. Free delivery on most orders."),
    ("Medline",            "exempt_on_file", 0,    0,  "Direct account. Shipping negotiated."),
    ("Bound Tree Medical", "exempt_on_file", 0,    0,  "Medical supplier. Shipping negotiated."),
    ("Henry Schein",       "exempt_on_file", 0,    0,  "Medical/dental supplier. Shipping negotiated."),
    ("Fisher Scientific",  "exempt_on_file", 0,    5,  "Lab supplier. Resale cert on file."),
    ("Zoro",               "exempt_on_file", 50,   0,  "Grainger subsidiary. Free ship >$50."),
    ("Global Industrial",  "exempt_on_file", 0,    0,  "Free shipping on most items."),
    ("Fastenal",           "exempt_on_file", 0,    0,  "Branch pickup or delivery. Resale cert on file."),
    ("Moore Medical",      "exempt_on_file", 0,    5,  "Medical supplier. Resale cert on file."),
]


def _seed_supplier_profiles():
    """Pre-seed supplier profiles on first run. Skips existing rows."""
    try:
        with get_db() as conn:
            for name, tax, threshold, ship_pct, notes in _SEED_SUPPLIERS:
                conn.execute("""
                    INSERT OR IGNORE INTO supplier_profiles
                    (supplier_name, tax_exempt_status, free_shipping_threshold,
                     default_shipping_pct, notes)
                    VALUES (?, ?, ?, ?, ?)
                """, (name, tax, threshold, ship_pct, notes))
    except Exception as e:
        log.debug("Supplier profile seed: %s", e)


def get_supplier_profile(supplier_name):
    """Look up a supplier's tax/shipping profile. Returns dict or None."""
    if not supplier_name:
        return None
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM supplier_profiles WHERE supplier_name = ?",
                (supplier_name,)
            ).fetchone()
            if row:
                return dict(row)
            # Fuzzy match: try case-insensitive contains
            row = conn.execute(
                "SELECT * FROM supplier_profiles WHERE LOWER(supplier_name) = LOWER(?)",
                (supplier_name,)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def get_all_supplier_profiles():
    """Return all supplier profiles as list of dicts."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM supplier_profiles ORDER BY supplier_name"
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def calc_landed_cost(unit_cost, qty=1, supplier_name="", order_total=None):
    """Calculate landed cost per unit factoring in shipping + tax.

    Returns: {
        landed_cost: float,      # per-unit all-in cost
        shipping_per_unit: float,
        tax_per_unit: float,
        raw_cost: float,         # original supplier unit cost
        supplier_profile: dict|None,
        breakdown: str,          # human-readable explanation
    }
    """
    if not unit_cost or unit_cost <= 0:
        return {"landed_cost": 0, "shipping_per_unit": 0, "tax_per_unit": 0,
                "raw_cost": 0, "supplier_profile": None, "breakdown": ""}

    profile = get_supplier_profile(supplier_name)
    shipping_per_unit = 0.0
    tax_per_unit = 0.0
    parts = []

    if profile:
        # Shipping estimate
        threshold = profile.get("free_shipping_threshold") or 0
        ship_pct = profile.get("default_shipping_pct") or 0
        est_order = order_total or (unit_cost * qty)
        if threshold > 0 and est_order < threshold and ship_pct > 0:
            shipping_per_unit = round(unit_cost * ship_pct / 100, 4)
            parts.append(f"+{ship_pct}% ship (order <${threshold:.0f})")
        elif ship_pct > 0 and threshold == 0:
            shipping_per_unit = round(unit_cost * ship_pct / 100, 4)
            parts.append(f"+{ship_pct}% ship")

        # Tax on purchase (if supplier doesn't honor exemption)
        tax_status = profile.get("tax_exempt_status", "unknown")
        if tax_status == "not_accepted":
            # Use CA base rate as estimate for tax paid on purchase
            ca_rate = 0.0875  # ~8.75% avg CA rate
            tax_per_unit = round(unit_cost * ca_rate, 4)
            parts.append(f"+{ca_rate*100:.1f}% tax (no exemption)")

    landed = round(unit_cost + shipping_per_unit + tax_per_unit, 4)
    breakdown = f"${unit_cost:.2f}"
    if parts:
        breakdown += " " + " ".join(parts) + f" = ${landed:.2f}"

    return {
        "landed_cost": landed,
        "shipping_per_unit": shipping_per_unit,
        "tax_per_unit": tax_per_unit,
        "raw_cost": unit_cost,
        "supplier_profile": profile,
        "breakdown": breakdown,
    }


def _reconcile_quotes_json():
    """Sync quotes_log.json statuses from DB (source of truth).

    Fixes drift where DB status was updated but JSON dual-write lagged.
    Runs once on every boot — safe and idempotent.
    """
    json_path = os.path.join(DATA_DIR, "quotes_log.json")
    if not os.path.exists(json_path):
        return
    try:
        with open(json_path) as f:
            quotes = json.load(f)
        if not quotes:
            return

        conn = _make_connection()
        conn.row_factory = sqlite3.Row
        db_quotes = {r["quote_number"]: dict(r)
                     for r in conn.execute("SELECT quote_number, status FROM quotes").fetchall()}
        conn.close()

        patched = 0
        for q in quotes:
            qn = q.get("quote_number")
            if qn and qn in db_quotes:
                db_status = db_quotes[qn]["status"]
                if q.get("status") != db_status:
                    q["status"] = db_status
                    patched += 1

        if patched:
            with open(json_path, "w") as f:
                json.dump(quotes, f, indent=2, default=str)
            log.info("Reconciled %d quote statuses (DB → JSON)", patched)
    except Exception as e:
        log.warning("_reconcile_quotes_json: %s", e)

# ── Quote operations ──────────────────────────────────────────────────────────
def upsert_quote(q: dict, actor: str = "system") -> bool:
    """Insert or update a quote record. Called from _log_quote().

    Computes profit fields from line_items if vendor_cost is present.
    This is the source of truth for per-quote profitability.
    """
    # Contract enforcement (Law 28) — block empty shells
    try:
        from src.core.contracts import validate_quote, log_blocked_save
        is_void = q.get("status") in ("void", "cancelled")
        if not is_void:
            is_valid, violations = validate_quote(q, strict=True)
            if not is_valid:
                log_blocked_save("quote", q.get("quote_number", "?"), violations, "upsert_quote")
                return False
    except ImportError as _e:
        log.debug("suppressed: %s", _e)  # contracts.py not deployed yet — skip
    now = datetime.now().isoformat()

    # Compute profit from line items — use first-class fields if available
    line_items = _decode_json_field(
        q.get("line_items") or q.get("items_detail") or [],
        field="line_items",
        where="db.profit_compute",
    )
    if line_items is None:
        line_items = []
    total_cost = 0.0
    gross_profit = 0.0
    items_costed = 0
    for li in (line_items if isinstance(line_items, list) else []):
        vc = li.get("vendor_cost") or li.get("unit_cost") or li.get("supplier_cost") or 0
        up = li.get("unit_price") or li.get("our_price") or 0
        qty = li.get("qty", 1) or 1
        if vc and up:
            total_cost += float(vc) * qty
            gross_profit += (float(up) - float(vc)) * qty
            items_costed += 1
    subtotal = float(q.get("subtotal") or q.get("total") or 0)
    margin_pct = round(gross_profit / subtotal * 100, 1) if subtotal and gross_profit else 0

    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO quotes
                  (quote_number, created_at, agency, institution, requestor,
                   contact_name, contact_email, contact_phone, rfq_number,
                   ship_to_name, ship_to_address, subtotal, tax, total,
                   items_count, items_text, items_detail, line_items,
                   status, pdf_path, source_pc_id, source_rfq_id,
                   source, sent_at, notes, status_history,
                   po_number, is_test,
                   total_cost, gross_profit, margin_pct, items_costed,
                   updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(quote_number) DO UPDATE SET
                  agency=excluded.agency, institution=excluded.institution,
                  requestor=excluded.requestor, contact_name=excluded.contact_name,
                  contact_email=excluded.contact_email, contact_phone=excluded.contact_phone,
                  ship_to_name=excluded.ship_to_name, ship_to_address=excluded.ship_to_address,
                  subtotal=excluded.subtotal, tax=excluded.tax, total=excluded.total,
                  items_count=excluded.items_count, items_text=excluded.items_text,
                  items_detail=excluded.items_detail, line_items=excluded.line_items,
                  status=excluded.status, notes=excluded.notes,
                  status_history=excluded.status_history,
                  pdf_path=excluded.pdf_path, po_number=excluded.po_number,
                  total_cost=excluded.total_cost, gross_profit=excluded.gross_profit,
                  margin_pct=excluded.margin_pct, items_costed=excluded.items_costed,
                  updated_at=excluded.updated_at
            """, (
                q.get("quote_number"), q.get("created_at", now),
                q.get("agency"), q.get("institution"),
                q.get("requestor") or q.get("contact_name"),
                q.get("contact_name") or q.get("requestor"),
                q.get("email") or q.get("requestor_email") or q.get("contact_email"),
                q.get("phone") or q.get("contact_phone"),
                q.get("rfq_number"),
                q.get("ship_to_name"),
                json.dumps(q.get("ship_to_address", [])),
                q.get("subtotal", 0), q.get("tax", 0), q.get("total", 0),
                q.get("items_count", 0),
                q.get("items_text", ""),
                json.dumps(q.get("items_detail", [])),
                json.dumps(line_items),
                q.get("status", "pending"),
                q.get("pdf_path") or q.get("path"),
                q.get("source_pc_id"), q.get("source_rfq_id"),
                q.get("source", ""),
                q.get("sent_at", ""),
                q.get("notes", ""),
                json.dumps(q.get("status_history", [])),
                q.get("po_number"),
                1 if q.get("is_test") else 0,
                round(total_cost, 2), round(gross_profit, 2), margin_pct, items_costed,
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
            d[field] = _decode_json_field(d[field], field=field, where="db.get_quote")
    return d


def get_all_quotes_db(status: str = None, limit: int = 500, include_test: bool = False) -> list:
    """Get all quotes from SQLite, newest first. Excludes test quotes by default."""
    with get_db() as conn:
        test_filter = "" if include_test else "AND is_test=0"
        if status:
            rows = conn.execute(
                f"SELECT * FROM quotes WHERE status=? {test_filter} ORDER BY created_at DESC LIMIT ?",
                (status, limit)).fetchall()
        else:
            rows = conn.execute(
                f"SELECT * FROM quotes WHERE 1=1 {test_filter} ORDER BY created_at DESC LIMIT ?",
                (limit,)).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        for field in ("items_detail", "ship_to_address"):
            if d.get(field):
                d[field] = _decode_json_field(d[field], field=field, where="db.get_all_quotes_db")
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
    def _do():
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
    try:
        return db_retry(_do)
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


# ── Price audit operations ────────────────────────────────────────────────────
def record_audit(item_description: str, field_changed: str,
                 old_value: float | None, new_value: float | None,
                 source: str, rfq_id: str = "", part_number: str = "",
                 actor: str = "system", notes: str = "") -> int | None:
    """Record a price change event for audit trail."""
    if not item_description or not field_changed:
        return None
    try:
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO price_audit
                  (ts, rfq_id, item_description, part_number, field_changed,
                   old_value, new_value, source, actor, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                datetime.now().isoformat(),
                rfq_id, item_description[:500], part_number[:100],
                field_changed, old_value, new_value,
                source, actor, notes[:500],
            ))
            return cur.lastrowid
    except Exception as e:
        log.debug("record_audit: %s", e)
        return None


def get_audit_trail(description: str = "", part_number: str = "",
                    rfq_id: str = "", limit: int = 20) -> list:
    """Get price change audit trail. Returns newest first."""
    conditions = []
    params = []
    if description:
        conditions.append("LOWER(item_description) LIKE ?")
        params.append(f"%{description.lower()[:100]}%")
    if part_number:
        conditions.append("LOWER(part_number) LIKE ?")
        params.append(f"%{part_number.lower()}%")
    if rfq_id:
        conditions.append("rfq_id=?")
        params.append(rfq_id)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)
    try:
        with get_db() as conn:
            rows = conn.execute(
                f"SELECT * FROM price_audit {where} ORDER BY ts DESC LIMIT ?",
                params).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.debug("get_audit_trail: %s", e)
        return []


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


def get_all_contacts() -> dict:
    """Return all CRM contacts as a dict keyed by contact ID.
    SQLite is the authoritative source. Returns {} on error."""
    try:
        with get_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT id, created_at, buyer_name, buyer_email, buyer_phone,
                       agency, title, department, linkedin, notes, tags,
                       total_spend, po_count, categories, items_purchased,
                       purchase_orders, last_purchase, score, opportunity_score,
                       is_reytech_customer, outreach_status, source,
                       intel_synced_at, updated_at
                FROM contacts ORDER BY updated_at DESC
            """).fetchall()
            contacts = {}
            for r in rows:
                cid = r["id"]
                contacts[cid] = {
                    "id": cid,
                    "created_at": r["created_at"] or "",
                    "buyer_name": r["buyer_name"] or "",
                    "buyer_email": r["buyer_email"] or "",
                    "buyer_phone": r["buyer_phone"] or "",
                    "agency": r["agency"] or "",
                    "title": r["title"] or "",
                    "department": r["department"] or "",
                    "linkedin": r["linkedin"] or "",
                    "notes": r["notes"] or "",
                    "tags": json.loads(r["tags"] or "[]"),
                    "total_spend": r["total_spend"] or 0,
                    "po_count": r["po_count"] or 0,
                    "categories": json.loads(r["categories"] or "{}"),
                    "items_purchased": json.loads(r["items_purchased"] or "[]"),
                    "purchase_orders": json.loads(r["purchase_orders"] or "[]"),
                    "last_purchase": r["last_purchase"] or "",
                    "score": r["score"] or 0,
                    "opportunity_score": r["opportunity_score"] or 0,
                    "is_reytech_customer": bool(r["is_reytech_customer"]),
                    "outreach_status": r["outreach_status"] or "new",
                    "source": r["source"] or "manual",
                    "intel_synced_at": r["intel_synced_at"] or "",
                    "updated_at": r["updated_at"] or "",
                }
            return contacts
    except Exception as e:
        log.error("get_all_contacts: %s", e)
        return {}


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
            d["metadata"] = _decode_json_field(d["metadata"], field="metadata", where="db.get_activity_log")
        result.append(d)
    return result


# ── Revenue operations ────────────────────────────────────────────────────────
def log_revenue(amount: float, description: str, source: str = "manual",
                quote_number: str = "", po_number: str = "",
                agency: str = "", date: str = "") -> str | None:
    """Record a revenue entry. Uses stable ID based on quote_number to prevent duplicates."""
    # Stable ID: if quote_number provided, use it to prevent duplicates
    if quote_number:
        rid = f"rev-{quote_number}"
    else:
        rid = f"REV-{datetime.now().strftime('%Y%m%d')}-{os.urandom(3).hex()}"
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO revenue_log
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
              "orders", "rfqs", "revenue_log", "price_checks"]
    stats = {"db_path": DB_PATH, "db_size_kb": 0}
    try:
        stats["db_size_kb"] = round(os.path.getsize(DB_PATH) / 1024, 1)
    except FileNotFoundError as _e:
        log.debug("suppressed: %s", _e)
    with get_db() as conn:
        for table in tables:
            try:
                count = conn.execute("SELECT COUNT(*) FROM " + _safe_identifier(table)).fetchone()[0]
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
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    log.info("JSON→DB migration: %s", counts)

    # ── email_outbox migration ──
    outbox = _try_load("email_outbox.json")
    if isinstance(outbox, list):
        for em in outbox:
            try:
                upsert_outbox_email(em)
                counts["quotes"] += 0  # count it under general migration
            except Exception as _e:
                log.debug("suppressed: %s", _e)
    elif isinstance(outbox, dict):
        for eid, em in outbox.items():
            try:
                if isinstance(em, dict):
                    em['id'] = em.get('id', eid)
                    upsert_outbox_email(em)
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    # ── growth_outreach migration ──
    growth = _try_load("growth_outreach.json")
    if isinstance(growth, dict):
        for camp in growth.get("campaigns", []):
            try:
                save_growth_campaign(camp)
            except Exception as _e:
                log.debug("suppressed: %s", _e)
    elif isinstance(growth, list):
        for camp in growth:
            try:
                save_growth_campaign(camp)
            except Exception as _e:
                log.debug("suppressed: %s", _e)

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


def _boot_sync_quotes():
    """Ensure quotes from JSON are in SQLite. Runs every boot.
    Only inserts quotes that are MISSING from SQLite (doesn't overwrite)."""
    ql_path = os.path.join(DATA_DIR, "quotes_log.json")
    if not os.path.exists(ql_path):
        return
    try:
        with open(ql_path) as f:
            quotes = json.load(f)
        if not isinstance(quotes, list) or not quotes:
            return
    except Exception:
        return

    synced = 0
    with get_db() as conn:
        existing = set(r[0] for r in conn.execute("SELECT quote_number FROM quotes").fetchall())
        for q in quotes:
            qn = q.get("quote_number", "")
            if not qn or qn in existing:
                continue
            if upsert_quote(q):
                synced += 1
    if synced:
        log.info("Boot sync: added %d quotes from JSON → SQLite", synced)


def _boot_sync_pcs():
    """Restore PCs from SQLite → JSON if JSON is empty. Runs every boot."""
    pc_path = os.path.join(DATA_DIR, "price_checks.json")
    # Check current JSON state
    json_count = 0
    try:
        if os.path.exists(pc_path):
            with open(pc_path) as f:
                pcs = json.load(f)
            json_count = len(pcs) if isinstance(pcs, dict) else 0
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    if json_count > 0:
        return  # JSON has data, no need to restore

    # JSON is empty — restore from SQLite
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM price_checks WHERE status NOT IN ('dismissed','cancelled')"
            ).fetchall()
            if not rows:
                return
            restored = {}
            for row in rows:
                r = dict(row)
                pc_id = r["id"]
                items = _decode_json_field(
                    r["items"], field="items", where="db.restore_pcs_from_quotes",
                ) or []
                restored[pc_id] = {
                    "id": pc_id,
                    "pc_number": r.get("pc_number") or r.get("quote_number") or pc_id,
                    "institution": r.get("institution") or r.get("agency") or "",
                    "requestor": r.get("requestor") or "",
                    "items": items,
                    "source_pdf": r.get("source_file") or "",
                    "status": r.get("status") or "parsed",
                    "created_at": r.get("created_at") or "",
                    "reytech_quote_number": r.get("quote_number") or "",
                    "email_uid": r.get("email_uid") or "",
                    "email_subject": r.get("email_subject") or "",
                    "due_date": r.get("due_date") or "",
                    "source": "email_auto",
                }
            if restored:
                with open(pc_path, "w") as f:
                    json.dump(restored, f, indent=2, default=str)
                log.info("Boot sync: restored %d PCs from SQLite → JSON", len(restored))
    except Exception as e:
        log.warning("Boot PC restore: %s", e)


def startup() -> dict:
    """Initialize DB and migrate existing data. Call once at app start."""
    print("[BOOT:DB] startup() entered", flush=True)
    # Step 1: If volume is fresh, copy seed JSON into it so the app can read them
    if _is_railway_volume():
        print("[BOOT:DB] Seeding volume JSON...", flush=True)
        seed_results = _seed_volume_json()
        copied = [k for k, v in seed_results.items() if v == "copied"]
        if copied:
            log.info("Volume first-boot seed: copied %d files", len(copied))

    print("[BOOT:DB] init_db()...", flush=True)
    init_db()
    print("[BOOT:DB] init_db() done", flush=True)

    # ── Auto-reconcile DB → JSON quote statuses on every boot ──
    print("[BOOT:DB] reconcile quotes...", flush=True)
    _reconcile_quotes_json()

    # ── Auto-dedup price checks on every boot ──
    print("[BOOT:DB] dedup PCs...", flush=True)
    _dedup_price_checks_on_boot()

    # ── Data integrity fixes on every boot ──
    print("[BOOT:DB] fix data...", flush=True)
    _fix_data_on_boot()
    print("[BOOT:DB] fix data done", flush=True)

    stats_before = get_db_stats()
    if stats_before.get("quotes", 0) == 0 and stats_before.get("contacts", 0) == 0:
        # First run — migrate from JSON seed files
        migrated = migrate_json_to_db()
        log.info("First-run migration complete: %s", migrated)
    else:
        # ── Always sync quotes + PCs from JSON → SQLite on boot ──────
        # Even if contacts exist, quotes/PCs might be missing from SQLite
        # (created in JSON before SQLite sync was added, or lost on deploy)
        try:
            _boot_sync_quotes()
        except Exception as e:
            log.warning("Boot quote sync: %s", e)
        try:
            _boot_sync_pcs()
        except Exception as e:
            log.warning("Boot PC sync: %s", e)
    stats = get_db_stats()
    is_vol = _is_railway_volume()
    log.info("DB ready [volume=%s]: %s", is_vol,
             {k: v for k, v in stats.items() if k not in ("db_path", "db_size_kb")})
    
    # ── PC persistence diagnostic ──
    pc_path = os.path.join(DATA_DIR, "price_checks.json")
    pc_count = 0
    try:
        if os.path.exists(pc_path):
            with open(pc_path) as _f:
                _pcs = json.load(_f)
                pc_count = len(_pcs) if isinstance(_pcs, dict) else 0
            log.info("BOOT PC CHECK: %d price checks in %s (size=%d bytes)",
                     pc_count, pc_path, os.path.getsize(pc_path))
        else:
            log.warning("BOOT PC CHECK: price_checks.json NOT FOUND at %s", pc_path)
    except Exception as e:
        log.warning("BOOT PC CHECK: error reading price_checks.json: %s", e)
    
    return {"ok": True, "db_path": DB_PATH, "stats": stats, "is_volume": is_vol}


def _fix_data_on_boot():
    """Idempotent data integrity fixes that run on every deploy.
    
    Fixes:
    1. Clean revenue_log duplicate entries (random IDs → stable IDs)
    2. Sync orders.json → SQLite orders table
    3. Mark test fixture quotes as is_test=1
    4. Mark quotes with orders as 'won' if still 'pending'
    """
    import re as _re
    try:
        with get_db() as conn:
            fixes = []

            # Fix 1: Clean revenue_log duplicates (entries with random REV- IDs)
            dupes = conn.execute("""
                SELECT quote_number, COUNT(*) as c FROM revenue_log
                WHERE id LIKE 'REV-%' AND quote_number != ''
                GROUP BY quote_number HAVING c > 1
            """).fetchall()
            for d in dupes:
                # Keep one, delete rest
                conn.execute("""
                    DELETE FROM revenue_log
                    WHERE id LIKE 'REV-%' AND quote_number = ?
                    AND id NOT IN (
                        SELECT id FROM revenue_log
                        WHERE quote_number = ? ORDER BY logged_at DESC LIMIT 1
                    )
                """, (d["quote_number"], d["quote_number"]))
                fixes.append(f"cleaned {d['c']-1} dupes for {d['quote_number']}")

            # Fix 2: One-time migration of orders.json → SQLite (if not yet migrated)
            orders_path = os.path.join(DATA_DIR, "orders.json")
            if os.path.exists(orders_path):
                try:
                    with open(orders_path) as f:
                        json_orders = json.load(f)
                    migrated = 0
                    for oid, o in json_orders.items():
                        exists = conn.execute("SELECT id FROM orders WHERE id=?", (oid,)).fetchone()
                        if not exists:
                            conn.execute("""
                                INSERT OR IGNORE INTO orders
                                (id, quote_number, po_number, agency, institution,
                                 total, status, items, created_at, updated_at, data_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (oid, o.get("quote_number", ""), o.get("po_number", ""),
                                  o.get("agency", ""), o.get("institution", o.get("customer", "")),
                                  o.get("total", 0), o.get("status", "new"),
                                  json.dumps(o.get("line_items", [])),
                                  o.get("created_at", ""), datetime.now().isoformat(),
                                  json.dumps(o, default=str)))
                            migrated += 1
                    if migrated:
                        fixes.append(f"migrated {migrated} orders from JSON")
                    # Rename to .migrated so we don't re-read on next boot
                    os.rename(orders_path, orders_path + ".migrated")
                    fixes.append("renamed orders.json to orders.json.migrated")
                except Exception as _e:
                    log.debug("suppressed: %s", _e)

            # Fix 3: Mark test fixture quotes as is_test=1.
            # 2026-04-26 (Phase 0.7d): exempt the QuoteWerks/SCPRS imports.
            # The pre-app QuoteWerks export has DocNos like '23-0003' and
            # 'DEMQ1001' — none of these match ^R26Q\d+$. Without the
            # exemption, this sweep re-flags 464 of 503 prod quotes as
            # is_test=1 every boot, breaking win-rate analytics. The
            # importers stamp 'QuoteWerks:' or 'SCPRS-verify' into
            # status_notes so we can recognize them here without a
            # separate flag column.
            real_pattern = _re.compile(r'^R26Q\d+$')
            test_quotes = conn.execute("""
                SELECT quote_number, COALESCE(status_notes, '') AS notes
                FROM quotes
                WHERE is_test = 0
            """).fetchall()
            test_qns = []
            for q in test_quotes:
                qn = q["quote_number"] or ""
                notes = q["notes"] or ""
                if real_pattern.match(qn):
                    continue
                # Exempt rows with importer stamps — these ARE real data.
                if "QuoteWerks:" in notes or "SCPRS-verify" in notes:
                    continue
                test_qns.append(qn)
            if test_qns:
                placeholders = ",".join(["?" for _ in test_qns])
                conn.execute(
                    f"UPDATE quotes SET is_test = 1 WHERE quote_number IN ({placeholders})",
                    test_qns)
                fixes.append(f"marked {len(test_qns)} test quotes")

            # Fix 4: Mark quotes with orders as 'won' if still pending
            pending_with_order = conn.execute("""
                SELECT q.quote_number, o.po_number
                FROM quotes q JOIN orders o ON o.quote_number = q.quote_number
                WHERE q.status = 'pending' AND o.total > 0
            """).fetchall()
            for pwo in pending_with_order:
                conn.execute("""
                    UPDATE quotes SET status = 'won', po_number = ?
                    WHERE quote_number = ? AND status = 'pending'
                """, (pwo["po_number"], pwo["quote_number"]))
                fixes.append(f"marked {pwo['quote_number']} as won")

            # Fix 5: Orders V2 — explode data_json line items into order_line_items
            already_migrated = conn.execute(
                "SELECT name FROM migrations_applied WHERE name='orders_v2_line_items'"
            ).fetchone()
            if not already_migrated:
                try:
                    rows = conn.execute(
                        "SELECT id, data_json, created_at FROM orders WHERE data_json IS NOT NULL AND data_json != ''"
                    ).fetchall()
                    v2_migrated = 0
                    for row in rows:
                        oid = row["id"]
                        blob = row["data_json"]
                        if not blob:
                            continue
                        # Skip if already has line items in new table
                        existing = conn.execute(
                            "SELECT COUNT(*) as c FROM order_line_items WHERE order_id=?", (oid,)
                        ).fetchone()
                        if existing and existing["c"] > 0:
                            continue
                        try:
                            order = json.loads(blob)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        items = order.get("line_items", order.get("items", []))
                        if not items or not isinstance(items, list):
                            continue
                        for i, it in enumerate(items):
                            if not isinstance(it, dict):
                                continue
                            qty = it.get("qty", 0) or 0
                            price = it.get("unit_price", 0) or 0
                            cost = it.get("cost", 0) or 0
                            conn.execute("""
                                INSERT INTO order_line_items
                                (order_id, line_number, description, part_number,
                                 mfg_number, asin, uom, qty_ordered,
                                 unit_price, unit_cost, extended_price, extended_cost,
                                 sourcing_status, supplier_name, supplier_url,
                                 tracking_number, carrier, ship_date, delivery_date,
                                 invoice_status, invoice_number, notes,
                                 created_at, updated_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, (
                                oid, i + 1,
                                it.get("description", ""),
                                it.get("part_number", ""),
                                it.get("mfg_number", ""),
                                it.get("asin", ""),
                                it.get("uom", "EA"),
                                qty,
                                price,
                                cost,
                                round(qty * price, 2),
                                round(qty * cost, 2),
                                it.get("sourcing_status", "pending"),
                                it.get("supplier", ""),
                                it.get("supplier_url", ""),
                                it.get("tracking_number", ""),
                                it.get("carrier", ""),
                                it.get("ship_date", ""),
                                it.get("delivery_date", ""),
                                it.get("invoice_status", "pending"),
                                it.get("invoice_number", ""),
                                it.get("notes", ""),
                                order.get("created_at", row["created_at"] or datetime.now().isoformat()),
                                datetime.now().isoformat(),
                            ))
                        # Update structured columns on orders table
                        total_cost = sum(
                            (it.get("cost", 0) or 0) * (it.get("qty", 0) or 0)
                            for it in items if isinstance(it, dict)
                        )
                        conn.execute("""
                            UPDATE orders SET
                                buyer_name = COALESCE(buyer_name, ?),
                                buyer_email = COALESCE(buyer_email, ?),
                                ship_to = COALESCE(ship_to, ?),
                                total_cost = ?,
                                po_pdf_path = COALESCE(po_pdf_path, ?)
                            WHERE id = ?
                        """, (
                            order.get("buyer_name", ""),
                            order.get("buyer_email", ""),
                            order.get("ship_to_name", order.get("ship_to", "")),
                            round(total_cost, 2),
                            order.get("po_pdf_path", ""),
                            oid,
                        ))
                        v2_migrated += 1
                    conn.execute(
                        "INSERT OR IGNORE INTO migrations_applied (name, applied_at) VALUES (?, ?)",
                        ("orders_v2_line_items", datetime.now().isoformat())
                    )
                    if v2_migrated:
                        fixes.append(f"V2 migration: exploded {v2_migrated} orders into order_line_items")
                    log.info("Orders V2 migration complete: %d orders exploded", v2_migrated)
                except Exception as v2e:
                    log.error("Orders V2 migration failed: %s", v2e, exc_info=True)

            # Fix 6: Orders V2 — merge purchase_orders into orders + order_line_items
            po_migrated = conn.execute(
                "SELECT name FROM migrations_applied WHERE name='orders_v2_merge_po'"
            ).fetchone()
            if not po_migrated:
                try:
                    # Check if purchase_orders table exists
                    po_exists = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='purchase_orders'"
                    ).fetchone()
                    if po_exists:
                        po_rows = conn.execute("SELECT * FROM purchase_orders").fetchall()
                        merged = 0
                        for po in po_rows:
                            po_d = dict(po)
                            po_num = po_d.get("po_number", "")
                            if not po_num:
                                continue
                            # Skip if order already exists with this PO number
                            existing = conn.execute(
                                "SELECT id FROM orders WHERE po_number=?", (po_num,)
                            ).fetchone()
                            if existing:
                                continue
                            # Create order from purchase_order
                            oid = f"ORD-PO-{po_num}"
                            existing_oid = conn.execute(
                                "SELECT id FROM orders WHERE id=?", (oid,)
                            ).fetchone()
                            if existing_oid:
                                continue
                            conn.execute("""
                                INSERT OR IGNORE INTO orders
                                (id, po_number, agency, institution, total, status,
                                 buyer_name, buyer_email, created_at, updated_at, notes)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                            """, (
                                oid, po_num, "", po_d.get("institution", ""),
                                po_d.get("total_amount", 0),
                                po_d.get("status", "new"),
                                po_d.get("buyer_name", ""),
                                po_d.get("buyer_email", ""),
                                po_d.get("created_at", datetime.now().isoformat()),
                                datetime.now().isoformat(),
                                po_d.get("notes", ""),
                            ))
                            # Migrate po_line_items → order_line_items
                            po_lines = conn.execute(
                                "SELECT * FROM po_line_items WHERE po_id=?", (po_d.get("id", ""),)
                            ).fetchall()
                            for i, pl in enumerate(po_lines):
                                pl_d = dict(pl)
                                conn.execute("""
                                    INSERT INTO order_line_items
                                    (order_id, line_number, description, part_number,
                                     mfg_number, uom, qty_ordered, qty_backordered,
                                     unit_price, extended_price,
                                     sourcing_status, tracking_number, carrier,
                                     ship_date, delivery_date, notes,
                                     created_at, updated_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                """, (
                                    oid, i + 1,
                                    pl_d.get("description", ""),
                                    pl_d.get("item_number", ""),
                                    pl_d.get("mfg_number", ""),
                                    pl_d.get("uom", "EA"),
                                    pl_d.get("qty_ordered", 0),
                                    pl_d.get("qty_backordered", 0),
                                    pl_d.get("unit_price", 0),
                                    pl_d.get("extended_price", 0),
                                    pl_d.get("status", "pending"),
                                    pl_d.get("tracking_number", ""),
                                    pl_d.get("carrier", ""),
                                    pl_d.get("ship_date", ""),
                                    pl_d.get("delivery_date", ""),
                                    pl_d.get("notes", ""),
                                    po_d.get("created_at", datetime.now().isoformat()),
                                    datetime.now().isoformat(),
                                ))
                            merged += 1
                        conn.execute(
                            "INSERT OR IGNORE INTO migrations_applied (name, applied_at) VALUES (?, ?)",
                            ("orders_v2_merge_po", datetime.now().isoformat())
                        )
                        if merged:
                            fixes.append(f"V2 PO merge: imported {merged} purchase_orders into orders")
                        log.info("Orders V2 PO merge complete: %d POs imported", merged)
                    else:
                        # No purchase_orders table — mark as done
                        conn.execute(
                            "INSERT OR IGNORE INTO migrations_applied (name, applied_at) VALUES (?, ?)",
                            ("orders_v2_merge_po", datetime.now().isoformat())
                        )
                except Exception as po_e:
                    log.error("Orders V2 PO merge failed: %s", po_e, exc_info=True)

            if fixes:
                log.info("Boot data fixes: %s", "; ".join(fixes))

    except Exception as e:
        log.warning("_fix_data_on_boot: %s", e)


def _dedup_price_checks_on_boot():
    """Remove duplicate PCs (same pc_number+institution), keep newest. Recalculate quote counter."""
    import re as _re
    pc_path = os.path.join(DATA_DIR, "price_checks.json")
    if not os.path.exists(pc_path):
        return
    try:
        with open(pc_path) as f:
            pcs = json.load(f)
        if not pcs or not isinstance(pcs, dict):
            return

        # Group by (pc_number, institution) — keep OLDEST (original), remove newer dupes
        seen = {}  # key → (pcid, created_at)
        dupes_to_remove = []
        for pcid, pc in pcs.items():
            key = (pc.get("pc_number", "").strip(), pc.get("institution", "").strip().lower())
            if key == ("", "") or key == ("unknown", ""):
                continue
            created = pc.get("created_at", "")
            if key not in seen:
                seen[key] = (pcid, created)
            else:
                # Keep the OLDER one (original), remove the newer duplicate
                existing_id, existing_created = seen[key]
                if created < existing_created:
                    # Current is older → keep current, remove existing
                    dupes_to_remove.append(existing_id)
                    seen[key] = (pcid, created)
                else:
                    # Current is newer → it's the dupe
                    dupes_to_remove.append(pcid)

        if not dupes_to_remove:
            return

        # Collect quote numbers being freed
        freed_quotes = []
        for dup_id in dupes_to_remove:
            pc = pcs[dup_id]
            qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")
            if qn:
                freed_quotes.append(qn)
            pcs[dup_id]["status"] = "dismissed"  # Law 22: never delete

        with open(pc_path, "w") as f:
            json.dump(pcs, f, indent=2, default=str)
        log.info("Boot dedup: removed %d duplicate PCs: %s", len(dupes_to_remove), dupes_to_remove)

        # Remove freed draft quotes from quotes_log.json
        if freed_quotes:
            ql_path = os.path.join(DATA_DIR, "quotes_log.json")
            if os.path.exists(ql_path):
                try:
                    with open(ql_path) as f:
                        quotes = json.load(f)
                    before = len(quotes)
                    quotes = [q for q in quotes
                              if not (q.get("quote_number") in freed_quotes
                                      and q.get("status") in ("draft", "pending"))]
                    if len(quotes) < before:
                        with open(ql_path, "w") as f:
                            json.dump(quotes, f, indent=2, default=str)
                        log.info("Boot dedup: removed %d freed draft quotes: %s",
                                 before - len(quotes), freed_quotes)
                except Exception as e:
                    log.warning("Boot dedup quotes cleanup: %s", e)

        # Recalculate quote counter to highest remaining quote
        try:
            from src.forms.quote_generator import _load_counter, _save_counter
            max_seq = 0
            # Check remaining PCs
            for pc in pcs.values():
                qn = pc.get("reytech_quote_number", "") or ""
                m = _re.search(r'R\d{2}Q(\d+)', qn)
                if m:
                    max_seq = max(max_seq, int(m.group(1)))
            # Check remaining quotes
            ql_path = os.path.join(DATA_DIR, "quotes_log.json")
            if os.path.exists(ql_path):
                with open(ql_path) as f:
                    for q in json.load(f):
                        if q.get("is_test"):
                            continue
                        qn = q.get("quote_number", "")
                        m = _re.search(r'R\d{2}Q(\d+)', qn)
                        if m:
                            max_seq = max(max_seq, int(m.group(1)))
            old = _load_counter()
            if max_seq > 0 and max_seq < old.get("seq", 0):
                _save_counter({"year": old.get("year", 2026), "seq": max_seq})
                log.info("Boot dedup: counter reset %d → %d (next Q%d)", old["seq"], max_seq, max_seq + 1)
        except Exception as e:
            log.warning("Boot dedup counter fix: %s", e)

    except Exception as e:
        log.warning("Boot dedup failed (non-fatal): %s", e)


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
    conn = _make_connection()
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
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_vendor(v: dict) -> bool:
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection()
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
    ALLOWED_COLS = {'status', 'approved_at', 'sent_at', 'error', 'sent_by'}
    conn = _make_connection()
    now = datetime.now(timezone.utc).isoformat()
    updates = {'status': status}
    if status == 'approved':
        updates['approved_at'] = now
    elif status == 'sent':
        updates['sent_at'] = kwargs.get('sent_at', now)
    # Whitelist columns to prevent injection
    updates = {k: v for k, v in updates.items() if k in ALLOWED_COLS}
    sets = ', '.join(f"{k}=?" for k in updates)
    conn.execute("UPDATE email_outbox SET " + sets + " WHERE id=?",
                 list(updates.values()) + [email_id])
    conn.commit()
    conn.close()


# ── QA REPORTS ────────────────────────────────────────────────────────────────

def save_qa_report(report: dict) -> bool:
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM email_templates").fetchall()
    conn.close()
    return {r['id']: dict(r) for r in rows}


def upsert_email_template(tid: str, tmpl: dict) -> bool:
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM market_intelligence").fetchall()
    conn.close()
    return {r['section']: _jl(r['data']) for r in rows}


def upsert_market_intelligence(section: str, data) -> bool:
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection()
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
    conn = _make_connection(); conn.row_factory = sqlite3.Row
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
    conn = _make_connection()
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



# ── RFQs (rfq_store table removed in migration 16 — use rfqs table via data_layer) ──


def upsert_rfq(rfq: dict) -> bool:
    """Deprecated: rfq_store table dropped in migration 16. No-op."""
    log.warning("upsert_rfq called but rfq_store table is removed (migration 16)")
    return False


def get_all_rfqs(status: str = None) -> list:
    """Deprecated: rfq_store table dropped in migration 16. Returns empty list."""
    log.warning("get_all_rfqs called but rfq_store table is removed (migration 16)")
    return []


# ── APP SETTINGS (quote counter, etc.) ───────────────────────────────────────

def get_setting(key: str, default=None):
    """Read a single app_settings value using the thread-local connection pool."""
    with get_db() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    val = row[0]
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def set_setting(key: str, value) -> bool:
    """Write a single app_settings value using the thread-local connection pool."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO app_settings (key, value, updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (key, str(value), now))
        return True
    except Exception as e:
        log.error("set_setting: %s", e)
        return False


# ── LEADS ─────────────────────────────────────────────────────────────────────

def get_all_leads(status: str = None) -> list:
    conn = _make_connection()
    conn.row_factory = sqlite3.Row
    if status:
        rows = conn.execute(
            "SELECT * FROM leads WHERE status=? ORDER BY score DESC, created_at DESC", (status,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM leads ORDER BY score DESC, created_at DESC"
        ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['tags'] = _jl(d.get('tags'), [])
        d['activity'] = _jl(d.get('activity'), [])
        result.append(d)
    return result


def upsert_lead(lead: dict) -> bool:
    conn = _make_connection()
    now = datetime.now(timezone.utc).isoformat()
    lid = lead.get('id') or f"lead-{__import__('uuid').uuid4().hex[:8]}"
    try:
        conn.execute("""
            INSERT INTO leads
              (id, created_at, name, email, phone, company, agency, source,
               status, score, notes, tags, activity, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              status=excluded.status, score=excluded.score,
              notes=excluded.notes, tags=excluded.tags,
              activity=excluded.activity, updated_at=excluded.updated_at
        """, (lid, lead.get('created_at', now), lead.get('name', lead.get('buyer_name','')),
              lead.get('email', lead.get('buyer_email','')), lead.get('phone',''),
              lead.get('company',''), lead.get('agency',''), lead.get('source','manual'),
              lead.get('status','new'), float(lead.get('score',0) or 0),
              lead.get('notes',''), _jd(lead.get('tags',[])),
              _jd(lead.get('activity',[])), now))
        conn.commit()
        return True
    except Exception as e:
        log.error("upsert_lead: %s", e)
        return False
    finally:
        conn.close()


# ── EMAIL SENT LOG ────────────────────────────────────────────────────────────

def log_email_sent(email: dict) -> bool:
    conn = _make_connection()
    now = datetime.now(timezone.utc).isoformat()
    eid = email.get('id') or f"sent-{__import__('uuid').uuid4().hex[:10]}"
    try:
        conn.execute("""
            INSERT OR IGNORE INTO email_sent_log
              (id, sent_at, to_address, subject, body, type, ref_id, success, error)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (eid, email.get('sent_at', now), email.get('to', email.get('to_address','')),
              email.get('subject',''), email.get('body',''), email.get('type',''),
              email.get('ref_id',''), 1 if email.get('success', True) else 0,
              email.get('error','')))
        conn.commit()
        return True
    except Exception as e:
        log.error("log_email_sent: %s", e)
        return False
    finally:
        conn.close()


def get_email_sent_log(limit: int = 100) -> list:
    conn = _make_connection()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM email_sent_log ORDER BY sent_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── WORKFLOW RUNS ─────────────────────────────────────────────────────────────

def log_workflow_run(run: dict) -> str:
    conn = _make_connection()
    now = datetime.now(timezone.utc).isoformat()
    rid = run.get('id') or f"run-{__import__('uuid').uuid4().hex[:10]}"
    try:
        conn.execute("""
            INSERT OR REPLACE INTO workflow_runs
              (id, started_at, finished_at, type, status, input, output, error, duration_ms)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (rid, run.get('started_at', now), run.get('finished_at'),
              run.get('type',''), run.get('status','running'),
              _jd(run.get('input',{})), _jd(run.get('output',{})),
              run.get('error',''), run.get('duration_ms')))
        conn.commit()
    except Exception as e:
        log.error("log_workflow_run: %s", e)
    finally:
        conn.close()
    return rid


# ── SENT DOCUMENTS ──────────────────────────────────────────────────────────

def create_sent_document(pc_id: str, filepath: str, items: list = None,
                         header: dict = None, notes: str = "", 
                         created_by: str = "user") -> int:
    """Create a sent document version. Returns the new document ID."""
    conn = _make_connection()
    now = datetime.now(timezone.utc).isoformat()
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM sent_documents WHERE pc_id = ?",
            (pc_id,)
        ).fetchone()
        next_version = (row[0] if row else 0) + 1
        conn.execute(
            "UPDATE sent_documents SET status = 'superseded' WHERE pc_id = ? AND status = 'current'",
            (pc_id,)
        )
        file_size = 0
        try:
            file_size = __import__('os').path.getsize(filepath)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        filename = __import__('os').path.basename(filepath)
        change_summary = ""
        if next_version > 1:
            prev = conn.execute(
                "SELECT items_json FROM sent_documents WHERE pc_id = ? AND version = ?",
                (pc_id, next_version - 1)
            ).fetchone()
            if prev and prev[0] and items:
                try:
                    old_items = __import__('json').loads(prev[0])
                    changes = []
                    for i, (new, old) in enumerate(zip(items, old_items)):
                        if new.get("unit_price") != old.get("unit_price"):
                            changes.append(f"Item {i+1}: price {old.get('unit_price')} -> {new.get('unit_price')}")
                        if new.get("description","")[:30] != old.get("description","")[:30]:
                            changes.append(f"Item {i+1}: desc changed")
                        if new.get("qty") != old.get("qty"):
                            changes.append(f"Item {i+1}: qty {old.get('qty')} -> {new.get('qty')}")
                    if len(items) != len(old_items):
                        changes.append(f"Items: {len(old_items)} -> {len(items)}")
                    change_summary = "; ".join(changes) if changes else "Minor edits"
                except Exception:
                    change_summary = "Updated"
        conn.execute("""
            INSERT INTO sent_documents
              (pc_id, version, created_at, filename, filepath, file_size, status,
               notes, created_by, items_json, header_json, change_summary)
            VALUES (?, ?, ?, ?, ?, ?, 'current', ?, ?, ?, ?, ?)
        """, (pc_id, next_version, now, filename, filepath, file_size,
              notes, created_by, _jd(items or []), _jd(header or {}),
              change_summary))
        conn.commit()
        doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        log.info("sent_document created: pc=%s v%d id=%d file=%s", 
                 pc_id, next_version, doc_id, filename)
        return doc_id
    except Exception as e:
        log.error("create_sent_document %s: %s", pc_id, e)
        return 0
    finally:
        conn.close()


def get_sent_documents(pc_id: str) -> list:
    """Get all document versions for a PC, newest first."""
    conn = _make_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM sent_documents WHERE pc_id = ? ORDER BY version DESC",
            (pc_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_sent_documents %s: %s", pc_id, e)
        return []
    finally:
        conn.close()


def get_sent_document(doc_id: int) -> dict:
    """Get a single sent document by ID."""
    conn = _make_connection()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM sent_documents WHERE id = ?", (doc_id,)).fetchone()
        if row:
            d = dict(row)
            d["items"] = _jl(d.get("items_json"), [])
            d["header"] = _jl(d.get("header_json"), {})
            return d
        return {}
    except Exception as e:
        log.error("get_sent_document %d: %s", doc_id, e)
        return {}
    finally:
        conn.close()


# ── API Key Management ───────────────────────────────────────────────────────

def generate_api_key(name: str, created_by: str = "system") -> str:
    """Generate a new API key. Returns the raw key (only shown once)."""
    import hashlib, secrets
    raw_key = f"reytech_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO api_keys (key_hash, name, created_by) VALUES (?, ?, ?)",
            (key_hash, name, created_by))
    log.info("API key created: name=%s by=%s", name, created_by)
    return raw_key


def validate_api_key(raw_key: str) -> dict | None:
    """Validate an API key. Returns key info dict or None if invalid."""
    import hashlib
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, name, is_active, scopes FROM api_keys WHERE key_hash = ?",
            (key_hash,)).fetchone()
        if row and row["is_active"]:
            conn.execute(
                "UPDATE api_keys SET last_used = datetime('now') WHERE id = ?",
                (row["id"],))
            return {"id": row["id"], "name": row["name"],
                    "scopes": json.loads(row["scopes"] or '["read","write"]')}
    return None


def list_api_keys() -> list:
    """List all API keys (without hashes)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, created_at, last_used, is_active, created_by, scopes "
            "FROM api_keys ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


def revoke_api_key(key_id: int) -> bool:
    """Revoke an API key by ID."""
    with get_db() as conn:
        conn.execute("UPDATE api_keys SET is_active = 0 WHERE id = ?", (key_id,))
    log.info("API key revoked: id=%d", key_id)
    return True
