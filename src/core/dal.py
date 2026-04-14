"""
src/core/dal.py — Data Access Layer (Single Source of Truth)

WHY THIS EXISTS:
  Before this file, leads/customers/vendors/outbox lived in JSON files
  with some also dual-written to SQLite. JSON files are lost on every
  Railway redeploy. This DAL makes SQLite the single source of truth
  and provides clean CRUD functions for all callers.

MIGRATION PATH:
  1. On boot, _migrate_json_to_db() imports any JSON records not yet in DB
  2. All reads come from DB
  3. All writes go to DB only (no more JSON writes)
  4. JSON files kept as read-only backup/import source

USAGE:
  from src.core.dal import get_all_leads, upsert_lead, get_lead
  leads = get_all_leads()
  upsert_lead({"id": "abc", "agency": "CDCR", ...})
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger("dal")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    from contextlib import contextmanager
    import threading
    _lock = threading.Lock()
    @contextmanager
    def get_db():
        with _lock:
            conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=30)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()

def _nulls_to_empty(d: dict) -> dict:
    """Convert None values to empty strings for JSON compat."""
    return {k: (v if v is not None else "") for k, v in d.items()}


_now = lambda: datetime.now(timezone.utc).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# LEADS
# ══════════════════════════════════════════════════════════════════════════════

_LEAD_COLS = [
    "id", "created_at", "updated_at", "status", "agency", "institution",
    "buyer_name", "buyer_email", "buyer_phone", "category", "score",
    "score_breakdown", "score_history", "score_updated_at",
    "po_number", "po_value", "po_date", "due_date",
    "items_count", "match_type", "matched_items",
    "our_historical_price", "scprs_listed_price", "estimated_savings_pct",
    "outreach_draft", "outreach_sent_at", "response_received_at",
    "notes", "source", "nurture_active", "nurture_sequence", "nurture_steps",
    "nurture_started_at", "nurture_paused_at", "nurture_pause_reason",
    "converted_at", "converted_contact_id", "extra_json",
]

# JSON-serializable fields (stored as TEXT in DB)
_LEAD_JSON_FIELDS = {"score_breakdown", "score_history", "matched_items",
                     "outreach_draft", "nurture_steps", "extra_json"}


def get_all_leads(status: str = None) -> list:
    """Get all leads, optionally filtered by status."""
    try:
        with get_db() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM leads WHERE status = ? ORDER BY score DESC", (status,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM leads ORDER BY score DESC"
                ).fetchall()
            return [_lead_from_row(r) for r in rows]
    except Exception as e:
        log.error("get_all_leads: %s", e)
        return _fallback_load_json("leads.json", [])


def get_lead(lead_id: str) -> dict:
    """Get a single lead by ID."""
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
            return _lead_from_row(row) if row else {}
    except Exception:
        return {}


def upsert_lead(lead: dict) -> bool:
    """Insert or update a lead."""
    if not lead.get("id"):
        return False
    lead.setdefault("created_at", _now())
    lead["updated_at"] = _now()
    try:
        with get_db() as conn:
            _upsert_row(conn, "leads", "id", lead, _LEAD_COLS, _LEAD_JSON_FIELDS)
        return True
    except Exception as e:
        log.error("upsert_lead: %s", e)
        return False


def save_all_leads(leads: list) -> int:
    """Bulk save leads (used by lead_gen, nurture, etc.)."""
    saved = 0
    try:
        with get_db() as conn:
            for lead in leads:
                if not lead.get("id"):
                    continue
                lead.setdefault("created_at", _now())
                lead["updated_at"] = _now()
                _upsert_row(conn, "leads", "id", lead, _LEAD_COLS, _LEAD_JSON_FIELDS)
                saved += 1
    except Exception as e:
        log.error("save_all_leads: %s", e)
    return saved


def delete_lead(lead_id: str) -> bool:
    """Delete a lead."""
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
        return True
    except Exception:
        return False


def _lead_from_row(row) -> dict:
    """Convert DB row to dict, deserializing JSON fields."""
    if not row:
        return {}
    d = _nulls_to_empty(dict(row))
    # Defaults for JSON fields (must be new instance per row — mutable!)
    _json_defaults = {
        "score_breakdown": dict, "score_history": list, "matched_items": list,
        "outreach_draft": dict, "nurture_steps": list, "extra_json": dict,
    }
    for f in _LEAD_JSON_FIELDS:
        val = d.get(f)
        if val and isinstance(val, str):
            try:
                d[f] = json.loads(val)
            except Exception:
                d[f] = _json_defaults.get(f, dict)()
        elif not val or val == "":
            d[f] = _json_defaults.get(f, dict)()
    # Compat: nurture_active as bool
    d["nurture_active"] = bool(d.get("nurture_active", 0))
    return d


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ══════════════════════════════════════════════════════════════════════════════

_CUST_COLS = [
    "id", "qb_name", "display_name", "company", "parent", "agency",
    "address", "city", "state", "zip", "phone", "email",
    "open_balance", "source", "is_parent_org",
    "bill_to", "bill_to_city", "bill_to_state", "bill_to_zip",
    "abbreviation", "child_count", "created_at", "updated_at", "extra_json",
]

_CUST_JSON_FIELDS = {"extra_json"}


def get_all_customers() -> list:
    """Get all customers."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM customers ORDER BY display_name"
            ).fetchall()
            return [_nulls_to_empty(dict(r)) for r in rows]
    except Exception as e:
        log.error("get_all_customers: %s", e)
        return _fallback_load_json("customers.json", [])


def get_customer(customer_id: int = None, qb_name: str = None, email: str = None) -> dict:
    """Get a customer by ID, QB name, or email."""
    try:
        with get_db() as conn:
            if customer_id:
                row = conn.execute("SELECT * FROM customers WHERE id = ?", (customer_id,)).fetchone()
            elif qb_name:
                row = conn.execute("SELECT * FROM customers WHERE qb_name = ?", (qb_name,)).fetchone()
            elif email:
                row = conn.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
            else:
                return {}
            return _nulls_to_empty(dict(row)) if row else {}
    except Exception:
        return {}


def upsert_customer(cust: dict) -> int:
    """Insert or update a customer. Returns the row ID."""
    cust["updated_at"] = _now()
    cust.setdefault("created_at", _now())
    try:
        with get_db() as conn:
            # Use qb_name as natural key for dedup
            qb_name = cust.get("qb_name") or cust.get("display_name", "")
            if qb_name:
                existing = conn.execute(
                    "SELECT id FROM customers WHERE qb_name = ?", (qb_name,)
                ).fetchone()
                if existing:
                    cust["id"] = existing["id"]

            if cust.get("id"):
                _update_row(conn, "customers", "id", cust, _CUST_COLS)
                return cust["id"]
            else:
                cols = [c for c in _CUST_COLS if c != "id" and c in cust]
                vals = [_serialize(cust.get(c), c in _CUST_JSON_FIELDS) for c in cols]
                placeholders = ",".join("?" for _ in cols)
                cur = conn.execute(
                    f"INSERT INTO customers ({','.join(cols)}) VALUES ({placeholders})", vals
                )
                return cur.lastrowid
    except Exception as e:
        log.error("upsert_customer: %s", e)
        return 0


def save_all_customers(customers: list) -> int:
    """Bulk save customers."""
    saved = 0
    for c in customers:
        if upsert_customer(c):
            saved += 1
    return saved


# ══════════════════════════════════════════════════════════════════════════════
# VENDORS
# ══════════════════════════════════════════════════════════════════════════════

_VENDOR_COLS = [
    "id", "name", "company", "address", "city", "state", "zip",
    "phone", "email", "website", "source", "open_balance",
    "price_score", "reliability_score", "speed_score", "breadth_score",
    "overall_score", "scored_at", "categories_served",
    "gsa_contract", "notes", "created_at", "updated_at", "extra_json",
]

_VENDOR_JSON_FIELDS = {"categories_served", "extra_json"}


def get_all_vendors() -> list:
    """Get all vendors."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM vendors ORDER BY name"
            ).fetchall()
            return [_vendor_from_row(r) for r in rows]
    except Exception as e:
        log.error("get_all_vendors: %s", e)
        return _fallback_load_json("vendors.json", [])


def get_vendor(vendor_id: int = None, name: str = None) -> dict:
    """Get a vendor by ID or name."""
    try:
        with get_db() as conn:
            if vendor_id:
                row = conn.execute("SELECT * FROM vendors WHERE id = ?", (vendor_id,)).fetchone()
            elif name:
                row = conn.execute("SELECT * FROM vendors WHERE name = ? OR company = ?",
                                   (name, name)).fetchone()
            else:
                return {}
            return _vendor_from_row(row) if row else {}
    except Exception:
        return {}


def upsert_vendor(vendor: dict) -> int:
    """Insert or update a vendor. Returns the row ID."""
    vendor["updated_at"] = _now()
    vendor.setdefault("created_at", _now())
    try:
        with get_db() as conn:
            name = vendor.get("name") or vendor.get("company", "")
            if name:
                existing = conn.execute(
                    "SELECT id FROM vendors WHERE name = ?", (name,)
                ).fetchone()
                if existing:
                    vendor["id"] = existing["id"]

            if vendor.get("id"):
                _update_row(conn, "vendors", "id", vendor, _VENDOR_COLS)
                return vendor["id"]
            else:
                cols = [c for c in _VENDOR_COLS if c != "id" and c in vendor]
                vals = [_serialize(vendor.get(c), c in _VENDOR_JSON_FIELDS) for c in cols]
                placeholders = ",".join("?" for _ in cols)
                cur = conn.execute(
                    f"INSERT INTO vendors ({','.join(cols)}) VALUES ({placeholders})", vals
                )
                return cur.lastrowid
    except Exception as e:
        log.error("upsert_vendor: %s", e)
        return 0


def save_all_vendors(vendors: list) -> int:
    """Bulk save vendors."""
    saved = 0
    for v in vendors:
        if upsert_vendor(v):
            saved += 1
    return saved


def _vendor_from_row(row) -> dict:
    if not row:
        return {}
    d = _nulls_to_empty(dict(row))
    for f in _VENDOR_JSON_FIELDS:
        if d.get(f) and isinstance(d[f], str):
            try:
                d[f] = json.loads(d[f])
            except Exception as _e:
                log.debug("suppressed: %s", _e)
    return d


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL OUTBOX (DB-only — replaces JSON dual-write)
# ══════════════════════════════════════════════════════════════════════════════

def get_outbox(status: str = None, limit: int = 500) -> list:
    """Get outbox entries from DB."""
    try:
        with get_db() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM email_outbox WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM email_outbox ORDER BY created_at DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                # Parse metadata JSON
                for f in ("metadata", "entities"):
                    if d.get(f) and isinstance(d[f], str):
                        try:
                            d[f] = json.loads(d[f])
                        except Exception as _e:
                            log.debug("suppressed: %s", _e)
                # Backwards compat: email_outreach uses "to", DB uses "to_address"
                if "to_address" in d and "to" not in d:
                    d["to"] = d["to_address"]
                result.append(d)
            return result
    except Exception as e:
        log.error("get_outbox: %s", e)
        return []


def upsert_outbox_email(email: dict) -> bool:
    """Insert or update an outbox email."""
    if not email.get("id"):
        return False
    email.setdefault("created_at", _now())
    try:
        with get_db() as conn:
            # Serialize JSON fields
            metadata = email.get("metadata", {})
            if isinstance(metadata, dict):
                metadata = json.dumps(metadata, default=str)
            entities = email.get("entities", {})
            if isinstance(entities, dict):
                entities = json.dumps(entities, default=str)

            conn.execute("""
                INSERT INTO email_outbox
                (id, created_at, status, type, to_address, subject, body,
                 intent, entities, approved_at, sent_at, metadata,
                 retry_count, last_error, retry_at, open_count, click_count,
                 last_opened, last_clicked, tracking_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  status=excluded.status, to_address=excluded.to_address,
                  subject=excluded.subject, body=excluded.body,
                  intent=excluded.intent, entities=excluded.entities,
                  approved_at=excluded.approved_at, sent_at=excluded.sent_at,
                  metadata=excluded.metadata, retry_count=excluded.retry_count,
                  last_error=excluded.last_error, retry_at=excluded.retry_at,
                  open_count=excluded.open_count, click_count=excluded.click_count,
                  last_opened=excluded.last_opened, last_clicked=excluded.last_clicked,
                  tracking_id=excluded.tracking_id
            """, (
                email.get("id"), email.get("created_at"), email.get("status", "draft"),
                email.get("type", ""), email.get("to_address") or email.get("to", ""),
                email.get("subject", ""), email.get("body", ""),
                email.get("intent", ""), entities, email.get("approved_at", ""),
                email.get("sent_at", ""), metadata,
                email.get("retry_count", 0), email.get("last_error", ""),
                email.get("retry_at", ""), email.get("open_count", 0),
                email.get("click_count", 0), email.get("last_opened", ""),
                email.get("last_clicked", ""), email.get("tracking_id", ""),
            ))
        return True
    except Exception as e:
        log.error("upsert_outbox_email: %s", e)
        return False


def delete_outbox_email(email_id: str) -> bool:
    """Delete from outbox."""
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM email_outbox WHERE id = ?", (email_id,))
        return True
    except Exception:
        return False


def update_outbox_status(email_id: str, status: str, **extra) -> bool:
    """Update just the status (+ optional extra fields)."""
    try:
        with get_db() as conn:
            sets = ["status = ?"]
            vals = [status]
            for k, v in extra.items():
                sets.append(f"{k} = ?")
                vals.append(v)
            vals.append(email_id)
            conn.execute(
                f"UPDATE email_outbox SET {', '.join(sets)} WHERE id = ?", vals
            )
        return True
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# BOOT-TIME JSON → DB MIGRATION
# ══════════════════════════════════════════════════════════════════════════════

def migrate_json_to_db() -> dict:
    """Import JSON files into DB tables. Safe to run multiple times (deduplicates)."""
    results = {}

    # ── Leads ──
    try:
        path = os.path.join(DATA_DIR, "leads.json")
        if os.path.exists(path):
            with open(path) as f:
                leads = json.load(f)
            if isinstance(leads, list):
                imported = 0
                with get_db() as conn:
                    for lead in leads:
                        if not lead.get("id"):
                            continue
                        existing = conn.execute(
                            "SELECT id FROM leads WHERE id = ?", (lead["id"],)
                        ).fetchone()
                        if not existing:
                            _upsert_row(conn, "leads", "id", lead, _LEAD_COLS, _LEAD_JSON_FIELDS)
                            imported += 1
                results["leads"] = {"total": len(leads), "imported": imported}
                if imported:
                    log.info("Migrated %d leads from JSON → DB", imported)
    except Exception as e:
        results["leads"] = {"error": str(e)}

    # ── Customers ──
    try:
        path = os.path.join(DATA_DIR, "customers.json")
        if os.path.exists(path):
            with open(path) as f:
                customers = json.load(f)
            if isinstance(customers, list):
                imported = 0
                with get_db() as conn:
                    for cust in customers:
                        name = cust.get("qb_name") or cust.get("display_name", "")
                        if not name:
                            continue
                        existing = conn.execute(
                            "SELECT id FROM customers WHERE qb_name = ?", (name,)
                        ).fetchone()
                        if not existing:
                            cols = [c for c in _CUST_COLS if c != "id" and c in cust]
                            vals = [_serialize(cust.get(c), c in _CUST_JSON_FIELDS) for c in cols]
                            if cols:
                                conn.execute(
                                    f"INSERT INTO customers ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
                                    vals
                                )
                                imported += 1
                results["customers"] = {"total": len(customers), "imported": imported}
                if imported:
                    log.info("Migrated %d customers from JSON → DB", imported)
    except Exception as e:
        results["customers"] = {"error": str(e)}

    # ── Vendors ──
    try:
        path = os.path.join(DATA_DIR, "vendors.json")
        if os.path.exists(path):
            with open(path) as f:
                vendors = json.load(f)
            if isinstance(vendors, list):
                imported = 0
                with get_db() as conn:
                    for v in vendors:
                        name = v.get("name") or v.get("company", "")
                        if not name:
                            continue
                        existing = conn.execute(
                            "SELECT id FROM vendors WHERE name = ?", (name,)
                        ).fetchone()
                        if not existing:
                            v.setdefault("name", name)
                            cols = [c for c in _VENDOR_COLS if c != "id" and c in v]
                            vals = [_serialize(v.get(c), c in _VENDOR_JSON_FIELDS) for c in cols]
                            if cols:
                                conn.execute(
                                    f"INSERT INTO vendors ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
                                    vals
                                )
                                imported += 1
                results["vendors"] = {"total": len(vendors), "imported": imported}
                if imported:
                    log.info("Migrated %d vendors from JSON → DB", imported)
    except Exception as e:
        results["vendors"] = {"error": str(e)}

    # ── Email Outbox ──
    try:
        path = os.path.join(DATA_DIR, "email_outbox.json")
        if os.path.exists(path):
            with open(path) as f:
                outbox = json.load(f)
            if isinstance(outbox, list):
                imported = 0
                with get_db() as conn:
                    for email in outbox:
                        eid = email.get("id")
                        if not eid:
                            continue
                        existing = conn.execute(
                            "SELECT id FROM email_outbox WHERE id = ?", (eid,)
                        ).fetchone()
                        if not existing:
                            # Inline insert (avoid nested get_db lock)
                            metadata = email.get("metadata", {})
                            if isinstance(metadata, dict):
                                metadata = json.dumps(metadata, default=str)
                            entities = email.get("entities", {})
                            if isinstance(entities, dict):
                                entities = json.dumps(entities, default=str)
                            conn.execute("""
                                INSERT OR IGNORE INTO email_outbox
                                (id, created_at, status, type, to_address, subject, body,
                                 intent, entities, approved_at, sent_at, metadata)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                            """, (
                                eid, email.get("created_at", ""), email.get("status", "draft"),
                                email.get("type", ""),
                                email.get("to_address") or email.get("to", ""),
                                email.get("subject", ""), email.get("body", ""),
                                email.get("intent", ""), entities,
                                email.get("approved_at", ""), email.get("sent_at", ""),
                                metadata,
                            ))
                            imported += 1
                results["email_outbox"] = {"total": len(outbox), "imported": imported}
                if imported:
                    log.info("Migrated %d outbox emails from JSON → DB", imported)
    except Exception as e:
        results["email_outbox"] = {"error": str(e)}

    log.info("JSON→DB migration complete: %s", results)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _serialize(value, is_json_field: bool = False):
    """Serialize a value for DB storage."""
    if value is None:
        return None
    if is_json_field and isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, bool):
        return int(value)
    return value


def _upsert_row(conn, table: str, pk: str, data: dict, columns: list, json_fields: set = None):
    """Generic upsert using INSERT OR REPLACE."""
    json_fields = json_fields or set()
    cols = [c for c in columns if c in data]
    vals = [_serialize(data.get(c), c in json_fields) for c in cols]
    if not cols:
        return
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols if c != pk)
    conn.execute(
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT({pk}) DO UPDATE SET {updates}",
        vals
    )


def _update_row(conn, table: str, pk: str, data: dict, columns: list):
    """Update an existing row. Table and column names are validated against the schema."""
    import re as _re_upd
    # Validate table name against known tables (prevents injection via table name)
    _ALLOWED_TABLES = {
        "leads", "customers", "vendors", "outbox", "email_outbox",
        "product_catalog", "price_history", "contacts", "quotes",
        "orders", "rfqs", "price_checks", "activity_log", "po_line_items",
    }
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table}")
    # Validate column names — only alphanumeric + underscore allowed
    for c in columns:
        if not _re_upd.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', c):
            raise ValueError(f"Invalid column name: {c!r}")
    cols = [c for c in columns if c in data and c != pk]
    if not cols:
        return
    sets = ",".join(f"{c}=?" for c in cols)
    vals = [data.get(c) for c in cols]
    vals.append(data[pk])
    conn.execute("UPDATE " + table + " SET " + sets + " WHERE " + pk + "=?", vals)


def _fallback_load_json(filename: str, default):
    """Emergency fallback if DB is unavailable."""
    try:
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return default


def _safe_json(raw, default=None):
    """Parse a JSON string safely, returning default on failure."""
    if raw is None:
        return default if default is not None else None
    if isinstance(raw, (list, dict)):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else raw


def _get_actor() -> str:
    """Resolve the current actor for audit trail entries."""
    try:
        from flask import g, session, has_request_context
        if has_request_context():
            if hasattr(g, 'api_auth') and g.api_auth:
                return 'api_key'
            return session.get('user', 'web')
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return 'system'


def _audit(entity_type: str, entity_id: str, action: str, actor: str = None,
           old_value: str = None, new_value: str = None):
    """Log an entity change to the audit trail. Never raises."""
    if actor is None:
        actor = _get_actor()
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO audit_trail
                (item_description, rfq_id, field_changed, source, actor, old_value, new_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (entity_type, entity_id, action, actor, actor,
                  (old_value or "")[:2000], (new_value or "")[:2000]))
    except Exception as e:
        log.warning("_audit(%s, %s, %s) failed: %s", entity_type, entity_id, action, e)


def _snapshot_before_update(entity_type: str, entity_id: str, get_fn):
    """Take a snapshot before overwriting an existing record. Never raises."""
    try:
        existing = get_fn(entity_id)
        if existing:
            from src.core.snapshots import create_snapshot, init_snapshots
            init_snapshots()  # Ensure table exists (idempotent)
            create_snapshot("dal", entity_type, existing, run_id=entity_id,
                           notes=f"pre-update snapshot for {entity_type} {entity_id}")
    except Exception as e:
        log.warning("_snapshot_before_update(%s, %s) failed: %s", entity_type, entity_id, e)


# ═══════════════════════════════════════════════════════════════════════════════
# RFQ Entity
# ═══════════════════════════════════════════════════════════════════════════════

def get_rfq(rfq_id: str) -> dict | None:
    """Get a single RFQ by ID.
    Input: rfq_id (str)
    Output: dict with all RFQ fields + parsed items, or None if not found.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM rfqs WHERE id = ?", (rfq_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            d["items"] = _safe_json(d.get("items"), [])
            return d
    except Exception as e:
        log.error("get_rfq(%s) failed: %s", rfq_id, e, exc_info=True)
        raise


def list_rfqs(status: str = None, limit: int = 500) -> list[dict]:
    """List RFQs, optionally filtered by status.
    Input: status (optional str), limit (int, default 500)
    Output: list of RFQ dicts sorted by received_at desc.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM rfqs WHERE status = ? ORDER BY received_at DESC LIMIT ?",
                    (status, limit)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rfqs ORDER BY received_at DESC LIMIT ?",
                    (limit,)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["items"] = _safe_json(d.get("items"), [])
                result.append(d)
            return result
    except Exception as e:
        log.error("list_rfqs(status=%s) failed: %s", status, e, exc_info=True)
        raise


def save_rfq(rfq: dict, actor: str = "system") -> bool:
    """Insert or update an RFQ record.
    Input: rfq dict (must have 'id'), actor (str for audit trail)
    Output: True on success.
    Side effects: Writes to rfqs table.
    """
    rfq_id = rfq.get("id")
    if not rfq_id:
        raise ValueError("RFQ must have an 'id' field")
    try:
        with get_db() as conn:
            _existing = conn.execute("SELECT id FROM rfqs WHERE id=?", (rfq_id,)).fetchone()
            if _existing:
                _snapshot_before_update("rfq", rfq_id, get_rfq)
            conn.execute("""
                INSERT INTO rfqs (id, received_at, agency, institution, requestor_name,
                    requestor_email, rfq_number, items, status, source, email_uid, notes, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    agency=excluded.agency, institution=excluded.institution,
                    requestor_name=excluded.requestor_name, requestor_email=excluded.requestor_email,
                    rfq_number=excluded.rfq_number, items=excluded.items,
                    status=excluded.status, source=excluded.source,
                    email_uid=excluded.email_uid, notes=excluded.notes,
                    updated_at=excluded.updated_at
            """, (rfq_id, rfq.get("received_at", ""), rfq.get("agency", ""),
                  rfq.get("institution", ""), rfq.get("requestor_name", ""),
                  rfq.get("requestor_email", ""), rfq.get("rfq_number", ""),
                  json.dumps(rfq.get("items", []), default=str),
                  rfq.get("status", "new"), rfq.get("source", ""),
                  rfq.get("email_uid", ""), rfq.get("notes", "")))
        # Audit trail
        try:
            _audit("rfq", rfq_id, "create" if not _existing else "update", actor,
                   new_value=json.dumps(rfq, default=str)[:2000])
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        # Fire webhook for new RFQ creation
        try:
            from src.core.webhooks import fire_webhook
            fire_webhook("rfq.created", {
                "rfq_id": rfq_id,
                "solicitation_number": rfq.get("solicitation_number", ""),
                "agency": rfq.get("agency", ""),
                "item_count": len(rfq.get("items", [])),
            })
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("save_rfq(%s) failed: %s", rfq_id, e, exc_info=True)
        raise


def update_rfq_status(rfq_id: str, status: str, actor: str = "system") -> bool:
    """Update only the status field of an RFQ.
    Input: rfq_id, new status string, actor for audit
    Output: True on success.
    Side effects: Writes to rfqs table.
    """
    try:
        with get_db() as conn:
            old = conn.execute("SELECT status FROM rfqs WHERE id=?", (rfq_id,)).fetchone()
            old_status = old["status"] if old else ""
            conn.execute(
                "UPDATE rfqs SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (status, rfq_id))
        # Audit trail
        try:
            _audit("rfq", rfq_id, "status_change", actor,
                   old_value=old_status, new_value=status)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        # Fire webhook for status change
        try:
            from src.core.webhooks import fire_webhook
            fire_webhook("rfq.status_changed", {
                "rfq_id": rfq_id, "new_status": status, "actor": actor,
            })
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("update_rfq_status(%s, %s) failed: %s", rfq_id, status, e, exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# PriceCheck Entity
# ═══════════════════════════════════════════════════════════════════════════════

def get_pc(pc_id: str) -> dict | None:
    """Get a single price check by ID.
    Input: pc_id (str)
    Output: dict with all PC fields + parsed items, or None if not found.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM price_checks WHERE id = ?", (pc_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            d["items"] = _safe_json(d.get("items"), [])
            # Unpack pc_data blob into top-level dict
            pc_blob = d.get("pc_data", "")
            if pc_blob and isinstance(pc_blob, str):
                try:
                    import json as _json
                    pc_data = _json.loads(pc_blob)
                    if isinstance(pc_data, dict):
                        for k, v in pc_data.items():
                            if k == "items":
                                # pc_data blob has richer item data (pricing, notes, links)
                                # than the separate items column. Use the richer version.
                                blob_items = v if isinstance(v, list) else _safe_json(v, [])
                                col_items = d.get("items", [])
                                if isinstance(col_items, list) and isinstance(blob_items, list):
                                    blob_richness = len(blob_items[0].keys()) if blob_items and isinstance(blob_items[0], dict) else 0
                                    col_richness = len(col_items[0].keys()) if col_items and isinstance(col_items[0], dict) else 0
                                    if blob_richness > col_richness:
                                        d["items"] = blob_items
                                elif blob_items:
                                    d["items"] = blob_items
                            elif k not in d or not d[k]:
                                d[k] = v
                except (json.JSONDecodeError, TypeError) as _e:
                    log.debug("suppressed: %s", _e)
            return d
    except Exception as e:
        log.error("get_pc(%s) failed: %s", pc_id, e, exc_info=True)
        raise


def list_pcs(status: str = None, limit: int = 500) -> list[dict]:
    """List price checks, optionally filtered by status.
    Input: status (optional str), limit (int, default 500)
    Output: list of PC dicts sorted by created_at desc.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM price_checks WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM price_checks ORDER BY created_at DESC LIMIT ?",
                    (limit,)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["items"] = _safe_json(d.get("items"), [])
                # Unpack pc_data blob into top-level dict
                pc_blob = d.get("pc_data", "")
                if pc_blob and isinstance(pc_blob, str):
                    try:
                        import json as _json
                        pc_data = _json.loads(pc_blob)
                        if isinstance(pc_data, dict):
                            for k, v in pc_data.items():
                                if k not in d or not d[k]:
                                    d[k] = v
                    except (json.JSONDecodeError, TypeError) as _e:
                        log.debug("suppressed: %s", _e)
                result.append(d)
            return result
    except Exception as e:
        log.error("list_pcs(status=%s) failed: %s", status, e, exc_info=True)
        raise


def save_pc(pc: dict, actor: str = "system") -> bool:
    """Insert or update a price check record.
    Input: pc dict (must have 'id'), actor for audit
    Output: True on success.
    Side effects: Writes to price_checks table.
    """
    pc_id = pc.get("id")
    if not pc_id:
        raise ValueError("PC must have an 'id' field")
    try:
        with get_db() as conn:
            _existing = conn.execute("SELECT id FROM price_checks WHERE id=?", (pc_id,)).fetchone()
            if _existing:
                _snapshot_before_update("price_check", pc_id, get_pc)
            conn.execute("""
                INSERT INTO price_checks (id, created_at, requestor, agency, institution,
                    items, source_file, quote_number, pc_number, total_items, status,
                    email_uid, email_subject, due_date, pc_data, ship_to)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    requestor=excluded.requestor, agency=excluded.agency,
                    institution=excluded.institution, items=excluded.items,
                    status=excluded.status, quote_number=excluded.quote_number,
                    pc_number=excluded.pc_number, total_items=excluded.total_items,
                    email_uid=excluded.email_uid, email_subject=excluded.email_subject,
                    due_date=excluded.due_date, pc_data=excluded.pc_data,
                    ship_to=excluded.ship_to
            """, (pc_id, pc.get("created_at", ""), pc.get("requestor", ""),
                  pc.get("agency", ""), pc.get("institution", ""),
                  json.dumps(pc.get("items", []), default=str),
                  pc.get("source_file", ""), pc.get("quote_number", ""),
                  pc.get("pc_number", ""), len(pc.get("items", [])),
                  pc.get("status", "parsed"),
                  pc.get("email_uid", ""), pc.get("email_subject", ""),
                  pc.get("due_date", ""), pc.get("pc_data", "{}"),
                  pc.get("ship_to", "")))
        # Audit trail
        try:
            _audit("price_check", pc_id, "create" if not _existing else "update", actor,
                   new_value=json.dumps(pc, default=str)[:2000])
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("save_pc(%s) failed: %s", pc_id, e, exc_info=True)
        raise


def update_pc_status(pc_id: str, status: str, actor: str = "system") -> bool:
    """Update only the status field of a price check.
    Input: pc_id, new status string, actor for audit
    Output: True on success.
    Side effects: Writes to price_checks table.
    """
    try:
        with get_db() as conn:
            old = conn.execute("SELECT status FROM price_checks WHERE id=?", (pc_id,)).fetchone()
            old_status = old["status"] if old else ""
            conn.execute(
                "UPDATE price_checks SET status = ? WHERE id = ?",
                (status, pc_id))
        # Audit trail
        try:
            _audit("price_check", pc_id, "status_change", actor,
                   old_value=old_status, new_value=status)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("update_pc_status(%s, %s) failed: %s", pc_id, status, e, exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Order Entity
# ═══════════════════════════════════════════════════════════════════════════════

def get_order(order_id: str) -> dict | None:
    """Get a single order by ID.
    Input: order_id (str)
    Output: dict with all order fields + parsed items, or None if not found.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            # Orders V2 phase 4 (2026-04-14): drop data_json blob read.
            # The normalized `items` column is authoritative; order_dal
            # also reads from order_line_items for per-line state. This
            # function is kept alive only for the 7 legacy callers that
            # haven't migrated to order_dal yet. Preferring the blob over
            # `items` was the last thing preventing us from dropping the
            # data_json column entirely.
            d.pop("data_json", None)  # discard any stale blob
            d["items"] = _safe_json(d.get("items"), [])
            return d
    except Exception as e:
        log.error("get_order(%s) failed: %s", order_id, e, exc_info=True)
        raise


def list_orders(status: str = None, limit: int = 500) -> list[dict]:
    """List orders, optionally filtered by status.
    Input: status (optional str), limit (int, default 500)
    Output: list of order dicts sorted by created_at desc.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM orders WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?",
                    (limit,)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                oid = d.get("id", "")
                # Orders V2 phase 4: see get_order() above — read from
                # normalized `items` column, not the data_json blob.
                d.pop("data_json", None)
                d["items"] = _safe_json(d.get("items"), [])
                d["order_id"] = oid
                result.append(d)
            return result
    except Exception as e:
        log.error("list_orders(status=%s) failed: %s", status, e, exc_info=True)
        raise


def save_order(order: dict, actor: str = "system") -> bool:
    """Insert or update an order record.
    Input: order dict (must have 'id'), actor for audit
    Output: True on success.
    Side effects: Writes to orders table — `items` column only.

    Orders V2 phase 4 (2026-04-14): the data_json blob write is
    removed. order_dal.py already stopped writing the blob in V2
    phase 3; this function was the last writer. The column itself
    will be dropped in a follow-up PR after a 48h monitoring window.
    """
    order_id = order.get("id")
    if not order_id:
        raise ValueError("Order must have an 'id' field")
    try:
        with get_db() as conn:
            _existing = conn.execute("SELECT id FROM orders WHERE id=?", (order_id,)).fetchone()
            if _existing:
                _snapshot_before_update("order", order_id, get_order)
            conn.execute("""
                INSERT INTO orders (id, quote_number, agency, institution, po_number,
                    po_date, status, total, items, notes, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    quote_number=excluded.quote_number, agency=excluded.agency,
                    institution=excluded.institution, po_number=excluded.po_number,
                    po_date=excluded.po_date, status=excluded.status,
                    total=excluded.total, items=excluded.items,
                    notes=excluded.notes, updated_at=excluded.updated_at
            """, (order_id, order.get("quote_number", ""), order.get("agency", ""),
                  order.get("institution", ""), order.get("po_number", ""),
                  order.get("po_date", ""), order.get("status", "new"),
                  order.get("total", 0),
                  json.dumps(order.get("items", order.get("line_items", [])), default=str),
                  order.get("notes", ""), order.get("created_at", "")))
        # Audit trail
        try:
            _audit("order", order_id, "create" if not _existing else "update", actor,
                   new_value=json.dumps(order, default=str)[:2000])
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("save_order(%s) failed: %s", order_id, e, exc_info=True)
        raise


def update_order_status(order_id: str, status: str, actor: str = "system") -> bool:
    """Update only the status field of an order.
    Input: order_id, new status string, actor for audit
    Output: True on success.
    Side effects: Writes to orders table.
    """
    try:
        with get_db() as conn:
            old = conn.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
            old_status = old["status"] if old else ""
            conn.execute(
                "UPDATE orders SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (status, order_id))
        # Audit trail
        try:
            _audit("order", order_id, "status_change", actor,
                   old_value=old_status, new_value=status)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        # Fire webhook for status change
        try:
            from src.core.webhooks import fire_webhook
            fire_webhook("order.updated", {
                "order_id": order_id, "old_status": old_status, "new_status": status,
                "actor": actor,
            })
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return True
    except Exception as e:
        log.error("update_order_status(%s, %s) failed: %s", order_id, status, e, exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# LineItem Entity
# ═══════════════════════════════════════════════════════════════════════════════

def get_line_items(parent_id: str, parent_type: str = "rfq") -> list[dict]:
    """Get line items for a parent entity (RFQ, PC, or Order).
    Input: parent_id, parent_type ('rfq'|'price_check'|'order')
    Output: list of item dicts (parsed from JSON column).
    Side effects: None.
    """
    table_map = {"rfq": "rfqs", "price_check": "price_checks", "order": "orders"}
    table = table_map.get(parent_type)
    if not table:
        raise ValueError(f"Unknown parent_type: {parent_type}")
    try:
        with get_db() as conn:
            row = conn.execute(f"SELECT items FROM {table} WHERE id = ?",
                               (parent_id,)).fetchone()
            if not row:
                return []
            return _safe_json(row["items"], [])
    except Exception as e:
        log.error("get_line_items(%s, %s) failed: %s", parent_id, parent_type, e, exc_info=True)
        raise


def save_line_items(parent_id: str, items: list[dict],
                    parent_type: str = "rfq") -> bool:
    """Save line items for a parent entity.
    Input: parent_id, items list, parent_type ('rfq'|'price_check'|'order')
    Output: True on success.
    Side effects: Writes items JSON column in parent table.
    """
    table_map = {"rfq": "rfqs", "price_check": "price_checks", "order": "orders"}
    table = table_map.get(parent_type)
    if not table:
        raise ValueError(f"Unknown parent_type: {parent_type}")
    try:
        with get_db() as conn:
            conn.execute(
                f"UPDATE {table} SET items = ?, updated_at = datetime('now') WHERE id = ?",
                (json.dumps(items, default=str), parent_id))
        return True
    except Exception as e:
        log.error("save_line_items(%s, %s) failed: %s", parent_id, parent_type, e, exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Price History
# ═══════════════════════════════════════════════════════════════════════════════

def get_price_history_for_item(part_number: str = "", description: str = "",
                                limit: int = 5) -> list[dict]:
    """Get price history records matching an item by part number or description.
    Input: part_number (exact match first), description (keyword match fallback), limit
    Output: list of price history dicts sorted by found_at desc.
    Side effects: None.
    """
    try:
        with get_db() as conn:
            results = []
            # Try exact part number match first
            if part_number and part_number.strip():
                rows = conn.execute(
                    "SELECT found_at, unit_price, source, agency, quote_number "
                    "FROM price_history WHERE part_number = ? "
                    "ORDER BY found_at DESC LIMIT ?",
                    (part_number.strip(), limit)).fetchall()
                results = [dict(r) for r in rows]

            # Fallback: keyword match on description
            if not results and description and description.strip():
                # Extract first 4 meaningful words (skip short ones)
                words = [w for w in description.split() if len(w) >= 3][:4]
                if words:
                    like_pattern = "%" + "%".join(words) + "%"
                    rows = conn.execute(
                        "SELECT found_at, unit_price, source, agency, quote_number "
                        "FROM price_history WHERE description LIKE ? "
                        "ORDER BY found_at DESC LIMIT ?",
                        (like_pattern, limit)).fetchall()
                    results = [dict(r) for r in rows]

            return results
    except Exception as e:
        log.error("get_price_history_for_item(pn=%s, desc=%s) failed: %s",
                  part_number, description[:50] if description else "", e, exc_info=True)
        raise


def _safe_json(raw, default=None):
    """Parse a JSON string, returning default on failure."""
    if default is None:
        default = []
    if isinstance(raw, (list, dict)):
        return raw
    if not raw or not isinstance(raw, str):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


# ═══════════════════════════════════════════════════════════════════════════════
# Tenant Profile
# ═══════════════════════════════════════════════════════════════════════════════

def get_tenant_profile(tenant_id: str = "reytech") -> dict:
    """Get full tenant profile.
    Input: tenant_id
    Output: dict with all tenant fields, JSON fields parsed. {} if not found.
    Side effects: none.
    """
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM tenant_profiles WHERE tenant_id=?",
                               (tenant_id,)).fetchone()
            if not row:
                return {}
            d = dict(row)
            for field in ("dba_names", "vendor_search_names", "vendor_codes",
                          "certifications", "naics_codes", "licenses_json"):
                d[field] = _safe_json(d.get(field), [])
            return d
    except Exception as e:
        log.error("get_tenant_profile(%s) failed: %s", tenant_id, e, exc_info=True)
        return {}


def get_tenant_vendor_names(tenant_id: str = "reytech") -> list:
    """Returns vendor_search_names for this tenant. Used by harvest.
    Falls back to ['reytech'] if not configured.
    """
    try:
        profile = get_tenant_profile(tenant_id)
        names = profile.get("vendor_search_names", [])
        return names if names else ["reytech"]
    except Exception:
        return ["reytech"]


def get_tenant_certifications(tenant_id: str = "reytech") -> list:
    """Returns active certifications for tenant."""
    try:
        profile = get_tenant_profile(tenant_id)
        return [c for c in profile.get("certifications", [])
                if isinstance(c, dict) and c.get("active", True)]
    except Exception:
        return []


def get_tenant_naics_codes(tenant_id: str = "reytech") -> list:
    """Returns NAICS codes for tenant."""
    try:
        profile = get_tenant_profile(tenant_id)
        return profile.get("naics_codes", [])
    except Exception:
        return []


def check_compliance_alerts(tenant_id: str = "reytech") -> list:
    """Returns list of compliance items needing attention.
    Each alert: {type, message, severity, due_date}
    Severity: 'critical' (overdue/<30d), 'warning' (30-90d), 'info' (90-180d)
    """
    from datetime import datetime, timedelta
    alerts = []
    try:
        profile = get_tenant_profile(tenant_id)
        if not profile:
            return []
        now = datetime.now()

        # Check Statement of Info
        soi_due = profile.get("statement_of_info_due", "")
        if soi_due:
            try:
                due_dt = datetime.strptime(soi_due[:10], "%Y-%m-%d")
                days_until = (due_dt - now).days
                if days_until < 0:
                    alerts.append({
                        "type": "statement_of_info",
                        "message": f"CA Statement of Information OVERDUE (was due {soi_due})",
                        "severity": "critical",
                        "due_date": soi_due,
                        "link": "https://bizfileonline.sos.ca.gov/search/business"
                    })
                elif days_until <= 30:
                    alerts.append({
                        "type": "statement_of_info",
                        "message": f"CA Statement of Information due in {days_until} days ({soi_due})",
                        "severity": "critical",
                        "due_date": soi_due,
                        "link": "https://bizfileonline.sos.ca.gov/search/business"
                    })
                elif days_until <= 60:
                    alerts.append({
                        "type": "statement_of_info",
                        "message": f"CA Statement of Information due in {days_until} days ({soi_due})",
                        "severity": "warning",
                        "due_date": soi_due,
                        "link": "https://bizfileonline.sos.ca.gov/search/business"
                    })
            except Exception as _e:
                log.debug("suppressed: %s", _e)

        # Check certifications with expiry
        for cert in profile.get("certifications", []):
            if not isinstance(cert, dict):
                continue
            expiry = cert.get("expiry")
            cert_type = cert.get("type", "Unknown")
            cert_num = cert.get("number", "")
            state = cert.get("state", cert.get("jurisdiction", ""))

            if not cert.get("active", True):
                alerts.append({
                    "type": f"cert_{cert_type}",
                    "message": f"{cert_type} #{cert_num} ({state}) is INACTIVE",
                    "severity": "critical",
                    "due_date": None
                })
                continue

            if expiry:
                try:
                    exp_dt = datetime.strptime(expiry[:10], "%Y-%m-%d")
                    days_until = (exp_dt - now).days
                    if days_until < 0:
                        alerts.append({
                            "type": f"cert_{cert_type}",
                            "message": f"{cert_type} #{cert_num} ({state}) EXPIRED on {expiry}",
                            "severity": "critical",
                            "due_date": expiry
                        })
                    elif days_until <= 90:
                        sev = "critical" if days_until <= 30 else "warning"
                        alerts.append({
                            "type": f"cert_{cert_type}",
                            "message": f"{cert_type} #{cert_num} ({state}) expires in {days_until} days",
                            "severity": sev,
                            "due_date": expiry
                        })
                except Exception as _e:
                    log.debug("suppressed: %s", _e)

        return alerts
    except Exception as e:
        log.error("check_compliance_alerts(%s) failed: %s", tenant_id, e, exc_info=True)
        return []


def update_tenant_profile(tenant_id: str, updates: dict) -> bool:
    """Update tenant profile fields. Only updates provided keys.
    Input: tenant_id, dict of field:value pairs to update
    Output: True on success
    Side effects: writes to tenant_profiles table
    """
    _ALLOWED_FIELDS = {
        "legal_name", "dba_names", "entity_number", "entity_type",
        "state_of_formation", "formation_date", "status", "website",
        "phone", "address", "city", "state", "zip",
        "vendor_search_names", "vendor_codes", "certifications",
        "naics_codes", "statement_of_info_due", "licenses_json",
        "notify_phone", "notify_email", "base_url", "api_key_hash",
        "approval_threshold",
    }
    if not updates:
        return True
    # Filter to allowed fields only
    updates = {k: v for k, v in updates.items() if k in _ALLOWED_FIELDS}
    if not updates:
        return True
    try:
        with get_db() as conn:
            # Serialize JSON fields
            for field in ("dba_names", "vendor_search_names", "vendor_codes",
                          "certifications", "naics_codes", "licenses_json"):
                if field in updates and isinstance(updates[field], (list, dict)):
                    updates[field] = json.dumps(updates[field])
            sets = ", ".join(f"{k}=?" for k in updates)
            vals = list(updates.values()) + [tenant_id]
            conn.execute(
                f"UPDATE tenant_profiles SET {sets}, updated_at=datetime('now') WHERE tenant_id=?",
                vals)
        return True
    except Exception as e:
        log.error("update_tenant_profile(%s) failed: %s", tenant_id, e, exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline Aggregates (replaces common raw SQL in routes_analytics)
# ═══════════════════════════════════════════════════════════════════════════════

def get_pipeline_counts(tenant_id: str = "reytech") -> dict:
    """Queue depths for home dashboard. Replaces common aggregate queries.
    Input: tenant_id
    Output: {rfqs_new, rfqs_sent, pcs_new, pcs_sent, orders_active, orders_shipped}
    Side effects: none
    """
    try:
        with get_db() as conn:
            def _count(table, status):
                try:
                    return conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE status=?", (status,)
                    ).fetchone()[0]
                except Exception:
                    return 0
            return {
                "rfqs_new": _count("rfqs", "new"),
                "rfqs_sent": _count("rfqs", "sent"),
                "pcs_new": _count("price_checks", "parsed"),
                "pcs_sent": _count("price_checks", "sent"),
                "orders_active": _count("orders", "new") + _count("orders", "active"),
                "orders_shipped": _count("orders", "shipped"),
            }
    except Exception as e:
        log.error("get_pipeline_counts failed: %s", e, exc_info=True)
        return {}


def get_funnel_stats(tenant_id: str = "reytech") -> dict:
    """Conversion funnel data. Replaces funnel query in routes_analytics.
    Input: tenant_id
    Output: {imported, parsed, priced, sent, won, lost, total}
    Side effects: none
    """
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM rfqs GROUP BY status"
            ).fetchall()
            counts = {r[0]: r[1] for r in rows}
            return {
                "imported": counts.get("new", 0),
                "parsed": counts.get("draft", 0),
                "priced": counts.get("priced", 0) + counts.get("generated", 0),
                "sent": counts.get("sent", 0) + counts.get("quoted", 0),
                "won": counts.get("won", 0),
                "lost": counts.get("lost", 0),
                "total": sum(counts.values()),
            }
    except Exception as e:
        log.error("get_funnel_stats failed: %s", e, exc_info=True)
        return {}


# ═══════════════════════════════════════════════════════════════════
# Package Audit Trail DAL
# ═══════════════════════════════════════════════════════════════════

def create_package_manifest(rfq_id, agency_key, agency_name, required_forms,
                            generated_forms, missing_forms=None, quote_number="",
                            quote_total=0, item_count=0, created_by="system",
                            items_snapshot=None):
    """Create a new package manifest record. Returns manifest ID."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT MAX(version) FROM package_manifest WHERE rfq_id = ?",
                (rfq_id,)).fetchone()
            version = (row[0] or 0) + 1
            cursor = conn.execute("""
                INSERT INTO package_manifest
                (rfq_id, version, created_at, created_by, agency_key, agency_name,
                 required_forms, generated_forms, missing_forms, overall_status,
                 quote_number, quote_total, item_count, total_forms, items_snapshot)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (rfq_id, version, datetime.now().isoformat(), created_by,
                  agency_key, agency_name,
                  json.dumps(required_forms), json.dumps(generated_forms),
                  json.dumps(missing_forms or []), "draft",
                  quote_number, quote_total, item_count, len(generated_forms),
                  json.dumps(items_snapshot, default=str) if items_snapshot else None))
            manifest_id = cursor.lastrowid
            for form in generated_forms:
                form_id = form.get("form_id", "") if isinstance(form, dict) else str(form)
                form_filename = form.get("filename", "") if isinstance(form, dict) else ""
                conn.execute("""
                    INSERT INTO package_review
                    (manifest_id, form_id, form_filename, verdict)
                    VALUES (?,?,?,?)
                """, (manifest_id, form_id, form_filename, "pending"))
            log.info("Created package manifest %d for RFQ %s (v%d, %d forms)",
                     manifest_id, rfq_id, version, len(generated_forms))
            return manifest_id
    except Exception as e:
        log.error("create_package_manifest failed: %s", e)
        return None


def get_package_manifest(manifest_id):
    """Get a single package manifest with its review records."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM package_manifest WHERE id = ?",
                (manifest_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            for field in ("required_forms", "generated_forms", "missing_forms",
                          "source_validation", "field_audit"):
                if d.get(field):
                    try:
                        d[field] = json.loads(d[field])
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
            reviews = conn.execute(
                "SELECT * FROM package_review WHERE manifest_id = ? ORDER BY id",
                (manifest_id,)).fetchall()
            d["reviews"] = [dict(r) for r in reviews]
            return d
    except Exception as e:
        log.error("get_package_manifest failed: %s", e)
        return None


def get_latest_manifest(rfq_id):
    """Get the most recent package manifest for an RFQ."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT id FROM package_manifest WHERE rfq_id = ? ORDER BY version DESC LIMIT 1",
                (rfq_id,)).fetchone()
            if row:
                return get_package_manifest(row[0])
            return None
    except Exception as e:
        log.error("get_latest_manifest failed: %s", e)
        return None


def update_manifest_status(manifest_id, status, package_filename=None, package_size=None):
    """Update manifest status (draft → reviewed → approved → sent)."""
    try:
        with get_db() as conn:
            updates = ["overall_status = ?"]
            params = [status]
            if package_filename:
                updates.append("package_filename = ?")
                params.append(package_filename)
            if package_size:
                updates.append("package_size = ?")
                params.append(package_size)
            params.append(manifest_id)
            conn.execute(f"UPDATE package_manifest SET {', '.join(updates)} WHERE id = ?", params)
        return True
    except Exception as e:
        log.error("update_manifest_status failed: %s", e)
        return False


def review_form(manifest_id, form_id, verdict, reviewed_by="user", notes=""):
    """Record a review verdict for a specific form in a manifest."""
    try:
        with get_db() as conn:
            conn.execute("""
                UPDATE package_review
                SET verdict = ?, reviewed_at = ?, reviewed_by = ?, notes = ?
                WHERE manifest_id = ? AND form_id = ?
            """, (verdict, datetime.now().isoformat(), reviewed_by, notes,
                  manifest_id, form_id))
            pending = conn.execute(
                "SELECT COUNT(*) FROM package_review WHERE manifest_id = ? AND verdict = 'pending'",
                (manifest_id,)).fetchone()[0]
            if pending == 0:
                conn.execute(
                    "UPDATE package_manifest SET overall_status = 'reviewed' WHERE id = ? AND overall_status = 'draft'",
                    (manifest_id,))
            return True
    except Exception as e:
        log.error("review_form failed: %s", e)
        return False


def reset_form_verdict(manifest_id, form_id):
    """Reset a form's review verdict to 'pending' after a refill/edit."""
    try:
        with get_db() as conn:
            conn.execute("""
                UPDATE package_review
                SET verdict = 'pending', reviewed_at = NULL, reviewed_by = NULL,
                    notes = COALESCE(notes, '') || ' [refilled ' || ? || ']'
                WHERE manifest_id = ? AND form_id = ?
            """, (datetime.now().strftime("%Y-%m-%d %H:%M"), manifest_id, form_id))
            # Also reset manifest status back to draft if it was reviewed/approved
            conn.execute("""
                UPDATE package_manifest SET overall_status = 'draft'
                WHERE id = ? AND overall_status IN ('reviewed', 'approved')
            """, (manifest_id,))
            return True
    except Exception as e:
        log.error("reset_form_verdict failed: %s", e)
        return False


def record_package_delivery(manifest_id, rfq_id, recipient_email, recipient_name="",
                            email_subject="", email_log_id=None, package_hash=""):
    """Record that a package was sent."""
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO package_delivery
                (manifest_id, rfq_id, delivered_at, recipient_email, recipient_name,
                 email_subject, email_log_id, package_hash)
                VALUES (?,?,?,?,?,?,?,?)
            """, (manifest_id, rfq_id, datetime.now().isoformat(),
                  recipient_email, recipient_name, email_subject,
                  email_log_id, package_hash))
            conn.execute(
                "UPDATE package_manifest SET overall_status = 'sent' WHERE id = ?",
                (manifest_id,))
            return True
    except Exception as e:
        log.error("record_package_delivery failed: %s", e)
        return False


def log_lifecycle_event(entity_type, entity_id, event_type, summary,
                        actor="system", detail=None, metadata=None):
    """Log an immutable lifecycle event. INSERT-only, never updated or deleted."""
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO lifecycle_events
                (entity_type, entity_id, event_type, occurred_at, actor, summary, detail_json, metadata)
                VALUES (?,?,?,?,?,?,?,?)
            """, (entity_type, entity_id, event_type, datetime.now().isoformat(),
                  actor, summary,
                  json.dumps(detail, default=str) if detail else None,
                  metadata))
        return True
    except Exception as e:
        log.error("log_lifecycle_event failed: %s", e)
        return False


def get_lifecycle_events(entity_type, entity_id, limit=100):
    """Get lifecycle events for an entity, newest first."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT * FROM lifecycle_events
                WHERE entity_type = ? AND entity_id = ?
                ORDER BY occurred_at DESC LIMIT ?
            """, (entity_type, entity_id, limit)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                if d.get("detail_json"):
                    try:
                        d["detail"] = json.loads(d["detail_json"])
                    except Exception:
                        d["detail"] = d["detail_json"]
                result.append(d)
            return result
    except Exception as e:
        log.error("get_lifecycle_events failed: %s", e)
        return []


def get_qa_effectiveness_metrics(days=90):
    """Aggregate QA effectiveness metrics from lifecycle_events.

    Returns: form QA pass rates, PC QA scores, outcome correlation
    (clean vs dirty win rates), regeneration patterns, weekly trend.
    """
    from datetime import timedelta
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with get_db() as conn:
            # 1. Form QA stats
            qa_rows = conn.execute(
                "SELECT detail_json FROM lifecycle_events "
                "WHERE event_type = 'form_qa_completed' AND occurred_at >= ? "
                "ORDER BY occurred_at DESC", (cutoff,)
            ).fetchall()

            total_qa_runs = len(qa_rows)
            qa_passed = 0
            category_counts = {}
            total_critical = 0
            total_warnings = 0
            for row in qa_rows:
                try:
                    d = json.loads(row[0]) if row[0] else {}
                except Exception:
                    continue
                if d.get("passed"):
                    qa_passed += 1
                total_critical += d.get("critical_count", 0)
                total_warnings += d.get("warning_count", 0)
                for cat, cnt in d.get("categories", {}).items():
                    if cnt > 0:
                        category_counts[cat] = category_counts.get(cat, 0) + cnt

            # 2. Outcome correlations
            corr_rows = conn.execute(
                "SELECT detail_json FROM lifecycle_events "
                "WHERE event_type = 'outcome_qa_correlation' AND occurred_at >= ?",
                (cutoff,)
            ).fetchall()

            clean_won = clean_lost = dirty_won = dirty_lost = 0
            regen_counts = []
            for row in corr_rows:
                try:
                    d = json.loads(row[0]) if row[0] else {}
                except Exception:
                    continue
                qa_clean = d.get("last_qa_passed", True)
                outcome = d.get("outcome", "")
                if qa_clean and outcome == "won":
                    clean_won += 1
                elif qa_clean and outcome == "lost":
                    clean_lost += 1
                elif not qa_clean and outcome == "won":
                    dirty_won += 1
                elif not qa_clean and outcome == "lost":
                    dirty_lost += 1
                regen_counts.append(d.get("generation_count", 1))

            clean_total = clean_won + clean_lost
            dirty_total = dirty_won + dirty_lost
            clean_win_rate = round(clean_won / clean_total * 100, 1) if clean_total else None
            dirty_win_rate = round(dirty_won / dirty_total * 100, 1) if dirty_total else None
            avg_regens = round(sum(regen_counts) / len(regen_counts), 1) if regen_counts else 0
            multi_gen = sum(1 for g in regen_counts if g > 1)

            # 3. PC QA stats
            pc_rows = conn.execute(
                "SELECT detail_json FROM lifecycle_events "
                "WHERE event_type = 'pc_qa_completed' AND occurred_at >= ?",
                (cutoff,)
            ).fetchall()
            pc_total = len(pc_rows)
            pc_passed = 0
            pc_scores = []
            for row in pc_rows:
                try:
                    d = json.loads(row[0]) if row[0] else {}
                except Exception:
                    continue
                if d.get("passed"):
                    pc_passed += 1
                if d.get("score") is not None:
                    pc_scores.append(d["score"])

            # 4. Weekly trend
            weekly_rows = conn.execute(
                "SELECT strftime('%%Y-W%%W', occurred_at) as week, "
                "SUM(CASE WHEN json_extract(detail_json, '$.passed') = 1 THEN 1 ELSE 0 END) as passed, "
                "COUNT(*) as total "
                "FROM lifecycle_events "
                "WHERE event_type = 'form_qa_completed' AND occurred_at >= ? "
                "GROUP BY week ORDER BY week", (cutoff,)
            ).fetchall()
            weekly_trend = [{"week": r[0], "passed": r[1], "total": r[2],
                             "rate": round(r[1] / r[2] * 100, 1) if r[2] else 0}
                            for r in weekly_rows]

        return {
            "ok": True,
            "period_days": days,
            "form_qa": {
                "total_runs": total_qa_runs,
                "passed": qa_passed,
                "failed": total_qa_runs - qa_passed,
                "pass_rate": round(qa_passed / total_qa_runs * 100, 1) if total_qa_runs else None,
                "total_critical_issues": total_critical,
                "total_warnings": total_warnings,
                "top_categories": sorted(category_counts.items(), key=lambda x: -x[1])[:6],
            },
            "pc_qa": {
                "total_runs": pc_total,
                "passed": pc_passed,
                "pass_rate": round(pc_passed / pc_total * 100, 1) if pc_total else None,
                "avg_score": round(sum(pc_scores) / len(pc_scores), 1) if pc_scores else None,
            },
            "outcome_correlation": {
                "clean_won": clean_won, "clean_lost": clean_lost,
                "dirty_won": dirty_won, "dirty_lost": dirty_lost,
                "clean_win_rate": clean_win_rate,
                "dirty_win_rate": dirty_win_rate,
                "sample_size": clean_total + dirty_total,
            },
            "regeneration": {
                "avg_generations": avg_regens,
                "multi_gen_pct": round(multi_gen / len(regen_counts) * 100, 1) if regen_counts else 0,
                "total_with_outcome": len(regen_counts),
            },
            "weekly_trend": weekly_trend,
        }
    except Exception as e:
        log.error("get_qa_effectiveness_metrics failed: %s", e)
        return {"ok": False, "error": str(e)}


def save_buyer_preference(buyer_email, preference_key, preference_value,
                          buyer_name="", agency_key="", source="manual", notes=""):
    """Save or update a buyer preference."""
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO buyer_preferences
                (buyer_email, buyer_name, agency_key, preference_key, preference_value,
                 source, learned_at, notes)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(buyer_email, preference_key) DO UPDATE SET
                    preference_value = excluded.preference_value,
                    source = excluded.source,
                    learned_at = excluded.learned_at,
                    notes = excluded.notes
            """, (buyer_email.lower(), buyer_name, agency_key, preference_key,
                  preference_value, source, datetime.now().isoformat(), notes))
        return True
    except Exception as e:
        log.error("save_buyer_preference failed: %s", e)
        return False


def get_buyer_preferences(buyer_email):
    """Get all preferences for a buyer."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM buyer_preferences WHERE buyer_email = ?",
                (buyer_email.lower(),)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_buyer_preferences failed: %s", e)
        return []


def seed_known_buyer_preferences():
    """Seed buyer preferences from known feedback. Safe to run multiple times."""
    prefs = [
        ("grace.pfost@cdcr.ca.gov", "Grace Pfost", "cchcs",
         "no_modify_buyer_fields", "true",
         "buyer_feedback", "Do not change highlighted fields in AMS 704B. Buyer fills department and phone/email. Vendor fills vendor information only. (2026-03-17)"),
        ("eva.madison@calvet.ca.gov", "Eva Madison", "calvet",
         "ship_to_override", "190 California Dr, Yountville, CA 94599",
         "buyer_feedback", "Ship to 190 California Dr not 260. (2026-03-17)"),
    ]
    for email, name, agency, key, value, source, notes in prefs:
        save_buyer_preference(email, key, value, buyer_name=name,
                              agency_key=agency, source=source, notes=notes)


def seed_form_template_registry():
    """Register all known form templates. Safe to run multiple times."""
    import hashlib
    templates = [
        ("bidder_decl", "Bidder Declaration GSPD-05-105", "data/templates/bidder_declaration_blank.pdf", "08/09"),
        ("darfur_act", "Darfur Contracting Act DGS PD 1", "data/templates/darfur_act_blank.pdf", "12/19"),
        ("dvbe843", "DVBE Declaration PD 843", "data/templates/dvbe_843_blank.pdf", "09/2019"),
        ("calrecycle74", "CalRecycle 74", "data/templates/calrecycle_74_blank.pdf", ""),
        ("cv012_cuf", "CV 012 CUF Certification", "data/templates/cv012_cuf_blank.pdf", "01/2026"),
        ("std204", "STD 204 Payee Data Record", "data/templates/std204_blank.pdf", ""),
        ("std1000", "STD 1000 Terms and Conditions", "data/templates/std1000_blank.pdf", ""),
        ("sellers_permit", "Sellers Permit (static copy)", "data/templates/sellers_permit_reytech.pdf", ""),
    ]
    try:
        with get_db() as conn:
            for form_id, name, path, rev in templates:
                sha = ""
                field_count = 0
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        sha = hashlib.sha256(f.read()).hexdigest()
                    try:
                        from pypdf import PdfReader
                        field_count = len(PdfReader(path).get_fields() or {})
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
                conn.execute("""
                    INSERT INTO form_templates
                    (form_id, form_name, template_path, revision_date, field_count, sha256, last_verified)
                    VALUES (?,?,?,?,?,?,datetime('now'))
                    ON CONFLICT(form_id) DO UPDATE SET
                        template_path = excluded.template_path,
                        sha256 = excluded.sha256,
                        field_count = excluded.field_count,
                        last_verified = excluded.last_verified
                """, (form_id, name, path, rev, field_count, sha))
            log.info("Seeded %d form templates", len(templates))
    except Exception as e:
        log.error("seed_form_template_registry failed: %s", e)
