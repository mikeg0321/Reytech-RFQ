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
            except Exception:
                pass
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
                        except Exception:
                            pass
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
    """Update an existing row."""
    cols = [c for c in columns if c in data and c != pk]
    if not cols:
        return
    sets = ",".join(f"{c}=?" for c in cols)
    vals = [data.get(c) for c in cols]
    vals.append(data[pk])
    conn.execute(f"UPDATE {table} SET {sets} WHERE {pk}=?", vals)


def _fallback_load_json(filename: str, default):
    """Emergency fallback if DB is unavailable."""
    try:
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return default
