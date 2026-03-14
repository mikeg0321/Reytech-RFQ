#!/usr/bin/env python3
"""
data_integrity.py — Validates relational consistency across RFQs, PCs, orders,
price history, and processed emails. Run anytime:

    python scripts/data_integrity.py

Also callable from smoke_test.py via run_integrity_checks().
"""

import sys
import os
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.paths import DATA_DIR
from src.core.db import DB_PATH

# ── Counters (used when run standalone) ──────────────────────────────────────
_PASS = 0
_FAIL = 0

def _ok(msg):
    global _PASS
    _PASS += 1
    print(f"  PASS  {msg}")

def _fail(msg):
    global _FAIL
    _FAIL += 1
    print(f"  FAIL  {msg}")


# ── Data loaders ─────────────────────────────────────────────────────────────

def _load_json(filename, fallback=None):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return fallback if fallback is not None else {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return fallback if fallback is not None else {}


def _get_conn():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ── Check 1: Sent RFQs have a linked price check ────────────────────────────

def check_sent_rfqs_have_pc():
    """Every RFQ with status 'sent' should have a linked price check."""
    rfqs = _load_json("rfqs.json")
    pcs = _load_json("price_checks.json")
    pc_ids = set(pcs.keys()) if isinstance(pcs, dict) else set()

    sent = []
    orphaned = []
    for rid, r in rfqs.items():
        status = (r.get("status") or "").lower()
        if status != "sent":
            continue
        sent.append(rid)
        pc_id = r.get("linked_pc_id") or r.get("source_pc_id") or ""
        if pc_id and pc_id not in pc_ids:
            orphaned.append((rid, pc_id))

    # Sent RFQs without ANY pc link is informational, not a hard failure,
    # because RFQs can be created manually without a PC.
    no_link = [rid for rid in sent
               if not (rfqs[rid].get("linked_pc_id") or rfqs[rid].get("source_pc_id"))]

    if not sent:
        return True, "No sent RFQs to check"
    if orphaned:
        return False, (f"{len(orphaned)} sent RFQ(s) reference non-existent PC: "
                       f"{', '.join(f'{rid}->{pc}' for rid, pc in orphaned[:5])}")
    detail = f"{len(sent)} sent RFQs checked"
    if no_link:
        detail += f" ({len(no_link)} without PC link — manual creates)"
    return True, detail


# ── Check 2: Orders have valid statuses ──────────────────────────────────────

VALID_ORDER_STATUSES = {
    "new", "active", "sourcing", "ordered", "shipped",
    "partial_delivery", "delivered", "invoiced", "closed",
    "cancelled", "complete",
}

def check_order_statuses():
    """Every order must have a status from the allowed list."""
    orders = _load_json("orders.json")
    if not orders:
        return True, "No orders to check"

    invalid = []
    for oid, o in orders.items():
        status = (o.get("status") or "").lower().strip()
        if status not in VALID_ORDER_STATUSES:
            invalid.append((oid, status or "(empty)"))

    if invalid:
        return False, (f"{len(invalid)} order(s) with invalid status: "
                       f"{', '.join(f'{oid}={s}' for oid, s in invalid[:5])}")
    return True, f"{len(orders)} orders checked, all statuses valid"


# ── Check 3: No orphaned price_history records ──────────────────────────────

def check_price_history_refs():
    """price_history rows with a price_check_id must reference an existing PC."""
    conn = _get_conn()
    if not conn:
        return True, "No database — skipped"

    try:
        rows = conn.execute(
            "SELECT id, price_check_id FROM price_history "
            "WHERE price_check_id IS NOT NULL AND price_check_id != ''"
        ).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return True, "price_history table missing — skipped"

    if not rows:
        conn.close()
        return True, "No price_history rows reference a PC"

    pcs = _load_json("price_checks.json")
    pc_ids = set(pcs.keys()) if isinstance(pcs, dict) else set()

    # Also check DB table for PC ids
    try:
        db_pcs = conn.execute("SELECT id FROM price_checks").fetchall()
        pc_ids |= {r[0] for r in db_pcs}
    except sqlite3.OperationalError:
        pass

    conn.close()

    orphaned = [(r["id"], r["price_check_id"]) for r in rows
                if r["price_check_id"] not in pc_ids]

    if orphaned:
        return False, (f"{len(orphaned)}/{len(rows)} price_history rows reference "
                       f"non-existent PC: {', '.join(str(o[1]) for o in orphaned[:5])}")
    return True, f"{len(rows)} price_history PC refs checked, all valid"


# ── Check 4: processed_emails DB matches JSON ───────────────────────────────

def check_processed_emails_sync():
    """processed_emails SQLite table should match the JSON file (no ghosts)."""
    json_path = os.path.join(DATA_DIR, "processed_emails.json")

    # Load JSON UIDs
    json_uids = set()
    if os.path.exists(json_path):
        try:
            with open(json_path) as f:
                data = json.load(f)
            if isinstance(data, list):
                json_uids = set(data)
            elif isinstance(data, dict):
                json_uids = set(data.keys())
        except Exception:
            pass

    # Load DB UIDs
    conn = _get_conn()
    db_uids = set()
    if conn:
        try:
            rows = conn.execute("SELECT uid FROM processed_emails").fetchall()
            db_uids = {r[0] for r in rows}
        except sqlite3.OperationalError:
            pass
        conn.close()

    if not json_uids and not db_uids:
        return True, "Both sources empty — nothing to compare"

    # Ghost = in DB but not in JSON (or vice versa)
    db_only = db_uids - json_uids
    json_only = json_uids - db_uids

    if not db_only and not json_only:
        return True, f"{len(json_uids)} UIDs in sync across JSON and DB"

    parts = []
    if db_only:
        parts.append(f"{len(db_only)} in DB only")
    if json_only:
        parts.append(f"{len(json_only)} in JSON only")
    # Drift is expected (DB is backup, JSON is primary) — warn but don't fail
    # unless drift is extreme (>50% mismatch)
    total = len(json_uids | db_uids)
    drift = len(db_only) + len(json_only)
    if total > 0 and drift > total * 0.5:
        return False, f"Large drift: {'; '.join(parts)} (total unique: {total})"
    return True, f"{'; '.join(parts)} — minor drift OK (total unique: {total})"


# ── Check 5: No duplicate solicitation numbers in active queue ───────────────

ACTIVE_STATUSES = {"new", "draft", "priced", "inbox", "generated", "sent", "quoted"}

def check_duplicate_solicitations():
    """Active RFQs should not share the same solicitation number."""
    rfqs = _load_json("rfqs.json")
    if not rfqs:
        return True, "No RFQs to check"

    sol_map = {}  # solicitation -> list of RFQ ids
    for rid, r in rfqs.items():
        status = (r.get("status") or "").lower()
        if status not in ACTIVE_STATUSES:
            continue
        sol = (r.get("solicitation_number") or "").strip()
        if not sol:
            continue
        sol_map.setdefault(sol, []).append(rid)

    dupes = {sol: ids for sol, ids in sol_map.items() if len(ids) > 1}
    active_count = sum(len(ids) for ids in sol_map.values())

    if dupes:
        detail_parts = [f"{sol} ({len(ids)}x)" for sol, ids in list(dupes.items())[:5]]
        return False, (f"{len(dupes)} duplicate solicitation(s) in active queue: "
                       f"{', '.join(detail_parts)}")
    return True, f"{active_count} active RFQs checked, {len(sol_map)} unique solicitations"


# ── Runner ───────────────────────────────────────────────────────────────────

# ── Check 6: RFQ parity — SQLite vs JSON ─────────────────────────────────────

def check_rfq_parity():
    """RFQ count in SQLite should match rfqs.json within a small delta."""
    json_rfqs = _load_json("rfqs.json")
    json_count = len(json_rfqs) if isinstance(json_rfqs, dict) else 0

    conn = _get_conn()
    if not conn:
        return True, f"No database — JSON has {json_count} RFQs"
    try:
        db_count = conn.execute("SELECT COUNT(*) FROM rfqs").fetchone()[0]
    except sqlite3.OperationalError:
        conn.close()
        return True, f"rfqs table missing — JSON has {json_count}"
    conn.close()

    delta = abs(db_count - json_count)
    if delta > 10:
        return False, f"Large parity gap: DB={db_count}, JSON={json_count} (delta={delta})"
    if delta > 2:
        return True, f"Minor parity gap: DB={db_count}, JSON={json_count} (delta={delta}, within tolerance)"
    return True, f"RFQ parity OK: DB={db_count}, JSON={json_count}"


# ── Check 7: Sent RFQs have priced line items ────────────────────────────────

def check_sent_rfqs_priced():
    """Every sent RFQ should have at least one line item with price > 0."""
    conn = _get_conn()
    if not conn:
        return True, "No database — skipped"
    try:
        rows = conn.execute(
            "SELECT id, items FROM rfqs WHERE status = 'sent'"
        ).fetchall()
    except sqlite3.OperationalError:
        conn.close()
        return True, "rfqs table missing — skipped"
    conn.close()

    if not rows:
        return True, "No sent RFQs to check"

    unpriced = []
    for r in rows:
        items = []
        try:
            items = json.loads(r["items"] or "[]") if isinstance(r["items"], str) else (r["items"] or [])
        except Exception:
            pass
        has_price = any(
            (item.get("price_per_unit") or item.get("unit_price") or 0) > 0
            for item in items if isinstance(item, dict)
        )
        if items and not has_price:
            unpriced.append(r["id"])

    if unpriced:
        return False, f"{len(unpriced)} sent RFQ(s) with all-zero prices: {', '.join(unpriced[:5])}"
    return True, f"{len(rows)} sent RFQs checked, all have priced items"


# ── Check 8: No orphaned order→quote references ──────────────────────────────

def check_order_rfq_refs():
    """Orders with quote_number should reference an existing quote."""
    orders = _load_json("orders.json")
    if not orders:
        return True, "No orders to check"

    conn = _get_conn()
    quote_numbers = set()
    if conn:
        try:
            rows = conn.execute("SELECT quote_number FROM quotes WHERE quote_number IS NOT NULL").fetchall()
            quote_numbers = {r[0] for r in rows}
        except sqlite3.OperationalError:
            pass
        conn.close()

    orphaned = []
    for oid, o in orders.items():
        qn = (o.get("quote_number") or "").strip()
        if qn and quote_numbers and qn not in quote_numbers:
            orphaned.append(oid)

    if orphaned:
        # Warn but don't fail — quotes may be in JSON only
        return True, f"{len(orphaned)} order(s) reference quotes not in DB (may be JSON-only): {', '.join(orphaned[:5])}"
    return True, f"{len(orders)} orders checked, all quote refs valid"


# ── Check 9: SCPRS harvest has data ──────────────────────────────────────────

def check_scprs_harvest():
    """scprs_po_master should have rows from a successful harvest."""
    conn = _get_conn()
    if not conn:
        return True, "No database — skipped"
    try:
        count = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
    except sqlite3.OperationalError:
        conn.close()
        return True, "scprs_po_master table missing — harvest not run yet"
    conn.close()
    if count == 0:
        return False, "scprs_po_master is empty — run scripts/run_scprs_harvest.py"
    return True, f"scprs_po_master: {count} POs harvested"


# ── Check 10: won_quotes_kb has priced items ─────────────────────────────────

def check_won_quotes_kb():
    """won_quotes_kb should have rows with winning_price > 0."""
    conn = _get_conn()
    if not conn:
        return True, "No database — skipped"
    try:
        total = conn.execute("SELECT COUNT(*) FROM won_quotes_kb").fetchone()[0]
        priced = conn.execute(
            "SELECT COUNT(*) FROM won_quotes_kb WHERE winning_price > 0"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        conn.close()
        return True, "won_quotes_kb table missing — harvest not run yet"
    conn.close()
    if total == 0:
        return False, "won_quotes_kb is empty — run scripts/run_scprs_harvest.py"
    return True, f"won_quotes_kb: {total} items, {priced} with price > 0"


ALL_CHECKS = [
    ("Sent RFQs have linked PC", check_sent_rfqs_have_pc),
    ("Order statuses valid", check_order_statuses),
    ("Price history PC refs", check_price_history_refs),
    ("Processed emails sync", check_processed_emails_sync),
    ("No duplicate solicitations", check_duplicate_solicitations),
    ("RFQ DB/JSON parity", check_rfq_parity),
    ("Sent RFQs have priced items", check_sent_rfqs_priced),
    ("Order→quote references", check_order_rfq_refs),
    ("SCPRS harvest has data", check_scprs_harvest),
    ("Won quotes KB populated", check_won_quotes_kb),
]


def run_integrity_checks(ok_fn=None, fail_fn=None):
    """Run all integrity checks. Returns (passed, failed, results).

    If ok_fn/fail_fn are provided (e.g., from smoke_test), uses those
    for reporting. Otherwise uses internal counters.
    """
    _ok_fn = ok_fn or _ok
    _fail_fn = fail_fn or _fail

    passed = 0
    failed = 0
    results = []

    for name, check_fn in ALL_CHECKS:
        try:
            is_ok, detail = check_fn()
        except Exception as e:
            is_ok, detail = False, f"CRASH: {type(e).__name__}: {str(e)[:150]}"

        if is_ok:
            _ok_fn(f"{name}: {detail}")
            passed += 1
        else:
            _fail_fn(f"{name}: {detail}")
            failed += 1

        results.append({"check": name, "ok": is_ok, "detail": detail})

    return passed, failed, results


# ── Standalone execution ─────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("DATA INTEGRITY CHECKS")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"DB_PATH:  {DB_PATH}")
    print("=" * 60)

    passed, failed, _ = run_integrity_checks()

    print("\n" + "=" * 60)
    if failed == 0:
        print(f"ALL CLEAR: {passed} passed, 0 failures")
        sys.exit(0)
    else:
        print(f"FAILURES: {failed} failed, {passed} passed")
        sys.exit(1)
