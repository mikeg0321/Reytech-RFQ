"""
Seed Test0321 PC + R26Q0321 RFQ from real R25Q94 data.

REAL DATA, RE-KEYED. The items, prices, MFG#s, and descriptions come verbatim
from a real Reytech quote sent to CCHCS in July 2025 (solicitation 10819488).
The identifiers are rewritten so the test fixture cannot be confused with a
real prod quote and never increments the real quote counter.

Usage:
    python scripts/seed_golden_fixture.py            # seed dev DB
    python scripts/seed_golden_fixture.py --check    # exit 0 if seeded, 1 if not
    python scripts/seed_golden_fixture.py --remove   # delete the test rows

Refuses to run in prod (RAILWAY_ENVIRONMENT=production).
"""
import argparse
import json
import os
import sys
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

FIXTURE_PATH = os.path.join(ROOT, "tests", "fixtures", "golden", "test0321_real_cchcs.json")
PC_ID = "Test0321"
PC_NUMBER = "Test0321"
RFQ_ID = "Test0321-rfq"
RFQ_QUOTE_NUMBER = "R26Q0321"


def _guard_prod():
    env = os.environ.get("RAILWAY_ENVIRONMENT", "").lower()
    if env in ("production", "prod"):
        sys.stderr.write(f"REFUSING: RAILWAY_ENVIRONMENT={env}. Test fixtures are dev-only.\n")
        sys.exit(2)


def _load_fixture():
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def seed():
    _guard_prod()
    f = _load_fixture()
    h = f["header"]
    items = f["line_items"]

    from src.core.dal import save_pc, save_rfq

    now_iso = datetime.utcnow().isoformat(timespec="seconds")

    pc_data = {
        "header": h,
        "parsed": {"header": h, "line_items": items},
        "line_items": items,
        "items": items,
        "totals": f["totals"],
        "test_fixture": True,
    }
    pc = {
        "id": PC_ID,
        "created_at": now_iso,
        "requestor": h["requestor"],
        "agency": h["agency"],
        "institution": h["institution"],
        "items": items,
        "source_file": "tests/fixtures/golden/test0321_real_cchcs.json",
        "quote_number": "",
        "pc_number": PC_NUMBER,
        "total_items": len(items),
        "status": "priced",
        "email_uid": "",
        "email_subject": f"[TEST FIXTURE] R25Q94 CCHCS {h['solicitation_number']}",
        "due_date": h["due_date"],
        "pc_data": json.dumps(pc_data, default=str),
        "ship_to": h["ship_to"],
    }
    save_pc(pc, actor="seed_golden_fixture")

    rfq = {
        "id": RFQ_ID,
        "received_at": now_iso,
        "agency": h["agency"],
        "institution": h["institution"],
        "requestor_name": h["requestor"],
        "requestor_email": h["requestor_email"],
        "rfq_number": RFQ_QUOTE_NUMBER,
        "items": items,
        "status": "in_progress",
        "source": "test_fixture",
        "email_uid": "",
        "notes": f"Test fixture from R25Q94 (real CCHCS quote, sol {h['solicitation_number']}). NEVER REAL.",
    }
    save_rfq(rfq, actor="seed_golden_fixture")

    print(f"Seeded PC {PC_ID!r} and RFQ {RFQ_ID!r} (quote {RFQ_QUOTE_NUMBER!r})")
    print(f"  agency={h['agency']}, institution={h['institution']}")
    print(f"  items={len(items)}, subtotal=${f['totals']['subtotal']}, total=${f['totals']['total']}")
    print(f"  view: /pc/{PC_ID}  and  /rfq/{RFQ_ID}")


def check():
    import sqlite3
    db = os.path.join(ROOT, "data", "reytech.db")
    if not os.path.exists(db):
        print("DB missing"); sys.exit(1)
    c = sqlite3.connect(db)
    pc = c.execute("SELECT id FROM price_checks WHERE id=?", (PC_ID,)).fetchone()
    rfq = c.execute("SELECT id FROM rfqs WHERE id=?", (RFQ_ID,)).fetchone()
    if pc and rfq:
        print(f"OK: PC {PC_ID} and RFQ {RFQ_ID} both present"); sys.exit(0)
    print(f"MISSING: pc={bool(pc)} rfq={bool(rfq)}"); sys.exit(1)


def remove():
    _guard_prod()
    import sqlite3
    db = os.path.join(ROOT, "data", "reytech.db")
    c = sqlite3.connect(db)
    c.execute("DELETE FROM price_checks WHERE id=?", (PC_ID,))
    c.execute("DELETE FROM rfqs WHERE id=?", (RFQ_ID,))
    c.commit()
    print(f"Removed PC {PC_ID} and RFQ {RFQ_ID}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    p.add_argument("--remove", action="store_true")
    args = p.parse_args()
    if args.check: check()
    elif args.remove: remove()
    else: seed()
