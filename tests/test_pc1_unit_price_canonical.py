"""PC-1 live-ships-wrong-price guard.

Audit: project_pc_module_audit_2026_04_21 — stale `unit_price` on
cost/markup edits → emails + PDFs ship wrong price. Partially fixed by
PR #321 (recompute on write). Fully closed 2026-04-23 by
`src.core.pricing_math.canonical_unit_price` (read-side truth) +
`scripts/backfill_unit_price.py` (heal pre-PR-321 records).

Prod evidence 2026-04-23: pc_f7ba7a6b (Cortech mattress) UI=$567.79,
email=$558.48, gap $9.31 × 16 qty = $148.96 under-quote per send.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import importlib
from unittest.mock import patch

import pytest


# ── Pure-function unit tests ────────────────────────────────────────────────


def test_canonical_prefers_cost_times_markup_over_stale_unit_price():
    from src.core.pricing_math import canonical_unit_price
    # The exact prod case that ships wrong: cost 465.40, markup 22%,
    # stored unit_price 558.48 (stale from before PR #321).
    item = {
        "vendor_cost": 465.40,
        "markup_pct": 22,
        "unit_price": 558.48,   # stale persisted value
        "pricing": {"unit_cost": 465.40, "markup_pct": 22,
                    "recommended_price": 558.48},
    }
    assert canonical_unit_price(item) == 567.79


def test_canonical_falls_back_to_unit_price_when_no_cost():
    from src.core.pricing_math import canonical_unit_price
    item = {"unit_price": 42.5, "markup_pct": 25}
    assert canonical_unit_price(item) == 42.5


def test_canonical_falls_back_to_recommended_price():
    from src.core.pricing_math import canonical_unit_price
    item = {"pricing": {"recommended_price": 12.34}}
    assert canonical_unit_price(item) == 12.34


def test_canonical_zero_when_nothing_usable():
    from src.core.pricing_math import canonical_unit_price
    assert canonical_unit_price({}) == 0.0
    assert canonical_unit_price({"unit_price": None, "pricing": {}}) == 0.0


def test_canonical_zero_cost_defers_to_unit_price():
    """A zero cost is a misconfigured item — don't multiply by (1+markup)
    and return 0, fall back to the persisted unit_price so the operator
    can see + correct something."""
    from src.core.pricing_math import canonical_unit_price
    item = {"vendor_cost": 0, "markup_pct": 25, "unit_price": 100}
    assert canonical_unit_price(item) == 100


def test_canonical_handles_string_numerics():
    from src.core.pricing_math import canonical_unit_price
    item = {"vendor_cost": "100.00", "markup_pct": "25"}
    assert canonical_unit_price(item) == 125.0


# ── 2026-04-23 prod fallthrough regression ──────────────────────────────────


def test_canonical_falls_through_zero_vendor_cost_to_pricing_unit_cost():
    """Prod pc_f7ba7a6b shape: vendor_cost=0 (explicit) but pricing.unit_cost=465.40.

    The first-shipped helper used an `is None` guard on the flat field,
    so 0 skipped the pricing fallback and fell back to the stale
    unit_price. Email body shipped $558.48 while UI rendered $567.79.
    The UI handles this correctly with `or`-chaining; the helper now
    does the same.
    """
    from src.core.pricing_math import canonical_unit_price
    item = {
        "vendor_cost": 0,  # explicit zero on the flat field
        "markup_pct": 22,
        "unit_price": 558.48,  # stale
        "pricing": {
            "unit_cost": 465.40,  # real cost lives here
            "markup_pct": 22,
            "recommended_price": 558.48,
        },
    }
    assert canonical_unit_price(item) == 567.79


def test_canonical_handles_rfq_supplier_cost_alias():
    """RFQ items store cost as `supplier_cost` (routes_rfq_gen.py:671),
    not `vendor_cost`. Helper must accept either."""
    from src.core.pricing_math import canonical_unit_price
    item = {"supplier_cost": 100, "markup_pct": 25, "unit_price": 110}
    assert canonical_unit_price(item) == 125.0


def test_canonical_handles_generic_cost_alias():
    """Some legacy / import paths use a flat `cost` field."""
    from src.core.pricing_math import canonical_unit_price
    item = {"cost": 80, "markup_pct": 25}
    assert canonical_unit_price(item) == 100.0


def test_canonical_handles_pricing_markup_alias():
    """`pricing.markup` (no _pct) appears in some legacy shapes."""
    from src.core.pricing_math import canonical_unit_price
    item = {"vendor_cost": 100, "pricing": {"markup": 40}}
    assert canonical_unit_price(item) == 140.0


def test_is_stale_flags_zero_vendor_cost_with_pricing_cost():
    """The prod regression case — must be flagged as stale so the backfill
    heals it on the next run."""
    from src.core.pricing_math import is_unit_price_stale
    assert is_unit_price_stale({
        "vendor_cost": 0,
        "markup_pct": 22,
        "unit_price": 558.48,
        "pricing": {"unit_cost": 465.40, "markup_pct": 22},
    }) is True


def test_is_unit_price_stale_flags_prod_regression():
    from src.core.pricing_math import is_unit_price_stale
    assert is_unit_price_stale({
        "vendor_cost": 465.40, "markup_pct": 22, "unit_price": 558.48,
    }) is True


def test_is_unit_price_stale_passes_when_fresh():
    from src.core.pricing_math import is_unit_price_stale
    assert is_unit_price_stale({
        "vendor_cost": 465.40, "markup_pct": 22, "unit_price": 567.79,
    }) is False


def test_is_unit_price_stale_tolerates_sub_cent_jitter():
    """Sub-cent jitter (floating-point noise) shouldn't trip the check."""
    from src.core.pricing_math import is_unit_price_stale
    # 100 × 1.25 = 125.00 exactly; round-trip drift sub-cent
    assert is_unit_price_stale({
        "vendor_cost": 100.00, "markup_pct": 25, "unit_price": 125.002,
    }) is False
    # One cent off IS stale (precision > ½¢ default)
    assert is_unit_price_stale({
        "vendor_cost": 100.00, "markup_pct": 25, "unit_price": 125.01,
    }) is True


def test_is_unit_price_stale_skips_items_without_cost():
    from src.core.pricing_math import is_unit_price_stale
    assert is_unit_price_stale({"unit_price": 42.0, "markup_pct": 25}) is False


# ── Email body / read-path regression ───────────────────────────────────────


def test_build_item_summary_uses_canonical_price(auth_client, temp_data_dir):
    """The exact PC-1 case: a record with stale unit_price must render
    the live cost×markup derivation in the email body, not the stale
    persisted value."""
    from src.api.data_layer import _save_single_pc
    pid = "pc_pc1_email_body"
    _save_single_pc(pid, {
        "id": pid, "status": "draft",
        "pc_number": "10844466-PC1",
        "institution": "CCHCS",
        "requestor": "Mohammad Chechi",
        "items": [{
            "description": "Cortech USA C453075P mattress",
            "qty": 16,
            "vendor_cost": 465.40,
            "markup_pct": 22,
            "unit_price": 558.48,   # STALE — represents pre-PR#321 save
            "pricing": {"unit_cost": 465.40, "markup_pct": 22,
                        "recommended_price": 558.48},
        }],
    })
    resp = auth_client.get(f"/api/pricecheck/{pid}/email-preview")
    assert resp.status_code == 200
    body = resp.get_json().get("body", "")
    # Must quote at the canonical derivation, NOT the stale persisted value
    assert "$567.79" in body, (
        f"Email body still ships stale price. PC-1 regression. Body:\n{body}"
    )
    assert "$558.48" not in body


def test_recompute_unit_price_delegates_to_canonical():
    """_recompute_unit_price in routes_pricecheck.py must stay in lock-step
    with canonical_unit_price so write + read paths agree forever."""
    from src.api.modules.routes_pricecheck import _recompute_unit_price
    from src.core.pricing_math import canonical_unit_price
    item = {"vendor_cost": 465.40, "markup_pct": 22,
            "pricing": {"unit_cost": 465.40, "markup_pct": 22}}
    _recompute_unit_price(item)
    assert item["unit_price"] == canonical_unit_price(item) == 567.79
    assert item["pricing"]["recommended_price"] == 567.79


# ── Backfill script contract tests ──────────────────────────────────────────


@pytest.fixture(scope="module")
def backfill_mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "backfill_unit_price" in sys.modules:
        del sys.modules["backfill_unit_price"]
    return importlib.import_module("backfill_unit_price")


def _seed_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY, created_at TEXT, requestor TEXT,
            agency TEXT, institution TEXT, items TEXT, source_file TEXT,
            quote_number TEXT, pc_number TEXT, total_items INTEGER,
            status TEXT, email_uid TEXT, email_subject TEXT,
            due_date TEXT, pc_data TEXT, ship_to TEXT, data_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY, received_at TEXT, agency TEXT,
            institution TEXT, requestor_name TEXT, requestor_email TEXT,
            rfq_number TEXT, items TEXT, status TEXT, source TEXT,
            email_uid TEXT, notes TEXT, updated_at TEXT, data_json TEXT
        );
    """)
    conn.commit()
    conn.close()


def _insert_pc(db_path, pid, blob):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO price_checks (id, created_at, status, pc_number, data_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (pid, blob.get("created_at", "2026-03-01T00:00:00"),
         blob.get("status", "draft"),
         blob.get("pc_number", ""), json.dumps(blob))
    )
    conn.commit()
    conn.close()


def _read_pc(db_path, pid):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT data_json FROM price_checks WHERE id=?", (pid,)
    ).fetchone()
    conn.close()
    return json.loads(row["data_json"]) if row else None


def test_missing_db_exits_2(backfill_mod):
    rc = backfill_mod.run("/nonexistent/x.db", apply=False)
    assert rc == 2


def test_dry_run_does_not_write(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_dry", {
        "status": "draft", "pc_number": "P1",
        "items": [{"vendor_cost": 100, "markup_pct": 25, "unit_price": 110,
                   "qty": 3, "description": "Widget"}],
    })
    rc = backfill_mod.run(db, apply=False)
    assert rc == 0
    blob = _read_pc(db, "pc_dry")
    # unit_price stays at its pre-run stale value; no _unit_price_backfilled_at
    assert blob["items"][0]["unit_price"] == 110
    assert "_unit_price_backfilled_at" not in blob


def test_apply_heals_stale_pc(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_apply", {
        "status": "draft", "pc_number": "P2",
        "items": [{
            "vendor_cost": 465.40, "markup_pct": 22, "unit_price": 558.48,
            "qty": 16, "description": "Cortech mattress",
            "pricing": {"unit_cost": 465.40, "markup_pct": 22,
                        "recommended_price": 558.48},
        }],
    })
    rc = backfill_mod.run(db, apply=True)
    assert rc == 0
    blob = _read_pc(db, "pc_apply")
    item = blob["items"][0]
    assert item["unit_price"] == 567.79
    assert item["pricing"]["recommended_price"] == 567.79
    assert item["_unit_price_backfilled_prior"] == 558.48
    assert item["_unit_price_backfilled_at"]
    assert blob["_unit_price_backfilled_at"]


def test_apply_skips_already_fresh_items(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_fresh", {
        "status": "draft", "pc_number": "P3",
        "items": [{"vendor_cost": 100, "markup_pct": 25, "unit_price": 125,
                   "qty": 5, "description": "Fresh"}],
    })
    rc = backfill_mod.run(db, apply=True)
    assert rc == 0
    blob = _read_pc(db, "pc_fresh")
    item = blob["items"][0]
    assert item["unit_price"] == 125
    # Not touched — no backfill marker should appear
    assert "_unit_price_backfilled_at" not in item
    assert "_unit_price_backfilled_at" not in blob


def test_apply_skips_items_missing_cost_or_markup(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_partial", {
        "status": "draft", "pc_number": "P4",
        "items": [
            # No cost → can't derive → must leave unit_price alone
            {"markup_pct": 25, "unit_price": 42, "qty": 1, "description": "x"},
            # No markup → same
            {"vendor_cost": 100, "unit_price": 150, "qty": 1, "description": "y"},
        ],
    })
    rc = backfill_mod.run(db, apply=True)
    assert rc == 0
    blob = _read_pc(db, "pc_partial")
    assert blob["items"][0]["unit_price"] == 42
    assert blob["items"][1]["unit_price"] == 150
    for it in blob["items"]:
        assert "_unit_price_backfilled_at" not in it


def test_only_pc_scopes_correctly(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_s", {
        "status": "draft", "pc_number": "PS",
        "items": [{"vendor_cost": 100, "markup_pct": 25, "unit_price": 110,
                   "qty": 1, "description": "pc"}],
    })
    # Insert RFQ with same stale-line shape
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO rfqs (id, received_at, status, rfq_number, data_json) "
        "VALUES (?, ?, ?, ?, ?)",
        ("rfq_s", "2026-04-01T00:00:00", "draft", "RS",
         json.dumps({"items": [{"vendor_cost": 100, "markup_pct": 25,
                                "unit_price": 110, "qty": 1,
                                "description": "rfq"}]}))
    )
    conn.commit(); conn.close()
    rc = backfill_mod.run(db, apply=True, only="pc")
    assert rc == 0
    assert _read_pc(db, "pc_s")["items"][0]["unit_price"] == 125
    # RFQ untouched
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    r = conn.execute("SELECT data_json FROM rfqs WHERE id=?", ("rfq_s",)).fetchone()
    conn.close()
    assert json.loads(r["data_json"])["items"][0]["unit_price"] == 110


def test_only_rejects_invalid(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    assert backfill_mod.run(db, apply=True, only="bogus") == 1


def test_cli_apply_flag(backfill_mod, tmp_path):
    db = str(tmp_path / "d.db")
    _seed_db(db)
    _insert_pc(db, "pc_cli", {
        "status": "draft", "pc_number": "PCLI",
        "items": [{"vendor_cost": 100, "markup_pct": 20, "unit_price": 110,
                   "qty": 1, "description": "cli"}],
    })
    rc = backfill_mod.main(["--db", db, "--apply"])
    assert rc == 0
    assert _read_pc(db, "pc_cli")["items"][0]["unit_price"] == 120
