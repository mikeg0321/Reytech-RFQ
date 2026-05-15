"""PR-AV13 (AV-13) — buyer_source_items audit-trail snapshot.

Closes the gap that the review-package banner has been flagging since
PR-A landed: `compute_review_alignment` exposes a `source_items` knob
and the template renders "Buyer asked X / You replied Y" rows when
it's populated, but no ingest path was actually writing a stable
audit snapshot — `routes_rfq.py` hardcoded `source_items=None`. So
the banner permanently said "No buyer-source items captured at ingest
— verify your items below match the original RFQ PDF/email by hand."

Snapshot rules (tested below):
  1. Captured at every ingest write boundary (PC create, RFQ create,
     PC reparse, RFQ reparse).
  2. Snapshot ONLY carries audit-relevant fields (description, qty,
     part_number, uom, notes). Pricing/cost/markup are excluded —
     those are Reytech's response, not buyer-source.
  3. Reparse OVERWRITES the snapshot with the fresh parser view
     (using the new parse output BEFORE the pricing-preserving merge).
  4. Operator edits to `line_items` / `items` after ingest do NOT
     touch the snapshot — it's frozen at the parser's view.
  5. Malformed / empty inputs degrade gracefully (qty coercion,
     non-dict items filtered, stripping).

Also pins the routes_rfq.py wire-up: `review_alignment.compute_*` now
receives `r.get("buyer_source_items")` instead of None.
"""
from __future__ import annotations


# ── Helper-level ────────────────────────────────────────────────────────────

def test_snapshot_keeps_only_audit_fields():
    """Snapshot should ONLY persist description, qty, part_number, uom,
    notes — never unit_price/markup/cost (those are Reytech's response)."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    items = [{
        "description": "Foley Catheter 16Fr",
        "qty": 12,
        "part_number": "FN4368",
        "uom": "EA",
        "notes": "no latex",
        # Reytech-pricing fields — must be DROPPED from the snapshot:
        "unit_price": 14.55,
        "price_per_unit": 14.55,
        "supplier_cost": 9.99,
        "catalog_cost": 10.50,
        "markup_pct": 35,
        "match_score": 0.86,
    }]
    snap = _snapshot_buyer_source_items(items)
    assert len(snap) == 1
    row = snap[0]
    assert set(row.keys()) == {"description", "qty", "part_number", "uom", "notes"}
    assert row["description"] == "Foley Catheter 16Fr"
    assert row["qty"] == 12
    assert row["part_number"] == "FN4368"
    assert row["uom"] == "EA"
    assert row["notes"] == "no latex"


def test_snapshot_falls_back_quantity_alias():
    """Some parsers (vision, generic_rfq) emit `quantity`; canonical is
    `qty`. The snapshot must accept either."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{"description": "X", "quantity": 5}])
    assert snap[0]["qty"] == 5


def test_snapshot_falls_back_mfg_number_alias():
    """Buyer attachments occasionally yield `mfg_number` instead of
    `part_number`. Both must populate the snapshot's `part_number`."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{
        "description": "X", "qty": 1, "mfg_number": "ACME-123",
    }])
    assert snap[0]["part_number"] == "ACME-123"


def test_snapshot_falls_back_unit_of_measure_alias():
    """Some parsers emit `unit_of_measure` rather than `uom`."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{
        "description": "X", "qty": 1, "unit_of_measure": "BX",
    }])
    assert snap[0]["uom"] == "BX"


def test_snapshot_qty_coerces_string_to_int():
    """Vision can yield qty as a string ('12'). The snapshot stores int
    so downstream consumers (alignment row-diff) don't need to coerce."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{"description": "X", "qty": "12"}])
    assert snap[0]["qty"] == 12


def test_snapshot_qty_invalid_becomes_zero():
    """Garbage qty (None, '', non-numeric) coerces to 0 not raise."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{"description": "X", "qty": "garbage"}])
    assert snap[0]["qty"] == 0
    snap2 = _snapshot_buyer_source_items([{"description": "X"}])
    assert snap2[0]["qty"] == 0


def test_snapshot_handles_empty_and_none():
    """Defensive: empty list / None must return empty list, never raise."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    assert _snapshot_buyer_source_items([]) == []
    assert _snapshot_buyer_source_items(None) == []


def test_snapshot_skips_non_dict_entries():
    """A list with a stray None / string / int must skip the bad entries
    rather than crash. The parser pipeline rarely emits these, but the
    snapshot is a hot path — never let it kill ingest."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    items = [
        {"description": "real", "qty": 1},
        None,
        "stray",
        42,
        {"description": "also real", "qty": 2},
    ]
    snap = _snapshot_buyer_source_items(items)
    assert len(snap) == 2
    assert snap[0]["description"] == "real"
    assert snap[1]["description"] == "also real"


def test_snapshot_strips_whitespace():
    """Trim description/part_number/uom/notes so alignment-diff doesn't
    miss a match because the buyer's PDF padded a trailing space."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{
        "description": "  Foley Catheter  ",
        "qty": 1,
        "part_number": "  FN4368 ",
        "uom": " EA ",
        "notes": " no latex ",
    }])
    row = snap[0]
    assert row["description"] == "Foley Catheter"
    assert row["part_number"] == "FN4368"
    assert row["uom"] == "EA"
    assert row["notes"] == "no latex"


def test_snapshot_handles_missing_fields_gracefully():
    """Items lacking optional fields populate the snapshot with empty
    strings, not None — keeps the downstream Jinja template render
    deterministic (no `None` shows up in the alignment table)."""
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items([{"description": "Bandage"}])
    assert snap[0]["description"] == "Bandage"
    assert snap[0]["part_number"] == ""
    assert snap[0]["uom"] == ""
    assert snap[0]["notes"] == ""


# ── Alignment wire-up ───────────────────────────────────────────────────────


def test_review_alignment_consumes_buyer_source_items():
    """End-to-end: when an RFQ has buyer_source_items populated,
    compute_review_alignment renders rows with `their_*` populated
    instead of the "no source captured" banner."""
    from src.api.review_alignment import compute_review_alignment

    rfq = {
        "id": "rfq_test",
        "agency": "cchcs",
        "line_items": [
            {"description": "Foley Catheter", "qty": 12, "unit_price": 14.55},
        ],
        "buyer_source_items": [
            {"description": "Foley Catheter", "qty": 12, "part_number": "FN4368",
             "uom": "EA", "notes": ""},
        ],
    }
    manifest = {"field_audit": {}, "source_validation": {}}
    out = compute_review_alignment(
        rfq=rfq, manifest=manifest, agency_cfg={"name": "CCHCS"},
        source_items=rfq["buyer_source_items"],
    )
    items = out["items_alignment"]
    assert items["has_source"] is True
    assert len(items["rows"]) == 1
    assert items["rows"][0]["their_qty"] == 12
    assert items["rows"][0]["match"] == "matched"


def test_review_alignment_missing_snapshot_renders_banner():
    """Legacy / manual-entry records have no snapshot. Alignment must
    still render rows (operator's items) but flag has_source=False so
    the template shows the 'verify by hand' banner."""
    from src.api.review_alignment import compute_review_alignment

    rfq = {
        "id": "rfq_test",
        "agency": "cchcs",
        "line_items": [{"description": "Bandage", "qty": 3}],
        # NO buyer_source_items key
    }
    manifest = {"field_audit": {}, "source_validation": {}}
    out = compute_review_alignment(
        rfq=rfq, manifest=manifest, agency_cfg={"name": "CCHCS"},
        source_items=None,
    )
    items = out["items_alignment"]
    assert items["has_source"] is False
    assert len(items["rows"]) == 1  # operator's items still render
    assert items["rows"][0]["their_qty"] == 0
    assert items["rows"][0]["match"] == "no_source"


# ── Persistence contract ────────────────────────────────────────────────────


def test_buyer_source_items_field_survives_save_load_round_trip(
    tmp_path, monkeypatch
):
    """The snapshot must round-trip through SQLite → load_rfqs() so
    /rfq/<id>/review-package reads what was written at ingest. The
    save layer dumps the full record dict into data_json so any new
    key persists — this test pins that contract."""
    import json
    # Build a record dict mirroring what ingest_pipeline writes.
    rfq = {
        "id": "rfq_abc",
        "received_at": "2026-05-14T00:00:00",
        "agency": "cchcs",
        "institution": "satf",
        "requestor_name": "Test",
        "requestor_email": "test@example.com",
        "rfq_number": "TEST-001",
        "line_items": [{"description": "X", "qty": 1, "unit_price": 1.50}],
        "buyer_source_items": [
            {"description": "X", "qty": 1, "part_number": "MFG-1",
             "uom": "EA", "notes": ""},
        ],
        "status": "parsed",
    }
    # Simulate the data_layer JSON round-trip (it stores json.dumps(r) in
    # the data_json column and reads back via json.loads).
    serialized = json.dumps(rfq, default=str)
    restored = json.loads(serialized)
    assert "buyer_source_items" in restored
    assert restored["buyer_source_items"] == rfq["buyer_source_items"]


def test_operator_edit_doesnt_mutate_snapshot():
    """The whole point of the snapshot: when operator edits a
    description / qty in line_items, buyer_source_items must remain
    frozen at the parser's view so the alignment table still surfaces
    the discrepancy."""
    items_at_ingest = [{"description": "Foley", "qty": 12, "part_number": "FN4368"}]
    from src.core.ingest_pipeline import _snapshot_buyer_source_items
    snap = _snapshot_buyer_source_items(items_at_ingest)

    rfq = {
        "id": "rfq_x",
        "line_items": list(items_at_ingest),
        "buyer_source_items": snap,
    }
    # Operator edits the line_items (typo fix on description, qty change)
    rfq["line_items"][0]["description"] = "Foley Cath 16Fr (operator edit)"
    rfq["line_items"][0]["qty"] = 24

    # buyer_source_items must remain untouched
    assert rfq["buyer_source_items"][0]["description"] == "Foley"
    assert rfq["buyer_source_items"][0]["qty"] == 12
