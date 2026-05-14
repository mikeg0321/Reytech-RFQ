"""PR-AJ — auto-price reference fields at ingest.

Mike's funnel: pre-fix every ingested item landed with empty
catalog_cost / supplier / source_url / asin / confidence, forcing
the operator to engage with each row (Oracle lookup, URL paste,
supplier scrape) before they could even see the cost basis. This
hook calls recommend_for_item() per item at _create_record time
and stamps the REFERENCE fields. unit_cost is intentionally left
unset — that's the operator's explicit decision per the standing
"operator-typed cost is sacred" rule.

Tests pin:
  1. Items with Oracle hits get reference fields stamped + the
     auto_priced_at_ingest=True flag for downstream UI signal.
  2. Items with NO Oracle hit (returns None) are unchanged — no
     polluting empty stamps.
  3. unit_cost stays unset — operator decision, never auto-applied
     by this hook.
  4. Items that already carry operator-confirmed cost are NOT
     overwritten — re-ingest safety + URL-paste protection.
  5. Per-item exception in recommend_for_item is logged and skipped;
     other items in the same record continue processing.
  6. Total Oracle unavailability (ImportError) is logged and the
     entire hook becomes a no-op — record still creates with
     un-enriched items, ingest NEVER blocks.

Hermetic — monkeypatched recommend_for_item, no real DB or
network. Mirrors test_auto_tax_at_ingest.py structure.
"""
from __future__ import annotations

import pytest


def _make_classification():
    from src.core.request_classifier import RequestClassification
    return RequestClassification(
        shape="ams_704_quote",
        agency="cchcs",
        confidence=0.9,
        institution="CSP-SAC",
        solicitation_number="TEST-PRICE-001",
    )


def _invoke_with_items(items):
    """Run _create_record with the given items list."""
    from src.core.ingest_pipeline import _create_record
    rid = _create_record(
        record_type="pc",
        items=items,
        header={"ship_to": "100 Prison Rd, Coalinga, CA 93210"},
        classification=_make_classification(),
        primary_path=None,
        email_subject="test",
        email_sender="test@example.com",
        email_uid="test-uid-price",
    )
    return rid


def _load_pc_items(rid):
    from src.api.dashboard import _load_price_checks
    pc = _load_price_checks().get(rid)
    return (pc or {}).get("items") or []


# ── Happy path ───────────────────────────────────────────────────────


def test_auto_price_stamps_reference_fields_on_oracle_hit(temp_data_dir, monkeypatch):
    """Single item with a clean Oracle return → catalog_cost,
    supplier, source_url, asin, confidence all stamped + auto_priced
    flag set."""
    def _rec(description, part_number="", qty=1, upc=""):
        return {
            "unit_cost": None,  # Oracle returns None when not "cost_fresh"
            "catalog_cost": 12.34,
            "supplier_cost": 12.34,
            "supplier": "TestSupplier",
            "source": "oracle",
            "asin": "B0TEST",
            "source_url": "https://example.com/p/123",
            "amazon_price": None,
            "scprs_price": None,
            "confidence": 0.85,
        }
    monkeypatch.setattr("src.core.pricing_oracle_v2.recommend_for_item", _rec)
    rid = _invoke_with_items([
        {"description": "Test widget", "quantity": 5, "unit_price": 0},
    ])
    items = _load_pc_items(rid)
    assert len(items) == 1
    it = items[0]
    assert it["catalog_cost"] == 12.34
    assert it["supplier"] == "TestSupplier"
    assert it["source_url"] == "https://example.com/p/123"
    assert it["asin"] == "B0TEST"
    assert it["confidence"] == 0.85
    assert it.get("auto_priced_at_ingest") is True
    assert it.get("auto_price_at")  # timestamp present


def test_auto_price_does_not_set_unit_cost_even_on_hit(temp_data_dir, monkeypatch):
    """The Oracle's `unit_cost` field is INTENTIONALLY ignored at
    ingest. Operator-typed cost is sacred — this hook only fills
    reference fields. Pre-fix regression: a naive implementation
    that wrote unit_cost from Oracle would clobber the operator's
    URL-paste / hand-typed cost on autosave races."""
    def _rec(description, part_number="", qty=1, upc=""):
        return {
            "unit_cost": 99.99,    # Oracle suggests this; we MUST not write it
            "catalog_cost": 99.99,
            "supplier_cost": 99.99,
            "supplier": "X", "source": "oracle", "asin": "",
            "source_url": "", "amazon_price": None, "scprs_price": None,
            "confidence": 0.9,
        }
    monkeypatch.setattr("src.core.pricing_oracle_v2.recommend_for_item", _rec)
    rid = _invoke_with_items([{"description": "Y", "quantity": 1}])
    items = _load_pc_items(rid)
    assert items[0].get("unit_cost") in (None, 0, "")


# ── Defensive paths ─────────────────────────────────────────────────


def test_auto_price_no_oracle_hit_leaves_item_clean(temp_data_dir, monkeypatch):
    """recommend_for_item returns None → item lands without auto_priced
    flag and without polluted reference fields."""
    monkeypatch.setattr(
        "src.core.pricing_oracle_v2.recommend_for_item",
        lambda **kw: None,
    )
    rid = _invoke_with_items([{"description": "Unknown widget", "quantity": 1}])
    items = _load_pc_items(rid)
    assert items[0].get("auto_priced_at_ingest") is None or \
           items[0].get("auto_priced_at_ingest") is False
    assert not items[0].get("catalog_cost")
    assert not items[0].get("supplier")


def test_auto_price_does_not_overwrite_operator_cost(temp_data_dir, monkeypatch):
    """Item arriving WITH a unit_cost or supplier_cost already set
    (re-ingest, manual override path) must be skipped entirely — no
    Oracle call, no reference-field stamps."""
    _called = []

    def _rec(**kwargs):
        _called.append(kwargs)
        return {"catalog_cost": 999, "supplier": "Hacker", "confidence": 1.0}

    monkeypatch.setattr("src.core.pricing_oracle_v2.recommend_for_item", _rec)
    rid = _invoke_with_items([
        {"description": "Pre-priced item", "quantity": 1, "unit_cost": 5.00},
    ])
    items = _load_pc_items(rid)
    assert _called == []  # never called for already-priced item
    assert items[0]["unit_cost"] == 5.00
    assert items[0].get("catalog_cost") in (None, 0, "")
    assert items[0].get("supplier") in (None, "")


def test_auto_price_per_item_exception_does_not_block_others(temp_data_dir, monkeypatch):
    """One item's Oracle call raises → that item skipped, OTHER items
    in the same record still get enriched."""
    def _rec(description, **kwargs):
        if "boom" in description.lower():
            raise RuntimeError("simulated Oracle error")
        return {
            "catalog_cost": 1.11, "supplier": "S",
            "confidence": 0.7, "source_url": "", "asin": "",
        }
    monkeypatch.setattr("src.core.pricing_oracle_v2.recommend_for_item", _rec)
    rid = _invoke_with_items([
        {"description": "good widget", "quantity": 1},
        {"description": "BOOM trigger", "quantity": 1},
        {"description": "another good", "quantity": 1},
    ])
    items = _load_pc_items(rid)
    assert len(items) == 3
    assert items[0]["catalog_cost"] == 1.11
    assert items[0].get("auto_priced_at_ingest") is True
    assert not items[1].get("catalog_cost")  # BOOM item unenriched
    assert items[2]["catalog_cost"] == 1.11


def test_auto_price_oracle_import_failure_does_not_block_ingest(temp_data_dir, monkeypatch):
    """Total Oracle unavailability — record still creates with all
    items intact, just without reference-field enrichment."""
    # Force ImportError by replacing the symbol with one that raises.
    import sys
    import types
    fake = types.ModuleType("src.core.pricing_oracle_v2")
    # Don't define recommend_for_item — ImportError on `from ... import`
    monkeypatch.setitem(sys.modules, "src.core.pricing_oracle_v2", fake)

    rid = _invoke_with_items([
        {"description": "any item", "quantity": 1},
    ])
    items = _load_pc_items(rid)
    assert len(items) == 1
    # Item lands without enrichment but is otherwise intact
    assert items[0]["description"] == "any item"
    assert not items[0].get("auto_priced_at_ingest")


def test_auto_price_skips_items_without_description(temp_data_dir, monkeypatch):
    """Items missing description can't be Oracle-looked-up. Skip them
    silently — no Oracle call, no stamps."""
    _called = []

    def _rec(**kwargs):
        _called.append(kwargs)
        return {"catalog_cost": 9, "supplier": "S", "confidence": 0.5}

    monkeypatch.setattr("src.core.pricing_oracle_v2.recommend_for_item", _rec)
    rid = _invoke_with_items([
        {"description": "", "quantity": 1},
        {"description": "real item", "quantity": 1},
    ])
    items = _load_pc_items(rid)
    assert len(_called) == 1
    assert _called[0]["description"] == "real item"
    assert not items[0].get("auto_priced_at_ingest")
    assert items[1].get("auto_priced_at_ingest") is True
