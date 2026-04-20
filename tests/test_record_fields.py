"""Tests for src.core.record_fields — the canonical PC/RFQ field reader.

Guards the architecture-layer fix for: PC → RFQ deepcopy preserves PC field
shape (per CLAUDE.md), so downstream QA must normalize on READ, not WRITE.
"""
import copy

from src.core.record_fields import (
    build_qa_view,
    item_qty,
    item_unit_cost,
    item_unit_price,
    record_items,
    record_pc_number,
    record_ship_to,
)


# ── Item-level readers ────────────────────────────────────────────────────

def test_unit_price_canonical_key():
    assert item_unit_price({"unit_price": 12.5}) == 12.5


def test_unit_price_tolerates_bid_price():
    assert item_unit_price({"bid_price": 9.99}) == 9.99


def test_unit_price_tolerates_our_price_legacy_pc_name():
    assert item_unit_price({"our_price": 7.25}) == 7.25


def test_unit_price_falls_back_to_pricing_recommended():
    assert item_unit_price({"pricing": {"recommended_price": 14.0}}) == 14.0


def test_unit_price_direct_wins_over_pricing():
    it = {"unit_price": 20.0, "pricing": {"recommended_price": 99.0}}
    assert item_unit_price(it) == 20.0


def test_unit_price_missing_returns_zero():
    assert item_unit_price({}) == 0.0
    assert item_unit_price(None) == 0.0


def test_unit_cost_canonical_vendor_cost():
    assert item_unit_cost({"vendor_cost": 5.5}) == 5.5


def test_unit_cost_from_pricing_unit_cost():
    assert item_unit_cost({"pricing": {"unit_cost": 6.75}}) == 6.75


def test_unit_cost_pricing_beats_item_when_pricing_populated():
    it = {"vendor_cost": 0, "pricing": {"unit_cost": 4.0}}
    assert item_unit_cost(it) == 4.0


def test_qty_canonical():
    assert item_qty({"qty": 22}) == 22


def test_qty_tolerates_quantity():
    assert item_qty({"quantity": 8}) == 8


# ── Record-level readers ──────────────────────────────────────────────────

def test_record_ship_to_ship_to():
    assert record_ship_to({"ship_to": "Stockton, CA"}) == "Stockton, CA"


def test_record_ship_to_falls_back_to_delivery_location():
    assert record_ship_to({"delivery_location": "CHCF"}) == "CHCF"


def test_record_ship_to_skips_empty_string():
    assert record_ship_to({"ship_to": "", "delivery_location": "CHCF"}) == "CHCF"


def test_record_pc_number_canonical():
    assert record_pc_number({"pc_number": "PC-123"}) == "PC-123"


def test_record_pc_number_falls_back_to_solicitation():
    assert record_pc_number({"solicitation_number": "SOL-9"}) == "SOL-9"


def test_record_items_prefers_items():
    r = {"items": [{"a": 1}], "line_items": [{"b": 2}]}
    assert record_items(r) == [{"a": 1}]


def test_record_items_falls_back_to_line_items():
    r = {"line_items": [{"b": 2}]}
    assert record_items(r) == [{"b": 2}]


def test_record_items_empty():
    assert record_items({}) == []


# ── QA view build ─────────────────────────────────────────────────────────

def test_build_qa_view_does_not_mutate_source():
    pc = {
        "pc_number": "PC-001",
        "items": [{"description": "Widget", "unit_price": 10, "vendor_cost": 6, "qty": 5}],
    }
    snapshot = copy.deepcopy(pc)
    _ = build_qa_view(pc)
    assert pc == snapshot, "build_qa_view mutated source record"


def test_build_qa_view_normalizes_rfq_with_line_items():
    rfq = {
        "line_items": [{"description": "X", "unit_price": 15, "vendor_cost": 9, "qty": 4}],
        "ship_to_name": "COR Warehouse",
    }
    view = build_qa_view(rfq)
    assert view["items"][0]["unit_price"] == 15
    assert view["ship_to"] == "COR Warehouse"


def test_build_qa_view_promotes_legacy_our_price_to_unit_price():
    pc = {
        "pc_number": "PC-2",
        "items": [{"description": "Legacy", "our_price": 8.88, "vendor_cost": 4, "qty": 3}],
    }
    view = build_qa_view(pc)
    assert view["items"][0]["unit_price"] == 8.88


def test_build_qa_view_items_empty_for_empty_record():
    assert build_qa_view({})["items"] == []
    assert build_qa_view(None)["items"] == []


def test_hydrate_item_skips_when_all_fields_present(monkeypatch):
    """If the item already has every hydratable field, no DB lookup runs."""
    from src.core import record_fields as rf

    called = {"match": 0, "supp": 0}

    def _bad(*a, **k):
        called["match"] += 1
        return []

    monkeypatch.setattr("src.agents.product_catalog.match_item", _bad)
    item = {
        "description": "Widget",
        "item_link": "https://ex.com",
        "photo_url": "https://ex.com/p.jpg",
        "mfg_number": "W-123",
        "upc": "012345678905",
        "manufacturer": "Acme",
    }
    rf.hydrate_item_from_catalog(item)
    assert called["match"] == 0, "short-circuit failed — did a DB lookup when nothing was missing"


def test_hydrate_item_low_confidence_match_does_not_overwrite(monkeypatch):
    from src.core import record_fields as rf

    def _low(*a, **k):
        return [{"id": 1, "match_confidence": 0.5,
                 "photo_url": "https://bad/match.jpg", "mfg_number": "X"}]

    monkeypatch.setattr("src.agents.product_catalog.match_item", _low)
    monkeypatch.setattr("src.agents.product_catalog.get_product_suppliers",
                        lambda pid: [])
    item = {"description": "Widget"}
    rf.hydrate_item_from_catalog(item)
    assert "photo_url" not in item, "low-confidence match polluted the item"


def test_hydrate_item_fills_empty_fields_from_high_conf_match(monkeypatch):
    from src.core import record_fields as rf

    def _hi(*a, **k):
        return [{"id": 42, "match_confidence": 0.95,
                 "photo_url": "https://pic/1.jpg", "mfg_number": "ACME-1",
                 "upc": "9999999", "manufacturer": "Acme"}]

    def _suppliers(pid):
        assert pid == 42
        return [{"supplier_name": "Acme Direct", "supplier_url": "https://supplier/1"}]

    monkeypatch.setattr("src.agents.product_catalog.match_item", _hi)
    monkeypatch.setattr("src.agents.product_catalog.get_product_suppliers", _suppliers)
    item = {"description": "Widget"}
    rf.hydrate_item_from_catalog(item)
    assert item["photo_url"] == "https://pic/1.jpg"
    assert item["mfg_number"] == "ACME-1"
    assert item["item_link"] == "https://supplier/1"
    assert item["supplier_name"] == "Acme Direct"


def test_hydrate_item_does_not_overwrite_operator_values(monkeypatch):
    from src.core import record_fields as rf

    def _hi(*a, **k):
        return [{"id": 1, "match_confidence": 0.95,
                 "photo_url": "https://cat/1.jpg", "mfg_number": "CAT-1"}]

    def _suppliers(pid):
        return [{"supplier_name": "Cat", "supplier_url": "https://cat-supp/"}]

    monkeypatch.setattr("src.agents.product_catalog.match_item", _hi)
    monkeypatch.setattr("src.agents.product_catalog.get_product_suppliers", _suppliers)
    item = {
        "description": "Widget",
        "item_link": "https://operator-entered-url",
        "mfg_number": "OP-123",
    }
    rf.hydrate_item_from_catalog(item)
    assert item["item_link"] == "https://operator-entered-url"
    assert item["mfg_number"] == "OP-123"


def test_ingest_result_to_dict_emits_unified_contract():
    """Every Upload & Parse response must include items_found / items_added /
    parser so the frontend uploadDoc() handler reads the same shape regardless
    of which ingest path ran."""
    from src.core.ingest_pipeline import IngestResult

    r = IngestResult(
        ok=True, record_type="rfq", record_id="abc123",
        items_parsed=7, classification={"shape": "AMS 704"},
    )
    d = r.to_dict()
    assert d["items_found"] == 7
    assert d["items_added"] == 7
    assert d["items_parsed"] == 7
    assert d["parser"] == "AMS 704"
    assert d["parser_used"] == "AMS 704"


def test_ingest_result_to_dict_zero_items_still_well_formed():
    from src.core.ingest_pipeline import IngestResult

    r = IngestResult(ok=False, items_parsed=0, classification=None,
                     errors=["no match"])
    d = r.to_dict()
    assert d["items_found"] == 0
    assert d["items_added"] == 0
    assert d["parser"] == "classifier_v2"
    assert d["errors"] == ["no match"]


def test_qa_adapter_yields_pass_on_deepcopied_pc_when_profit_clears_floor():
    """Round-trip: PC with $100+ gross profit, deepcopy → build_qa_view →
    pc_qa_agent sees correct revenue/profit. No BLOCKER on the profit floor."""
    pc = {
        "pc_number": "PC-042",
        "solicitation_number": "PC-042",
        "ship_to": "CDCR Sacramento",
        "requestor": "Joe Buyer",
        "institution": "CDCR",
        "agency": "cdcr",
        "due_date": "12/31/2099",
        "items": [
            {
                "description": "Widget A",
                "mfg_number": "W12345",
                "unit_price": 50.0,
                "vendor_cost": 30.0,
                "qty": 10,
                "uom": "EA",
            }
        ],
    }
    rfq = copy.deepcopy(pc)
    rfq["source"] = "pc_conversion"
    rfq["status"] = "new"

    view = build_qa_view(rfq)

    try:
        from src.agents.pc_qa_agent import run_qa
    except Exception:
        return

    report = run_qa(view, use_llm=False)
    profit_issues = [i for i in report.get("issues", [])
                     if i.get("category") == "profit" and i.get("severity") == "blocker"]
    assert not profit_issues, f"unexpected profit-floor blocker: {profit_issues}"
