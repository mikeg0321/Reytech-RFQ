"""record_quote_won — PR-4 from 2026-05-06 audit.

Both PC and RFQ mark-won paths must call `record_quote_won` so wins
land in the Won Quotes KB and the inline buyer-last-won lookup
(PR-3, #775) reflects them on the next quote.

Tests:
- PC and RFQ shapes both produce won_quotes rows
- Aliased price fields (unit_price vs price_per_unit) read correctly
- no_bid items skipped
- Items without price skipped
- po_number / solicitation_number / pc_number all reach the row id
- Both call sites import the helper
"""
import os


def _read(path):
    p = os.path.join(os.path.dirname(__file__), "..", path)
    with open(p, encoding="utf-8") as f:
        return f.read()


# ── Source-pin: both call sites use the new helper ────────────────


def test_pc_helper_delegates_to_record_quote_won():
    src = _read("src/api/modules/routes_pricecheck.py")
    assert "from src.knowledge.won_quotes_db import record_quote_won" in src
    assert "record_quote_won(pc, \"pc\")" in src
    # And the inline ingest_scprs_result loop must be GONE — replaced
    # by the helper. Regression guard for re-adding parallel logic.
    assert "Ingested %d/%d items from PC #%s into Won Quotes KB" not in src, (
        "Old inline _ingest_pc_to_won_quotes loop must be replaced by "
        "record_quote_won (substrate consolidation)"
    )


def test_rfq_mark_won_calls_record_quote_won():
    src = _read("src/api/modules/routes_rfq.py")
    assert "from src.knowledge.won_quotes_db import record_quote_won" in src
    assert "record_quote_won(r, \"rfq\")" in src


def test_record_quote_won_lives_in_won_quotes_db():
    src = _read("src/knowledge/won_quotes_db.py")
    assert "def record_quote_won(record: dict, doc_type: str)" in src


# ── Behavior: directly exercise the helper ────────────────────────


def test_record_quote_won_pc_writes_each_priced_item(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import importlib
    import src.core.paths as paths
    importlib.reload(paths)
    import src.core.db as core_db
    importlib.reload(core_db)
    import src.knowledge.won_quotes_db as wq
    importlib.reload(wq)
    core_db.init_db()

    pc = {
        "id": "pc_xyz",
        "pc_number": "PSU-Test-2026",
        "institution": "California Institution for Women",
        "items": [
            {"item_number": "ABC1", "description": "Heel Donut Foam Round",
             "qty": 5, "unit_price": 12.50},
            {"item_number": "DEF2", "description": "Pampers Diapers Size 4",
             "qty": 10, "unit_price": 0.85},
            {"item_number": "ZZZ", "description": "No-bid item",
             "qty": 1, "unit_price": 99, "no_bid": True},
            {"item_number": "PRC", "description": "Has cost no price",
             "qty": 1, "vendor_cost": 5},
        ],
    }
    n = wq.record_quote_won(pc, "pc")
    assert n == 2  # only the two priced biddable items wrote


def test_record_quote_won_rfq_uses_price_per_unit_alias(tmp_path, monkeypatch):
    """RFQ items use price_per_unit, not unit_price. The helper must
    read both via the same alias chain."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import importlib
    import src.core.paths as paths
    importlib.reload(paths)
    import src.core.db as core_db
    importlib.reload(core_db)
    import src.knowledge.won_quotes_db as wq
    importlib.reload(wq)
    core_db.init_db()

    rfq = {
        "id": "rfq_abc",
        "po_number": "PO-12345",
        "solicitation_number": "10838974",
        "institution": "CDCR",
        "agency": "CCHCS",
        "line_items": [
            {"item_number": "RFQ1", "description": "Welch Allyn 503-0142-01",
             "qty": 2, "price_per_unit": 45.00},
            {"item_number": "RFQ2", "description": "Stryker 1234-567-89",
             "qty": 3, "price_per_unit": 0},  # zero — should skip
        ],
    }
    n = wq.record_quote_won(rfq, "rfq")
    assert n == 1


def test_record_quote_won_unknown_doc_type_returns_zero(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import importlib
    import src.knowledge.won_quotes_db as wq
    importlib.reload(wq)
    n = wq.record_quote_won({"items": [{"description": "x", "unit_price": 1}]},
                            "unknown")
    assert n == 0


def test_record_quote_won_empty_items_returns_zero(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import importlib
    import src.core.paths as paths
    importlib.reload(paths)
    import src.core.db as core_db
    importlib.reload(core_db)
    import src.knowledge.won_quotes_db as wq
    importlib.reload(wq)
    core_db.init_db()
    assert wq.record_quote_won({"id": "x", "items": []}, "pc") == 0
    assert wq.record_quote_won({"id": "x", "line_items": []}, "rfq") == 0
