"""CP-1 regression guard: quote_engine.enrich_pricing imported a symbol
`recommend_for_item` that never existed in pricing_oracle_v2 — every call
raised ImportError, which was swallowed at `log.info` level, so oracle
enrichment was silently dead on every quote.

Audited 2026-04-22. Fixed by importing get_pricing (the real entry point)
and adapting its nested dict output to the flat dict shape enrich_pricing
expects.
"""
from __future__ import annotations

import importlib


def test_pricing_oracle_v2_exports_get_pricing():
    """The symbol quote_engine imports from pricing_oracle_v2 must exist."""
    mod = importlib.import_module("src.core.pricing_oracle_v2")
    assert hasattr(mod, "get_pricing"), (
        "CP-1 regression: src.core.pricing_oracle_v2.get_pricing is gone. "
        "quote_engine.enrich_pricing imports it lazily — if it's renamed, "
        "every quote's oracle enrichment silently no-ops."
    )


def test_quote_engine_does_not_import_recommend_for_item():
    """`recommend_for_item` never existed. Guard against a regression that
    re-introduces the bad import."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1]
           / "src" / "core" / "quote_engine.py").read_text(encoding="utf-8")
    assert "recommend_for_item" not in src, (
        "CP-1 regression: quote_engine.py references `recommend_for_item`, "
        "which does not exist in pricing_oracle_v2. Use get_pricing + the "
        "_oracle_recommendation_to_flat adapter instead."
    )


def test_oracle_recommendation_to_flat_handles_empty():
    """Adapter must tolerate an empty/None oracle response without crashing."""
    from src.core.quote_engine import _oracle_recommendation_to_flat
    assert _oracle_recommendation_to_flat(None) == {}
    assert _oracle_recommendation_to_flat({}) == {}


def test_oracle_recommendation_to_flat_projects_fields():
    """Adapter projects matched_item + cost + market into the flat shape
    enrich_pricing consumes (asin, supplier, source_url, catalog_cost, etc)."""
    from src.core.quote_engine import _oracle_recommendation_to_flat
    raw = {
        "matched_item": {
            "asin": "B00TESTASIN",
            "supplier": "Grainger",
            "product_url": "https://example.com/item",
            "last_cost": 12.34,
            "confidence": 0.85,
        },
        "cost": {"locked_cost": 11.11, "supplier": "Grainger"},
        "market": {"competitor_low": 14.50, "low": 13.00},
        "recommendation": {"recommended_price": 18.75},
        "sources_used": ["item_memory"],
        "confidence": 0.85,
    }
    flat = _oracle_recommendation_to_flat(raw)
    assert flat["asin"] == "B00TESTASIN"
    assert flat["supplier"] == "Grainger"
    assert flat["source_url"] == "https://example.com/item"
    assert flat["unit_cost"] == 11.11
    assert flat["scprs_price"] == 14.50
    assert flat["confidence"] == 0.85
    assert flat["source"] == "item_memory"
    assert flat["recommended_price"] == 18.75


def test_enrich_pricing_survives_oracle_exception():
    """If get_pricing raises on a given line item, enrichment logs a warning
    and moves on — it must not crash the whole quote."""
    from src.core.quote_engine import enrich_pricing
    from src.core.quote_model import Quote, DocType, LineItem
    from decimal import Decimal

    q = Quote(quote_id="CP1-test", doc_type=DocType.PC, agency="CDCR")
    q.line_items.append(LineItem(
        line_no=1, item_no="ABC123", description="Test widget",
        qty=1, unit_cost=Decimal("0"),
    ))
    # Even with no oracle DB seeded in test env, enrich_pricing must
    # return the quote unchanged, not raise.
    result = enrich_pricing(q)
    assert result is q
    assert len(result.line_items) == 1
