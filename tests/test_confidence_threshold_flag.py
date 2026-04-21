"""`pipeline.confidence_threshold` must gate auto-writeback in record_fields.

Before this flag: the 0.75 threshold was a hardcoded magic number in
src/core/record_fields.py. To tune it (e.g., loosen to 0.70 after a false-
negative audit, or tighten to 0.80 after a false-positive audit) required a
code change + deploy.

Fix: read `pipeline.confidence_threshold` via get_flag() with 0.75 default.
Prod can now tune via /api/admin/flags without a deploy.

Two behaviors asserted:

  1. Default (flag unset) → 0.75 still gates. A match at 0.74 is rejected,
     0.76 is accepted.
  2. Lowering the flag to 0.60 → a match at 0.65 (previously rejected) now
     writes back photo_url/mfg_number onto the item.

The flag is cleared between tests so state doesn't leak.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clear_flag():
    from src.core.flags import delete_flag
    delete_flag("pipeline.confidence_threshold")
    yield
    delete_flag("pipeline.confidence_threshold")


def _stub_match(monkeypatch, confidence: float):
    """Stub product_catalog.match_item to return one fake match with the
    given confidence. The hydrate function only uses match_confidence,
    photo_url, mfg_number, upc, manufacturer, and id from the match dict."""
    import src.agents.product_catalog as pc

    fake = {
        "id": 42,
        "match_confidence": confidence,
        "photo_url": "https://example.com/fake.jpg",
        "mfg_number": "FAKE-MFG-123",
        "upc": "012345678905",
        "manufacturer": "FakeCo",
    }
    monkeypatch.setattr(pc, "match_item", lambda *a, **kw: [fake])
    monkeypatch.setattr(pc, "get_product_suppliers", lambda *a, **kw: [])


class TestConfidenceThresholdFlag:
    def test_default_threshold_still_rejects_below_075(self, monkeypatch):
        """With no flag set, a match at 0.74 is below the default 0.75 gate
        and must not write back any hydration fields."""
        from src.core.record_fields import hydrate_item_from_catalog
        _stub_match(monkeypatch, confidence=0.74)

        item = {"description": "widget", "pricing": {}}
        hydrate_item_from_catalog(item)

        assert "photo_url" not in item, (
            f"0.74 is below default 0.75 threshold — hydrate must reject. "
            f"item: {item}")
        assert "mfg_number" not in item

    def test_default_threshold_accepts_above_075(self, monkeypatch):
        """With no flag set, a match at 0.76 crosses the default 0.75 gate
        and must write back photo_url + mfg_number."""
        from src.core.record_fields import hydrate_item_from_catalog
        _stub_match(monkeypatch, confidence=0.76)

        item = {"description": "widget", "pricing": {}}
        hydrate_item_from_catalog(item)

        assert item.get("photo_url") == "https://example.com/fake.jpg", (
            f"0.76 is above default 0.75 — hydrate should fill. item: {item}")
        assert item.get("mfg_number") == "FAKE-MFG-123"

    def test_lowering_flag_accepts_previously_rejected_match(
            self, monkeypatch):
        """Setting pipeline.confidence_threshold=0.60 must let a 0.65 match
        through — proving the flag is actually consulted, not ignored."""
        from src.core.flags import set_flag
        from src.core.record_fields import hydrate_item_from_catalog
        set_flag("pipeline.confidence_threshold", "0.60")
        _stub_match(monkeypatch, confidence=0.65)

        item = {"description": "widget", "pricing": {}}
        hydrate_item_from_catalog(item)

        assert item.get("photo_url") == "https://example.com/fake.jpg", (
            f"flag lowered to 0.60 → 0.65 should hydrate. item: {item}")

    def test_raising_flag_rejects_previously_accepted_match(
            self, monkeypatch):
        """Setting pipeline.confidence_threshold=0.85 must reject a 0.80
        match — same direction as above, opposite sign."""
        from src.core.flags import set_flag
        from src.core.record_fields import hydrate_item_from_catalog
        set_flag("pipeline.confidence_threshold", "0.85")
        _stub_match(monkeypatch, confidence=0.80)

        item = {"description": "widget", "pricing": {}}
        hydrate_item_from_catalog(item)

        assert "photo_url" not in item, (
            f"flag raised to 0.85 → 0.80 should be rejected. item: {item}")
