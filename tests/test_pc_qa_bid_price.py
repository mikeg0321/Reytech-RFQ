"""QA agent must read RFQ `bid_price` as well as PC `unit_price`.

Live incident on RFQ 6655f190 (CCHCS, PREQ10843276, 2026-04-21): operator
entered cost $339 / markup 24% / bid $420.36 / totals $6305.40. QA returned
calculated_total=0, displayed_total=0 → three blockers ("Cost exists but no
sell price set", "Total profit $-5085 below $75 floor", "1 of 1 active items
unpriced"). All Save/Generate/Send/Fill-Forms paths disabled.

Root cause: pc_qa_agent.py read `it.get("unit_price")` in six places. RFQ
items persist the operator's price in `bid_price`; PC items in `unit_price`.
QA silently reported 0 for every priced RFQ.

These tests seed an RFQ-shape item (bid_price only) and assert QA does not
blocker-flag it — covering the five distinct read sites (math/profit/
completeness/identity/unpriced-count) so the single-line patch doesn't
reappear somewhere else on the next refactor.
"""
from __future__ import annotations

from src.agents.pc_qa_agent import run_qa, BLOCKER, CAT_COMPLETE


def _rfq_item(**overrides) -> dict:
    """RFQ-shape item: sell price lives in `bid_price` only.

    Deliberately NO `unit_price` and NO `pricing.recommended_price` — the
    prod bug only manifests when bid_price is the *only* price source, which
    matches what live RFQ 6655f190 looked like at the QA call site."""
    base = {
        "item_number": "1",
        "description": "DELL LATITUDE 7450 LAPTOP",
        "mfg_number": "LAT-7450",
        "qty": 15,
        "uom": "EA",
        "no_bid": False,
        "bid_price": 420.36,
        "pricing": {"unit_cost": 339.00},  # cost only — no recommended_price
    }
    base.update(overrides)
    return base


def _rfq(items=None, **overrides) -> dict:
    base = {
        "id": "test-rfq",
        "pc_number": "PREQ10843276",
        "agency": "CCHCS",
        "ship_to": "CCHCS HQ",
        "items": items or [_rfq_item()],
        "profit_summary": {"total_revenue": 6305.40, "total_cost": 5085.00,
                           "gross_profit": 1220.40},
    }
    base.update(overrides)
    return base


class TestRfqBidPriceReadAcrossAllChecks:
    """Each QA check that evaluates price must accept RFQ bid_price."""

    def test_completeness_does_not_flag_unset_price_when_bid_price_present(self):
        """The incident symptom: 'Cost exists but no sell price set' on a
        fully-priced RFQ. Check reads price; with bid_price only it saw 0."""
        report = run_qa(_rfq(), use_llm=False)
        msgs = [i["message"] for i in report["issues"]
                if i.get("severity") == BLOCKER and i.get("category") == CAT_COMPLETE]
        assert not any("no sell price set" in m.lower() for m in msgs), (
            f"QA wrongly flagged priced RFQ as missing sell price. "
            f"Blockers: {msgs}"
        )

    def test_unpriced_count_is_zero_when_bid_price_present(self):
        """'1 of 1 active items have no price' came from an unpriced-count
        loop that ignored bid_price."""
        report = run_qa(_rfq(), use_llm=False)
        msgs = [i["message"] for i in report["issues"]
                if i.get("severity") == BLOCKER and "have no price" in i["message"]]
        assert not msgs, f"unpriced-count tripped on priced RFQ: {msgs}"

    def test_below_cost_check_reads_bid_price(self):
        """Math check flags 'selling below cost'. With bid_price ignored,
        it saw price=0 < cost=339 on every RFQ line."""
        report = run_qa(_rfq(), use_llm=False)
        msgs = [i["message"] for i in report["issues"]
                if i.get("severity") == BLOCKER and "below cost" in i["message"].lower()]
        assert not msgs, f"below-cost flag wrongly raised: {msgs}"

    def test_identity_no_mfg_flag_reads_bid_price(self):
        """Identity check ('priced item has no MFG#') guards with price > 0.
        If price read was bid_price-blind, the guard never triggered → MFG#
        warning never fired on RFQ items. Proves the guard now sees the price."""
        rfq = _rfq(items=[_rfq_item(mfg_number="")])
        report = run_qa(rfq, use_llm=False)
        mfg_issues = [i for i in report["issues"]
                      if "part number" in (i.get("message") or "").lower()
                      or "mfg" in (i.get("field") or "").lower()]
        # With bid_price now visible, the priced-no-MFG warning should appear
        assert mfg_issues, (
            "Identity check missed priced-no-MFG# condition — price read "
            "still blind to bid_price"
        )

    def test_totals_check_computes_nonzero_calculated_total(self):
        """_verify_totals multiplies price * qty. With unit_price=None and
        bid_price=420.36 and qty=15, calculated_total should be 6305.40."""
        report = run_qa(_rfq(), use_llm=False)
        totals = report.get("totals_check") or {}
        assert totals.get("calculated_total", 0) > 0, (
            f"totals_check saw zero: {totals}"
        )
        # 15 × 420.36 = 6305.40 ± rounding
        assert abs(totals["calculated_total"] - 6305.40) < 1.0


class TestPcUnitPriceStillWorks:
    """Regression: the PC happy path (unit_price) must keep working."""

    def test_pc_unit_price_passes_qa(self):
        pc = _rfq(items=[{
            "item_number": "1",
            "description": "Priced PC item",
            "mfg_number": "PC-001",
            "qty": 1,
            "uom": "EA",
            "no_bid": False,
            "unit_price": 100.0,             # PC shape: unit_price, no bid_price
            "pricing": {"unit_cost": 80.0, "recommended_price": 100.0},
        }])
        pc["profit_summary"] = {"total_revenue": 100.0, "total_cost": 80.0,
                                 "gross_profit": 20.0}
        report = run_qa(pc, use_llm=False)
        msgs = [i["message"] for i in report["issues"] if i.get("severity") == BLOCKER]
        assert not any("no sell price" in m.lower() for m in msgs), msgs
        assert not any("have no price" in m for m in msgs), msgs


class TestCostMarkupFallback:
    """Incident 2026-05-04 (PC AUTO_177b18e6, Auralis Plus Mat Connect):
    catalog imported cost=$215, operator set markup=60%, UI showed bid=$344
    live (cost × 1.6). Operator clicked Re-run QA before any input blurred,
    so autosave's debounce hadn't fired and `unit_price` was still 0 on disk.
    QA fired the false-positive "Cost exists but no sell price set" blocker
    even though the displayed bid was correct.

    Fix: when no price field is persisted but cost+markup are, derive the
    price from cost × (1 + markup/100). Mirrors what the UI shows live."""

    def _item_cost_markup_only(self, **overrides):
        """Cost + markup persisted; no unit_price/bid_price/recommended_price
        — the displayed≠persisted state."""
        base = {
            "item_number": "1",
            "description": "Assy, Auralis Plus Mat Connect",
            "mfg_number": "636612",
            "qty": 2,
            "uom": "CASE",
            "no_bid": False,
            "pricing": {"unit_cost": 215.00, "markup_pct": 60},
        }
        base.update(overrides)
        return base

    def test_completeness_clears_when_cost_and_markup_persisted(self):
        """The exact prod symptom: red banner clears when cost+markup are
        on disk even if the price-field write hadn't been flushed yet."""
        rfq = _rfq(items=[self._item_cost_markup_only()])
        rfq["profit_summary"] = {}  # force per-item math
        report = run_qa(rfq, use_llm=False)
        msgs = [i["message"] for i in report["issues"]
                if i.get("severity") == BLOCKER and i.get("category") == CAT_COMPLETE]
        assert not any("no sell price set" in m.lower() for m in msgs), (
            f"QA still flagged sell-price missing despite cost×markup fallback. "
            f"Blockers: {msgs}"
        )
        assert not any("have no price" in m for m in msgs), msgs

    def test_fallback_value_matches_ui_math(self):
        """$215 × 1.60 = $344.00. _verify_totals must read the derived bid
        so calculated_total == subtotal in the UI ($688 for qty=2)."""
        rfq = _rfq(items=[self._item_cost_markup_only()])
        rfq["profit_summary"] = {}
        report = run_qa(rfq, use_llm=False)
        totals = report.get("totals_check") or {}
        # 2 × 344.00 = 688.00 ± rounding
        assert abs(totals.get("calculated_total", 0) - 688.00) < 0.01, (
            f"derived bid math mismatch: {totals}"
        )

    def test_fallback_does_not_fire_without_markup(self):
        """Cost-only (no markup yet) is genuinely unpriced — keep the blocker."""
        rfq = _rfq(items=[self._item_cost_markup_only(
            pricing={"unit_cost": 215.00})])  # markup_pct missing
        rfq["profit_summary"] = {}
        report = run_qa(rfq, use_llm=False)
        msgs = [i["message"] for i in report["issues"]
                if i.get("severity") == BLOCKER and i.get("category") == CAT_COMPLETE]
        assert any("no sell price set" in m.lower() for m in msgs), (
            "Cost-only (markup missing) should still trigger the blocker — "
            f"operator hasn't priced this yet. Got: {msgs}"
        )

    def test_persisted_price_wins_over_fallback(self):
        """When unit_price IS persisted, use it — don't override with derived."""
        rfq = _rfq(items=[self._item_cost_markup_only(
            unit_price=999.99,  # operator override; cost×markup would say $344
            pricing={"unit_cost": 215.00, "markup_pct": 60})])
        rfq["profit_summary"] = {}
        report = run_qa(rfq, use_llm=False)
        totals = report.get("totals_check") or {}
        # 2 × 999.99 = 1999.98 — proves persisted wins
        assert abs(totals.get("calculated_total", 0) - 1999.98) < 0.01, (
            f"derived value wrongly overrode persisted unit_price: {totals}"
        )
