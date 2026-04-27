"""Plan §4.4 — Historical replay regression test.

The plan calls for replaying 50 historical sent quotes through the current
pricing pipeline and diffing against originally-sent PDFs to catch silent
regressions. Full PDF round-trip needs prod fixtures (525MB reytech.db is
not in the repo). This module ships the **structural-integrity** half of
that gate now: realistic items × multiple agencies × varied quantities,
piped through `pricing_oracle_v2.get_pricing()`, asserting the pipeline:

  • Doesn't crash on any of the 30+ inventory shapes Reytech actually quotes
  • Returns the documented response keys every time (no None-vs-missing surprises)
  • Respects the cost floor — recommendation never recommends a price below cost
  • Handles edge cases: empty description, qty=0, missing cost, all 4 valid agencies

Future PR can layer the heavier PDF-diff replay on top once we ship a
fixture set of historical PCs + their original PDFs to a test corpus.

The 30+ items here come from the actual product categories in
`docs/STATE_OF_THE_UNION_2026_04_26.md` — gloves, incontinence, wound care,
N95s, sharps, restraints, sanitizer, activity supplies, compression. These
ARE Reytech's bread-and-butter SKUs, not synthetic fixtures.
"""
import pytest


# ─── Realistic Reytech inventory ───────────────────────────────────────────
# Categories pulled from actual quoted items + AGENCY_REGISTRY.what_they_buy.
HISTORICAL_ITEMS = [
    # description, item_number, qty_per_uom — the inputs the operator types
    # in a real PC row. Mix of bulk and pack sizes.
    ("Nitrile Exam Gloves Powder-Free Medium", "MED1086", 100),
    ("Nitrile Exam Gloves Powder-Free Large", "MED1087", 100),
    ("Adult Brief Heavy Absorbency Large", "FN4368", 80),
    ("Adult Brief Heavy Absorbency XL", "FN4369", 80),
    ("Underpad Disposable 23x36", "NL304", 150),
    ("Underpad Reusable 34x36", "NL305", 1),
    ("N95 Respirator Mask 1860", "1860", 20),
    ("N95 Respirator Mask 1870+", "1870PLUS", 20),
    ("Hand Sanitizer 8oz Pump", "HS8", 24),
    ("Hand Sanitizer Gallon", "HS128", 4),
    ("Wound Care Gauze 4x4", "GZ44", 200),
    ("Wound Care Gauze 2x2", "GZ22", 200),
    ("Compression Stocking Knee-High Medium", "CS-KH-M", 1),
    ("Compression Stocking Thigh-High Large", "CS-TH-L", 1),
    ("Sharps Container 1 Quart", "SC1Q", 1),
    ("Sharps Container 5 Gallon", "SC5G", 1),
    ("Restraint Strap Soft Wrist", "RS-SW", 2),
    ("Restraint Strap Posey", "RS-POSEY", 2),
    ("Activity Marker Set Crayola 24-pack", "16753", 1),
    ("Construction Paper Assorted 9x12", "CP-AST", 50),
    ("Tempera Paint 1 Gallon White", "TP-W128", 1),
    ("Disposable Cup 9oz Hot", "DC9-HOT", 50),
    ("Toilet Tissue 2-ply Industrial", "TT-2P", 80),
    ("Trash Bag 55-Gallon Heavy", "TB-55H", 100),
    ("Bleach 1 Gallon Industrial", "BL128", 1),
    ("Disinfecting Wipes 7x8 Tub", "DW-78", 75),
    ("Pillowcase Disposable", "PC-DISP", 100),
    ("Bedsheet Hospital Twin", "BS-HT", 1),
    ("Patient Gown Reusable", "PG-RE", 1),
    ("Walker Folding Adult", "W-FA", 1),
]

AGENCIES = ["CCHCS", "CalVet", "DSH", "CDCR"]

REQUIRED_KEYS = {"description", "quantity", "matched_item", "confidence",
                 "cost", "market", "recommendation", "strategies",
                 "tiers", "competitors", "cross_sell", "sources_used"}


@pytest.fixture
def get_pricing():
    """Import inside fixture so DB_PATH monkeypatch in conftest takes effect."""
    from src.core.pricing_oracle_v2 import get_pricing as fn
    return fn


class TestPipelineStructuralIntegrity:
    """The pricing pipeline never crashes on any realistic input shape.

    These tests would catch a regression where someone changes the cost
    cascade, market analyzer, or recommendation builder in a way that
    breaks the contract — `result["recommendation"]` becomes None, a
    required key vanishes, exception bubbles through, etc.
    """

    @pytest.mark.parametrize("agency", AGENCIES)
    def test_all_realistic_items_return_expected_shape(self, get_pricing, agency):
        # 30 items × 4 agencies = 120 calls. Any single divergence fails.
        for desc, item_num, qty_uom in HISTORICAL_ITEMS:
            r = get_pricing(
                description=desc, quantity=10, cost=5.00,
                item_number=item_num, department=agency,
                qty_per_uom=qty_uom, line_count=5,
            )
            assert isinstance(r, dict), f"non-dict result for {desc}@{agency}"
            missing = REQUIRED_KEYS - set(r.keys())
            assert not missing, f"missing keys {missing} for {desc}@{agency}"
            # recommendation may be empty dict if no signal — never None.
            assert r["recommendation"] is not None
            assert r["sources_used"] is not None
            assert isinstance(r["sources_used"], list)

    def test_empty_description_returns_safe_default(self, get_pricing):
        # Empty / too-short description must NOT crash; returns the bare result
        # template so callers can branch on it.
        r = get_pricing(description="", quantity=1)
        assert isinstance(r, dict)
        assert r["description"] == ""
        assert REQUIRED_KEYS <= set(r.keys())

    def test_short_description_returns_safe_default(self, get_pricing):
        r = get_pricing(description="ab", quantity=1)
        assert REQUIRED_KEYS <= set(r.keys())

    @pytest.mark.parametrize("qty", [0, 1, 5, 100, 1000, 10000])
    def test_quantity_extremes_dont_crash(self, get_pricing, qty):
        r = get_pricing(description="Nitrile Exam Gloves Medium",
                        quantity=qty, cost=5.00, department="CCHCS")
        assert isinstance(r, dict)
        # quantity returned as-given (or normalized), not None
        assert r["quantity"] is not None

    def test_no_cost_doesnt_crash(self, get_pricing):
        # Operator hasn't entered a cost yet — pipeline must still run.
        r = get_pricing(description="Adult Brief Large", quantity=10,
                        cost=None, department="CCHCS")
        assert REQUIRED_KEYS <= set(r.keys())

    def test_unknown_department_doesnt_crash(self, get_pricing):
        r = get_pricing(description="Nitrile Gloves", quantity=10, cost=5.00,
                        department="NOT-A-REAL-AGENCY")
        assert REQUIRED_KEYS <= set(r.keys())


class TestRecommendationContract:
    """When a recommendation IS produced, it respects basic invariants.

    The pricing oracle's job is to recommend a quote price that wins. The
    one constant: it must never recommend a price BELOW our cost (we'd
    lose money on every won bid). This catches a class of regressions where
    a markup-percentage refactor accidentally produces negative or zero
    margins on edge cases.
    """

    @pytest.mark.parametrize("cost", [1.00, 5.00, 25.00, 100.00, 500.00])
    def test_recommended_price_at_or_above_cost(self, get_pricing, cost):
        r = get_pricing(description="Nitrile Exam Gloves Medium",
                        quantity=10, cost=cost, department="CCHCS",
                        line_count=5)
        rec = r.get("recommendation") or {}
        quote_price = rec.get("quote_price")
        if quote_price is not None:
            assert quote_price >= cost, (
                f"recommendation.quote_price ({quote_price}) below cost ({cost}) "
                f"— pipeline would lose money on every win at this input"
            )

    def test_zero_cost_handled_safely(self, get_pricing):
        # If cost=0 reaches the pipeline (data bug upstream), recommendation
        # should either decline (no quote_price) or return non-negative.
        # Never a crash.
        r = get_pricing(description="Nitrile Gloves", quantity=10, cost=0,
                        department="CCHCS")
        rec = r.get("recommendation") or {}
        qp = rec.get("quote_price")
        if qp is not None:
            assert qp >= 0


class TestCrossAgencyConsistency:
    """The same item priced across all 4 agencies returns 4 valid responses.

    Catches: an agency-specific code path crashes on one agency but not
    the others (e.g., a missing agency in a hardcoded dict).
    """

    @pytest.mark.parametrize("desc,item_num", [
        ("Nitrile Exam Gloves Medium", "MED1086"),
        ("Adult Brief Large", "FN4368"),
        ("N95 Respirator Mask", "1860"),
    ])
    def test_same_item_all_agencies(self, get_pricing, desc, item_num):
        responses = {}
        for agency in AGENCIES:
            r = get_pricing(description=desc, quantity=10, cost=5.00,
                            item_number=item_num, department=agency,
                            line_count=5)
            assert isinstance(r, dict), f"crash on {agency}"
            responses[agency] = r
        # All 4 must return — no agency silently dropped.
        assert len(responses) == len(AGENCIES)
