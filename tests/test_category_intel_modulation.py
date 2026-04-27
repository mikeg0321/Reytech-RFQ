"""Phase 4.7: Pricing engine integration tests for the category-intel
modulation layer.

Covers all three flavors (A: auto-lower, B: suggest-only, C: block)
plus OFF and idempotency. Seeds prod-shape footwear losses and
incontinence wins so the classifier+rollup actually fire.
"""

import json
import os
import sqlite3

import pytest

from src.core.db import get_db, DB_PATH


def _seed_quote(qnum, status, items, agency="X"):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, institution,
                                line_items, total, created_at, is_test)
            VALUES (?, ?, ?, ?, ?, 100.0, '2025-06-15T12:00:00', 0)
        """, (qnum, status, agency, agency, json.dumps(items)))
        conn.commit()


def _seed_loss_bucket(prefix="FW", count=6):
    """Seed `count` footwear losses so danger fires."""
    for i in range(count):
        _seed_quote(
            f"{prefix}-{i}", "lost",
            [{"description": "Propet M3705 Walker Strap"}]
        )


def _seed_win_bucket(prefix="WIN", count=6):
    """Seed `count` incontinence wins so WIN BUCKET fires."""
    for i in range(count):
        _seed_quote(
            f"{prefix}-{i}", "won",
            [{"description": "TENA ProSkin Adult Brief XL"}]
        )


def _live_db():
    """Return a sqlite connection to the test-isolated DB.

    Uses get_db() under the hood so pytest's per-test DB patching is
    respected — `sqlite3.connect(DB_PATH)` would resolve the original
    constant at module load time, missing the override.
    """
    return get_db()


@pytest.fixture(autouse=True)
def reset_flavor():
    """Each test starts with no env override."""
    prev = os.environ.pop("CATEGORY_INTEL_FLAVOR", None)
    yield
    if prev is None:
        os.environ.pop("CATEGORY_INTEL_FLAVOR", None)
    else:
        os.environ["CATEGORY_INTEL_FLAVOR"] = prev


class TestUncategorized:
    def test_uncategorized_description_records_no_signal(self, client):
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Some Random Industrial Widget XYZ",
                                 "CDCR", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci["category"] == "uncategorized"
        assert ci["danger"] is False
        assert ci["win_bucket"] is False
        # Engine markup unchanged
        assert rec["markup_pct"] == 22.0


class TestFlavorB_Suggest:
    def test_default_flavor_is_B(self, client):
        from src.core.category_intel_modulation import _get_active_flavor
        os.environ.pop("CATEGORY_INTEL_FLAVOR", None)
        assert _get_active_flavor() == "B"

    def test_loss_bucket_suggests_alternative(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "B"
        _seed_loss_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        # Engine recommendation is unchanged
        assert rec["markup_pct"] == 22.0
        assert rec["quote_price"] == 122.0
        # But suggested alternative is present
        assert ci["danger"] is True
        assert "suggested_alternative" in ci
        alt = ci["suggested_alternative"]
        assert alt["markup_pct"] == 11.0  # 22 × 0.5
        assert alt["quote_price"] == 111.0  # 100 × 1.11
        assert "Damping" in alt["rationale"]

    def test_no_change_when_below_threshold(self, client):
        # Only 4 quotes — below n>=5 floor
        os.environ["CATEGORY_INTEL_FLAVOR"] = "B"
        for i in range(4):
            _seed_quote(f"LOW-{i}", "lost",
                        [{"description": "Propet Walker"}])
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci["danger"] is False
        assert "suggested_alternative" not in ci

    def test_win_bucket_records_signal_no_modulation(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "B"
        _seed_win_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "TENA Brief", "X", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci["win_bucket"] is True
        assert "WIN BUCKET" in ci["warning_text"]
        # No suggested_alternative for wins
        assert "suggested_alternative" not in ci
        # Engine unchanged
        assert rec["markup_pct"] == 22.0


class TestFlavorA_AutoLower:
    def test_loss_bucket_auto_lowers_markup(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "A"
        _seed_loss_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0,
               "rationale": "Engine baseline"}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci["danger"] is True
        assert ci["action"] == "auto_lowered"
        # Engine markup_pct is now reduced
        assert rec["markup_pct"] == 11.0  # 22 * 0.5
        # Original retained for audit
        assert rec["markup_pct_pre_intel"] == 22.0
        assert rec["quote_price_pre_intel"] == 122.0
        # Price recomputed against cost
        assert rec["quote_price"] == 111.0
        # Rationale updated
        assert "Auto-lowered" in rec["rationale"]

    def test_floor_of_5_pct_over_cost(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "A"
        _seed_loss_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        # Engine started at 8% — half is 4% which is below the 5% floor
        rec = {"markup_pct": 8.0, "quote_price": 108.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        # Floor enforced
        assert rec["markup_pct"] == 5.0


class TestFlavorC_Block:
    def test_block_only_at_severe_loss_rate(self, client):
        # Need n>=10 AND rate < 8% for hard-block to fire
        os.environ["CATEGORY_INTEL_FLAVOR"] = "C"
        for i in range(10):
            _seed_quote(f"BL-{i}", "lost",
                        [{"description": "Propet Walker"}])
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci.get("block") is True
        assert ci["action"] == "blocked"
        assert "DO NOT BID" in ci["block_reason"]

    def test_falls_through_to_suggest_when_below_block_bar(self, client):
        # 6 losses at 0% rate → danger=true but quotes < 10 floor
        os.environ["CATEGORY_INTEL_FLAVOR"] = "C"
        _seed_loss_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        # Not hard-blocked but still suggested
        assert ci.get("block") is not True
        assert "suggested_alternative" in ci


class TestFlavorOFF:
    def test_off_disables_all_modulation(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "OFF"
        _seed_loss_bucket(count=10)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        ci = rec["category_intel"]
        assert ci["active"] is False
        assert ci["flavor"] == "OFF"
        # Engine fully unchanged
        assert rec["markup_pct"] == 22.0
        assert rec["quote_price"] == 122.0


class TestIdempotency:
    def test_double_apply_does_not_compound(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "A"
        _seed_loss_bucket(count=6)
        from src.core.category_intel_modulation import apply_category_intel
        rec = {"markup_pct": 22.0, "quote_price": 122.0}
        with _live_db() as db:
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
            first_markup = rec["markup_pct"]
            apply_category_intel(rec, "Propet Walker", "X", db, cost=100.0)
        # Second call must NOT re-damp 11 → 5.5
        assert rec["markup_pct"] == first_markup


class TestUnknownFlavorFallsBackToB:
    def test_garbage_env_value_treated_as_B(self, client):
        os.environ["CATEGORY_INTEL_FLAVOR"] = "ZZZ"
        from src.core.category_intel_modulation import _get_active_flavor
        assert _get_active_flavor() == "B"
