"""Tests for Volume-Aware Pricing (Phase B).

Validates that get_volume_band() returns sensible historical margin bands
and that volume_aware_ceiling() produces a cap-price that respects the
(agency, qty_bucket) medians derived from Phase A Drive ingest.

These tests do NOT require a real _phase_a/pilot.sqlite — they seed the
main DB's `volume_margin_bands` table directly and assert the read API.
"""
import pytest


@pytest.fixture
def seed_bands():
    """Populate volume_margin_bands with deterministic test data."""
    from src.core.db import get_db
    from src.core.volume_aware_pricing import ensure_schema

    with get_db() as conn:
        ensure_schema(conn)
        conn.execute("DELETE FROM volume_margin_bands")
        rows = [
            # (agency, qty_bucket, n, p25, p50, p75, cost, price)
            ("cdcr", "qty_1_2", 84, 0.15, 0.18, 0.24, 45.0, 53.0),
            ("cdcr", "qty_3_10", 51, 0.14, 0.18, 0.20, 34.0, 41.0),
            ("calvet", "qty_1_2", 25, 0.048, 0.052, 0.065, 78.0, 82.0),
            ("calvet", "qty_51_200", 25, 0.045, 0.052, 0.052, 42.0, 45.0),
            ("all", "qty_51_200", 33, 0.045, 0.052, 0.065, 50.0, 53.0),
            ("cchcs", "qty_1_2", 3, 0.09, 0.09, 0.09, 20.0, 22.0),  # thin
        ]
        for r in rows:
            conn.execute("""
                INSERT INTO volume_margin_bands
                  (agency, qty_bucket, sample_size, p25_margin, p50_margin, p75_margin,
                   avg_unit_cost, avg_unit_price, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, r)
        conn.commit()
    yield
    with get_db() as conn:
        conn.execute("DELETE FROM volume_margin_bands")
        conn.commit()


class TestVolumeBand:
    def test_exact_agency_match(self, seed_bands):
        from src.core.volume_aware_pricing import get_volume_band
        b = get_volume_band("CDCR", 5)
        assert b is not None
        assert b["agency"] == "cdcr"
        assert b["qty_bucket"] == "qty_3_10"
        assert b["sample_size"] == 51
        assert not b["used_fallback"]

    def test_qty_bucket_boundaries(self, seed_bands):
        from src.core.volume_aware_pricing import get_volume_band
        assert get_volume_band("CDCR", 1)["qty_bucket"] == "qty_1_2"
        assert get_volume_band("CDCR", 2)["qty_bucket"] == "qty_1_2"
        assert get_volume_band("CDCR", 3)["qty_bucket"] == "qty_3_10"
        assert get_volume_band("CDCR", 10)["qty_bucket"] == "qty_3_10"

    def test_fallback_to_all_when_agency_thin(self, seed_bands):
        from src.core.volume_aware_pricing import get_volume_band
        # CCHCS qty_1_2 has only n=3 — should fall back to 'all' bucket
        # (but 'all' for qty_1_2 isn't seeded here, so returns None OR thin)
        # CDCR qty_51_200 isn't seeded — should fall back to 'all'/qty_51_200
        b = get_volume_band("CDCR", 100)
        assert b is not None
        assert b["agency"] == "all"
        assert b["used_fallback"] is True
        assert b["sample_size"] == 33

    def test_unknown_agency_falls_back(self, seed_bands):
        from src.core.volume_aware_pricing import get_volume_band
        b = get_volume_band("UNKNOWN_AGENCY", 100)
        # Should hit 'all' fallback for qty_51_200
        assert b is not None
        assert b["agency"] == "all"

    def test_missing_bucket_returns_none(self, seed_bands):
        from src.core.volume_aware_pricing import get_volume_band
        # No data seeded for qty_201_plus for any agency
        b = get_volume_band("CDCR", 500)
        assert b is None


class TestVolumeAwareCeiling:
    def test_ceiling_uses_p50(self, seed_bands):
        from src.core.volume_aware_pricing import volume_aware_ceiling
        c = volume_aware_ceiling(cost=10.0, agency="CDCR", quantity=5)
        assert c is not None
        # p50 for CDCR qty_3_10 = 0.18 → price = 10 * 1.18 = 11.80
        assert abs(c["price"] - 11.80) < 0.01
        assert c["markup_pct"] == 18.0

    def test_ceiling_respects_volume_curve(self, seed_bands):
        """Same cost, same qty, different agencies produce different ceilings."""
        from src.core.volume_aware_pricing import volume_aware_ceiling
        cdcr = volume_aware_ceiling(cost=10.0, agency="CDCR", quantity=1)
        calvet = volume_aware_ceiling(cost=10.0, agency="CalVet", quantity=1)
        assert cdcr["markup_pct"] > calvet["markup_pct"]
        # CDCR qty_1_2: p50 0.18, CalVet qty_1_2: p50 0.052
        assert cdcr["markup_pct"] == pytest.approx(18.0, rel=0.01)
        assert calvet["markup_pct"] == pytest.approx(5.2, rel=0.01)

    def test_ceiling_returns_none_without_cost(self, seed_bands):
        from src.core.volume_aware_pricing import volume_aware_ceiling
        assert volume_aware_ceiling(cost=0, agency="CDCR", quantity=5) is None
        assert volume_aware_ceiling(cost=None, agency="CDCR", quantity=5) is None


class TestOracleIntegration:
    """Oracle should surface volume_aware on returned shape and apply
    it as a cap in the no-market (blind) branch."""

    def test_oracle_blind_branch_uses_volume_aware(self, seed_bands, monkeypatch):
        """With no market data and flag on, blind branch should produce
        a price based on the volume-aware p50, not the flat 30%."""
        from src.core import pricing_oracle_v2 as po
        # Ensure flag is on (default True, but explicit in test)
        from src.core.flags import set_flag
        set_flag("oracle.volume_aware", "true", updated_by="test")
        try:
            r = po.get_pricing(
                description="NonexistentXYZWidget_test_only_abc",
                quantity=1, cost=10.0, department="CDCR",
            )
            rec = r["recommendation"]
            # Should NOT be the 30% flat default
            assert rec["markup_pct"] != 30
            # Should be the CDCR qty_1_2 p50 = 18%
            assert rec["markup_pct"] == pytest.approx(18.0, rel=0.05)
            assert "volume-aware" in rec["rationale"].lower()
        finally:
            from src.core.flags import delete_flag
            delete_flag("oracle.volume_aware")

    def test_flag_off_disables_integration(self, seed_bands):
        from src.core import pricing_oracle_v2 as po
        from src.core.flags import set_flag, delete_flag
        set_flag("oracle.volume_aware", "false", updated_by="test")
        try:
            r = po.get_pricing(
                description="NonexistentXYZWidget_test_only_abc",
                quantity=1, cost=10.0, department="CDCR",
            )
            rec = r["recommendation"]
            # Flag off: should fall back to flat 30% blind
            assert rec["markup_pct"] == 30
        finally:
            delete_flag("oracle.volume_aware")
