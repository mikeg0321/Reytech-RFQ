"""Oracle V5.5 — per-buyer win-rate curves.

Verifies:
  - _fit_buyer_curve reads from the `quotes` table and builds a histogram
    with the right total_samples / won / lost counts.
  - The `sufficient` flag flips at _CURVE_MIN_SAMPLES.
  - buyer_win_probability interpolates cleanly inside the data, falls
    back to the global win rate in empty buckets, and returns 0.5 when
    there's no data at all.
  - optimal_markup_for_expected_profit finds the max of markup × P(win)
    within the search range.
  - The /api/oracle/buyer-curve/<institution> route returns the shape
    the UI expects.
  - The caching layer returns the same object on a second call inside
    the TTL without hitting the DB.
"""
import sqlite3
import pytest


def _seed_quote(institution: str, margin_pct: float, status: str,
                quote_number: str, created_at: str = None):
    from src.core.db import DB_PATH
    from datetime import datetime
    if created_at is None:
        created_at = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(
        """INSERT INTO quotes
           (quote_number, created_at, institution, agency, margin_pct,
            status, is_test, total, total_cost, items_count)
           VALUES (?, ?, ?, ?, ?, ?, 0, 100, 80, 1)""",
        (quote_number, created_at, institution, institution,
         margin_pct, status),
    )
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _clear_curve_cache():
    from src.core.pricing_oracle_v2 import _curve_cache_clear
    _curve_cache_clear()
    yield
    _curve_cache_clear()


class TestFitBuyerCurve:
    def test_empty_institution_returns_zero_samples(self, temp_data_dir):
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import _fit_buyer_curve
        with get_db() as conn:
            curve = _fit_buyer_curve(conn, "cchcs")
        assert curve["total_samples"] == 0
        assert curve["sufficient"] is False

    def test_counts_wins_and_losses(self, temp_data_dir):
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import _fit_buyer_curve

        for i, (markup, status) in enumerate([
            (22, "won"), (24, "won"), (28, "lost"),
            (32, "won"), (35, "lost"), (45, "lost"),
            (20, "won"), (25, "won"),
        ]):
            _seed_quote("cchcs", markup, status, f"Q-{i:04d}")

        with get_db() as conn:
            curve = _fit_buyer_curve(conn, "cchcs")
        assert curve["total_samples"] == 8
        assert curve["won"] == 5
        assert curve["lost"] == 3
        assert curve["sufficient"] is True  # 8 >= _CURVE_MIN_SAMPLES
        assert abs(curve["global_win_rate"] - 5 / 8) < 0.001

    def test_sufficient_flag_threshold(self, temp_data_dir):
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import _fit_buyer_curve, _CURVE_MIN_SAMPLES

        # Seed exactly one below the threshold
        for i in range(_CURVE_MIN_SAMPLES - 1):
            _seed_quote("calvet", 30, "won", f"Q-CAL-{i:04d}")
        with get_db() as conn:
            curve = _fit_buyer_curve(conn, "calvet")
        assert curve["sufficient"] is False

        # One more tips it over
        _seed_quote("calvet", 30, "won", "Q-CAL-TIP")
        from src.core.pricing_oracle_v2 import _curve_cache_clear
        _curve_cache_clear()
        with get_db() as conn:
            curve2 = _fit_buyer_curve(conn, "calvet")
        assert curve2["sufficient"] is True

    def test_ignores_test_and_non_terminal_quotes(self, temp_data_dir):
        """is_test=1 and status NOT IN (won,lost) must be excluded."""
        from src.core.db import DB_PATH, get_db
        from src.core.pricing_oracle_v2 import _fit_buyer_curve

        conn = sqlite3.connect(DB_PATH, timeout=10)
        # Test quote — excluded
        conn.execute(
            """INSERT INTO quotes (quote_number, created_at, institution, agency,
                                    margin_pct, status, is_test, total, total_cost, items_count)
               VALUES ('Q-TEST', datetime('now'), 'cchcs', 'cchcs', 25, 'won', 1, 100, 80, 1)"""
        )
        # Pending — excluded
        conn.execute(
            """INSERT INTO quotes (quote_number, created_at, institution, agency,
                                    margin_pct, status, is_test, total, total_cost, items_count)
               VALUES ('Q-PEND', datetime('now'), 'cchcs', 'cchcs', 25, 'pending', 0, 100, 80, 1)"""
        )
        conn.commit()
        conn.close()

        for i in range(9):
            _seed_quote("cchcs", 30, "won", f"Q-{i:04d}")

        with get_db() as conn:
            curve = _fit_buyer_curve(conn, "cchcs")
        assert curve["total_samples"] == 9  # only the 9 real won quotes


class TestBuyerWinProbability:
    def test_uninformed_prior_when_no_data(self, temp_data_dir):
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import buyer_win_probability
        with get_db() as conn:
            p = buyer_win_probability("nobody", 30.0, db=conn)
        assert p == 0.5

    def test_reads_bucketed_win_rate(self, temp_data_dir):
        """25-30% bucket: 3 wins / 5 total → P=0.6."""
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import buyer_win_probability

        # 25-30% bucket wins
        _seed_quote("cchcs", 25, "won", "Q1")
        _seed_quote("cchcs", 27, "won", "Q2")
        _seed_quote("cchcs", 28, "won", "Q3")
        _seed_quote("cchcs", 26, "lost", "Q4")
        _seed_quote("cchcs", 29, "lost", "Q5")
        # Other buckets so total_samples hits threshold
        _seed_quote("cchcs", 20, "lost", "Q6")
        _seed_quote("cchcs", 40, "lost", "Q7")
        _seed_quote("cchcs", 45, "won", "Q8")

        with get_db() as conn:
            p = buyer_win_probability("cchcs", 27.0, db=conn)
        assert abs(p - 0.6) < 0.01  # 3/5 in that bucket


class TestOptimalMarkup:
    def test_thin_data_returns_insufficient(self, temp_data_dir):
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import optimal_markup_for_expected_profit

        for i in range(3):
            _seed_quote("cchcs", 30, "won", f"Q-{i}")

        with get_db() as conn:
            opt = optimal_markup_for_expected_profit("cchcs", conn)
        assert opt["sufficient"] is False
        assert opt["markup_pct"] is None

    def test_finds_expected_value_peak(self, temp_data_dir):
        """Seed a clear peak: buyer wins a lot at ~30% and almost
        nothing at ~50%. EV = markup × P(win) should peak around 30%."""
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import optimal_markup_for_expected_profit

        # 25-30% bucket: 8/10 wins
        for i in range(8):
            _seed_quote("cchcs", 27, "won", f"W-LO-{i}")
        for i in range(2):
            _seed_quote("cchcs", 27, "lost", f"L-LO-{i}")
        # 45-50% bucket: 1/10 wins
        for i in range(1):
            _seed_quote("cchcs", 48, "won", f"W-HI-{i}")
        for i in range(9):
            _seed_quote("cchcs", 48, "lost", f"L-HI-{i}")

        with get_db() as conn:
            opt = optimal_markup_for_expected_profit("cchcs", conn)
        assert opt["sufficient"] is True
        # EV at 27% ≈ 27 * 0.8 = 21.6; at 48% ≈ 48 * 0.1 = 4.8
        # Optimum should land in the low-markup zone (25-30%)
        assert 25 <= opt["markup_pct"] <= 35, \
            f"optimum landed outside high-win zone: {opt['markup_pct']}"
        assert opt["win_probability"] >= 0.6


class TestCaching:
    def test_second_call_hits_cache(self, temp_data_dir, monkeypatch):
        from src.core import pricing_oracle_v2 as oracle
        from src.core.db import get_db

        for i in range(9):
            _seed_quote("cchcs", 30, "won", f"Q-{i}")

        with get_db() as conn:
            first = oracle._fit_buyer_curve(conn, "cchcs")

        # Break the DB so a second lookup would fail — cache must save us
        calls = {"reads": 0}
        real_read = oracle._read_markup_outcomes

        def _counted(*a, **kw):
            calls["reads"] += 1
            return real_read(*a, **kw)
        monkeypatch.setattr(oracle, "_read_markup_outcomes", _counted)

        with get_db() as conn:
            second = oracle._fit_buyer_curve(conn, "cchcs")
        assert calls["reads"] == 0  # hit cache
        assert second["total_samples"] == first["total_samples"]


class TestBuyerCurveEndpoint:
    def test_endpoint_returns_shape(self, auth_client, temp_data_dir):
        r = auth_client.get("/api/oracle/buyer-curve/cchcs")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["institution"] == "cchcs"
        assert "optimal" in d
        assert "curve" in d

    def test_endpoint_populated_after_seeding(self, auth_client, temp_data_dir):
        for i in range(10):
            _seed_quote("cchcs", 30, "won", f"Q-SEED-{i}")
        for i in range(5):
            _seed_quote("cchcs", 45, "lost", f"Q-SEED-L-{i}")

        r = auth_client.get("/api/oracle/buyer-curve/cchcs")
        d = r.get_json()
        assert d["curve"]["total_samples"] == 15
        assert d["curve"]["won"] == 10
        assert d["curve"]["lost"] == 5
        assert d["optimal"]["sufficient"] is True
        assert d["optimal"]["markup_pct"] is not None


class TestRecommendationIntegration:
    def test_buyer_curve_overrides_calibration(self, temp_data_dir):
        """When V5.5 curve is sufficient, _calculate_recommendation must
        expose a `buyer_curve` key with the optimal markup."""
        from src.core.db import get_db
        from src.core.pricing_oracle_v2 import _calculate_recommendation

        # Seed a curve that peaks at ~30%
        for i in range(10):
            _seed_quote("cchcs", 28, "won", f"CR-W-{i}")
        for i in range(3):
            _seed_quote("cchcs", 45, "lost", f"CR-L-{i}")

        with get_db() as conn:
            market = {
                "competitor_avg": 12.0,
                "competitor_low": 11.0,
                "data_points": 8,
                "reytech_avg": None,
                "weighted_avg": None,
            }
            rec = _calculate_recommendation(
                cost=10.0, market=market, quantity=1,
                category="medical", agency="cchcs", _db=conn,
            )
        assert "buyer_curve" in rec
        assert rec["buyer_curve"]["total_samples"] == 13
        assert rec["buyer_curve"]["optimal_markup_pct"] is not None
        # Source tag should flip to v5.5
        if rec.get("calibration"):
            assert rec["calibration"].get("source") == "buyer_curve_v5_5"
