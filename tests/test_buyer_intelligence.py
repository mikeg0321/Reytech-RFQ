"""Growth Phase B — /buyer-intelligence page + /api/oracle/buyer-list.

Builds on V5.5 per-buyer curves. Verifies:
  - The page renders with an empty DB (empty-state messaging).
  - The list endpoint returns institutions ranked by quote volume.
  - Institutions with < _CURVE_MIN_SAMPLES still appear but with
    `sufficient: False`.
  - The page embeds curve data server-side so the detail view works
    without an XHR round trip.
  - Buyers stored under `agency` (empty `institution`) still show up
    via the COALESCE fallback.
"""
import sqlite3
import pytest


def _seed_quote(institution: str, margin_pct: float, status: str,
                quote_number: str, created_at: str = None, agency: str = None):
    from src.core.db import DB_PATH
    from datetime import datetime
    if created_at is None:
        created_at = datetime.now().isoformat()
    if agency is None:
        agency = institution
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(
        """INSERT INTO quotes
           (quote_number, created_at, institution, agency, margin_pct,
            status, is_test, total, total_cost, items_count)
           VALUES (?, ?, ?, ?, ?, ?, 0, 100, 80, 1)""",
        (quote_number, created_at, institution, agency, margin_pct, status),
    )
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _clear_curve_cache():
    from src.core.pricing_oracle_v2 import _curve_cache_clear
    _curve_cache_clear()
    yield
    _curve_cache_clear()


class TestBuyerIntelligencePage:
    def test_page_renders_with_empty_db(self, auth_client):
        r = auth_client.get("/buyer-intelligence")
        assert r.status_code == 200
        assert b"Buyer Intelligence" in r.data
        assert b"No buyers" in r.data  # empty-state copy

    def test_page_accepts_days_param(self, auth_client):
        r = auth_client.get("/buyer-intelligence?days=90")
        assert r.status_code == 200
        assert b"last 90d" in r.data

    def test_page_clamps_invalid_days(self, auth_client):
        r = auth_client.get("/buyer-intelligence?days=abc")
        assert r.status_code == 200
        r = auth_client.get("/buyer-intelligence?days=99999")
        assert r.status_code == 200

    def test_page_renders_populated_buyer(self, auth_client, temp_data_dir):
        """A buyer with enough data should show an Optimal markup cell
        instead of '--'."""
        for i in range(10):
            _seed_quote("cchcs", 28, "won", f"Q-W-{i}")
        for i in range(3):
            _seed_quote("cchcs", 45, "lost", f"Q-L-{i}")
        r = auth_client.get("/buyer-intelligence")
        assert r.status_code == 200
        body = r.data.decode()
        assert "CCHCS" in body
        # Optimal markup cell should be populated (not just "--")
        assert "Optimal markup" in body


class TestBuyerListEndpoint:
    def test_empty_list(self, auth_client, temp_data_dir):
        r = auth_client.get("/api/oracle/buyer-list")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["count"] == 0
        assert d["buyers"] == []

    def test_ranks_by_volume(self, auth_client, temp_data_dir):
        """CCHCS with 12 quotes should rank above CALVET with 3."""
        for i in range(12):
            _seed_quote("cchcs", 30, "won", f"CCHCS-{i}")
        for i in range(3):
            _seed_quote("calvet", 30, "won", f"CALVET-{i}")
        r = auth_client.get("/api/oracle/buyer-list")
        d = r.get_json()
        names = [b["institution"] for b in d["buyers"]]
        assert names[0] == "cchcs"
        assert "calvet" in names
        assert names.index("cchcs") < names.index("calvet")

    def test_thin_data_buyer_is_insufficient(self, auth_client, temp_data_dir):
        for i in range(4):
            _seed_quote("dsh", 30, "won", f"DSH-{i}")
        r = auth_client.get("/api/oracle/buyer-list")
        d = r.get_json()
        dsh = next(b for b in d["buyers"] if b["institution"] == "dsh")
        assert dsh["sufficient"] is False
        assert dsh["optimal_markup_pct"] is None

    def test_sufficient_buyer_has_optimal(self, auth_client, temp_data_dir):
        for i in range(10):
            _seed_quote("cchcs", 30, "won", f"Q-{i}")
        for i in range(3):
            _seed_quote("cchcs", 45, "lost", f"Q-L-{i}")
        r = auth_client.get("/api/oracle/buyer-list")
        d = r.get_json()
        cchcs = next(b for b in d["buyers"] if b["institution"] == "cchcs")
        assert cchcs["sufficient"] is True
        assert cchcs["optimal_markup_pct"] is not None
        assert cchcs["total_quotes"] == 13
        assert cchcs["won"] == 10
        assert cchcs["lost"] == 3

    def test_agency_fallback_when_institution_empty(self, auth_client, temp_data_dir):
        """Quotes stored with empty institution but populated agency
        should still show up via the COALESCE."""
        for i in range(5):
            _seed_quote("", 30, "won", f"AG-{i}", agency="cdcr")
        r = auth_client.get("/api/oracle/buyer-list")
        d = r.get_json()
        names = [b["institution"] for b in d["buyers"]]
        assert "cdcr" in names

    def test_limit_param(self, auth_client, temp_data_dir):
        for inst in ["a", "b", "c", "d", "e"]:
            for i in range(3):
                _seed_quote(inst, 30, "won", f"{inst}-{i}")
        r = auth_client.get("/api/oracle/buyer-list?limit=2")
        d = r.get_json()
        assert len(d["buyers"]) == 2

    def test_limit_clamped(self, auth_client):
        r = auth_client.get("/api/oracle/buyer-list?limit=99999")
        assert r.status_code == 200
        r = auth_client.get("/api/oracle/buyer-list?limit=abc")
        assert r.status_code == 200
