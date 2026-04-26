"""Phase 4.5: yearly win-rate trajectory tests."""

from datetime import datetime

from src.core.db import get_db


def _seed_quote(qnum, status, agency, year, total=100.0):
    created = f"{year}-06-15T12:00:00"
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, institution,
                                line_items, total, created_at, is_test)
            VALUES (?, ?, ?, ?, '[]', ?, ?, 0)
        """, (qnum, status, agency, agency, total, created))
        conn.commit()


class TestYearlyEndpoint:
    def test_empty_returns_empty_years(self, client):
        r = client.get("/api/oracle/win-rate-yearly")
        body = r.get_json()
        assert body["ok"] is True
        assert body["years"] == []

    def test_buckets_by_year(self, client):
        _seed_quote("Y-22-1", "won", "CDCR", 2022)
        _seed_quote("Y-22-2", "lost", "CDCR", 2022)
        _seed_quote("Y-23-1", "won", "CDCR", 2023)
        _seed_quote("Y-23-2", "won", "CDCR", 2023)
        _seed_quote("Y-23-3", "lost", "CDCR", 2023)
        r = client.get("/api/oracle/win-rate-yearly")
        body = r.get_json()
        years = {y["year"]: y for y in body["years"]}
        assert "2022" in years
        assert "2023" in years
        assert years["2022"]["wins"] == 1
        assert years["2022"]["losses"] == 1
        assert years["2022"]["win_rate_pct"] == 50.0
        assert years["2023"]["wins"] == 2
        assert years["2023"]["losses"] == 1
        assert years["2023"]["win_rate_pct"] == 66.7

    def test_agency_filter(self, client):
        _seed_quote("Y-FA-1", "won", "Veterans Home Barstow", 2024)
        _seed_quote("Y-FA-2", "lost", "Veterans Home Barstow", 2024)
        _seed_quote("Y-FA-3", "won", "CDCR Sacramento", 2024)
        r = client.get("/api/oracle/win-rate-yearly?agency=Barstow")
        body = r.get_json()
        years = {y["year"]: y for y in body["years"]}
        assert "2024" in years
        # Only Barstow quotes counted
        assert years["2024"]["quotes"] == 2
        assert years["2024"]["wins"] == 1

    def test_orders_chronologically(self, client):
        _seed_quote("Y-ORD-26", "won", "X", 2026)
        _seed_quote("Y-ORD-22", "won", "X", 2022)
        _seed_quote("Y-ORD-24", "won", "X", 2024)
        r = client.get("/api/oracle/win-rate-yearly")
        years_in_order = [y["year"] for y in r.get_json()["years"]]
        # Sorted ascending
        assert years_in_order == sorted(years_in_order)

    def test_won_value_aggregated(self, client):
        _seed_quote("Y-VAL-1", "won", "X", 2025, total=1000)
        _seed_quote("Y-VAL-2", "won", "X", 2025, total=2500)
        _seed_quote("Y-VAL-3", "lost", "X", 2025, total=400)
        r = client.get("/api/oracle/win-rate-yearly")
        years = {y["year"]: y for y in r.get_json()["years"]}
        assert years["2025"]["won_value"] == 3500.0
        assert years["2025"]["lost_value"] == 400.0
