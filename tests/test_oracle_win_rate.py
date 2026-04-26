"""Phase 4.4: per-agency win-rate analytics tests.

Pin the canonicalization + aggregation rules so a future regression
can't silently scramble agency rollups.
"""

import json
from datetime import datetime, timedelta

import pytest

from src.core.db import get_db


# Inlined helper — importing from routes_oracle_win_rate at test time
# would re-register its @bp.route decorators against the test app and
# trigger 'View function mapping is overwriting an existing endpoint'.
# Keep this in lockstep with the function in routes_oracle_win_rate.py.
def _normalize_agency(raw):
    s = (raw or "").strip().lower()
    if not s:
        return ""
    out = "".join(c if c.isalnum() else " " for c in s)
    tokens = [t for t in out.split() if t]
    stop = {"of", "the", "and", "for", "ca", "california", "dept",
            "department", "inc", "co", "company", "corp", "corporation",
            "rehab", "rehabilitation"}
    tokens = [t for t in tokens if t not in stop]
    return " ".join(tokens)


def _seed_quote(qnum, status, agency, total=100.0, days_ago=10):
    created = (datetime.now() - timedelta(days=days_ago)).isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, institution,
                                line_items, total, created_at, is_test)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (qnum, status, agency, agency, "[]", total, created))
        conn.commit()


class TestNormalizeAgency:
    def test_collapses_punctuation(self):
        a = _normalize_agency("California State Prison - Sacramento")
        b = _normalize_agency("CA State Prison Sacramento")
        # 'california' is in the stopword list, so the two strings end
        # up sharing the same content tokens after normalization.
        assert a == b

    def test_drops_dept_corrections_rehab_stopwords(self):
        a = _normalize_agency("Dept of Corrections & Rehabilitation")
        b = _normalize_agency("California Department of Corrections and Rehabilitation")
        assert "corrections" in a
        assert a == b

    def test_blank_returns_blank(self):
        assert _normalize_agency("") == ""
        assert _normalize_agency(None) == ""

    def test_preserves_facility_specifics(self):
        # Two distinct facilities should stay distinct
        barstow = _normalize_agency("Veterans Home of California - Barstow")
        chula = _normalize_agency("Veterans Home of California - Chula Vista")
        assert "barstow" in barstow
        assert "chula" in chula
        assert barstow != chula


class TestEndpoint:
    def test_empty_db_returns_zero(self, client):
        r = client.get("/api/oracle/win-rate-by-agency?days=365")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["overall"]["quotes"] == 0
        assert body["agencies"] == []

    def test_aggregates_per_canonical_agency(self, client):
        _seed_quote("WR-1", "won", "CDCR", total=1000)
        _seed_quote("WR-2", "won", "CDCR", total=500)
        _seed_quote("WR-3", "lost", "CDCR", total=2000)
        _seed_quote("WR-4", "lost", "CalVet", total=300)
        r = client.get("/api/oracle/win-rate-by-agency?days=365&min_quotes=1")
        body = r.get_json()
        assert body["ok"] is True
        assert body["overall"]["quotes"] == 4
        assert body["overall"]["wins"] == 2
        assert body["overall"]["losses"] == 2
        agencies_by_canon = {a["canonical_name"]: a for a in body["agencies"]}
        assert "cdcr" in agencies_by_canon
        cdcr = agencies_by_canon["cdcr"]
        assert cdcr["quotes"] == 3
        assert cdcr["wins"] == 2
        assert cdcr["losses"] == 1
        assert cdcr["win_rate_pct"] == 66.7
        assert cdcr["won_value"] == 1500.0

    def test_respects_min_quotes_filter(self, client):
        _seed_quote("WR-MIN-1", "won", "AgencyA", total=10)
        _seed_quote("WR-MIN-2", "won", "AgencyB", total=10)
        _seed_quote("WR-MIN-3", "won", "AgencyB", total=10)
        _seed_quote("WR-MIN-4", "won", "AgencyB", total=10)
        r = client.get("/api/oracle/win-rate-by-agency?days=365&min_quotes=3")
        body = r.get_json()
        # AgencyA has only 1 quote → filtered out
        canonicals = {a["canonical_name"] for a in body["agencies"]}
        assert any("agencyb" in c for c in canonicals)
        assert not any("agencya" == c for c in canonicals)

    def test_date_window_filter(self, client):
        _seed_quote("WR-OLD", "won", "OldAgency", total=10, days_ago=400)
        _seed_quote("WR-NEW", "won", "OldAgency", total=10, days_ago=30)
        r = client.get("/api/oracle/win-rate-by-agency?days=180&min_quotes=1")
        body = r.get_json()
        # Only the new quote should count
        agencies_by_canon = {a["canonical_name"]: a for a in body["agencies"]}
        old = agencies_by_canon.get("oldagency")
        assert old is not None
        assert old["quotes"] == 1

    def test_sorts_by_quote_count_desc(self, client):
        for i in range(5):
            _seed_quote(f"WR-SORT-A-{i}", "won", "BusyAgency", total=10)
        for i in range(2):
            _seed_quote(f"WR-SORT-B-{i}", "won", "QuietAgency", total=10)
        r = client.get("/api/oracle/win-rate-by-agency?days=365&min_quotes=1")
        body = r.get_json()
        assert body["agencies"][0]["canonical_name"] == "busyagency"
