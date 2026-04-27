"""Phase 1.6 PR3a: /api/quote/<type>/<id>/fill-plan endpoint smoke tests."""

import json
from unittest.mock import patch

import pytest


def _seed_pc(seed_db_quote):
    """Use existing fixture to drop a PC row in the test DB."""
    seed_db_quote(
        quote_number="PC-FP-1",
        agency="CDCR Folsom",
        total=1234.56,
    )


class TestFillPlanEndpoint:
    def test_unknown_quote_type_returns_400(self, client):
        r = client.get("/api/quote/bogus/abc/fill-plan")
        assert r.status_code == 400
        d = r.get_json()
        assert d.get("ok") is False

    def test_pc_route_returns_ok_envelope(self, client, seed_db_quote):
        _seed_pc(seed_db_quote)
        # Use the seeded quote_number as the id — seed_db_quote may not return id
        r = client.get("/api/quote/pc/PC-FP-1/fill-plan")
        # Accept either 200 (found) or 200 with empty plan (id mismatch ok for smoke)
        assert r.status_code == 200
        d = r.get_json()
        assert d.get("ok") is True
        assert "plan" in d
        # Required fields on the plan envelope
        plan = d["plan"]
        for key in ("quote_id", "quote_type", "items", "total_required",
                    "total_ready", "total_warning", "total_blocked",
                    "contract_source", "contract_summary"):
            assert key in plan, f"missing key: {key}"
        assert plan["quote_type"] == "pc"

    def test_rfq_route_returns_ok_envelope(self, client):
        r = client.get("/api/quote/rfq/nonexistent-rid/fill-plan")
        assert r.status_code == 200
        d = r.get_json()
        assert d.get("ok") is True
        assert d["plan"]["quote_type"] == "rfq"
        # nonexistent quote → empty plan, no items
        assert d["plan"]["total_required"] == 0
