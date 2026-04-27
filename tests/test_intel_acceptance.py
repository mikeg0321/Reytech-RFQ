"""Phase 4.7.3: acceptance audit log endpoint tests."""

import json


def _post_decision(client, **overrides):
    body = {
        "description": "Propet Walker Strap",
        "agency": "CDCR Sacramento",
        "category": "footwear-orthopedic",
        "flavor": "B",
        "engine_markup_pct": 22.0,
        "engine_price": 122.00,
        "suggested_markup_pct": 11.0,
        "suggested_price": 111.00,
        "final_price": 111.00,
        "accepted": True,
    }
    body.update(overrides)
    return client.post(
        "/api/oracle/intel-acceptance",
        data=json.dumps(body),
        content_type="application/json",
    )


class TestLogEndpoint:
    def test_missing_required_returns_400(self, client):
        r = client.post(
            "/api/oracle/intel-acceptance",
            data=json.dumps({"description": "x"}),
            content_type="application/json",
        )
        assert r.status_code == 400

    def test_bad_flavor_returns_400(self, client):
        r = _post_decision(client, flavor="ZZZ")
        assert r.status_code == 400

    def test_accepted_decision_persists(self, client):
        r = _post_decision(client, accepted=True)
        body = r.get_json()
        assert body["ok"] is True
        assert body["accepted"] is True
        assert body["id"] > 0

    def test_rejected_decision_persists(self, client):
        r = _post_decision(client, accepted=False)
        body = r.get_json()
        assert body["accepted"] is False


class TestStatsEndpoint:
    def test_empty_returns_zero(self, client):
        r = client.get("/api/oracle/intel-acceptance-stats")
        body = r.get_json()
        assert body["ok"] is True
        assert body["total"] == 0
        assert body["by_category"] == []

    def test_counts_per_category(self, client):
        # 3 footwear accepts + 1 reject + 2 incontinence accepts
        for _ in range(3):
            _post_decision(client, accepted=True)
        _post_decision(client, accepted=False)
        for _ in range(2):
            _post_decision(client, category="incontinence",
                           description="TENA Brief", accepted=True)
        r = client.get("/api/oracle/intel-acceptance-stats")
        body = r.get_json()
        assert body["total"] == 6
        assert body["accepted"] == 5
        assert body["rejected"] == 1
        cats = {c["category"]: c for c in body["by_category"]}
        assert cats["footwear-orthopedic"]["total"] == 4
        assert cats["footwear-orthopedic"]["accepted"] == 3
        assert cats["footwear-orthopedic"]["rejected"] == 1
        assert cats["footwear-orthopedic"]["accept_rate_pct"] == 75.0
        assert cats["incontinence"]["accept_rate_pct"] == 100.0

    def test_category_filter_narrows(self, client):
        _post_decision(client, accepted=True)
        _post_decision(client, category="incontinence",
                       description="TENA", accepted=False)
        r = client.get(
            "/api/oracle/intel-acceptance-stats?category=incontinence"
        )
        body = r.get_json()
        assert body["total"] == 1
        assert len(body["by_category"]) == 1
        assert body["by_category"][0]["category"] == "incontinence"

    def test_flavor_filter_narrows(self, client):
        _post_decision(client, flavor="A", accepted=True)
        _post_decision(client, flavor="B", accepted=True)
        r = client.get("/api/oracle/intel-acceptance-stats?flavor=A")
        body = r.get_json()
        assert body["total"] == 1
