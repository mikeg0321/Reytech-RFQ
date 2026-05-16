"""GET /spine/queue/rejected — triage surface for missed-bid silent-drops.

Closes the mode-c missed-bid class structurally: every rejection emits a
row, and this route + reason_code filter is the operator/watcher entry
point. Test isolation via make_spine_blueprint(db_path, auth_decorator=None).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    IngestRejection,
    init_db,
    write_ingest_rejection,
)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_queue.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def _seed(db_path: str, n: int, *, reason_code="parse_failed",
          base_ts=datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)) -> list[str]:
    ids = []
    for i in range(n):
        rid = f"rej_seed_{reason_code}_{i:03d}"
        write_ingest_rejection(db_path, IngestRejection(
            rejection_id=rid,
            source_email_id=f"msg{i:03d}",
            sender_email="argarin@cchcs.ca.gov",
            subject=f"Subject {i}",
            reason_code=reason_code,
            reason_detail=f"detail {i}",
            rejected_at=base_ts.replace(minute=i % 60),
        ))
        ids.append(rid)
    return ids


# ── Empty state ──────────────────────────────────────────────────────


def test_empty_returns_zero_rejections(client):
    r = client.get("/spine/queue/rejected")
    assert r.status_code == 200
    body = r.get_json()
    assert body == {
        "count": 0,
        "limit": 50,
        "reason_code_filter": None,
        "rejections": [],
    }


# ── Filled state ─────────────────────────────────────────────────────


def test_returns_rejections_newest_first(client, db_path):
    _seed(db_path, n=3)
    r = client.get("/spine/queue/rejected")
    assert r.status_code == 200
    body = r.get_json()
    assert body["count"] == 3
    # newest minute (2) appears first
    ids = [row["rejection_id"] for row in body["rejections"]]
    assert ids == ["rej_seed_parse_failed_002",
                   "rej_seed_parse_failed_001",
                   "rej_seed_parse_failed_000"]


def test_reason_code_filter_restricts_result(client, db_path):
    _seed(db_path, n=2, reason_code="parse_failed")
    _seed(db_path, n=1, reason_code="tax_lookup_failed",
          base_ts=datetime(2026, 5, 15, 13, 0, tzinfo=timezone.utc))
    r = client.get("/spine/queue/rejected?reason_code=tax_lookup_failed")
    assert r.status_code == 200
    body = r.get_json()
    assert body["count"] == 1
    assert body["reason_code_filter"] == "tax_lookup_failed"
    assert body["rejections"][0]["reason_code"] == "tax_lookup_failed"


def test_limit_param_caps_result(client, db_path):
    _seed(db_path, n=10)
    r = client.get("/spine/queue/rejected?limit=3")
    assert r.status_code == 200
    body = r.get_json()
    assert body["count"] == 3
    assert body["limit"] == 3


# ── Bad-request handling ─────────────────────────────────────────────


def test_invalid_limit_returns_400(client):
    r = client.get("/spine/queue/rejected?limit=0")
    assert r.status_code == 400
    assert r.get_json()["error"] == "bad_request"


def test_non_integer_limit_returns_400(client):
    r = client.get("/spine/queue/rejected?limit=abc")
    assert r.status_code == 400
    assert "integer" in r.get_json()["detail"]


def test_limit_over_ceiling_returns_400(client):
    r = client.get("/spine/queue/rejected?limit=1001")
    assert r.status_code == 400


def test_unknown_reason_code_returns_empty_not_400(client, db_path):
    _seed(db_path, n=2)
    # SQL filter against a value that's not in the literal still returns
    # zero rows (the route doesn't pre-validate against RejectionReason
    # because the DB may carry historical codes from earlier versions).
    r = client.get("/spine/queue/rejected?reason_code=never_existed")
    assert r.status_code == 200
    assert r.get_json()["count"] == 0
