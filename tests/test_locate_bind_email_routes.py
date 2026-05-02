"""Integration tests for /api/rfq/<id>/locate-email and /bind-email
endpoints introduced in PR-B1. Covers:

- locate-email returns candidates from a stubbed Gmail service.
- locate-email surfaces 400 when Gmail isn't configured (guarded by the
  is_configured() probe in routes_rfq.py).
- bind-email rejects missing fields and persists thread_id + message_id
  to the RFQ record.
- The persisted thread_id is then read back as `email_thread_id` (the
  template/UI binding contract).
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest


def _reload_rfq(rid):
    """Read the RFQ back through the same loader the app uses (DB or JSON)."""
    from src.api.dashboard import load_rfqs
    return load_rfqs().get(rid, {})


class TestLocateEmailRoute:

    def test_returns_candidates_when_gmail_configured(self, auth_client,
                                                      seed_rfq, temp_data_dir):
        rid = seed_rfq
        # The route imports `gmail_api` from src.core, then `is_configured`
        # off it; patch both paths the route touches.
        fake_cands = [{
            "gmail_id": "g1", "thread_id": "t_g1",
            "subject": "Bid for thing", "from": "jane@state.ca.gov",
            "date": "Mon, 21 Apr 2026 09:00:00 -0700",
            "message_id": "<msgid-1@state.ca.gov>", "to": "", "cc": "",
        }]
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=object()), \
             patch("src.api.email_locator.locate_candidate_emails",
                   return_value=fake_cands) as mock_locate:
            r = auth_client.post(f"/api/rfq/{rid}/locate-email")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["candidates"] == fake_cands
        assert mock_locate.called

    def test_returns_400_when_gmail_unconfigured(self, auth_client, seed_rfq):
        with patch("src.core.gmail_api.is_configured", return_value=False):
            r = auth_client.post(f"/api/rfq/{seed_rfq}/locate-email")
        assert r.status_code == 400
        assert "not configured" in r.get_json().get("error", "").lower()

    def test_returns_404_for_unknown_rfq(self, auth_client):
        with patch("src.core.gmail_api.is_configured", return_value=True):
            r = auth_client.post("/api/rfq/no-such-rfq/locate-email")
        assert r.status_code == 404


class TestBindEmailRoute:

    def test_persists_thread_id_and_message_id(self, auth_client, seed_rfq):
        rid = seed_rfq
        r = auth_client.post(
            f"/api/rfq/{rid}/bind-email",
            data=json.dumps({"message_id": "<m@x>", "thread_id": "thr_abc"}),
            content_type="application/json",
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["thread_id"] == "thr_abc"

        # Round-trip — record reloaded through the app loader has the binding
        on_disk = _reload_rfq(rid)
        assert on_disk.get("email_thread_id") == "thr_abc"
        assert on_disk.get("email_message_id") == "<m@x>"

    def test_rejects_missing_fields(self, auth_client, seed_rfq):
        r = auth_client.post(
            f"/api/rfq/{seed_rfq}/bind-email",
            data=json.dumps({"thread_id": "t1"}),  # missing message_id
            content_type="application/json",
        )
        assert r.status_code == 400
        assert "required" in r.get_json().get("error", "").lower()

    def test_rejects_unknown_rfq(self, auth_client):
        r = auth_client.post(
            "/api/rfq/no-such/bind-email",
            data=json.dumps({"message_id": "<m@x>", "thread_id": "t"}),
            content_type="application/json",
        )
        assert r.status_code == 404
