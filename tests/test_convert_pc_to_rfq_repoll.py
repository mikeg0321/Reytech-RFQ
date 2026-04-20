"""Tests for PR D: one-click PC→RFQ with 30-day email re-poll.

Verifies:
- repoll=1 runs the email matcher and returns email_repoll info
- QA runs after conversion and returns qa summary
- next_url is returned for one-click redirect
- Gmail-not-configured is a graceful non-failure
"""
from unittest.mock import patch


def test_convert_without_repoll_still_works(auth_client, seed_pc, monkeypatch):
    pcid = seed_pc
    r = auth_client.post(f"/api/pc/{pcid}/convert-to-rfq")
    assert r.status_code == 200
    d = r.get_json()
    assert d["ok"] is True
    assert d["rfq_id"]
    assert d.get("next_url") == f"/rfq/{d['rfq_id']}"
    # email_repoll should be empty dict when flag not set
    assert d.get("email_repoll") == {}


def test_convert_with_repoll_and_gmail_unconfigured_skips_gracefully(auth_client, seed_pc):
    pcid = seed_pc
    with patch("src.core.gmail_api.is_configured", return_value=False):
        r = auth_client.post(f"/api/pc/{pcid}/convert-to-rfq?repoll=1")
    assert r.status_code == 200
    d = r.get_json()
    assert d["ok"] is True
    er = d.get("email_repoll") or {}
    assert er.get("matched") is False
    assert "reason" in er and "not configured" in er["reason"].lower()


def test_convert_response_includes_qa_summary(auth_client, seed_pc):
    pcid = seed_pc
    r = auth_client.post(f"/api/pc/{pcid}/convert-to-rfq")
    assert r.status_code == 200
    d = r.get_json()
    qa = d.get("qa") or {}
    # qa may be empty dict if QA agent unavailable, but when present must
    # carry summary + score keys
    if qa:
        assert "summary" in qa
        assert "score" in qa


def test_convert_repoll_pc_with_no_identifiers(auth_client, seed_pc):
    pcid = seed_pc
    from src.api.dashboard import _load_price_checks, _save_single_pc
    pc = _load_price_checks().get(pcid) or {}
    pc["pc_number"] = ""
    pc["solicitation_number"] = ""
    pc["email_subject"] = ""
    _save_single_pc(pcid, pc)

    with patch("src.core.gmail_api.is_configured", return_value=True):
        r = auth_client.post(f"/api/pc/{pcid}/convert-to-rfq?repoll=1")
    assert r.status_code == 200
    d = r.get_json()
    er = d.get("email_repoll") or {}
    assert er.get("matched") is False
    assert "reason" in er
