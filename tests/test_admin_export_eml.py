"""Tests for routes_admin_export_eml.

Path-B substrate endpoint (2026-05-16): GET /api/admin/export_eml returns
the raw RFC 2822 bytes of the first message matching `subject:<sol>`.
Auth-gated by DASH_PASS / X-API-Key.
"""
import pytest


def _install_get_raw(monkeypatch, body: bytes):
    import src.core.gmail_api as gmail_mod
    monkeypatch.setattr(gmail_mod, "get_raw_message",
                        lambda svc, mid, return_thread_id=False: body)


def test_export_eml_400_when_no_params(auth_client):
    r = auth_client.get("/api/admin/export_eml")
    assert r.status_code == 400
    body = r.get_json()
    assert body["ok"] is False
    assert "sol or q required" in body["error"]


def test_export_eml_400_when_sol_fails_validation(auth_client):
    # path traversal / injection attempt is rejected by the regex
    r = auth_client.get("/api/admin/export_eml?sol=../../etc/passwd")
    assert r.status_code == 400
    assert "sol failed validation" in r.get_json()["error"]


def test_export_eml_400_when_inbox_invalid(auth_client):
    r = auth_client.get("/api/admin/export_eml?sol=10847457&inbox=evil")
    assert r.status_code == 400
    assert "inbox" in r.get_json()["error"]


def test_export_eml_503_when_gmail_unconfigured(auth_client, mock_gmail):
    mock_gmail.set_configured(False)
    r = auth_client.get("/api/admin/export_eml?sol=10847457")
    assert r.status_code == 503
    assert "not configured" in r.get_json()["error"].lower()


def test_export_eml_404_when_no_match(auth_client, mock_gmail, monkeypatch):
    mock_gmail.set_configured(True)
    mock_gmail.set_messages([])  # both inboxes return zero ids
    _install_get_raw(monkeypatch, b"")
    r = auth_client.get("/api/admin/export_eml?sol=NOSUCHSOL")
    assert r.status_code == 404
    body = r.get_json()
    assert body["error"] == "not found"
    assert body["tried_inboxes"] == ["mike", "sales"]
    assert body["query"] == "subject:NOSUCHSOL"


def test_export_eml_200_returns_rfc822_bytes(auth_client, mock_gmail, monkeypatch):
    mock_gmail.set_configured(True)
    mock_gmail.set_messages([{"id": "msgABC", "subject": "PREQ 10847457 SAC"}])
    raw_bytes = (
        b"From: marc.argarin@cchcs.ca.gov\r\n"
        b"To: sales@reytechinc.com\r\n"
        b"Subject: PREQ 10847457 SAC Bid Request\r\n"
        b"\r\n"
        b"Please respond with 703B, 704B, bid package, and quote.\r\n"
    )
    _install_get_raw(monkeypatch, raw_bytes)

    r = auth_client.get("/api/admin/export_eml?sol=10847457")
    assert r.status_code == 200
    assert r.mimetype == "message/rfc822"
    assert r.data == raw_bytes
    assert r.headers.get("X-Reytech-Match-Count") == "1"
    assert r.headers.get("X-Reytech-Inbox") == "mike"
    assert "10847457" in r.headers.get("Content-Disposition", "")


def test_export_eml_filename_sanitized_against_path_traversal(auth_client, mock_gmail, monkeypatch):
    mock_gmail.set_configured(True)
    mock_gmail.set_messages([{"id": "m1"}])
    _install_get_raw(monkeypatch, b"raw")
    # Spaces and dashes are sanitized to underscores; result fits the
    # validator allowlist and never escapes the attachment header.
    r = auth_client.get("/api/admin/export_eml?sol=PREQ%2010847262")
    assert r.status_code == 200
    disp = r.headers.get("Content-Disposition", "")
    assert "PREQ_10847262.eml" in disp
    # No traversal characters reach the Content-Disposition.
    assert ".." not in disp
    assert "/" not in disp.split("filename=")[1]


def test_export_eml_q_param_overrides_sol(auth_client, mock_gmail, monkeypatch):
    mock_gmail.set_configured(True)
    mock_gmail.set_messages([{"id": "m1"}])
    _install_get_raw(monkeypatch, b"raw")
    # Free-form query bypasses sol validation; useful for sender-based pulls
    # (e.g. q=from:demidenko subject:hooks).
    r = auth_client.get(
        "/api/admin/export_eml?q=from%3Avalentina%20subject%3Ahooks"
    )
    assert r.status_code == 200
    # Filename falls back to "message" when no sol is supplied.
    assert 'filename="message.eml"' in r.headers.get("Content-Disposition", "")


def test_export_eml_requires_auth(anon_client):
    # Unauthenticated client gets 401 from auth_required decorator before
    # any business logic runs.
    r = anon_client.get("/api/admin/export_eml?sol=10847457")
    assert r.status_code == 401


def test_export_eml_explicit_inbox_skips_fallback(auth_client, mock_gmail, monkeypatch):
    mock_gmail.set_configured(True)
    mock_gmail.set_messages([])  # nothing in either inbox
    _install_get_raw(monkeypatch, b"")
    r = auth_client.get("/api/admin/export_eml?sol=10847457&inbox=sales")
    assert r.status_code == 404
    # Only `sales` was tried; assistant can pivot to `mike` deliberately.
    assert r.get_json()["tried_inboxes"] == ["sales"]
