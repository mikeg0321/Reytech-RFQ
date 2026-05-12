"""PR-A2 (2026-05-12) — Proofpoint auto-pull, web-side HTTP client.

Architecture moved: Playwright/Chromium runs in the scprs-scraper
service; the web service is now a thin HTTP client. Tests pin:
  - extract_portal_url: pulls the secure-reader URL out of wrapper
    email body
  - is_available: gates on SCRAPER_SERVICE_URL env var + creds + flag
  - pull_via_url: POSTs to ${SCRAPER_SERVICE_URL}/proofpoint/pull,
    decodes base64 attachments to local files, returns [] on any
    failure mode

The scraper-side Playwright module is tested separately by hand
against a real DSH portal once env vars + flag are in place.
"""
from __future__ import annotations

import base64
import os
from unittest.mock import patch, MagicMock

import pytest


# ─── extract_portal_url ──────────────────────────────────────────────────


class TestExtractPortalUrl:

    def test_returns_securereader_url(self):
        from src.agents.proofpoint_pull import extract_portal_url
        body = (
            "<html><body>"
            "<p>You have received a secure message.</p>"
            "<a href='https://securereader.proofpoint.com/?u=abc123def456'>"
            "Read the Message</a>"
            "</body></html>"
        )
        url = extract_portal_url(body)
        assert url is not None
        assert url.startswith("https://securereader.proofpoint.com")

    def test_returns_agency_securemail_url(self):
        from src.agents.proofpoint_pull import extract_portal_url
        body = "Click https://portal.securemail.dsh.ca.gov/msg/xyz to read."
        url = extract_portal_url(body)
        assert url is not None
        assert "securemail.dsh.ca.gov" in url

    def test_returns_none_on_no_match(self):
        from src.agents.proofpoint_pull import extract_portal_url
        body = "Hi Mike, please find attached the price-check worksheet."
        assert extract_portal_url(body) is None

    def test_returns_none_on_empty_body(self):
        from src.agents.proofpoint_pull import extract_portal_url
        assert extract_portal_url("") is None
        assert extract_portal_url(None) is None

    def test_strips_trailing_punctuation(self):
        from src.agents.proofpoint_pull import extract_portal_url
        body = "Click here: https://securereader.proofpoint.com/?u=abc."
        url = extract_portal_url(body)
        assert url.endswith("abc")

    def test_decodes_html_entities(self):
        from src.agents.proofpoint_pull import extract_portal_url
        body = (
            "<a href='https://securereader.proofpoint.com/"
            "?u=abc&amp;s=def&amp;r=ghi'>Read</a>"
        )
        url = extract_portal_url(body)
        assert url is not None
        assert "&amp;" not in url


# ─── is_available — env + creds + flag gating ───────────────────────────


class TestIsAvailable:

    def test_returns_false_when_scraper_url_missing(self, monkeypatch):
        from src.agents import proofpoint_pull
        monkeypatch.delenv("SCRAPER_SERVICE_URL", raising=False)
        assert proofpoint_pull.is_available() is False

    def test_returns_false_when_email_missing(self, monkeypatch):
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        def _fake_get_key(name):
            return "" if name == "proofpoint_email" else "secret"

        with patch("src.core.secrets.get_key", side_effect=_fake_get_key):
            assert proofpoint_pull.is_available() is False

    def test_returns_false_when_password_missing(self, monkeypatch):
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        def _fake_get_key(name):
            return "" if name == "proofpoint_password" else "user@x.gov"

        with patch("src.core.secrets.get_key", side_effect=_fake_get_key):
            assert proofpoint_pull.is_available() is False

    def test_returns_false_when_flag_off(self, monkeypatch):
        """Even with env + creds, the feature flag must be on. Default
        OFF — Mike's opt-in preference."""
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")
        with patch("src.core.secrets.get_key", return_value="x"), \
             patch("src.core.flags.get_flag", return_value=False):
            assert proofpoint_pull.is_available() is False

    def test_returns_true_when_all_gates_pass(self, monkeypatch):
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")
        with patch("src.core.secrets.get_key", return_value="x"), \
             patch("src.core.flags.get_flag", return_value=True):
            assert proofpoint_pull.is_available() is True


# ─── pull_via_url — HTTP client behavior ────────────────────────────────


class TestPullViaUrl:

    def test_returns_empty_on_no_url(self):
        from src.agents.proofpoint_pull import pull_via_url
        assert pull_via_url("") == []
        assert pull_via_url(None) == []

    def test_returns_empty_when_not_available(self):
        from src.agents import proofpoint_pull
        with patch.object(proofpoint_pull, "is_available",
                          return_value=False):
            assert proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
            ) == []

    def test_posts_to_scraper_and_decodes_attachments(
        self, tmp_path, monkeypatch,
    ):
        """Happy path: scraper returns base64 attachments; we decode
        them to local files and return the paths."""
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")
        monkeypatch.setenv("SCRAPER_SECRET", "test-secret")

        pdf_bytes = b"%PDF-1.4 stub content"
        b64 = base64.b64encode(pdf_bytes).decode("ascii")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "ok": True,
            "data": [
                {"filename": "AMS_704_DSH.pdf", "content_b64": b64, "size": len(pdf_bytes)},
                {"filename": "spec.pdf", "content_b64": b64, "size": len(pdf_bytes)},
            ],
        }

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key",
                   side_effect=lambda k: "user@x.gov" if k == "proofpoint_email" else "pass"), \
             patch("requests.post", return_value=mock_resp) as mock_post:
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )

        assert len(paths) == 2
        for p in paths:
            assert os.path.exists(p)
            with open(p, "rb") as fh:
                assert fh.read() == pdf_bytes

        # Scraper auth header attached.
        _args, kwargs = mock_post.call_args
        assert kwargs["headers"]["X-Scraper-Secret"] == "test-secret"
        # Endpoint path appended to SCRAPER_SERVICE_URL.
        assert mock_post.call_args.args[0].endswith("/proofpoint/pull")
        # Creds in body.
        assert kwargs["json"]["email"] == "user@x.gov"
        assert kwargs["json"]["password"] == "pass"
        assert kwargs["json"]["portal_url"].endswith("/abc")

    def test_returns_empty_on_http_error(self, tmp_path, monkeypatch):
        from src.agents import proofpoint_pull
        import requests
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("requests.post",
                   side_effect=requests.RequestException("conn refused")):
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert paths == []

    def test_returns_empty_on_scraper_5xx(self, tmp_path, monkeypatch):
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        mock_resp = MagicMock()
        mock_resp.status_code = 502
        mock_resp.text = "bad gateway"

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("requests.post", return_value=mock_resp):
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert paths == []

    def test_returns_empty_when_scraper_reports_failure(
        self, tmp_path, monkeypatch,
    ):
        """Scraper returned 200 but with `ok: false` — treat as
        failure, fall back to manual-pull."""
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ok": False, "error": "login failed"}

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("requests.post", return_value=mock_resp):
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert paths == []

    def test_returns_empty_when_scraper_returns_no_attachments(
        self, tmp_path, monkeypatch,
    ):
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ok": True, "data": []}

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("requests.post", return_value=mock_resp):
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert paths == []

    def test_sanitizes_attachment_filename(self, tmp_path, monkeypatch):
        """Scraper might return an attachment with path-traversal
        characters in the filename. We must sanitize before writing."""
        from src.agents import proofpoint_pull
        monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scraper:8001")

        pdf_bytes = b"%PDF-1.4"
        b64 = base64.b64encode(pdf_bytes).decode("ascii")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "ok": True,
            "data": [{
                "filename": "../../etc/passwd",
                "content_b64": b64,
                "size": len(pdf_bytes),
            }],
        }

        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("requests.post", return_value=mock_resp):
            paths = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )

        # Sanitization replaced path-traversal chars; file landed in
        # download_dir, not in /etc.
        assert len(paths) == 1
        assert str(tmp_path) in paths[0]
        assert "passwd" in os.path.basename(paths[0])
