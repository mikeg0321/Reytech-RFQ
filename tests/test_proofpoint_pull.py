"""PR-A Step 7 (2026-05-11) — Proofpoint auto-pull module.

Pins the public API surface of `src/agents/proofpoint_pull.py`:
  - extract_portal_url: pulls the secure-reader URL out of the
    wrapper email body
  - is_available: gates the auto-pull on creds + Playwright + flag
  - pull_via_url: top-level sync entry — returns [] on every failure
    mode so the SecureMessage handler can fall back cleanly

Playwright is not actually invoked in these tests; we mock the
public surface so they run without a browser install. The auto-pull
flow itself is exercised by hand against a real DSH wrapper email
once creds land in Railway env vars.
"""
from __future__ import annotations

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
        """URLs in plain-text bodies often have trailing dots/commas."""
        from src.agents.proofpoint_pull import extract_portal_url
        body = "Click here: https://securereader.proofpoint.com/?u=abc."
        url = extract_portal_url(body)
        assert url.endswith("abc")

    def test_decodes_html_entities(self):
        """Gmail HTML body passes & as &amp;; reader URL has query string."""
        from src.agents.proofpoint_pull import extract_portal_url
        body = (
            "<a href='https://securereader.proofpoint.com/"
            "?u=abc&amp;s=def&amp;r=ghi'>Read</a>"
        )
        url = extract_portal_url(body)
        assert url is not None
        assert "&amp;" not in url
        assert url.count("&") == 2  # both ampersands decoded


# ─── is_available — credential + flag gating ────────────────────────────


class TestIsAvailable:

    def test_returns_false_when_playwright_missing(self):
        from src.agents import proofpoint_pull
        with patch.object(proofpoint_pull, "_playwright_available",
                          return_value=False):
            assert proofpoint_pull.is_available() is False

    def test_returns_false_when_email_missing(self):
        from src.agents import proofpoint_pull

        def _fake_get_key(name):
            return "" if name == "proofpoint_email" else "secret"

        with patch.object(proofpoint_pull, "_playwright_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", side_effect=_fake_get_key):
            assert proofpoint_pull.is_available() is False

    def test_returns_false_when_password_missing(self):
        from src.agents import proofpoint_pull

        def _fake_get_key(name):
            return "" if name == "proofpoint_password" else "user@x.gov"

        with patch.object(proofpoint_pull, "_playwright_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", side_effect=_fake_get_key):
            assert proofpoint_pull.is_available() is False

    def test_returns_false_when_flag_off(self):
        """Even with creds + Playwright, the feature flag must be on.
        Default OFF — Mike's opt-in preference."""
        from src.agents import proofpoint_pull
        with patch.object(proofpoint_pull, "_playwright_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("src.core.flags.get_flag", return_value=False):
            assert proofpoint_pull.is_available() is False

    def test_returns_true_when_all_gates_pass(self):
        from src.agents import proofpoint_pull
        with patch.object(proofpoint_pull, "_playwright_available",
                          return_value=True), \
             patch("src.core.secrets.get_key", return_value="x"), \
             patch("src.core.flags.get_flag", return_value=True):
            assert proofpoint_pull.is_available() is True


# ─── pull_via_url — top-level error handling ────────────────────────────


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

    def test_returns_empty_on_async_failure(self, tmp_path):
        """When the async pull raises, pull_via_url swallows and
        returns []. SecureMessage handler then flips needs_manual_pull."""
        from src.agents import proofpoint_pull
        with patch.object(proofpoint_pull, "is_available",
                          return_value=True), \
             patch.object(proofpoint_pull, "_pull_async",
                          side_effect=RuntimeError("playwright crashed")):
            result = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert result == []

    def test_returns_downloads_on_success(self, tmp_path):
        from src.agents import proofpoint_pull
        fake_paths = [str(tmp_path / "rfq.pdf"), str(tmp_path / "spec.pdf")]
        # Pre-create the files to mirror what _pull_async would do.
        for p in fake_paths:
            with open(p, "wb") as fh:
                fh.write(b"%PDF-1.4 stub")

        async def _fake_pull(*args, **kwargs):
            return fake_paths

        with patch.object(proofpoint_pull, "is_available", return_value=True), \
             patch.object(proofpoint_pull, "_pull_async",
                          side_effect=_fake_pull):
            result = proofpoint_pull.pull_via_url(
                "https://securereader.proofpoint.com/abc",
                download_dir=str(tmp_path),
            )
        assert result == fake_paths
