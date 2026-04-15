"""Tests for the CS-draft auto-reject list.

Audit finding: admin@govspendemail.com is a notification-only inbox where
auto-CS-replies vanish into the void. The CS agent must never draft to
that address (or other known notification mailboxes / domains).
"""

from src.agents.cs_agent import (
    _should_skip_cs_draft,
    build_cs_response_draft,
    CS_AUTO_REJECT_SENDERS,
    CS_AUTO_REJECT_DOMAINS,
)


class TestShouldSkipCSDraft:
    def test_admin_govspendemail_skipped(self):
        skip, reason = _should_skip_cs_draft("admin@govspendemail.com")
        assert skip is True
        assert "auto-reject" in reason

    def test_govspendemail_subdomain_skipped_by_domain(self):
        skip, _ = _should_skip_cs_draft("notifications@govspendemail.com")
        assert skip is True

    def test_real_buyer_passes(self):
        skip, _ = _should_skip_cs_draft("purchasing@cdcr.ca.gov")
        assert skip is False

    def test_empty_sender_passes(self):
        skip, _ = _should_skip_cs_draft("")
        assert skip is False

    def test_case_insensitive(self):
        skip, _ = _should_skip_cs_draft("Admin@GovSpendEmail.com")
        assert skip is True


class TestBuildDraftSkipsNotificationSenders:
    def test_returns_skipped_envelope(self):
        result = build_cs_response_draft(
            classification={"intent": "general",
                            "sender_email": "admin@govspendemail.com"},
            subject="New solicitation alert",
            body="There's a new RFQ from CDCR posted today",
            sender="admin@govspendemail.com",
        )
        assert result["ok"] is True
        assert result.get("skipped") is True
        assert result["draft"] is None
        assert result["auto_saved"] is False
        assert "auto-reject" in result["skip_reason"]

    def test_real_buyer_still_drafts(self, tmp_path, monkeypatch):
        # Smoke test that the normal happy path still produces a draft envelope
        # for legitimate senders. We don't assert on the body contents here —
        # just that we don't accidentally skip a real customer.
        monkeypatch.chdir(tmp_path)
        result = build_cs_response_draft(
            classification={"intent": "general",
                            "sender_email": "buyer@cdcr.ca.gov",
                            "sender_name": "Real Buyer"},
            subject="Question about quote R26Q500",
            body="Hi, I had a question about the bid",
            sender="buyer@cdcr.ca.gov",
        )
        assert result["ok"] is True
        assert result.get("skipped") is not True
        assert result["draft"] is not None
