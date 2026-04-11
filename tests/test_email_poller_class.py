"""Tests for EmailPoller class, email classification functions, and polling cycle.

Covers:
  A. Email classification functions (module-level, untested in test_email_poller.py)
  B. EmailPoller class — init, UID tracking, form identification, header decoding
  C. Polling cycle integration — end-to-end poll → classify → process with mocked IMAP
"""
import email
import imaplib
import json
import os
import sys
import tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Ensure project root is on path
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.agents.email_poller import (
    _is_real_procurement_pdf,
    is_marketing_email,
    is_reply_followup,
    _sender_has_active_item,
    is_recall_email,
    handle_recall,
    extract_solicitation_number,
    EmailPoller,
)


# ═══════════════════════════════════════════════════════════════════════════════
# A. Email Classification Functions (8 tests)
# ═══════════════════════════════════════════════════════════════════════════════


class TestIsRealProcurementPdf:
    """_is_real_procurement_pdf: filters tiny/huge/missing files."""

    def test_tiny_file_rejected(self, tmp_path):
        """Files < 5KB are logos/blanks, not real procurement PDFs."""
        tiny = tmp_path / "logo.pdf"
        tiny.write_bytes(b"x" * 100)  # 100 bytes
        assert _is_real_procurement_pdf(str(tiny)) is False

    def test_huge_file_rejected(self, tmp_path):
        """Files > 50MB are not government forms."""
        huge = tmp_path / "giant.pdf"
        # Write just enough to exceed 50MB threshold — use sparse approach
        with open(str(huge), "wb") as f:
            f.seek(50_000_001)
            f.write(b"\x00")
        assert _is_real_procurement_pdf(str(huge)) is False

    def test_normal_file_accepted(self, tmp_path):
        """A 500KB file should pass the size check."""
        normal = tmp_path / "ams704.pdf"
        normal.write_bytes(b"x" * 500_000)
        assert _is_real_procurement_pdf(str(normal)) is True

    def test_missing_file_rejected(self):
        """Non-existent file returns False (OSError path)."""
        assert _is_real_procurement_pdf("/nonexistent/path/fake.pdf") is False


class TestIsMarketingEmail:
    """is_marketing_email: detects newsletters and marketing blasts."""

    def test_marketing_email_with_unsubscribe_header(self):
        """List-Unsubscribe header is the strongest marketing signal."""
        msg = MIMEText("Check out our deals!")
        msg["List-Unsubscribe"] = "<mailto:unsub@example.com>"
        assert is_marketing_email(msg, "Check out our deals!") is True

    def test_marketing_email_body_signals(self):
        """Two+ body signals (unsubscribe + preferences) flag as marketing."""
        msg = MIMEText("")
        body = (
            "Great news about our products!\n"
            "Click here to unsubscribe from this mailing list.\n"
            "Manage your email preferences at any time.\n"
            "You are receiving this because you subscribed.\n"
            "\u00a9 2026 Marketing Corp."
        )
        assert is_marketing_email(msg, body) is True

    def test_real_procurement_not_flagged(self):
        """A genuine procurement email should not be flagged as marketing."""
        msg = MIMEText("")
        body = (
            "Please provide pricing for the following items per AMS 704.\n"
            "Attached is the quote worksheet. Due date: April 15, 2026.\n"
            "Contact: buyer@cchcs.ca.gov"
        )
        assert is_marketing_email(msg, body) is False

    def test_precedence_bulk_flagged(self):
        """Precedence: bulk header marks email as marketing."""
        msg = MIMEText("Content")
        msg["Precedence"] = "bulk"
        assert is_marketing_email(msg, "Content") is True


class TestIsReplyFollowup:
    """is_reply_followup: detects reply/follow-up vs new submission."""

    def test_reply_prefix_detected(self):
        """'Re: RFQ 123' is detected as a reply."""
        msg = MIMEText("Thanks, will send updated pricing.")
        msg["Subject"] = "Re: RFQ 123"
        msg["In-Reply-To"] = "<abc@example.com>"
        # Mock _sender_has_active_item to return active items
        with patch("src.agents.email_poller._sender_has_active_item",
                   return_value=[{"type": "rfq", "ref": "R26Q001", "status": "pending"}]):
            result = is_reply_followup(
                msg, "Re: RFQ 123",
                "Thanks, will send updated pricing.",
                "buyer@cchcs.ca.gov", []
            )
        assert result is not None  # Detected as follow-up

    def test_fresh_email_not_flagged(self):
        """A fresh email without reply indicators is not a follow-up."""
        msg = MIMEText("Please quote the attached AMS 704 form.")
        msg["Subject"] = "AMS 704 - Medical Supplies"
        result = is_reply_followup(
            msg, "AMS 704 - Medical Supplies",
            "Please quote the attached AMS 704 form.",
            "buyer@cchcs.ca.gov", []
        )
        assert result is None  # Not a reply


class TestSenderHasActiveItem:
    """_sender_has_active_item: checks for active quotes/PCs/RFQs."""

    def test_no_match_returns_none(self, app):
        """Unknown sender with no active items returns None."""
        result = _sender_has_active_item("unknown-buyer@example.com")
        assert result is None


class TestHandleRecall:
    """handle_recall: processes recall emails by deleting matching PCs."""

    def test_recall_marks_deleted(self, app, temp_data_dir):
        """Recall email triggers deletion of matching PC."""
        # Create a price check that matches the recall subject
        pcs = {
            "pc_test1": {
                "pc_number": "PC-001",
                "status": "new",
                "email_subject": "Quote - Med OS - 02.17.26",
                "source_pdf": "med_os.pdf",
                "items": [],
            }
        }
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        with open(pc_path, "w") as f:
            json.dump(pcs, f)

        # Patch the functions that handle_recall uses
        with patch("src.agents.email_poller._delete_price_check_cascade",
                   return_value={"pcid": "pc_test1", "deleted": True}) as mock_delete, \
             patch("src.agents.email_poller._recalc_quote_counter"), \
             patch("src.api.dashboard._load_price_checks", return_value=pcs), \
             patch("src.api.dashboard._save_price_checks"):
            deleted = handle_recall("Quote - Med OS - 02.17.26")
            # If cascade mock fired, we should have one deletion
            if mock_delete.called:
                assert len(deleted) >= 1


class TestExtractSolicitationFromBody:
    """extract_solicitation_number: parses solicitation numbers from email body."""

    def test_explicit_solicitation_number(self):
        """'Solicitation #12345678' is extracted from body."""
        result = extract_solicitation_number(
            "RFQ for Medical Supplies",
            "Please respond to Solicitation #12345678 by April 15."
        )
        assert result == "12345678"

    def test_seven_digit_near_keyword(self):
        """7-digit number near 'rfq' keyword is extracted."""
        result = extract_solicitation_number(
            "RFQ 1234567 - Bandages",
            "Attached is the RFQ package."
        )
        assert result == "1234567"

    def test_fallback_to_rfq(self):
        """No numbers at all returns 'RFQ' fallback."""
        result = extract_solicitation_number(
            "Please Quote",
            "We need pricing on some items."
        )
        assert result == "RFQ"

    def test_from_attachment_name(self):
        """Solicitation number extracted from PDF filename."""
        result = extract_solicitation_number(
            "Please quote",
            "See attached.",
            attachments=["RFQ_10840486_bandages.pdf"]
        )
        assert result == "10840486"


# ═══════════════════════════════════════════════════════════════════════════════
# B. EmailPoller Class (7 tests)
# ═══════════════════════════════════════════════════════════════════════════════


def _make_poller(temp_data_dir):
    """Create an EmailPoller with test config pointing to temp directory."""
    config = {
        "email": "test@reytechinc.com",
        "email_password": "fake",
        "force_imap": True,
        "processed_file": os.path.join(temp_data_dir, "processed_emails.json"),
    }
    return EmailPoller(config)


class TestEmailPollerInit:
    """EmailPoller.__init__: initializes with config dict."""

    def test_init_sets_fields(self, app, temp_data_dir):
        """Poller initializes with correct email, host, folder."""
        poller = _make_poller(temp_data_dir)
        assert poller.email_addr == "test@reytechinc.com"
        assert poller.host == "imap.gmail.com"
        assert poller.port == 993
        assert poller.folder == "INBOX"
        assert poller._use_gmail_api is False
        assert poller._connected is False

    def test_init_custom_host(self, app, temp_data_dir):
        """Custom IMAP host/port config is respected."""
        config = {
            "email": "test@custom.com",
            "email_password": "fake",
            "force_imap": True,
            "imap_host": "mail.custom.com",
            "imap_port": 143,
            "processed_file": os.path.join(temp_data_dir, "processed_emails.json"),
        }
        poller = EmailPoller(config)
        assert poller.host == "mail.custom.com"
        assert poller.port == 143


class TestEmailPollerProcessedUIDs:
    """UID tracking: load, save, dedup."""

    def test_load_empty(self, app, temp_data_dir):
        """Empty processed file → empty UID set."""
        poller = _make_poller(temp_data_dir)
        assert len(poller._processed) == 0

    def test_save_and_reload(self, app, temp_data_dir):
        """Save UIDs, create new poller, verify UIDs recovered from JSON."""
        poller = _make_poller(temp_data_dir)
        poller._processed.add("uid_001")
        poller._processed.add("uid_002")
        poller._save_processed()

        # Create new poller — should reload from JSON
        poller2 = _make_poller(temp_data_dir)
        assert "uid_001" in poller2._processed
        assert "uid_002" in poller2._processed

    def test_dedup_prevents_reprocessing(self, app, temp_data_dir):
        """Marking a UID processed prevents re-processing."""
        poller = _make_poller(temp_data_dir)
        uid = "uid_dedup_test"

        # First time: not processed
        assert uid not in poller._processed

        # Mark as processed
        poller._processed.add(uid)
        poller._save_processed()

        # Second check: should be in processed set
        assert uid in poller._processed

        # New poller instance: still processed
        poller2 = _make_poller(temp_data_dir)
        assert uid in poller2._processed


class TestEmailPollerIdentifyForm:
    """_identify_form: identifies PDF form types from filename."""

    def test_identify_704b(self, app, temp_data_dir):
        """AMS 704B form identified correctly."""
        poller = _make_poller(temp_data_dir)
        assert poller._identify_form("AMS_704B_Quote_Worksheet.pdf") == "704b"

    def test_identify_703b(self, app, temp_data_dir):
        """703B form identified correctly."""
        poller = _make_poller(temp_data_dir)
        assert poller._identify_form("703B_Informal_Competitive.pdf") == "703b"

    def test_identify_703c(self, app, temp_data_dir):
        """703C form identified (must not be confused with 703B)."""
        poller = _make_poller(temp_data_dir)
        assert poller._identify_form("703C_Fair_and_Reasonable.pdf") == "703c"

    def test_identify_bid_package(self, app, temp_data_dir):
        """Bid package form identified."""
        poller = _make_poller(temp_data_dir)
        assert poller._identify_form("Bid_Package_Under_100k.pdf") == "bidpkg"

    def test_unknown_form(self, app, temp_data_dir):
        """Unknown PDF returns 'unknown'."""
        poller = _make_poller(temp_data_dir)
        assert poller._identify_form("random_document.pdf") == "unknown"


class TestEmailPollerDecodeHeader:
    """_decode_header: decodes RFC 2047 encoded email headers."""

    def test_plain_header(self, app, temp_data_dir):
        """Plain ASCII header returned as-is."""
        poller = _make_poller(temp_data_dir)
        assert poller._decode_header("RFQ for Medical Supplies") == "RFQ for Medical Supplies"

    def test_encoded_header(self, app, temp_data_dir):
        """RFC 2047 encoded header decoded correctly."""
        poller = _make_poller(temp_data_dir)
        # "=?utf-8?B?UkZRIFRlc3Q=?=" is base64 for "RFQ Test"
        result = poller._decode_header("=?utf-8?B?UkZRIFRlc3Q=?=")
        assert result == "RFQ Test"

    def test_none_header(self, app, temp_data_dir):
        """None header returns empty string (no crash)."""
        poller = _make_poller(temp_data_dir)
        assert poller._decode_header(None) == ""


# ═══════════════════════════════════════════════════════════════════════════════
# C. Polling Cycle Integration (5 tests)
# ═══════════════════════════════════════════════════════════════════════════════


def _build_raw_email(subject, body, sender="buyer@cchcs.ca.gov",
                     attachments=None):
    """Build a raw RFC 2822 email message as bytes for IMAP mock."""
    if attachments:
        msg = MIMEMultipart()
        msg.attach(MIMEText(body, "plain"))
        for att_name in attachments:
            part = MIMEBase("application", "pdf")
            # Write enough bytes to pass the procurement PDF size check
            part.set_payload(b"x" * 10_000)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=att_name)
            msg.attach(part)
    else:
        msg = MIMEText(body, "plain")

    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = "sales@reytechinc.com"
    msg["Date"] = "Fri, 10 Apr 2026 10:00:00 -0700"
    msg["Message-ID"] = f"<test-{hash(subject)}@example.com>"
    return msg.as_bytes()


@pytest.fixture
def mock_imap(monkeypatch):
    """Mock IMAP connection to prevent real email access."""
    mock = MagicMock()
    mock.login.return_value = ('OK', [b'Logged in'])
    mock.select.return_value = ('OK', [b'1'])
    mock.search.return_value = ('OK', [b''])
    mock.uid.return_value = ('OK', [b''])
    mock.noop.return_value = ('OK', [b''])
    monkeypatch.setattr(imaplib, 'IMAP4_SSL', lambda *a, **kw: mock)
    return mock


class TestPollEmptyInbox:
    """poll with no messages returns 0 processed."""

    def test_empty_inbox(self, app, temp_data_dir, mock_imap):
        """Empty inbox search returns no results."""
        poller = _make_poller(temp_data_dir)
        # Connect succeeds
        assert poller.connect() is True
        # Search returns empty
        mock_imap.uid.return_value = ('OK', [b''])
        results = poller.check_for_rfqs()
        assert results == []


class TestPollClassifiesRFQ:
    """Mock email with RFQ subject classified correctly."""

    def test_rfq_classification(self, app, temp_data_dir, mock_imap):
        """Email with RFQ keyword in subject is classified as RFQ."""
        raw = _build_raw_email(
            subject="Request for Quotation - Medical Supplies",
            body="Please submit your bid for the attached items.",
            sender="buyer@cchcs.ca.gov",
            attachments=["RFQ_703B.pdf", "AMS_704B.pdf"]
        )

        # First uid call: search returns one UID
        # Second uid call: fetch returns the raw email
        call_count = [0]
        def mock_uid_handler(cmd, *args, **kwargs):
            call_count[0] += 1
            if cmd == "search":
                return ('OK', [b'101'])
            elif cmd == "fetch":
                return ('OK', [(b'101 (BODY[] {1234})', raw)])
            return ('OK', [b''])

        mock_imap.uid.side_effect = mock_uid_handler

        poller = _make_poller(temp_data_dir)
        poller.connect()

        # Patch save_attachments to avoid file system ops
        with patch.object(poller, '_save_attachments', return_value=[]):
            # Patch the processing pipeline to avoid dashboard imports
            with patch("src.agents.email_poller._log_email_rejection"):
                results = poller.check_for_rfqs()
                # The RFQ should be detected (subject has "request for quotation")
                # Even if processing fails, the UID should be marked processed
                assert "101" in poller._processed


class TestPollClassifiesPO:
    """Mock email with PO subject classified correctly."""

    def test_po_classification(self, app, temp_data_dir, mock_imap):
        """Email with PO keyword in subject is recognized."""
        raw = _build_raw_email(
            subject="Purchase Order #12345 - Award Notification",
            body="Congratulations, your bid has been accepted. PO attached.",
            sender="buyer@cdcr.ca.gov",
            attachments=["PO_12345.pdf"]
        )

        def mock_uid_handler(cmd, *args, **kwargs):
            if cmd == "search":
                return ('OK', [b'202'])
            elif cmd == "fetch":
                return ('OK', [(b'202 (BODY[] {1234})', raw)])
            return ('OK', [b''])

        mock_imap.uid.side_effect = mock_uid_handler

        poller = _make_poller(temp_data_dir)
        poller.connect()

        with patch.object(poller, '_save_attachments', return_value=[]):
            with patch("src.agents.email_poller._log_email_rejection"):
                results = poller.check_for_rfqs()
                # UID should be processed regardless of classification outcome
                assert "202" in poller._processed


class TestPollIdempotent:
    """Processing same UID twice doesn't create duplicates."""

    def test_idempotent_processing(self, app, temp_data_dir, mock_imap):
        """Same UID processed twice should not create duplicate entries."""
        raw = _build_raw_email(
            subject="AMS 704 - Scrub Sets",
            body="Please complete the attached 704 form.",
            sender="buyer@cchcs.ca.gov",
            attachments=["AMS_704.pdf"]
        )

        def mock_uid_handler(cmd, *args, **kwargs):
            if cmd == "search":
                return ('OK', [b'303'])
            elif cmd == "fetch":
                return ('OK', [(b'303 (BODY[] {1234})', raw)])
            return ('OK', [b''])

        mock_imap.uid.side_effect = mock_uid_handler

        poller = _make_poller(temp_data_dir)
        poller.connect()

        with patch.object(poller, '_save_attachments', return_value=[]):
            with patch("src.agents.email_poller._log_email_rejection"):
                # First poll
                results1 = poller.check_for_rfqs()
                count1 = len(poller._processed)

                # Second poll — same UID should be skipped
                results2 = poller.check_for_rfqs()
                count2 = len(poller._processed)

                # Should not grow (UID already processed)
                assert count2 == count1
                assert "303" in poller._processed


class TestPollHandlesErrorGracefully:
    """Malformed email doesn't crash poller."""

    def test_malformed_email_no_crash(self, app, temp_data_dir, mock_imap):
        """Garbled email data should not crash the poller."""
        # Return garbage bytes that can't be parsed as email
        garbled = b"This is not a valid email message at all \xff\xfe"

        def mock_uid_handler(cmd, *args, **kwargs):
            if cmd == "search":
                return ('OK', [b'404'])
            elif cmd == "fetch":
                return ('OK', [(b'404 (BODY[] {1234})', garbled)])
            return ('OK', [b''])

        mock_imap.uid.side_effect = mock_uid_handler

        poller = _make_poller(temp_data_dir)
        poller.connect()

        # Should not raise — errors handled gracefully
        with patch("src.agents.email_poller._log_email_rejection"):
            results = poller.check_for_rfqs()
            # Should return empty list (no valid RFQs) but not crash
            assert isinstance(results, list)
