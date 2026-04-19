"""Tests for tmp-file cleanup in the `sent` stage.

`_send_package` writes the merged PDF to a NamedTemporaryFile with
delete=False so EmailSender (file-based) can attach it. Previously
the file was never removed — so each send leaked a `reytech_pkg_*.pdf`
file in the system temp dir. On a long-running production process
that's a slow disk-fill bug.

Now `_send_package` deletes the tmp file in a finally block, regardless
of whether the SMTP call succeeded or raised.
"""
from __future__ import annotations

import glob
import os
import tempfile
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


def _quote_ready_to_send() -> Quote:
    q = Quote(doc_type=DocType.PC, line_items=[
        LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
    ])
    q.header.solicitation_number = "R26Q0042"
    q.header.agency_key = "cchcs"
    q.buyer.requestor_email = "buyer@example.gov"
    q.buyer.requestor_name = "Jane Buyer"
    return q


def _result_with_package() -> OrchestratorResult:
    quote = _quote_ready_to_send()
    result = OrchestratorResult(quote=quote)
    pkg = MagicMock()
    pkg.merged_pdf = b"%PDF-1.4 fake bytes\n"
    result.package = pkg
    return result


def _existing_pkg_tmps() -> set[str]:
    return set(glob.glob(os.path.join(tempfile.gettempdir(), "reytech_pkg_*.pdf")))


class TestSendTmpCleanup:
    def test_tmp_file_removed_after_successful_send(self):
        orch = QuoteOrchestrator(persist_audit=False)
        result = _result_with_package()
        before = _existing_pkg_tmps()

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                MockSender.return_value.send = MagicMock(return_value=None)
                info = orch._send_package(
                    result.quote,
                    QuoteRequest(target_stage="sent"),
                    result,
                )

        after = _existing_pkg_tmps()
        leaked = after - before
        assert not leaked, f"tmp file leaked after success: {leaked}"
        assert info["bytes"] == len(b"%PDF-1.4 fake bytes\n")

    def test_tmp_file_removed_when_smtp_raises(self):
        """If SMTP fails, the tmp file must still be cleaned up."""
        orch = QuoteOrchestrator(persist_audit=False)
        result = _result_with_package()
        before = _existing_pkg_tmps()

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                MockSender.return_value.send = MagicMock(
                    side_effect=ConnectionError("smtp down"),
                )
                try:
                    orch._send_package(
                        result.quote,
                        QuoteRequest(target_stage="sent"),
                        result,
                    )
                except RuntimeError:
                    pass  # expected — wraps SMTP error
                else:
                    raise AssertionError("expected RuntimeError on SMTP failure")

        after = _existing_pkg_tmps()
        leaked = after - before
        assert not leaked, f"tmp file leaked after SMTP failure: {leaked}"

    def test_tmp_file_attachment_received_by_sender(self):
        """Sanity: the sender DOES receive a real, readable file at attach time."""
        orch = QuoteOrchestrator(persist_audit=False)
        result = _result_with_package()
        captured: dict = {}

        def capture_and_check(draft):
            path = draft["attachments"][0]
            assert os.path.exists(path), f"attachment missing at send time: {path}"
            with open(path, "rb") as f:
                captured["bytes"] = f.read()

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                MockSender.return_value.send = MagicMock(side_effect=capture_and_check)
                orch._send_package(
                    result.quote,
                    QuoteRequest(target_stage="sent"),
                    result,
                )

        assert captured["bytes"] == b"%PDF-1.4 fake bytes\n"
