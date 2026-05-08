"""Pin S-10 — sync send route has idempotency gate.

Audit 2026-05-07 v2 §S-10: pre-fix /rfq/<rid>/send had no idempotency
protection. Operator double-click caused two consecutive EmailSender
calls — buyer received two PDFs. api_resend_package already had a
60-second recently_delivered gate; this route needed equivalent
protection.

These tests pin a source-level guard: the route body contains the
S-10 fix block + sentinel. Runtime behavior is documented; the
source check ensures a future edit can't silently revert.
"""
from __future__ import annotations

import os
import sys
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _send_email_body() -> str:
    """Extract the send_email function body from routes_rfq_gen.py."""
    import pathlib
    src = pathlib.Path(
        "src/api/modules/routes_rfq_gen.py"
    ).read_text(encoding="utf-8")

    # Find `def send_email(` and extract the body up to the next
    # top-level def. Body starts at the decorator above (look back).
    def_start = src.find("def send_email(")
    assert def_start >= 0, "send_email function not found"
    end = src.find("\ndef ", def_start + 1)
    assert end > def_start, "could not find end of send_email function"
    return src[def_start:end]


class TestSyncSendHasIdempotencyGate:
    def test_send_email_has_s10_sentinel(self):
        body = _send_email_body()
        assert "S-10" in body, \
            "S-10 sentinel comment missing from send_email — fix may have been reverted"

    def test_send_email_checks_sent_at_age(self):
        body = _send_email_body()
        assert "sent_at" in body, "send_email no longer reads sent_at"
        assert "60" in body, (
            "send_email no longer references 60s window. The idempotency "
            "gate should short-circuit if status='sent' and sent_at < 60s ago."
        )
        # The gate should compute an age delta.
        assert "total_seconds" in body, \
            "send_email no longer computes age via total_seconds()"

    def test_send_email_returns_early_on_dedup(self):
        body = _send_email_body()
        # The fix should redirect back to the RFQ page (not proceed to send)
        # when within the 60s window. Look for the redirect inside the
        # idempotency block.
        idem_block = re.search(
            r"S-10.*?dedup.*?return redirect\(f\"/rfq/\{rid\}\"\)",
            body, re.DOTALL,
        )
        assert idem_block, (
            "S-10 idempotency gate doesn't redirect early. The dedup branch "
            "should return before EmailSender.send() is called."
        )
