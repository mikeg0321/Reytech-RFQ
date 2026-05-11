"""Self-email filter — block our own outbound mail from ingesting as RFQ/PC.

Mike P0 2026-05-11: prod found 2 PCs (pc_number 45007500 + 45007355) with
"Michael Guadan" as buyer. Both came from Mike's OWN outbound reply emails
— he replied to a buyer with a quote PDF, Gmail threaded the reply back
into INBOX view, and the email poller ingested it as a fresh buyer RFQ.

Root cause: the bypass-self-filter rule fired on any 2-of-4 signals over
{is_forward, has_fwd_body, has_pdfs, _has_rfc822}. A reply with a PDF
hit:
  * has_fwd_body=True (because "from:" appears in any quoted-reply tail —
    Gmail quotes the original message; "From: <original-sender>" is in the
    quoted block)
  * has_pdfs=True (the quote PDF Mike attached)
  → 2 signals → bypassed self-filter → ingested as buyer RFQ.

Fix: `is_forward=True` (Fwd:/Fw: in subject) is now a HARD prerequisite.
Plus 1+ additional signal. A reply never has Fwd: so reply-with-PDF can
no longer bypass. Real forwards (Mike forwarding a buyer RFQ to his own
inbox) still pass.

Also: "from:" was removed from has_fwd_body markers — too loose, fired
on quoted-reply tails.
"""
from __future__ import annotations

from src.agents.email_poller import classify_self_email


OUR_EMAIL = "mtnbkr.8654@gmail.com"
OUR_DOMAINS = ["reytechinc.com", "reytech.com"]


# ─── Non-self senders pass through unchanged ─────────────────────────────


def test_external_sender_is_not_self():
    """Buyer email from a non-Reytech address is not self; caller proceeds."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email="keith.alsing@calvet.ca.gov",
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="RFQ - Medical Supplies",
        body="Please quote the attached items.",
        has_pdfs=True,
        has_rfc822=False,
    )
    assert is_self is False
    assert should_skip is False
    assert is_real_forward is False


# ─── Self-email with reply (the 2026-05-11 incident) — MUST be skipped ───


def test_self_reply_with_pdf_attachment_is_skipped():
    """The exact 2026-05-11 case: Mike replied to a buyer with a quote PDF.
    Gmail threaded the reply into INBOX. Body has 'From:' from the quoted
    tail. Pre-fix this hit 2 signals (has_fwd_body + has_pdfs) and was
    treated as a forward. Post-fix: skipped."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Re: RFQ - Medical Supplies",  # NOTE: Re:, not Fwd:
        body=(
            "Hi Keith, please find our quote attached.\n\n"
            "On Mon, May 6, 2026 Keith Alsing <keith.alsing@calvet.ca.gov> wrote:\n"
            "> The Department of Veterans Affairs has a Request for Quote.\n"
            "> From: Keith Alsing\n"
        ),
        has_pdfs=True,
        has_rfc822=False,
    )
    assert is_self is True
    assert should_skip is True, (
        "A reply to a buyer (Re: not Fwd:) carrying a quote PDF must NEVER "
        "ingest as a fresh RFQ. This was the prod incident."
    )
    assert is_real_forward is False


def test_self_reply_with_no_attachments_is_skipped():
    """Plain self-reply with no PDF — should always be skipped."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Re: Question about your quote",
        body="Sure, here's the answer.",
        has_pdfs=False,
        has_rfc822=False,
    )
    assert is_self is True
    assert should_skip is True


# ─── Real forwards (Mike → his own inbox) — MUST pass ────────────────────


def test_real_forward_with_fwd_subject_and_pdfs_passes():
    """Mike forwards a buyer's RFQ email (Fwd: subject) to his own inbox to
    re-trigger ingest after a parse failure. Subject starts with Fwd:,
    PDFs attached. Must pass through (not skipped)."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Fwd: RFQ - Medical Supplies Due 5/11",
        body=(
            "---------- Forwarded message ---------\n"
            "From: Keith Alsing <keith.alsing@calvet.ca.gov>\n"
            "Subject: RFQ - Medical Supplies\n\n"
            "Please quote the attached."
        ),
        has_pdfs=True,
        has_rfc822=False,
    )
    assert is_self is True
    assert should_skip is False
    assert is_real_forward is True


def test_real_forward_fwd_subject_plus_rfc822_passes():
    """Forward with nested message/rfc822 part (some Gmail forwards have
    this shape) instead of inline body markers — still a real forward."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Fw: RFQ Office Supplies",
        body="(no forwarded-body markers)",
        has_pdfs=False,
        has_rfc822=True,
    )
    assert is_self is True
    assert is_real_forward is True
    assert should_skip is False


def test_fwd_subject_alone_without_pdfs_or_body_markers_is_skipped():
    """Subject says Fwd: but no PDFs, no body markers, no rfc822 — likely
    a partial / aborted forward Mike never actually forwarded. Should be
    skipped, not processed."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Fwd: (no actual forward)",
        body="hello",
        has_pdfs=False,
        has_rfc822=False,
    )
    assert is_self is True
    # is_forward TRUE but additional signals 0 → not a real forward
    assert is_real_forward is False
    assert should_skip is True


# ─── Domain-based self detection ─────────────────────────────────────────


def test_self_detected_via_reytech_domain():
    """Senders from @reytechinc.com or @reytech.com are also self."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email="staff@reytechinc.com",
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Re: Quote update",
        body="",
        has_pdfs=False,
        has_rfc822=False,
    )
    assert is_self is True
    assert should_skip is True


def test_domain_match_is_exact_not_substring():
    """A buyer at 'not-reytechinc.com' (or similar partial) is NOT self.
    Domain match must require the full '@<domain>' suffix."""
    is_self, _, _ = classify_self_email(
        sender_email="buyer@not-reytechinc.com",
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="RFQ",
        body="",
        has_pdfs=True,
        has_rfc822=False,
    )
    assert is_self is False


# ─── "from:" no longer a fwd_body signal (the loose marker) ──────────────


def test_from_word_in_reply_body_does_NOT_count_as_forward_signal():
    """Before the fix, 'From: <name>' in a quoted-reply tail counted as a
    forwarded-body signal, contributing to the 2-of-4 bypass. Post-fix,
    'from:' was removed from the marker list — only explicit forward
    headers count."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Re: Your inquiry",
        body=(
            "Thanks for the question.\n\n"
            "On Mon, May 6 Keith Alsing wrote:\n"
            "> From: Keith Alsing\n"  # ← would have triggered old has_fwd_body
            "> Hello, please quote..."
        ),
        has_pdfs=True,
        has_rfc822=False,
    )
    assert is_self is True
    assert should_skip is True, (
        "Reply with quoted 'From:' tail + PDF must NOT be classified as "
        "a forward — this was the 2026-05-11 incident's exact shape."
    )


def test_explicit_forwarded_body_marker_with_fwd_subject_passes():
    """A subject starting with Fwd: PLUS an explicit '---------- Forwarded'
    body marker (no PDFs) — still a real forward. Two-signal path."""
    is_self, should_skip, is_real_forward = classify_self_email(
        sender_email=OUR_EMAIL,
        our_email=OUR_EMAIL,
        our_domains=OUR_DOMAINS,
        subject="Fwd: RFQ",
        body="---------- Forwarded message ----------\nFrom: buyer",
        has_pdfs=False,
        has_rfc822=False,
    )
    assert is_self is True
    assert is_real_forward is True
    assert should_skip is False
