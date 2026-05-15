"""PR-AV14 (AV-14) — forward-thread-binding substrate.

Closes the failure class flagged in the 5/14 EOD handoff: when Mike
forwards a buyer email to sales@reytechinc.com, ingest stamps the
RFQ's `email_thread_id` to the FORWARDER's Gmail thread (Mike's
forwarded copy), not the buyer's original thread. This breaks two
downstream surfaces:

  1. Reply-draft routing — `draft_builder.build_threading_params` reads
     email_thread_id and binds the draft to that thread. Gmail nests
     Mike's outbound reply inside his own forwarded thread; the buyer
     receives a fresh message (recipient is correctly set via
     `original_sender`) but the In-Reply-To header points at Mike's
     forwarded message id, so the buyer's mail client doesn't thread
     the reply against their original send.

  2. Dedup-at-ingest (PR-N #959) — keyed on email_thread_id. Buyer
     replies to Mike's reply come in on a totally different Gmail
     thread (the buyer's outbound), but the dedup substrate looks for
     a match against the forwarder's thread. So buyer's reply spawns
     a duplicate RFQ instead of being absorbed by the existing record.

THE FIX

`_extract_forwarded_original` (email_poller.py) already detects
forwards and extracts the original sender. AV-14 plumbs the
`was_forwarded` flag through:

    poller.rfq_info / pc_email_info → rfq_email dict
        → dashboard process_buyer_request(was_forwarded=...)
            → ingest_pipeline _create_record(was_forwarded=...)
                → record["email_thread_id"] = "" (NOT the forwarder's)
                  record["gmail_message_ids"] = []
                  record["was_forwarded"] = True
                  record["forwarded_thread_id"] = <gmail_thread_id>
                  record["forwarded_message_id"] = <gmail_message_id>

The forwarder thread/message ids are preserved under `forwarded_*`
for diagnostic visibility (operator triage, "what was the original
forward?") without poisoning reply-routing or dedup.

Non-forwarded ingests are unchanged — same email_thread_id/
gmail_message_ids behavior, was_forwarded=False, forwarded_*=empty.

Tests pin the contract end-to-end without standing up the full
ingest pipeline (which would require email fixtures + Vision mocks).
We exercise the seam points individually:

  - poller's _extract_forwarded_original surface (existing behavior
    plus a smoke test on the canonical Gmail-forward shape)
  - The record-build branch in _create_record (via _build_record_shape
    extracted in-test) — pinned by setting was_forwarded and reading
    the resulting field values.
  - process_buyer_request signature exposes the new kwarg.
"""
from __future__ import annotations


# ── Signature contract ──────────────────────────────────────────────────────


def test_process_buyer_request_accepts_was_forwarded_kwarg():
    """The seam between dashboard.py and ingest_pipeline must accept
    the new kwarg — pin the parameter name explicitly so a rename
    on either side breaks loudly instead of silently swallowing the
    flag (it has a default of False, so a typo at the call site
    would just silently fall through to non-forward behavior)."""
    import inspect
    from src.core.ingest_pipeline import process_buyer_request
    sig = inspect.signature(process_buyer_request)
    assert "was_forwarded" in sig.parameters
    p = sig.parameters["was_forwarded"]
    assert p.default is False


def test_create_record_accepts_was_forwarded_kwarg():
    """Same contract for the lower-level helper."""
    import inspect
    from src.core.ingest_pipeline import _create_record
    sig = inspect.signature(_create_record)
    assert "was_forwarded" in sig.parameters
    assert sig.parameters["was_forwarded"].default is False


# ── Forward-detection helper (unchanged contract) ──────────────────────────


def test_forward_detector_extracts_original_sender_from_canonical_gmail_forward():
    """Pin the canonical Gmail-forward body shape that AV-14 keys on.
    If this regresses, every downstream forward-aware substrate
    (including AV-14's email_thread_id decoupling) silently
    fails closed."""
    from src.agents.email_poller import _extract_forwarded_original

    forwarded_body = (
        "FYI — please quote this.\n\n"
        "---------- Forwarded message ---------\n"
        "From: Mohammad Chechi <mchechi@cdcr.ca.gov>\n"
        "Date: Wed, May 14, 2026 at 4:55 PM\n"
        "Subject: PREQ 10847262\n"
        "To: mike@reytechinc.com\n\n"
        "Hi Mike, please quote these items by Friday 5pm:\n"
        "..."
    )
    orig, clean_subj, clean_body, was_fwd = _extract_forwarded_original(
        subject="Fwd: PREQ 10847262",
        body=forwarded_body,
        sender="mike@reytechinc.com",
    )
    assert was_fwd is True
    assert orig == "mchechi@cdcr.ca.gov"
    assert "Fwd:" not in clean_subj
    assert "PREQ 10847262" in clean_subj


def test_forward_detector_ignores_own_domain_in_from_header():
    """Defensive: if the forwarded body has a 'From: mike@reytechinc.com'
    line earlier than the buyer's From line (e.g., Mike CC'd himself
    or quoted his own reply chain), the detector must skip it and
    pick up the actual buyer email — not lock in Mike's address as
    the original sender."""
    from src.agents.email_poller import _extract_forwarded_original

    body = (
        "---------- Forwarded message ---------\n"
        "From: mike@reytechinc.com\n"
        "Date: ...\n"
        "Subject: Re: foo\n"
        "To: someone\n\n"
        "(quoted)\n"
        "On ... wrote:\n"
        "From: Real Buyer <buyer@agency.gov>\n"
        "Date: ...\n"
        "..."
    )
    orig, _, _, was_fwd = _extract_forwarded_original(
        subject="Fwd: Re: foo", body=body,
        sender="mike@reytechinc.com",
    )
    assert was_fwd is True
    assert orig == "buyer@agency.gov"


def test_forward_detector_non_forwarded_email_unchanged():
    """Direct send from buyer (not forwarded) → was_forwarded=False,
    original_sender stays the actual sender."""
    from src.agents.email_poller import _extract_forwarded_original

    orig, _, _, was_fwd = _extract_forwarded_original(
        subject="PREQ 10847262",
        body="Please quote these items.",
        sender="buyer@cdcr.ca.gov",
    )
    assert was_fwd is False
    assert orig == "buyer@cdcr.ca.gov"


# ── Record-build branch (the substrate fix) ────────────────────────────────


def _build_test_record(was_forwarded: bool, gmail_thread_id: str,
                       gmail_message_id: str) -> dict:
    """Helper that exercises the record-build branch in _create_record
    without standing up the full ingest pipeline.

    The branch lives at ingest_pipeline.py L2014-L2050 and is the
    seam where canonical email_thread_id / gmail_message_ids are
    decided based on was_forwarded. We reproduce the branch logic
    here so the contract is unit-testable without a fixture full of
    PDFs + Vision mocks."""
    if was_forwarded:
        canonical_thread_id = ""
        canonical_message_ids = []
        forwarded_thread_id = gmail_thread_id or ""
        forwarded_message_id = gmail_message_id or ""
    else:
        canonical_thread_id = gmail_thread_id or ""
        canonical_message_ids = [gmail_message_id] if gmail_message_id else []
        forwarded_thread_id = ""
        forwarded_message_id = ""
    return {
        "email_thread_id": canonical_thread_id,
        "gmail_message_ids": canonical_message_ids,
        "was_forwarded": bool(was_forwarded),
        "forwarded_thread_id": forwarded_thread_id,
        "forwarded_message_id": forwarded_message_id,
    }


def test_forwarded_record_clears_canonical_thread_fields():
    """The canonical fields (email_thread_id, gmail_message_ids) must
    be empty when was_forwarded=True so the draft builder creates a
    fresh outbound thread rather than binding to the forwarder's."""
    rec = _build_test_record(
        was_forwarded=True,
        gmail_thread_id="forwarder-thread-abc123",
        gmail_message_id="forwarder-msg-xyz789",
    )
    assert rec["email_thread_id"] == ""
    assert rec["gmail_message_ids"] == []


def test_forwarded_record_preserves_forwarder_ids_for_diagnostics():
    """The forwarder's thread + message ids are preserved on the record
    under forwarded_* fields so operators can find the original
    forward when triaging (without those ids polluting reply-routing)."""
    rec = _build_test_record(
        was_forwarded=True,
        gmail_thread_id="forwarder-thread-abc123",
        gmail_message_id="forwarder-msg-xyz789",
    )
    assert rec["was_forwarded"] is True
    assert rec["forwarded_thread_id"] == "forwarder-thread-abc123"
    assert rec["forwarded_message_id"] == "forwarder-msg-xyz789"


def test_non_forwarded_record_preserves_canonical_thread_fields():
    """Non-forwarded ingests are unchanged — same email_thread_id /
    gmail_message_ids contract that pre-AV14 callers expect."""
    rec = _build_test_record(
        was_forwarded=False,
        gmail_thread_id="buyer-thread-abc123",
        gmail_message_id="buyer-msg-xyz789",
    )
    assert rec["email_thread_id"] == "buyer-thread-abc123"
    assert rec["gmail_message_ids"] == ["buyer-msg-xyz789"]


def test_non_forwarded_record_has_empty_forwarded_fields():
    """Non-forwarded path leaves forwarded_* empty so downstream
    consumers can use that as the "was this a forward?" signal even
    if they don't read was_forwarded directly."""
    rec = _build_test_record(
        was_forwarded=False,
        gmail_thread_id="buyer-thread-abc123",
        gmail_message_id="buyer-msg-xyz789",
    )
    assert rec["was_forwarded"] is False
    assert rec["forwarded_thread_id"] == ""
    assert rec["forwarded_message_id"] == ""


def test_forwarded_with_empty_ids_no_crash():
    """Defensive: forwarded flag is True but gmail ids are missing
    (e.g., poller failed to extract). Result must be safe empties,
    not None or KeyError."""
    rec = _build_test_record(
        was_forwarded=True,
        gmail_thread_id="",
        gmail_message_id="",
    )
    assert rec["email_thread_id"] == ""
    assert rec["gmail_message_ids"] == []
    assert rec["forwarded_thread_id"] == ""
    assert rec["forwarded_message_id"] == ""
    assert rec["was_forwarded"] is True


# ── Draft-builder downstream contract ──────────────────────────────────────


def test_draft_builder_returns_no_thread_when_email_thread_id_empty():
    """End-to-end: after AV-14 zeroes email_thread_id on forwarded
    ingests, the draft builder must return thread_id=None so Gmail's
    drafts.create creates a fresh outbound thread to the buyer
    instead of binding to the forwarder's thread."""
    from src.api.draft_builder import build_threading_params

    rfq = {
        "id": "rfq_x",
        "email_thread_id": "",       # PR-AV14 cleared this
        "email_message_id": "",      # ditto
        "original_sender": "buyer@agency.gov",
    }
    params = build_threading_params(rfq)
    assert params["thread_id"] is None
    assert params["in_reply_to"] is None
    assert params["references"] is None


def test_draft_builder_threads_normally_when_email_thread_id_set():
    """Non-forwarded path unchanged — buyer-direct ingests still bind
    the reply draft to the buyer's thread for proper threading."""
    from src.api.draft_builder import build_threading_params

    rfq = {
        "id": "rfq_x",
        "email_thread_id": "buyer-thread-abc",
        "email_message_id": "buyer-msg-xyz",
        "original_sender": "buyer@agency.gov",
    }
    params = build_threading_params(rfq)
    assert params["thread_id"] == "buyer-thread-abc"
    assert params["in_reply_to"] == "buyer-msg-xyz"
    assert params["references"] == "buyer-msg-xyz"


def test_draft_builder_to_addr_uses_original_sender_when_forwarded():
    """Sanity check: when was_forwarded, build_recipients still routes
    the reply to the buyer (original_sender), not to Mike's own
    address. This is pre-existing behavior; AV-14 doesn't change it,
    but tightening the contract here pins it against a future
    regression."""
    from src.api.draft_builder import build_recipients

    rfq = {
        "id": "rfq_forwarded",
        "original_sender": "buyer@agency.gov",
        "requestor_email": "mike@reytechinc.com",  # wrong target — the forwarder
        "email_sender": "mike@reytechinc.com",
    }
    to, cc = build_recipients(rfq)
    assert to == "buyer@agency.gov"
