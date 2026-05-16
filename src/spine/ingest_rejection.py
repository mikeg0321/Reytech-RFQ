"""IngestRejection — every email considered emits a row.

Closes the "missed-bid silent-drop" failure class (5/15 missed-bid mode
c). When the parser refuses an inbound email — because the agency isn't
supported, attachments are missing, parse fails, tax lookup fails, etc.
— the rejection must be DURABLY RECORDED so:

  - the operator can audit what was dropped and why
  - the Telegram missed-bid watcher (queued) can escalate
  - regressions are visible without re-running mail history

Architectural rules:
  - `extra="forbid"` on the model — no alias creep.
  - Append-only `spine_ingest_rejections` table; one writer
    (`write_ingest_rejection` in db.py).
  - Reason codes are a closed Literal; adding a new reason requires an
    explicit edit + test update (intentional friction).
  - Raw excerpt is truncated to 1000 chars at construction — never
    store full message bodies; the original .eml lives in Gmail.

This is the **substrate** for the missed-bid watcher. The watcher
(Telegram alerts on aging / unhandled rejections) is queued behind
this row and depends on this row existing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# Closed set. Add a new reason by:
#   1. extending this Literal,
#   2. extending the test_ingest_rejection.py parametrize matrix,
#   3. wiring the producer (ingest_email_contract or upstream) to emit it.
# Intentional friction — silent reason proliferation defeats triage.
RejectionReason = Literal[
    "agency_not_supported",       # not CCHCS in v1
    "no_attachments",             # email body alone; ambiguous bid
    "parse_failed",               # Vision/OCR couldn't extract a contract
    "tax_lookup_failed",          # CDTFA call failed for the ship-to
    "missing_solicitation_number",
    "duplicate_thread",           # already-ingested thread; not new info
    "unrecognized_form_code",     # attachment matched no FormCode literal
    "low_parse_confidence",       # parser produced output but won't trust it
    "other",                      # ALWAYS pair with reason_detail
]


class IngestRejection(BaseModel):
    """One row per email the ingest pipeline considered and refused.

    Constructed at the point of refusal; written once by
    `write_ingest_rejection`. Append-only.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    rejection_id: str = Field(
        min_length=4, max_length=80,
        pattern=r"^[A-Za-z0-9_\-]+$",
        description=(
            "Stable id. Convention: "
            "'rej_<gmail_msg_id_or_thread>_<epoch_secs>'."
        ),
    )

    # Source identity — three optional pointers so triage can re-find
    # the original email from any one of them.
    source_email_id: str | None = Field(default=None, max_length=128)
    source_thread_id: str | None = Field(default=None, max_length=128)
    sender_email: str | None = Field(default=None, max_length=128)
    subject: str | None = Field(default=None, max_length=200)

    # Why we refused.
    reason_code: RejectionReason
    reason_detail: str | None = Field(default=None, max_length=1000)

    # First ~1KB of the body for triage without re-fetching from Gmail.
    raw_excerpt: str | None = Field(default=None, max_length=1000)

    # Timestamps.
    received_at: datetime | None = Field(
        default=None,
        description="When the buyer sent the email (RFC 2822 Date header).",
    )
    rejected_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    # Provenance.
    parser_version: str = Field(default="unknown", max_length=32)
