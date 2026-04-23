"""Bundle-2 PR-2c: ingest-time ghost-record detection.

Source: `feedback_ghost_record_heuristics` + audit item E in the
2026-04-22 session audit. A "ghost" record is one ingest created by
mistake — self-as-buyer, zero-item parse fail, 45XXXXXX synthetic
prefix, Reytech-internal sender treated as a buyer. Before this
module they accumulated in the queue and polluted every triage /
NEXT UP / analytics view.

### Contract
`detect_ghost_pattern(record, email_sender="", items_parsed=0)` →
`Optional[str]` return value is the ghost reason (short slug) or
None if the record looks legitimate. The caller (ingest pipeline,
backfill sweep) is responsible for doing something with the reason —
typically stamping `record["hidden_reason"] = reason` so the queue /
triage filter can skip it.

### Policy: marks-not-deletes
Detection NEVER deletes records. A wrongly-flagged ghost can always
be un-hidden by clearing `hidden_reason`. This keeps operator trust
in the app — "the record is wrong but it's still there if I need it"
is a much better failure mode than "the record vanished."

### Policy: feature-flagged
The ingest caller reads `ingest.ghost_quarantine_enabled` (default
False) before calling. Shadow mode = detection runs but doesn't
stamp (just emits telemetry). Go-live mode = stamping active.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

log = logging.getLogger("reytech.ghost_detection")


# ── Sender-domain allow-list (known CA agencies). Any sender on a
# domain OUTSIDE this list is a candidate for "non-agency = probably
# not a real buyer". Mike's direct quote: "@reytechinc.com addresses
# are NEVER buyers."
KNOWN_AGENCY_DOMAIN_SUFFIXES = (
    "cdcr.ca.gov",
    "cchcs.ca.gov",
    "calvet.ca.gov",
    "dsh.ca.gov",
    "dgs.ca.gov",
    "caltrans.ca.gov",
    "ca.gov",  # catch-all umbrella
    "dir.ca.gov",
    "dmv.ca.gov",
    "calrecycle.ca.gov",
    # Federal agencies occasionally buy too:
    ".gov",  # suffix match — any .gov TLD counts as gov
)

# Internal/partner senders that must NEVER be treated as buyers.
# These appear on ingested records when an operator forwards an RFQ
# from their own mailbox, or when a synthetic PO arrives from a
# vendor partner.
INTERNAL_SENDER_PATTERNS = (
    "reytechinc.com",
    "michael guadan",
    "michael.guadan",
)

# CCHCS synthetic-test prefix: real CCHCS PCs use 10843XXX / 10844XXX
# ranges. 45XXXXXX numbers are seeded by the agency's QA system and
# are never real quote requests (observed repeatedly in Feb-April
# 2026 ingests).
_SYNTHETIC_PREFIX_RE = re.compile(r"^45\d{5,}$")


# ── Reason slugs — keep short + greppable ──────────────────────────
REASON_SELF_BUYER = "self_buyer"
REASON_SYNTHETIC_PREFIX = "synthetic_45_prefix"
REASON_ZERO_ITEMS_BLANK_INSTITUTION = "zero_items_blank_institution"
REASON_INTERNAL_SENDER = "internal_sender"
REASON_NON_AGENCY_SENDER_SELF = "non_agency_sender_self_as_institution"


def _norm(s: Any) -> str:
    """Lower-strip a string-ish value, safe for None / int / odd types."""
    if s is None:
        return ""
    return str(s).strip().lower()


def _sender_domain(sender: str) -> str:
    """Return the lowercased domain of an email sender, or ''.
    Handles `Name <addr@host>` and bare `addr@host` forms."""
    if not sender:
        return ""
    m = re.search(r"<([^>]+)>", sender)
    addr = m.group(1) if m else sender
    if "@" in addr:
        return addr.split("@", 1)[1].strip().lower()
    return ""


def _is_known_agency_domain(domain: str) -> bool:
    if not domain:
        return False
    domain = domain.lower()
    return any(
        domain == suf or domain.endswith("." + suf) or domain.endswith(suf)
        for suf in KNOWN_AGENCY_DOMAIN_SUFFIXES
    )


def _is_internal_sender(sender: str) -> bool:
    s = _norm(sender)
    return any(pat in s for pat in INTERNAL_SENDER_PATTERNS)


def detect_ghost_pattern(
    record: Dict[str, Any],
    email_sender: str = "",
    items_parsed: Optional[int] = None,
) -> Optional[str]:
    """Decide whether a freshly-ingested record looks like a ghost.

    Returns the short reason slug for the first pattern that matches,
    or None if the record looks like a real buyer request. Order of
    checks matters — more-specific patterns fire first so the reason
    slug gives the operator (or the telemetry view) the most useful
    explanation.

    Examples:
      - sender=michael@reytechinc.com → REASON_INTERNAL_SENDER
      - pc_number=45007355, buyer=same as institution → SELF_BUYER
        (the "more specific" self-buyer signal wins over prefix)
      - items_parsed=0 AND institution blank → ZERO_ITEMS_BLANK_INSTITUTION

    Safe to call with partial input. Missing fields don't trigger
    spurious ghost flags.
    """
    if not isinstance(record, dict):
        return None

    # 1. Internal / partner sender — Mike's #1 ghost rule.
    if _is_internal_sender(email_sender):
        return REASON_INTERNAL_SENDER

    # 2. Self-buyer: the same human appears in both buyer and
    # institution columns. Agencies never buy from themselves; a
    # matching buyer==institution means either the parser glued the
    # sender's name into both slots, or the buyer typo'd their own
    # identity as the institution.
    buyer = _norm(
        record.get("buyer_name")
        or record.get("requestor_name")
        or record.get("buyer")
    )
    institution = _norm(
        record.get("institution")
        or record.get("agency")
        or record.get("department")
    )
    if buyer and institution and buyer == institution:
        return REASON_SELF_BUYER

    # 3. Non-agency sender AND institution matches the sender's
    # personal name — same failure mode as #2 but caught by a
    # different signal path (ingest-time matching).
    sender_domain = _sender_domain(email_sender)
    sender_local = ""
    if "@" in (email_sender or ""):
        sender_local = email_sender.split("@", 1)[0].strip().lower()
        m = re.search(r"<([^>]+)>", email_sender or "")
        if m:
            addr = m.group(1)
            if "@" in addr:
                sender_local = addr.split("@", 1)[0].strip().lower()
    if (
        sender_domain
        and not _is_known_agency_domain(sender_domain)
        and institution
        and sender_local
        and sender_local.replace(".", " ") in institution
    ):
        return REASON_NON_AGENCY_SENDER_SELF

    # 4. Synthetic CCHCS 45XXXXXX prefix.
    pc_num = _norm(
        record.get("pc_number")
        or record.get("rfq_number")
        or record.get("solicitation_number")
    )
    if pc_num and _SYNTHETIC_PREFIX_RE.match(pc_num):
        return REASON_SYNTHETIC_PREFIX

    # 5. Zero items parsed AND institution blank — pure parse-fail
    # "placeholder" with no identifying info. (Zero items with an
    # institution is fine — could be a form-only packet.)
    parsed = items_parsed
    if parsed is None:
        # Fall back to whatever's on the record.
        parsed = (
            record.get("items_parsed")
            or len(record.get("items") or record.get("line_items") or [])
        )
    if (parsed or 0) == 0 and not institution:
        return REASON_ZERO_ITEMS_BLANK_INSTITUTION

    return None


def is_quarantined(record: Dict[str, Any]) -> bool:
    """Thin helper for queue / triage filters. Returns True if the
    record carries a non-empty `hidden_reason`. Callers typically
    use this to decide `continue` inside a queue-building loop.
    """
    return bool(
        isinstance(record, dict) and str(record.get("hidden_reason") or "").strip()
    )
