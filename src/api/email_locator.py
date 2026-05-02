"""Locate the original buyer email for an RFQ/PC that was manually uploaded
or whose thread binding was lost.

Built 2026-05-01 (PR-B1). Mike's directive: "every PC/RFQ will have an
email, even manual upload has emails, are only manual because parse didnt
work or needs reset. locate email, is best." This module is the operator
escape hatch for that case.

Strategy:
  1. Build a focused Gmail search query from RFQ identity:
       - from: requestor_email (and original_sender if forward)
       - subject contains the solicitation number, OR
       - body+attachments contain the solicitation number
       - bounded by date window (90d back from RFQ created_at)
  2. Return up to N candidate threads with subject preview + Message-ID +
     threadId so the operator can pick the right one.
  3. /api/rfq/<id>/bind-email writes the picked Message-ID + threadId
     onto the RFQ record.

Pure helper — no DB, no Flask. Caller passes the RFQ dict + Gmail service.
Tests stub the service.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional

log = logging.getLogger(__name__)


def build_locator_query(rfq, max_age_days: int = 120) -> str:
    """Build a Gmail search query for locating the buyer's RFQ email.

    Examples:
      from:"keith@calvet.ca.gov" "R26Q38" newer_than:120d
      from:"keith@calvet.ca.gov" "Flushable Wipes" newer_than:120d

    Returns "" if there's nothing to search on (no buyer email + no sol#).
    """
    parts: List[str] = []

    buyer_emails = []
    for k in ("requestor_email", "original_sender", "email_sender"):
        v = (rfq.get(k) or "").strip().lower()
        if v and "@" in v and v not in buyer_emails:
            buyer_emails.append(v)

    if buyer_emails:
        if len(buyer_emails) == 1:
            parts.append(f'from:"{buyer_emails[0]}"')
        else:
            parts.append("(" + " OR ".join(f'from:"{e}"' for e in buyer_emails) + ")")

    # Match by solicitation number, RFQ number, or quote number — most
    # buyers include one of these in subject or body. Quote subjects with
    # all-caps strings match Gmail's case-insensitive search.
    sol_terms = []
    for k in ("solicitation_number", "rfq_number", "reytech_quote_number"):
        v = (rfq.get(k) or "").strip()
        if v and len(v) >= 3 and v.upper() not in ("WORKSHEET", "GOOD", "RFQ", "TBD"):
            sol_terms.append(f'"{v}"')

    # Subject keyword fallback (parsed_subject if present, else use the
    # uploaded filename's stem)
    subj = (rfq.get("email_subject") or rfq.get("subject") or "").strip()
    if not sol_terms and subj:
        # Strip Re:/Fwd: prefixes, take first 5-6 meaningful words
        subj_clean = re.sub(r'^(re:|fwd?:|fw:)\s*', '', subj, flags=re.I).strip()
        words = [w for w in re.findall(r"[A-Za-z0-9]+", subj_clean) if len(w) >= 4][:5]
        if words:
            sol_terms.append('"' + " ".join(words) + '"')

    if sol_terms:
        if len(sol_terms) == 1:
            parts.append(sol_terms[0])
        else:
            parts.append("(" + " OR ".join(sol_terms) + ")")

    if not parts:
        return ""  # nothing to search on

    parts.append(f"newer_than:{max_age_days}d")
    parts.append("in:anywhere")  # sweep Inbox + Spam + All Mail
    return " ".join(parts)


def locate_candidate_emails(service, rfq, max_results: int = 10):
    """Search Gmail for messages that could be the original RFQ email.

    Returns a list of candidate dicts:
      [{gmail_id, thread_id, subject, from, date, message_id, snippet}]
    sorted newest first. Empty list if no query buildable or zero matches.

    Caller (route handler) shows these in a picker UI; operator picks one;
    /api/rfq/<id>/bind-email persists the IDs onto the RFQ record.
    """
    query = build_locator_query(rfq)
    if not query:
        log.info("Email locator: no searchable identity on RFQ %s",
                 rfq.get("id", "?"))
        return []

    log.info("Email locator search: %s", query[:200])

    try:
        from src.core.gmail_api import list_message_ids, get_message_metadata
        ids = list_message_ids(service, query=query, max_results=max_results)
    except Exception as e:
        log.error("Email locator: list_message_ids failed: %s", e)
        return []

    candidates = []
    for gid in ids:
        try:
            meta = get_message_metadata(service, gid)
            candidates.append({
                "gmail_id": gid,
                "thread_id": meta.get("thread_id", ""),
                "subject": meta.get("subject", "")[:150],
                "from": meta.get("from", ""),
                "to": meta.get("to", ""),
                "cc": meta.get("cc", ""),
                "date": meta.get("date", ""),
                "message_id": meta.get("message_id", ""),
            })
        except Exception as _e:
            log.debug("metadata fetch failed for %s: %s", gid, _e)

    return candidates
