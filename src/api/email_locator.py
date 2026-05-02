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


def _extract_identity(rfq):
    """Pull the searchable identity off an RFQ record once.

    Returns (buyer_emails, sol_terms, subj_terms) — used by both the
    legacy single-query builder and the progressive broadening builder.
    """
    buyer_emails = []
    for k in ("requestor_email", "original_sender", "email_sender"):
        v = (rfq.get(k) or "").strip().lower()
        if v and "@" in v and v not in buyer_emails:
            buyer_emails.append(v)

    sol_terms = []
    for k in ("solicitation_number", "rfq_number", "reytech_quote_number"):
        v = (rfq.get(k) or "").strip()
        if v and len(v) >= 3 and v.upper() not in ("WORKSHEET", "GOOD", "RFQ", "TBD"):
            sol_terms.append(f'"{v}"')

    subj_terms = []
    subj = (rfq.get("email_subject") or rfq.get("subject") or "").strip()
    if subj:
        # Strip Re:/Fwd: prefixes, take first 5 meaningful words
        subj_clean = re.sub(r'^(re:|fwd?:|fw:)\s*', '', subj, flags=re.I).strip()
        words = [w for w in re.findall(r"[A-Za-z0-9]+", subj_clean) if len(w) >= 4][:5]
        if words:
            subj_terms.append('"' + " ".join(words) + '"')

    return buyer_emails, sol_terms, subj_terms


def build_locator_query(rfq, max_age_days: int = 120) -> str:
    """Build a single Gmail search query (strict tier).

    Kept for back-compat — `build_locator_queries` is what the locator
    uses now. Returns "" when there's nothing to search on.
    """
    qs = build_locator_queries(rfq, max_age_days=max_age_days)
    return qs[0] if qs else ""


def build_locator_queries(rfq, max_age_days: int = 120) -> List[str]:
    """Build a tiered list of Gmail queries — strict → relaxed.

    The original single-query design (PR-B1) AND-ed `from:<buyer>` with
    the sol# / subject terms. That fails the *most common* operator
    flow: Mike forwards a buyer's RFQ from his personal inbox to
    sales@reytechinc.com, so the message appears in Gmail with
    `From: mike@reytechinc.com`, not the buyer's address. The strict
    `from:"keith@calvet.ca.gov"` filter then excludes it even though
    the body still contains "R26Q38".

    Tiered queries (Bug-8 fix 2026-05-02):
      1. **Strict** — `from:<buyer> AND <sol#>` (or subject if no sol#).
         Fastest hit when the buyer emailed us directly.
      2. **Sol-only** — `<sol#>` alone. Hits the forwarded variant
         because Gmail searches the message body by default. Skipped
         when the record has no sol#.
      3. **Subject-only** — `<subject keywords>` when no sol# at all.

    Caller runs them in order and accumulates unique results until the
    candidate cap is reached. Each query also gets the date window +
    `in:anywhere` so it sweeps Inbox + Sent + Spam + All Mail.
    """
    buyer_emails, sol_terms, subj_terms = _extract_identity(rfq)
    age_clause = f"newer_than:{max_age_days}d in:anywhere"

    queries: List[str] = []

    def _join_or(parts):
        if not parts:
            return ""
        if len(parts) == 1:
            return parts[0]
        return "(" + " OR ".join(parts) + ")"

    from_clause = ""
    if buyer_emails:
        from_clause = _join_or([f'from:"{e}"' for e in buyer_emails])

    sol_clause = _join_or(sol_terms)
    subj_clause = _join_or(subj_terms)

    # Tier 1 — strict: from: AND (sol# or subject). Most specific.
    body_clause = sol_clause or subj_clause
    if from_clause and body_clause:
        queries.append(f"{from_clause} {body_clause} {age_clause}")
    elif from_clause:
        queries.append(f"{from_clause} {age_clause}")

    # Tier 2 — sol# alone, no `from:` filter. Catches forwarded mail
    # where the From line is the operator's own address but the body
    # carries the sol#. Always run when we have a sol# (it's a superset
    # of tier-1 results when from: is present, but Gmail returns ranked
    # results so tier-1 hits still bubble up first).
    if sol_clause:
        q2 = f"{sol_clause} {age_clause}"
        if q2 not in queries:
            queries.append(q2)

    # Tier 3 — subject keywords as a final fallback. Run when no sol#
    # is set at all OR when sol# tier returned nothing (the caller is
    # the one that decides to keep going; we just enumerate tiers).
    if subj_clause:
        q3 = f"{subj_clause} {age_clause}"
        if q3 not in queries:
            queries.append(q3)

    return queries


def locate_candidate_emails(service, rfq, max_results: int = 10):
    """Search Gmail for messages that could be the original RFQ email.

    Returns a list of candidate dicts (newest first, deduped on
    gmail_id) accumulated across the progressive query tiers. Each
    candidate carries ``match_tier`` (1=strict, 2=sol-only, 3=subject)
    so the picker UI can hint why a result showed up.

    Bug-8 fix 2026-05-02: forwarded RFQs (Mike forwards from his
    personal inbox to sales@reytechinc.com) have `From:` = operator's
    own address, so the strict tier-1 query misses. Tier 2 drops the
    `from:` filter and matches the sol# in the body — that's the
    common path for `rfq_7813c4e1`-style cases.
    """
    queries = build_locator_queries(rfq)
    if not queries:
        log.info("Email locator: no searchable identity on RFQ %s",
                 rfq.get("id", "?"))
        return []

    try:
        from src.core.gmail_api import list_message_ids, get_message_metadata
    except Exception as e:
        log.error("Email locator: gmail_api import failed: %s", e)
        return []

    seen = set()
    candidates = []
    for tier_idx, query in enumerate(queries, start=1):
        if len(candidates) >= max_results:
            break
        log.info("Email locator T%d: %s", tier_idx, query[:200])
        try:
            ids = list_message_ids(service, query=query,
                                   max_results=max_results)
        except Exception as e:
            log.error("Email locator T%d list_message_ids failed: %s",
                      tier_idx, e)
            continue
        for gid in ids:
            if gid in seen or len(candidates) >= max_results:
                continue
            seen.add(gid)
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
                    "match_tier": tier_idx,
                })
            except Exception as _e:
                log.debug("metadata fetch failed for %s: %s", gid, _e)

    return candidates
