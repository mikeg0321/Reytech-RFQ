"""Observed-send detector — find outbound RFQ-quote messages in
Mike's Gmail Sent folder and match them back to existing RFQ/PC records.

Post-quote queue item 23 (2026-05-07). The current
"Mark-Sent-Manually" modal is single-file, lossy, and not searchable;
operators routinely send a real quote, fail to click the modal, and
the record stays stuck at status='generated' forever. We have Gmail
API plumbing already (src/core/gmail_api.py); this module uses it to
scan `in:sent` for outbound messages that look like RFQ packages, and
returns candidate matches for an operator-confirmation step (or, after
the 8-week 100%-confirm-rate doctrine, an auto-attach flip).

This PR (PR-G1) ships the detector primitive only:
  * `detect_observed_sends(since_days=, max_messages=) → result dict`
  * Match signals: reytech_quote_number in subject (highest), Gmail
    threadId matches an inbound record's email_thread_id, solicitation
    number in subject, requestor_email recency.
  * `already_attached` flag for matches whose gmail_message_id is
    already in the record's `gmail_message_ids` (PR #808 / PR-E
    forward path) — those are not "missed sends", just observations.
  * No writes, no UI. The caller (admin endpoint or future modal)
    decides what to do with the candidates.

Future PRs in this arc:
  * PR-G2 — `observed_sends` table to persist confirmed/rejected
    decisions + 8-week confirm-rate metric.
  * PR-G3 — UI modal on RFQ/PC detail to confirm/reject.
  * PR-G4 — auto-flip when confirm-rate ≥ 100% over 8 weeks.
  * PR-H — Drive backup of confirmed observed-sends.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


# ─── Match signal patterns ────────────────────────────────────────────


# Reytech quote numbers look like R26Q40, R26Q123, R25Q9.
_QUOTE_NUMBER_RE = re.compile(r"\bR\d{2}Q\d+[A-Z]?\b")

# Solicitation numbers — wide variety. Conservative heuristic: 4+ digit
# strings adjacent to common solicitation prefixes the buyers use.
_SOL_NUMBER_RES = [
    re.compile(r"\bRFQ[\s#:]*\d{6,}\b", re.IGNORECASE),
    re.compile(r"\bSolicitation[\s#:]*\d{6,}\b", re.IGNORECASE),
    re.compile(r"\b\d{8}\b"),  # bare 8-digit numbers (CCHCS shape)
]

# Subject prefixes that strongly signal an outbound quote send.
_QUOTE_SUBJECT_HINTS = (
    "reytech quote",
    "reytech inc quote",
    "quote r",
    "rfq response",
    "your quote",
    "bid response",
)


def _extract_quote_numbers(text: str) -> List[str]:
    """Return all R##Q## quote numbers in `text`, uppercase + deduped."""
    if not text:
        return []
    seen: List[str] = []
    for m in _QUOTE_NUMBER_RE.finditer(text):
        v = m.group(0).upper()
        if v not in seen:
            seen.append(v)
    return seen


def _extract_solicitation_numbers(text: str) -> List[str]:
    """Return likely solicitation numbers in `text`, deduped."""
    if not text:
        return []
    seen: List[str] = []
    for r in _SOL_NUMBER_RES:
        for m in r.finditer(text):
            v = m.group(0).strip()
            # Normalize: strip prefix like "RFQ #" / "Solicitation:" so
            # we have just the digit run for cross-record matching.
            digits = re.search(r"\d{6,}", v)
            if digits:
                d = digits.group(0)
                if d not in seen:
                    seen.append(d)
    return seen


def _looks_like_quote_subject(subject: str) -> bool:
    s = (subject or "").lower()
    return any(h in s for h in _QUOTE_SUBJECT_HINTS) \
        or bool(_QUOTE_NUMBER_RE.search(subject or ""))


# ─── Record indexing helpers ─────────────────────────────────────────


def _index_records_by_quote_number(
        rfqs: Dict, pcs: Dict) -> Dict[str, tuple]:
    """quote_number → (kind, id). Reytech quote numbers are globally
    unique; an R26Q40 belongs to exactly one record."""
    idx: Dict[str, tuple] = {}
    for rid, r in (rfqs or {}).items():
        qn = (r.get("reytech_quote_number")
              or r.get("quote_number") or "").strip().upper()
        if qn:
            idx[qn] = ("rfq", rid)
    for pid, p in (pcs or {}).items():
        qn = (p.get("reytech_quote_number")
              or p.get("quote_number") or "").strip().upper()
        if qn:
            idx[qn] = ("pc", pid)
    return idx


def _index_records_by_thread_id(
        rfqs: Dict, pcs: Dict) -> Dict[str, tuple]:
    """email_thread_id → (kind, id). One thread = one inbound record;
    a thread can host multiple outbound sends but they all match back."""
    idx: Dict[str, tuple] = {}
    for rid, r in (rfqs or {}).items():
        tid = (r.get("email_thread_id") or "").strip()
        if tid:
            idx[tid] = ("rfq", rid)
    for pid, p in (pcs or {}).items():
        tid = (p.get("email_thread_id") or "").strip()
        if tid:
            idx[tid] = ("pc", pid)
    return idx


def _index_records_by_solicitation(
        rfqs: Dict, pcs: Dict) -> Dict[str, list]:
    """digit-only sol_number → list of (kind, id). Multiple records
    may share a sol number across years."""
    idx: Dict[str, list] = {}
    for rid, r in (rfqs or {}).items():
        sol = re.sub(r"\D", "", (r.get("solicitation_number") or ""))
        if sol and len(sol) >= 6:
            idx.setdefault(sol, []).append(("rfq", rid))
    for pid, p in (pcs or {}).items():
        sol = re.sub(r"\D", "", (p.get("solicitation_number") or ""))
        if sol and len(sol) >= 6:
            idx.setdefault(sol, []).append(("pc", pid))
    return idx


def _record_already_has_message(record: Dict, gmail_message_id: str) -> bool:
    if not gmail_message_id:
        return False
    msgs = record.get("gmail_message_ids") or []
    if isinstance(msgs, list) and gmail_message_id in msgs:
        return True
    # Legacy: some records stored a single gmail_message_id field
    legacy = (record.get("sent_gmail_message_id") or "").strip()
    if legacy == gmail_message_id:
        return True
    return False


# ─── Match resolution ────────────────────────────────────────────────


def _match_one_message(
        meta: Dict,
        *,
        rfqs: Dict, pcs: Dict,
        idx_qn: Dict, idx_tid: Dict, idx_sol: Dict,
        ) -> Optional[Dict]:
    """Resolve a single outbound message to a record. Returns a match
    dict on hit, None on miss. Caller batches into the run report."""
    subject = meta.get("subject", "") or ""
    thread_id = (meta.get("thread_id") or "").strip()
    gmail_id = meta.get("gmail_id") or ""

    # Signal 1 — quote number in subject (highest confidence).
    for qn in _extract_quote_numbers(subject):
        hit = idx_qn.get(qn)
        if hit:
            kind, rid = hit
            record = (rfqs if kind == "rfq" else pcs).get(rid, {})
            return {
                "match_signal": "quote_number",
                "match_value": qn,
                "confidence": 0.95,
                "matched_record_id": rid,
                "matched_record_kind": kind,
                "already_attached": _record_already_has_message(
                    record, gmail_id),
            }

    # Signal 2 — Gmail threadId matches an inbound record's thread_id.
    if thread_id:
        hit = idx_tid.get(thread_id)
        if hit:
            kind, rid = hit
            record = (rfqs if kind == "rfq" else pcs).get(rid, {})
            return {
                "match_signal": "thread_id",
                "match_value": thread_id,
                "confidence": 0.90,
                "matched_record_id": rid,
                "matched_record_kind": kind,
                "already_attached": _record_already_has_message(
                    record, gmail_id),
            }

    # Signal 3 — solicitation number in subject.
    for sol in _extract_solicitation_numbers(subject):
        candidates = idx_sol.get(sol, [])
        if len(candidates) == 1:
            kind, rid = candidates[0]
            record = (rfqs if kind == "rfq" else pcs).get(rid, {})
            return {
                "match_signal": "solicitation_number",
                "match_value": sol,
                "confidence": 0.75,
                "matched_record_id": rid,
                "matched_record_kind": kind,
                "already_attached": _record_already_has_message(
                    record, gmail_id),
            }
        if len(candidates) > 1:
            # Ambiguous — include the first candidate but flag low
            # confidence so the operator UI shows a warning.
            kind, rid = candidates[0]
            record = (rfqs if kind == "rfq" else pcs).get(rid, {})
            return {
                "match_signal": "solicitation_number_ambiguous",
                "match_value": sol,
                "confidence": 0.40,
                "matched_record_id": rid,
                "matched_record_kind": kind,
                "ambiguous_candidates": [r for _k, r in candidates],
                "already_attached": _record_already_has_message(
                    record, gmail_id),
            }

    return None


# ─── Public API ──────────────────────────────────────────────────────


def detect_observed_sends(
        *,
        since_days: int = 7,
        max_messages: int = 200,
        rfqs: Optional[Dict] = None,
        pcs: Optional[Dict] = None,
        gmail_service=None,
        ) -> Dict:
    """Scan `in:sent newer_than:Nd` and match each outbound message to
    an existing RFQ/PC record.

    `rfqs`, `pcs`, `gmail_service` are injection points for tests; in
    production they're loaded from the canonical data layer + Gmail API.

    Returns:
      {
        "ok": bool,
        "since_days": N,
        "scanned": int,         # total messages fetched from Sent
        "matches": [...],       # quote-number / thread / sol matches
        "unmatched": [...],     # outbound messages that LOOK like
                                # quote sends but didn't match a record
        "skipped_non_quote": int,  # ignored messages (replies, internal)
        "error": str,           # populated on hard failure
      }

    Match dict shape (per entry in `matches`):
      {
        "gmail_message_id": ...,
        "thread_id": ...,
        "subject": ...,
        "to": ...,
        "date": ...,
        "matched_record_id": "rfq_xxx",
        "matched_record_kind": "rfq",
        "match_signal": "quote_number",
        "match_value": "R26Q40",
        "confidence": 0.95,
        "already_attached": bool,
      }
    """
    # Lazy imports so the module is importable without Gmail/data deps.
    if rfqs is None or pcs is None:
        try:
            from src.api.data_layer import load_rfqs, _load_price_checks
            if rfqs is None:
                rfqs = load_rfqs() or {}
            if pcs is None:
                pcs = _load_price_checks() or {}
        except Exception as e:
            return {"ok": False,
                    "error": f"could not load records: {e}",
                    "since_days": since_days, "scanned": 0,
                    "matches": [], "unmatched": [],
                    "skipped_non_quote": 0}

    if gmail_service is None:
        try:
            from src.core.gmail_api import get_service, is_configured
            if not is_configured():
                return {"ok": False,
                        "error": "Gmail not configured (no refresh token)",
                        "since_days": since_days, "scanned": 0,
                        "matches": [], "unmatched": [],
                        "skipped_non_quote": 0}
            gmail_service = get_service("sales")
        except Exception as e:
            return {"ok": False,
                    "error": f"Gmail service unavailable: {e}",
                    "since_days": since_days, "scanned": 0,
                    "matches": [], "unmatched": [],
                    "skipped_non_quote": 0}

    # Build indices once per scan.
    idx_qn = _index_records_by_quote_number(rfqs, pcs)
    idx_tid = _index_records_by_thread_id(rfqs, pcs)
    idx_sol = _index_records_by_solicitation(rfqs, pcs)

    # Query Gmail for sent messages in window.
    query = f"in:sent newer_than:{int(since_days)}d"
    try:
        from src.core.gmail_api import list_message_ids, get_message_metadata
        ids = list_message_ids(
            gmail_service, query=query, max_results=max_messages)
    except Exception as e:
        return {"ok": False,
                "error": f"list_message_ids failed: {e}",
                "since_days": since_days, "scanned": 0,
                "matches": [], "unmatched": [],
                "skipped_non_quote": 0}

    matches: List[Dict] = []
    unmatched: List[Dict] = []
    skipped_non_quote = 0

    for msg_id in ids:
        try:
            meta = get_message_metadata(gmail_service, msg_id)
        except Exception as e:
            log.debug("metadata fetch failed for %s: %s", msg_id, e)
            continue

        subject = meta.get("subject", "") or ""
        # Skip messages that don't even look like a quote send. Cheaper
        # than running the full match pipeline.
        if not _looks_like_quote_subject(subject):
            skipped_non_quote += 1
            continue

        match = _match_one_message(
            meta, rfqs=rfqs, pcs=pcs,
            idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol,
        )
        common = {
            "gmail_message_id": meta.get("gmail_id", msg_id),
            "thread_id": meta.get("thread_id", ""),
            "subject": subject[:200],
            "to": meta.get("to", "")[:200],
            "date": meta.get("date", ""),
        }
        if match:
            common.update(match)
            matches.append(common)
        else:
            common["match_signal"] = "no_match"
            unmatched.append(common)

    return {
        "ok": True,
        "since_days": since_days,
        "scanned": len(ids),
        "matches": matches,
        "unmatched": unmatched,
        "skipped_non_quote": skipped_non_quote,
    }
