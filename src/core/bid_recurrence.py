"""Detect bid recurrence — when the same institution sends substantially the
same items+quantities back for re-bid.

Government buyers re-bid the same SKUs on a cadence (CCHCS / CDCR / CalVet
all do this — 30/60/90-day cycles for art supplies, medical supplies,
electronics, etc.). When a new PC/RFQ comes in for items we've quoted
before at the same institution, the operator should see that in one
click and reuse prior pricing as a starting point.

Mike's 2026-05-05 ask (auto-PC pc_a523d364, CCHCS / Sommony Pech vs
his manual PC AMS 704 - RHU Art Supplies, Carolyn Montgomery / CIW RHU):
"same institution + same item descriptions + same QTY = bid indicator".
The institution is the durable signal because requestor names rotate
even when the buying program doesn't.

Read-side only — this module queries existing records and returns matches.
It does NOT mutate ingest. UI consumes via `find_recurring_bids` on
detail-page render. At-ingest persistence is a follow-up PR once the
chip has fired on real data and the threshold is tuned.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

_TOKEN_RE = re.compile(r"[a-z0-9]+")

DEFAULT_OVERLAP_THRESHOLD = 0.75
DEFAULT_DESC_THRESHOLD = 0.65

# Known agency keys the resolver returns (per src/core/institution_resolver.py).
# When an input is a raw agency string (e.g. ingest stamped "cchcs" directly
# without going through the resolver), we promote it into the same agency:
# bucket so it matches resolver-normalized siblings.
_KNOWN_AGENCY_KEYS = frozenset({"cchcs", "cdcr", "calvet", "dsh", "dgs", "csu"})


def _normalize_institution(s: str) -> str:
    """Canonical institution key for cross-record matching.

    Per `feedback_institution_resolver_canonical`: any code comparing
    institution strings MUST route through `institution_resolver.resolve()`
    and compare on the agency bucket. The resolver maps CIW / CHCF / SAC
    / "Chino-Corona" / ZIP 92880 / etc. all to agency=`cchcs` — string
    comparison says different, the resolver says same.

    Returns one of:
      "agency:<key>"  — resolver-canonicalized to a known agency bucket
                        (cchcs/cdcr/calvet/dsh/dgs/csu)
      raw lowercase   — when the resolver can't classify (e.g. a vendor
                        name or unknown institution); records with
                        identical raw strings still match.
      ""              — empty input; never matches.
    """
    if not s:
        return ""
    raw = re.sub(r"\s+", " ", str(s).strip().lower())
    # Path 1: resolver maps facility-level names to agency.
    try:
        from src.core.institution_resolver import resolve
        result = resolve(s)
        agency = (result.get("agency") or "").lower()
        if agency:
            return f"agency:{agency}"
    except Exception:
        pass
    # Path 2: input is already a raw agency string ("cchcs" / "cdcr" /
    # etc.) that the resolver doesn't round-trip. Match it to the same
    # bucket the resolver would have used so a CHCF-tagged PC and a
    # cchcs-tagged ingest still group together.
    if raw in _KNOWN_AGENCY_KEYS:
        return f"agency:{raw}"
    return raw


def _tokens(s: str) -> set:
    """Lowercase alphanumeric token set."""
    if not s:
        return set()
    return set(_TOKEN_RE.findall(str(s).lower()))


def _description_overlap(a: str, b: str) -> float:
    """Jaccard token overlap, 0..1. Empty inputs return 0.0."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _qty_int(v: Any) -> Optional[int]:
    """Coerce a qty value to int, accepting str / float / None.
    Returns None when the value can't be coerced."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _items_match(
    item_a: Dict[str, Any],
    item_b: Dict[str, Any],
    *,
    desc_threshold: float = DEFAULT_DESC_THRESHOLD,
) -> bool:
    """Two items match when description token-overlap >= threshold AND
    qty is equal. Both signals are required — same-qty alone (e.g. qty=1
    boilerplate) would over-match; same-description with different qty is
    a different bid (e.g. operator bid 5 last time, buyer wants 50 now)."""
    qa = _qty_int(item_a.get("qty") or item_a.get("quantity"))
    qb = _qty_int(item_b.get("qty") or item_b.get("quantity"))
    if qa is None or qb is None or qa != qb:
        return False
    return _description_overlap(
        item_a.get("description", ""),
        item_b.get("description", ""),
    ) >= desc_threshold


def _items_overlap_pct(
    items_a: List[Dict[str, Any]],
    items_b: List[Dict[str, Any]],
    *,
    desc_threshold: float = DEFAULT_DESC_THRESHOLD,
) -> float:
    """Fraction of items_a (the new record) that have a match in items_b
    (the prior record). Asymmetric on purpose: the question we want to
    answer is 'do my CURRENT items appear in this prior bid?', not
    'are these two bids identical?'. Same answer when item sets are
    equal, different when one side is a strict subset."""
    if not items_a or not items_b:
        return 0.0
    matched = 0
    for ia in items_a:
        for ib in items_b:
            if _items_match(ia, ib, desc_threshold=desc_threshold):
                matched += 1
                break
    return matched / len(items_a)


def find_recurring_bids(
    record: Dict[str, Any],
    all_records: Dict[str, Dict[str, Any]],
    *,
    record_id: Optional[str] = None,
    overlap_threshold: float = DEFAULT_OVERLAP_THRESHOLD,
    desc_threshold: float = DEFAULT_DESC_THRESHOLD,
    max_results: int = 5,
) -> List[Dict[str, Any]]:
    """Find prior records that look like bid recurrence vs the current one.

    Args:
        record: the current PC or RFQ dict (needs `institution` + `items`).
        all_records: id -> record map (e.g., the full price_checks dict).
        record_id: id of the current record, excluded from results.
        overlap_threshold: fraction of current items that must match (default 0.75).
        desc_threshold: per-item description Jaccard threshold (default 0.65,
            same as catalog matcher per CLAUDE.md → never lower without
            cross-category accuracy testing).
        max_results: cap on returned matches.

    Returns: list of dicts (most recent first):
        {id, pc_number, created_at, overlap_pct, matched_items, total_items,
         status, requestor, url}
    """
    cur_inst = _normalize_institution(record.get("institution", ""))
    if not cur_inst:
        return []
    cur_items = record.get("items") or []
    if not cur_items:
        return []

    matches = []
    for rid, prior in (all_records or {}).items():
        if rid == record_id:
            continue
        if not isinstance(prior, dict):
            continue
        if _normalize_institution(prior.get("institution", "")) != cur_inst:
            continue
        prior_items = prior.get("items") or []
        if not prior_items:
            continue
        overlap = _items_overlap_pct(
            cur_items, prior_items, desc_threshold=desc_threshold
        )
        if overlap < overlap_threshold:
            continue
        matched_count = sum(
            1 for ia in cur_items
            if any(_items_match(ia, ib, desc_threshold=desc_threshold)
                   for ib in prior_items)
        )
        # URL prefix depends on record type — PC and RFQ use different
        # detail routes. Detect by id prefix; fall back to /pricecheck/
        # for legacy rows whose ids predate the prefix convention.
        if rid.startswith("rfq_"):
            url = f"/rfq/{rid}"
        else:
            url = f"/pricecheck/{rid}"
        matches.append({
            "id": rid,
            "pc_number": (
                prior.get("pc_number")
                or prior.get("solicitation_number")
                or prior.get("rfq_number")
                or ""
            ),
            "created_at": prior.get("created_at", ""),
            "overlap_pct": round(overlap, 2),
            "matched_items": matched_count,
            "total_items": len(cur_items),
            "status": prior.get("status", ""),
            "requestor": (
                prior.get("requestor")
                or prior.get("requestor_name")
                or ""
            ),
            "url": url,
        })

    matches.sort(key=lambda m: m["created_at"], reverse=True)
    return matches[:max_results]
