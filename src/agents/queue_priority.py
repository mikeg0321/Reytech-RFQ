"""PR-T — queue auto-prioritization.

Mike's spec: rank Price Checks by likelihood-to-send-and-win so the
operator works the highest-leverage items first. Mike's queue today is
unordered (date-only) which means a $200 dismissable noise PC sits
above a $40k ready-to-send CCHCS quote with 18 hours left.

Outputs a numeric score + a breakdown dict that the home template
renders next to each top-priority row. Pure function — no DB access
from this module (caller passes the PC dict already loaded from
canonical state). Tests stay deterministic and DB-free.

Score components (additive, 0-150 typical max):
  - priced_ready (50pt):     all items priced, no review flags, not sent yet
  - mark_sent_ready (20pt):  every active item has both unit_cost + unit_price
  - deadline_urgency (30pt): closer to due_date = more pts (3-tier ladder)
  - dollar_value (50pt cap): log-scaled (a $40k quote beats a $400 one)
  - agency_winrate (20pt):   agency's recent operator_drift_line win signal
                              (computed by caller, not this module)

Score 0 means "not worth surfacing." Default ranking is score DESC with
deterministic tiebreak by created_at ASC (older PC wins ties — fairness
to long-sitting work).
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("reytech.queue_priority")


PRICED_STATUSES = {
    "priced", "draft", "quoted", "generated", "auto_drafted", "ready",
    "completed",
}
"""States where an operator has done enough work that 'send' is the next
step. Excludes new/parsed (still need pricing) and terminal states
(won/lost/sent/dismissed)."""


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        s = str(v).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def _hours_until(due: Any, now: Optional[datetime] = None) -> Optional[float]:
    d = _parse_dt(due)
    if not d:
        return None
    n = now or datetime.now(d.tzinfo) if d.tzinfo else (now or datetime.now())
    try:
        return (d - n).total_seconds() / 3600.0
    except (TypeError, OverflowError):
        return None


def _active_items(pc: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = pc.get("items") or pc.get("line_items") or []
    return [it for it in items if isinstance(it, dict)
            and not it.get("no_bid")]


def _item_price(it: Dict[str, Any]) -> float:
    for key in ("unit_price", "bid_price"):
        v = it.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    pricing = it.get("pricing") or {}
    for key in ("recommended_price", "unit_price"):
        v = pricing.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _item_cost(it: Dict[str, Any]) -> float:
    for key in ("vendor_cost", "supplier_cost", "unit_cost"):
        v = it.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    pricing = it.get("pricing") or {}
    for key in ("unit_cost", "catalog_cost", "web_cost"):
        v = pricing.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _quote_total(pc: Dict[str, Any]) -> float:
    """Best estimate of dollar value: sum(qty * unit_price) over active
    items. Falls back to pc.total when items math comes back zero."""
    total = 0.0
    for it in _active_items(pc):
        try:
            qty = float(it.get("quantity") or it.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        total += qty * _item_price(it)
    if total > 0:
        return total
    for key in ("total", "subtotal", "grand_total"):
        v = pc.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def compute_priority_score(
    pc: Dict[str, Any],
    agency_winrate: Optional[float] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return {score, breakdown, total_value} for one PC.

    `agency_winrate` ∈ [0, 1] from operator_drift_line outcomes when
    the caller has it; None when no signal. Pure function.
    """
    breakdown: Dict[str, int] = {}
    status = (pc.get("status") or "").strip().lower()
    items = _active_items(pc)
    n = len(items)
    total = _quote_total(pc)

    # Priced+ready: high-value baseline. If all active items priced
    # and status is in the operator-done-pricing set, +50.
    priced_count = sum(1 for it in items if _item_price(it) > 0)
    if n > 0 and priced_count == n and status in PRICED_STATUSES:
        breakdown["priced_ready"] = 50
    elif n > 0 and priced_count == n:
        # priced but status hasn't caught up — still worth +20
        breakdown["priced_ready"] = 20
    else:
        breakdown["priced_ready"] = 0

    # Mark-sent ready: cost + price both present on every line.
    if n > 0 and all(_item_cost(it) > 0 and _item_price(it) > 0 for it in items):
        breakdown["mark_sent_ready"] = 20
    else:
        breakdown["mark_sent_ready"] = 0

    # Deadline urgency (4-tier ladder + stale-cutoff). Severely overdue
    # PCs (> 72h past due_date) are treated as stale — operator either
    # abandoned them or the deadline was wrong. Don't surface them in
    # the "top to send now" widget; they belong in the full queue.
    hours = _hours_until(pc.get("due_date") or pc.get("deadline"), now=now)
    if hours is None:
        breakdown["deadline_urgency"] = 0
    elif hours < -72:
        breakdown["deadline_urgency"] = 0  # stale / abandoned
    elif hours <= 24:
        breakdown["deadline_urgency"] = 30
    elif hours <= 48:
        breakdown["deadline_urgency"] = 20
    elif hours <= 72:
        breakdown["deadline_urgency"] = 10
    else:
        breakdown["deadline_urgency"] = 0

    # Dollar value: log-scaled. $400 → +13pt, $4k → +25pt, $40k → +38pt
    if total > 0:
        breakdown["dollar_value"] = min(50, int(math.log10(total + 1) * 13))
    else:
        breakdown["dollar_value"] = 0

    # Agency winrate signal (passed in by caller)
    if agency_winrate is not None and agency_winrate > 0:
        breakdown["agency_winrate"] = int(round(20 * float(agency_winrate)))
    else:
        breakdown["agency_winrate"] = 0

    score = sum(breakdown.values())
    return {
        "score": score,
        "breakdown": breakdown,
        "total_value": round(total, 2),
        "deadline_hours": round(hours, 1) if hours is not None else None,
        "items_priced": priced_count,
        "items_total": n,
    }


def rank_pcs(
    pcs: Dict[str, Dict[str, Any]],
    agency_winrates: Optional[Dict[str, float]] = None,
    limit: int = 5,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Rank a dict of {pc_id: pc} by priority score. Returns top-N list
    of {pc_id, pc, score, breakdown, total_value, ...} dicts.

    `agency_winrates` maps agency_key → win_rate ∈ [0, 1]. When missing
    for a PC's agency, the agency_winrate component scores 0.

    Score 0 PCs are filtered out — surfacing them in the "top to send"
    widget would be noise. If the result is empty, the home page can
    fall back to date-ordered queue display.
    """
    agency_winrates = agency_winrates or {}
    scored: List[Tuple[int, str, Dict[str, Any], Dict[str, Any]]] = []
    for pid, pc in (pcs or {}).items():
        if not isinstance(pc, dict):
            continue
        agency_key = (pc.get("agency") or pc.get("institution") or "").strip().lower()
        wr = agency_winrates.get(agency_key)
        if wr is None and "(" not in agency_key:
            # fuzzy lookup by prefix match (cchcs_non_it → cchcs)
            for k, v in agency_winrates.items():
                if k and agency_key.startswith(k):
                    wr = v
                    break
        result = compute_priority_score(pc, agency_winrate=wr, now=now)
        if result["score"] <= 0:
            continue
        scored.append((result["score"], pid, pc, result))

    # Sort by score DESC, then by created_at ASC (older wins ties)
    def _sort_key(t: Tuple[int, str, Dict[str, Any], Dict[str, Any]]):
        score, pid, pc, _r = t
        created = pc.get("created_at") or ""
        return (-score, created)

    scored.sort(key=_sort_key)
    top: List[Dict[str, Any]] = []
    for score, pid, pc, result in scored[:limit]:
        top.append({
            "pc_id": pid,
            "pc_number": pc.get("pc_number") or pid[:12],
            "agency": pc.get("agency") or pc.get("institution") or "",
            "requestor": pc.get("requestor") or pc.get("requestor_name") or "",
            "status": pc.get("status") or "",
            "created_at": pc.get("created_at") or "",
            "due_date": pc.get("due_date") or pc.get("deadline") or "",
            **result,
        })
    return top
