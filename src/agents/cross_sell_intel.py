"""Cross-sell intel — surface CA buyers who bought items Reytech sells
from competitors.

Mike P0 2026-05-11 needle-mover #2: "POs going out with items I have or
can source to sell." SCPRS publishes CA state procurement records ~3-4
times/week; scprs_po_lines.reytech_sells flag is now populated via the
real product_catalog (PR #900). This module aggregates that data into
the cross-sell prospect surfaces.

Three public functions:

  get_prospects(top_n, days_back)
    Ranked list of BUYERS who bought Reytech-sellable items from
    competitors. Score is (competitor_spend × recency_decay) — Mike's
    answer 2026-05-11 was "no" to pure $-ranking, so we blend recency.

  get_top_items_by_spend(top_n, days_back)
    Per-SKU rollup — which Reytech items have the most competitor
    spend (sized opportunity per item).

  get_general_recommendations(...)
    3-5 actionable bullets distilled from the data. The "intel that
    tells the operator what to DO" — not just a number dump.

Noise filters (learned from initial query 2026-05-11):
  - buyer_email IS NOT NULL  (skip POs with no buyer info — data quality)
  - supplier not Reytech itself  (obviously)
  - line_total in (0, 100_000]  (cap noise from service contracts)
  - reytech_sku NOT IN ('Services', '')  (catalog matched a service bucket)
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


_NOISE_SKUS = {"Services", "services", ""}


def _days_since(date_str: str | None) -> int | None:
    """Days since the given date string. Returns None if unparseable."""
    if not date_str:
        return None
    s = str(date_str).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(s[:10], fmt).replace(tzinfo=timezone.utc)
            delta = (datetime.now(timezone.utc) - d).days
            return max(delta, 0)
        except ValueError:
            continue
    return None


def _recency_decay(days_old: int | None, half_life_days: int = 90) -> float:
    """Exponential decay: 1.0 today, 0.5 at half_life, ~0.06 at 4 half-lives.

    Mike said "no" to pure $-ranking on 2026-05-11 — blend in recency
    so a $20K buy from 18 months ago doesn't outrank a $5K buy from
    last week. Half-life of 90 days matches the cross-sell outreach
    cadence (buyers who bought recently are still active prospects).
    """
    if days_old is None or days_old < 0:
        return 0.0
    return math.exp(-math.log(2) * days_old / half_life_days)


def _filter_clause() -> str:
    """SQL WHERE fragment that strips noise rows. Used by all queries."""
    return (
        "COALESCE(l.is_test, 0) = 0 "
        "AND COALESCE(m.is_test, 0) = 0 "
        "AND l.reytech_sells = 1 "
        "AND m.buyer_email IS NOT NULL AND m.buyer_email != '' "
        "AND LOWER(COALESCE(m.supplier, '')) NOT LIKE '%reytech%' "
        "AND l.line_total > 0 AND l.line_total < 100000 "
        f"AND COALESCE(l.reytech_sku, '') NOT IN ({','.join('?' * len(_NOISE_SKUS))})"
    )


def _filter_params() -> list[str]:
    return list(_NOISE_SKUS)


def get_prospects(top_n: int = 20, days_back: int = 365) -> list[dict]:
    """Return top cross-sell prospects ranked by spend × recency.

    Each row:
      {
        buyer_email, buyer_name, dept_name,
        competitor_spend, line_count, competitor_count,
        competitors (list of supplier names),
        skus (list of reytech_sku values),
        last_po_date, days_since_last_po,
        score (the blended ranking value),
      }
    """
    from src.core.db import get_db

    cutoff_days = days_back
    sql = f"""
        SELECT
            m.buyer_email,
            MAX(COALESCE(NULLIF(m.buyer_name, ''), m.buyer_email)) AS buyer_name,
            MAX(COALESCE(m.dept_name, '')) AS dept_name,
            ROUND(SUM(l.line_total), 2) AS competitor_spend,
            COUNT(DISTINCT l.id) AS line_count,
            COUNT(DISTINCT m.supplier) AS competitor_count,
            GROUP_CONCAT(DISTINCT m.supplier) AS competitors,
            GROUP_CONCAT(DISTINCT l.reytech_sku) AS skus,
            GROUP_CONCAT(DISTINCT l.category) AS categories,
            MAX(m.start_date) AS last_po_date
        FROM scprs_po_lines l
        JOIN scprs_po_master m ON l.po_id = m.id
        WHERE {_filter_clause()}
        GROUP BY m.buyer_email
        ORDER BY competitor_spend DESC
    """

    with get_db() as conn:
        rows = conn.execute(sql, _filter_params()).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        days_since = _days_since(d.get("last_po_date"))
        if days_since is not None and days_since > cutoff_days:
            continue
        d["days_since_last_po"] = days_since
        d["score"] = round(
            (d["competitor_spend"] or 0) * _recency_decay(days_since), 2
        )
        d["competitors"] = (d.get("competitors") or "").split(",") if d.get("competitors") else []
        d["skus"] = (d.get("skus") or "").split(",") if d.get("skus") else []
        d["categories"] = (d.get("categories") or "").split(",") if d.get("categories") else []
        out.append(d)

    out.sort(key=lambda x: x["score"], reverse=True)
    return out[:top_n]


def get_top_items_by_spend(top_n: int = 10, days_back: int = 365) -> list[dict]:
    """Top Reytech-sellable items by competitor spend.

    Surfaces "which items have the largest cross-sell opportunity" —
    different perspective than the per-buyer view. Useful for category
    strategy (e.g., "double down on nitrile gloves — $480K market").
    """
    from src.core.db import get_db

    sql = f"""
        SELECT
            l.category,
            l.reytech_sku,
            ROUND(SUM(l.line_total), 2) AS competitor_spend,
            COUNT(DISTINCT l.id) AS line_count,
            COUNT(DISTINCT m.buyer_email) AS distinct_buyers,
            MAX(m.start_date) AS last_seen_date
        FROM scprs_po_lines l
        JOIN scprs_po_master m ON l.po_id = m.id
        WHERE {_filter_clause()}
        GROUP BY l.reytech_sku, l.category
        ORDER BY competitor_spend DESC
    """
    with get_db() as conn:
        rows = conn.execute(sql, _filter_params()).fetchall()

    cutoff_days = days_back
    out = []
    for r in rows:
        d = dict(r)
        days_since = _days_since(d.get("last_seen_date"))
        if days_since is not None and days_since > cutoff_days:
            continue
        d["days_since_last_seen"] = days_since
        out.append(d)
    return out[:top_n]


def get_general_recommendations(days_back: int = 90) -> dict:
    """Distill the cross-sell data into 3-5 actionable bullets.

    Mike's 2026-05-11 ask: "the app is smart enough to give insights
    into who I should be selling to, but I am not getting any value
    from it." This function turns the raw aggregate into recommended
    next actions.
    """
    prospects = get_prospects(top_n=20, days_back=days_back)
    items = get_top_items_by_spend(top_n=10, days_back=days_back)

    bullets = []

    # Recommendation 1: top-recency-weighted prospect
    if prospects:
        p = prospects[0]
        comp_list = ", ".join(c for c in p["competitors"][:2] if c)
        sku_list = ", ".join(s for s in p["skus"][:3] if s)
        bullets.append({
            "kind": "top_prospect",
            "headline": (
                f"#1 prospect: {p['buyer_name']} — ${p['competitor_spend']:,.0f} of "
                f"{sku_list} bought from {comp_list} ({p['days_since_last_po'] or '?'}d ago)"
            ),
            "action": "Send outreach this week — they're an active buyer of items you carry.",
            "buyer_email": p["buyer_email"],
            "score": p["score"],
        })

    # Recommendation 2: largest category opportunity
    if items:
        top_item = items[0]
        bullets.append({
            "kind": "top_category",
            "headline": (
                f"#1 category opportunity: {top_item['category']} ({top_item['reytech_sku']}) — "
                f"${top_item['competitor_spend']:,.0f} across {top_item['distinct_buyers']} buyer(s)"
            ),
            "action": (
                f"Double down on {top_item['category']} outreach — "
                f"{top_item['line_count']} POs you could have won in last {days_back}d."
            ),
            "category": top_item["category"],
            "sku": top_item["reytech_sku"],
        })

    # Recommendation 3: agency concentration
    by_agency: dict[str, dict[str, Any]] = {}
    for p in prospects:
        agency = (p["dept_name"] or "").strip() or "(unknown)"
        bucket = by_agency.setdefault(agency, {"spend": 0.0, "buyers": set()})
        bucket["spend"] += p["competitor_spend"] or 0
        bucket["buyers"].add(p["buyer_email"])
    if by_agency:
        top_agency = max(by_agency.items(), key=lambda kv: kv[1]["spend"])
        agency_name, agency_data = top_agency
        bullets.append({
            "kind": "top_agency",
            "headline": (
                f"#1 agency: {agency_name} — ${agency_data['spend']:,.0f} across "
                f"{len(agency_data['buyers'])} buyer(s) in last {days_back}d"
            ),
            "action": (
                f"{agency_name} is your richest cross-sell agency. Map their staff "
                "structure (SB advocate, procurement lead) for systematic outreach."
            ),
            "agency": agency_name,
        })

    # Recommendation 4: stale prospects (haven't bought recently)
    stale = [p for p in prospects if (p["days_since_last_po"] or 0) > 60]
    if stale:
        stale_top = stale[0]
        bullets.append({
            "kind": "stale_prospect",
            "headline": (
                f"Re-engage {stale_top['buyer_name']} — bought ${stale_top['competitor_spend']:,.0f} "
                f"of your category items but no order in {stale_top['days_since_last_po']}d"
            ),
            "action": (
                "Reach out — their buying cycle is overdue. Ask about upcoming needs."
            ),
            "buyer_email": stale_top["buyer_email"],
        })

    # Recommendation 5: focus signal (categories with broad buyer reach)
    broad = sorted(items, key=lambda x: x.get("distinct_buyers") or 0, reverse=True)
    if broad and (broad[0].get("distinct_buyers") or 0) >= 5:
        b = broad[0]
        bullets.append({
            "kind": "broad_category",
            "headline": (
                f"{b['category']} reaches {b['distinct_buyers']} distinct buyers — "
                f"broad cross-sell base"
            ),
            "action": (
                f"This is your scale category. A category-wide email push to all "
                f"{b['distinct_buyers']} buyers could yield 2-3x the win rate of "
                "one-off outreach."
            ),
            "category": b["category"],
        })

    return {
        "ok": True,
        "bullets": bullets[:5],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": days_back,
        "prospect_count": len(prospects),
        "category_count": len(items),
    }
