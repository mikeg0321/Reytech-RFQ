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
    Known Reytech customers are EXCLUDED (Phase 2c-1) — this surface is
    distribution-list candidates, not retention.

  get_top_items_by_spend(top_n, days_back)
    Per-SKU rollup — which Reytech items have the most competitor
    spend (sized opportunity per item).

  get_general_recommendations(...)
    3-5 actionable bullets distilled from the data. The "intel that
    tells the operator what to DO" — not just a number dump. Action
    verb is "get on their distribution list" / "register on procurement
    portal", not "send outreach email" (Mike feedback 2026-05-11 on
    first digest: cold outreach on 4-month-old data isn't actionable —
    the substrate work is procurement-portal vendor registration).

Noise filters (learned from initial query 2026-05-11):
  - buyer_email IS NOT NULL  (skip POs with no buyer info — data quality)
  - supplier not Reytech itself  (obviously)
  - line_total in (0, 100_000]  (cap noise from service contracts)
  - reytech_sku NOT IN ('Services', '')  (catalog matched a service bucket)
  - buyer_email NOT IN known_reytech_customer_emails  (Phase 2c-1: hide
      buyers we already serve from the prospecting surface)
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


_NOISE_SKUS = {"Services", "services", ""}


def _get_known_customer_emails() -> set[str]:
    """Return the set of buyer emails we already serve as a Reytech customer.

    Sourced from THREE signals (any one is sufficient):
      1. scprs_buyers.buys_from_reytech = 1 OR reytech_spend > 0
         — enrichment flagged this buyer as a known Reytech customer.
      2. quotes.contact_email present
         — we've quoted this buyer at least once.
      3. contacts.is_reytech_customer = 1
         — explicit operator tag in CRM.

    Returned as a lowercased set for case-insensitive comparison. Empty
    on any error (the filter degrades gracefully — better to show a
    known customer in the prospect list than to crash the digest).
    """
    emails: set[str] = set()
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for sql in (
                "SELECT LOWER(buyer_email) FROM scprs_buyers "
                "WHERE buyer_email IS NOT NULL AND buyer_email != '' "
                "  AND (COALESCE(buys_from_reytech, 0) = 1 "
                "       OR COALESCE(reytech_spend, 0) > 0)",
                "SELECT DISTINCT LOWER(contact_email) FROM quotes "
                "WHERE contact_email IS NOT NULL AND contact_email != ''",
                "SELECT LOWER(buyer_email) FROM contacts "
                "WHERE buyer_email IS NOT NULL AND buyer_email != '' "
                "  AND COALESCE(is_reytech_customer, 0) = 1",
            ):
                try:
                    rows = conn.execute(sql).fetchall()
                except Exception as _e:
                    log.debug("known-customer source skipped: %s", _e)
                    continue
                for r in rows:
                    e = (r[0] or "").strip()
                    if e:
                        emails.add(e)
    except Exception as e:
        log.warning("known-customer lookup failed (continuing unfiltered): %s", e)
    return emails


def _freshness_tier(days_old: int | None) -> str:
    """Bucket days-since-last-buy into operator-meaningful tiers.

    Tiers:
      - "fresh"     ≤30d   actively buying THIS month
      - "warm"      ≤90d   inside one quarter
      - "stale"     ≤180d  inside two quarters — distro list still valid
      - "old"       ≤365d  historical signal — registration only
      - "unknown"   no parseable date
    """
    if days_old is None:
        return "unknown"
    if days_old <= 30:
        return "fresh"
    if days_old <= 90:
        return "warm"
    if days_old <= 180:
        return "stale"
    return "old"


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

    known_customer_emails = _get_known_customer_emails()

    out = []
    for r in rows:
        d = dict(r)
        email_lc = (d.get("buyer_email") or "").strip().lower()
        # Phase 2c-1: exclude known Reytech customers from the prospect
        # surface. This is the "distribution list candidate" lens —
        # buyers we already serve aren't candidates.
        if email_lc and email_lc in known_customer_emails:
            continue
        days_since = _days_since(d.get("last_po_date"))
        if days_since is not None and days_since > cutoff_days:
            continue
        d["days_since_last_po"] = days_since
        d["freshness"] = _freshness_tier(days_since)
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

    # Recommendation 1: top-recency-weighted prospect — register on
    # their agency's procurement portal so future buys route to you.
    if prospects:
        p = prospects[0]
        comp_list = ", ".join(c for c in p["competitors"][:2] if c)
        sku_list = ", ".join(s for s in p["skus"][:3] if s)
        agency = (p.get("dept_name") or "").strip() or "their agency"
        bullets.append({
            "kind": "top_prospect",
            "headline": (
                f"#1 distro-list target: {p['buyer_name']} ({agency}) — "
                f"${p['competitor_spend']:,.0f} of {sku_list} bought from "
                f"{comp_list} ({p['days_since_last_po'] or '?'}d ago)"
            ),
            "action": (
                f"Get on {agency}'s vendor distribution list for "
                f"{sku_list}. They're buying your category from competitors — "
                f"goal is to receive their next solicitation, not a one-off "
                f"cold email."
            ),
            "buyer_email": p["buyer_email"],
            "agency": agency,
            "freshness": p.get("freshness", "unknown"),
            "score": p["score"],
        })

    # Recommendation 2: largest category opportunity — focus area to
    # register across multiple agency portals.
    if items:
        top_item = items[0]
        bullets.append({
            "kind": "top_category",
            "headline": (
                f"#1 category: {top_item['category']} ({top_item['reytech_sku']}) — "
                f"${top_item['competitor_spend']:,.0f} across "
                f"{top_item['distinct_buyers']} buyer(s)"
            ),
            "action": (
                f"Audit Reytech's vendor registration for {top_item['category']} "
                f"across every agency buying it. {top_item['line_count']} POs "
                f"in {days_back}d means a standing solicitation pipeline you're "
                f"not on."
            ),
            "category": top_item["category"],
            "sku": top_item["reytech_sku"],
        })

    # Recommendation 3: agency concentration — single agency to target
    # for vendor registration first.
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
                f"#1 agency to register at: {agency_name} — "
                f"${agency_data['spend']:,.0f} across "
                f"{len(agency_data['buyers'])} buyer(s) in last {days_back}d"
            ),
            "action": (
                f"Confirm Reytech is on {agency_name}'s vendor distribution list "
                f"for your categories. {len(agency_data['buyers'])} buyers in "
                f"this agency are placing orders with competitors today."
            ),
            "agency": agency_name,
        })

    # Recommendation 4: fresh signal — buyers placing orders THIS month.
    # These are the highest-priority registration targets because their
    # next solicitation is imminent.
    fresh = [p for p in prospects if (p.get("days_since_last_po") or 999) <= 30]
    if fresh:
        f = fresh[0]
        f_agency = (f.get("dept_name") or "").strip() or "their agency"
        bullets.append({
            "kind": "fresh_signal",
            "headline": (
                f"FRESH: {f['buyer_name']} ({f_agency}) bought "
                f"${f['competitor_spend']:,.0f} of your category "
                f"{f['days_since_last_po']}d ago"
            ),
            "action": (
                f"Top priority — their buying cycle is ACTIVE. Verify Reytech "
                f"is registered as a {f_agency} vendor before they place their "
                f"next order. Cold outreach won't beat the distribution list."
            ),
            "buyer_email": f["buyer_email"],
            "agency": f_agency,
        })

    # Recommendation 5: focus signal (categories with broad buyer reach
    # → scale leverage from a single registration push).
    broad = sorted(items, key=lambda x: x.get("distinct_buyers") or 0, reverse=True)
    if broad and (broad[0].get("distinct_buyers") or 0) >= 5:
        b = broad[0]
        bullets.append({
            "kind": "broad_category",
            "headline": (
                f"Scale category: {b['category']} reaches {b['distinct_buyers']} "
                f"distinct buyers"
            ),
            "action": (
                f"Vendor-registration scale play: one {b['category']} catalog "
                f"sheet on file with {b['distinct_buyers']} agencies' procurement "
                f"systems gets you in front of every future solicitation in this "
                f"category."
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
