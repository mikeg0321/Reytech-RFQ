"""
prospect_scorer.py — Score and rank sales prospects from SCPRS + CRM data.
Sprint 4.2 (F9): Combines purchase volume, recency, product match, and
existing relationship status into a single outreach priority score.
"""
import json
import logging
from datetime import datetime, timedelta

log = logging.getLogger("reytech.prospects")


def _get_db():
    from src.core.db import get_db
    return get_db()


def score_prospects(limit: int = 50) -> dict:
    """
    Score and rank prospects for outreach.

    Scoring dimensions (0-100):
    - volume_score (30%): Total spend from SCPRS data
    - recency_score (25%): How recently they purchased
    - match_score (25%): % of their purchases that overlap Reytech catalog
    - gap_score (20%): Opportunity size from items they buy we don't sell yet

    Deprioritize: existing customers, already-contacted prospects.
    """
    now = datetime.now()
    six_months_ago = (now - timedelta(days=180)).isoformat()

    prospects = []

    try:
        with _get_db() as conn:
            # Get SCPRS supplier intel — agencies buying from competitors
            agencies = conn.execute("""
                SELECT p.dept_code, p.dept_name, p.agency_code,
                       COUNT(DISTINCT p.po_number) as po_count,
                       SUM(p.grand_total) as total_spend,
                       MAX(p.start_date) as last_po_date,
                       COUNT(DISTINCT p.supplier) as supplier_count,
                       SUM(CASE WHEN l.reytech_sells = 1 THEN l.line_total ELSE 0 END) as match_spend,
                       SUM(CASE WHEN l.opportunity_flag = 'GAP_ITEM' THEN l.line_total ELSE 0 END) as gap_spend,
                       SUM(CASE WHEN l.opportunity_flag = 'WIN_BACK' THEN l.line_total ELSE 0 END) as winback_spend
                FROM scprs_po_master p
                LEFT JOIN scprs_po_lines l ON l.po_id = p.id
                WHERE p.start_date >= date('now', '-365 days')
                GROUP BY p.dept_code
                HAVING total_spend > 1000
                ORDER BY total_spend DESC
            """).fetchall()

            if not agencies:
                return {"ok": True, "prospects": [], "message": "No SCPRS data — run universal pull first"}

            # Get existing Reytech customers (to deprioritize)
            existing = set()
            try:
                rows = conn.execute("""
                    SELECT DISTINCT agency FROM quotes
                    WHERE status IN ('won', 'sent', 'pending') AND is_test = 0
                """).fetchall()
                existing = {r["agency"].upper() for r in rows if r.get("agency")}
            except Exception as _e:
                log.debug("suppressed: %s", _e)

            # Get already-contacted prospects
            contacted = set()
            try:
                rows = conn.execute("""
                    SELECT DISTINCT agency FROM quotes
                    WHERE source = 'outreach' AND created_at >= ?
                """, (six_months_ago,)).fetchall()
                contacted = {r["agency"].upper() for r in rows if r.get("agency")}
            except Exception as _e:
                log.debug("suppressed: %s", _e)

            # Get buyer contacts from SCPRS
            buyers = {}
            try:
                rows = conn.execute("""
                    SELECT dept_code, buyer_name, buyer_email, buyer_phone,
                           COUNT(*) as po_count
                    FROM scprs_po_master
                    WHERE buyer_email IS NOT NULL AND buyer_email != ''
                    GROUP BY dept_code, buyer_email
                    ORDER BY po_count DESC
                """).fetchall()
                for r in rows:
                    dc = r["dept_code"]
                    if dc not in buyers:
                        buyers[dc] = []
                    # sqlite3.Row supports [], not .get() — direct index
                    # with try/except handles the column-may-be-absent case
                    # that the original .get() incorrectly tried to.
                    try:
                        phone = r["buyer_phone"]
                    except (IndexError, KeyError):
                        phone = None
                    buyers[dc].append({
                        "name": r["buyer_name"],
                        "email": r["buyer_email"],
                        "phone": phone,
                        "po_count": r["po_count"],
                    })
            except Exception as _e:
                log.debug("suppressed: %s", _e)

            # Score each agency
            max_spend = max((dict(a).get("total_spend") or 0) for a in agencies) or 1

            for row in agencies:
                a = dict(row)
                dept_code = a["dept_code"] or ""
                dept_name = a["dept_name"] or dept_code
                total_spend = a.get("total_spend") or 0
                match_spend = a.get("match_spend") or 0
                gap_spend = a.get("gap_spend") or 0
                winback_spend = a.get("winback_spend") or 0
                last_po = a.get("last_po_date") or ""

                # Volume score (0-30): normalized by max spend
                volume_score = min(30, round(30 * (total_spend / max_spend), 1))

                # Recency score (0-25): more recent = higher
                recency_score = 0
                if last_po:
                    try:
                        days_ago = (now - datetime.fromisoformat(last_po[:10])).days
                        if days_ago <= 30:
                            recency_score = 25
                        elif days_ago <= 90:
                            recency_score = 20
                        elif days_ago <= 180:
                            recency_score = 12
                        elif days_ago <= 365:
                            recency_score = 5
                    except (ValueError, TypeError) as _e:
                        log.debug("suppressed: %s", _e)

                # Match score (0-25): what % of their spend is on items we sell
                match_ratio = match_spend / total_spend if total_spend > 0 else 0
                match_score = round(25 * match_ratio, 1)

                # Gap score (0-20): opportunity from items they buy we don't sell
                gap_ratio = gap_spend / max_spend if max_spend > 0 else 0
                gap_score = min(20, round(20 * gap_ratio * 2, 1))  # boost factor

                total_score = round(volume_score + recency_score + match_score + gap_score, 1)

                # Relationship status
                dept_upper = dept_name.upper()
                if any(dept_upper in e for e in existing):
                    relationship = "existing_customer"
                    total_score *= 0.5  # deprioritize
                elif any(dept_upper in c for c in contacted):
                    relationship = "recently_contacted"
                    total_score *= 0.7
                else:
                    relationship = "new_prospect"

                prospects.append({
                    "dept_code": dept_code,
                    "dept_name": dept_name,
                    "agency_code": a.get("agency_code"),
                    "score": round(total_score, 1),
                    "score_breakdown": {
                        "volume": volume_score,
                        "recency": recency_score,
                        "match": match_score,
                        "gap": gap_score,
                    },
                    "total_spend": round(total_spend, 2),
                    "match_spend": round(match_spend, 2),
                    "gap_spend": round(gap_spend, 2),
                    "winback_spend": round(winback_spend, 2),
                    "po_count": a.get("po_count", 0),
                    "last_po_date": last_po,
                    "relationship": relationship,
                    "contacts": buyers.get(dept_code, [])[:3],
                })

            # Sort by score descending
            prospects.sort(key=lambda p: p["score"], reverse=True)

    except Exception as e:
        log.error("Prospect scoring failed: %s", e)
        return {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "count": len(prospects),
        "prospects": prospects[:limit],
        "scored_at": now.isoformat(),
        "summary": {
            "new_prospects": sum(1 for p in prospects if p["relationship"] == "new_prospect"),
            "existing_customers": sum(1 for p in prospects if p["relationship"] == "existing_customer"),
            "recently_contacted": sum(1 for p in prospects if p["relationship"] == "recently_contacted"),
            "total_opportunity": round(sum(p["gap_spend"] + p["winback_spend"] for p in prospects), 2),
        }
    }
