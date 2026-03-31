"""
pricing_feedback.py — Competitive Pricing Intelligence Feedback Loop

Consumes loss data from award_tracker and produces:
1. Per-product competitive floor prices (FLAG ONLY — no auto-adjustment)
2. Per-agency pricing recommendations
3. Margin pattern analysis (are we consistently too high on certain categories?)
4. Forward-looking pricing suggestions for new quotes

IMPORTANT: This module surfaces insights for human review. It does NOT
auto-adjust margins, prices, or recommendations. Mike decides.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("pricing_feedback")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

DB_PATH = os.path.join(DATA_DIR, "reytech.db")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# POST-LOSS INTELLIGENCE UPDATE
# ══════════════════════════════════════════════════════════════════════════════

def update_competitive_intelligence(analysis: dict, quote: dict, po: dict):
    """Called by award_tracker after each loss detection.

    Records patterns and flags insights. Does NOT auto-adjust pricing.

    Args:
        analysis: Dict from award_tracker._analyze_loss() with keys:
            line_comparison, loss_reason_class, margin_too_high_items, pct_diff, etc.
        quote: The quote dict (quote_number, agency, institution, total, etc.)
        po: The winning PO dict (po_number, supplier, grand_total, etc.)
    """
    loss_class = analysis.get("loss_reason_class", "price_higher")
    mth_items = analysis.get("margin_too_high_items", [])
    quote_num = quote.get("quote_number", "?")
    agency = quote.get("agency", "")
    winner = po.get("supplier_name", po.get("supplier", "Unknown"))

    log.info("PRICING_FEEDBACK: Processing loss %s — class=%s, margin_too_high=%d items",
             quote_num, loss_class, len(mth_items))

    conn = _db()
    now = datetime.now().isoformat()

    try:
        # ── Detect and record patterns ───────────────────────────────────
        patterns_found = []

        # Pattern 1: Margin too high on specific items
        if mth_items:
            for item in mth_items:
                pattern_desc = (
                    f"Margin too high on '{item['description'][:50]}': "
                    f"cost ${item['our_cost']:.2f}, bid ${item['our_sell']:.2f} "
                    f"({item['actual_margin_pct']:.0f}% margin), "
                    f"competitor sold at ${item['their_sell']:.2f}. "
                    f"Could have bid ${item['could_have_bid']:.2f} "
                    f"({item['possible_margin_pct']:.0f}% margin) and won."
                )
                _record_pattern(
                    conn, now,
                    pattern_type="margin_too_high",
                    category=_categorize_item(item["description"]),
                    agency=agency,
                    competitor=winner,
                    description=pattern_desc,
                    severity="warning",
                    recommendation=(
                        f"Consider bidding at ${item['could_have_bid']:.2f} "
                        f"({item['possible_margin_pct']:.0f}% margin) for similar items. "
                        f"Current {item['actual_margin_pct']:.0f}% margin is not competitive."
                    ),
                    data=item,
                )
                patterns_found.append("margin_too_high")

        # Pattern 2: Cost basis too high
        if loss_class == "cost_too_high":
            _record_pattern(
                conn, now,
                pattern_type="cost_basis",
                agency=agency,
                competitor=winner,
                description=(
                    f"Cost basis too high on {quote_num} — our COGS exceeded "
                    f"competitor's sell prices on >50% of items. "
                    f"Need better supplier pricing."
                ),
                severity="info",
                recommendation="Review supplier agreements for these items. "
                               "Consider alternative suppliers or volume pricing.",
                data={"quote_number": quote_num, "analysis_summary": analysis.get("summary", "")},
            )
            patterns_found.append("cost_basis")

        # Pattern 3: Relationship/incumbent loss
        if loss_class == "relationship_incumbent":
            _record_pattern(
                conn, now,
                pattern_type="relationship_loss",
                agency=agency,
                competitor=winner,
                description=(
                    f"Lost {quote_num} despite lower pricing — "
                    f"we were {abs(analysis.get('pct_diff', 0)):.1f}% cheaper. "
                    f"Competitor {winner} may have incumbent advantage."
                ),
                severity="info",
                recommendation=(
                    f"Build relationship with this buyer at {agency}. "
                    f"Consider requesting debrief or following up on next solicitation."
                ),
                data={"quote_number": quote_num, "winner": winner, "pct_diff": analysis.get("pct_diff", 0)},
            )
            patterns_found.append("relationship_loss")

        # ── Check for repeat patterns (same competitor/category losing) ──
        _check_repeat_patterns(conn, now, agency, winner, analysis)

        conn.commit()
        log.info("PRICING_FEEDBACK: Recorded %d patterns for %s", len(patterns_found), quote_num)

    except Exception as e:
        log.error("PRICING_FEEDBACK: Error processing loss %s: %s", quote_num, e, exc_info=True)
    finally:
        conn.close()


def _record_pattern(conn, now: str, pattern_type: str, category: str = "",
                    agency: str = "", competitor: str = "", description: str = "",
                    severity: str = "info", recommendation: str = "", data: dict = None):
    """Insert a loss pattern record."""
    try:
        conn.execute("""
            INSERT INTO loss_patterns
            (detected_at, pattern_type, category, agency, competitor,
             description, severity, recommendation, data_json)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (now, pattern_type, category, agency, competitor,
              description, severity, recommendation,
              json.dumps(data or {}, default=str)))
    except Exception as e:
        log.debug("Record pattern: %s", e)


def _check_repeat_patterns(conn, now: str, agency: str, competitor: str,
                           analysis: dict):
    """Check if we're seeing repeat losses to same competitor or at same agency."""
    try:
        # Count losses to this competitor in last 90 days
        cutoff = (datetime.now() - timedelta(days=90)).isoformat()
        comp_losses = conn.execute("""
            SELECT COUNT(*) as cnt, AVG(price_delta_pct) as avg_delta
            FROM competitor_intel
            WHERE competitor_name = ? AND found_at >= ? AND outcome = 'lost'
        """, (competitor, cutoff)).fetchone()

        if comp_losses and (comp_losses["cnt"] or 0) >= 3:
            avg_delta = comp_losses["avg_delta"] or 0
            _record_pattern(
                conn, now,
                pattern_type="competitor_dominance",
                competitor=competitor,
                agency=agency,
                description=(
                    f"Lost to {competitor} {comp_losses['cnt']} times in 90 days "
                    f"(avg delta: {avg_delta:+.1f}%). They may be a key competitor."
                ),
                severity="warning",
                recommendation=(
                    f"Study {competitor}'s pricing strategy. "
                    f"Average price difference: {avg_delta:+.1f}%. "
                    f"Consider targeted competitive pricing for items they bid on."
                ),
                data={"competitor": competitor, "losses_90d": comp_losses["cnt"],
                      "avg_delta_pct": round(avg_delta, 1)},
            )

        # Count losses at this agency in last 90 days
        if agency:
            agency_losses = conn.execute("""
                SELECT COUNT(*) as cnt, AVG(price_delta_pct) as avg_delta
                FROM competitor_intel
                WHERE agency = ? AND found_at >= ? AND outcome = 'lost'
            """, (agency, cutoff)).fetchone()

            if agency_losses and (agency_losses["cnt"] or 0) >= 5:
                _record_pattern(
                    conn, now,
                    pattern_type="category_losses",
                    agency=agency,
                    description=(
                        f"Lost {agency_losses['cnt']} bids at {agency} in 90 days. "
                        f"Win rate may be declining."
                    ),
                    severity="warning",
                    recommendation=(
                        f"Review pricing strategy for {agency}. "
                        f"Consider more aggressive pricing or relationship building."
                    ),
                    data={"agency": agency, "losses_90d": agency_losses["cnt"]},
                )

    except Exception as e:
        log.debug("Repeat pattern check: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# PRICING RECOMMENDATIONS (FLAG-ONLY)
# ══════════════════════════════════════════════════════════════════════════════

def get_pricing_recommendation(description: str, agency: str = "",
                               cost: float = 0, quantity: int = 1) -> dict:
    """For a new quote item, return pricing intelligence based on loss history.

    Returns informational recommendations only — does NOT auto-adjust anything.

    Args:
        description: Item description.
        agency: Target agency (e.g., 'CCHCS').
        cost: Our supplier cost per unit.
        quantity: Quantity being quoted.

    Returns:
        dict with:
            competitor_floor: lowest known competitor price
            suggested_range: (min, max) competitive price range
            loss_history: recent losses on similar items
            margin_warning: str if historical data suggests margin risk
            confidence: 0-1 how much data backs this recommendation
    """
    result = {
        "description": description[:80],
        "competitor_floor": None,
        "suggested_range": None,
        "loss_history": [],
        "margin_warning": None,
        "confidence": 0,
        "sources_used": 0,
    }

    if not description or len(description) < 5:
        return result

    conn = _db()
    try:
        # Extract search words
        words = [w for w in description.lower().split() if len(w) > 3][:4]
        if not words:
            return result

        # Search competitor_intel for similar items
        clauses = " AND ".join(["LOWER(item_summary) LIKE ?" for _ in words])
        like_params = [f"%{w}%" for w in words]

        losses = conn.execute(f"""
            SELECT competitor_name, competitor_price, our_price,
                   price_delta_pct, found_at, agency, loss_reason_class,
                   our_cost, our_margin_pct, margin_too_high
            FROM competitor_intel
            WHERE {clauses} AND outcome='lost'
            ORDER BY found_at DESC LIMIT 10
        """, like_params).fetchall()

        if not losses:
            return result

        losses = [dict(r) for r in losses]
        result["loss_history"] = losses
        result["sources_used"] = len(losses)

        # Calculate competitor floor
        competitor_prices = [r["competitor_price"] for r in losses if r.get("competitor_price", 0) > 0]
        if competitor_prices:
            result["competitor_floor"] = min(competitor_prices)
            avg_comp = sum(competitor_prices) / len(competitor_prices)
            result["suggested_range"] = (
                round(min(competitor_prices) * 0.98, 2),  # 2% under lowest
                round(avg_comp, 2),  # up to average
            )
            result["confidence"] = min(len(competitor_prices) / 5, 1.0)  # Max confidence at 5+ data points

        # Check for margin warnings
        margin_too_high_count = sum(1 for r in losses if r.get("margin_too_high"))
        if margin_too_high_count >= 2:
            avg_margin = sum(r.get("our_margin_pct", 0) for r in losses if r.get("our_margin_pct")) / max(
                sum(1 for r in losses if r.get("our_margin_pct")), 1)
            result["margin_warning"] = (
                f"Lost {margin_too_high_count} times on similar items with margin too high. "
                f"Avg margin at loss: {avg_margin:.0f}%. Consider lower markup."
            )

        # Agency-specific insight
        if agency:
            agency_losses = [r for r in losses if r.get("agency", "").upper() == agency.upper()]
            if agency_losses:
                avg_delta = sum(r.get("price_delta_pct", 0) for r in agency_losses) / len(agency_losses)
                if avg_delta > 5:
                    result["margin_warning"] = (
                        (result.get("margin_warning") or "") +
                        f" At {agency}, avg price gap is {avg_delta:+.1f}%."
                    ).strip()

    except Exception as e:
        log.error("PRICING_FEEDBACK: Recommendation error: %s", e)
    finally:
        conn.close()

    return result


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN DETECTION & TRENDS
# ══════════════════════════════════════════════════════════════════════════════

def detect_margin_patterns(days: int = 90) -> list:
    """Aggregate loss data for trends and patterns.

    Returns list of PatternInsight dicts with severity + recommendation.
    Human decides what action to take.
    """
    patterns = []
    conn = _db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    try:
        # ── Categories where we consistently lose ────────────────────────
        # Use item_summary words as proxy for categories
        all_losses = conn.execute("""
            SELECT item_summary, loss_reason_class, price_delta_pct,
                   margin_too_high, competitor_name, agency
            FROM competitor_intel
            WHERE outcome='lost' AND found_at >= ?
            ORDER BY found_at DESC
        """, (cutoff,)).fetchall()

        if not all_losses:
            conn.close()
            return patterns

        all_losses = [dict(r) for r in all_losses]

        # ── Margin analysis by loss class ────────────────────────────────
        class_counts = {}
        for r in all_losses:
            cls = r.get("loss_reason_class", "unknown") or "unknown"
            class_counts[cls] = class_counts.get(cls, 0) + 1

        total = len(all_losses)
        for cls, count in class_counts.items():
            pct = count / total * 100
            if pct >= 30 and count >= 3:
                severity = "warning" if pct >= 50 else "info"
                patterns.append({
                    "pattern_type": "loss_class_trend",
                    "description": f"{pct:.0f}% of losses ({count}/{total}) are '{cls.replace('_',' ')}' in last {days} days",
                    "severity": severity,
                    "recommendation": _recommendation_for_class(cls),
                    "data": {"class": cls, "count": count, "total": total, "pct": round(pct, 1)},
                })

        # ── Top competitors by losses ────────────────────────────────────
        comp_counts = {}
        for r in all_losses:
            name = r.get("competitor_name", "Unknown")
            if name not in comp_counts:
                comp_counts[name] = {"count": 0, "deltas": []}
            comp_counts[name]["count"] += 1
            if r.get("price_delta_pct"):
                comp_counts[name]["deltas"].append(r["price_delta_pct"])

        for name, info in sorted(comp_counts.items(), key=lambda x: -x[1]["count"]):
            if info["count"] >= 3:
                avg_delta = sum(info["deltas"]) / len(info["deltas"]) if info["deltas"] else 0
                patterns.append({
                    "pattern_type": "competitor_trend",
                    "description": f"Lost to {name} {info['count']} times in {days} days (avg delta: {avg_delta:+.1f}%)",
                    "severity": "warning" if info["count"] >= 5 else "info",
                    "competitor": name,
                    "recommendation": f"Study {name}'s pricing. They beat us by avg {abs(avg_delta):.1f}%. "
                                      f"{'They consistently undercut — may need to match.' if avg_delta > 0 else 'We are cheaper but losing — relationship play.'}",
                    "data": {"competitor": name, "losses": info["count"], "avg_delta": round(avg_delta, 1)},
                })

        # ── Margin too high trend ────────────────────────────────────────
        mth_count = sum(1 for r in all_losses if r.get("margin_too_high"))
        if mth_count >= 3:
            patterns.append({
                "pattern_type": "margin_trend",
                "description": f"{mth_count} losses with margin too high in {days} days — "
                               f"cost advantage wasted on {mth_count}/{total} bids",
                "severity": "critical" if mth_count >= total * 0.3 else "warning",
                "recommendation": "Review default markup strategy. Multiple bids lost where "
                                  "we had lower costs but bid too high. Consider reducing "
                                  "margins on competitive items.",
                "data": {"margin_too_high_count": mth_count, "total_losses": total},
            })

    except Exception as e:
        log.error("PRICING_FEEDBACK: Pattern detection error: %s", e)
    finally:
        conn.close()

    log.info("PRICING_FEEDBACK: Detected %d patterns over %d days", len(patterns), days)
    return patterns


def get_category_loss_trends(days: int = 90) -> dict:
    """Aggregate losses by product category over the last N days.

    Returns { category: { losses, avg_delta_pct, top_competitor, recommendation } }
    """
    conn = _db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    result = {}

    try:
        losses = conn.execute("""
            SELECT category, competitor_name, price_delta_pct, loss_reason_class
            FROM competitor_intel
            WHERE outcome='lost' AND found_at >= ? AND category != ''
        """, (cutoff,)).fetchall()

        cats = {}
        for r in losses:
            r = dict(r)
            cat = r.get("category", "Uncategorized") or "Uncategorized"
            if cat not in cats:
                cats[cat] = {"losses": 0, "deltas": [], "competitors": {}, "mth_count": 0}
            cats[cat]["losses"] += 1
            if r.get("price_delta_pct"):
                cats[cat]["deltas"].append(r["price_delta_pct"])
            comp = r.get("competitor_name", "Unknown")
            cats[cat]["competitors"][comp] = cats[cat]["competitors"].get(comp, 0) + 1
            if r.get("loss_reason_class") == "margin_too_high":
                cats[cat]["mth_count"] += 1

        for cat, info in cats.items():
            top_comp = max(info["competitors"].items(), key=lambda x: x[1])[0] if info["competitors"] else "Unknown"
            avg_delta = sum(info["deltas"]) / len(info["deltas"]) if info["deltas"] else 0
            result[cat] = {
                "losses": info["losses"],
                "avg_delta_pct": round(avg_delta, 1),
                "top_competitor": top_comp,
                "margin_too_high_count": info["mth_count"],
                "recommendation": (
                    f"Lost {info['losses']} bids in {cat}. "
                    f"Top competitor: {top_comp}. "
                    f"Avg price gap: {avg_delta:+.1f}%."
                    + (f" {info['mth_count']} bids had margin too high!" if info["mth_count"] else "")
                ),
            }

    except Exception as e:
        log.error("PRICING_FEEDBACK: Category trends error: %s", e)
    finally:
        conn.close()

    return result


def get_competitor_price_trends(competitor_name: str = None, days: int = 180) -> dict:
    """Track how a competitor's pricing has changed over time.

    Args:
        competitor_name: Specific competitor, or None for all.
        days: How far back to look.

    Returns:
        dict with pricing trends per competitor.
    """
    conn = _db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    result = {}

    try:
        query = """
            SELECT competitor_name, competitor_price, our_price,
                   price_delta_pct, found_at, agency, loss_reason_class
            FROM competitor_intel
            WHERE outcome='lost' AND found_at >= ?
        """
        params = [cutoff]
        if competitor_name:
            query += " AND competitor_name = ?"
            params.append(competitor_name)
        query += " ORDER BY found_at ASC"

        rows = conn.execute(query, params).fetchall()

        comp_data = {}
        for r in rows:
            r = dict(r)
            name = r["competitor_name"]
            if name not in comp_data:
                comp_data[name] = {"prices": [], "deltas": [], "dates": [], "agencies": set()}
            comp_data[name]["prices"].append(r.get("competitor_price", 0))
            comp_data[name]["deltas"].append(r.get("price_delta_pct", 0))
            comp_data[name]["dates"].append(r.get("found_at", "")[:10])
            comp_data[name]["agencies"].add(r.get("agency", ""))

        for name, data in comp_data.items():
            prices = [p for p in data["prices"] if p > 0]
            deltas = data["deltas"]
            # Check if they're getting more or less aggressive
            trend = "stable"
            if len(deltas) >= 3:
                first_half = deltas[:len(deltas)//2]
                second_half = deltas[len(deltas)//2:]
                avg_first = sum(first_half) / len(first_half)
                avg_second = sum(second_half) / len(second_half)
                if avg_second - avg_first > 5:
                    trend = "getting_more_aggressive"
                elif avg_first - avg_second > 5:
                    trend = "getting_less_aggressive"

            result[name] = {
                "total_encounters": len(data["prices"]),
                "avg_price": round(sum(prices) / len(prices), 2) if prices else 0,
                "avg_delta_pct": round(sum(deltas) / len(deltas), 1) if deltas else 0,
                "trend": trend,
                "agencies": list(data["agencies"]),
                "first_seen": data["dates"][0] if data["dates"] else "",
                "last_seen": data["dates"][-1] if data["dates"] else "",
            }

    except Exception as e:
        log.error("PRICING_FEEDBACK: Competitor trends error: %s", e)
    finally:
        conn.close()

    return result


# ══════════════════════════════════════════════════════════════════════════════
# UNACKNOWLEDGED PATTERNS
# ══════════════════════════════════════════════════════════════════════════════

def get_unacknowledged_patterns(limit: int = 20) -> list:
    """Get loss patterns that haven't been reviewed yet."""
    conn = _db()
    try:
        rows = conn.execute("""
            SELECT * FROM loss_patterns
            WHERE acknowledged = 0
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning' THEN 2
                    ELSE 3
                END,
                detected_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.debug("Unacknowledged patterns: %s", e)
        return []
    finally:
        conn.close()


def acknowledge_pattern(pattern_id: int) -> bool:
    """Mark a loss pattern as reviewed/acknowledged."""
    conn = _db()
    try:
        conn.execute("""
            UPDATE loss_patterns SET acknowledged = 1, acknowledged_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), pattern_id))
        conn.commit()
        return True
    except Exception as e:
        log.debug("Acknowledge pattern: %s", e)
        return False
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# WIN RATE TRACKING & TRENDS
# ══════════════════════════════════════════════════════════════════════════════

def get_win_rate_trends(days: int = 180) -> dict:
    """Calculate win/loss rate trends over sliding windows.

    Returns:
        {
            overall: {won, lost, pending, win_rate, total_won_value},
            by_period: [{period, won, lost, win_rate}],  # 30-day buckets
            by_agency: [{agency, won, lost, win_rate, trend}],
            by_competitor: [{competitor, losses, avg_delta, trend}],
            alerts: [{type, message, severity}],  # declining win rate warnings
        }
    """
    conn = _db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    result = {
        "overall": {},
        "by_period": [],
        "by_agency": [],
        "by_competitor": [],
        "alerts": [],
    }

    try:
        # ── Overall win/loss stats ───────────────────────────────────────
        overall = conn.execute("""
            SELECT
                SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status='won' THEN total ELSE 0 END) as won_value,
                SUM(CASE WHEN status='lost' THEN total ELSE 0 END) as lost_value
            FROM quotes
            WHERE is_test = 0 AND total > 0
              AND created_at >= ?
        """, (cutoff,)).fetchone()

        if overall:
            won = overall["won"] or 0
            lost = overall["lost"] or 0
            total_decided = won + lost
            result["overall"] = {
                "won": won,
                "lost": lost,
                "pending": overall["pending"] or 0,
                "win_rate": round(won / total_decided * 100, 1) if total_decided > 0 else 0,
                "won_value": round(overall["won_value"] or 0, 2),
                "lost_value": round(overall["lost_value"] or 0, 2),
                "total_decided": total_decided,
            }

        # ── Win rate by 30-day periods ───────────────────────────────────
        periods = []
        for i in range(min(days // 30, 6)):
            period_end = (datetime.now() - timedelta(days=i * 30)).isoformat()
            period_start = (datetime.now() - timedelta(days=(i + 1) * 30)).isoformat()
            period_label = f"{(i * 30)}–{(i + 1) * 30}d ago"

            row = conn.execute("""
                SELECT
                    SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won,
                    SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as lost
                FROM quotes
                WHERE is_test = 0 AND total > 0
                  AND created_at >= ? AND created_at < ?
            """, (period_start, period_end)).fetchone()

            if row:
                p_won = row["won"] or 0
                p_lost = row["lost"] or 0
                p_total = p_won + p_lost
                periods.append({
                    "period": period_label,
                    "won": p_won,
                    "lost": p_lost,
                    "win_rate": round(p_won / p_total * 100, 1) if p_total > 0 else 0,
                })

        result["by_period"] = periods

        # ── Win rate by agency ───────────────────────────────────────────
        agencies = conn.execute("""
            SELECT agency,
                SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN status='won' THEN total ELSE 0 END) as won_value
            FROM quotes
            WHERE is_test = 0 AND total > 0 AND agency != ''
              AND created_at >= ?
            GROUP BY agency
            HAVING (won + lost) >= 2
            ORDER BY (won + lost) DESC
        """, (cutoff,)).fetchall()

        for ag in agencies:
            ag = dict(ag)
            ag_won = ag["won"] or 0
            ag_lost = ag["lost"] or 0
            ag_total = ag_won + ag_lost
            win_rate = round(ag_won / ag_total * 100, 1) if ag_total > 0 else 0

            # Calculate trend: compare recent 60d vs prior 60d
            trend = "stable"
            if days >= 120:
                mid = (datetime.now() - timedelta(days=days // 2)).isoformat()
                recent = conn.execute("""
                    SELECT SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as w,
                           SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as l
                    FROM quotes WHERE is_test=0 AND agency=? AND created_at >= ?
                """, (ag["agency"], mid)).fetchone()
                older = conn.execute("""
                    SELECT SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as w,
                           SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as l
                    FROM quotes WHERE is_test=0 AND agency=? AND created_at >= ? AND created_at < ?
                """, (ag["agency"], cutoff, mid)).fetchone()

                if recent and older:
                    rw, rl = (recent["w"] or 0), (recent["l"] or 0)
                    ow, ol = (older["w"] or 0), (older["l"] or 0)
                    r_rate = rw / (rw + rl) * 100 if (rw + rl) > 0 else 0
                    o_rate = ow / (ow + ol) * 100 if (ow + ol) > 0 else 0
                    if r_rate - o_rate > 10:
                        trend = "improving"
                    elif o_rate - r_rate > 10:
                        trend = "declining"

            result["by_agency"].append({
                "agency": ag["agency"],
                "won": ag_won,
                "lost": ag_lost,
                "win_rate": win_rate,
                "won_value": round(ag.get("won_value", 0) or 0, 2),
                "trend": trend,
            })

        # ── Top competitors by loss frequency ────────────────────────────
        competitors = conn.execute("""
            SELECT competitor_name,
                COUNT(*) as losses,
                AVG(price_delta_pct) as avg_delta,
                SUM(CASE WHEN margin_too_high=1 THEN 1 ELSE 0 END) as mth_count
            FROM competitor_intel
            WHERE outcome='lost' AND found_at >= ?
            GROUP BY competitor_name
            HAVING losses >= 2
            ORDER BY losses DESC LIMIT 10
        """, (cutoff,)).fetchall()

        for comp in competitors:
            comp = dict(comp)
            result["by_competitor"].append({
                "competitor": comp["competitor_name"],
                "losses": comp["losses"],
                "avg_delta_pct": round(comp["avg_delta"] or 0, 1),
                "margin_too_high_count": comp["mth_count"] or 0,
            })

        # ── Generate alerts for declining win rates ──────────────────────
        if len(periods) >= 2:
            recent_rate = periods[0].get("win_rate", 0)
            prior_rate = periods[1].get("win_rate", 0)
            if prior_rate > 0 and recent_rate < prior_rate - 15:
                result["alerts"].append({
                    "type": "declining_win_rate",
                    "message": f"Win rate dropped from {prior_rate:.0f}% to {recent_rate:.0f}% "
                               f"in the last 30 days (vs prior 30 days)",
                    "severity": "warning" if recent_rate > 20 else "critical",
                })

        for ag_data in result["by_agency"]:
            if ag_data["trend"] == "declining" and ag_data["lost"] >= 3:
                result["alerts"].append({
                    "type": "agency_declining",
                    "message": f"Win rate declining at {ag_data['agency']} — "
                               f"currently {ag_data['win_rate']:.0f}% "
                               f"({ag_data['won']}W / {ag_data['lost']}L)",
                    "severity": "warning",
                })

    except Exception as e:
        log.error("PRICING_FEEDBACK: Win rate trends error: %s", e)
    finally:
        conn.close()

    return result


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _categorize_item(description: str) -> str:
    """Simple keyword-based categorization for loss patterns."""
    desc = description.lower()
    categories = {
        "Medical Supplies": ["nitrile", "glove", "syringe", "catheter", "bandage",
                             "gauze", "wound", "surgical", "gown", "mask", "restraint"],
        "Janitorial": ["trash", "mop", "disinfect", "cleaner", "soap", "sanitizer"],
        "Office Supplies": ["pen", "toner", "binder", "staple", "paper", "folder"],
        "IT & Electronics": ["battery", "cable", "keyboard", "printer", "adapter"],
        "Safety & PPE": ["safety glass", "hard hat", "vest", "boot"],
        "Food Service": ["cup", "plate", "napkin", "utensil"],
    }
    for cat, keywords in categories.items():
        if any(kw in desc for kw in keywords):
            return cat
    return "Other"


def _recommendation_for_class(loss_class: str) -> str:
    """Generate a recommendation based on loss classification."""
    recs = {
        "price_higher": "General pricing issue — we're bidding above competitors. "
                        "Review markup rates and supplier costs.",
        "cost_too_high": "COGS problem — our supplier costs exceed competitor sell prices. "
                         "Negotiate with suppliers or find alternative sources.",
        "margin_too_high": "Markup strategy issue — we have competitive costs but add "
                           "too much margin. Consider reducing markup on competitive items.",
        "relationship_incumbent": "Relationship/incumbent loss — pricing is competitive but "
                                  "buyers prefer existing vendors. Build relationships and "
                                  "leverage DVBE advantage.",
    }
    return recs.get(loss_class, "Review pricing strategy for this category of losses.")
