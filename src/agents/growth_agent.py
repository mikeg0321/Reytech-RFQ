"""
growth_agent.py — Growth Strategy Agent for Reytech
Phase 14 | Version: 1.0.0

Analyzes the entity graph, won_quotes_db, and lead history to answer:
  - Why did we win? Why did we lose?
  - Which institutions buy the most from us?
  - Which product categories have the highest margins?
  - Where should we focus outreach next?
  - What pricing strategy maximizes win rate × margin?

Data sources:
  - quotes_log.json (all quotes: won/lost/pending)
  - price_checks.json (PC pipeline data)
  - leads.json (lead gen data)
  - lead_history.json (lead conversion funnel)
  - won_quotes_db (historical SCPRS wins)

Output: Actionable insights as JSON — no dashboards, just data for decisions.
"""

import json
import os
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

log = logging.getLogger("growth")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


# ─── Data Loaders ────────────────────────────────────────────────────────────

def _load_json(filename: str) -> list | dict:
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return [] if filename != "price_checks.json" else {}


def _load_quotes() -> list:
    data = _load_json("quotes_log.json")
    return data if isinstance(data, list) else []


def _load_pcs() -> dict:
    data = _load_json("price_checks.json")
    return data if isinstance(data, dict) else {}


def _load_leads() -> list:
    data = _load_json("leads.json")
    return data if isinstance(data, list) else []


# ─── Win/Loss Analysis ──────────────────────────────────────────────────────

def win_loss_analysis() -> dict:
    """
    Comprehensive win/loss breakdown.
    Returns which agencies, institutions, categories we win/lose at.
    """
    quotes = _load_quotes()
    if not quotes:
        return {"error": "No quotes data", "total": 0}

    total = len(quotes)
    won = [q for q in quotes if q.get("status") == "won"]
    lost = [q for q in quotes if q.get("status") == "lost"]
    pending = [q for q in quotes if q.get("status", "pending") == "pending"]

    # By agency
    agency_stats = defaultdict(lambda: {"won": 0, "lost": 0, "pending": 0, "total_value": 0})
    for q in quotes:
        ag = q.get("agency", "DEFAULT")
        status = q.get("status", "pending")
        agency_stats[ag][status] = agency_stats[ag].get(status, 0) + 1
        if status == "won":
            agency_stats[ag]["total_value"] += q.get("total", 0)

    # By institution
    inst_stats = defaultdict(lambda: {"won": 0, "lost": 0, "total_value": 0, "quotes": 0})
    for q in quotes:
        inst = q.get("institution", "Unknown")
        inst_stats[inst]["quotes"] += 1
        status = q.get("status", "pending")
        if status in ("won", "lost"):
            inst_stats[inst][status] += 1
        if status == "won":
            inst_stats[inst]["total_value"] += q.get("total", 0)

    # Win rate
    decided = len(won) + len(lost)
    win_rate = round(len(won) / max(decided, 1) * 100, 1)

    # Average values
    avg_won_value = round(sum(q.get("total", 0) for q in won) / max(len(won), 1), 2)
    avg_lost_value = round(sum(q.get("total", 0) for q in lost) / max(len(lost), 1), 2)

    return {
        "summary": {
            "total_quotes": total,
            "won": len(won),
            "lost": len(lost),
            "pending": len(pending),
            "win_rate": win_rate,
            "avg_won_value": avg_won_value,
            "avg_lost_value": avg_lost_value,
            "total_revenue": round(sum(q.get("total", 0) for q in won), 2),
        },
        "by_agency": dict(agency_stats),
        "by_institution": dict(inst_stats),
        "top_institutions": sorted(
            inst_stats.items(),
            key=lambda x: x[1]["total_value"], reverse=True
        )[:10],
    }


# ─── Pricing Intelligence ───────────────────────────────────────────────────

def pricing_analysis() -> dict:
    """
    Analyze pricing patterns: what markup wins? What loses?
    """
    quotes = _load_quotes()
    pcs = _load_pcs()

    markup_data = {"won": [], "lost": []}
    margin_data = {"won": [], "lost": []}

    for q in quotes:
        status = q.get("status", "pending")
        if status not in ("won", "lost"):
            continue

        # Try to find associated PC for pricing details
        pc_id = q.get("pc_id", "")
        pc = pcs.get(pc_id, {})
        items = pc.get("items", q.get("items", []))

        for item in items:
            pricing = item.get("pricing", {})
            markup = pricing.get("markup_pct", 0)
            cost = pricing.get("unit_cost", 0)
            price = pricing.get("recommended_price", 0)

            if markup > 0:
                markup_data[status].append(markup)
            if cost > 0 and price > 0:
                margin = round((price - cost) / price * 100, 1)
                margin_data[status].append(margin)

    def _avg(lst):
        return round(sum(lst) / max(len(lst), 1), 1)

    return {
        "markup": {
            "avg_won_markup": _avg(markup_data["won"]),
            "avg_lost_markup": _avg(markup_data["lost"]),
            "won_samples": len(markup_data["won"]),
            "lost_samples": len(markup_data["lost"]),
            "insight": (
                "Lower markup wins more often"
                if _avg(markup_data["won"]) < _avg(markup_data["lost"])
                else "Markup doesn't seem to be the deciding factor"
            ) if markup_data["won"] and markup_data["lost"] else "Not enough data yet",
        },
        "margin": {
            "avg_won_margin": _avg(margin_data["won"]),
            "avg_lost_margin": _avg(margin_data["lost"]),
            "insight": "Analyze margin trends as more quotes close",
        },
    }


# ─── Pipeline Health ─────────────────────────────────────────────────────────

def pipeline_health() -> dict:
    """
    How healthy is the current pipeline?
    - How many PCs are stuck at each stage?
    - Average time from parse to completion?
    - Conversion rate through pipeline?
    """
    pcs = _load_pcs()
    if not pcs:
        return {"error": "No price checks data", "total": 0}

    by_status = defaultdict(int)
    completion_times = []
    now = datetime.now()

    for pcid, pc in pcs.items():
        status = pc.get("status", "parsed")
        by_status[status] += 1

        # Track age of PCs
        created = pc.get("created_at", pc.get("uploaded_at", ""))
        if created:
            try:
                created_dt = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
                age_hours = (now - created_dt).total_seconds() / 3600
                if status in ("completed", "converted"):
                    completion_times.append(age_hours)
            except (ValueError, TypeError):
                pass

    avg_completion = round(sum(completion_times) / max(len(completion_times), 1), 1)

    # Stuck PCs (parsed for > 24 hours)
    stuck = 0
    for pcid, pc in pcs.items():
        if pc.get("status") == "parsed":
            created = pc.get("created_at", "")
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
                    if (now - created_dt).total_seconds() > 86400:
                        stuck += 1
                except (ValueError, TypeError):
                    pass

    return {
        "total_pcs": len(pcs),
        "by_status": dict(by_status),
        "avg_completion_hours": avg_completion,
        "stuck_parsed": stuck,
        "conversion_rate": round(
            by_status.get("completed", 0) + by_status.get("converted", 0)
        ) / max(len(pcs), 1) * 100,
    }


# ─── Lead Funnel ─────────────────────────────────────────────────────────────

def lead_funnel() -> dict:
    """
    Lead generation → conversion funnel analysis.
    How many leads → contacted → quoted → won?
    """
    leads = _load_leads()
    if not leads:
        return {"error": "No leads data", "total": 0}

    by_status = defaultdict(int)
    by_institution = defaultdict(int)
    scores = []

    for lead in leads:
        by_status[lead.get("status", "unknown")] += 1
        by_institution[lead.get("institution", "Unknown")] += 1
        scores.append(lead.get("score", 0))

    avg_score = round(sum(scores) / max(len(scores), 1), 2)
    total = len(leads)
    contacted = by_status.get("contacted", 0) + by_status.get("quoted", 0) + by_status.get("won", 0)
    won = by_status.get("won", 0)

    return {
        "total_leads": total,
        "by_status": dict(by_status),
        "contact_rate": round(contacted / max(total, 1) * 100, 1),
        "win_rate": round(won / max(total, 1) * 100, 1),
        "avg_lead_score": avg_score,
        "top_institutions": sorted(
            by_institution.items(), key=lambda x: x[1], reverse=True
        )[:10],
    }


# ─── Recommendations ────────────────────────────────────────────────────────

def generate_recommendations() -> list:
    """
    Generate actionable recommendations based on all available data.
    Returns prioritized list of suggestions.
    """
    recs = []
    wl = win_loss_analysis()
    pricing = pricing_analysis()
    pipeline = pipeline_health()
    funnel = lead_funnel()

    # Win rate recommendations
    summary = wl.get("summary", {})
    win_rate = summary.get("win_rate", 0)
    if win_rate > 60:
        recs.append({
            "priority": "info",
            "area": "win_rate",
            "message": f"Win rate is {win_rate}% — strong. Consider increasing margins.",
            "action": "Test 5% higher markup on next 5 quotes",
        })
    elif win_rate > 0:
        recs.append({
            "priority": "warning",
            "area": "win_rate",
            "message": f"Win rate is {win_rate}% — analyze lost quotes for pricing gaps.",
            "action": "Review lost quotes: are we being undercut or losing on delivery?",
        })

    # Pending quotes
    pending = summary.get("pending", 0)
    if pending > 10:
        recs.append({
            "priority": "action",
            "area": "pipeline",
            "message": f"{pending} quotes pending — follow up on old quotes.",
            "action": "Mark stale quotes (>30 days) as lost and learn from them",
        })

    # Pricing intelligence
    markup = pricing.get("markup", {})
    if markup.get("won_samples", 0) > 3 and markup.get("lost_samples", 0) > 3:
        won_avg = markup.get("avg_won_markup", 0)
        lost_avg = markup.get("avg_lost_markup", 0)
        if won_avg < lost_avg:
            recs.append({
                "priority": "insight",
                "area": "pricing",
                "message": f"Winning markup avg: {won_avg}%, losing: {lost_avg}%.",
                "action": f"Target {won_avg + 2}% markup for optimal win rate × margin",
            })

    # Pipeline stuck
    stuck = pipeline.get("stuck_parsed", 0)
    if stuck > 0:
        recs.append({
            "priority": "action",
            "area": "pipeline",
            "message": f"{stuck} Price Check(s) stuck at 'parsed' for >24 hours.",
            "action": "Process stuck PCs or remove if no longer relevant",
        })

    # Lead generation
    total_leads = funnel.get("total_leads", 0)
    if total_leads == 0:
        recs.append({
            "priority": "action",
            "area": "leads",
            "message": "No leads generated yet. Start the SCPRS scanner.",
            "action": "POST /api/scanner/start to begin scanning for opportunities",
        })
    elif funnel.get("contact_rate", 0) < 30:
        recs.append({
            "priority": "warning",
            "area": "leads",
            "message": f"Only {funnel.get('contact_rate', 0)}% of leads contacted.",
            "action": "Review high-score leads in outbox and send outreach",
        })

    # Top performing institutions
    top_inst = wl.get("top_institutions", [])
    if top_inst:
        best = top_inst[0]
        recs.append({
            "priority": "insight",
            "area": "focus",
            "message": f"Top institution: {best[0]} (${best[1]['total_value']:,.0f} won).",
            "action": f"Prioritize {best[0]} — proactively seek their upcoming POs",
        })

    return sorted(recs, key=lambda x: {"action": 0, "warning": 1, "insight": 2, "info": 3}.get(x["priority"], 4))


# ─── Full Report ─────────────────────────────────────────────────────────────

def full_report() -> dict:
    """Generate a comprehensive growth report."""
    return {
        "generated_at": datetime.now().isoformat(),
        "win_loss": win_loss_analysis(),
        "pricing": pricing_analysis(),
        "pipeline": pipeline_health(),
        "lead_funnel": lead_funnel(),
        "recommendations": generate_recommendations(),
    }


def get_agent_status() -> dict:
    """Agent health status."""
    quotes = _load_quotes()
    pcs = _load_pcs()
    leads = _load_leads()
    return {
        "agent": "growth_strategy",
        "version": "1.0.0",
        "data_available": {
            "quotes": len(quotes),
            "price_checks": len(pcs),
            "leads": len(leads),
        },
        "has_enough_data": len(quotes) >= 5,
    }
