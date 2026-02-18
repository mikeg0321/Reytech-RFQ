"""
forecasting.py — Deal Forecasting + Win Probability (PRD Feature 4.4 P1)

Scores each open quote 0-100 using weighted signals:
  30% Agency relationship (prior POs from this agency)
  20% Category match (does our inventory match their typical purchases)
  20% Contact engagement (has contact replied, opened emails, taken calls)
  20% Price competitiveness (within 10% of SCPRS award prices)
  10% Time recency (quotes >30 days old score lower)

Used by:
  - GET /api/quotes/win-probability  → per-quote scores
  - Home bar: weighted pipeline $ value
  - Quote table: probability badge
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("forecasting")


def score_quote(quote: dict, contacts: list = None, price_history: list = None) -> dict:
    """Score a single quote 0-100 and return breakdown.

    Args:
        quote: Quote dict from quotes_log.json
        contacts: CRM contacts list (for relationship + engagement)
        price_history: Price history list (for competitiveness)

    Returns:
        {score, label, color, breakdown, weighted_value}
    """
    if contacts is None:
        contacts = []
    if price_history is None:
        price_history = []

    agency = (quote.get("agency") or "").lower()
    institution = (quote.get("institution") or "").lower()
    items_detail = quote.get("items_detail") or []
    total = quote.get("total") or 0
    created_at = quote.get("created_at") or quote.get("date") or ""
    status = quote.get("status") or "pending"

    scores = {}

    # ── 1. Agency Relationship (30%) ──────────────────────────────────────
    agency_contacts = [
        c for c in contacts
        if agency and agency in (c.get("agency") or "").lower()
        or institution and institution in (c.get("agency") or "").lower()
    ]
    prior_pos = sum(c.get("po_count") or 0 for c in agency_contacts)
    spend = sum(c.get("spend") or 0 for c in agency_contacts)

    if prior_pos >= 5:
        rel_score = 100
    elif prior_pos >= 2:
        rel_score = 75
    elif prior_pos == 1:
        rel_score = 50
    elif agency_contacts:
        rel_score = 25  # known contact, no POs
    else:
        rel_score = 10  # cold

    scores["agency_relationship"] = {"raw": rel_score, "weight": 0.30,
                                      "detail": f"{prior_pos} prior POs, {len(agency_contacts)} contacts"}

    # ── 2. Category Match (20%) ───────────────────────────────────────────
    our_categories = ["medical supplies", "janitorial", "cleaning", "office", "food service",
                      "safety", "personal protective equipment", "ppe", "gloves", "masks"]
    item_descs = " ".join(
        (it.get("description") or "").lower() for it in items_detail
    )
    matches = sum(1 for cat in our_categories if cat in item_descs)
    cat_score = min(100, matches * 25) if matches else 30  # 30 = unknown category

    scores["category_match"] = {"raw": cat_score, "weight": 0.20,
                                  "detail": f"{matches} category matches"}

    # ── 3. Contact Engagement (20%) ───────────────────────────────────────
    engaged = [c for c in agency_contacts if c.get("status") in
               ("responded", "active", "emailed", "warm")]
    if any(c.get("status") == "responded" for c in agency_contacts):
        eng_score = 90
    elif any(c.get("status") in ("active", "warm") for c in agency_contacts):
        eng_score = 60
    elif engaged:
        eng_score = 40
    elif agency_contacts:
        eng_score = 20
    else:
        eng_score = 5

    scores["contact_engagement"] = {"raw": eng_score, "weight": 0.20,
                                     "detail": f"{len(engaged)} engaged contacts"}

    # ── 4. Price Competitiveness (20%) ────────────────────────────────────
    comp_scores = []
    for item in items_detail:
        desc = (item.get("description") or "").lower()[:40]
        our_price = item.get("unit_price") or item.get("price_each") or 0
        if not our_price:
            continue
        # Find comparable prices in history
        comparable = [p for p in price_history
                      if desc[:20] in (p.get("description") or "").lower()[:40]]
        if comparable:
            avg_hist = sum(p.get("unit_price") or 0 for p in comparable) / len(comparable)
            if avg_hist > 0:
                ratio = our_price / avg_hist
                if ratio <= 0.90:
                    comp_scores.append(100)   # 10%+ below market
                elif ratio <= 1.00:
                    comp_scores.append(75)    # at market
                elif ratio <= 1.10:
                    comp_scores.append(50)    # 10% above
                else:
                    comp_scores.append(25)    # overpriced

    price_score = round(sum(comp_scores) / len(comp_scores)) if comp_scores else 50  # neutral if no data

    scores["price_competitiveness"] = {"raw": price_score, "weight": 0.20,
                                        "detail": f"{len(comp_scores)} items benchmarked"}

    # ── 5. Time Recency (10%) ─────────────────────────────────────────────
    days_old = 0
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at[:19])
            days_old = (datetime.now() - dt).days
        except Exception:
            pass

    if days_old <= 7:
        time_score = 100
    elif days_old <= 14:
        time_score = 80
    elif days_old <= 30:
        time_score = 50
    elif days_old <= 60:
        time_score = 25
    else:
        time_score = 10

    scores["time_recency"] = {"raw": time_score, "weight": 0.10,
                               "detail": f"{days_old} days old"}

    # ── Weighted Total ────────────────────────────────────────────────────
    total_score = sum(
        v["raw"] * v["weight"] for v in scores.values()
    )
    final_score = round(total_score)

    # Label + color
    if final_score >= 70:
        label, color = "High", "#3fb950"
    elif final_score >= 40:
        label, color = "Medium", "#e3b341"
    else:
        label, color = "Low", "#f85149"

    weighted_value = round(total * (final_score / 100), 2)

    return {
        "quote_number": quote.get("quote_number"),
        "score": final_score,
        "label": label,
        "color": color,
        "weighted_value": weighted_value,
        "total": total,
        "breakdown": scores,
        "agency": quote.get("agency") or institution,
        "status": status,
    }


def score_all_quotes() -> dict:
    """Score all open (non-won, non-lost) quotes.

    Returns:
        {ok, scores[], weighted_pipeline, avg_score, high_count, medium_count, low_count}
    """
    try:
        from src.forms.quote_generator import get_all_quotes
        from src.core.agent_context import get_context
        from src.core.db import get_db

        all_quotes = get_all_quotes()
        open_quotes = [q for q in all_quotes
                       if q.get("status") in ("pending", "sent", "draft")
                       and not q.get("is_test")]

        ctx = get_context(include_contacts=True)
        contacts = ctx.get("contacts", [])

        # Get price history
        price_history = []
        try:
            with get_db() as conn:
                rows = conn.execute(
                    "SELECT description, unit_price, agency FROM price_history LIMIT 500"
                ).fetchall()
                price_history = [dict(r) for r in rows]
        except Exception:
            pass

        scored = [score_quote(q, contacts=contacts, price_history=price_history)
                  for q in open_quotes]
        scored.sort(key=lambda x: x["score"], reverse=True)

        weighted_pipeline = sum(s["weighted_value"] for s in scored)
        raw_pipeline = sum(s["total"] for s in scored)
        avg_score = round(sum(s["score"] for s in scored) / len(scored)) if scored else 0

        return {
            "ok": True,
            "scores": scored,
            "count": len(scored),
            "weighted_pipeline": round(weighted_pipeline, 2),
            "raw_pipeline": round(raw_pipeline, 2),
            "avg_score": avg_score,
            "high_count": sum(1 for s in scored if s["label"] == "High"),
            "medium_count": sum(1 for s in scored if s["label"] == "Medium"),
            "low_count": sum(1 for s in scored if s["label"] == "Low"),
        }
    except Exception as e:
        log.error("score_all_quotes: %s", e)
        return {"ok": False, "error": str(e), "scores": [], "weighted_pipeline": 0}
