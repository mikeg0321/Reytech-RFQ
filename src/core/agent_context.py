"""
agent_context.py — Shared DB context layer for all Reytech agents.

Implements the "Domain-Specific Intelligence" pattern from:
  Anthropic Complete Guide to Building Skills for Claude (Feb 2026)
  Pattern 5: Domain-specific intelligence — skill adds specialized knowledge
  beyond tool access. Compliance before action. Comprehensive documentation.

Every agent can call get_context() to receive a rich snapshot of:
  - CRM contacts + recent activity
  - Live price history for any item
  - Quote performance (won/lost/pending)
  - Intel buyers and revenue progress
  - System health signals

This eliminates the need for agents to load files independently and ensures
they all operate from the same ground truth (SQLite on Railway volume).
"""
import os
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("agent_context")
_ctx_lock = threading.Lock()
_ctx_cache: dict = {}
_ctx_cache_ts: float = 0
CTX_TTL = 60  # seconds — refresh every minute max


def get_context(
    include_prices: bool = False,
    price_query: str = "",
    include_contacts: bool = True,
    include_quotes: bool = True,
    include_revenue: bool = True,
    contact_limit: int = 50,
    force_refresh: bool = False,
) -> dict:
    """Return a rich agent context snapshot from SQLite + JSON.

    Args:
        include_prices: Pull price history for price_query term
        price_query: Description/keyword to look up in price history
        include_contacts: Include CRM contact summary
        include_quotes: Include quote pipeline summary
        include_revenue: Include revenue goal progress
        contact_limit: Max contacts to include in summary
        force_refresh: Bypass cache

    Returns:
        {
          contacts: [{id, name, agency, email, spend, categories, last_activity}],
          quotes: {total, pending, sent, won, lost, pipeline_value, win_rate},
          revenue: {goal, closed, pct, gap, run_rate_annual, monthly_needed},
          prices: {query, count, best_price, avg_price, results[]},
          intel: {total_buyers, agencies, top_categories[]},
          timestamp: str,
          source: "sqlite"|"json"|"mixed",
        }
    """
    global _ctx_cache, _ctx_cache_ts
    now = datetime.now().timestamp()

    if not force_refresh and _ctx_cache and (now - _ctx_cache_ts) < CTX_TTL:
        # Still serve cached ctx but update prices if new query
        if include_prices and price_query:
            ctx = dict(_ctx_cache)
            ctx["prices"] = _get_price_context(price_query)
            return ctx
        return dict(_ctx_cache)

    with _ctx_lock:
        ctx = {
            "timestamp": datetime.now().isoformat(),
            "source": "mixed",
            "contacts": [],
            "quotes": {},
            "revenue": {},
            "intel": {},
            "prices": {},
        }

        # ── CRM Contacts ──────────────────────────────────────────────────
        if include_contacts:
            ctx["contacts"] = _get_contact_context(limit=contact_limit)

        # ── Quote Pipeline ────────────────────────────────────────────────
        if include_quotes:
            ctx["quotes"] = _get_quote_context()

        # ── Revenue Progress ──────────────────────────────────────────────
        if include_revenue:
            ctx["revenue"] = _get_revenue_context()

        # ── Intel Buyers ──────────────────────────────────────────────────
        ctx["intel"] = _get_intel_context()

        # ── Price History (on demand) ─────────────────────────────────────
        if include_prices and price_query:
            ctx["prices"] = _get_price_context(price_query)

        _ctx_cache = ctx
        _ctx_cache_ts = now
        return ctx


def _get_contact_context(limit: int = 50) -> list:
    """Load CRM contacts from SQLite or JSON fallback."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT id, buyer_name, agency, buyer_email, phone,
                          annual_spend, categories, outreach_status,
                          last_activity, items_purchased, po_count
                   FROM contacts
                   ORDER BY annual_spend DESC NULLS LAST
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            contacts = []
            for r in rows:
                cats = []
                try:
                    cats = json.loads(r["categories"] or "[]")
                except Exception:
                    pass
                contacts.append({
                    "id": r["id"],
                    "name": r["buyer_name"] or "",
                    "agency": r["agency"] or "",
                    "email": r["buyer_email"] or "",
                    "phone": r["phone"] or "",
                    "spend": r["annual_spend"] or 0,
                    "categories": cats,
                    "status": r["outreach_status"] or "new",
                    "last_activity": r["last_activity"] or "",
                    "po_count": r["po_count"] or 0,
                })
            if contacts:
                return contacts
    except Exception as e:
        log.debug("SQLite contact load: %s", e)

    # JSON fallback
    try:
        from src.core.paths import DATA_DIR
        with open(os.path.join(DATA_DIR, "crm_contacts.json")) as f:
            raw = json.load(f)
        contacts = []
        for cid, c in (raw.items() if isinstance(raw, dict) else {}.items()):
            contacts.append({
                "id": cid,
                "name": c.get("buyer_name", ""),
                "agency": c.get("agency", ""),
                "email": c.get("buyer_email", ""),
                "spend": c.get("annual_spend", 0),
                "categories": c.get("categories", []),
                "status": c.get("outreach_status", "new"),
            })
        return contacts[:limit]
    except Exception as e:
        log.debug("JSON contact load: %s", e)
        return []


def _get_quote_context() -> dict:
    """Summarize quote pipeline."""
    try:
        from src.forms.quote_generator import get_all_quotes
        quotes = [q for q in get_all_quotes() if not q.get("is_test")]
        pending = sum(1 for q in quotes if q.get("status") in ("pending", "draft"))
        sent = sum(1 for q in quotes if q.get("status") == "sent")
        won = sum(1 for q in quotes if q.get("status") == "won")
        lost = sum(1 for q in quotes if q.get("status") == "lost")
        pipeline_value = sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent", "draft"))
        total_won_value = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")
        decided = won + lost
        return {
            "total": len(quotes),
            "pending": pending,
            "sent": sent,
            "won": won,
            "lost": lost,
            "pipeline_value": pipeline_value,
            "total_won_value": total_won_value,
            "win_rate": round(won / decided * 100) if decided > 0 else 0,
        }
    except Exception as e:
        log.debug("Quote context: %s", e)
        return {}


def _get_revenue_context() -> dict:
    """Load revenue goal progress."""
    try:
        from src.core.paths import DATA_DIR
        with open(os.path.join(DATA_DIR, "intel_revenue.json")) as f:
            rv = json.load(f)
        if isinstance(rv, dict) and rv.get("ok"):
            return {
                "goal": rv.get("goal", 2_000_000),
                "closed": rv.get("closed_revenue", 0),
                "pct": rv.get("pct_to_goal", 0),
                "gap": rv.get("gap_to_goal", 2_000_000),
                "monthly_needed": rv.get("monthly_needed", 181818),
                "run_rate_annual": rv.get("run_rate_annual", 0),
                "on_track": rv.get("on_track", False),
                "year": rv.get("year", datetime.now().year),
            }
    except Exception:
        pass
    return {"goal": 2_000_000, "closed": 0, "pct": 0, "gap": 2_000_000}


def _get_intel_context() -> dict:
    """Load intel buyer summary."""
    try:
        from src.core.paths import DATA_DIR
        with open(os.path.join(DATA_DIR, "intel_buyers.json")) as f:
            d = json.load(f)
        if not isinstance(d, dict):
            return {}
        buyers = d.get("buyers", [])
        agencies = list(set(b.get("agency", "") for b in buyers if b.get("agency")))
        all_cats = []
        for b in buyers:
            all_cats.extend(b.get("categories", []))
        from collections import Counter
        top_cats = [c for c, _ in Counter(all_cats).most_common(5)]
        return {
            "total_buyers": d.get("total_buyers", len(buyers)),
            "agencies": agencies[:10],
            "top_categories": top_cats,
        }
    except Exception:
        return {}


def _get_price_context(query: str) -> dict:
    """Pull price history for a description query."""
    if not query:
        return {}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT description, unit_price, source, agency, quote_number,
                          part_number, manufacturer, looked_up_at
                   FROM price_history
                   WHERE lower(description) LIKE lower(?)
                   ORDER BY unit_price ASC
                   LIMIT 20""",
                (f"%{query}%",)
            ).fetchall()
            if rows:
                prices = [r["unit_price"] for r in rows]
                return {
                    "query": query,
                    "count": len(rows),
                    "best_price": min(prices),
                    "avg_price": round(sum(prices) / len(prices), 2),
                    "max_price": max(prices),
                    "results": [dict(r) for r in rows[:10]],
                }
    except Exception as e:
        log.debug("Price context: %s", e)
    return {"query": query, "count": 0}


def get_contact_by_agency(agency: str) -> list:
    """Get all contacts for a specific agency."""
    ctx = get_context(include_contacts=True)
    agency_l = agency.lower()
    return [c for c in ctx.get("contacts", [])
            if agency_l in (c.get("agency") or "").lower()]


def get_best_price(description: str) -> Optional[float]:
    """Get the best (lowest) price ever seen for a description."""
    result = _get_price_context(description)
    return result.get("best_price")


def format_context_for_agent(ctx: dict, focus: str = "all") -> str:
    """Format context as a structured prompt snippet for agent system prompts.

    Implements progressive disclosure: start with most relevant context,
    add detail as needed. Following the Anthropic Skills guide principle:
    'Keep SKILL.md focused on core instructions. Move detailed documentation
    to references/ and link to it.'

    focus: 'all' | 'crm' | 'quotes' | 'revenue' | 'intel'
    """
    lines = [
        "=== REYTECH LIVE CONTEXT ===",
        f"Timestamp: {ctx.get('timestamp', 'unknown')}",
        "",
    ]

    rev = ctx.get("revenue", {})
    if rev and focus in ("all", "revenue"):
        pct = rev.get("pct", 0)
        lines += [
            f"REVENUE GOAL {rev.get('year', 2026)}: ${rev.get('goal', 2_000_000):,.0f}",
            f"  Closed: ${rev.get('closed', 0):,.0f} ({pct:.1f}%)",
            f"  Gap: ${rev.get('gap', 2_000_000):,.0f}",
            f"  Monthly needed: ${rev.get('monthly_needed', 181818):,.0f}",
            f"  On track: {'YES' if rev.get('on_track') else 'NO — need to accelerate'}",
            "",
        ]

    qt = ctx.get("quotes", {})
    if qt and focus in ("all", "quotes"):
        lines += [
            f"QUOTE PIPELINE: {qt.get('total', 0)} total",
            f"  Pending: {qt.get('pending', 0)}  Sent: {qt.get('sent', 0)}  Won: {qt.get('won', 0)}  Lost: {qt.get('lost', 0)}",
            f"  Pipeline value: ${qt.get('pipeline_value', 0):,.0f}  Won value: ${qt.get('total_won_value', 0):,.0f}",
            f"  Win rate: {qt.get('win_rate', 0)}%",
            "",
        ]

    intel = ctx.get("intel", {})
    if intel and focus in ("all", "intel"):
        lines += [
            f"INTEL: {intel.get('total_buyers', 0)} SCPRS buyers tracked",
            f"  Agencies: {', '.join(intel.get('agencies', [])[:5])}",
            f"  Top categories: {', '.join(intel.get('top_categories', []))}",
            "",
        ]

    contacts = ctx.get("contacts", [])
    if contacts and focus in ("all", "crm"):
        warm = [c for c in contacts if c.get("status") in ("emailed", "responded", "active")]
        lines += [
            f"CRM: {len(contacts)} contacts loaded",
            f"  Warm/Active: {len(warm)}",
        ]
        if warm[:3]:
            for c in warm[:3]:
                lines.append(f"  → {c.get('name', '?')} @ {c.get('agency', '?')} ({c.get('email', '?')})")
        lines.append("")

    prices = ctx.get("prices", {})
    if prices.get("count", 0) > 0:
        lines += [
            f"PRICE HISTORY for '{prices['query']}': {prices['count']} data points",
            f"  Best: ${prices.get('best_price', 0):,.2f}  Avg: ${prices.get('avg_price', 0):,.2f}",
            "",
        ]

    return "\n".join(lines)
