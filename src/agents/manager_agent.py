"""
manager_agent.py — Manager / Orchestrator Agent for Reytech
Phase 14 | Version: 1.0.0

The agent that watches all other agents and briefs the human.

Provides:
  1. Morning Brief — what happened since you last looked, what needs attention
  2. Pending Approvals — emails to send, leads to contact, quotes to follow up
  3. Activity Feed — recent pipeline activity in chronological order
  4. Agent Health — which agents are working, which need attention

Data sources: All JSON stores in DATA_DIR + agent status endpoints.
"""

import json
import os
import logging
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("manager")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


def _load_json(filename: str, default=None):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default if default is not None else []


def _age_str(iso_str: str) -> str:
    """Convert ISO timestamp to human-readable age like '2h ago', '3d ago'."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)
        delta = datetime.now() - dt
        secs = delta.total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3600:
            return f"{int(secs/60)}m ago"
        if secs < 86400:
            return f"{int(secs/3600)}h ago"
        days = int(secs / 86400)
        return f"{days}d ago"
    except (ValueError, TypeError):
        return ""


# ─── Pending Approvals ───────────────────────────────────────────────────────

def _get_pending_approvals() -> list:
    """Things that need human sign-off before they can proceed."""
    approvals = []

    # 1. Draft emails in outbox
    outbox = _load_json("email_outbox.json", [])
    drafts = [e for e in outbox if e.get("status") == "draft"]
    for d in drafts[:5]:  # Cap at 5
        approvals.append({
            "type": "email_draft",
            "icon": "✉️",
            "title": f"Email to {d.get('to', 'unknown')}",
            "detail": d.get("subject", "")[:60],
            "age": _age_str(d.get("created_at", "")),
            "action_url": "/agents",
            "action_label": "Review in Outbox",
            "id": d.get("id", ""),
        })

    # 2. Approved emails waiting to send
    approved = [e for e in outbox if e.get("status") == "approved"]
    if approved:
        approvals.append({
            "type": "email_send",
            "icon": "🚀",
            "title": f"{len(approved)} email{'s' if len(approved)!=1 else ''} approved & ready to send",
            "detail": "Go to Agents → Send All Approved",
            "age": "",
            "action_url": "/agents",
            "action_label": "Send Now",
        })

    # 3. New leads needing outreach
    leads = _load_json("leads.json", [])
    new_leads = [l for l in leads if l.get("status") == "new"]
    if new_leads:
        top = sorted(new_leads, key=lambda x: x.get("score", 0), reverse=True)[:3]
        for l in top:
            approvals.append({
                "type": "lead_new",
                "icon": "🎯",
                "title": f"Lead: {l.get('institution', '?')} — score {l.get('score', 0):.0%}",
                "detail": f"PO {l.get('po_number', '?')} · ${l.get('po_value', 0):,.0f}",
                "age": _age_str(l.get("created_at", "")),
                "action_url": "/agents",
                "action_label": "Draft Outreach",
                "id": l.get("id", ""),
            })

    # 4. Pending quotes (no response for 7+ days)
    quotes = _load_json("quotes_log.json", [])
    stale_pending = []
    for q in quotes:
        if q.get("status") != "pending":
            continue
        created = q.get("created_at", q.get("generated_at", ""))
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
                if (datetime.now() - dt).days >= 7:
                    stale_pending.append(q)
            except (ValueError, TypeError):
                pass
    if stale_pending:
        approvals.append({
            "type": "stale_quote",
            "icon": "⏰",
            "title": f"{len(stale_pending)} quote{'s' if len(stale_pending)!=1 else ''} pending 7+ days",
            "detail": "Follow up or mark as won/lost",
            "age": "",
            "action_url": "/quotes?status=pending",
            "action_label": "Review Quotes",
        })

    return approvals


# ─── Activity Feed ───────────────────────────────────────────────────────────

def _get_activity_feed(limit: int = 10) -> list:
    """Recent pipeline activity, newest first."""
    events = []

    # PC status changes
    pcs = _load_json("price_checks.json", {})
    for pcid, pc in (pcs.items() if isinstance(pcs, dict) else []):
        history = pc.get("status_history", [])
        for h in history[-3:]:  # Last 3 transitions per PC
            events.append({
                "icon": "📄",
                "text": f"PC #{pc.get('pc_number', pcid[:8])} → {h.get('to', '?')}",
                "detail": pc.get("institution", ""),
                "timestamp": h.get("timestamp", ""),
                "age": _age_str(h.get("timestamp", "")),
            })

    # Quotes generated
    quotes = _load_json("quotes_log.json", [])
    for q in quotes[-10:]:
        ts = q.get("created_at", q.get("generated_at", ""))
        events.append({
            "icon": "📋",
            "text": f"Quote {q.get('quote_number', '?')} generated",
            "detail": f"{q.get('institution', '')} · ${q.get('total', 0):,.0f}",
            "timestamp": ts,
            "age": _age_str(ts),
        })
        # Status changes on quotes
        if q.get("status") in ("won", "lost"):
            events.append({
                "icon": "🏆" if q["status"] == "won" else "❌",
                "text": f"Quote {q.get('quote_number', '?')} marked {q['status']}",
                "detail": q.get("institution", ""),
                "timestamp": q.get("status_updated", ts),
                "age": _age_str(q.get("status_updated", ts)),
            })

    # Emails sent
    sent = _load_json("email_sent_log.json", [])
    for s in sent[-5:]:
        events.append({
            "icon": "📧",
            "text": f"Email sent to {s.get('to', '?')}",
            "detail": s.get("subject", "")[:50],
            "timestamp": s.get("sent_at", ""),
            "age": _age_str(s.get("sent_at", "")),
        })

    # Sort by timestamp, newest first
    events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return events[:limit]


# ─── Pipeline Summary ────────────────────────────────────────────────────────

def _get_pipeline_summary() -> dict:
    """Quick numbers: what's in each stage."""
    pcs = _load_json("price_checks.json", {})
    quotes = _load_json("quotes_log.json", [])
    leads = _load_json("leads.json", [])
    outbox = _load_json("email_outbox.json", [])

    pc_by_status = defaultdict(int)
    for pc in (pcs.values() if isinstance(pcs, dict) else []):
        pc_by_status[pc.get("status", "unknown")] += 1

    q_by_status = defaultdict(int)
    for q in quotes:
        q_by_status[q.get("status", "pending")] += 1

    total_revenue = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")

    return {
        "price_checks": {
            "total": len(pcs) if isinstance(pcs, dict) else 0,
            "parsed": pc_by_status.get("parsed", 0),
            "priced": pc_by_status.get("priced", 0),
            "completed": pc_by_status.get("completed", 0),
        },
        "quotes": {
            "total": len(quotes),
            "pending": q_by_status.get("pending", 0),
            "won": q_by_status.get("won", 0),
            "lost": q_by_status.get("lost", 0),
            "win_rate": round(
                q_by_status.get("won", 0) /
                max(q_by_status.get("won", 0) + q_by_status.get("lost", 0), 1) * 100
            ),
        },
        "leads": {
            "total": len(leads),
            "new": sum(1 for l in leads if l.get("status") == "new"),
            "contacted": sum(1 for l in leads if l.get("status") == "contacted"),
        },
        "outbox": {
            "drafts": sum(1 for e in outbox if e.get("status") == "draft"),
            "approved": sum(1 for e in outbox if e.get("status") == "approved"),
            "sent_total": len(_load_json("email_sent_log.json", [])),
        },
        "revenue": {
            "won_total": round(total_revenue, 2),
        },
    }


# ─── Manager Brief (the main output) ────────────────────────────────────────

def generate_brief() -> dict:
    """
    Generate the manager's daily brief.
    Everything the human needs to know in one glance.
    """
    approvals = _get_pending_approvals()
    activity = _get_activity_feed(limit=8)
    summary = _get_pipeline_summary()

    # Generate the headline
    headlines = []
    if approvals:
        action_count = len(approvals)
        headlines.append(f"{action_count} item{'s' if action_count!=1 else ''} need{'s' if action_count==1 else ''} your attention")
    if summary["outbox"]["drafts"] > 0:
        headlines.append(f"{summary['outbox']['drafts']} email draft{'s' if summary['outbox']['drafts']!=1 else ''} awaiting review")
    if summary["leads"]["new"] > 0:
        headlines.append(f"{summary['leads']['new']} new lead{'s' if summary['leads']['new']!=1 else ''} to pursue")
    if summary["price_checks"]["parsed"] > 0:
        headlines.append(f"{summary['price_checks']['parsed']} PC{'s' if summary['price_checks']['parsed']!=1 else ''} awaiting pricing")

    if not headlines:
        if summary["quotes"]["won"] > 0:
            headlines.append(f"Pipeline clear. {summary['quotes']['won']} quotes won — ${summary['revenue']['won_total']:,.0f} total revenue")
        else:
            headlines.append("Pipeline clear. Upload a PC or check email to get started.")

    # Recommendations (top 3)
    recs = []
    try:
        from src.agents.growth_agent import generate_recommendations
        recs = generate_recommendations()[:3]
    except Exception:
        pass

    return {
        "generated_at": datetime.now().isoformat(),
        "headline": headlines[0] if headlines else "All clear",
        "headlines": headlines,
        "pending_approvals": approvals,
        "approval_count": len(approvals),
        "activity": activity,
        "summary": summary,
        "recommendations": recs,
    }


def get_agent_status() -> dict:
    """Manager agent health."""
    return {
        "agent": "manager",
        "version": "1.0.0",
        "status": "active",
        "brief_available": True,
    }
