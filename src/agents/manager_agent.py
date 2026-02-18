"""
manager_agent.py — Manager / Orchestrator Agent for Reytech
Phase 14→26 | Version: 2.0.0

The agent that watches ALL other agents and briefs the human.

Checks: Pipeline, Quotes, PCs, Growth Engine, Sales Intel, Voice Agent,
        QA Health, Revenue Goal, Email Outbox, Orders, CRM activity.
"""

import json
import os
import logging
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("manager")
# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Gives this agent access to live CRM, quotes, revenue, price history from SQLite.
# Eliminates file loading duplication and ensures consistent ground truth.
try:
    from src.core.agent_context import (
        get_context, format_context_for_agent,
        get_contact_by_agency, get_best_price,
    )
    HAS_AGENT_CTX = True
except ImportError:
    HAS_AGENT_CTX = False
    def get_context(**kw): return {}
    def format_context_for_agent(c, **kw): return ""
    def get_contact_by_agency(a): return []
    def get_best_price(d): return None



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
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)
        delta = datetime.now() - dt
        secs = delta.total_seconds()
        if secs < 0:
            return "upcoming"
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


# ─── Pending Approvals ────────────────────────────────────────────────────

def _get_pending_approvals() -> list:
    approvals = []

    # 1. Draft emails in outbox
    outbox = _load_json("email_outbox.json", [])
    drafts = [e for e in outbox if e.get("status") == "draft"]
    for d in drafts[:5]:
        approvals.append({
            "type": "email_draft", "icon": "✉️",
            "title": f"Email to {d.get('to', 'unknown')}",
            "detail": d.get("subject", "")[:60],
            "age": _age_str(d.get("created_at", "")),
            "action_url": "/agents", "action_label": "Review in Outbox",
        })

    # 2. Approved emails ready to send
    approved = [e for e in outbox if e.get("status") == "approved"]
    if approved:
        approvals.append({
            "type": "email_send", "icon": "🚀",
            "title": f"{len(approved)} email{'s' if len(approved)!=1 else ''} approved & ready",
            "detail": "Go to Agents → Send All Approved",
            "age": "", "action_url": "/agents", "action_label": "Send Now",
        })

    # 3. New leads needing outreach
    leads = _load_json("leads.json", [])
    new_leads = [l for l in leads if l.get("status") == "new"]
    if new_leads:
        top = sorted(new_leads, key=lambda x: x.get("score", 0), reverse=True)[:3]
        for l in top:
            approvals.append({
                "type": "lead_new", "icon": "🎯",
                "title": f"Lead: {l.get('institution', '?')} — score {l.get('score', 0):.0%}",
                "detail": f"PO {l.get('po_number', '?')} · ${l.get('po_value', 0):,.0f}",
                "age": _age_str(l.get("created_at", "")),
                "action_url": "/agents", "action_label": "Draft Outreach",
            })

    # 4. Stale quotes (pending 7+ days)
    quotes = _load_json("quotes_log.json", [])
    for q in quotes:
        if q.get("status") not in ("pending", "sent") or q.get("is_test"):
            continue
        created = q.get("sent_at", q.get("created_at", ""))
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
                if (datetime.now() - dt).days >= 7:
                    approvals.append({
                        "type": "stale_quote", "icon": "⏰",
                        "title": f"Quote {q.get('quote_number','?')} — {q.get('status','')} for {(datetime.now()-dt).days}d",
                        "detail": f"{q.get('requestor','') or q.get('institution','')} · ${q.get('total',0):,.0f}",
                        "age": _age_str(created),
                        "action_url": "/quotes", "action_label": "Follow Up",
                    })
            except (ValueError, TypeError):
                pass

    # 5. Growth follow-ups due
    try:
        from src.agents.growth_agent import check_follow_ups
        fu = check_follow_ups()
        if fu.get("count", 0) > 0:
            approvals.append({
                "type": "growth_followup", "icon": "📞",
                "title": f"{fu['count']} growth prospect{'s' if fu['count']!=1 else ''} due for voice follow-up",
                "detail": "Email sent, no response — time to call",
                "age": "", "action_url": "/growth", "action_label": "Launch Calls",
            })
    except Exception:
        pass

    # 6. Growth bounced emails
    try:
        prospects = _load_json("growth_prospects.json", {})
        if isinstance(prospects, dict):
            bounced = [p for p in prospects.get("prospects", []) if p.get("outreach_status") == "bounced"]
            if bounced:
                approvals.append({
                    "type": "growth_bounced", "icon": "⛔",
                    "title": f"{len(bounced)} prospect email{'s' if len(bounced)!=1 else ''} bounced",
                    "detail": "Need alternate contacts",
                    "age": "", "action_url": "/growth", "action_label": "Fix Contacts",
                })
    except Exception:
        pass

    return approvals


# ─── Activity Feed ────────────────────────────────────────────────────────

def _get_activity_feed(limit: int = 12) -> list:
    events = []

    # Quotes
    quotes = _load_json("quotes_log.json", [])
    for q in quotes[-15:]:
        if q.get("is_test"):
            continue
        ts = q.get("created_at", q.get("generated_at", ""))
        events.append({
            "icon": "📋", "text": f"Quote {q.get('quote_number', '?')} — {q.get('status','pending')}",
            "detail": f"{q.get('requestor','') or q.get('institution','')} · ${q.get('total',0):,.0f}",
            "timestamp": ts, "age": _age_str(ts),
        })

    # PC status changes
    pcs = _load_json("price_checks.json", {})
    for pcid, pc in (pcs.items() if isinstance(pcs, dict) else []):
        for h in (pc.get("status_history", []) or [])[-2:]:
            events.append({
                "icon": "📄", "text": f"PC #{pc.get('pc_number', pcid[:8])} → {h.get('to', '?')}",
                "detail": pc.get("institution", ""),
                "timestamp": h.get("timestamp", ""), "age": _age_str(h.get("timestamp", "")),
            })

    # CRM events
    crm = _load_json("crm_activity.json", [])
    icons = {"quote_won": "🏆", "quote_lost": "❌", "quote_sent": "📤",
             "order_created": "📦", "voice_call": "📞", "email_sent": "📧"}
    for e in (crm[-10:] if isinstance(crm, list) else []):
        events.append({
            "icon": icons.get(e.get("event_type"), "📝"),
            "text": e.get("event_type", "event").replace("_", " ").title(),
            "detail": e.get("detail", "")[:60],
            "timestamp": e.get("timestamp", ""), "age": _age_str(e.get("timestamp", "")),
        })

    # Growth outreach
    outreach = _load_json("growth_outreach.json", {})
    if isinstance(outreach, dict):
        for c in outreach.get("campaigns", [])[-3:]:
            ts = c.get("launched_at", "")
            sent = sum(1 for o in c.get("outreach", []) if o.get("email_sent"))
            events.append({
                "icon": "🚀", "text": f"Growth campaign: {sent} emails sent",
                "detail": c.get("campaign_name", ""),
                "timestamp": ts, "age": _age_str(ts),
            })

    events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return events[:limit]


# ─── Agent Health Check ─────────────────────────────────────────────────

def _check_all_agents() -> list:
    """Check status of every agent in the system."""
    agents = []

    # Email Poller
    try:
        from src.agents.email_poller import EmailPoller
        agents.append({"name": "Email Poller", "icon": "📧", "status": "ready",
                        "detail": "Watches inbox for PCs and RFQs"})
    except ImportError:
        agents.append({"name": "Email Poller", "icon": "📧", "status": "unavailable", "detail": "Import failed"})

    # SCPRS Scanner
    try:
        from src.agents.scprs_scanner import get_scanner_status
        st = get_scanner_status()
        agents.append({"name": "SCPRS Scanner", "icon": "🔍", "status": st.get("status", "ready"),
                        "detail": f"Scanned {st.get('total_scans', 0)} times, {st.get('leads_generated', 0)} leads found"})
    except ImportError:
        agents.append({"name": "SCPRS Scanner", "icon": "🔍", "status": "unavailable", "detail": "Module not loaded"})
    except Exception as e:
        agents.append({"name": "SCPRS Scanner", "icon": "🔍", "status": "error", "detail": str(e)[:40]})

    # Voice Agent
    try:
        from src.agents.voice_agent import is_configured, SCRIPTS
        configured = is_configured()
        agents.append({"name": "Voice Agent", "icon": "📞",
                        "status": "active" if configured else "not configured",
                        "detail": f"{len(SCRIPTS)} scripts available"})
    except Exception:
        agents.append({"name": "Voice Agent", "icon": "📞", "status": "unavailable", "detail": "Not loaded"})

    # Growth Engine
    try:
        from src.agents.growth_agent import get_growth_status
        gs = get_growth_status()
        prospect_count = gs.get("prospects", {}).get("total", 0) if isinstance(gs, dict) else 0
        agents.append({"name": "Growth Engine", "icon": "🚀", "status": "active",
                        "detail": f"{prospect_count} prospects in pipeline"})
    except Exception:
        agents.append({"name": "Growth Engine", "icon": "🚀", "status": "unavailable", "detail": "Not loaded"})

    # Sales Intelligence
    try:
        from src.agents.sales_intel import get_intel_status
        si = get_intel_status()
        buyers = si.get("buyers", {}).get("total", 0) if isinstance(si, dict) else 0
        agents.append({"name": "Sales Intel", "icon": "🧠", "status": "active",
                        "detail": f"{buyers} buyers in database"})
    except Exception:
        agents.append({"name": "Sales Intel", "icon": "🧠", "status": "unavailable", "detail": "Not loaded"})

    # QA Agent
    try:
        from src.agents.qa_agent import agent_status as qa_status
        qs = qa_status()
        agents.append({"name": "QA Health Monitor", "icon": "🏥",
                        "status": qs.get("status", "unknown"),
                        "detail": f"Score: {qs.get('last_score','—')} ({qs.get('last_grade','—')})"})
    except Exception:
        agents.append({"name": "QA Health Monitor", "icon": "🏥", "status": "unavailable", "detail": "Not loaded"})

    # QuickBooks
    try:
        from src.agents.quickbooks_agent import is_configured as qb_is_configured, get_access_token
        configured = qb_is_configured()
        if configured:
            # Try to actually get a token to verify connection
            token = get_access_token()
            agents.append({"name": "QuickBooks", "icon": "💰",
                            "status": "connected" if token else "auth_expired",
                            "detail": "Invoices, AR, customer sync" if token else "Token expired — reconnect at /api/qb/connect"})
        else:
            agents.append({"name": "QuickBooks", "icon": "💰",
                            "status": "not configured",
                            "detail": "Set QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REALM_ID, QB_REFRESH_TOKEN"})
    except ImportError:
        agents.append({"name": "QuickBooks", "icon": "💰", "status": "unavailable", "detail": "Module not loaded"})
    except Exception as e:
        agents.append({"name": "QuickBooks", "icon": "💰", "status": "error", "detail": str(e)[:50]})

    # Predictive Intel
    try:
        from src.agents.predictive_intel import predict_win_probability, get_competitor_insights
        agents.append({"name": "Predictive Intel", "icon": "🔮", "status": "active",
                        "detail": "Win probability, competitor intel, shipping detection"})
    except ImportError:
        agents.append({"name": "Predictive Intel", "icon": "🔮", "status": "unavailable", "detail": "Module not loaded"})
    except Exception as e:
        agents.append({"name": "Predictive Intel", "icon": "🔮", "status": "error", "detail": str(e)[:40]})

    return agents


# ─── Revenue & Goal Tracking ─────────────────────────────────────────────

def _get_revenue_status() -> dict:
    """Pull revenue goal data from Sales Intel."""
    try:
        from src.agents.sales_intel import update_revenue_tracker
        return update_revenue_tracker()
    except Exception:
        # Fallback: just count won quotes
        quotes = _load_json("quotes_log.json", [])
        won_total = sum(q.get("total", 0) for q in quotes
                        if q.get("status") == "won" and not q.get("is_test"))
        return {"ok": True, "closed_revenue": won_total, "goal": 2000000,
                "pct_to_goal": round(won_total / 2000000 * 100, 1),
                "gap_to_goal": 2000000 - won_total}


# ─── Pipeline Summary ─────────────────────────────────────────────────────

def _get_pipeline_summary() -> dict:
    pcs = _load_json("price_checks.json", {})
    quotes = _load_json("quotes_log.json", [])
    live_quotes = [q for q in quotes if not q.get("is_test")]
    leads = _load_json("leads.json", [])
    outbox = _load_json("email_outbox.json", [])
    orders = _load_json("orders.json", {})
    live_orders = {k: v for k, v in (orders.items() if isinstance(orders, dict) else [])}

    pc_by_status = defaultdict(int)
    for pc in (pcs.values() if isinstance(pcs, dict) else []):
        pc_by_status[pc.get("status", "unknown")] += 1

    q_by_status = defaultdict(int)
    for q in live_quotes:
        q_by_status[q.get("status", "pending")] += 1

    total_revenue = sum(q.get("total", 0) for q in live_quotes if q.get("status") == "won")
    pipeline_value = sum(q.get("total", 0) for q in live_quotes if q.get("status") in ("pending", "sent"))

    # Growth prospects
    prospects = _load_json("growth_prospects.json", {})
    p_list = prospects.get("prospects", []) if isinstance(prospects, dict) else []
    growth_stats = defaultdict(int)
    for p in p_list:
        growth_stats[p.get("outreach_status", "new")] += 1

    return {
        "price_checks": {
            "total": len(pcs) if isinstance(pcs, dict) else 0,
            **dict(pc_by_status),
        },
        "quotes": {
            "total": len(live_quotes),
            "by_status": dict(q_by_status),
            "pipeline_value": round(pipeline_value, 2),
            "won_total": round(total_revenue, 2),
            "win_rate": round(q_by_status.get("won", 0) /
                              max(q_by_status.get("won", 0) + q_by_status.get("lost", 0), 1) * 100),
        },
        "leads": {
            "total": len(leads),
            "new": sum(1 for l in leads if l.get("status") == "new"),
        },
        "orders": {
            "total": len(live_orders),
            "active": sum(1 for o in live_orders.values() if o.get("status") not in ("closed",)),
        },
        "outbox": {
            "drafts": sum(1 for e in outbox if e.get("status") == "draft"),
            "approved": sum(1 for e in outbox if e.get("status") == "approved"),
        },
        "growth": {
            "total_prospects": len(p_list),
            "by_status": dict(growth_stats),
        },
    }


# ─── Manager Brief (main output) ──────────────────────────────────────────

def generate_brief() -> dict:
    """Generate the full manager brief. Everything in one glance.
    Now uses agent_context for live DB data (Skills Guide Pattern 5).
    """
    approvals = _get_pending_approvals()
    activity = _get_activity_feed(limit=10)
    summary = _get_pipeline_summary()
    agents = _check_all_agents()
    revenue = _get_revenue_status()

    # ── Pull live DB context (agent intelligence layer) ────────────────────
    db_ctx = {}
    if HAS_AGENT_CTX:
        try:
            db_ctx = get_context(include_contacts=True, include_quotes=True, include_revenue=True)
        except Exception:
            pass

    # Build headline
    headlines = []
    if approvals:
        headlines.append(f"{len(approvals)} item{'s' if len(approvals)!=1 else ''} need{'s' if len(approvals)==1 else ''} your attention")
    if summary["outbox"]["drafts"] > 0:
        headlines.append(f"{summary['outbox']['drafts']} email draft{'s' if summary['outbox']['drafts']!=1 else ''} awaiting review")
    if summary["leads"]["new"] > 0:
        headlines.append(f"{summary['leads']['new']} new lead{'s' if summary['leads']['new']!=1 else ''}")
    if summary.get("growth", {}).get("by_status", {}).get("follow_up_due", 0) > 0:
        n = summary["growth"]["by_status"]["follow_up_due"]
        headlines.append(f"{n} growth prospect{'s' if n!=1 else ''} ready for follow-up call")

    # DB-context headlines
    qt = db_ctx.get("quotes", {})
    if qt.get("sent", 0) > 0:
        headlines.append(f"{qt['sent']} quote{'s' if qt['sent']!=1 else ''} sent — awaiting PO")
    new_contacts = sum(1 for c in db_ctx.get("contacts", []) if c.get("status") == "new")
    if new_contacts > 5:
        headlines.append(f"{new_contacts} new contacts never contacted — run distro campaign")

    if not headlines:
        closed = revenue.get("closed_revenue", 0)
        if closed > 0:
            headlines.append(f"Pipeline clear. ${closed:,.0f} closed toward $2M goal")
        else:
            headlines.append("Pipeline clear. Upload a PC or run Growth Engine to get started.")

    # Agent health summary
    agents_ok = sum(1 for a in agents if a["status"] in ("active", "ready", "connected"))
    agents_down = sum(1 for a in agents if a["status"] in ("unavailable", "error"))
    agents_config = sum(1 for a in agents if a["status"] == "not configured")

    # Revenue snapshot (merge DB context if available)
    rev_db = db_ctx.get("revenue", {})
    rev_snapshot = {
        "closed": rev_db.get("closed") or revenue.get("closed_revenue", 0),
        "goal": rev_db.get("goal") or revenue.get("goal", 2000000),
        "pct": rev_db.get("pct") or revenue.get("pct_to_goal", 0),
        "gap": rev_db.get("gap") or revenue.get("gap_to_goal", 0),
        "on_track": rev_db.get("on_track") or revenue.get("on_track", False),
        "run_rate": rev_db.get("run_rate_annual") or revenue.get("run_rate_annual", 0),
        "monthly_needed": rev_db.get("monthly_needed") or revenue.get("monthly_needed", 181818),
    }

    # Growth campaign status
    growth_campaign = {}
    try:
        import json, os
        from src.core.paths import DATA_DIR
        outreach_path = os.path.join(DATA_DIR, "growth_outreach.json")
        if os.path.exists(outreach_path):
            with open(outreach_path) as f:
                od = json.load(f)
            if isinstance(od, dict):
                campaigns = od.get("campaigns", [])
                distro = [c for c in campaigns if c.get("type") == "distro_list_phase1"]
                growth_campaign = {
                    "distro_campaigns": len(distro),
                    "total_sent": od.get("total_sent", 0),
                    "last_campaign": distro[-1]["id"] if distro else None,
                }
    except Exception:
        pass

    return {
        "ok": True,
        "generated_at": datetime.now().isoformat(),
        "headline": headlines[0] if headlines else "All clear",
        "headlines": headlines,
        "pending_approvals": approvals,
        "approval_count": len(approvals),
        "activity": activity,
        "summary": summary,
        "agents": agents,
        "agents_summary": {
            "total": len(agents),
            "healthy": agents_ok,
            "down": agents_down,
            "needs_config": agents_config,
        },
        "revenue": rev_snapshot,
        "growth_campaign": growth_campaign,
        "db_context": {
            "contacts": len(db_ctx.get("contacts", [])),
            "pipeline_value": qt.get("pipeline_value", 0),
            "win_rate": qt.get("win_rate", 0),
            "intel_buyers": db_ctx.get("intel", {}).get("total_buyers", 0),
        },
    }



def get_agent_status() -> dict:
    return {
        "agent": "manager",
        "version": "2.0.0",
        "status": "active",
        "brief_available": True,
        "checks": [
            "Pipeline (quotes, PCs, orders)",
            "Growth Engine (prospects, follow-ups, bounced)",
            "Sales Intel (buyers, agencies, revenue goal)",
            "Voice Agent (scripts, config)",
            "QA Health Monitor (score, grade)",
            "QuickBooks (connection status)",
            "Email Outbox (drafts, approved)",
            "Agent health (8 agents monitored)",
        ],
    }
