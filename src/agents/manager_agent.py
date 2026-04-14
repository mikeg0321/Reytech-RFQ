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
# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_vendors, get_all_price_checks, get_price_check,
        upsert_price_check, get_outbox, upsert_outbox_email, update_outbox_status,
        get_email_templates, upsert_email_template, get_vendor_registrations,
        upsert_vendor_registration, get_market_intelligence, upsert_market_intelligence,
        get_intel_agencies, upsert_intel_agency, get_growth_outreach, save_growth_campaign,
        get_qa_reports, save_qa_report, get_latest_qa_report,
        upsert_customer, upsert_vendor,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────

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
    outbox = get_outbox() if _HAS_DB_DAL else _load_json("email_outbox.json", [])
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
    try:
        from src.core.dal import get_all_leads
        leads = get_all_leads()
    except Exception:
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

    # 3b. Pending / new RFQs needing action
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        with open(rfqs_path) as _f:
            _rfqs = json.load(_f)
        actionable_rfqs = [r for r in (_rfqs.values() if isinstance(_rfqs, dict) else [])
                           if (r.get("status") or "").lower() in ("new", "pending", "auto_drafted")]
        if actionable_rfqs:
            # Sort by due date if available
            actionable_rfqs.sort(key=lambda x: x.get("due_date", "9999"), reverse=False)
            for rfq in actionable_rfqs:
                sol = rfq.get("solicitation_number", "?")
                req = rfq.get("requestor_name", rfq.get("requestor_email", "?"))
                status = rfq.get("status", "new")
                due = rfq.get("due_date", "TBD")
                items = len(rfq.get("line_items", []))
                action_label = "Fill Out & Send" if status == "auto_drafted" else "Review & Price"
                approvals.append({
                    "type": "rfq_pending", "icon": "📬",
                    "title": f"RFQ #{sol} — {status.upper()}",
                    "detail": f"{req} · {items} item{'s' if items != 1 else ''} · Due {due}",
                    "age": _age_str(rfq.get("created_at", "")),
                    "action_url": f"/rfq/{rfq.get('id', '')}",
                    "action_label": action_label,
                })
    except (FileNotFoundError, Exception) as _e:
        log.debug("suppressed: %s", _e)

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
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # 7. Pending price checks (parsed, no quote yet) — these are real work items
    try:
        pcs_src = get_all_price_checks(include_test=False) if _HAS_DB_DAL else {}
        # Fall back to JSON if DB is empty (DB may not be migrated yet or PC table empty)
        if not pcs_src:
            pcs_src = _load_json("price_checks.json", {})
        if isinstance(pcs_src, dict):
            # Use canonical filter — only show standalone PCs, not auto-price PCs from RFQs
            try:
                from src.api.dashboard import _is_user_facing_pc
                _pc_filter = _is_user_facing_pc
            except ImportError:
                _pc_filter = lambda pc: pc.get("source") not in ("email_auto_draft", "email_auto") and not pc.get("rfq_id")
            pending_pcs = [
                (k, v) for k, v in pcs_src.items()
                if v.get("status") in ("parsed", "new")
                and _pc_filter(v)
                and not v.get("is_test")
            ]
            # Sort by due date
            pending_pcs.sort(key=lambda x: x[1].get("due_date", "9999"))
            for pc_id, pc in pending_pcs[:8]:
                due = pc.get("due_date", "")
                pc_num = pc.get("pc_number") or pc.get("solicitation_number") or pc_id[:10]
                inst = pc.get("institution") or pc.get("agency") or "Unknown"
                requestor = pc.get("requestor") or pc.get("requestor_email") or ""
                items_n = pc.get("total_items", len(pc.get("items", [])) if isinstance(pc.get("items"), list) else 0)
                approvals.append({
                    "type": "pc_pending", "icon": "📋",
                    "title": f"Price Check #{pc_num} — needs a quote",
                    "detail": f"{inst}{' · ' + requestor if requestor else ''}{' · ' + str(items_n) + ' item' + ('s' if items_n != 1 else '') if items_n else ''}{' · Due ' + due if due else ''}",
                    "age": _age_str(pc.get("created_at", "")),
                    "action_url": f"/pricecheck/{pc_id}",
                    "action_label": "Price & Quote",
                })
    except Exception as _e:
        log.debug("PC approvals failed: %s", _e)

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

    # PC arrivals + status changes
    try:
        pcs = get_all_price_checks(include_test=False) if _HAS_DB_DAL else _load_json("price_checks.json", {})
        for pcid, pc in (pcs.items() if isinstance(pcs, dict) else []):
            if pc.get("is_test"):
                continue
            ts_created = pc.get("created_at", "")
            if ts_created:
                pc_num = pc.get("pc_number") or pcid[:8]
                events.append({
                    "icon": "📥", "text": f"Price Check #{pc_num} received",
                    "detail": pc.get("institution") or pc.get("requestor") or "",
                    "timestamp": ts_created, "age": _age_str(ts_created),
                })
            for h in (pc.get("status_history", []) or [])[-1:]:
                events.append({
                    "icon": "📄", "text": f"PC #{pc.get('pc_number', pcid[:8])} → {h.get('to', '?')}",
                    "detail": pc.get("institution", ""),
                    "timestamp": h.get("timestamp", ""), "age": _age_str(h.get("timestamp", "")),
                })
    except Exception as _e:
        log.debug("PC activity events failed: %s", _e)

    # RFQ arrivals
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        with open(rfqs_path) as _rf:
            _rfqs = json.load(_rf)
        for _r in (_rfqs.values() if isinstance(_rfqs, dict) else []):
            ts_r = _r.get("created_at", _r.get("parsed_at", ""))
            sol = _r.get("solicitation_number", "?")
            req = _r.get("requestor_name", _r.get("requestor_email", ""))
            status = _r.get("status", "new")
            icon_map = {"new": "📬", "auto_drafted": "🤖", "sent": "📤", "pending": "⏳"}
            events.append({
                "icon": icon_map.get(status, "📋"),
                "text": f"RFQ #{sol} — {status}",
                "detail": req,
                "timestamp": ts_r, "age": _age_str(ts_r),
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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
    outreach = get_growth_outreach() if _HAS_DB_DAL else _load_json("growth_outreach.json", {})
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
    pcs = get_all_price_checks(include_test=True) if _HAS_DB_DAL else {}
    # Fall back to JSON if DB is empty (PCs are stored in JSON, not DB)
    if not pcs:
        pcs = _load_json("price_checks.json", {})
    # Only count user-facing PCs (not auto-price PCs from RFQ imports)
    try:
        from src.api.dashboard import _is_user_facing_pc
        _pc_filter = _is_user_facing_pc
    except ImportError:
        _pc_filter = lambda pc: pc.get("source") not in ("email_auto_draft", "email_auto") and not pc.get("rfq_id")
    user_pcs = {k: v for k, v in (pcs.items() if isinstance(pcs, dict) else {}.items()) if _pc_filter(v)}
    quotes = _load_json("quotes_log.json", [])
    live_quotes = [q for q in quotes if not q.get("is_test")]
    try:
        from src.core.dal import get_all_leads
        leads = get_all_leads()
    except Exception:
        leads = _load_json("leads.json", [])
    try:
        from src.core.dal import get_outbox as dal_outbox
        outbox = dal_outbox()
    except Exception:
        outbox = get_outbox() if _HAS_DB_DAL else _load_json("email_outbox.json", [])
    try:
        from src.core.dal import list_orders as _dal_list_orders
        _order_list = _dal_list_orders(limit=5000)
        orders = {o.get("id", o.get("order_id", "")): o for o in _order_list}
    except Exception:
        orders = {}
    live_orders = {k: v for k, v in (orders.items() if isinstance(orders, dict) else [])}

    pc_by_status = defaultdict(int)
    for pc in (user_pcs.values() if isinstance(user_pcs, dict) else []):
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

    # RFQs — live from rfqs.json
    rfq_by_status = defaultdict(int)
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        with open(rfqs_path) as _f:
            _rfqs = json.load(_f)
        for _r in (_rfqs.values() if isinstance(_rfqs, dict) else []):
            rfq_by_status[_r.get("status", "unknown")] += 1
    except (FileNotFoundError, Exception) as _e:
        log.debug("suppressed: %s", _e)

    return {
        "price_checks": {
            "total": len(user_pcs) if isinstance(user_pcs, dict) else 0,
            **dict(pc_by_status),
        },
        "rfqs": {
            "total": sum(rfq_by_status.values()),
            "new": rfq_by_status.get("new", 0),
            "pending": rfq_by_status.get("pending", 0),
            "auto_drafted": rfq_by_status.get("auto_drafted", 0),
            "ready": rfq_by_status.get("ready", 0),
            "generated": rfq_by_status.get("generated", 0),
            "by_status": dict(rfq_by_status),
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
            "total_value": round(sum(o.get("total", 0) for o in live_orders.values()), 2),
            "items_pending": sum(1 for o in live_orders.values() for it in o.get("line_items", []) if it.get("sourcing_status") == "pending"),
            "items_ordered": sum(1 for o in live_orders.values() for it in o.get("line_items", []) if it.get("sourcing_status") == "ordered"),
            "items_shipped": sum(1 for o in live_orders.values() for it in o.get("line_items", []) if it.get("sourcing_status") == "shipped"),
            "items_delivered": sum(1 for o in live_orders.values() for it in o.get("line_items", []) if it.get("sourcing_status") == "delivered"),
            "ready_to_invoice": sum(1 for o in live_orders.values() if o.get("status") == "delivered"),
            "has_draft_invoice": sum(1 for o in live_orders.values() if o.get("draft_invoice")),
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



def get_scprs_brief_section() -> dict:
    """
    Pull SCPRS intelligence into manager brief.
    Returns: {recommendations, gap_total, win_back_total, auto_closed, data_fresh}
    """
    try:
        from src.agents.growth_agent import get_scprs_growth_intelligence
        intel = get_scprs_growth_intelligence()
        if not intel.get("ok"):
            return {"available": False}
        recs = intel.get("recommendations", [])[:5]
        summary = intel.get("summary", {})
        losses = intel.get("recent_losses", [])
        return {
            "available": True,
            "recommendations": recs,
            "gap_opportunity": summary.get("total_gap_opportunity", 0),
            "win_back_opportunity": summary.get("total_win_back", 0),
            "total_opportunity": summary.get("total_opportunity", 0),
            "agencies_with_data": summary.get("agencies_with_data", 0),
            "recent_losses": losses[:3],
            "top_action": recs[0] if recs else None,
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def _get_auto_closed_count() -> int:
    """How many quotes auto-closed-lost by PO monitor today."""
    try:
        import sqlite3
        from src.core.paths import DATA_DIR
        import os
        db = os.path.join(DATA_DIR, "reytech.db")
        conn = sqlite3.connect(db, timeout=15)
        today = __import__('datetime').date.today().isoformat()
        n = conn.execute(
            "SELECT COUNT(*) FROM quote_po_matches WHERE auto_closed=1 AND matched_at LIKE ?",
            (f"{today}%",)
        ).fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0

def generate_brief() -> dict:
    """Generate the full manager brief. Everything in one glance.
    Each sub-call is individually guarded — this function NEVER throws.
    """
    try: approvals = _get_pending_approvals()
    except Exception as _e: log.warning("_get_pending_approvals failed: %s", _e); approvals = []
    try: activity = _get_activity_feed(limit=10)
    except Exception as _e: log.warning("_get_activity_feed failed: %s", _e); activity = []
    try: summary = _get_pipeline_summary()
    except Exception as _e: log.warning("_get_pipeline_summary failed: %s", _e); summary = {"price_checks": {}, "rfqs": {}, "quotes": {}, "leads": {}, "orders": {}, "outbox": {}, "growth": {}}
    try: agents = _check_all_agents()
    except Exception as _e: log.warning("_check_all_agents failed: %s", _e); agents = []
    try: revenue = _get_revenue_status()
    except Exception as _e: log.warning("_get_revenue_status failed: %s", _e); revenue = {"closed_revenue": 0, "goal": 2000000, "pct_to_goal": 0, "gap_to_goal": 2000000}

    # ── Pull live DB context (agent intelligence layer) ────────────────────
    db_ctx = {}
    if HAS_AGENT_CTX:
        try:
            db_ctx = get_context(include_contacts=True, include_quotes=True, include_revenue=True)
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    # Build headline — use .get() everywhere so a partial summary never throws
    headlines = []
    _ob = summary.get("outbox", {}) or {}
    _leads = summary.get("leads", {}) or {}
    _gr = summary.get("growth", {}) or {}
    if approvals:
        headlines.append(f"{len(approvals)} item{'s' if len(approvals)!=1 else ''} need{'s' if len(approvals)==1 else ''} your attention")
    if _ob.get("drafts", 0) > 0:
        _d = _ob["drafts"]
        headlines.append(f"{_d} email draft{'s' if _d!=1 else ''} awaiting review")
    if _leads.get("new", 0) > 0:
        _n = _leads["new"]
        headlines.append(f"{_n} new lead{'s' if _n!=1 else ''}")
    if _gr.get("by_status", {}).get("follow_up_due", 0) > 0:
        n = _gr["by_status"]["follow_up_due"]
        headlines.append(f"{n} growth prospect{'s' if n!=1 else ''} ready for follow-up call")

    # RFQ headlines — highest priority (pending RFQs need filling out)
    rfq_summary = summary.get("rfqs", {})
    rfq_action_count = rfq_summary.get("new", 0) + rfq_summary.get("pending", 0) + rfq_summary.get("auto_drafted", 0)
    if rfq_action_count > 0:
        headlines.insert(0, f"{rfq_action_count} RFQ{'s' if rfq_action_count != 1 else ''} waiting — fill out 704A/704B and send formal quote")

    # DB-context headlines
    qt = db_ctx.get("quotes", {})
    if qt.get("sent", 0) > 0:
        headlines.append(f"{qt['sent']} quote{'s' if qt['sent']!=1 else ''} sent — awaiting PO")
    new_contacts = sum(1 for c in db_ctx.get("contacts", []) if c.get("status") == "new")
    if new_contacts > 5:
        headlines.append(f"{new_contacts} new contacts never contacted — run distro campaign")

    # Order fulfillment headlines
    _ord = summary.get("orders", {}) or {}
    if _ord.get("items_pending", 0) > 0:
        headlines.append(f"{_ord['items_pending']} line item{'s' if _ord['items_pending']!=1 else ''} not yet ordered — source from suppliers")
    if _ord.get("items_shipped", 0) > 0:
        headlines.append(f"{_ord['items_shipped']} item{'s' if _ord['items_shipped']!=1 else ''} in transit")
    if _ord.get("ready_to_invoice", 0) > 0:
        headlines.append(f"{_ord['ready_to_invoice']} order{'s' if _ord['ready_to_invoice']!=1 else ''} delivered — invoice ready")

    # ── PRD-28 enhancements ─────────────────────────────────────────────────
    try:
        from src.agents.quote_lifecycle import get_expiring_soon, get_pipeline_summary as _ql_pipe
        _exp = get_expiring_soon(3)
        if _exp:
            headlines.append(f"⚠️ {len(_exp)} quote{'s' if len(_exp)!=1 else ''} expiring in < 3 days")
        _pipe = _ql_pipe()
        if _pipe.get("ok"):
            _conv = _pipe.get("conversion_rate", 0)
            if _conv > 0:
                headlines.append(f"Quote conversion rate: {_conv}%")
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    try:
        from src.agents.revenue_engine import get_goal_progress
        _goal = get_goal_progress()
        if _goal.get("ok") and _goal.get("ytd_revenue", 0) > 0:
            headlines.append(f"💰 Revenue: ${_goal['ytd_revenue']:,.0f} YTD ({_goal['pct_of_goal']:.1f}% of $2M goal)")
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    try:
        from src.agents.lead_nurture_agent import get_agent_status as _ln_st
        _ln = _ln_st()
        if _ln.get("active_nurtures", 0) > 0:
            headlines.append(f"🌱 {_ln['active_nurtures']} active lead nurture sequences")
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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

    # Revenue snapshot — fully guarded
    try:
        rev_db = db_ctx.get("revenue", {}) or {}
        rev_snapshot = {
            "closed": rev_db.get("closed") or revenue.get("closed_revenue", 0),
            "goal": rev_db.get("goal") or revenue.get("goal", 2000000),
            "pct": rev_db.get("pct") or revenue.get("pct_to_goal", 0),
            "gap": rev_db.get("gap") or revenue.get("gap_to_goal", 0),
            "on_track": rev_db.get("on_track") or revenue.get("on_track", False),
            "run_rate": rev_db.get("run_rate_annual") or revenue.get("run_rate_annual", 0),
            "monthly_needed": rev_db.get("monthly_needed") or revenue.get("monthly_needed", 181818),
        }
    except Exception as _e:
        log.warning("rev_snapshot failed: %s", _e)
        rev_snapshot = {"closed": 0, "goal": 2000000, "pct": 0, "gap": 2000000,
                        "on_track": False, "run_rate": 0, "monthly_needed": 181818}

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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # SCPRS intelligence section
    try: scprs_intel = get_scprs_brief_section()
    except Exception as _e: log.warning("get_scprs_brief_section failed: %s", _e); scprs_intel = {"available": False}
    try: auto_closed = _get_auto_closed_count()
    except Exception as _e: auto_closed = 0

    # Merge SCPRS signals into headlines
    if scprs_intel.get("available"):
        opp = scprs_intel.get("total_opportunity", 0)
        if opp > 0:
            headlines.append(f"${opp:,.0f} in SCPRS-identified opportunities — see Growth Intel")
        if scprs_intel.get("recent_losses"):
            l = scprs_intel["recent_losses"][0]
            delta = (l.get("total") or 0) - (l.get("scprs_total") or 0)
            if delta > 100:
                headlines.append(
                    f"Lost {l.get('quote_number','')} to {l.get('scprs_supplier','')} — "
                    f"we were ${delta:,.0f} too high. Reprice."
                )
    if auto_closed > 0:
        headlines.append(f"{auto_closed} quote{'s' if auto_closed!=1 else ''} auto-closed lost today via SCPRS monitor")

    return {
        "ok": True,
        "generated_at": datetime.now().isoformat(),
        "headline": headlines[0] if headlines else "All clear",
        "headlines": headlines,
        "scprs_intel": scprs_intel,
        "auto_closed_today": auto_closed,
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


# ════════════════════════════════════════════════════════════════════════════════
# INTELLIGENT RECOMMENDATIONS ENGINE (Phase 32)
# Pulls SCPRS intelligence → generates plain-english what-to-do-next
# ════════════════════════════════════════════════════════════════════════════════

def get_intelligent_recommendations() -> dict:
    """
    The 'what do I do next?' engine.
    Reads SCPRS intelligence + quote status + QB balances → generates
    ranked, plain-english action items with dollar amounts attached.
    """
    import sqlite3

    try:
        from src.core.paths import DATA_DIR
        import os
        db_path = os.path.join(DATA_DIR, "reytech.db")
        conn = sqlite3.connect(db_path, timeout=15)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        return {"ok": False, "error": str(e)}

    now = datetime.now()
    actions = []

    # ── SIGNAL 1: Outstanding AR (money owed to us NOW) ─────────────────────
    try:
        import json, os
        try:
            from src.core.dal import get_all_customers
            customers = get_all_customers()
        except Exception:
            customers = json.load(open(os.path.join(DATA_DIR, "customers.json")))
        ar_by_agency = {}
        for c in customers:
            bal = float(c.get("open_balance", 0) or 0)
            if bal > 0:
                agency = c.get("agency", "Unknown")
                ar_by_agency[agency] = ar_by_agency.get(agency, 0) + bal
        for agency, total in sorted(ar_by_agency.items(), key=lambda x: -x[1]):
            if total > 100:
                actions.append({
                    "priority": "P0",
                    "type": "collect_ar",
                    "signal": "outstanding_balance",
                    "agency": agency,
                    "title": f"Collect ${total:,.0f} AR from {agency}",
                    "why": f"{agency} owes ${total:,.0f}. Follow up before sending new quotes.",
                    "action": f"Email billing contact at {agency} with invoice summary",
                    "dollar_value": total,
                    "urgency": "THIS WEEK",
                })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 2: Open quotes past 14 days (should be won or lost) ──────────
    try:
        old_quotes = conn.execute("""
            SELECT quote_number, agency, total, created_at
            FROM quotes
            WHERE status IN ('sent','pending')
              AND is_test=0
              AND created_at < date('now', '-14 days')
              AND total > 0
            ORDER BY total DESC LIMIT 5
        """).fetchall()
        for q in old_quotes:
            q = dict(q)
            age = (now - datetime.fromisoformat(q["created_at"][:10])).days
            actions.append({
                "priority": "P0",
                "type": "follow_up_quote",
                "signal": "stale_quote",
                "agency": q.get("agency",""),
                "title": f"Follow up: Quote {q['quote_number']} ({age}d old, ${q.get('total',0):,.0f})",
                "why": f"Quote {q['quote_number']} to {q.get('agency','')} sent {age} days ago with no response. Either win it or close it.",
                "action": f"Call or email {q.get('agency','')} purchasing. Ask: 'Was our quote competitive? Did you award to another vendor?'",
                "dollar_value": q.get("total", 0),
                "urgency": "THIS WEEK",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 3: SCPRS gap items — products CCHCS/agencies buy, we don't sell ──
    try:
        gap_items = conn.execute("""
            SELECT l.description, l.category,
                   COUNT(DISTINCT p.dept_code) as agency_count,
                   COUNT(*) as times_ordered,
                   SUM(l.line_total) as total_spend,
                   AVG(l.unit_price) as avg_price,
                   GROUP_CONCAT(DISTINCT p.dept_name) as buying_agencies
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id=p.id
            WHERE l.opportunity_flag='GAP_ITEM' AND l.line_total > 100
            GROUP BY LOWER(l.description)
            HAVING total_spend > 500
            ORDER BY total_spend DESC LIMIT 10
        """).fetchall()
        for item in gap_items:
            item = dict(item)
            spend = item.get("total_spend", 0) or 0
            agencies = (item.get("buying_agencies") or "").split(",")[0]
            actions.append({
                "priority": "P1",
                "type": "add_product",
                "signal": "scprs_gap",
                "agency": agencies,
                "title": f"Add '{item['description'][:45]}' — ${spend:,.0f} visible spend",
                "why": f"{item.get('agency_count',1)} agencies buying this {item.get('times_ordered',0)}x. "
                       f"Avg price ${item.get('avg_price',0):.2f}. You're not in this product yet.",
                "action": f"Source from Cardinal/McKesson/Medline and add to catalog. "
                          f"Then quote {agencies} — they already buy this.",
                "dollar_value": spend,
                "urgency": "NEXT 30 DAYS",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 4: SCPRS win-back — we sell it, competitor is getting the PO ──
    try:
        win_back = conn.execute("""
            SELECT l.description,
                   p.supplier as their_vendor,
                   p.dept_name as agency,
                   SUM(l.line_total) as total_spend,
                   AVG(l.unit_price) as their_price
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id=p.id
            WHERE l.opportunity_flag='WIN_BACK' AND l.line_total > 100
            GROUP BY LOWER(l.description), p.supplier
            ORDER BY total_spend DESC LIMIT 8
        """).fetchall()
        for item in win_back:
            item = dict(item)
            spend = item.get("total_spend", 0) or 0
            actions.append({
                "priority": "P0",
                "type": "displace_competitor",
                "signal": "scprs_win_back",
                "agency": item.get("agency",""),
                "title": f"Displace {item.get('their_vendor','competitor')} on '{item['description'][:40]}'",
                "why": f"{item.get('agency','')} is buying this from {item.get('their_vendor','')} "
                       f"at ${item.get('their_price',0):.2f}. You already sell this. "
                       f"Beat their price by 3-5% and you win ${spend:,.0f}/yr.",
                "action": f"Quote {item.get('agency','')} on {item['description'][:40]} "
                          f"at ${(item.get('their_price',0) or 0)*0.96:.2f} (4% below their current price). "
                          f"Reference your SB/DVBE advantage.",
                "dollar_value": spend,
                "urgency": "THIS WEEK",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 5: Inactive CCHCS facilities (32 with $0 balance) ─────────────
    try:
        import json, os
        try:
            from src.core.dal import get_all_customers
            customers = get_all_customers()
        except Exception:
            customers = json.load(open(os.path.join(DATA_DIR, "customers.json")))
        inactive_cchcs = [c for c in customers
                          if c.get("agency") in ("CCHCS","CDCR")
                          and float(c.get("open_balance",0) or 0) == 0]
        if inactive_cchcs:
            actions.append({
                "priority": "P1",
                "type": "expand_existing_customer",
                "signal": "inactive_facilities",
                "agency": "CCHCS",
                "title": f"Activate {len(inactive_cchcs)} dormant CCHCS facilities",
                "why": f"You have {len(inactive_cchcs)} CCHCS facilities in QB with $0 balance — "
                       f"they're customers by name but haven't ordered. "
                       f"Email Timothy Anderson and ask for supply officer contacts at each facility.",
                "action": "Email timothy.anderson@cdcr.ca.gov: "
                          "'I see we have accounts set up for [list 5 facilities]. "
                          "Can you connect me with the supply officer at each? "
                          "I'd like to get them on contract for [nitrile gloves / chux].'",
                "dollar_value": len(inactive_cchcs) * 8000,
                "urgency": "THIS WEEK",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 6: Quotes auto-closed by SCPRS — pricing intel ────────────────
    try:
        lost_to_scprs = conn.execute("""
            SELECT quote_number, agency, status_notes, total
            FROM quotes
            WHERE is_test=0 AND status='closed_lost'
              AND status_notes LIKE 'SCPRS:%'
            ORDER BY updated_at DESC LIMIT 5
        """).fetchall()
        for q in lost_to_scprs:
            q = dict(q)
            actions.append({
                "priority": "P1",
                "type": "reprice_analysis",
                "signal": "auto_closed_lost",
                "agency": q.get("agency",""),
                "title": f"Pricing gap analysis: Quote {q['quote_number']} lost to SCPRS award",
                "why": q.get("status_notes",""),
                "action": "Run /pricecheck on these items using SCPRS price as target. "
                          "Adjust your supplier sourcing to hit 5% below the SCPRS award price.",
                "dollar_value": q.get("total", 0),
                "urgency": "THIS WEEK",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── SIGNAL 7: No SCPRS data pulled yet ────────────────────────────────────
    try:
        po_count = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
        if po_count == 0:
            actions.append({
                "priority": "P0",
                "type": "pull_data",
                "signal": "no_scprs_data",
                "agency": "ALL",
                "title": "Pull SCPRS data — intelligence layer is empty",
                "why": "No purchase order data has been pulled from SCPRS yet. "
                       "Without this, gap analysis, price intel, and auto-close are all blind.",
                "action": "Go to /intel/scprs → click 'Pull All Agencies Now'. "
                          "Takes 5-10 minutes. Runs in background. "
                          "After this, every signal above gets real data.",
                "dollar_value": 0,
                "urgency": "RIGHT NOW",
            })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    conn.close()

    # Sort: P0 first, then by dollar value
    priority_rank = {"P0": 0, "P1": 1, "P2": 2}
    actions.sort(key=lambda x: (priority_rank.get(x["priority"],9), -(x.get("dollar_value") or 0)))

    total_opp = sum(a.get("dollar_value",0) for a in actions if a["type"] != "collect_ar")
    ar_total = sum(a.get("dollar_value",0) for a in actions if a["type"] == "collect_ar")

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "action_count": len(actions),
        "actions": actions,
        "summary": {
            "outstanding_ar": ar_total,
            "revenue_opportunity": total_opp,
            "urgent_count": sum(1 for a in actions if a["urgency"] == "RIGHT NOW" or a["urgency"] == "THIS WEEK"),
            "next_action": actions[0]["title"] if actions else "No actions — pull SCPRS data to start",
            "next_action_why": actions[0]["why"] if actions else "",
        }
    }
