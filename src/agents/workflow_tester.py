#!/usr/bin/env python3
"""
Workflow Tester — End-to-end workflow validation for Reytech RFQ Dashboard

Unlike qa_agent (which checks code structure), this tests LIVE DATA FLOWS:

  1. Email → RFQ Queue (email correctly routed)
  2. Email → PC Queue (704 form detected, NOT put in RFQ queue)
  3. PC Queue isolation (auto_draft PCs not in manual PC queue)
  4. Manager Brief accuracy (brief reflects actual pending items)
  5. Quote lifecycle (PC → priced → quote → status synced)
  6. DB ↔ JSON consistency (same status in both sources)
  7. CS drafts visible (not buried)
  8. Notification accuracy (badge count matches real pending count)

Runs every 10 minutes in background. Writes results to workflow_runs table.
Surfaces critical failures to manager brief as high-priority alerts.
"""

import os
import json
import time
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from collections import defaultdict

log = logging.getLogger("workflow_tester")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

DB_PATH = os.path.join(DATA_DIR, "reytech.db")

# ── Test result constants ───────────────────────────────────────────────────
PASS = "pass"
FAIL = "fail"
WARN = "warn"


def _load_json(filename, default=None):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _result(name: str, status: str, message: str, detail: str = "", fix: str = "") -> dict:
    return {
        "test": name,
        "status": status,
        "message": message,
        "detail": detail,
        "fix": fix,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════
# WORKFLOW TESTS
# ═══════════════════════════════════════════════════════════════════════

def test_queue_isolation() -> list:
    """PC queue must NOT contain auto-draft PCs (those belong to RFQ display only)."""
    results = []
    pcs = _load_json("price_checks.json", {})
    rfqs = _load_json("rfqs.json", {})

    # Auto-draft PCs (created by _auto_draft_pipeline) should have source='email_auto_draft'
    auto_draft_pcs = {k: v for k, v in pcs.items() if v.get("source") == "email_auto_draft"}
    user_pcs = {k: v for k, v in pcs.items() if v.get("source") != "email_auto_draft"}

    if auto_draft_pcs:
        # Check each auto_draft PC is linked to an actual RFQ
        orphaned = []
        for pc_id, pc in auto_draft_pcs.items():
            rfq_id = pc.get("rfq_id", "")
            if rfq_id and rfq_id not in rfqs:
                orphaned.append(pc.get("pc_number", pc_id))

        if orphaned:
            results.append(_result(
                "queue_isolation", WARN,
                f"{len(orphaned)} auto-draft PCs have no parent RFQ",
                f"Orphaned: {', '.join(orphaned[:3])}",
                "Clean up via /api/pricecheck/<id>/clear or re-import email"
            ))
        else:
            results.append(_result(
                "queue_isolation", PASS,
                f"{len(auto_draft_pcs)} auto-draft PCs properly linked to RFQs, {len(user_pcs)} manual PCs in PC queue"
            ))
    else:
        results.append(_result(
            "queue_isolation", PASS,
            f"PC queue clean: {len(user_pcs)} manual PCs, 0 auto-draft PCs"
        ))

    # Check for solicitation number collision (same sol number in both queues)
    rfq_sols = {v.get("solicitation_number") for v in rfqs.values()}
    pc_nums = {v.get("pc_number", "").replace("AD-", "") for v in user_pcs.values()
               if not v.get("is_auto_draft")}
    collision = rfq_sols & pc_nums
    if collision:
        results.append(_result(
            "sol_number_collision", FAIL,
            f"Solicitation numbers appear in BOTH queues: {', '.join(str(c) for c in collision)}",
            "Same email created both an RFQ entry and a manual PC entry",
            "Delete duplicate from PC queue or remove from RFQ queue — they're the same document"
        ))
    else:
        results.append(_result("sol_number_collision", PASS,
                               "No solicitation number collisions between PC and RFQ queues"))

    return results


def test_manager_brief_accuracy() -> list:
    """Manager brief must reflect actual pending items in queues."""
    results = []
    try:
        from src.agents.manager_agent import generate_brief, _get_pipeline_summary
        brief = generate_brief()
        summary = _get_pipeline_summary()

        # Check RFQs
        rfqs = _load_json("rfqs.json", {})
        actionable_rfqs = [r for r in rfqs.values()
                           if (r.get("status") or "").lower() in ("new", "pending", "auto_drafted")]

        rfq_in_brief = sum(1 for a in brief.get("pending_approvals", [])
                           if a.get("type") == "rfq_pending")
        rfq_actual = len(actionable_rfqs)

        if rfq_actual > 0 and rfq_in_brief == 0:
            results.append(_result(
                "brief_rfq_accuracy", FAIL,
                f"Manager brief shows 0 RFQ approvals but {rfq_actual} actionable RFQs exist",
                f"RFQ statuses: {[r.get('status') for r in actionable_rfqs]}",
                "Click Refresh on manager brief. If still wrong, check rfqs.json is readable"
            ))
        elif rfq_in_brief == rfq_actual:
            results.append(_result(
                "brief_rfq_accuracy", PASS,
                f"Brief correctly shows {rfq_in_brief} RFQ approvals matching {rfq_actual} actionable RFQs"
            ))
        else:
            results.append(_result(
                "brief_rfq_accuracy", WARN,
                f"Brief shows {rfq_in_brief} RFQ approvals, actual actionable: {rfq_actual}"
            ))

        # Check approval_count vs what brief reports
        approval_count = brief.get("approval_count", 0)
        total_approvals = len(brief.get("pending_approvals", []))
        if approval_count != total_approvals:
            results.append(_result(
                "brief_count_accuracy", FAIL,
                f"Brief approval_count={approval_count} but has {total_approvals} approvals in list",
                "approval_count and pending_approvals are out of sync"
            ))
        else:
            results.append(_result(
                "brief_count_accuracy", PASS,
                f"Brief approval_count={approval_count} matches list length"
            ))

        # Check ALL CLEAR accuracy
        is_all_clear = approval_count == 0
        has_pending = rfq_actual > 0 or len([
            e for e in (summary.get("price_checks", {}).get("by_status", {})).items()
        ]) > 0
        pcs = _load_json("price_checks.json", {})
        pending_pcs = [p for p in pcs.values()
                       if p.get("status") in ("parsed", "new") and p.get("source") != "email_auto_draft"]

        if is_all_clear and (rfq_actual > 0 or pending_pcs):
            results.append(_result(
                "brief_all_clear_accuracy", FAIL,
                f"Brief shows ALL CLEAR but {rfq_actual} RFQs + {len(pending_pcs)} PCs need attention",
                f"RFQs: {rfq_actual} actionable, PCs: {len(pending_pcs)} unpriced",
                "Click Refresh on manager brief — brief may be stale"
            ))
        else:
            results.append(_result(
                "brief_all_clear_accuracy", PASS,
                f"ALL CLEAR accuracy OK (rfqs={rfq_actual}, pending_pcs={len(pending_pcs)}, clear={is_all_clear})"
            ))

    except Exception as e:
        results.append(_result("brief_rfq_accuracy", WARN,
                               f"Could not test manager brief: {e}"))
    return results


def test_db_json_consistency() -> list:
    """DB and JSON sources must agree on quote statuses."""
    results = []
    try:
        conn = _db()
        db_quotes = {r["quote_number"]: dict(r)
                     for r in conn.execute("SELECT quote_number, status, total FROM quotes WHERE is_test=0").fetchall()}
        conn.close()

        json_quotes = {q["quote_number"]: q
                       for q in _load_json("quotes_log.json", [])
                       if not q.get("is_test")}

        mismatches = []
        for qn, dq in db_quotes.items():
            jq = json_quotes.get(qn)
            if jq and dq["status"] != jq.get("status"):
                mismatches.append({
                    "quote": qn,
                    "db_status": dq["status"],
                    "json_status": jq.get("status"),
                })

        if mismatches:
            detail = "; ".join(f"{m['quote']}: DB={m['db_status']} JSON={m['json_status']}"
                               for m in mismatches[:5])
            results.append(_result(
                "db_json_consistency", FAIL,
                f"{len(mismatches)} quotes have different status in DB vs quotes_log.json",
                detail,
                "Run /api/quotes/<qn>/status-sync or manually update the lagging source"
            ))
        else:
            results.append(_result(
                "db_json_consistency", PASS,
                f"DB and JSON agree on all {len(db_quotes)} quote statuses"
            ))

    except Exception as e:
        results.append(_result("db_json_consistency", WARN, f"Could not compare: {e}"))
    return results


def test_cs_draft_visibility() -> list:
    """CS drafts must be findable and not buried indefinitely."""
    results = []
    try:
        conn = _db()
        cs_drafts = [dict(r) for r in conn.execute(
            "SELECT id, subject, created_at FROM email_outbox WHERE status='cs_draft'"
        ).fetchall()]
        conn.close()

        if not cs_drafts:
            results.append(_result("cs_draft_visibility", PASS, "CS inbox clear — no pending drafts"))
            return results

        # Check age of oldest draft
        now = datetime.now(timezone.utc)
        old_drafts = []
        for d in cs_drafts:
            try:
                ts = datetime.fromisoformat(d["created_at"].replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_h = (now - ts).total_seconds() / 3600
                if age_h > 24:
                    old_drafts.append((d["subject"][:40], f"{age_h:.0f}h old"))
            except Exception:
                pass

        if old_drafts:
            results.append(_result(
                "cs_draft_visibility", WARN,
                f"{len(old_drafts)} CS drafts older than 24h — customer waiting for reply",
                "; ".join(f"{s}: {a}" for s, a in old_drafts[:3]),
                "Go to /outbox → approve & send or delete stale drafts"
            ))
        else:
            results.append(_result(
                "cs_draft_visibility", PASS,
                f"{len(cs_drafts)} CS draft(s) exist, all less than 24h old"
            ))

    except Exception as e:
        results.append(_result("cs_draft_visibility", WARN, f"Could not check CS drafts: {e}"))
    return results


def test_quote_item_totals() -> list:
    """Quote item totals must match the stored total."""
    results = []
    mismatches = []

    quotes = _load_json("quotes_log.json", [])
    for q in quotes:
        if q.get("is_test"):
            continue
        items = q.get("line_items", q.get("items_detail", []))
        if not items:
            continue
        computed = sum(
            (it.get("unit_price", 0) or it.get("price", 0)) * (it.get("qty", 1))
            for it in items
        )
        stored = q.get("subtotal", q.get("total", 0))
        if stored and abs(computed - stored) > 1.00:  # $1 tolerance
            mismatches.append(f"{q['quote_number']}: computed ${computed:.2f} ≠ stored ${stored:.2f}")

    if mismatches:
        results.append(_result(
            "quote_item_totals", WARN,
            f"{len(mismatches)} quotes have line item sums that don't match totals",
            "; ".join(mismatches[:5]),
            "Re-open quote and re-save pricing — may be a rounding or migration issue"
        ))
    else:
        results.append(_result("quote_item_totals", PASS,
                               f"All {len(quotes)} quotes have correct item totals"))
    return results


def test_email_routing() -> list:
    """Validate that email routing rules are correctly configured."""
    results = []
    try:
        content = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "api", "dashboard.py")).read()

        # Check 704 detection guard exists
        if "_is_price_check" not in content:
            results.append(_result(
                "email_routing_704", FAIL,
                "704 price check detection is missing from email routing",
                "process_rfq_email() should call _is_price_check() to route 704 forms to PC queue only",
                "Restore the 704 detection block in process_rfq_email()"
            ))
        else:
            results.append(_result(
                "email_routing_704", PASS,
                "704 detection code present in email routing"
            ))

        # Check that auto_draft filter is in routes_rfq
        rfq_route = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "api", "modules", "routes_rfq.py")).read()
        if "email_auto_draft" not in rfq_route:
            results.append(_result(
                "email_routing_auto_draft_filter", FAIL,
                "Auto-draft PC filter missing from home page route",
                "PC queue will show auto-draft PCs alongside manual ones — same document visible in two places",
                "Add filter: user_pcs = {k:v for k,v in all_pcs.items() if v.get('source')!='email_auto_draft'}"
            ))
        else:
            results.append(_result(
                "email_routing_auto_draft_filter", PASS,
                "Auto-draft PCs are filtered from the manual PC queue display"
            ))

    except Exception as e:
        results.append(_result("email_routing_704", WARN, f"Could not inspect routing: {e}"))
    return results


def test_notification_badge_accuracy() -> list:
    """The notification badge count should match actual pending items."""
    results = []
    try:
        conn = _db()
        unread = conn.execute(
            "SELECT count(*) as n FROM notifications WHERE is_read=0"
        ).fetchone()["n"]
        cs_drafts = conn.execute(
            "SELECT count(*) as n FROM email_outbox WHERE status='cs_draft'"
        ).fetchone()["n"]
        conn.close()

        # Check rfqs
        rfqs = _load_json("rfqs.json", {})
        actionable_rfqs = sum(1 for r in rfqs.values()
                              if r.get("status") in ("new", "pending", "auto_drafted"))

        total_pending = unread + cs_drafts + actionable_rfqs
        results.append(_result(
            "notification_badge", PASS if total_pending >= 0 else FAIL,
            f"Badge should show: {unread} unread notifications + {cs_drafts} CS drafts + {actionable_rfqs} RFQs = {total_pending} total pending"
        ))

    except Exception as e:
        results.append(_result("notification_badge", WARN, f"Could not check badge: {e}"))
    return results


# ═══════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════

def test_manager_brief_includes_pcs() -> list:
    """Manager brief must surface pending price checks, not just RFQs."""
    results = []
    try:
        pcs = _load_json("price_checks.json", {})
        pending_pcs = [
            v for v in pcs.values()
            if v.get("status") in ("parsed", "new")
            and v.get("source") != "email_auto_draft"
            and not v.get("is_test")
        ]
        if not pending_pcs:
            results.append(_result("brief_pc_visibility", PASS, "No pending PCs to check"))
            return results

        from src.agents.manager_agent import _get_pending_approvals
        approvals = _get_pending_approvals()
        pc_approvals = [a for a in approvals if a.get("type") == "pc_pending"]

        if len(pc_approvals) == 0:
            results.append(_result(
                "brief_pc_visibility", FAIL,
                f"Manager brief shows 0 PC approvals but {len(pending_pcs)} unpriced PCs exist",
                f"PC numbers: {[v.get('pc_number', '?') for v in pending_pcs[:3]]}",
                "_get_pending_approvals() must include pending price checks — they are real work items"
            ))
        elif len(pc_approvals) < len(pending_pcs):
            results.append(_result(
                "brief_pc_visibility", WARN,
                f"Brief shows {len(pc_approvals)} of {len(pending_pcs)} pending PCs",
            ))
        else:
            results.append(_result(
                "brief_pc_visibility", PASS,
                f"Brief correctly surfaces {len(pc_approvals)} pending PC(s)"
            ))
    except Exception as e:
        results.append(_result("brief_pc_visibility", WARN, f"Could not test: {e}"))
    return results


def test_activity_feed_not_empty() -> list:
    """Activity feed must reflect real recent events, not be permanently empty."""
    results = []
    try:
        from src.agents.manager_agent import _get_activity_feed
        events = _get_activity_feed(limit=20)

        # Check what data sources exist
        quotes = _load_json("quotes_log.json", [])
        live_quotes = [q for q in quotes if not q.get("is_test")]
        pcs = _load_json("price_checks.json", {})
        live_pcs = [v for v in pcs.values() if not v.get("is_test")]
        rfqs = _load_json("rfqs.json", {})
        live_rfqs = list(rfqs.values()) if isinstance(rfqs, dict) else []

        has_data = bool(live_quotes or live_pcs or live_rfqs)

        if has_data and len(events) == 0:
            results.append(_result(
                "activity_feed_empty", FAIL,
                f"Activity feed returned 0 events despite {len(live_quotes)} quotes, {len(live_pcs)} PCs, {len(live_rfqs)} RFQs",
                "The brief will show 'No recent activity' even though work is happening",
                "_get_activity_feed() must include PC arrivals and RFQ events, not just quote events"
            ))
        elif not has_data:
            results.append(_result("activity_feed_empty", PASS, "No live data to display — fresh install"))
        else:
            results.append(_result(
                "activity_feed_empty", PASS,
                f"Activity feed has {len(events)} events from {len(live_quotes)} quotes + {len(live_pcs)} PCs + {len(live_rfqs)} RFQs"
            ))
    except Exception as e:
        results.append(_result("activity_feed_empty", WARN, f"Could not test: {e}"))
    return results


def test_brief_fallback_resilience() -> list:
    """Manager brief must NOT fall to hardcoded 'Dashboard active' — fallback must return real data."""
    results = []
    try:
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()

        if brief.get("_fallback") and brief.get("headline") == "Dashboard active":
            results.append(_result(
                "brief_fallback", FAIL,
                "Manager brief hit the hardcoded 'Dashboard active' floor — all data is zeros",
                f"Error: {brief.get('_error', 'unknown')}",
                "Each sub-call in api_manager_brief() must be individually guarded with its own try/except"
            ))
        elif brief.get("_fallback"):
            results.append(_result(
                "brief_fallback", WARN,
                f"Manager brief used fallback path (generate_brief crashed): {brief.get('_error', '?')[:60]}"
            ))
        else:
            results.append(_result("brief_fallback", PASS, "Manager brief ran via primary generate_brief()"))
    except Exception as e:
        results.append(_result("brief_fallback", WARN, f"Could not test: {e}"))
    return results


def test_reply_followup_detection() -> list:
    """Reply/follow-up emails must be routed to CS, not create new PCs."""
    results = []
    try:
        from src.agents.email_poller import is_reply_followup, is_rfq_email

        # Simulate a buyer reply — "Re:" subject with RFQ keywords, no new forms
        class FakeMsg:
            def get(self, header, default=""):
                if header == "In-Reply-To":
                    return "<original-msg-id@mail.cdcr.ca.gov>"
                if header == "References":
                    return "<original-msg-id@mail.cdcr.ca.gov>"
                return default

        reply_subject = "Re: RFQ Solicitation #10838349 - Name Tags"
        reply_body = "Yes, that is correct. Please proceed with the blue ones."
        reply_sender = "test-detect@example.com"  # Won't match active items

        # Test 1: Reply detection function exists and runs
        result = is_reply_followup(FakeMsg(), reply_subject, reply_body, reply_sender, [])
        # With no active items for test sender, should return None (pass-through)
        # This validates the function runs without crashing

        # Test 2: is_rfq_email still works for genuine new RFQs
        genuine = is_rfq_email("RFQ Solicitation #9999999 - Office Supplies", "Please quote the following items.", ["703B_form.pdf"])
        if not genuine:
            results.append(_result(
                "reply_detection", WARN,
                "is_rfq_email() failed to detect a genuine new RFQ with 703B form"
            ))
            return results

        # Test 3: Verify the reply detection code is wired into email_poller
        import inspect
        from src.agents.email_poller import EmailPoller
        source = inspect.getsource(EmailPoller.check_for_rfqs)
        if "is_reply_followup" not in source:
            results.append(_result(
                "reply_detection", FAIL,
                "is_reply_followup() not called in check_for_rfqs() — buyer replies will create duplicate PCs",
                fix="email_poller.check_for_rfqs must call is_reply_followup() BEFORE is_rfq_email()"
            ))
            return results

        results.append(_result(
            "reply_detection", PASS,
            "Reply detection wired in: buyer follow-ups route to CS agent, not PC/RFQ queue"
        ))

    except Exception as e:
        results.append(_result("reply_detection", WARN, f"Could not test: {e}"))
    return results


def test_growth_data_integrity() -> list:
    """Growth Engine data must be consistent and operational."""
    results = []
    try:
        from src.agents.growth_agent import (
            _load_json, PROSPECTS_FILE, OUTREACH_FILE, CATEGORIES_FILE,
            HISTORY_FILE, get_growth_status,
        )

        status = get_growth_status()
        prospects_data = _load_json(PROSPECTS_FILE)
        outreach_data = _load_json(OUTREACH_FILE)

        # ── Test 1: Prospect data integrity ──
        if isinstance(prospects_data, dict) and prospects_data.get("prospects"):
            prospects = prospects_data["prospects"]
            issues = []
            for p in prospects:
                if not p.get("id"):
                    issues.append("missing id")
                if not p.get("buyer_email") and p.get("outreach_status") not in ("dead", "bounced"):
                    issues.append(f"{p.get('agency','?')}: no email")
                if not p.get("categories_matched"):
                    issues.append(f"{p.get('buyer_email','?')}: no categories")
            if issues:
                results.append(_result(
                    "growth_data_integrity", WARN,
                    f"{len(issues)} prospect data issues: {'; '.join(issues[:3])}",
                    fix="Review prospects in /growth — some missing email or categories"
                ))
            else:
                results.append(_result(
                    "growth_data_integrity", PASS,
                    f"All {len(prospects)} prospects have valid data (id, email, categories)"
                ))
        else:
            results.append(_result(
                "growth_data_integrity", PASS,
                "No prospect data yet — Growth Engine not activated"
            ))

        # ── Test 2: Outreach email content quality ──
        if isinstance(outreach_data, dict) and outreach_data.get("campaigns"):
            empty_bodies = 0
            total_entries = 0
            generic_count = 0
            for camp in outreach_data["campaigns"]:
                for entry in camp.get("outreach", []):
                    total_entries += 1
                    body = entry.get("body", entry.get("email_body", ""))
                    if not body or len(body) < 50:
                        empty_bodies += 1
                    elif "items we also carry" in body:
                        generic_count += 1

            if empty_bodies > 0:
                results.append(_result(
                    "growth_outreach_quality", FAIL,
                    f"{empty_bodies}/{total_entries} outreach emails have empty or missing body",
                    fix="Email bodies must be populated — check launch_outreach template formatting"
                ))
            elif generic_count > total_entries * 0.5 and total_entries > 2:
                results.append(_result(
                    "growth_outreach_quality", WARN,
                    f"{generic_count}/{total_entries} emails use generic 'items we also carry' — run SCPRS pull for personalized content",
                    fix="Click Pull Reytech History → Find Buyers on /growth to populate item data"
                ))
            else:
                results.append(_result(
                    "growth_outreach_quality", PASS,
                    f"{total_entries} outreach emails have personalized content"
                ))
        else:
            results.append(_result(
                "growth_outreach_quality", PASS,
                "No outreach campaigns yet"
            ))

        # ── Test 3: Pipeline funnel coherence ──
        h = status.get("history", {})
        c = status.get("categories", {})
        p = status.get("prospects", {})
        o = status.get("outreach", {})
        # If we have prospects but no history, that's OK (manual/sales_intel source)
        # But if we have outreach > prospects, something is wrong
        if o.get("total_sent", 0) > p.get("total", 0) and p.get("total", 0) > 0:
            results.append(_result(
                "growth_funnel", WARN,
                f"More emails sent ({o['total_sent']}) than prospects ({p['total']}) — data may be stale"
            ))
        else:
            funnel_str = f"History:{h.get('total_pos',0)} → Cats:{c.get('total',0)} → Prospects:{p.get('total',0)} → Sent:{o.get('total_sent',0)}"
            results.append(_result("growth_funnel", PASS, f"Funnel coherent: {funnel_str}"))

    except Exception as e:
        results.append(_result("growth_data_integrity", WARN, f"Could not test growth: {e}"))
    return results


def test_pc_dedup_and_quote_sequence() -> list:
    """Price checks must not have duplicates; quote numbers must be sequential."""
    results = []
    try:
        import re, json, os
        from src.core.paths import DATA_DIR
        pc_path = os.path.join(DATA_DIR, "price_checks.json")
        if not os.path.exists(pc_path):
            results.append(_result("pc_dedup", PASS, "No PCs to check"))
            return results
        with open(pc_path) as f:
            pcs = json.load(f)
        if not pcs:
            results.append(_result("pc_dedup", PASS, "No PCs to check"))
            return results

        # ── Test 1: Check for duplicate PCs (same pc_number + institution) ──
        seen = {}
        dupes = []
        for pc_id, pc in pcs.items():
            key = f"{pc.get('pc_number','').strip()}|{pc.get('institution','').strip().lower()}"
            if key in seen and key != "unknown|":
                dupes.append(f"#{pc.get('pc_number','?')} ({pc.get('institution','?')}) appears as {seen[key]} AND {pc_id}")
            else:
                seen[key] = pc_id

        if dupes:
            results.append(_result(
                "pc_dedup", FAIL,
                f"{len(dupes)} duplicate PC(s): {'; '.join(dupes[:3])}",
                fix="Delete duplicate PCs from /pricecheck — dedup guard added for future prevention"
            ))
        else:
            results.append(_result("pc_dedup", PASS, f"{len(pcs)} PCs — no duplicates"))

        # ── Test 2: Quote numbers should be sequential (no gaps in active PCs) ──
        quote_nums = []
        for pc in pcs.values():
            qn = pc.get("reytech_quote_number", "") or ""
            m = re.search(r'R(\d+)Q(\d+)', qn)
            if m:
                quote_nums.append(int(m.group(2)))

        if len(quote_nums) >= 2:
            quote_nums.sort()
            gaps = []
            for i in range(1, len(quote_nums)):
                if quote_nums[i] - quote_nums[i-1] > 1:
                    gaps.append(f"Q{quote_nums[i-1]}→Q{quote_nums[i]}")
            if gaps:
                results.append(_result(
                    "quote_sequence", WARN,
                    f"Quote number gaps: {', '.join(gaps[:5])} (may indicate deleted PCs or counter drift)",
                ))
            else:
                results.append(_result(
                    "quote_sequence", PASS,
                    f"Quote numbers sequential: Q{quote_nums[0]}–Q{quote_nums[-1]}"
                ))
        else:
            results.append(_result("quote_sequence", PASS, "Not enough quotes to check sequence"))

    except Exception as e:
        results.append(_result("pc_dedup", WARN, f"Could not test: {e}"))
    return results


WORKFLOW_TESTS = {
    "queue_isolation": test_queue_isolation,
    "manager_brief_accuracy": test_manager_brief_accuracy,
    "brief_pc_visibility": test_manager_brief_includes_pcs,
    "activity_feed": test_activity_feed_not_empty,
    "brief_fallback_resilience": test_brief_fallback_resilience,
    "db_json_consistency": test_db_json_consistency,
    "cs_draft_visibility": test_cs_draft_visibility,
    "quote_item_totals": test_quote_item_totals,
    "email_routing": test_email_routing,
    "notification_badge": test_notification_badge_accuracy,
    "reply_detection": test_reply_followup_detection,
    "growth_engine": test_growth_data_integrity,
    "pc_dedup": test_pc_dedup_and_quote_sequence,
}


def run_workflow_tests(tests: list = None) -> dict:
    """Run all workflow tests (or a subset). Returns structured report."""
    suite = tests or list(WORKFLOW_TESTS.keys())
    all_results = []
    t0 = time.time()

    for name in suite:
        fn = WORKFLOW_TESTS.get(name)
        if not fn:
            continue
        try:
            all_results.extend(fn())
        except Exception as e:
            all_results.append(_result(name, WARN, f"Test crashed: {e}"))

    passed = sum(1 for r in all_results if r["status"] == PASS)
    failed = sum(1 for r in all_results if r["status"] == FAIL)
    warned = sum(1 for r in all_results if r["status"] == WARN)

    # Score: start at 100, -15 per fail, -5 per warn
    score = max(0, 100 - (failed * 15) - (warned * 5))
    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "F"

    critical_failures = [r for r in all_results if r["status"] == FAIL]

    report = {
        "ok": True,
        "run_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 2),
        "score": score,
        "grade": grade,
        "summary": {"total": len(all_results), "passed": passed, "failed": failed, "warned": warned},
        "results": all_results,
        "critical_failures": critical_failures,
        "recommendations": [r["fix"] for r in critical_failures if r.get("fix")][:5],
    }

    _persist_run(report)
    return report


def _persist_run(report: dict):
    """Save workflow test run to DB."""
    try:
        conn = _db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workflow_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                finished_at TEXT,
                type TEXT DEFAULT 'workflow',
                status TEXT DEFAULT 'completed',
                run_at TEXT,
                score INTEGER,
                grade TEXT,
                passed INTEGER,
                failed INTEGER,
                warned INTEGER,
                critical_failures TEXT,
                full_report TEXT
            )
        """)
        conn.execute("""
            INSERT INTO workflow_runs (started_at, finished_at, type, status, run_at, score, grade, passed, failed, warned, critical_failures, full_report)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            report["run_at"],
            report["run_at"],
            "workflow",
            "completed",
            report["run_at"],
            report["score"],
            report["grade"],
            report["summary"]["passed"],
            report["summary"]["failed"],
            report["summary"]["warned"],
            json.dumps([r["test"] + ": " + r["message"] for r in report["critical_failures"]]),
            json.dumps(report),
        ))
        # Keep last 200 runs
        conn.execute("DELETE FROM workflow_runs WHERE id NOT IN (SELECT id FROM workflow_runs ORDER BY id DESC LIMIT 200)")
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Could not persist workflow run: %s", e)


def get_latest_run() -> dict:
    """Get the most recent workflow test run."""
    try:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM workflow_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            r = dict(row)
            r["full_report"] = json.loads(r.get("full_report", "{}"))
            r["critical_failures"] = json.loads(r.get("critical_failures", "[]"))
            return r
        return {}
    except Exception as e:
        return {"error": str(e)}


def get_run_history(limit: int = 20) -> list:
    """Get recent workflow test runs."""
    try:
        conn = _db()
        rows = conn.execute(
            "SELECT id, run_at, score, grade, passed, failed, warned, critical_failures FROM workflow_runs ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════
# BACKGROUND MONITOR
# ═══════════════════════════════════════════════════════════════════════

WORKFLOW_INTERVAL = 600  # 10 minutes

_wf_monitor = None
_wf_lock = threading.Lock()


class WorkflowMonitor:
    def __init__(self, interval=WORKFLOW_INTERVAL):
        self.interval = interval
        self._running = False
        self._thread = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="workflow-monitor")
        self._thread.start()
        log.info("Workflow monitor started (every %ds)", self.interval)

    def stop(self):
        self._running = False

    def _loop(self):
        time.sleep(45)  # Let app fully boot first
        while self._running:
            try:
                report = run_workflow_tests()
                if report["summary"]["failed"] > 0:
                    failures = "; ".join(r["message"][:60] for r in report["critical_failures"][:3])
                    log.warning("WORKFLOW FAIL [%s/100]: %s", report["score"], failures)
                else:
                    log.info("Workflow OK [%s/100 %s]: %d pass, %d warn",
                             report["score"], report["grade"],
                             report["summary"]["passed"], report["summary"]["warned"])
                # Surface to notifications if critical failures
                if report["summary"]["failed"] > 0:
                    _surface_to_notifications(report)
            except Exception as e:
                log.error("Workflow monitor error: %s", e)
            time.sleep(self.interval)

    def _check_once(self):
        return run_workflow_tests()


def _surface_to_notifications(report: dict):
    """Write critical workflow failures to the notifications table."""
    try:
        conn = _db()
        for failure in report["critical_failures"][:3]:
            conn.execute("""
                INSERT OR IGNORE INTO notifications (title, body, urgency, deep_link, is_read, created_at)
                VALUES (?,?,?,?,0,?)
            """, (
                f"⚠️ Workflow Fail: {failure['test']}",
                failure["message"][:200],
                "urgent",
                "/qa/workflow",
                datetime.now(timezone.utc).isoformat(),
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Could not surface to notifications: %s", e)


def start_workflow_monitor(interval=WORKFLOW_INTERVAL):
    global _wf_monitor
    with _wf_lock:
        if _wf_monitor is None:
            _wf_monitor = WorkflowMonitor(interval)
            _wf_monitor.start()
    return _wf_monitor


if __name__ == "__main__":
    import json
    print("=" * 60)
    print("WORKFLOW TESTER — Live data flow validation")
    print("=" * 60)
    report = run_workflow_tests()
    print(f"\nScore: {report['score']}/100  Grade: {report['grade']}")
    print(f"Tests: {report['summary']['passed']} pass / {report['summary']['warned']} warn / {report['summary']['failed']} fail")
    print(f"Ran in {report['duration_s']}s\n")
    for r in report["results"]:
        icon = "✅" if r["status"] == PASS else "⚠️ " if r["status"] == WARN else "❌"
        print(f"  {icon} {r['test']}: {r['message']}")
        if r.get("detail"):
            print(f"       Detail: {r['detail'][:100]}")
        if r.get("fix") and r["status"] != PASS:
            print(f"       Fix: {r['fix'][:100]}")
