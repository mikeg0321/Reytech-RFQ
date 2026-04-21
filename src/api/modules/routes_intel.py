# routes_intel.py

# ── Security middleware ──────────────────────────────────────────────────────
# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page
from markupsafe import escape as esc
import os


def _notify_wrapper(type_or_dict, title="", urgency="info"):
    """Adapter: scprs engine calls notify_fn("bell", "msg", "info")
    but _push_notification expects a dict. Handles both conventions."""
    if isinstance(type_or_dict, dict):
        _push_notification(type_or_dict)
    else:
        _push_notification({"type": type_or_dict, "title": title, "urgency": urgency})

try:
    from src.core.security import rate_limit, audit_action, _log_audit_internal
    _HAS_SECURITY = True
except ImportError:
    _HAS_SECURITY = False
    def rate_limit(tier="default"):
        def decorator(f): return f
        return decorator
    def audit_action(name):
        def decorator(f): return f
        return decorator

# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_price_checks, get_price_check, upsert_price_check,
        get_outbox, upsert_outbox_email, update_outbox_status, get_email_templates,
        get_market_intelligence, upsert_market_intelligence, get_intel_agencies,
        get_all_vendors, get_vendor_registrations, get_qa_reports, save_qa_report,
        get_growth_outreach, save_growth_campaign,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────
# 170 routes, 7596 lines
# Loaded by dashboard.py via load_module()

# GROWTH INTELLIGENCE — Full SCPRS pull + Gap analysis + Auto close-lost
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/scprs/pull-all", methods=["POST"])
@auth_required
@safe_route
def api_intel_pull_all():
    """Trigger full SCPRS pull for ALL agencies in background."""
    try:
        from src.agents.scprs_intelligence_engine import pull_all_agencies_background
        priority = (request.get_json(force=True, silent=True) or {}).get("priority", "P0")
        result = pull_all_agencies_background(notify_fn=_notify_wrapper, priority_filter=priority)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/engine-status")
@auth_required
@safe_route
def api_intel_engine_status():
    """Full SCPRS engine status — pull progress, record counts, schedule."""
    try:
        from src.agents.scprs_intelligence_engine import get_engine_status, _engine_status
        status = get_engine_status()
        # Add raw engine state for debugging
        status["_raw"] = {
            "running": _engine_status.get("running"),
            "current_agency": _engine_status.get("current_agency"),
            "last_results_keys": list(_engine_status.get("last_results", {}).keys()),
        }
        return jsonify({"ok": True, **status})
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/scprs/test-connection")
@auth_required
@safe_route
def api_scprs_test_connection():
    """Test if SCPRS/FI$Cal session can be established. Returns session status."""
    try:
        from src.agents.scprs_lookup import FiscalSession
        session = FiscalSession()
        init_ok = session.init_session()
        if not init_ok:
            return jsonify({"ok": False, "error": "SCPRS session init failed — FI$Cal may be down or blocking",
                           "hint": "Try again in a few minutes. FI$Cal sometimes rate-limits or has maintenance windows."})
        # Try a simple search
        results = session.search(description="glove", from_date="01/01/2025")
        return jsonify({
            "ok": True,
            "session": "established",
            "test_search": f"'glove' returned {len(results)} results",
            "results_sample": [{"po": r.get("po_number",""), "dept": r.get("dept",""), "total": r.get("grand_total","")} for r in results[:3]],
        })
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/scprs/po-monitor", methods=["POST"])
@auth_required
@safe_route
def api_intel_po_monitor():
    """Run PO award monitor — check open quotes against SCPRS, auto close-lost."""
    try:
        from src.agents.scprs_intelligence_engine import run_po_award_monitor
        result = run_po_award_monitor(notify_fn=_notify_wrapper)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Award Tracker (automated SCPRS polling) ──────────────────────────────────
# NOTE: /api/intel/award-tracker/status and /run are defined at end of file
# with enhanced schedule info. See "AWARD INTELLIGENCE" section below.

@bp.route("/api/intel/award-tracker/history")
@auth_required
@safe_route
def api_award_tracker_history():
    """Get full award check history and loss reports."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Recent checks
            checks = conn.execute("""
                SELECT * FROM award_tracker_log
                ORDER BY checked_at DESC LIMIT 50
            """).fetchall()

            # All matches (wins + losses)
            matches = conn.execute("""
                SELECT * FROM quote_po_matches
                ORDER BY matched_at DESC LIMIT 30
            """).fetchall()

        return jsonify({
            "ok": True,
            "checks": [dict(r) for r in checks],
            "matches": [dict(r) for r in matches],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── SCPRS Data Health Validator ──────────────────────────────────────────────

@bp.route("/api/intel/scprs-health")
@auth_required
@safe_route
def api_scprs_health():
    """Validate SCPRS data is being pulled and flowing into CRM/catalog/growth.
    Returns health status with specific issues and recommended actions."""
    try:
        from src.core.db import get_db
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        health = {"ok": True, "checks": [], "score": 100, "timestamp": now.isoformat()}

        with get_db() as conn:
            # ── 1. Pull Schedule Health ──
            try:
                schedule = conn.execute("""
                    SELECT agency_key, last_pull, next_pull, pull_interval_hours, enabled
                    FROM scprs_pull_schedule ORDER BY priority ASC
                """).fetchall()
                overdue = []
                never_pulled = []
                for s in schedule:
                    d = dict(s)
                    if not d.get("last_pull"):
                        never_pulled.append(d["agency_key"])
                    elif d.get("next_pull"):
                        try:
                            next_dt = datetime.fromisoformat(d["next_pull"].replace("Z","+00:00"))
                            if now > next_dt + timedelta(hours=6):
                                hours_late = (now - next_dt).total_seconds() / 3600
                                overdue.append({"agency": d["agency_key"], "hours_late": round(hours_late)})
                        except (ValueError, TypeError) as e:
                            log.debug("pull_schedule next_pull parse: %s", e)

                if not schedule:
                    health["checks"].append({"check": "pull_schedule", "status": "critical",
                        "message": "No SCPRS pull schedule configured — agencies never pulled",
                        "action": "Run /api/intel/scprs/pull to initialize"})
                    health["score"] -= 30
                elif never_pulled:
                    health["checks"].append({"check": "never_pulled", "status": "warning",
                        "message": f"{len(never_pulled)} agencies never pulled: {never_pulled}",
                        "action": "Trigger manual pull for these agencies"})
                    health["score"] -= 15
                elif overdue:
                    health["checks"].append({"check": "overdue_pulls", "status": "warning",
                        "message": f"{len(overdue)} agencies overdue for pull",
                        "details": overdue,
                        "action": "Check SCPRS session — may be blocked or rate-limited"})
                    health["score"] -= 10
                else:
                    health["checks"].append({"check": "pull_schedule", "status": "ok",
                        "message": f"{len(schedule)} agencies scheduled, all current"})
            except Exception as e:
                health["checks"].append({"check": "pull_schedule", "status": "error", "message": str(e)})
                health["score"] -= 20

            # ── 2. PO Data Volume ──
            try:
                po_count = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
                line_count = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
                if po_count == 0:
                    health["checks"].append({"check": "po_data", "status": "critical",
                        "message": "No SCPRS PO data — catalog/CRM/growth intelligence has no input",
                        "action": "Run full SCPRS pull: POST /api/intel/scprs/pull"})
                    health["score"] -= 25
                else:
                    latest = conn.execute("SELECT MAX(pulled_at) FROM scprs_po_master").fetchone()[0]
                    days_old = 999
                    if latest:
                        try:
                            days_old = (now - datetime.fromisoformat(latest.replace("Z","+00:00"))).days
                        except (ValueError, TypeError) as e:
                            log.debug("po_data latest pulled_at parse: %s", e)
                    health["checks"].append({"check": "po_data", "status": "ok" if days_old < 7 else "warning",
                        "message": f"{po_count} POs, {line_count} line items, last pull {days_old}d ago"})
                    if days_old > 7: health["score"] -= 10
            except Exception as e:
                health["checks"].append({"check": "po_data", "status": "error", "message": str(e)})
                health["score"] -= 20

            # ── 3. Buyer Data Flow → CRM ──
            try:
                scprs_buyers = conn.execute("""
                    SELECT COUNT(DISTINCT buyer_email) FROM scprs_po_master
                    WHERE buyer_email IS NOT NULL AND buyer_email != ''
                """).fetchone()[0]
                crm_contacts = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
                health["checks"].append({"check": "buyer_flow", "status": "ok" if crm_contacts > 0 else "warning",
                    "message": f"SCPRS buyers: {scprs_buyers}, CRM contacts: {crm_contacts}",
                    "action": "Run /api/crm/sync-intel to sync SCPRS buyers into CRM" if scprs_buyers > crm_contacts * 2 else None})
            except Exception as e:
                health["checks"].append({"check": "buyer_flow", "status": "error", "message": str(e)})

            # ── 4. Item Data Flow → Catalog ──
            try:
                scprs_items = conn.execute("SELECT COUNT(DISTINCT description) FROM scprs_po_lines").fetchone()[0]
                catalog_items = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
                catalog_with_scprs = conn.execute("""
                    SELECT COUNT(*) FROM product_catalog
                    WHERE scprs_last_price IS NOT NULL AND scprs_last_price > 0
                """).fetchone()[0]
                health["checks"].append({"check": "item_flow", "status": "ok" if catalog_with_scprs > 0 else "warning",
                    "message": f"SCPRS items: {scprs_items}, Catalog: {catalog_items}, With SCPRS pricing: {catalog_with_scprs}",
                    "action": "SCPRS price data not flowing to catalog — check post-pull enrichment" if scprs_items > 0 and catalog_with_scprs == 0 else None})
            except Exception as e:
                health["checks"].append({"check": "item_flow", "status": "error", "message": str(e)})

            # ── 5. Price Intelligence ──
            try:
                ph_count = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
                wq_count = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
                health["checks"].append({"check": "price_intel", "status": "ok" if ph_count > 0 or wq_count > 0 else "warning",
                    "message": f"Price history: {ph_count}, Won quotes KB: {wq_count}",
                    "action": "No pricing intelligence — SCPRS data not being recorded" if ph_count == 0 and wq_count == 0 else None})
            except Exception as e:
                health["checks"].append({"check": "price_intel", "status": "error", "message": str(e)})

            # ── 6. Growth Intelligence ──
            try:
                gap_items = conn.execute("""
                    SELECT COUNT(*) FROM scprs_po_lines WHERE opportunity_flag='GAP_ITEM'
                """).fetchone()[0]
                win_back = conn.execute("""
                    SELECT COUNT(*) FROM scprs_po_lines WHERE reytech_sells=1
                """).fetchone()[0]
                health["checks"].append({"check": "growth_intel", "status": "ok" if gap_items > 0 or win_back > 0 else "info",
                    "message": f"Gap items: {gap_items}, Win-back: {win_back}"})
            except Exception as e:
                health["checks"].append({"check": "growth_intel", "status": "error", "message": str(e)})

            # ── 7. Pull Log (recent activity) ──
            try:
                recent = conn.execute("""
                    SELECT pulled_at, search_term, results_found, new_pos, error
                    FROM scprs_pull_log ORDER BY pulled_at DESC LIMIT 5
                """).fetchall()
                errors = [dict(r) for r in recent if r["error"]]
                if not recent:
                    health["checks"].append({"check": "pull_log", "status": "info",
                        "message": "No pull history yet"})
                elif errors:
                    health["checks"].append({"check": "pull_log", "status": "warning",
                        "message": f"Last {len(recent)} pulls: {len(errors)} had errors",
                        "details": errors[:3]})
                    health["score"] -= 5
                else:
                    total_pos = sum(r["new_pos"] or 0 for r in recent)
                    health["checks"].append({"check": "pull_log", "status": "ok",
                        "message": f"Last {len(recent)} pulls: {total_pos} new POs found"})
            except Exception as e:
                health["checks"].append({"check": "pull_log", "status": "error", "message": str(e)})

        # Score interpretation
        health["score"] = max(health["score"], 0)
        health["grade"] = "A" if health["score"] >= 90 else "B" if health["score"] >= 75 else "C" if health["score"] >= 60 else "D" if health["score"] >= 40 else "F"
        health["ok"] = health["score"] >= 40

        # Remove None actions
        for c in health["checks"]:
            if c.get("action") is None:
                c.pop("action", None)

        return jsonify(health)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── SCPRS Backfill + Competitor Intelligence + Search ────────────────────────

@bp.route("/api/intel/scprs/backfill", methods=["POST"])
@auth_required
@safe_route
def api_scprs_backfill():
    """Backfill historical SCPRS data for a full year.
    POST {year: 2025, force: true} — runs in background."""
    try:
        from src.agents.scprs_intelligence_engine import backfill_historical, _engine_status
        data = request.get_json(silent=True) or {}
        year = int(data.get("year", 2025))
        force = data.get("force", False)
        if year < 2020 or year > 2026:
            return jsonify({"ok": False, "error": "Year must be 2020-2026"})
        # Reset stuck running state if force
        if force:
            _engine_status["running"] = False
        result = backfill_historical(year=year, notify_fn=_notify_wrapper, force=force)
        return jsonify(result)
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/scprs/test-pull", methods=["POST", "GET"])
@auth_required
@safe_route
def api_scprs_test_pull():
    """Diagnostic SCPRS pull — shows raw results, filtering, and storage.
    GET or POST {agency: "CCHCS", term: "glove", days: 365}"""
    try:
        from src.agents.scprs_lookup import FiscalSession
        from src.agents.scprs_intelligence_engine import (
            _is_target_agency, _store_po, _db, AGENCY_REGISTRY
        )
        import time

        data = request.get_json(silent=True) or {}
        agency = data.get("agency", request.args.get("agency", "CCHCS"))
        term = data.get("term", request.args.get("term", "glove"))
        days = int(data.get("days", request.args.get("days", 365)))

        from datetime import datetime, timedelta
        from_date = (datetime.now() - timedelta(days=days)).strftime("%m/%d/%Y")

        # Init session
        session = FiscalSession()
        if not session.init_session():
            return jsonify({"ok": False, "error": "SCPRS session init failed"})

        # Search
        results = session.search(description=term, from_date=from_date)

        # Analyze results
        raw_count = len(results)
        depts_seen = {}
        for r in results:
            dept = r.get("dept", r.get("dept_name", "(none)"))
            depts_seen[dept] = depts_seen.get(dept, 0) + 1

        # Filter for target agency
        matched = []
        for r in results:
            dept_val = r.get("dept", r.get("dept_name", ""))
            if _is_target_agency("", dept_val, agency):
                matched.append(r)

        # Store matched POs
        stored_pos = 0
        stored_lines = 0
        if matched:
            conn = _db()
            for po in matched:
                try:
                    result = _store_po(conn, po, agency, term, "diagnostic")
                    if result["is_new"]:
                        stored_pos += 1
                    stored_lines += result["lines_added"]
                except Exception as e:
                    return jsonify({"ok": False, "error": f"_store_po failed: {e}",
                                   "sample_po": {k: str(v)[:100] for k, v in po.items() if not k.startswith("_")}})
            conn.commit()
            conn.close()

        # Registry info
        reg = AGENCY_REGISTRY.get(agency, {})

        return jsonify({
            "ok": True,
            "search": {"term": term, "from_date": from_date, "agency": agency},
            "raw_results": raw_count,
            "departments_found": depts_seen,
            "agency_filter": {
                "dept_name_patterns": reg.get("dept_name_patterns", []),
                "dept_codes": reg.get("dept_codes", []),
            },
            "matched_after_filter": len(matched),
            "stored_pos": stored_pos,
            "stored_lines": stored_lines,
            "sample_matched": [{
                "po": m.get("po_number", ""),
                "dept": m.get("dept", ""),
                "supplier": m.get("supplier_name", ""),
                "total": m.get("grand_total", ""),
            } for m in matched[:5]],
            "sample_rejected": [{
                "dept": r.get("dept", ""),
                "supplier": r.get("supplier_name", ""),
            } for r in results[:5] if r not in matched],
        })
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/competitors")
@auth_required
@safe_route
def api_intel_competitors():
    """Competitor intelligence: who sells what, to whom, for how much,
    what contract vehicles they use, and where Reytech can displace."""
    try:
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        agency = request.args.get("agency", "")
        try:
            limit = max(1, min(int(request.args.get("limit", 50)), 500))
        except (ValueError, TypeError, OverflowError):
            limit = 50
        return jsonify(get_competitor_intelligence(agency_filter=agency, limit=limit))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs-search")
@auth_required
@safe_route
def api_scprs_search():
    """Search SCPRS data — suppliers, items, buyers, institutions, POs.
    GET ?q=gloves&type=item&agency=CCHCS"""
    try:
        from src.agents.scprs_intelligence_engine import search_scprs_data
        q = request.args.get("q", "").strip()
        if not q or len(q) < 2:
            return jsonify({"ok": False, "error": "Query must be at least 2 characters"})
        search_type = request.args.get("type", "all")
        agency = request.args.get("agency", "")
        try:
            limit = max(1, min(int(request.args.get("limit", 50)), 500))
        except (ValueError, TypeError, OverflowError):
            limit = 50
        return jsonify(search_scprs_data(q, search_type=search_type, agency=agency, limit=limit))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Growth Discovery Endpoints ──────────────────────────────────────────────

@bp.route("/api/intel/discover-agencies")
@auth_required
@safe_route
def api_discover_agencies():
    """Discover new agencies buying products Reytech sells but not buying from Reytech."""
    try:
        from src.agents.growth_discovery import discover_new_agencies
        try:
            min_spend = max(0.0, min(float(request.args.get("min_spend", 10000)), 999999999.0))
        except (ValueError, TypeError, OverflowError):
            min_spend = 10000.0
        return jsonify(discover_new_agencies(min_spend=min_spend))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/loss-intelligence")
@auth_required
@safe_route
def api_loss_intelligence():
    """Why we lose: price gaps, competitor patterns, actionable fixes."""
    try:
        from src.agents.growth_discovery import get_loss_intelligence
        return jsonify(get_loss_intelligence())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/dvbe-calculator")
@auth_required
@safe_route
def api_dvbe_calculator():
    """Calculate DVBE 3% mandate opportunity per agency."""
    try:
        from src.agents.growth_discovery import calculate_dvbe_opportunity
        return jsonify(calculate_dvbe_opportunity())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/dbe-opportunities")
@auth_required
@safe_route
def api_dbe_opportunities():
    """DBE/DOT opportunities Reytech isn't leveraging."""
    try:
        from src.agents.growth_discovery import get_dbe_opportunities
        return jsonify(get_dbe_opportunities())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/contract-vehicles")
@auth_required
@safe_route
def api_contract_vehicles():
    """Contract vehicle advisory — which to pursue, how, when."""
    try:
        from src.agents.growth_discovery import get_contract_vehicle_advisory
        return jsonify(get_contract_vehicle_advisory())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/intel/growth-discovery")
@auth_required
@safe_page
def page_growth_discovery():
    """Growth Discovery Dashboard — new agencies, DVBE math, DBE, contract vehicles."""
    return render_page("growth_discovery.html", active_page="Intelligence")


@bp.route("/api/intel/growth")
@auth_required
@safe_route
def api_intel_growth():
    """Full growth intelligence JSON — gaps, win-back, competitors, recs."""
    try:
        from src.agents.growth_agent import get_scprs_growth_intelligence
        return jsonify(get_scprs_growth_intelligence())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# BUYERS PAGE — SCPRS buyer outreach workflow
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/buyers")
@auth_required
@safe_page
def page_buyers():
    """SCPRS Buyers — every buyer who purchases items we can supply."""
    return render_page("buyers.html", active_page="Buyers")


@bp.route("/api/buyers")
@auth_required
@safe_route
def api_buyers():
    """Get all SCPRS buyers with spend, items, and catalog overlap."""
    try:
        agency_filter = request.args.get("agency", "")
        with get_db() as conn:
            if agency_filter:
                buyers = conn.execute("""
                    SELECT p.buyer_name, p.buyer_email, p.institution,
                           COALESCE(p.agency_key, p.dept_name) as agency,
                           COUNT(DISTINCT p.po_number) as po_count,
                           SUM(p.grand_total) as total_spend,
                           GROUP_CONCAT(DISTINCT p.supplier) as suppliers
                    FROM scprs_po_master p
                    WHERE p.buyer_email IS NOT NULL AND p.buyer_email != ''
                    AND p.agency_key = ?
                    GROUP BY LOWER(p.buyer_email)
                    ORDER BY total_spend DESC
                """, (agency_filter,)).fetchall()
            else:
                buyers = conn.execute("""
                    SELECT p.buyer_name, p.buyer_email, p.institution,
                           COALESCE(p.agency_key, p.dept_name) as agency,
                           COUNT(DISTINCT p.po_number) as po_count,
                           SUM(p.grand_total) as total_spend,
                           GROUP_CONCAT(DISTINCT p.supplier) as suppliers
                    FROM scprs_po_master p
                    WHERE p.buyer_email IS NOT NULL AND p.buyer_email != ''
                    GROUP BY LOWER(p.buyer_email)
                    ORDER BY total_spend DESC
                """).fetchall()

            # Get catalog categories for overlap detection
            catalog_tokens = set()
            for row in conn.execute("""
                SELECT DISTINCT LOWER(category) as cat FROM product_catalog
                WHERE category IS NOT NULL AND category != ''
            """).fetchall():
                catalog_tokens.add(row["cat"])

            # Get items each buyer purchases
            buyer_list = []
            overlap_count = 0
            agencies_seen = set()

            for b in buyers:
                email = b["buyer_email"]
                agencies_seen.add(b["agency"] or "")

                # Get their line items
                items = conn.execute("""
                    SELECT DISTINCT SUBSTR(l.description, 1, 50) as item,
                           l.category, l.reytech_sells
                    FROM scprs_po_lines l
                    JOIN scprs_po_master p ON l.po_id = p.id
                    WHERE LOWER(p.buyer_email) = LOWER(?)
                    ORDER BY l.line_total DESC LIMIT 15
                """, (email,)).fetchall()

                top_items = ", ".join(i["item"] for i in items if i["item"])[:200]
                overlap = [i["item"] for i in items if i["reytech_sells"]]
                if overlap:
                    overlap_count += 1

                buyer_list.append({
                    "buyer_name": b["buyer_name"] or "",
                    "buyer_email": email,
                    "institution": b["institution"] or "",
                    "agency": b["agency"] or "",
                    "po_count": b["po_count"],
                    "total_spend": b["total_spend"] or 0,
                    "suppliers": (b["suppliers"] or "")[:100],
                    "top_items": top_items,
                    "overlap_items": overlap[:5],
                })

        return jsonify({
            "ok": True,
            "buyers": buyer_list,
            "stats": {
                "total_buyers": len(buyer_list),
                "total_spend": sum(b["total_spend"] for b in buyer_list),
                "buyers_with_overlap": overlap_count,
                "agencies": len(agencies_seen),
            },
            "agencies_list": sorted(a for a in agencies_seen if a),
        })
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/buyers/save-draft", methods=["POST"])
@auth_required
@safe_route
def api_buyers_save_draft():
    """Save an outreach email draft to the outbox."""
    try:
        data = request.get_json(silent=True) or {}
        to = (data.get("to") or "").strip()
        subject = (data.get("subject") or "").strip()
        body = (data.get("body") or "").strip()
        if not to or not subject:
            return jsonify({"ok": False, "error": "to and subject required"})

        import json as _json
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        try:
            outbox = _json.load(open(outbox_path))
        except Exception:
            outbox = []

        draft = {
            "id": f"outreach_{__import__('hashlib').md5(to.encode()).hexdigest()[:8]}",
            "to": to,
            "subject": subject,
            "body": body,
            "status": "draft",
            "type": "outreach",
            "created_at": __import__('datetime').datetime.now().isoformat(),
        }
        outbox.append(draft)
        with open(outbox_path, "w") as f:
            _json.dump(outbox, f, indent=2, default=str)

        return jsonify({"ok": True, "draft_id": draft["id"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/buyers/start-nurture", methods=["POST"])
@auth_required
@safe_route
def api_buyers_start_nurture():
    """Convert SCPRS buyers into leads and start nurture sequences."""
    try:
        data = request.get_json(silent=True) or {}
        buyer_emails = data.get("emails", [])  # specific emails, or empty for all
        
        from src.core.db import get_db
        with get_db() as conn:
            if buyer_emails:
                placeholders = ",".join("?" * len(buyer_emails))
                buyers = conn.execute(
                    "SELECT DISTINCT buyer_name, buyer_email, institution, "
                    "COALESCE(agency_key, dept_name) as agency "
                    "FROM scprs_po_master WHERE buyer_email IN (" + placeholders + ")",
                    buyer_emails
                ).fetchall()
            else:
                buyers = conn.execute("""
                    SELECT DISTINCT buyer_name, buyer_email, institution,
                    COALESCE(agency_key, dept_name) as agency
                    FROM scprs_po_master 
                    WHERE buyer_email IS NOT NULL AND buyer_email != ''
                    LIMIT 50
                """).fetchall()
        
        created = 0
        nurtured = 0
        for b in buyers:
            email = b["buyer_email"]
            if not email: continue
            # Create contact if not exists
            try:
                conn2 = get_db()
                existing = conn2.execute(
                    "SELECT id FROM contacts WHERE email=?", (email,)
                ).fetchone()
                if not existing:
                    import uuid
                    cid = f"lead_{uuid.uuid4().hex[:8]}"
                    conn2.execute("""
                        INSERT INTO contacts (id, name, email, agency, institution, source, status, created_at)
                        VALUES (?, ?, ?, ?, ?, 'scprs_buyer', 'lead', datetime('now'))
                    """, (cid, b["buyer_name"] or "", email, b["agency"] or "", b["institution"] or ""))
                    conn2.commit()
                    created += 1
                conn2.close()
            except Exception as e:
                log.debug("Buyer→contact: %s", e)
            
            # Start nurture
            try:
                from src.agents.lead_nurture_agent import start_nurture
                result = start_nurture(email, sequence_key="scprs_buyer_intro")
                if result.get("ok"): nurtured += 1
            except Exception as e:
                log.debug("Nurture start: %s", e)
        
        return jsonify({"ok": True, "buyers_found": len(buyers), 
                       "contacts_created": created, "nurtures_started": nurtured})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
@bp.route("/intel/competitors")
@auth_required
@safe_page
def page_intel_competitors():
    """Competitor Intelligence Dashboard — who sells what, contract vehicles, DVBE opportunities."""
    return render_page("competitor_intel.html", active_page="Intelligence")



# ════════════════════════════════════════════════════════════════════════════════
# UNIVERSAL SCPRS INTELLIGENCE — All agencies, auto-close, price intel
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/scprs/pull", methods=["POST"])
@auth_required
@safe_route
def api_scprs_universal_pull():
    """Trigger full SCPRS pull for all agencies."""
    try:
        from src.agents.scprs_universal_pull import pull_background
        priority = (request.get_json(force=True, silent=True) or {}).get("priority", "P0")
        result = pull_background(priority=priority)
        _notify_wrapper("bell", f"SCPRS universal pull started ({priority})", "info")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/status")
@auth_required
@safe_route
def api_scprs_universal_status():
    try:
        from src.agents.scprs_universal_pull import get_pull_status
        return jsonify({"ok": True, **get_pull_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/intelligence")
@auth_required
@safe_route
def api_scprs_intelligence():
    try:
        from src.agents.scprs_universal_pull import get_universal_intelligence
        agency = request.args.get("agency")
        return jsonify({"ok": True, **get_universal_intelligence(agency_code=agency)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/close-lost", methods=["POST"])
@auth_required
@safe_route
def api_scprs_check_close_lost():
    """Run quote auto-close check against SCPRS now."""
    try:
        from src.agents.scprs_universal_pull import check_quotes_against_scprs
        result = check_quotes_against_scprs()
        if result["auto_closed"] > 0:
            _notify_wrapper("bell", f"SCPRS: {result['auto_closed']} quotes auto-closed lost", "warn")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/manager/recommendations")
@auth_required
@safe_route
def api_manager_recommendations():
    """Intelligent action recommendations from manager agent."""
    try:
        from src.agents.manager_agent import get_intelligent_recommendations
        return jsonify(get_intelligent_recommendations())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/intel/scprs")
@auth_required
@safe_page
def page_intel_scprs():
    """Universal SCPRS Intelligence Dashboard — all agencies, all products."""
    try:
        from src.agents.scprs_universal_pull import get_universal_intelligence, get_pull_status
        intel = get_universal_intelligence()
        status = get_pull_status()
    except Exception as e:
        intel = {"summary": {}, "gap_items": [], "win_back": [], "by_agency": [],
                 "competitors": [], "auto_closed_quotes": []}
        status = {"pos_stored": 0, "lines_stored": 0, "running": False, "progress": ""}

    try:
        from src.agents.manager_agent import get_intelligent_recommendations
        recs = get_intelligent_recommendations()
    except Exception:
        recs = {"actions": [], "summary": {}}

    summary = intel.get("summary", {})
    pos = status.get("pos_stored", 0)
    lines = status.get("lines_stored", 0)
    running = status.get("running", False)
    gap_opp = summary.get("gap_opportunity", 0) or 0
    win_opp = summary.get("win_back_opportunity", 0) or 0
    total_mkt = summary.get("total_market_spend", 0) or 0
    auto_closed = len(intel.get("auto_closed_quotes", []))
    no_data = pos == 0

    return render_page("scprs_intel.html", active_page="Intel",
        rec_actions=recs.get("actions", []),
        rec_summary=recs.get("summary", {}),
        gap_items=intel.get("gap_items", []),
        win_back_items=intel.get("win_back", []),
        by_agency=intel.get("by_agency", []),
        auto_closed_quotes=intel.get("auto_closed_quotes", []),
        lines=lines,
        pos=pos,
        auto_closed=auto_closed,
        recs=recs,
        gap_opp=gap_opp,
        win_opp=win_opp,
        total_mkt=total_mkt,
        running=running,
        no_data=no_data,
        status=status)


# ════════════════════════════════════════════════════════════════════════════════
# CCHCS PURCHASING INTELLIGENCE — What are they buying? Who from? At what price?
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/cchcs/intel/pull", methods=["POST"])
@auth_required
@safe_route
def api_cchcs_intel_pull():
    """Trigger CCHCS SCPRS purchasing data pull in background."""
    try:
        from src.agents.cchcs_intel_puller import pull_in_background
        priority = (request.get_json(force=True, silent=True) or {}).get("priority", "P0") if request.is_json else "P0"
        result = pull_in_background(priority=priority)
        _notify_wrapper("bell", f"CCHCS intel pull started (priority={priority})", "info")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cchcs/intel/status")
@auth_required
@safe_route
def api_cchcs_intel_status():
    """Check CCHCS intel pull status and DB record counts."""
    try:
        from src.agents.cchcs_intel_puller import get_pull_status, _pull_status
        status = get_pull_status()
        status["pull_running"] = _pull_status.get("running", False)
        status["last_result"] = _pull_status.get("last_result")
        return jsonify({"ok": True, **status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cchcs/intel/data")
@auth_required
@safe_route
def api_cchcs_intel_data():
    """Full CCHCS purchasing intelligence: gaps, win-backs, suppliers, facilities."""
    try:
        from src.agents.cchcs_intel_puller import get_cchcs_intelligence
        intel = get_cchcs_intelligence()
        return jsonify({"ok": True, **intel})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})



# ════════════════════════════════════════════════════════════════════════════════
# VENDOR ORDERING ROUTES
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/vendors")
@auth_required
@safe_page
def page_vendors():
    """Vendor management — redirects to unified catalog page."""
    return redirect("/catalog?tab=vendors")


@bp.route("/api/vendor/status")
@auth_required
@safe_route
def api_vendor_status():
    """Vendor ordering agent status + setup guide."""
    from src.agents.vendor_ordering_agent import get_agent_status as _voas
    return jsonify({"ok": True, **_voas()})


@bp.route("/api/vendor/search")
@auth_required
@safe_route
def api_vendor_search():
    """Search a vendor catalog.
    ?vendor=grainger&q=nitrile+gloves
    """
    vendor = request.args.get("vendor", "grainger")
    q = request.args.get("q", "")
    if not q:
        return jsonify({"ok": False, "error": "q required"})
    try:
        from src.agents.vendor_ordering_agent import grainger_search, amazon_search_catalog, compare_vendor_prices
        if vendor == "grainger":
            results = grainger_search(q, max_results=10)
        elif vendor == "amazon":
            results = amazon_search_catalog(q, max_results=10)
        elif vendor == "compare":
            try:
                qty = max(1, min(int(request.args.get("qty", 1)), 999999))
            except (ValueError, TypeError, OverflowError):
                qty = 1
            return jsonify(compare_vendor_prices(q, qty))
        else:
            return jsonify({"ok": False, "error": f"Unknown vendor: {vendor}"})
        return jsonify({"ok": True, "vendor": vendor, "query": q, "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/compare")
@auth_required
@safe_route
def api_vendor_compare():
    """Compare prices across all vendors for a product.
    ?q=nitrile+gloves+medium&qty=10
    """
    q = request.args.get("q", "")
    try:
        qty = max(1, min(int(request.args.get("qty", 1)), 999999))
    except (ValueError, TypeError, OverflowError):
        qty = 1
    if not q:
        return jsonify({"ok": False, "error": "q required"})
    try:
        from src.agents.vendor_ordering_agent import compare_vendor_prices
        return jsonify({"ok": True, **compare_vendor_prices(q, qty)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/order", methods=["POST"])
@auth_required
@safe_route
def api_vendor_order():
    """Place a vendor order or email PO.
    POST {vendor_key, items: [{description, quantity, unit_price}], po_number, quote_number}
    """
    data = request.get_json(silent=True) or {}
    vendor_key = data.get("vendor_key", "")
    items = data.get("items", [])
    po_number = data.get("po_number", "")
    quote_number = data.get("quote_number", "")

    if not vendor_key or not items or not po_number:
        return jsonify({"ok": False, "error": "vendor_key, items, and po_number required"})

    try:
        from src.agents.vendor_ordering_agent import VENDOR_CATALOG, grainger_place_order, send_email_po
        vendor = VENDOR_CATALOG.get(vendor_key, {})
        
        if not vendor:
            return jsonify({"ok": False, "error": f"Unknown vendor: {vendor_key}"})
        if not vendor.get("can_order"):
            return jsonify({"ok": False, "error": f"Vendor {vendor_key} not configured for ordering", "setup": vendor.get("env_needed", [])})
        
        api_type = vendor.get("api_type", "")
        if api_type == "rest":
            result = grainger_place_order(items, po_number)
        elif api_type == "email_po":
            result = send_email_po(vendor_key, items, po_number, quote_number)
        else:
            return jsonify({"ok": False, "error": f"Vendor type {api_type} not supported for ordering"})
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/orders")
@auth_required
@safe_route
def api_vendor_orders():
    """Get vendor order history."""
    status_filter = request.args.get("status")
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 500))
    except (ValueError, TypeError, OverflowError):
        limit = 50
    try:
        from src.agents.vendor_ordering_agent import get_vendor_orders
        orders = get_vendor_orders(limit=limit, status=status_filter)
        return jsonify({"ok": True, "count": len(orders), "orders": orders})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/enrich", methods=["POST"])
@auth_required
@safe_route
def api_vendor_enrich():
    """Get enriched vendor list with API metadata."""
    try:
        from src.agents.vendor_ordering_agent import get_enriched_vendor_list
        return jsonify({"ok": True, "vendors": get_enriched_vendor_list()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# ════════════════════════════════════════════════════════════════════════════════
# CS AGENT ROUTES — Inbound Customer Service
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/cs/classify", methods=["POST"])
@auth_required
@safe_route
def api_cs_classify():
    """Classify an email as an update request and get its intent.
    POST {subject, body, sender}
    """
    body = request.get_json(silent=True) or {}
    try:
        from src.agents.cs_agent import classify_inbound_email
        result = classify_inbound_email(
            subject=body.get("subject",""),
            body=body.get("body",""),
            sender=body.get("sender",""),
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/draft", methods=["POST"])
@auth_required
@safe_route
def api_cs_draft():
    """Build a CS response draft for an inbound email.
    POST {subject, body, sender}
    Returns a draft ready for review in the outbox.
    """
    body = request.get_json(silent=True) or {}
    try:
        from src.agents.cs_agent import classify_inbound_email, build_cs_response_draft
        subject = body.get("subject","")
        email_body = body.get("body","")
        sender = body.get("sender","")
        classification = classify_inbound_email(subject, email_body, sender)
        result = build_cs_response_draft(classification, subject, email_body, sender)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/drafts", methods=["GET"])
@auth_required
@safe_route
def api_cs_drafts():
    """Get all pending CS drafts from the outbox."""
    try:
        from src.agents.cs_agent import get_cs_drafts
        drafts = get_cs_drafts(limit=50)
        return jsonify({"ok": True, "count": len(drafts), "drafts": drafts})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/call", methods=["POST"])
@auth_required
@safe_route
def api_cs_call():
    """Place a CS follow-up call via Vapi.
    POST {phone_number, context: {intent, po_number, quote_number, institution, buyer_name}}
    """
    body = request.get_json(silent=True) or {}
    phone = body.get("phone_number","")
    if not phone:
        return jsonify({"ok": False, "error": "phone_number required"})
    try:
        from src.agents.cs_agent import place_cs_call
        result = place_cs_call(phone, context=body.get("context",{}))
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/status", methods=["GET"])
@auth_required
@safe_route
def api_cs_status():
    """Get CS agent status."""
    try:
        from src.agents.cs_agent import get_agent_status
        return jsonify({"ok": True, **get_agent_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/debug")
@auth_required
@safe_page
def debug_agent():
    """Live debug + monitoring agent — system health, data flow, automation status."""
    return render_page("debug.html", active_page="Intel", title="Debug Agent")


@bp.route("/api/debug/run")
@auth_required
@safe_route
def api_debug_run():
    """Run all debug checks and return JSON results. Used by /debug page."""
    results = {}
    start = time.time()

    # 1. DB health
    try:
        from src.core.db import get_db_stats, _is_railway_volume, DB_PATH
        db = get_db_stats()
        results["db"] = {
            "ok": True, "path": DB_PATH,
            "size_kb": db.get("db_size_kb", 0),
            "is_volume": _is_railway_volume(),
            "tables": {k: v for k, v in db.items() if k not in ("db_path","db_size_kb")},
        }
    except Exception as e:
        results["db"] = {"ok": False, "error": str(e)}

    # 2. Data files
    data_files = {}
    for fname in ["quotes_log.json","crm_contacts.json","intel_buyers.json",
                  "intel_agencies.json","quote_counter.json"]:
        fp = os.path.join(DATA_DIR, fname)
        if os.path.exists(fp):
            try:
                d = json.load(open(fp))
                n = len(d) if isinstance(d, (list, dict)) else 0
                data_files[fname] = {"exists": True, "records": n, "size_kb": round(os.path.getsize(fp)/1024,1)}
            except Exception:
                data_files[fname] = {"exists": True, "records": "parse_error"}
        else:
            data_files[fname] = {"exists": False, "records": 0}
    results["data_files"] = data_files

    # 3. Quote counter
    try:
        nxt = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else "N/A"
        results["quote_counter"] = {"ok": True, "next": nxt}
    except Exception as e:
        results["quote_counter"] = {"ok": False, "error": str(e)}

    # 4. Intel + CRM sync state
    try:
        intel_buyers = 0
        crm_count = len(_load_crm_contacts())
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import _load_json as _il2, BUYERS_FILE as _BF2
            bd = _il2(_BF2)
            intel_buyers = bd.get("total_buyers", 0) if isinstance(bd, dict) else 0
        in_sync = intel_buyers == crm_count or crm_count >= intel_buyers
        results["sync"] = {
            "ok": in_sync,
            "intel_buyers": intel_buyers,
            "crm_contacts": crm_count,
            "delta": abs(crm_count - intel_buyers),
        }
    except Exception as e:
        results["sync"] = {"ok": False, "error": str(e)}

    # 5. Auto-seed check
    try:
        crm_count = results.get("sync", {}).get("crm_contacts", 0)
        results["auto_seed"] = {
            "needed": crm_count == 0,
            "crm_contacts": crm_count,
            "status": "empty — run Load Demo Data" if crm_count == 0 else f"ok ({crm_count} contacts)",
        }
    except Exception as e:
        results["auto_seed"] = {"ok": False, "error": str(e)}

    # 6. Funnel stats
    try:
        quotes = [q for q in get_all_quotes() if not q.get("is_test")]
        results["funnel"] = {
            "ok": True,
            "quotes_total": len(quotes),
            "quotes_sent": sum(1 for q in quotes if q.get("status") == "sent"),
            "quotes_won": sum(1 for q in quotes if q.get("status") == "won"),
            "orders": len(_load_orders()),
        }
    except Exception as e:
        results["funnel"] = {"ok": False, "error": str(e)}

    # 7. Module availability
    results["modules"] = {
        "quote_gen": QUOTE_GEN_AVAILABLE,
        "price_check": PRICE_CHECK_AVAILABLE,
        "intel": INTEL_AVAILABLE,
        "growth": GROWTH_AVAILABLE,
        "qb": QB_AVAILABLE,
        "predict": PREDICT_AVAILABLE,
        "auto_processor": AUTO_PROCESSOR_AVAILABLE,
    }

    # 8. Recent errors from QA
    try:
        if QA_AVAILABLE:
            hist = get_qa_history(5)
            last = hist[0] if hist else {}
            results["qa"] = {
                "score": last.get("health_score", 0),
                "grade": last.get("grade", "?"),
                "critical_issues": last.get("critical_issues", []),
                "last_run": last.get("timestamp", "never"),
            }
    except Exception as e:
        results["qa"] = {"error": str(e)}

    # 9. Railway environment
    results["railway"] = {
        "environment": os.environ.get("RAILWAY_ENVIRONMENT", "local"),
        "volume_name": os.environ.get("RAILWAY_VOLUME_NAME", "not mounted"),
        "volume_path": os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "not mounted"),
        "deployment_id": os.environ.get("RAILWAY_DEPLOYMENT_ID", "local")[:16] if os.environ.get("RAILWAY_DEPLOYMENT_ID") else "local",
    }

    results["elapsed_ms"] = round((time.time() - start) * 1000)
    results["ok"] = True
    results["timestamp"] = datetime.now().isoformat()
    return jsonify(results)


@bp.route("/api/debug/fix/<fix_name>", methods=["POST"])
@auth_required
@safe_route
def api_debug_fix(fix_name):
    """Run an automated fix. fix_name: seed_demo | sync_crm | clear_cache | reset_counter"""
    if fix_name == "seed_demo":
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import seed_demo_data
            r = seed_demo_data()
            return jsonify({"ok": True, "result": r})
        return jsonify({"ok": False, "error": "Intel not available"})

    elif fix_name == "sync_crm":
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import sync_buyers_to_crm
            r = sync_buyers_to_crm()
            return jsonify({"ok": True, "result": r})
        return jsonify({"ok": False, "error": "Intel not available"})

    elif fix_name == "clear_cache":
        with _json_cache_lock:
            count = len(_json_cache)
            _json_cache.clear()
        return jsonify({"ok": True, "cleared": count})

    elif fix_name == "migrate_db":
        try:
            from src.core.db import migrate_json_to_db
            r = migrate_json_to_db()
            return jsonify({"ok": True, "result": r})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    return jsonify({"ok": False, "error": f"Unknown fix: {fix_name}"})


@bp.route("/search-intel")  # Moved to /search-intel — primary /search is in routes_search.py
@auth_required
@safe_page
def universal_search_page_intel():
    """Universal search page (legacy intel version) — primary /search is in routes_search.py."""
    q = (_sanitize_input(request.args.get("q", "")) or "").strip()

    # Run search if query provided
    results = []
    breakdown = {}
    error = None

    if q and len(q) >= 2:
        try:
            # Reuse the API logic directly
            from flask import g as _g
            # Call search inline to avoid HTTP round-trip
            ql = q.lower()
            limit = 50

            # ── Quotes ──
            if QUOTE_GEN_AVAILABLE:
                try:
                    for qt in search_quotes(query=ql, limit=20):
                        qn = qt.get("quote_number", "")
                        inst = qt.get("institution","") or qt.get("ship_to_name","") or "—"
                        ag   = qt.get("agency","") or "—"
                        results.append({
                            "type": "quote", "icon": "📋",
                            "title": qn,
                            "subtitle": f"{ag} · {inst[:50]}",
                            "meta": f"${qt.get('total',0):,.0f} · {qt.get('status','')} · {str(qt.get('created_at',''))[:10]}",
                            "url": f"/quote/{qn}",
                        })
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)

            # ── CRM Contacts ──
            try:
                contacts = _load_crm_contacts()
                for cid, c in contacts.items():
                    fields = " ".join([
                        c.get("buyer_name",""), c.get("buyer_email",""),
                        c.get("agency",""), c.get("title",""), c.get("notes",""),
                        c.get("buyer_phone",""),
                        " ".join(str(k) for k in c.get("categories",{}).keys()),
                        " ".join(i.get("description","") for i in c.get("items_purchased",[])[:5]),
                    ]).lower()
                    if ql in fields:
                        spend = c.get("total_spend",0)
                        results.append({
                            "type": "contact", "icon": "👤",
                            "title": c.get("buyer_name","") or c.get("buyer_email",""),
                            "subtitle": f"{c.get('agency','')} · {c.get('buyer_email','')}",
                            "meta": f"${spend:,.0f} spend · {c.get('outreach_status','new')} · {len(c.get('activity',[]))} interactions",
                            "url": f"/growth/prospect/{cid}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # ── Intel Buyers (not yet in CRM) ──
            if INTEL_AVAILABLE:
                try:
                    from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
                    buyers_data = _il(_BF)
                    crm_ids = set(_load_crm_contacts().keys())
                    if isinstance(buyers_data, dict):
                        for b in buyers_data.get("buyers", []):
                            if b.get("id","") in crm_ids:
                                continue
                            email = (b.get("email","") or b.get("buyer_email","")).lower()
                            fields = " ".join([
                                b.get("name","") or b.get("buyer_name",""),
                                email, b.get("agency",""),
                                " ".join(b.get("categories",{}).keys()),
                                " ".join(i.get("description","") for i in b.get("items_purchased",[])[:5]),
                            ]).lower()
                            if ql in fields:
                                results.append({
                                    "type": "intel_buyer", "icon": "🧠",
                                    "title": b.get("name","") or b.get("buyer_name","") or email,
                                    "subtitle": f"{b.get('agency','')} · {email}",
                                    "meta": f"${b.get('total_spend',0):,.0f} spend · score {b.get('opportunity_score',0)} · not in CRM",
                                    "url": f"/growth/prospect/{b.get('id','')}",
                                })
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)

            # ── Orders ──
            try:
                orders = _load_orders()
                for oid, o in orders.items():
                    fields = " ".join([
                        o.get("quote_number",""), o.get("agency",""),
                        o.get("institution",""), o.get("po_number",""), oid,
                    ]).lower()
                    if ql in fields:
                        results.append({
                            "type": "order", "icon": "📦",
                            "title": oid,
                            "subtitle": f"{o.get('agency','')} · {o.get('institution','')}",
                            "meta": f"PO {o.get('po_number','—')} · {o.get('status','')}",
                            "url": f"/order/{oid}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # ── RFQs ──
            try:
                rfqs = load_rfqs()
                for rid, r in rfqs.items():
                    fields = " ".join([
                        r.get("rfq_number",""), r.get("requestor_name",""),
                        r.get("institution",""), r.get("agency",""), rid,
                        " ".join(str(i.get("description","")) for i in r.get("items",[])),
                    ]).lower()
                    if ql in fields:
                        results.append({
                            "type": "rfq", "icon": "📄",
                            "title": r.get("rfq_number","") or rid[:12],
                            "subtitle": f"{r.get('agency','')} · {r.get('requestor_name','')}",
                            "meta": f"{len(r.get('items',[]))} items · {r.get('status','')}",
                            "url": f"/rfq/{rid}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # ── Price Checks ──
            try:
                pcs = _load_price_checks()
                for pid, pc in pcs.items():
                    fields = " ".join([
                        pc.get("institution",""), pc.get("requestor",""),
                        pc.get("pc_number",""), pid,
                        " ".join(str(i.get("description","")) for i in pc.get("items",[])[:10]),
                    ]).lower()
                    if ql in fields:
                        results.append({
                            "type": "pricecheck", "icon": "💲",
                            "title": pc.get("pc_number","") or pid[:12],
                            "subtitle": f"{pc.get('institution','')} · {pc.get('requestor','')}",
                            "meta": f"{len(pc.get('items',[]))} items · {pc.get('status','')}",
                            "url": f"/pricecheck/{pid}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # ── Product Catalog ──
            try:
                from src.core.db import get_db as _sdb
                with _sdb() as _sconn:
                    cat_rows = _sconn.execute("""
                        SELECT id, name, description, sku, mfg_number, category, cost, sell_price
                        FROM product_catalog
                        WHERE LOWER(name) LIKE ? OR LOWER(description) LIKE ?
                           OR LOWER(sku) LIKE ? OR LOWER(mfg_number) LIKE ?
                        LIMIT 15
                    """, (f"%{ql}%", f"%{ql}%", f"%{ql}%", f"%{ql}%")).fetchall()
                    for cr in cat_rows:
                        name = cr["name"] or cr["description"] or "?"
                        results.append({
                            "type": "product", "icon": "📦",
                            "title": name[:60],
                            "subtitle": f"{cr['category'] or '—'} · SKU: {cr['sku'] or cr['mfg_number'] or '—'}",
                            "meta": f"Cost ${cr['cost'] or 0:,.2f} → ${cr['sell_price'] or 0:,.2f}",
                            "url": f"/catalog?q={_sanitize_input(q[:30])}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # ── Vendors ──
            try:
                import json as _vjson
                vendors = _vjson.load(open(os.path.join(DATA_DIR, "vendors.json")))
                for v in vendors:
                    vname = v.get("name","")
                    fields = " ".join([vname, v.get("contact",""), v.get("email",""),
                                       " ".join(v.get("categories_served",[]))]).lower()
                    if ql in fields:
                        results.append({
                            "type": "vendor", "icon": "🏭",
                            "title": vname,
                            "subtitle": f"{v.get('contact','')} · {v.get('email','')}",
                            "meta": f"Score: {v.get('overall_score',0)}",
                            "url": f"/supplier/{vname}",
                        })
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

            # Dedupe by URL
            seen = set()
            deduped = []
            for r in results:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    deduped.append(r)
            results = deduped[:limit]

            breakdown = {t: sum(1 for r in results if r["type"]==t)
                         for t in ("quote","contact","intel_buyer","order","rfq","pricecheck","product","vendor")}
        except Exception as e:
            error = str(e)

    # Build type badge colors
    type_styles = {
        "quote":       ("#58a6ff", "rgba(88,166,255,.12)",  "📋 Quote"),
        "contact":     ("#a78bfa", "rgba(167,139,250,.12)", "👤 Contact"),
        "intel_buyer": ("#3fb950", "rgba(52,211,153,.12)",  "🧠 Intel Buyer"),
        "order":       ("#fbbf24", "rgba(251,191,36,.12)",  "📦 Order"),
        "rfq":         ("#f87171", "rgba(248,113,113,.12)", "📄 RFQ"),
        "pricecheck":  ("#d29922", "rgba(210,153,34,.12)",  "💲 Price Check"),
        "product":     ("#58a6ff", "rgba(88,166,255,.12)",  "🏷 Product"),
        "vendor":      ("#3fb950", "rgba(63,185,80,.12)",   "🏭 Vendor"),
    }

    rows_html = ""
    for r in results:
        color, bg, lbl = type_styles.get(r["type"], ("#8b949e","rgba(139,148,160,.12)","?"))
        rows_html += f"""
        <a href="{r['url']}" style="display:block;text-decoration:none;padding:14px 16px;border-bottom:1px solid var(--bd);transition:background .1s" onmouseover="this.style.background='rgba(79,140,255,.06)'" onmouseout="this.style.background=''">
         <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:14px;padding:3px 8px;border-radius:10px;color:{color};background:{bg};white-space:nowrap;font-weight:600">{lbl}</span>
          <div style="flex:1;min-width:0">
           <div style="font-weight:600;font-size:14px;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{esc(r['title'])}</div>
           <div style="font-size:14px;color:var(--tx2);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{esc(r['subtitle'])}</div>
          </div>
          <div style="font-size:14px;color:var(--tx2);white-space:nowrap;text-align:right">{esc(r['meta'])}</div>
          <span style="color:var(--ac);font-size:16px">→</span>
         </div>
        </a>"""

    breakdown_html = ""
    if breakdown:
        for t, count in breakdown.items():
            if count:
                color, bg, lbl = type_styles.get(t, ("#8b949e","rgba(139,148,160,.12)",t))
                breakdown_html += f'<span style="font-size:14px;padding:3px 10px;border-radius:10px;color:{color};background:{bg}">{lbl}: {count}</span>'

    empty_state = ""
    if q and len(q) >= 2 and not results:
        empty_state = f"""
        <div style="text-align:center;padding:48px 24px;color:var(--tx2)">
         <div style="font-size:40px;margin-bottom:12px">🔍</div>
         <div style="font-size:16px;font-weight:600;margin-bottom:6px">No results for "{esc(q)}"</div>
         <div style="font-size:13px;margin-bottom:20px">Try a name, agency, email, item description, or quote number</div>
         <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap">
          <a href="/quotes" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">📋 Browse Quotes</a>
          <a href="/contacts" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">👥 Browse CRM</a>
          <a href="/intelligence" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">🧠 Sales Intel</a>
         </div>
        </div>"""

    q_escaped = q.replace('"','&quot;')
    type_badges = ''.join(f'<span style="font-size:14px;padding:2px 8px;border-radius:8px;color:{c};background:{bg}">{lbl}</span>' for t,(c,bg,lbl) in type_styles.items())
    return render_page("search.html", active_page="Search",
        q=q, q_escaped=q_escaped, results=results,
        rows_html=rows_html, breakdown_html=breakdown_html,
        empty_state=empty_state, error=error, type_badges=type_badges)


@bp.route("/quotes")
@auth_required
@safe_page
def quotes_list():
    """Browse / search all generated Reytech quotes with win/loss tracking."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect("/")
    q = request.args.get("q", "")
    agency_filter = request.args.get("agency", "")
    status_filter = request.args.get("status", "")
    since_filter = request.args.get("since", "")  # "24h" or "" (all)
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(10, int(request.args.get("per_page", 50))))
    since_hours = 24 if since_filter == "24h" else 0
    all_quotes = search_quotes(query=q, agency=agency_filter, status=status_filter,
                               limit=500, since_hours=since_hours)

    # Hide ghost quotes from the list view (HIDE, not delete — data stays in DB).
    # A ghost quote has $0 total AND 0 items AND no real agency. Stats bar still
    # reflects the underlying DB so we don't silently swallow data; the filter
    # only suppresses these from the visible row list.
    def _is_ghost_quote(_qt):
        try:
            _total = float(_qt.get("total") or 0)
        except (TypeError, ValueError):
            _total = 0.0
        try:
            _items_count = int(_qt.get("items_count") or 0)
        except (TypeError, ValueError):
            _items_count = 0
        _raw_agency = (_qt.get("agency") or "").strip()
        return _total == 0.0 and _items_count == 0 and _raw_agency in ("", "DEFAULT")
    all_quotes = [q for q in all_quotes if not _is_ghost_quote(q)]

    # Paginate
    total_quotes = len(all_quotes)
    total_pages = max(1, (total_quotes + per_page - 1) // per_page)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    quotes = all_quotes[start:start + per_page]

    next_num = peek_next_quote_number()
    # P0.12 fix: use unified metrics instead of get_quote_stats() so
    # /quotes and /pipeline show the same numbers.
    from src.core.metrics import get_win_rate as _unified_wr
    _uwr = _unified_wr()
    stats = {
        "total": _uwr["total"], "won": _uwr["won"], "lost": _uwr["lost"],
        "pending": _uwr["pending"], "sent": _uwr.get("sent", 0),
        "won_total": _uwr["won_total"], "pending_total": _uwr["pending_total"],
        "win_rate": _uwr["rate"],
    }

    # Check if logo exists
    logo_exists = any(os.path.exists(os.path.join(DATA_DIR, f"reytech_logo.{e}"))
                      for e in ("png", "jpg", "jpeg", "gif"))

    # Status badge colors
    status_cfg = {
        "won":     ("✅ Won",     "#3fb950", "rgba(52,211,153,.08)"),
        "lost":    ("❌ Lost",    "#f85149", "rgba(248,113,113,.08)"),
        "pending": ("⏳ Pending", "#d29922", "rgba(210,153,34,.08)"),
        "draft":   ("📝 Draft",   "#8b949e", "rgba(139,148,160,.08)"),
        "sent":    ("📤 Sent",    "#58a6ff", "rgba(88,166,255,.08)"),
        "expired": ("⏰ Expired", "#8b949e", "rgba(139,148,160,.08)"),
    }

    rows_html = ""
    for qt in quotes:
        fname = os.path.basename(qt.get("pdf_path") or "")
        if fname:
            dl = (f'<button type="button" class="quote-preview-btn" data-pdf-url="/api/pricecheck/view-pdf/{fname}"'
                  f' data-quote-num="{esc(qt.get("quote_number",""))}" title="Preview PDF"'
                  f' style="background:none;border:none;cursor:pointer;font-size:14px;padding:0 2px">👁️</button>'
                  f'<a href="/api/pricecheck/download/{fname}" title="Download PDF" style="font-size:14px">📥</a>')
        else:
            dl = ""
        st = qt.get("status", "pending")

        # Derive institution from ship_to if empty/missing
        institution = qt.get("institution", "")
        if not institution or institution.strip() == "":
            ship_name = qt.get("ship_to_name", "")
            if ship_name:
                institution = ship_name
            else:
                # Try from items_text or rfq_number as last resort
                institution = qt.get("rfq_number", "") or "—"

        # Fix DEFAULT agency using ALL available data
        agency = qt.get("agency", "")
        if agency in ("DEFAULT", "", None) and QUOTE_GEN_AVAILABLE:
            try:
                agency = _detect_agency(qt)
            except Exception as e:
                log.debug("Suppressed: %s", e)
                agency = ""
        if agency == "DEFAULT":
            agency = ""

        lbl, color, bg = status_cfg.get(st, status_cfg["pending"])
        po = qt.get("po_number", "")
        po_html = f'<br><span style="font-size:13px;color:#8b949e">PO: {po}</span>' if po else ""
        qn = qt.get("quote_number", "")
        items_detail = qt.get("items_detail", [])
        items_text = qt.get("items_text", "")

        # Build expandable detail row
        detail_rows = ""
        if items_detail:
            for it in items_detail[:10]:
                desc = str(it.get("description", ""))[:80]
                pn = it.get("part_number", "")
                pn_link = f'<a href="https://amazon.com/dp/{pn}" target="_blank" style="color:#58a6ff;font-size:13px">{pn}</a>' if pn and pn.startswith("B0") else (f'<span style="color:#8b949e;font-size:13px">{pn}</span>' if pn else "")
                detail_rows += f'<div style="display:flex;gap:8px;align-items:baseline;padding:2px 0"><span style="color:var(--tx2);font-size:14px;flex:1">{desc}</span>{pn_link}<span style="font-family:monospace;font-size:14px;color:#d29922">${it.get("unit_price",0):.2f} × {it.get("qty",0)}</span></div>'
        elif items_text:
            detail_rows = f'<div style="color:var(--tx2);font-size:14px;padding:2px 0">{items_text[:200]}</div>'

        detail_id = f"detail-{qn.replace(' ','')}"
        toggle = f"""<button onclick="document.getElementById('{detail_id}').style.display=document.getElementById('{detail_id}').style.display==='none'?'table-row':'none'" style="background:none;border:none;cursor:pointer;font-size:13px;color:var(--tx2);padding:0" title="Show items">▶ {qt.get('items_count',0)}</button>""" if (items_detail or items_text) else str(qt.get('items_count', 0))

        # Quote number links to source RFQ/PC page for editing + resend
        # Falls back to quote detail page if no source link
        source_rfq = qt.get("source_rfq_id", "")
        source_pc = qt.get("source_pc_id", "")
        rfq_num_val = qt.get("rfq_number", "")

        # Backfill the RFQ # column from the source RFQ record when the quote
        # row itself doesn't carry one. Without this, the RFQ # column is
        # almost always "—" even when the quote was generated from a real RFQ.
        if not rfq_num_val and source_rfq:
            try:
                from src.api.modules.routes_rfq import load_rfqs as _lr
                _src_rfq = _lr().get(source_rfq) or {}
                rfq_num_val = (
                    _src_rfq.get("solicitation_number")
                    or _src_rfq.get("rfq_number")
                    or ""
                )
            except Exception as _e:
                log.debug("rfq# backfill suppressed: %s", _e)

        # Build link: prefer RFQ detail → PC detail → quote detail
        if source_rfq:
            qn_href = f"/rfq/{source_rfq}"
        elif source_pc:
            qn_href = f"/pricecheck/{source_pc}"
        elif rfq_num_val:
            # Try to find RFQ by solicitation number
            qn_href = f"/quote/{qn}"  # fallback
            try:
                from src.api.modules.routes_rfq import load_rfqs as _lr
                for _rid, _rfq in _lr().items():
                    if str(_rfq.get("solicitation_number", "")).strip() == str(rfq_num_val).strip():
                        qn_href = f"/rfq/{_rid}"
                        break
            except Exception as _e:
                log.debug("suppressed: %s", _e)
        else:
            qn_href = f"/quote/{qn}"
        
        test_badge = ' <span style="background:#d29922;color:#000;font-size:13px;padding:1px 5px;border-radius:4px;font-weight:700">TEST</span>' if qt.get("is_test") or qt.get("source_pc_id", "").startswith("test_") else ""
        qn_cell = f'<a href="{qn_href}" style="color:var(--ac);text-decoration:none;font-family:\'JetBrains Mono\',monospace;font-weight:700" title="Open RFQ to edit and resend">{esc(qn)}</a>{test_badge}'

        # RFQ # column — also links to RFQ detail
        rfq_cell = f'<a href="{qn_href}" style="color:#58a6ff;text-decoration:none">{esc(rfq_num_val)}</a>' if rfq_num_val else "—"

        # Decided rows get subtle opacity
        row_style = "opacity:0.5" if st in ("won", "lost", "expired") else ""

        rows_html += f"""<tr data-qn="{esc(qn)}" style="{row_style}">
         <td>{qn_cell}</td>
         <td class="mono" style="white-space:nowrap">{esc(qt.get('date',''))}</td>
         <td>{esc(agency)}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal;font-weight:500">{esc(institution)}</td>
         <td class="mono">{rfq_cell}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${qt.get('total',0):,.2f}</td>
         <td style="text-align:center">{toggle}</td>
         <td style="text-align:center">
          <span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:14px;font-weight:600;color:{color};background:{bg}">{lbl}</span>{po_html}
         </td>
         <td style="text-align:center;white-space:nowrap">
          {"<a href=\"/order/ORD-" + qn + "\" style=\"font-size:14px;color:#3fb950;text-decoration:none;padding:2px 6px\" title=\"View order\">📦 Order</a>" if st == "won" else "<span style=\"font-size:14px;color:#8b949e;padding:2px 6px\">lost</span>" if st == "lost" else f"<button onclick=\"markQuote('{qn}','won')\" class=\"btn btn-sm\" style=\"background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);padding:2px 6px;font-size:14px;cursor:pointer\" title=\"Mark Won\">✅</button><button onclick=\"markQuote('{qn}','lost')\" class=\"btn btn-sm\" style=\"background:rgba(248,113,113,.15);color:#f85149;border:1px solid rgba(248,113,113,.3);padding:2px 6px;font-size:14px;cursor:pointer\" title=\"Mark Lost\">❌</button>" if st not in ("expired",) else "<span style=\"font-size:14px;color:#8b949e\">expired</span>"}
          {dl}
         </td>
        </tr>
        <tr id="{detail_id}" style="display:none"><td colspan="9" style="background:var(--sf2);padding:8px 16px;border-left:3px solid var(--ac)">{detail_rows if detail_rows else '<span style="color:var(--tx2);font-size:14px">No item details available</span>'}</td></tr>"""

    # Win rate stats bar
    wr = stats.get("win_rate", 0)
    wr_color = "#3fb950" if wr >= 50 else ("#d29922" if wr >= 30 else "#f85149")
    expired_count = sum(1 for qt in quotes if qt.get("status") == "expired")
    stats_html = f"""
     <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:13px;font-family:'JetBrains Mono',monospace">
      <span><b>{stats['total']}</b> total</span>
      <span style="color:#3fb950"><b>{stats['won']}</b> won (${stats['won_total']:,.0f})</span>
      <span style="color:#f85149"><b>{stats['lost']}</b> lost</span>
      <span style="color:#d29922"><b>{stats['pending']}</b> pending</span>
      {f'<span style="color:#8b949e"><b>{expired_count}</b> expired</span>' if expired_count else ''}
      <span>WR: <b style="color:{wr_color}">{wr}%</b></span>
      <span style="color:#8b949e">Next: <b style="color:var(--tx)">{next_num}</b></span>
     </div>
    """

    return render_page("quotes.html", active_page="Quotes",
        stats_html=stats_html, q=q, agency_filter=agency_filter,
        status_filter=status_filter, since_filter=since_filter,
        logo_exists=logo_exists, rows_html=rows_html,
        title="Quotes Database",
        stat_total=stats['total'], stat_won=stats['won'], stat_lost=stats['lost'],
        stat_pending=stats['pending'], stat_sent=stats.get('sent', 0),
        stat_won_total=stats['won_total'], stat_pending_total=stats.get('pending_total', 0),
        stat_win_rate=wr, stat_expired=expired_count,
        page=page, per_page=per_page, total_pages=total_pages, total_quotes=total_quotes,
    )


def _get_package_files(qt, source_link):
    """Get all generated package files from the source RFQ/PC for display on quote detail."""
    package = []  # list of {filename, download_url, view_url, icon}
    try:
        source_rid = qt.get("source_rfq_id", "")
        if not source_rid and source_link and "/rfq/" in source_link:
            source_rid = source_link.split("/rfq/")[-1]
        if source_rid:
            from src.api.modules.routes_rfq import load_rfqs
            rfqs = load_rfqs()
            r = rfqs.get(source_rid)
            if r and r.get("output_files"):
                for f in r["output_files"]:
                    icon = "📝" if "703B" in f else "📊" if "704B" in f else "📋" if "BidPackage" in f else "💰" if "Quote" in f else "📄"
                    package.append({
                        "filename": f,
                        "download_url": f"/dl/{source_rid}/{f}",
                        "view_url": f"/api/pricecheck/view-pdf/{f}",
                        "icon": icon,
                    })
        # Also try DB files if output_files was empty
        if not package and source_rid:
            from src.api.modules.routes_rfq import list_rfq_files
            db_files = list_rfq_files(source_rid, category="generated")
            for db_f in db_files:
                f = db_f.get("filename", "")
                icon = "📝" if "703B" in f else "📊" if "704B" in f else "📋" if "BidPackage" in f else "💰" if "Quote" in f else "📄"
                package.append({
                    "filename": f,
                    "download_url": f"/dl/{source_rid}/{f}",
                    "view_url": f"/api/pricecheck/view-pdf/{f}",
                    "icon": icon,
                })
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return package


@bp.route("/quote/<qn>")
@auth_required
@safe_page
def quote_detail(qn):
    """Dedicated quote detail page."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect("/")
    quotes = get_all_quotes()
    qt = None
    for q in quotes:
        if q.get("quote_number") == qn:
            qt = q
            break
    if not qt:
        flash(f"Quote {qn} not found", "error")
        return redirect("/quotes")

    # Derive institution from ship_to if empty
    institution = qt.get("institution", "")
    if not institution or institution.strip() == "":
        institution = qt.get("ship_to_name", "") or qt.get("rfq_number", "") or "—"

    # Fix DEFAULT agency using all available data
    agency = qt.get("agency", "")
    if agency in ("DEFAULT", "", None):
        try:
            agency = _detect_agency(qt)
        except Exception:
            agency = ""
    if agency == "DEFAULT":
        agency = ""

    st = qt.get("status", "pending")
    fname = os.path.basename(qt.get("pdf_path") or "")

    # ── PRD-28 WI-1: Lifecycle info ──
    expires_at = qt.get("expires_at", "")
    expiry_display = "—"
    expiry_class = ""
    if expires_at and st in ("pending", "sent"):
        try:
            from datetime import datetime as _dt, timezone as _tz
            exp_dt = _dt.fromisoformat(expires_at.replace("Z", "+00:00")) if "T" in expires_at else _dt.fromisoformat(expires_at)
            days_left = (exp_dt.replace(tzinfo=None) - _dt.now()).days
            if days_left < 0:
                expiry_display = "Expired"
                expiry_class = "color:var(--rd);font-weight:600"
            elif days_left <= 7:
                expiry_display = f"{days_left}d left"
                expiry_class = "color:var(--rd);font-weight:600"
            elif days_left <= 14:
                expiry_display = f"{days_left}d left"
                expiry_class = "color:var(--yl);font-weight:600"
            else:
                expiry_display = f"{days_left}d left"
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    elif st == "won":
        expiry_display = "Won ✅"
        expiry_class = "color:var(--gn);font-weight:600"
    elif st == "lost":
        expiry_display = "Lost"
        expiry_class = "color:var(--rd)"
    elif st == "expired":
        expiry_display = "Expired"
        expiry_class = "color:var(--tx2)"

    close_reason = qt.get("close_reason", "")
    closed_by = qt.get("closed_by_agent", "")
    revision_count = qt.get("revision_count", 0) or 0
    follow_up_count = qt.get("follow_up_count", 0) or 0
    # items_detail is canonical; line_items is the field in quotes_log.json
    items = qt.get("items_detail") or qt.get("line_items") or []
    source_link = ""
    source_label = ""
    if qt.get("source_pc_id"):
        source_link = f'/pricecheck/{qt["source_pc_id"]}'
        source_label = f"PC # {qt.get('rfq_number', 'Price Check')}"
    elif qt.get("source_rfq_id"):
        source_link = f'/rfq/{qt["source_rfq_id"]}'
        source_label = "RFQ"
    
    # Smart fallback: search by rfq_number in PCs and RFQs
    if not source_link:
        rfq_num = qt.get("rfq_number", "") or ""
        if rfq_num:
            # Search price_checks
            try:
                import json as _j2
                _pcs = _j2.load(open(os.path.join(DATA_DIR, "price_checks.json")))
                _rn = str(rfq_num).strip().lower().replace(" ", "").replace("-", "")
                for _pid, _pc in _pcs.items():
                    _pnum = str(_pc.get("pc_number", "")).lower().replace(" ", "").replace("-", "")
                    if _pnum == _rn:
                        source_link = f"/pricecheck/{_pid}"
                        source_label = f"PC # {rfq_num}"
                        break
            except Exception as _e:
                log.debug("suppressed: %s", _e)
            # Search RFQs
            if not source_link:
                try:
                    from src.api.modules.routes_rfq import load_rfqs as _load_rfqs
                    _rfqs = _load_rfqs()
                    for _rid, _r in _rfqs.items():
                        if str(_r.get("solicitation_number", "")).strip() == str(rfq_num).strip():
                            source_link = f"/rfq/{_rid}"
                            source_label = f"RFQ # {rfq_num}"
                            break
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
        # Also check notes for PC#
        if not source_link and qt.get("notes") and "PC#" in str(qt.get("notes", "")):
            import re as _re2
            _m = _re2.search(r"PC#\s*([^|\n]+)", str(qt.get("notes", "")))
            if _m:
                source_label = f"PC # {_m.group(1).strip()}"
                try:
                    import json as _j2
                    _pcs = _j2.load(open(os.path.join(DATA_DIR, "price_checks.json")))
                    _pc_num = _m.group(1).strip().lower().replace(" ", "").replace("-", "")
                    for _pid, _pc in _pcs.items():
                        _pnum = str(_pc.get("pc_number","")).lower().replace(" ", "").replace("-", "")
                        if _pnum == _pc_num:
                            source_link = f"/pricecheck/{_pid}"
                            break
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)

    # Status config
    status_cfg = {
        "won":     ("✅ Won",     "var(--gn)", "rgba(52,211,153,.1)"),
        "lost":    ("❌ Lost",    "var(--rd)", "rgba(248,113,113,.1)"),
        "pending": ("⏳ Pending", "var(--yl)", "rgba(251,191,36,.1)"),
        "draft":   ("📝 Draft",   "var(--tx2)", "rgba(139,148,160,.1)"),
        "sent":    ("📤 Sent",    "var(--ac)", "rgba(79,140,255,.1)"),
        "expired": ("⏰ Expired", "var(--tx2)", "rgba(139,148,160,.1)"),
    }
    lbl, color, bg = status_cfg.get(st, status_cfg["pending"])

    # Items table rows
    items_html = ""
    for it in items:
        desc = str(it.get("description", ""))
        pn = it.get("part_number", "")
        pn_cell = f'<a href="https://amazon.com/dp/{esc(pn)}" target="_blank" style="color:var(--ac)">{esc(pn)}</a>' if pn and pn.startswith("B0") else (esc(pn) or "—")
        up = it.get("unit_price", 0)
        qty = it.get("qty", 0)
        items_html += f"""<tr>
         <td style="color:var(--tx2)">{it.get('line_number', '')}</td>
         <td style="max-width:400px;word-wrap:break-word;white-space:normal">{esc(desc)}</td>
         <td class="mono">{pn_cell}</td>
         <td class="mono" style="text-align:center">{qty}</td>
         <td class="mono" style="text-align:right">${up:,.2f}</td>
         <td class="mono" style="text-align:right;font-weight:600">${up*qty:,.2f}</td>
        </tr>"""

    # Status history
    history = qt.get("status_history", [])
    history_html = ""
    for h in reversed(history[-10:]):
        history_html += f'<div style="font-size:14px;color:var(--tx2);padding:3px 0"><span class="mono">{esc(h.get("timestamp","")[:16])}</span> → <b>{esc(h.get("status",""))}</b>{" by " + str(esc(h.get("actor",""))) if h.get("actor") else ""}{" (PO: " + str(esc(h["po_number"])) + ")" if h.get("po_number") else ""}</div>'

    # Build action buttons separately to avoid f-string escaping
    has_order = False
    try:
        _orders = _load_orders()
        has_order = f"ORD-{qn}" in _orders
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    if st in ('pending', 'sent'):
        action_btns = '<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap">'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;won&quot;)" class="btn btn-g" style="font-size:13px">✅ Mark Won</button>'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;lost&quot;)" class="btn" style="background:rgba(248,113,113,.15);color:var(--rd);border:1px solid rgba(248,113,113,.3);font-size:13px">❌ Mark Lost</button>'
        action_btns += f'<button onclick="convertToOrder(&quot;{qn}&quot;)" class="btn" style="background:rgba(52,211,153,.15);color:#34d399;border:1px solid rgba(52,211,153,.3);font-size:13px">📦 Convert to Order</button>'
        action_btns += '</div>'
    elif st == 'won' and not has_order:
        action_btns = '<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;display:flex;gap:8px;justify-content:center">'
        action_btns += f'<button onclick="convertToOrder(&quot;{qn}&quot;)" class="btn btn-g" style="font-size:14px;padding:10px 24px">📦 Convert to Order</button>'
        action_btns += '</div>'
    elif st == 'won' and has_order:
        action_btns = '<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;display:flex;gap:8px;justify-content:center">'
        action_btns += f'<a href="/order/ORD-{qn}" class="btn btn-g" style="font-size:14px;padding:10px 24px;text-decoration:none">📦 View Order</a>'
        action_btns += '</div>'
    else:
        action_btns = ""

    return render_page("quote_detail.html", active_page="Quotes",
        qn=qn, qt=qt, institution=institution, agency=agency, st=st, fname=fname,
        lbl=lbl, color=color, bg=bg, expiry_display=expiry_display, expiry_class=expiry_class,
        close_reason=close_reason, closed_by=closed_by,
        revision_count=revision_count, follow_up_count=follow_up_count,
        items=items, items_html=items_html, source_link=source_link, source_label=source_label,
        history_html=history_html, action_btns=action_btns,
        package_files=_get_package_files(qt, source_link),
    )


# ═══════════════════════════════════════════════════════════════════════
# Quote-to-Order Conversion (PRD-v32 F2)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/quote/<qn>/update-email", methods=["POST"])
@auth_required
@safe_route
def api_quote_update_email(qn):
    """Update contact email on a quote (for quotes missing buyer email)."""
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()
        if not email or "@" not in email:
            return jsonify({"ok": False, "error": "Valid email required"})
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("UPDATE quotes SET contact_email=?, updated_at=? WHERE quote_number=?",
                         (email, __import__('datetime').datetime.now().isoformat(), qn))
        # Also update JSON
        import json as _json
        ql_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(ql_path):
            ql = _json.load(open(ql_path))
            for q in ql:
                if q.get("quote_number") == qn:
                    q["contact_email"] = email
                    break
            with open(ql_path, "w") as f:
                _json.dump(ql, f, indent=2, default=str)
        return jsonify({"ok": True, "email": email})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/quote/<qn>/convert-to-order", methods=["POST"])
@auth_required
@safe_route
def api_quote_convert_to_order(qn):
    """One-click conversion of a quote to an order. Pre-fills from quote data."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})

    qt = None
    for q in get_all_quotes():
        if q.get("quote_number") == qn:
            qt = q
            break
    if not qt:
        return jsonify({"ok": False, "error": f"Quote {qn} not found"})

    # Check if order already exists
    orders = _load_orders()
    oid = f"ORD-{qn}"
    if oid in orders:
        return jsonify({"ok": False, "error": f"Order {oid} already exists", "order_id": oid})

    data = request.get_json(silent=True) or {}
    po_number = data.get("po_number", "") or qt.get("po_number", "")

    # Mark quote as won if not already
    st = qt.get("status", "pending")
    if st not in ("won",):
        try:
            update_quote_status(qn, "won", po_number)
        except Exception as e:
            log.debug("Quote status update during conversion: %s", e)

    order = _create_order_from_quote(qt, po_number=po_number)
    return jsonify({
        "ok": True,
        "order_id": order["order_id"],
        "total": order["total"],
        "items": len(order["line_items"]),
        "message": f"Order {order['order_id']} created from quote {qn}",
    })


# ═══════════════════════════════════════════════════════════════════════
# Pipeline Dashboard (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/pipeline")
@auth_required
@safe_page
def pipeline_page():
    """Autonomous pipeline dashboard — full funnel visibility."""
    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    orders = {k: v for k, v in _load_orders().items() if not v.get("is_test")}
    crm = _load_crm_activity()
    leads = []
    try:
        from src.core.dal import get_all_leads
        leads = get_all_leads()
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── Funnel Counts (P0.12 fix: unified metrics) ──
    total_leads = len(leads)
    from src.core.metrics import get_win_rate as _pipe_wr, get_active_orders as _pipe_ao
    _pwr = _pipe_wr()
    _pao = _pipe_ao()
    total_quotes = _pwr["total"]
    sent = _pwr.get("sent", 0)
    pending = _pwr["pending"]
    won = _pwr["won"]
    lost = _pwr["lost"]
    expired = _pwr.get("expired", 0)
    total_orders = _pao["total"]
    invoiced = _pao["closed"]  # closed includes invoiced per unified def

    # ── Revenue (from unified stats) ──
    total_quoted = _pwr["won_total"] + _pwr["lost_total"] + _pwr["pending_total"]
    total_won = _pwr["won_total"]
    total_pending = _pwr["pending_total"]
    total_invoiced = _pao["invoiced_value"]

    # ── Conversion Rates ──
    def rate(a, b): return round(a/b*100) if b > 0 else 0
    lead_to_quote = rate(total_quotes, total_leads) if total_leads else "—"
    quote_to_sent = rate(sent + won + lost, total_quotes) if total_quotes else "—"
    sent_to_won = rate(won, won + lost) if (won + lost) else "—"
    won_to_invoiced = rate(invoiced, total_orders) if total_orders else "—"

    # ── Funnel bars ──
    max_count = max(total_leads, total_quotes, 1)
    def bar(count, color, label, sublabel=""):
        pct = max(5, round(count / max_count * 100))
        return f"""<div style="margin-bottom:8px">
         <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
          <span style="font-size:14px;font-weight:600">{label}</span>
          <span style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:{color}">{count}</span>
         </div>
         <div style="background:var(--sf2);border-radius:6px;height:24px;overflow:hidden">
          <div style="width:{pct}%;height:100%;background:{color};border-radius:6px;display:flex;align-items:center;padding-left:8px">
           <span style="font-size:13px;color:#fff;font-weight:600">{sublabel}</span>
          </div>
         </div>
        </div>"""

    funnel = (
        bar(total_leads, "#58a6ff", "🔍 Leads (SCPRS)", f"{total_leads} opportunities") +
        bar(total_quotes, "#bc8cff", "📋 Quotes Generated", f"${total_quoted:,.0f} total") +
        bar(sent + won + lost, "#d29922", "📤 Sent to Buyer", f"{sent} active") +
        bar(won, "#3fb950", "✅ Won", f"${total_won:,.0f} revenue") +
        bar(total_orders, "#58a6ff", "📦 Orders", f"{total_orders} active") +
        bar(invoiced, "#3fb950", "💰 Invoiced", f"${total_invoiced:,.0f}")
    )

    # ── Recent CRM events ──
    recent = sorted(crm, key=lambda e: e.get("timestamp", ""), reverse=True)[:15]
    evt_icons = {
        "quote_won": "✅", "quote_lost": "❌", "quote_sent": "📤", "quote_generated": "📋",
        "order_created": "📦", "voice_call": "📞", "email_sent": "📧", "note": "📝",
        "shipping_detected": "🚚", "invoice_full": "💰", "invoice_partial": "½",
    }
    events_html = ""
    for e in recent:
        icon = evt_icons.get(e.get("event_type", ""), "●")
        ts = e.get("timestamp", "")[:16].replace("T", " ")
        events_html += f"""<div style="padding:6px 0;border-bottom:1px solid var(--bd);font-size:14px;display:flex;gap:8px;align-items:flex-start">
         <span style="flex-shrink:0">{icon}</span>
         <div style="flex:1"><div>{e.get('description','')[:100]}</div><div style="color:var(--tx2);font-size:13px;margin-top:2px">{ts}</div></div>
        </div>"""

    # ── Prediction leaderboard for pending quotes ──
    predictions_html = ""
    if PREDICT_AVAILABLE:
        preds = []
        for q in quotes:
            if q.get("status") in ("pending", "sent"):
                p = predict_win_probability(
                    institution=q.get("institution", ""),
                    agency=q.get("agency", ""),
                    po_value=q.get("total", 0),
                )
                preds.append({**q, "win_prob": p["probability"], "rec": p["recommendation"]})
        preds.sort(key=lambda x: x["win_prob"], reverse=True)
        for q in preds[:10]:
            prob = round(q["win_prob"] * 100)
            clr = "#3fb950" if prob >= 60 else ("#d29922" if prob >= 40 else "#f85149")
            predictions_html += f"""<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--bd);font-size:14px">
             <span style="font-family:'JetBrains Mono',monospace;font-weight:700;color:{clr};min-width:36px">{prob}%</span>
             <a href="/quote/{esc(q.get('quote_number',''))}" style="color:var(--ac);text-decoration:none;font-weight:600">{esc(q.get('quote_number',''))}</a>
             <span style="color:var(--tx2);flex:1">{esc(q.get('institution','')[:30])}</span>
             <span style="font-family:'JetBrains Mono',monospace">${q.get('total',0):,.0f}</span>
            </div>"""

    # BI Revenue bar data
    rev_data = None
    try:
        rev = update_revenue_tracker() if INTEL_AVAILABLE else {}
        if rev.get("ok"):
            rv_pct = min(100, rev.get("pct_to_goal", 0))
            rev_data = {
                "pct": rv_pct,
                "closed": rev.get("closed_revenue", 0),
                "goal": rev.get("goal", 2000000),
                "gap": rev.get("gap_to_goal", 0),
                "run_rate": rev.get("run_rate_annual", 0),
                "on_track": rev.get("on_track", False),
                "color": "#3fb950" if rv_pct >= 50 else "#d29922" if rv_pct >= 25 else "#f85149",
            }
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return render_page("pipeline.html", active_page="Pipeline",
        total_leads=total_leads, total_quotes=total_quotes,
        total_pending=total_pending, total_won=total_won,
        funnel=funnel, predictions_html=predictions_html,
        lead_to_quote=lead_to_quote, quote_to_sent=quote_to_sent,
        sent_to_won=sent_to_won, won_to_invoiced=won_to_invoiced,
        pending=pending, sent=sent, total_orders=total_orders,
        rev=rev_data)


# ═══════════════════════════════════════════════════════════════════════
# Pipeline API (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/stats")
@auth_required
@safe_route
def api_pipeline_stats():
    """Full pipeline statistics as JSON."""
    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    orders = {k: v for k, v in _load_orders().items() if not v.get("is_test")}

    statuses = {}
    for q in quotes:
        s = q.get("status", "pending")
        statuses[s] = statuses.get(s, 0) + 1

    return jsonify({
        "ok": True,
        "quotes": {
            "total": len(quotes),
            "by_status": statuses,
            "total_value": sum(q.get("total", 0) for q in quotes),
            "won_value": sum(q.get("total", 0) for q in quotes if q.get("status") == "won"),
            "pending_value": sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent")),
        },
        "orders": {
            "total": len(orders),
            "total_value": sum(o.get("total", 0) for o in orders.values()),
            "invoiced_value": sum(o.get("invoice_total", 0) for o in orders.values()),
        },
        "conversion": {
            "win_rate": round(statuses.get("won", 0) / max(statuses.get("won", 0) + statuses.get("lost", 0), 1) * 100, 1),
            "quote_count": len(quotes),
            "decided": statuses.get("won", 0) + statuses.get("lost", 0),
        },
        "annual_goal": update_revenue_tracker() if INTEL_AVAILABLE else None,
    })


@bp.route("/api/pipeline/analyze-reply", methods=["POST"])
@auth_required
@safe_route
def api_analyze_reply():
    """Analyze an email reply for win/loss/question signals.
    POST: {subject, body, sender}"""
    if not REPLY_ANALYZER_AVAILABLE:
        return jsonify({"ok": False, "error": "Reply analyzer not available"})
    data = request.get_json(silent=True) or {}
    quotes = get_all_quotes()
    result = find_quote_from_reply(
        data.get("subject", ""), data.get("body", ""),
        data.get("sender", ""), quotes)
    result["ok"] = True

    # Auto-flag quote if high confidence win/loss
    if result.get("matched_quote") and result.get("confidence", 0) >= 0.6:
        signal = result.get("signal")
        qn = result["matched_quote"]
        if signal == "win":
            _log_crm_activity(qn, "win_signal_detected",
                              f"Email reply signals WIN for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "loss":
            _log_crm_activity(qn, "loss_signal_detected",
                              f"Email reply signals LOSS for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "question":
            _log_crm_activity(qn, "question_detected",
                              f"Buyer question detected for {qn} — follow up needed",
                              actor="system", metadata=result)

    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
# Predictive Intelligence & Shipping Monitor (Phase 19)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/predict/win")
@auth_required
@safe_route
def api_predict_win():
    """Predict win probability for an institution/agency.
    GET ?institution=CSP-Sacramento&agency=CDCR&value=5000"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    try:
        value = max(0.0, float(request.args.get("value", 0) or 0))
    except (ValueError, TypeError, OverflowError):
        value = 0.0
    result = predict_win_probability(institution=inst, agency=agency, po_value=value)
    return jsonify({"ok": True, **result})


@bp.route("/api/predict/batch", methods=["POST"])
@auth_required
@safe_route
def api_predict_batch():
    """Batch predict for multiple opportunities. POST JSON: [{institution, agency, value}, ...]"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    data = request.get_json(silent=True) or []
    results = []
    for opp in data[:50]:
        pred = predict_win_probability(
            institution=opp.get("institution", ""),
            agency=opp.get("agency", ""),
            po_value=opp.get("value", 0),
        )
        results.append({**opp, **pred})
    results.sort(key=lambda r: r.get("probability", 0), reverse=True)
    return jsonify({"ok": True, "predictions": results})


@bp.route("/api/intel/competitors/predict")
@auth_required
@safe_route
def api_competitor_insights():
    """Competitor intelligence from prediction module (supplementary).
    GET ?institution=...&agency=...&limit=20"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    try:
        limit = max(1, min(int(request.args.get("limit", 20)), 200))
    except (ValueError, TypeError, OverflowError):
        limit = 20
    result = get_competitor_insights(institution=inst, agency=agency, limit=limit)
    return jsonify({"ok": True, **result})


@bp.route("/api/shipping/scan-email", methods=["POST"])
@auth_required
@safe_route
def api_shipping_scan():
    """Scan an email for shipping/tracking info. POST: {subject, body, sender}"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Shipping monitor not available"})
    data = request.get_json(silent=True) or {}
    tracking_info = detect_shipping_email(
        data.get("subject", ""), data.get("body", ""), data.get("sender", ""))

    if not tracking_info.get("is_shipping"):
        return jsonify({"ok": True, "is_shipping": False})

    # Try to match to an order
    orders = _load_orders()
    matched_oid = match_tracking_to_order(tracking_info, orders)
    result = {"ok": True, **tracking_info, "matched_order": matched_oid}

    # Auto-update order if matched
    if matched_oid:
        update = update_order_from_tracking(matched_oid, tracking_info, orders)
        _save_orders(orders)
        _update_order_status(matched_oid)
        result["update"] = update
        _log_crm_activity(matched_oid, "shipping_detected",
                          f"Shipping email detected — {tracking_info.get('carrier','')} "
                          f"tracking {', '.join(tracking_info.get('tracking_numbers',[])) or 'N/A'} — "
                          f"status: {tracking_info.get('delivery_status','')}",
                          actor="system", metadata=tracking_info)

    return jsonify(result)


