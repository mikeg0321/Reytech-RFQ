"""Quoting Status Dashboard — single pane of glass for the orchestrator.

Routes:
    GET  /quoting/status                    — list recent quotes + stage
    GET  /quoting/status/<doc_id>           — full audit trail for one quote
    GET  /api/quoting/status                — JSON: recent quotes summary
    GET  /api/quoting/status/<doc_id>       — JSON: single quote trail
    GET  /api/quoting/status/export.csv     — CSV: latest row per doc
    GET  /api/quoting/status/<doc_id>/export.csv — CSV: full trail for one doc
    POST /api/quoting/override/<doc_id>     — record an operator override
    POST /api/quoting/retry/<doc_id>        — re-run orchestrator on one doc
    POST /api/quoting/backfill              — drive existing PCs through orchestrator

Data source: `quote_audit_log` (migration 21). Rows are written by
`QuoteOrchestrator._persist_audit` on every stage transition attempt —
advanced, blocked, error, or skipped.

An override is itself an audit log row with outcome="override" and the
operator's ID as actor. It is purely informational — advancing a quote
still has to go through the orchestrator's normal transition.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone

from flask import Response, jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


def _fetch_recent_summary(limit: int = 50) -> list[dict]:
    """Return the most-recent audit row per quote_doc_id, up to `limit`."""
    try:
        from src.core.db import get_db
    except Exception as e:
        log.warning("db unavailable: %s", e)
        return []

    rows: list[dict] = []
    try:
        with get_db() as conn:
            # Don't mutate row_factory — this is the shared thread-local
            # connection from get_db(). Setting it to None here breaks every
            # subsequent caller in the same thread that does dict(row) on the
            # connection (e.g. _load_price_checks → "dictionary update sequence
            # element #0 has length N; 2 is required" → empty result → 1-click
            # banner smoke 302). Index access r[0]..r[8] works on sqlite3.Row.
            # Per-quote latest row via a self-join on MAX(at).
            cur = conn.execute(
                """SELECT q.quote_doc_id, q.doc_type, q.agency_key,
                          q.stage_from, q.stage_to, q.outcome,
                          q.reasons_json, q.actor, q.at
                     FROM quote_audit_log q
                     JOIN (
                       SELECT quote_doc_id, MAX(at) AS max_at
                         FROM quote_audit_log
                        GROUP BY quote_doc_id
                     ) m
                       ON q.quote_doc_id = m.quote_doc_id
                      AND q.at = m.max_at
                    ORDER BY q.at DESC
                    LIMIT ?""",
                (limit,),
            )
            for r in cur.fetchall():
                try:
                    reasons = json.loads(r[6] or "[]")
                except Exception:
                    reasons = []
                rows.append({
                    "doc_id": r[0],
                    "doc_type": r[1],
                    "agency_key": r[2],
                    "stage_from": r[3],
                    "stage_to": r[4],
                    "outcome": r[5],
                    "reasons": reasons,
                    "actor": r[7],
                    "at": r[8],
                })
    except Exception as e:
        log.error("fetch_recent_summary error: %s", e)
    return rows


def _fetch_trail(doc_id: str) -> list[dict]:
    """Return the full chronological trail for one quote_doc_id."""
    if not doc_id:
        return []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # See _fetch_recent_summary note — never mutate the shared
            # thread-local row_factory. Index access works on sqlite3.Row.
            cur = conn.execute(
                """SELECT stage_from, stage_to, outcome, reasons_json,
                          actor, at, doc_type, agency_key
                     FROM quote_audit_log
                    WHERE quote_doc_id = ?
                    ORDER BY at ASC""",
                (doc_id,),
            )
            rows = cur.fetchall()
    except Exception as e:
        log.error("fetch_trail error for %s: %s", doc_id, e)
        return []

    out = []
    for r in rows:
        try:
            reasons = json.loads(r[3] or "[]")
        except Exception:
            reasons = []
        out.append({
            "stage_from": r[0],
            "stage_to": r[1],
            "outcome": r[2],
            "reasons": reasons,
            "actor": r[4],
            "at": r[5],
            "doc_type": r[6],
            "agency_key": r[7],
        })
    return out


# ── JSON API ────────────────────────────────────────────────────────────────

@bp.route("/api/quoting/status")
@auth_required
def api_quoting_status():
    """Recent quotes with their latest stage. Caps at 200."""
    try:
        limit = max(1, min(int(request.args.get("limit", "50")), 200))
    except ValueError:
        limit = 50
    rows = _fetch_recent_summary(limit=limit)

    # Group counts by final outcome for dashboard KPIs.
    outcome_counts: dict[str, int] = {}
    blocked: list[dict] = []
    for r in rows:
        outcome_counts[r["outcome"]] = outcome_counts.get(r["outcome"], 0) + 1
        if r["outcome"] in ("blocked", "error"):
            blocked.append(r)
    return jsonify({
        "ok": True,
        "total": len(rows),
        "outcome_counts": outcome_counts,
        "blocked_now": blocked,
        "rows": rows,
    })


@bp.route("/api/quoting/status/<doc_id>")
@auth_required
def api_quoting_status_detail(doc_id: str):
    trail = _fetch_trail(doc_id)
    if not trail:
        return jsonify({"ok": False, "error": "no audit trail for that doc_id"}), 404
    return jsonify({
        "ok": True,
        "doc_id": doc_id,
        "trail": trail,
        "latest_stage": trail[-1]["stage_to"],
        "latest_outcome": trail[-1]["outcome"],
    })


# ── CSV exports ─────────────────────────────────────────────────────────────

def _csv_response(rows: list[list[str]], header: list[str], filename: str) -> Response:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _parse_outcome_filter(raw: str) -> set[str]:
    if not raw:
        return set()
    return {o.strip().lower() for o in raw.split(",") if o.strip()}


@bp.route("/api/quoting/status/export.csv")
@auth_required
def api_quoting_status_export_csv():
    """Export the latest-per-doc summary as CSV. Optional ?outcome=blocked,error."""
    try:
        limit = max(1, min(int(request.args.get("limit", "200")), 1000))
    except ValueError:
        limit = 200
    outcomes = _parse_outcome_filter(request.args.get("outcome", ""))

    rows = _fetch_recent_summary(limit=limit)
    if outcomes:
        rows = [r for r in rows if (r.get("outcome") or "").lower() in outcomes]

    out = [
        [r.get("doc_id") or "", r.get("doc_type") or "", r.get("agency_key") or "",
         r.get("stage_from") or "", r.get("stage_to") or "", r.get("outcome") or "",
         " | ".join(r.get("reasons") or []), r.get("actor") or "", r.get("at") or ""]
        for r in rows
    ]
    return _csv_response(
        out,
        ["doc_id", "doc_type", "agency_key", "stage_from", "stage_to",
         "outcome", "reasons", "actor", "at"],
        "quoting_status.csv",
    )


@bp.route("/api/quoting/status/<doc_id>/export.csv")
@auth_required
def api_quoting_trail_export_csv(doc_id: str):
    """Export the full audit trail for one doc_id as CSV."""
    trail = _fetch_trail(doc_id)
    if not trail:
        return jsonify({"ok": False, "error": "no audit trail for that doc_id"}), 404

    safe_id = "".join(c for c in doc_id if c.isalnum() or c in ("-", "_")) or "trail"
    out = [
        [t.get("stage_from") or "", t.get("stage_to") or "", t.get("outcome") or "",
         " | ".join(t.get("reasons") or []), t.get("actor") or "",
         t.get("at") or "", t.get("doc_type") or "", t.get("agency_key") or ""]
        for t in trail
    ]
    return _csv_response(
        out,
        ["stage_from", "stage_to", "outcome", "reasons", "actor", "at",
         "doc_type", "agency_key"],
        f"quoting_trail_{safe_id}.csv",
    )


@bp.route("/api/quoting/override/<doc_id>", methods=["POST"])
@auth_required
def api_quoting_override(doc_id: str):
    """Record an operator override decision on a blocked quote.

    This does NOT advance the quote — it records the reason so the audit
    trail shows a human accepted the blocker. Re-running the orchestrator
    with the same blocker will still fail; the operator must fix the root
    cause (e.g., fill the missing form) or adjust scope.
    """
    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    if not reason:
        return jsonify({"ok": False, "error": "reason is required"}), 400

    actor = (body.get("actor") or "operator").strip() or "operator"
    at = datetime.now(timezone.utc).isoformat()

    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Look up the last row for this doc to carry forward doc_type/agency.
            last = conn.execute(
                "SELECT doc_type, agency_key, stage_to FROM quote_audit_log "
                "WHERE quote_doc_id = ? ORDER BY at DESC LIMIT 1",
                (doc_id,),
            ).fetchone()
            if not last:
                return jsonify({"ok": False, "error": "doc_id not found in audit log"}), 404
            doc_type, agency_key, stage_to = last
            conn.execute(
                """INSERT INTO quote_audit_log
                   (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                    outcome, reasons_json, actor, at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (doc_id, doc_type, agency_key, stage_to, stage_to,
                 "override", json.dumps([reason[:500]]), actor, at),
            )
    except Exception as e:
        log.error("override write failed for %s: %s", doc_id, e)
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "recorded_at": at})


# ── Single-doc retry ────────────────────────────────────────────────────────

# Allowed target stages for one-click retry. Generation/sending stay manual —
# retry is for re-driving the analysis half of the pipeline after a fix.
_RETRY_TARGET_STAGES = {"parsed", "priced", "qa_pass"}


@bp.route("/api/quoting/retry/<doc_id>", methods=["POST"])
@auth_required
def api_quoting_retry(doc_id: str):
    """Re-run the orchestrator on a single doc after the operator fixes a blocker.

    Looks up the source PC by doc_id (which equals pc_id for PC docs),
    constructs a QuoteRequest using the doc_type/agency_key recorded in the
    most-recent audit row, and calls orchestrator.run(). The orchestrator is
    idempotent — it advances from current stage forward, so this is safe to
    call repeatedly.

    POST body:
        target_stage  one of {"parsed","priced","qa_pass"}, default "qa_pass"
        actor         default "operator"
        reason        optional human note recorded as a separate audit row

    Returns the orchestrator result summary (final_stage, blockers, profiles_used).
    """
    if not doc_id:
        return jsonify({"ok": False, "error": "doc_id required"}), 400

    body = request.get_json(silent=True) or {}
    target_stage = (body.get("target_stage") or "qa_pass").strip()
    if target_stage not in _RETRY_TARGET_STAGES:
        return jsonify({
            "ok": False,
            "error": f"target_stage must be one of {sorted(_RETRY_TARGET_STAGES)}",
        }), 400
    actor = (body.get("actor") or "operator").strip() or "operator"
    reason = (body.get("reason") or "").strip()

    try:
        from src.core.db import get_all_price_checks, get_db
        from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest
    except Exception as e:
        log.error("retry import failed: %s", e)
        return jsonify({"ok": False, "error": f"import failed: {e}"}), 500

    # Source lookup. Today only PC docs are retriable from here — RFQs don't
    # round-trip cleanly without their original parse blob, which we don't keep.
    try:
        pcs = get_all_price_checks(include_test=False)
    except Exception as e:
        log.error("retry: get_all_price_checks failed: %s", e)
        return jsonify({"ok": False, "error": f"source lookup failed: {e}"}), 500

    pc = pcs.get(doc_id)
    if not pc:
        return jsonify({
            "ok": False,
            "error": "doc_id not found in price-check store (RFQ-only retry not supported yet)",
        }), 404

    # Carry doc_type/agency_key from the latest audit row (so retry uses the
    # same identity the orchestrator originally resolved).
    doc_type = "pc"
    agency_key = (pc.get("agency") or "").strip()
    try:
        with get_db() as conn:
            last = conn.execute(
                "SELECT doc_type, agency_key FROM quote_audit_log "
                "WHERE quote_doc_id = ? ORDER BY at DESC LIMIT 1",
                (doc_id,),
            ).fetchone()
            if last:
                doc_type = last[0] or doc_type
                agency_key = last[1] or agency_key
    except Exception as e:
        log.warning("retry: audit lookup failed for %s (proceeding with PC defaults): %s", doc_id, e)

    try:
        orchestrator = QuoteOrchestrator()
        req = QuoteRequest(
            source=pc,
            doc_type=doc_type,
            agency_key=agency_key,
            solicitation_number=(pc.get("pc_number") or "").strip(),
            target_stage=target_stage,
            actor=actor,
        )
        result = orchestrator.run(req)
    except Exception as e:
        log.error("retry orchestrator run failed for %s: %s", doc_id, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

    # Optional human-note audit row alongside whatever the orchestrator wrote.
    if reason:
        try:
            from src.core.db import get_db as _get_db
            with _get_db() as conn:
                conn.execute(
                    """INSERT INTO quote_audit_log
                       (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                        outcome, reasons_json, actor, at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (doc_id, doc_type, agency_key, result.final_stage, result.final_stage,
                     "override", json.dumps([f"retry-note: {reason[:480]}"]),
                     actor, datetime.now(timezone.utc).isoformat()),
                )
        except Exception as e:
            log.warning("retry: failed to record reason note: %s", e)

    return jsonify({
        "ok": result.ok,
        "doc_id": doc_id,
        "final_stage": result.final_stage,
        "blockers": result.blockers,
        "warnings": result.warnings[:5],
        "profiles_used": result.profiles_used,
        "target_stage": target_stage,
    })


# ── Backfill ────────────────────────────────────────────────────────────────

@bp.route("/api/quoting/backfill", methods=["POST"])
@auth_required
def api_quoting_backfill():
    """Drive existing PCs through the orchestrator to populate audit log.

    Safe default: pulls PCs from the live store, runs each through the
    orchestrator with target_stage="priced". The orchestrator persists an
    audit row per stage transition, which lights up /quoting/status.

    POST body:
        mode         "all" (default) | "ids"
        ids          list[str] — required when mode=="ids"
        target_stage default "priced" (catalog-priced PCs are safe)
        limit        default 25, cap 100 — per-call safety bound
        actor        default "backfill"
        skip_filled  default true — skip PCs already in audit log
    """
    body = request.get_json(silent=True) or {}
    mode = (body.get("mode") or "all").strip()
    target_stage = (body.get("target_stage") or "priced").strip()
    actor = (body.get("actor") or "backfill").strip() or "backfill"
    skip_filled = bool(body.get("skip_filled", True))
    try:
        limit = max(1, min(int(body.get("limit", 25)), 100))
    except (TypeError, ValueError):
        limit = 25

    try:
        from src.core.db import get_all_price_checks, get_db
        from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest
    except Exception as e:
        log.error("backfill import failed: %s", e)
        return jsonify({"ok": False, "error": f"import failed: {e}"}), 500

    pcs = get_all_price_checks(include_test=False)

    if mode == "ids":
        ids = body.get("ids") or []
        if not isinstance(ids, list) or not ids:
            return jsonify({"ok": False, "error": "ids list required when mode=ids"}), 400
        candidates = [(pid, pcs[pid]) for pid in ids if pid in pcs]
    else:
        candidates = list(pcs.items())

    if skip_filled:
        try:
            with get_db() as conn:
                existing = {
                    row[0] for row in conn.execute(
                        "SELECT DISTINCT quote_doc_id FROM quote_audit_log"
                    ).fetchall()
                }
            candidates = [(pid, pc) for pid, pc in candidates if pid not in existing]
        except Exception as e:
            log.warning("skip_filled query failed (proceeding without skip): %s", e)

    candidates = candidates[:limit]
    orchestrator = QuoteOrchestrator()
    results: list[dict] = []

    for pid, pc in candidates:
        try:
            req = QuoteRequest(
                source=pc,
                doc_type="pc",
                agency_key=(pc.get("agency") or "").strip(),
                solicitation_number=(pc.get("pc_number") or "").strip(),
                target_stage=target_stage,
                actor=actor,
            )
            res = orchestrator.run(req)
            results.append({
                "pc_id": pid,
                "ok": res.ok,
                "final_stage": res.final_stage,
                "blockers": res.blockers,
                "warnings": res.warnings[:3],
                "profiles_used": res.profiles_used,
            })
        except Exception as e:
            log.error("backfill run failed for %s: %s", pid, e, exc_info=True)
            results.append({"pc_id": pid, "ok": False, "error": str(e)})

    advanced = sum(1 for r in results if r.get("ok"))
    blocked = sum(1 for r in results if not r.get("ok"))
    return jsonify({
        "ok": True,
        "processed": len(results),
        "advanced": advanced,
        "blocked": blocked,
        "target_stage": target_stage,
        "results": results,
    })


# ── HTML pages ──────────────────────────────────────────────────────────────

@bp.route("/quoting/status")
@auth_required
def quoting_status_page():
    from src.api.render import render_page
    rows = _fetch_recent_summary(limit=50)
    outcome_counts: dict[str, int] = {}
    for r in rows:
        outcome_counts[r["outcome"]] = outcome_counts.get(r["outcome"], 0) + 1
    return render_page(
        "quoting_status.html",
        active_page="Quoting",
        rows=rows,
        outcome_counts=outcome_counts,
    )


@bp.route("/quoting/status/<doc_id>")
@auth_required
def quoting_status_detail_page(doc_id: str):
    from src.api.render import render_page
    trail = _fetch_trail(doc_id)
    return render_page(
        "quoting_status_detail.html",
        active_page="Quoting",
        doc_id=doc_id,
        trail=trail,
    )


# ── Approval queue ──────────────────────────────────────────────────────────

# Stages where the orchestrator stops and a human decides what's next.
# qa_pass = ready to generate package; priced = ready for QA review.
_APPROVAL_STAGES = {"qa_pass", "priced"}


def _fetch_approval_queue(limit: int = 100) -> list[dict]:
    """Latest audit row per doc, filtered to docs sitting at an approval stage.

    A doc is "awaiting approval" when its most-recent stage_to is in
    _APPROVAL_STAGES AND its outcome is "advanced" (i.e., the orchestrator
    completed cleanly and is yielding control to the operator).
    """
    rows = _fetch_recent_summary(limit=max(limit, 200))
    out = []
    for r in rows:
        if (r.get("stage_to") in _APPROVAL_STAGES
                and (r.get("outcome") or "") == "advanced"):
            out.append(r)
        if len(out) >= limit:
            break
    return out


@bp.route("/api/quoting/approval-queue")
@auth_required
def api_quoting_approval_queue():
    """JSON: docs sitting at qa_pass or priced, awaiting operator action."""
    try:
        limit = max(1, min(int(request.args.get("limit", "50")), 200))
    except ValueError:
        limit = 50
    rows = _fetch_approval_queue(limit=limit)
    by_stage: dict[str, int] = {}
    for r in rows:
        by_stage[r["stage_to"]] = by_stage.get(r["stage_to"], 0) + 1
    return jsonify({
        "ok": True,
        "total": len(rows),
        "by_stage": by_stage,
        "rows": rows,
    })


@bp.route("/quoting/approval-queue")
@auth_required
def quoting_approval_queue_page():
    """Operator approval queue — quotes ready for human decision.

    Shows quotes that finished orchestration cleanly (outcome=advanced) and
    landed at qa_pass (ready to generate) or priced (ready for QA). Action
    links route operators back to the source doc for the actual decision.
    """
    from src.api.render import render_page
    rows = _fetch_approval_queue(limit=100)
    by_stage: dict[str, int] = {}
    for r in rows:
        by_stage[r["stage_to"]] = by_stage.get(r["stage_to"], 0) + 1
    return render_page(
        "approval_queue.html",
        active_page="Quoting",
        rows=rows,
        by_stage=by_stage,
    )
