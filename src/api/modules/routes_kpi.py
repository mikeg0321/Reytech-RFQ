"""KPI Dashboard — Phase A.

Routes:
    GET /kpi                        — Top-level redirect to /kpi/funnel
    GET /kpi/funnel                 — Pipeline funnel + per-buyer win rate
    GET /kpi/orphans                — Orphan-order triage (categorized list)
    GET /api/kpi/funnel             — JSON funnel snapshot
    GET /api/kpi/orphans            — JSON orphan classification snapshot

Phase A surface is READ-ONLY. Operator actions on orphan rows reuse
the existing /api/order/<oid>/link-quote endpoint via inline links.

Predecessor docs:
    docs/KPI_DASHBOARD_SCOPE.md — 5-strip plan, Phase A is strips 3+5
    scripts/rebuild_orphan_orders.py — the substrate this UI reads from
"""
from __future__ import annotations

import logging
import subprocess
import json
import os
from datetime import datetime, timezone

from flask import jsonify, redirect

from src.api.shared import bp, auth_required
from src.api.render import render_page

log = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


def _safe_funnel() -> dict:
    """Read funnel + pipeline counts via the DAL helpers. Returns empty
    dict on any error so the page renders rather than 500-ing."""
    try:
        from src.core.dal import get_pipeline_counts, get_funnel_stats
        return {
            "pipeline": get_pipeline_counts() or {},
            "funnel": get_funnel_stats() or {},
        }
    except Exception as e:
        log.error("kpi funnel fetch: %s", e, exc_info=True)
        return {"pipeline": {}, "funnel": {}, "error": str(e)}


def _top_buyers_winrate(limit: int = 10) -> list[dict]:
    """Top N buyers by RFQ volume with win rate. Delegates to
    `src.core.dal.get_top_buyers_winrate` so the inline status filter
    lives in canonical-state-lint-friendly territory."""
    try:
        from src.core.dal import get_top_buyers_winrate
        return get_top_buyers_winrate(limit=limit)
    except Exception as e:
        log.error("top buyers query: %s", e, exc_info=True)
        return []


def _orphan_classification() -> dict:
    """Run the orphan-rebuild script in JSON dry-run mode and parse
    the result. Subprocess to keep import isolation — the script
    holds its own DB connection."""
    try:
        result = subprocess.run(
            ["python", os.path.join(_PROJECT_ROOT, "scripts",
                                    "rebuild_orphan_orders.py"), "--json"],
            capture_output=True, text=True, timeout=60,
            cwd=_PROJECT_ROOT,
        )
        if result.returncode != 0:
            return {"error": f"script exit={result.returncode}",
                    "stderr": result.stderr[:500]}
        # Script prints non-JSON header lines + the JSON blob. Find the
        # first '{' and parse from there.
        out = result.stdout
        i = out.find("{")
        if i < 0:
            return {"error": "no JSON in output", "stdout": out[:500]}
        return json.loads(out[i:])
    except subprocess.TimeoutExpired:
        return {"error": "orphan-rebuild script timed out"}
    except Exception as e:
        log.error("orphan classification: %s", e, exc_info=True)
        return {"error": str(e)}


@bp.route("/kpi")
@auth_required
def kpi_index():
    return redirect("/kpi/funnel")


@bp.route("/kpi/funnel")
@auth_required
def kpi_funnel_page():
    """Pipeline funnel + per-buyer win rate."""
    snapshot = _safe_funnel()
    top_buyers = _top_buyers_winrate(limit=10)
    return render_page(
        "kpi_funnel.html",
        active_page="KPI",
        snapshot=snapshot,
        top_buyers=top_buyers,
        scanned_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


@bp.route("/api/kpi/funnel")
@auth_required
def api_kpi_funnel():
    return jsonify({
        "ok": True,
        "scanned_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot": _safe_funnel(),
        "top_buyers": _top_buyers_winrate(limit=10),
    })


@bp.route("/kpi/orphans")
@auth_required
def kpi_orphans_page():
    """Orphan-order triage page. READ-only; operator actions reuse the
    existing /api/order/<oid>/link-quote endpoint."""
    classification = _orphan_classification()
    return render_page(
        "kpi_orphans.html",
        active_page="KPI",
        classification=classification,
    )


@bp.route("/api/kpi/orphans")
@auth_required
def api_kpi_orphans():
    return jsonify({
        "ok": True,
        "scanned_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "classification": _orphan_classification(),
    })
