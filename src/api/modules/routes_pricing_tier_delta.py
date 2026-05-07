"""Telemetry endpoint for pricing-tier vs row-bid delta logging.

Post-quote queue item #6 (2026-05-07). PR #818 closed the
showTierComparison() bug where the tier panel projected revenue from
stale Amazon catalog prices instead of the operator's edited cost.
This module is the watcher: every time the operator edits a row,
the client computes (a) what the tier panel projects for the active
buffer and (b) what the actual row bid extensions sum to. If those
diverge by more than the noise floor, the client fires a beacon and
we append a JSONL row.

Path-1 logging only. No QA blocker, no UI change beyond the silent
beacon. After ~1 week of clean data we'll know if a real-time
divergence guard is warranted (and at what threshold).

Same shape as `data/thread_dedup_log.jsonl` (PR-B/#809) so the
existing audit-doc patterns (rotate after 30d, tail with jq) apply.

Endpoints
---------
POST /api/pricing-tier-delta
    Body:
      {
        "context":      "pc"|"rfq",
        "record_id":    "pc_xxx" or "rfq_xxx",
        "active_buf":   0|10|15|20,
        "tier_revenue": 1234.56,    # tier panel projection
        "row_revenue":  1234.05,    # sum(price * qty) across rows
        "tier_cost":    876.54,
        "item_count":   N,
        "delta":        0.51,
        "url":          "/pricecheck/<id>"   (optional)
      }
    Beacon-friendly: accepts text/plain JSON without CSRF preflight.
    Returns 204 No Content on success.

GET /api/pricing-tier-delta/summary?days=14
    Returns aggregate stats for the operator dashboard:
      {
        "ok": true,
        "days": 14,
        "total_logged": N,
        "by_day": {"2026-05-07": 12, ...},
        "by_record": {"pc_xxx": 4, ...},
        "max_delta": 23.45,
        "recent": [last 20 raw rows],
      }
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from collections import Counter

from flask import jsonify, request
from src.api.shared import bp, auth_required

log = logging.getLogger("reytech.pricing_tier_delta")

_LOG_FILENAME = "pricing_tier_delta_log.jsonl"
_DELTA_NOISE_FLOOR = 0.50  # cents-of-rounding tolerance, see _is_actionable
_RATE_LIMIT_WINDOW = 2.0   # seconds — same record can't fire faster than this
_recent_fires: dict = {}    # record_id → last fire epoch (in-memory dedup)


def _data_dir() -> str:
    """Resolve DATA_DIR. The dashboard module sets this as a global;
    fall back to data/ relative to cwd if unavailable."""
    try:
        from src.api.config import DATA_DIR
        return DATA_DIR
    except Exception:
        return os.path.abspath("data")


def _is_actionable(delta: float) -> bool:
    """Filter pure rounding noise. Per-row extensions can drift by
    cents across many rows; > $0.50 is the substrate threshold also
    used by the canonical pricing-alignment gate (#810)."""
    try:
        return abs(float(delta)) > _DELTA_NOISE_FLOOR
    except (TypeError, ValueError):
        return False


def _rate_limit_ok(record_id: str) -> bool:
    """Drop beacons fired faster than _RATE_LIMIT_WINDOW for the same
    record_id. The frontend re-runs showTierComparison on every
    keystroke; we only want one log line per logical edit."""
    if not record_id:
        return True
    now = time.time()
    last = _recent_fires.get(record_id, 0)
    if now - last < _RATE_LIMIT_WINDOW:
        return False
    _recent_fires[record_id] = now
    # Trim cache if it grows past 500 records (memory cap).
    if len(_recent_fires) > 500:
        cutoff = now - 600  # 10min
        for k in list(_recent_fires.keys()):
            if _recent_fires[k] < cutoff:
                del _recent_fires[k]
    return True


def _safe_str(v, max_len: int = 200) -> str:
    if v is None:
        return ""
    return str(v)[:max_len]


def _safe_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


@bp.route("/api/pricing-tier-delta", methods=["POST"])
def api_pricing_tier_delta_log():
    """Beacon endpoint — append a tier-vs-row delta event to the
    JSONL log. Open route (no @auth_required) so the beacon fires
    even when the page session expired mid-edit. Defended by:
      * record_id rate limit (silent drop on duplicate keystrokes)
      * delta noise floor ($0.50)
      * payload size cap on _safe_str
      * append-only — never reads operator state
    """
    try:
        # Accept either application/json OR beacon's text/plain.
        body = request.get_json(silent=True)
        if body is None:
            try:
                body = json.loads(request.get_data(as_text=True) or "{}")
            except (ValueError, TypeError):
                body = {}
        if not isinstance(body, dict):
            return ("", 204)

        delta = _safe_float(body.get("delta"))
        if not _is_actionable(delta):
            return ("", 204)

        record_id = _safe_str(body.get("record_id"), 64)
        if not _rate_limit_ok(record_id):
            return ("", 204)

        entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "context": _safe_str(body.get("context"), 16),
            "record_id": record_id,
            "active_buf": _safe_float(body.get("active_buf")),
            "tier_revenue": _safe_float(body.get("tier_revenue")),
            "row_revenue": _safe_float(body.get("row_revenue")),
            "tier_cost": _safe_float(body.get("tier_cost")),
            "item_count": int(_safe_float(body.get("item_count"))),
            "delta": delta,
            "url": _safe_str(body.get("url"), 240),
            "ua": _safe_str(request.headers.get("User-Agent", ""), 200),
        }

        log_path = os.path.join(_data_dir(), _LOG_FILENAME)
        try:
            os.makedirs(_data_dir(), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            log.debug("tier-delta log write suppressed: %s", e)

        log.info("pricing-tier-delta: record=%s delta=$%.2f buf=%s "
                 "items=%d (Path 1, logging only)",
                 entry["record_id"], entry["delta"],
                 entry["active_buf"], entry["item_count"])
        return ("", 204)
    except Exception as e:
        log.error("tier-delta log error: %s", e, exc_info=True)
        # Never propagate — the beacon must be fire-and-forget.
        return ("", 204)


@bp.route("/api/pricing-tier-delta/summary", methods=["GET"])
@auth_required
def api_pricing_tier_delta_summary():
    """Aggregate the JSONL log over a recent window. Cheap; reads at
    most ~10K lines per call (the file rotates well before that)."""
    try:
        days = max(1, min(int(request.args.get("days", 14) or 14), 90))
    except (TypeError, ValueError):
        days = 14

    log_path = os.path.join(_data_dir(), _LOG_FILENAME)
    if not os.path.exists(log_path):
        return jsonify({
            "ok": True, "days": days, "total_logged": 0,
            "by_day": {}, "by_record": {}, "max_delta": 0.0,
            "recent": [],
        })

    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"
    by_day: Counter = Counter()
    by_record: Counter = Counter()
    max_delta = 0.0
    rows: list = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except (ValueError, TypeError):
                    continue
                ts = e.get("ts", "")
                if ts < cutoff:
                    continue
                day = ts[:10]
                by_day[day] += 1
                rid = e.get("record_id", "")
                if rid:
                    by_record[rid] += 1
                d = abs(_safe_float(e.get("delta")))
                if d > max_delta:
                    max_delta = d
                rows.append(e)
    except Exception as ex:
        log.error("tier-delta summary read error: %s", ex, exc_info=True)
        return jsonify({"ok": False, "error": str(ex)}), 500

    recent = rows[-20:] if len(rows) > 20 else rows
    return jsonify({
        "ok": True,
        "days": days,
        "total_logged": len(rows),
        "by_day": dict(by_day),
        "by_record": dict(by_record.most_common(20)),
        "max_delta": round(max_delta, 2),
        "recent": recent,
    })
