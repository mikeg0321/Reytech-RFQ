# routes_oracle_win_rate.py
#
# Phase 4.4 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-26).
# Per-agency win-rate analytics. Mike has 102 wins / 479 quotes (21%);
# this endpoint surfaces WHERE he's winning so he can prioritize.
#
# Powered by the Phase 0.7d historical backfill (QuoteWerks + SCPRS-wins
# imports). Aggregates the `quotes` table by canonical agency + date
# window into a ranked rollup.

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger("reytech")


def _normalize_agency(raw: str) -> str:
    """Collapse spelling variants of the same buyer into a canonical
    bucket so wins/losses for 'CA State Prison Sacramento' aggregate
    with 'California State Prison - Sacramento'.

    Heuristic: lowercase, strip non-alphanumeric, collapse common
    facility-prefix synonyms. Conservative — if in doubt, keep distinct.
    """
    s = (raw or "").strip().lower()
    if not s:
        return ""
    # Drop punctuation
    out = "".join(c if c.isalnum() else " " for c in s)
    # Collapse whitespace
    tokens = [t for t in out.split() if t]
    # Drop stopword tokens
    stop = {"of", "the", "and", "for", "ca", "california", "dept",
            "department", "inc", "co", "company", "corp", "corporation",
            "rehab", "rehabilitation"}
    tokens = [t for t in tokens if t not in stop]
    return " ".join(tokens)


@bp.route("/api/oracle/win-rate-by-agency")
@auth_required
def api_oracle_win_rate_by_agency():
    """Per-agency win-rate rollup over the last N days.

    Query params:
        days (int, optional, default 365) — lookback window
        min_quotes (int, optional, default 3) — only show agencies with
            ≥ this many quotes (cuts noise)

    Response:
      {
        ok, days, min_quotes, overall: {quotes, wins, losses, win_rate_pct,
                                        won_value, lost_value},
        agencies: [
          {
            canonical_name, display_name (most-common spelling),
            quotes, wins, losses, win_rate_pct,
            won_value, lost_value,
            recent_quote_dates: [...up to 5 most recent ISO dates]
          }, ...
        ]
      }

    Sorted by quote count descending so the busiest buyers come first —
    that's where to focus the calibration work.
    """
    try:
        days = max(1, min(3650, int(request.args.get("days", "365"))))
    except (TypeError, ValueError):
        days = 365
    try:
        min_quotes = max(1, int(request.args.get("min_quotes", "3")))
    except (TypeError, ValueError):
        min_quotes = 3

    cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()

    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       total, created_at
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost', 'sent')
                  AND created_at >= ?
            """, (cutoff,)).fetchall()
    except Exception as e:
        log.exception("win_rate_by_agency load")
        return jsonify({"ok": False, "error": str(e)}), 500

    # Bucket by canonical agency
    buckets = defaultdict(lambda: {
        "raw_names": defaultdict(int),
        "quotes": 0, "wins": 0, "losses": 0, "sent": 0,
        "won_value": 0.0, "lost_value": 0.0,
        "recent_dates": [],
    })

    for r in rows:
        agency_raw = (r["agency"] or r["institution"] or "").strip()
        if not agency_raw:
            continue
        canonical = _normalize_agency(agency_raw)
        if not canonical:
            continue
        b = buckets[canonical]
        b["raw_names"][agency_raw] += 1
        b["quotes"] += 1
        try:
            total = float(r["total"] or 0)
        except (TypeError, ValueError):
            total = 0.0
        st = r["status"]
        if st == "won":
            b["wins"] += 1
            b["won_value"] += total
        elif st == "lost":
            b["losses"] += 1
            b["lost_value"] += total
        else:
            b["sent"] += 1
        b["recent_dates"].append(r["created_at"])

    agencies = []
    overall_q = overall_w = overall_l = 0
    overall_wv = overall_lv = 0.0
    for canonical, b in buckets.items():
        if b["quotes"] < min_quotes:
            continue
        decided = b["wins"] + b["losses"]
        win_rate = round(100.0 * b["wins"] / decided, 1) if decided else None
        # Pick most-common spelling as display name
        display = sorted(b["raw_names"].items(),
                         key=lambda kv: kv[1], reverse=True)[0][0]
        # Most-recent 5 dates
        recent = sorted([d for d in b["recent_dates"] if d],
                        reverse=True)[:5]
        agencies.append({
            "canonical_name": canonical,
            "display_name": display,
            "quotes": b["quotes"],
            "wins": b["wins"],
            "losses": b["losses"],
            "sent": b["sent"],
            "win_rate_pct": win_rate,
            "won_value": round(b["won_value"], 2),
            "lost_value": round(b["lost_value"], 2),
            "recent_quote_dates": recent,
        })
        overall_q += b["quotes"]
        overall_w += b["wins"]
        overall_l += b["losses"]
        overall_wv += b["won_value"]
        overall_lv += b["lost_value"]

    # Sort: most-quoted first
    agencies.sort(key=lambda a: a["quotes"], reverse=True)

    overall_decided = overall_w + overall_l
    overall_rate = (round(100.0 * overall_w / overall_decided, 1)
                    if overall_decided else None)

    return jsonify({
        "ok": True,
        "days": days,
        "min_quotes": min_quotes,
        "overall": {
            "quotes": overall_q,
            "wins": overall_w,
            "losses": overall_l,
            "win_rate_pct": overall_rate,
            "won_value": round(overall_wv, 2),
            "lost_value": round(overall_lv, 2),
        },
        "agencies": agencies,
    })
