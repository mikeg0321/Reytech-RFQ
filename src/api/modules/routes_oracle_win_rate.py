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


@bp.route("/api/admin/fix-quotewerks-is-test", methods=["POST"])
@auth_required
def api_admin_fix_quotewerks_is_test():
    """One-shot fix: clear is_test=1 on the QuoteWerks-imported quotes.

    Diagnostic confirmed 464 of 479 QuoteWerks-imported quotes ended up
    with is_test=1 (likely set by an existing background sweeper that
    doesn't recognize the QuoteWerks DocNo format). Win-rate analytics
    skips them, so the dashboard shows ~5% of real volume.

    Identifies the QuoteWerks-imported set by status_notes containing
    'QuoteWerks:' or 'SCPRS-verify' (both are markers our importers/
    verify endpoint stamp). Idempotent — re-running is harmless.

    Body: {"dry_run": true} to preview without writing.
    """
    try:
        from src.core.db import get_db
        body = request.json or {}
        dry_run = body.get("dry_run", False)

        with get_db() as conn:
            count_before = conn.execute("""
                SELECT COUNT(*) FROM quotes
                WHERE is_test = 1
                  AND (status_notes LIKE '%QuoteWerks:%'
                       OR status_notes LIKE '%SCPRS-verify%')
            """).fetchone()[0]
            if not dry_run:
                conn.execute("""
                    UPDATE quotes SET is_test = 0
                    WHERE is_test = 1
                      AND (status_notes LIKE '%QuoteWerks:%'
                           OR status_notes LIKE '%SCPRS-verify%')
                """)
            count_after = conn.execute("""
                SELECT COUNT(*) FROM quotes WHERE is_test = 1
            """).fetchone()[0]

        return jsonify({
            "ok": True,
            "dry_run": dry_run,
            "matched_for_unflag": count_before,
            "remaining_test_quotes": count_after,
        })
    except Exception as e:
        log.exception("fix-quotewerks-is-test")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/admin/quotes-diagnostic")
@auth_required
def api_admin_quotes_diagnostic():
    """One-shot diagnostic — distribution of quotes table by
    is_test × status × created_at-bucket. Helps debug why win-rate
    aggregation undercounts."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM quotes"
            ).fetchone()[0]
            by_status = conn.execute("""
                SELECT COALESCE(status,'NULL') as s, COUNT(*) c
                FROM quotes GROUP BY s ORDER BY c DESC
            """).fetchall()
            by_test = conn.execute("""
                SELECT COALESCE(is_test, -1) as t, COUNT(*) c
                FROM quotes GROUP BY t
            """).fetchall()
            by_year = conn.execute("""
                SELECT SUBSTR(COALESCE(created_at,'?'),1,4) as y, COUNT(*) c
                FROM quotes GROUP BY y ORDER BY y
            """).fetchall()
            sample = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       SUBSTR(created_at,1,16) ca, is_test
                FROM quotes
                WHERE quote_number LIKE '25-%' OR quote_number LIKE '26-%'
                ORDER BY created_at DESC LIMIT 5
            """).fetchall()
            counted_in_endpoint = conn.execute("""
                SELECT COUNT(*) FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost', 'sent')
                  AND created_at IS NOT NULL
            """).fetchone()[0]
        return jsonify({
            "ok": True,
            "total": total,
            "by_status": [{"status": r["s"], "count": r["c"]} for r in by_status],
            "by_is_test": [{"is_test": r["t"], "count": r["c"]} for r in by_test],
            "by_year": [{"year": r["y"], "count": r["c"]} for r in by_year],
            "qw_sample": [dict(r) for r in sample],
            "would_count_in_win_rate": counted_in_endpoint,
        })
    except Exception as e:
        log.exception("quotes-diagnostic")
        return jsonify({"ok": False, "error": str(e)}), 500


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
