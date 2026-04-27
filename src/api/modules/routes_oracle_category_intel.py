# routes_oracle_category_intel.py
#
# Phase 4.6 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-26).
#
# Surfaces a category-level historical win/loss bucket for any item
# description. Direct response to the 2026-04-26 finding that
# orthopedic footwear is 0/111 across two years.
#
# When Mike pricing a Propet shoe → /api/oracle/category-intel hits
# returns danger=true with the bucket history, so the front-end can
# show a red "DO NOT BID at current markup" banner.

import json
import logging
from collections import defaultdict

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.intel_categories import intel_category, all_categories

log = logging.getLogger("reytech")


def _danger_threshold(quotes: int, win_rate_pct: float | None) -> bool:
    """Return True iff this bucket is statistically a known loser.

    Rules:
      - need ≥ 5 quotes (single-quote flukes don't count)
      - win rate < 15% (against ~21% all-time baseline → meaningfully
        below average; 10% was too strict and missed footwear at 12.9%)
    """
    if quotes < 5:
        return False
    if win_rate_pct is None:
        return False
    return win_rate_pct < 15.0


def _aggregate_category(description: str, agency_filter: str = ""):
    """Walk the quotes table, classify every line item, and bucket
    by intel category. Returns the rollup for the input description's
    category — the rest is dropped to keep the response small."""
    from src.core.db import get_db

    target_cat, target_label = intel_category(description)

    # Walk quotes, accumulate per-category rollup so we can include
    # a "compare to other buckets" hint in the response.
    rollup = defaultdict(lambda: {
        "quotes": 0, "wins": 0, "losses": 0,
        "won_value": 0.0, "lost_value": 0.0,
        "label": "",
    })

    with get_db() as conn:
        sql = """
            SELECT status, agency, institution, line_items, total
            FROM quotes
            WHERE is_test = 0
              AND status IN ('won', 'lost')
              AND line_items IS NOT NULL
        """
        rows = conn.execute(sql).fetchall()

    # Expand agency to all known match-patterns so an "CCHCS" filter still
    # hits rows stored as "California Correctional Health Care Services".
    try:
        from src.core.agency_config import resolve_agency_patterns
        agency_patterns = resolve_agency_patterns(agency_filter or "")
    except Exception:
        agency_patterns = [(agency_filter or "").strip().lower()] if agency_filter else []

    for r in rows:
        if agency_patterns:
            row_agency = (r["agency"] or r["institution"] or "").lower()
            if not any(p in row_agency for p in agency_patterns):
                continue
        try:
            items = json.loads(r["line_items"] or "[]")
        except (TypeError, ValueError):
            continue
        if not isinstance(items, list):
            continue

        # Quote-level outcome applies to every line item in the quote
        # (we don't have per-line outcomes — that's the realistic
        # assumption since the buyer awards or doesn't award the bid).
        st = r["status"]
        try:
            total = float(r["total"] or 0)
        except (TypeError, ValueError):
            total = 0.0
        # Distribute total across line items as a proxy for line value.
        denom = max(1, len(items))
        per_line = total / denom if total else 0.0

        # Track which categories this quote hit so we don't double-
        # count a quote that has 5 items all in the same category.
        seen_cats = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            desc = it.get("description", "") or ""
            cat_id, cat_label = intel_category(desc)
            if cat_id in seen_cats:
                continue
            seen_cats.add(cat_id)
            b = rollup[cat_id]
            b["label"] = cat_label
            b["quotes"] += 1
            if st == "won":
                b["wins"] += 1
                b["won_value"] += per_line
            elif st == "lost":
                b["losses"] += 1
                b["lost_value"] += per_line

    target_bucket = rollup.get(target_cat, {
        "quotes": 0, "wins": 0, "losses": 0,
        "won_value": 0.0, "lost_value": 0.0,
        "label": target_label,
    })
    target_decided = target_bucket["wins"] + target_bucket["losses"]
    target_rate = (round(100.0 * target_bucket["wins"] / target_decided, 1)
                   if target_decided else None)

    # Top 5 *other* buckets by quote volume — gives Mike comparative
    # context without a full dump.
    others = []
    for cat_id, b in rollup.items():
        if cat_id == target_cat:
            continue
        decided = b["wins"] + b["losses"]
        rate = (round(100.0 * b["wins"] / decided, 1) if decided else None)
        others.append({
            "category": cat_id,
            "label": b["label"],
            "quotes": b["quotes"],
            "wins": b["wins"],
            "losses": b["losses"],
            "win_rate_pct": rate,
        })
    others.sort(key=lambda x: x["quotes"], reverse=True)
    others = others[:5]

    return target_cat, target_label, target_bucket, target_rate, others


@bp.route("/api/oracle/category-intel")
@auth_required
def api_oracle_category_intel():
    """Return historical win/loss bucket for the category implied by
    the input description.

    Query params:
        description (str, required) — the item description to classify
        agency (str, optional) — substring filter on agency name
                                 (case-insensitive)

    Response:
      {
        ok, input_description,
        category, category_label,
        quotes, wins, losses, win_rate_pct,
        won_value, lost_value,
        danger (bool), warning_text (str|null),
        other_categories: [...top 5 by volume...],
      }
    """
    description = (request.args.get("description") or "").strip()
    if not description:
        return jsonify({"ok": False,
                        "error": "description query param required"}), 400
    agency = (request.args.get("agency") or "").strip()

    try:
        cat_id, cat_label, bucket, rate, others = _aggregate_category(
            description, agency_filter=agency
        )
    except Exception as e:
        log.exception("category_intel aggregation")
        return jsonify({"ok": False, "error": str(e)}), 500

    quotes = bucket["quotes"]
    wins = bucket["wins"]
    losses = bucket["losses"]
    danger = _danger_threshold(quotes, rate)

    warning_text = None
    if danger:
        warning_text = (f"LOSS BUCKET: {wins}/{quotes} wins on "
                        f"{cat_label}. Recalibrate markup before bidding.")
    elif quotes >= 5 and rate is not None and rate >= 50:
        # Bonus: surface the high-win flag too — same shape, opposite
        # signal. Threshold = 50% because Mike's all-time baseline is
        # ~21%; 50%+ is dominant against that.
        warning_text = (f"WIN BUCKET: {wins}/{quotes} wins on "
                        f"{cat_label}. Confident territory.")

    return jsonify({
        "ok": True,
        "input_description": description,
        "agency_filter": agency,
        "category": cat_id,
        "category_label": cat_label,
        "quotes": quotes,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": rate,
        "won_value": round(bucket["won_value"], 2),
        "lost_value": round(bucket["lost_value"], 2),
        "danger": danger,
        "warning_text": warning_text,
        "other_categories": others,
    })


@bp.route("/api/oracle/category-intel-flavor")
@auth_required
def api_oracle_category_intel_flavor():
    """Phase 4.7 introspection: report which modulation flavor is
    active and where the value came from.

    Source values:
      - 'flag'    → runtime flag oracle.category_intel_flavor is set
      - 'env'     → env var CATEGORY_INTEL_FLAVOR is set
      - 'default' → neither set; using B (suggest)

    To flip flavors at runtime without a redeploy:
      POST /api/admin/flags
        body: {"key": "oracle.category_intel_flavor", "value": "A"}

    Or delete the flag to fall back to env / default:
      DELETE /api/admin/flags/oracle.category_intel_flavor
    """
    try:
        from src.core.category_intel_modulation import (
            _get_flavor_source, FLAG_KEY,
        )
        flavor, source = _get_flavor_source()
        descriptions = {
            "A": "auto_lower — engine markup damped 50% on danger buckets",
            "B": "suggest — sidecar suggested_alternative; engine unchanged",
            "C": "block — hard-block on severe loss buckets (rate<8%, n>=10)",
            "OFF": "modulation disabled — engine fully untouched",
        }
        return jsonify({
            "ok": True,
            "flavor": flavor,
            "source": source,
            "description": descriptions.get(flavor, "unknown"),
            "flag_key": FLAG_KEY,
        })
    except Exception as e:
        log.exception("category_intel_flavor")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/oracle/category-list")
@auth_required
def api_oracle_category_list():
    """Diagnostic / admin: full list of intel categories the
    classifier knows about. For wiring sanity checks."""
    return jsonify({
        "ok": True,
        "categories": [
            {"id": cid, "label": label}
            for (cid, label) in all_categories().items()
        ],
    })


@bp.route("/api/oracle/category-summary")
@auth_required
def api_oracle_category_summary():
    """Phase 4.6.2: at-a-glance rollup of ALL intel categories.

    Walks the quotes table once, buckets every line item by intel
    category, returns one row per category with quotes/wins/losses/
    win_rate/danger/warning. Sorted with danger=true first, then by
    quote volume.

    Use case: Mike wants to know 'which categories am I losing right
    now' without probing each one individually.

    Query params:
        agency (str, optional) — substring filter
        min_quotes (int, optional, default 1) — drop low-volume rows

    Response:
      {
        ok, agency_filter, min_quotes,
        categories: [
          {category, category_label, quotes, wins, losses,
           win_rate_pct, danger, warning_text, won_value, lost_value},
          ...
        ],
        overall: {quotes, wins, losses, win_rate_pct,
                  danger_buckets, win_buckets},
      }
    """
    from src.core.db import get_db

    agency = (request.args.get("agency") or "").strip()
    try:
        min_quotes = max(1, int(request.args.get("min_quotes", "1")))
    except (TypeError, ValueError):
        min_quotes = 1

    rollup = defaultdict(lambda: {
        "quotes": 0, "wins": 0, "losses": 0,
        "won_value": 0.0, "lost_value": 0.0,
        "label": "",
    })

    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT status, agency, institution, line_items, total
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost')
                  AND line_items IS NOT NULL
            """).fetchall()
    except Exception as e:
        log.exception("category_summary load")
        return jsonify({"ok": False, "error": str(e)}), 500

    try:
        from src.core.agency_config import resolve_agency_patterns
        agency_patterns = resolve_agency_patterns(agency)
    except Exception:
        agency_patterns = [agency.lower()] if agency else []
    for r in rows:
        if agency_patterns:
            row_a = (r["agency"] or r["institution"] or "").lower()
            if not any(p in row_a for p in agency_patterns):
                continue
        try:
            items = json.loads(r["line_items"] or "[]")
        except (TypeError, ValueError):
            continue
        if not isinstance(items, list):
            continue
        try:
            total = float(r["total"] or 0)
        except (TypeError, ValueError):
            total = 0.0
        denom = max(1, len(items))
        per_line = total / denom if total else 0.0

        seen_cats = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            from src.core.intel_categories import intel_category
            cid, label = intel_category(it.get("description") or "")
            if cid in seen_cats:
                continue
            seen_cats.add(cid)
            b = rollup[cid]
            b["label"] = label
            b["quotes"] += 1
            if r["status"] == "won":
                b["wins"] += 1
                b["won_value"] += per_line
            elif r["status"] == "lost":
                b["losses"] += 1
                b["lost_value"] += per_line

    out = []
    danger_n = win_n = 0
    overall_q = overall_w = overall_l = 0
    for cid, b in rollup.items():
        if b["quotes"] < min_quotes:
            continue
        decided = b["wins"] + b["losses"]
        rate = (round(100.0 * b["wins"] / decided, 1) if decided else None)
        danger = (b["quotes"] >= 5 and rate is not None and rate < 15.0)
        warning = None
        if danger:
            warning = (f"LOSS BUCKET: {b['wins']}/{b['quotes']} wins on "
                       f"{b['label']}. Recalibrate markup before bidding.")
            danger_n += 1
        elif b["quotes"] >= 5 and rate is not None and rate >= 50.0:
            warning = (f"WIN BUCKET: {b['wins']}/{b['quotes']} wins on "
                       f"{b['label']}. Confident territory.")
            win_n += 1
        out.append({
            "category": cid,
            "category_label": b["label"] or cid,
            "quotes": b["quotes"],
            "wins": b["wins"],
            "losses": b["losses"],
            "win_rate_pct": rate,
            "won_value": round(b["won_value"], 2),
            "lost_value": round(b["lost_value"], 2),
            "danger": danger,
            "warning_text": warning,
        })
        overall_q += b["quotes"]
        overall_w += b["wins"]
        overall_l += b["losses"]

    # Sort: danger first, then by quote volume desc
    out.sort(key=lambda x: (
        not x["danger"],
        -x["quotes"],
    ))

    overall_decided = overall_w + overall_l
    overall_rate = (round(100.0 * overall_w / overall_decided, 1)
                    if overall_decided else None)

    return jsonify({
        "ok": True,
        "agency_filter": agency,
        "min_quotes": min_quotes,
        "categories": out,
        "overall": {
            "quotes": overall_q,
            "wins": overall_w,
            "losses": overall_l,
            "win_rate_pct": overall_rate,
            "danger_buckets": danger_n,
            "win_buckets": win_n,
        },
    })
