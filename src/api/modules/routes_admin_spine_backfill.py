# routes_admin_spine_backfill.py — Admin: spine counter + display_number backfill.
#
# Added 2026-05-18 after Mike's 5/18 walk found a quote PDF rendering
# `pc_e96e0408` instead of `R26Q####`. Root cause: that quote was
# ingested BEFORE PR #1040 deployed (the one that added the
# quote_seq/quote_year auto-assignment), so it never got a
# display_number stamped. This module gives the operator a one-call
# path to align the spine counter with their legacy buyer-facing
# counter and then retroactively stamp every unassigned spine quote.
#
# Endpoints:
#   GET  /api/admin/spine/counter/<name>           → current value (or null)
#   POST /api/admin/spine/counter/<name>           → set counter to value
#                                                    body: {"value": int, "actor": "..."}
#   POST /api/admin/spine/backfill-display-numbers → walk every spine quote
#                                                    with quote_seq IS NULL,
#                                                    stamp from next_value().
#                                                    body: {"actor": "...",
#                                                           "dry_run": bool=false}
#
# All require @auth_required (DASH_PASS / X-API-Key surface).
#
# The backfill is IDEMPOTENT — second run is a no-op because the
# already-stamped quotes have quote_seq != NULL on the second read.

import logging
import os
from datetime import datetime, timezone

from flask import jsonify, request

from src.api.shared import auth_required, bp

log = logging.getLogger("reytech.spine_backfill")


def _spine_db_path() -> str:
    """Same resolution as routes_spine + spine_bridge.shadow_ingest."""
    p = os.environ.get("SPINE_DB_PATH")
    if p:
        return p
    try:
        from src.core.paths import DATA_DIR
        return os.path.join(str(DATA_DIR), "spine.db")
    except Exception:
        return os.path.join(os.getcwd(), "data", "spine.db")


# ──────────────────────────────────────────────────────────────────────
# GET /api/admin/spine/counter/<name>
# ──────────────────────────────────────────────────────────────────────


@bp.route("/api/admin/spine/counter/<name>", methods=["GET"])
@auth_required
def admin_get_spine_counter(name: str):
    """Read the current value of a spine counter.

    Returns 200 with {"counter_name": ..., "value": int | None}.
    `value` is None when the counter has never been assigned.
    """
    try:
        from src.spine import get_counter, init_db
        db = _spine_db_path()
        init_db(db)
        value = get_counter(db, name)
    except Exception as e:
        log.exception("admin_get_spine_counter failed for %r", name)
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True, "counter_name": name, "value": value})


# ──────────────────────────────────────────────────────────────────────
# POST /api/admin/spine/counter/<name>
# ──────────────────────────────────────────────────────────────────────


@bp.route("/api/admin/spine/counter/<name>", methods=["POST"])
@auth_required
def admin_set_spine_counter(name: str):
    """Set a spine counter to a specific integer.

    Use case: aligning the spine's quote_<year> counter with Mike's
    legacy buyer-facing counter. E.g., if the last legacy quote was
    R26Q39, POST {"value": 39} to "quote_2026" so the next next_value()
    call returns 40.

    The substrate's set_counter() enforces COUNTER_MAX_JUMP=5 above
    the current value (typo guard); a 0→39 jump on a fresh counter
    needs to be staged. To work around that we walk in steps of 5.
    """
    body = request.get_json(silent=True) or {}
    raw_value = body.get("value")
    actor = (body.get("actor") or "").strip() or "operator"

    if raw_value is None or not isinstance(raw_value, int) or raw_value < 0:
        return jsonify({
            "ok": False,
            "error": "value (non-negative int) is required in body",
        }), 400

    try:
        from src.spine import get_counter, set_counter, init_db, COUNTER_MAX_JUMP
        db = _spine_db_path()
        init_db(db)
        current = get_counter(db, name) or 0
        target = int(raw_value)
        if target < current:
            # Decrement directly — set_counter allows backward correction.
            set_counter(db, name, target, actor=actor)
        else:
            # Walk up in steps to respect COUNTER_MAX_JUMP.
            steps = []
            cursor = current
            while cursor < target:
                cursor = min(cursor + COUNTER_MAX_JUMP, target)
                set_counter(db, name, cursor, actor=actor)
                steps.append(cursor)
            return jsonify({
                "ok": True,
                "counter_name": name,
                "prior_value": current,
                "new_value": target,
                "steps_taken": steps,
                "actor": actor,
            })
        return jsonify({
            "ok": True,
            "counter_name": name,
            "prior_value": current,
            "new_value": target,
            "actor": actor,
        })
    except Exception as e:
        log.exception("admin_set_spine_counter failed for %r", name)
        return jsonify({"ok": False, "error": str(e)}), 400


# ──────────────────────────────────────────────────────────────────────
# POST /api/admin/spine/backfill-display-numbers
# ──────────────────────────────────────────────────────────────────────


@bp.route("/api/admin/spine/backfill-display-numbers", methods=["POST"])
@auth_required
def admin_backfill_display_numbers():
    """Stamp display_number (quote_seq + quote_year) on every spine quote
    that doesn't have one.

    Use this after deploying PR #1040 to fix pre-existing rows that
    never got an auto-assigned quote_seq. Idempotent — already-stamped
    quotes are skipped.

    Body:
      {"actor": "operator",           # required; recorded in event log
       "dry_run": false,              # default false; true → plan only
       "year": 2026}                  # optional; defaults to current UTC year

    Behavior:
      - reads every quote via iter_quote_ids
      - filters to those with quote_seq IS NULL
      - sorts by created_at ASC so older quotes get lower numbers
      - for each: calls next_value("quote_<year>") and write_quote with
        the stamped Quote
      - returns the assignment plan/result

    Returns 200 with {"ok": True, "assigned": [{"quote_id", "display_number"}],
                      "skipped_already_stamped": [...], "dry_run": bool}.
    """
    body = request.get_json(silent=True) or {}
    actor = (body.get("actor") or "").strip() or "operator_backfill"
    dry_run = bool(body.get("dry_run", False))
    year = int(body.get("year") or datetime.now(timezone.utc).year)

    try:
        from src.spine import (
            init_db,
            iter_quote_ids,
            next_value,
            read_quote,
            write_quote,
        )
        db = _spine_db_path()
        init_db(db)

        candidates = []
        skipped = []
        for qid in iter_quote_ids(db):
            q = read_quote(db, qid)
            if q is None:
                continue
            if q.quote_seq is not None and q.quote_year is not None:
                skipped.append({
                    "quote_id": qid,
                    "display_number": q.display_number,
                })
                continue
            candidates.append((q.created_at, q))

        # Oldest-first so legacy quotes get lower numbers, matching how
        # the operator's manual counter would have advanced.
        candidates.sort(key=lambda pair: (pair[0] or datetime.min.replace(tzinfo=timezone.utc)))

        assigned = []
        for _ts, q in candidates:
            if dry_run:
                # Preview: peek at what the next value WOULD be without
                # actually consuming it. There's no peek API; we report
                # the count of unassigned quotes and the current counter
                # value so the operator knows the range.
                continue
            seq = next_value(db, f"quote_{year}", actor=actor)
            stamped = q.model_copy(update={
                "quote_seq": seq,
                "quote_year": year,
            })
            write_quote(db, stamped, actor=actor,
                        note=f"display_number_backfill seq={seq}")
            assigned.append({
                "quote_id": q.quote_id,
                "display_number": stamped.display_number,
            })

        if dry_run:
            from src.spine import get_counter
            current_counter = get_counter(db, f"quote_{year}") or 0
            return jsonify({
                "ok": True,
                "dry_run": True,
                "year": year,
                "would_assign_count": len(candidates),
                "current_counter": current_counter,
                "next_assignment_starts_at": current_counter + 1,
                "skipped_already_stamped_count": len(skipped),
                "first_3_to_assign": [
                    {"quote_id": q.quote_id, "created_at": (ts.isoformat() if ts else None)}
                    for ts, q in candidates[:3]
                ],
            })

        return jsonify({
            "ok": True,
            "dry_run": False,
            "year": year,
            "actor": actor,
            "assigned": assigned,
            "skipped_already_stamped_count": len(skipped),
        })
    except Exception as e:
        log.exception("admin_backfill_display_numbers failed")
        return jsonify({"ok": False, "error": str(e)}), 500
