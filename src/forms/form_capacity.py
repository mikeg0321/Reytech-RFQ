"""Per-form row capacity registry + pre-fill overflow check.

Mike P0 2026-05-06 RFQ a5b09b56 (8 items): server log emitted

    [wrn]  CalRecycle 74: 8 items but only 6 rows on template.
    Items 7+ not listed.

…and items 7-8 were silently dropped from the CalRecycle 74 form. The
warning is invisible to the operator. Mike's prior quotes have had up
to 37 items — every form whose row capacity is exceeded silently
drops the overflow on every quote.

This module is the substrate fix (post-quote queue item 22):

  1. **Registry** — `FORM_CAPACITY` maps every known form_id to its
     row capacity per page + whether an overflow path exists.
  2. **`check_overflow(form_id, item_count)`** — pre-fill check used
     by the package generator. Returns a structured result so the
     completeness gate can surface a QA blocker for the operator.
  3. **`get_capacity(form_id)`** — single-form lookup for fillers
     that want to know the limit.

The registry is intentionally conservative — every value is pinned
in `tests/test_form_capacity.py` against pdfplumber-measured truth
on the actual template files in `data/templates/`. New entries
require a measurement-pinned test, not a guess.
"""
from __future__ import annotations

from typing import Optional


# ─── Capacity registry ────────────────────────────────────────────────
# Each entry:
#   - rows_pg1: row count on page 1 (form-field rows)
#   - rows_pg2: row count on page 2 (`_2`-suffix or equivalent), or 0
#   - has_overflow: True when a pages-3+ overflow path exists (e.g.
#     `_append_overflow_pages` for AMS 704 master). When False, items
#     past `rows_pg1 + rows_pg2` are SILENTLY DROPPED unless the
#     package generator surfaces a blocker.
#   - overflow_fn: name of the helper, for documentation. None when
#     no path exists.
#   - max_safe_items: rows_pg1 + rows_pg2 (or sys.maxsize when
#     overflow path exists). The check below uses this.

FORM_CAPACITY: dict[str, dict] = {
    # AMS 704 master template — Reytech-controlled.
    # CLAUDE.md: 11 rows on page 1, 8 rows on page 2 (`_2` suffix),
    # pages 3+ via reportlab canvas overlay.
    "704": {
        "rows_pg1": 11,
        "rows_pg2": 8,
        "has_overflow": True,
        "overflow_fn": "_append_overflow_pages",
    },
    # AMS 704B — buyer-supplied template. Variants exist (15 rows on
    # page 1 alone is one common shape; the master 11+8 is another).
    # OVERFLOW PATH (added 2026-05-28 / Coleman 10842771): when items
    # exceed the template's capacity, `fill_704b` (reytech_filler_v4.py)
    # chunks the items into capacity-sized groups, fills the empty
    # template once per chunk, flattens each filled copy, and
    # concatenates the chunks into a multi-page output. Each chunk
    # carries its own merchandise_subtotal; the Reytech Quote PDF
    # carries the grand total.
    "704b": {
        "rows_pg1": 15,
        "rows_pg2": 0,
        "has_overflow": True,
        "overflow_fn": "fill_704b:chunked_refill",
    },
    # CalRecycle 74 — bid-package internal form. 6 rows on page 1 + SABRC
    # reference table on page 2. Overflow IS implemented at
    # `reytech_filler_v4.py:3110-3155`: when items > 6, additional
    # CalRecycle 74 line-item pages are appended (each carrying 6 more
    # rows) and the SABRC reference table is preserved at the end.
    # Registry was previously stale and blocked QA on 15-item CalVet RFQs
    # (Mike P0 2026-05-12 rfq_8efe9fae).
    "calrecycle74": {
        "rows_pg1": 6,
        "rows_pg2": 0,
        "has_overflow": True,
        "overflow_fn": "fill_calrecycle_standalone:append_overflow",
    },
    # OBS 1600 (CA Agricultural Food Product Cert) — 18 rows per
    # CLAUDE.md. Reytech doesn't grow any food, so this is always
    # filled with N/A — overflow rarely matters in practice. Marking
    # has_overflow=False so future food-heavy quotes still surface
    # an explicit blocker.
    "obs_1600": {
        "rows_pg1": 18,
        "rows_pg2": 0,
        "has_overflow": False,
        "overflow_fn": None,
    },
    # 703B — request-for-quotation header form. Doesn't carry line
    # items in the standard variant; capacity is irrelevant for the
    # row sense. Marked here for completeness so a future variant
    # that DID carry items would prompt registry update.
    "703b": {
        "rows_pg1": 0,
        "rows_pg2": 0,
        "has_overflow": False,
        "overflow_fn": None,
    },
}


def get_capacity(form_id: str) -> Optional[dict]:
    """Return the capacity entry for `form_id`, or None if unknown."""
    return FORM_CAPACITY.get((form_id or "").lower().strip())


def check_overflow(form_id: str, item_count: int) -> dict:
    """Pre-fill capacity check.

    Returns a structured result the package generator's completeness
    gate consumes:

      {
        "form_id": "calrecycle74",
        "registered": True,
        "ok": False,                  # False = items exceed capacity
        "items_total": 8,
        "items_capacity": 6,
        "items_dropped": 2,
        "has_overflow": False,
        "severity": "blocker",        # "blocker" | "warn" | "ok"
        "message": "CalRecycle 74 has 6 rows; quote has 8 items. Items 7-8 will not appear on the form. Hand-fill required for overflow, or split into multiple submissions.",
      }

    For unknown form_ids: returns `{"registered": False, ...}` and
    severity="ok" — unknown forms aren't blocked (false-positive
    avoidance) but ARE logged so operator can register the form.
    """
    cap = get_capacity(form_id)
    if cap is None:
        return {
            "form_id": form_id,
            "registered": False,
            "ok": True,
            "items_total": item_count,
            "items_capacity": None,
            "items_dropped": 0,
            "has_overflow": None,
            "severity": "ok",
            "message": (
                f"Form '{form_id}' not in capacity registry — "
                "consider adding to src/forms/form_capacity.py"
            ),
        }

    capacity = (cap["rows_pg1"] or 0) + (cap["rows_pg2"] or 0)
    has_overflow = bool(cap["has_overflow"])

    # Capacity == 0 means "this form doesn't carry line items" — pass
    # through unconditionally regardless of item_count.
    if capacity == 0:
        return {
            "form_id": form_id,
            "registered": True,
            "ok": True,
            "items_total": item_count,
            "items_capacity": 0,
            "items_dropped": 0,
            "has_overflow": has_overflow,
            "severity": "ok",
            "message": "Form does not carry line items.",
        }

    if has_overflow or item_count <= capacity:
        return {
            "form_id": form_id,
            "registered": True,
            "ok": True,
            "items_total": item_count,
            "items_capacity": capacity,
            "items_dropped": 0,
            "has_overflow": has_overflow,
            "severity": "ok",
            "message": (
                f"Within capacity ({item_count}/{capacity})."
                if not has_overflow
                else f"Capacity {capacity} + overflow path available."
            ),
        }

    dropped = item_count - capacity
    first_dropped_line = capacity + 1
    last_dropped_line = item_count
    return {
        "form_id": form_id,
        "registered": True,
        "ok": False,
        "items_total": item_count,
        "items_capacity": capacity,
        "items_dropped": dropped,
        "has_overflow": False,
        "severity": "blocker",
        "message": (
            f"{form_id.upper()} has {capacity} rows but quote has "
            f"{item_count} items. Items {first_dropped_line}-"
            f"{last_dropped_line} will NOT appear on the form. "
            "Hand-fill required for overflow, or split into multiple "
            "submissions."
        ),
    }


def check_required_forms(required_form_ids: list[str], item_count: int) -> dict:
    """Walk every required form and return aggregate check.

    Returns `{ok, blockers, warnings, by_form}`. The package generator
    can surface `blockers` as QA blockers via the existing
    `errors.append(...)` + `_lle("rfq", ..., "package_incomplete", ...)`
    path that PR D wired in `routes_rfq_gen.py:2630`.
    """
    by_form = {}
    blockers = []
    warnings = []
    for fid in required_form_ids:
        result = check_overflow(fid, item_count)
        by_form[fid] = result
        if result["severity"] == "blocker":
            blockers.append(result)
        elif result["severity"] == "warn":
            warnings.append(result)
    return {
        "ok": not blockers,
        "blockers": blockers,
        "warnings": warnings,
        "by_form": by_form,
    }
