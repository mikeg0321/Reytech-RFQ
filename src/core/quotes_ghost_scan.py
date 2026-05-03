"""Read-only ghost scan over the `quotes` SQLite table.

Background — PR #675 + PR #699 gated *future* allocations of
quote-counter sequences against placeholder/ghost source data. They
don't backfill clean any quote-table rows that already had a real
seq burned on them before the gates landed.

The session memo `project_session_2026_05_01_ghost_quote_arc.md`
flagged this as item #4 of the post-arc punch list:
  > 504 entries in the quotes table aren't audited yet — only the
  > rfqs.json side. Some quotes table rows likely have placeholder
  > solicitation_number with real counter seqs burned. Scope: extend
  > the scan to walk the quotes table, classify by ghost markers,
  > expose to operator. Don't auto-delete — that touches financial
  > history.

This module does the read-only enumeration. It walks every quote and
buckets it by ghost markers. **It never writes.** The output is a
structured report the operator reviews; the ghost-binding clear path
already exists at `/api/admin/clear-ghost-quote-bindings` for the
RFQs.json side. A symmetric quotes-table cleanup endpoint can be
built later — after Mike has eyes on the actual numbers.

Ghost markers checked per quote row:

  * **placeholder source** — the quote was generated from an RFQ or
    PC whose identifier was a placeholder. We resolve the source by
    `rfq_number`, `source_rfq_id`, or `source_pc_id`, and run
    `is_ready_for_quote_allocation` / `is_ready_for_pc_quote_allocation`
    against it. If the source is itself a ghost, the quote is too.

  * **orphaned source** — `rfq_number` / `source_rfq_id` /
    `source_pc_id` is set, but the referenced record is gone (deleted,
    never persisted). The quote is hanging in the air.

  * **no source link at all** — none of the source fields populated.
    Some operator-direct quotes are legitimately like this; the
    classifier defers to the row's own data quality.

  * **own-data ghost markers** — empty institution AND total=0 AND
    items_count=0 → almost certainly a draft that was never finished.
    Reytech buyer email on the quote → parser misclassified sender.

This is deliberately conservative. The bucket counts and the
representative quote_numbers are the deliverable; per-row clearance
needs Mike's review per the no-ghost-data feedback.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

log = logging.getLogger("quotes_ghost_scan")


def _safe_json_loads(raw) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _row_to_dict(row) -> dict:
    return dict(row) if hasattr(row, "keys") else dict(row)


def _quote_has_own_ghost_markers(q: dict) -> list[str]:
    """Markers detectable from the quote row alone, no source lookup."""
    reasons: list[str] = []

    # Reytech buyer on a quote that we sent — parser misclassified sender.
    contact_email = (q.get("contact_email") or "").lower().strip()
    if contact_email.endswith("@reytechinc.com"):
        reasons.append(
            f"contact_email {contact_email!r} is a Reytech address — "
            "Reytech is never the buyer on a quote we sent"
        )

    # An "empty draft" pattern: no institution, no items, no total. These
    # are almost always abandoned in-flight ghosts, not real quotes.
    inst = (q.get("institution") or "").strip()
    items_count = int(q.get("items_count") or 0)
    items_detail = _safe_json_loads(q.get("items_detail")) or _safe_json_loads(q.get("line_items"))
    items_n = items_count or (len(items_detail) if isinstance(items_detail, list) else 0)
    total = float(q.get("total") or 0)
    if not inst and items_n == 0 and total <= 0:
        reasons.append(
            "empty draft — no institution, zero items, zero total "
            "(probably an abandoned or never-finished quote)"
        )

    return reasons


def _resolve_source(conn, q: dict) -> tuple[str, Optional[dict]]:
    """Return (source_kind, source_dict_or_None) for a quote row.

    Looks up rfqs.json / price_checks.json on disk. We can't always
    rely on the source_*_id fields being populated, so we also fall
    back to `rfq_number`. `source_kind` is one of {"rfq", "pc",
    "none"} and the dict is the parsed record (or None if we know it
    referenced a source by id but the source is gone).
    """
    # Prefer explicit foreign keys, then fall back to rfq_number.
    rfq_id = (q.get("source_rfq_id") or "").strip()
    pc_id = (q.get("source_pc_id") or "").strip()
    rfq_number = (q.get("rfq_number") or "").strip()

    # No FK + no rfq_number string → no source link at all.
    if not (rfq_id or pc_id or rfq_number):
        return ("none", None)

    # Resolve via the data layer (which is what every other reader uses).
    try:
        from src.api.data_layer import load_rfqs, _load_price_checks
    except Exception as e:
        log.debug("data_layer import suppressed: %s", e)
        return ("unknown", None)

    if rfq_id:
        try:
            rfqs = load_rfqs() or {}
            if rfq_id in rfqs:
                return ("rfq", rfqs[rfq_id])
        except Exception as e:
            log.debug("load_rfqs failed: %s", e)
    if pc_id:
        try:
            pcs = _load_price_checks() or {}
            if pc_id in pcs:
                return ("pc", pcs[pc_id])
        except Exception as e:
            log.debug("_load_price_checks failed: %s", e)

    # Last resort: match by rfq_number as a free-text key against rfqs.
    if rfq_number:
        try:
            rfqs = load_rfqs() or {}
            for rfq in rfqs.values():
                if (rfq.get("solicitation_number") or "").strip() == rfq_number:
                    return ("rfq", rfq)
                if rfq.get("id") == rfq_number:
                    return ("rfq", rfq)
        except Exception as e:
            log.debug("rfq_number lookup failed: %s", e)

    # We knew there was a source pointer, but the record is gone.
    return ("orphaned", None)


def classify_quote(conn, q: dict) -> dict:
    """Classify a single quote row. Pure function over (q, source DB).

    Returns:
        {
          "quote_number": ...,
          "verdict": "clean" | "ghost",
          "bucket": "placeholder_source" | "orphaned_source" | "own_markers" | "no_source" | "clean",
          "source_kind": "rfq" | "pc" | "orphaned" | "none" | "unknown",
          "reasons": [str, ...],   # empty when verdict=='clean'
        }
    """
    out = {
        "quote_number": q.get("quote_number"),
        "agency": q.get("agency"),
        "institution": q.get("institution"),
        "total": q.get("total"),
        "status": q.get("status"),
        "created_at": q.get("created_at"),
        "verdict": "clean",
        "bucket": "clean",
        "source_kind": "none",
        "reasons": [],
    }

    own = _quote_has_own_ghost_markers(q)

    source_kind, source = _resolve_source(conn, q)
    out["source_kind"] = source_kind

    if source_kind == "orphaned":
        out["verdict"] = "ghost"
        out["bucket"] = "orphaned_source"
        out["reasons"].append(
            "source RFQ/PC was referenced but no longer exists (deleted "
            "or never persisted)"
        )

    elif source_kind in ("rfq", "pc") and source is not None:
        # Run the gate against the source record. If the gate would
        # block this source today, the quote was burned on a ghost.
        try:
            from src.api.dashboard import (
                is_ready_for_quote_allocation,
                is_ready_for_pc_quote_allocation,
            )
            gate = (
                is_ready_for_quote_allocation if source_kind == "rfq"
                else is_ready_for_pc_quote_allocation
            )
            ok, reasons = gate(source)
            if not ok:
                out["verdict"] = "ghost"
                out["bucket"] = "placeholder_source"
                out["reasons"].extend(
                    f"source {source_kind}: {r}" for r in reasons
                )
        except Exception as e:
            log.debug("gate suppressed for %s: %s", q.get("quote_number"), e)

    elif source_kind == "none" and own:
        # No source link AND own data is sparse — almost certainly an
        # operator-direct draft that never got finished.
        out["verdict"] = "ghost"
        out["bucket"] = "no_source"

    if own:
        out["reasons"].extend(own)
        # Don't override a more specific bucket; mark only if we hadn't
        # categorized this row as ghost yet.
        if out["verdict"] == "clean":
            out["verdict"] = "ghost"
            out["bucket"] = "own_markers"

    return out


def scan_quotes(*, include_test: bool = False, limit: Optional[int] = None) -> dict:
    """Read-only walk over every quote. Returns a bucketed report.

    The shape:
        {
          "ok": True,
          "total_quotes": int,
          "ghost_count": int,
          "clean_count": int,
          "by_bucket": {
              "placeholder_source": [classification, ...],
              "orphaned_source":    [classification, ...],
              "own_markers":        [classification, ...],
              "no_source":          [classification, ...],
          },
          "by_source_kind": {kind: count},
        }

    `include_test=False` (the default) skips rows where `is_test=1`.
    `limit` is for spot-checks against a large prod table; None walks
    every row.
    """
    from src.core.db import get_db

    report: dict[str, Any] = {
        "ok": True,
        "total_quotes": 0,
        "ghost_count": 0,
        "clean_count": 0,
        "by_bucket": {
            "placeholder_source": [],
            "orphaned_source": [],
            "own_markers": [],
            "no_source": [],
        },
        "by_source_kind": {},
    }

    sql = "SELECT * FROM quotes"
    args: list = []
    if not include_test:
        sql += " WHERE COALESCE(is_test, 0) = 0"
    sql += " ORDER BY created_at DESC"
    if limit:
        sql += " LIMIT ?"
        args.append(limit)

    with get_db() as conn:
        rows = conn.execute(sql, args).fetchall()
        for row in rows:
            q = _row_to_dict(row)
            cls = classify_quote(conn, q)
            report["total_quotes"] += 1
            kind = cls["source_kind"]
            report["by_source_kind"][kind] = report["by_source_kind"].get(kind, 0) + 1
            if cls["verdict"] == "ghost":
                report["ghost_count"] += 1
                report["by_bucket"][cls["bucket"]].append(cls)
            else:
                report["clean_count"] += 1

    log.info(
        "quotes ghost scan: total=%d ghost=%d clean=%d "
        "(placeholder=%d orphaned=%d own=%d no_source=%d)",
        report["total_quotes"], report["ghost_count"], report["clean_count"],
        len(report["by_bucket"]["placeholder_source"]),
        len(report["by_bucket"]["orphaned_source"]),
        len(report["by_bucket"]["own_markers"]),
        len(report["by_bucket"]["no_source"]),
    )
    return report
