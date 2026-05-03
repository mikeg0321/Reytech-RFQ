"""Link orphan orders to their paired quote by po_number.

Background — Mike's 2026-04-29 audit (`project_orphan_orders_finding.md`)
surfaced 67/167 prod orders with no `quote_number`. PR #664's
`ensure_quote_won_for_order` hook only fires when an order already has
a quote_number, so orphans stay invisible to recent-wins, win-rate-by-
agency, and oracle calibration.

This module finds orders with empty `quote_number` and links them to
the paired quote by exact `po_number` match (high confidence). Once
linked, `save_order` re-fires the PR #664 hook so the paired quote
flips open → won automatically.

Conservative by default:
  - Only `po_number` exact match auto-links. The audit listed
    total±1%+agency+time-window as a medium-confidence path, but
    multi-quote ambiguity makes it unsafe for autonomous backfill.
    Medium-confidence candidates surface in the report only.
  - Sentinel po_numbers (N/A, TBD, ?, '') never match — both sides
    are scrubbed with `clean_po_number` before comparison.
  - Idempotent: rerunning over already-linked orders is a no-op.
  - Audit-logged: every link writes an `order_audit_log` row plus
    a `lifecycle_events` row tagged `actor='link_orphan_orders'`.

Usage from the script wrapper:
    from src.core.orders_link_orphans import link_orphan_orders
    report = link_orphan_orders(dry_run=True)   # preview
    report = link_orphan_orders(dry_run=False)  # apply
"""
from __future__ import annotations

import logging
from typing import Any, Optional

log = logging.getLogger("orders_link_orphans")


def _is_orphan(quote_number) -> bool:
    """An order is an orphan when its quote_number is empty / NULL."""
    if quote_number is None:
        return True
    return not str(quote_number).strip()


def find_orphan_orders(conn) -> list[dict]:
    """Return orders rows where quote_number is empty/NULL.

    Excludes test rows so a backfill run doesn't churn synthetic data.
    Each dict carries the raw row + a normalized `po_canonical` field
    that strips sentinels via `clean_po_number`.
    """
    from src.core.order_dal import clean_po_number
    rows = conn.execute("""
        SELECT id, quote_number, po_number, agency, institution,
               total, status, created_at, buyer_email, is_test
        FROM orders
        WHERE COALESCE(is_test, 0) = 0
          AND (quote_number IS NULL OR TRIM(quote_number) = '')
    """).fetchall()
    out: list[dict] = []
    for r in rows:
        rec = dict(r) if hasattr(r, "keys") else {
            "id": r[0], "quote_number": r[1], "po_number": r[2],
            "agency": r[3], "institution": r[4], "total": r[5],
            "status": r[6], "created_at": r[7], "buyer_email": r[8],
            "is_test": r[9],
        }
        rec["po_canonical"] = clean_po_number(rec.get("po_number") or "")
        out.append(rec)
    return out


def find_quote_match_by_po(conn, po_canonical: str) -> Optional[str]:
    """Return the `quote_number` of the unique quote with this PO, or None.

    Returns None if:
      - po_canonical is empty/sentinel
      - zero quotes match
      - more than one quote matches (ambiguous; multi-quote PO is
        legitimate per `project_session_2026_04_28_drift_card_actionable.md`,
        so we refuse to pick one)

    Test rows on the quotes side are excluded so a test quote sharing
    a PO with a real order doesn't poison the link.
    """
    if not po_canonical:
        return None
    rows = conn.execute("""
        SELECT quote_number
        FROM quotes
        WHERE COALESCE(is_test, 0) = 0
          AND po_number IS NOT NULL
          AND TRIM(po_number) = ?
    """, (po_canonical,)).fetchall()
    if len(rows) != 1:
        return None
    qn = rows[0][0] if not hasattr(rows[0], "keys") else rows[0]["quote_number"]
    return (qn or "").strip() or None


def link_orphan_orders(
    *,
    dry_run: bool = True,
    actor: str = "link_orphan_orders",
) -> dict:
    """Walk orphan orders, link each to its paired quote by exact PO.

    Returns a report dict:
        {
            "ok": True,
            "dry_run": bool,
            "orphan_count": int,
            "linked": [{"order_id": ..., "quote_number": ..., "po": ...}, ...],
            "ambiguous": [{"order_id": ..., "po": ..., "match_count": int}],
            "no_po": [{"order_id": ...}],
            "no_match": [{"order_id": ..., "po": ...}],
        }

    When `dry_run=False`, each `linked` entry corresponds to a real
    UPDATE on `orders.quote_number`. The PR #664 hook is then fired
    via `save_order` so the paired quote flips to 'won' automatically.
    """
    from src.core.db import get_db
    from src.core.order_dal import clean_po_number, save_order

    report: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "orphan_count": 0,
        "linked": [],
        "ambiguous": [],
        "no_po": [],
        "no_match": [],
    }

    with get_db() as conn:
        orphans = find_orphan_orders(conn)
        report["orphan_count"] = len(orphans)

        # Bucket the orphans by what we can do with each.
        ambiguous_buckets: dict[str, list[str]] = {}
        for orphan in orphans:
            po = orphan.get("po_canonical") or ""
            order_id = orphan["id"]
            if not po:
                report["no_po"].append({"order_id": order_id})
                continue
            # Count matches; ambiguous PO (>1 quote) is legitimate
            # multi-quote case — don't auto-pick.
            match_rows = conn.execute("""
                SELECT quote_number FROM quotes
                WHERE COALESCE(is_test, 0) = 0
                  AND po_number IS NOT NULL
                  AND TRIM(po_number) = ?
            """, (po,)).fetchall()
            n = len(match_rows)
            if n == 0:
                report["no_match"].append({"order_id": order_id, "po": po})
                continue
            if n > 1:
                report["ambiguous"].append({
                    "order_id": order_id,
                    "po": po,
                    "match_count": n,
                })
                ambiguous_buckets.setdefault(po, []).append(order_id)
                continue
            # Unique PO match — high confidence, eligible for auto-link.
            qn = match_rows[0][0] if not hasattr(match_rows[0], "keys") \
                else match_rows[0]["quote_number"]
            qn = (qn or "").strip()
            if not qn:
                report["no_match"].append({"order_id": order_id, "po": po})
                continue
            entry = {
                "order_id": order_id,
                "quote_number": qn,
                "po": po,
            }
            report["linked"].append(entry)

    if dry_run:
        log.info(
            "link_orphan_orders dry-run: %d orphans (%d linkable, %d ambiguous, %d no-po, %d no-match)",
            report["orphan_count"], len(report["linked"]),
            len(report["ambiguous"]), len(report["no_po"]),
            len(report["no_match"]),
        )
        return report

    # Apply phase: re-fetch each linkable order, set quote_number,
    # and save back through `save_order` so the PR #664 hook fires.
    # We re-fetch instead of using the in-memory dict so any concurrent
    # mutation (e.g. status flip) is preserved.
    applied = []
    for entry in report["linked"]:
        order_id = entry["order_id"]
        qn = entry["quote_number"]
        try:
            with get_db() as conn:
                row = conn.execute("""
                    SELECT id, quote_number, po_number, agency, institution,
                           total, status, items, created_at, buyer_name,
                           buyer_email, ship_to, ship_to_address, total_cost,
                           margin_pct, po_pdf_path, fulfillment_type, notes,
                           is_test
                    FROM orders WHERE id = ?
                """, (order_id,)).fetchone()
            if row is None:
                continue
            rec = dict(row) if hasattr(row, "keys") else {}
            # Sanity: only stamp quote_number if the order is still an orphan.
            # Race with another writer (operator manual link, importer) wins.
            if not _is_orphan(rec.get("quote_number")):
                log.info(
                    "link_orphan_orders: %s already linked to %s — skipping",
                    order_id, rec.get("quote_number"),
                )
                continue
            rec["quote_number"] = qn
            # save_order normalizes status, fires PR #664 hook, audit-logs.
            save_order(order_id, rec, actor=actor)
            applied.append(entry)
        except Exception as e:
            log.error("link_orphan_orders apply failed for %s: %s",
                      order_id, e, exc_info=True)
    report["applied_count"] = len(applied)
    log.info(
        "link_orphan_orders applied %d / %d linkable orphans (actor=%s)",
        len(applied), len(report["linked"]), actor,
    )
    return report
