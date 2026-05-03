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

    Excludes test rows AND `is_intentional_orphan=1` rows so a backfill
    run doesn't churn rows the operator already triaged as not-our-quote.
    Each dict carries the raw row + a normalized `po_canonical` field
    that strips sentinels via `clean_po_number`.
    """
    from src.core.order_dal import clean_po_number
    # `is_intentional_orphan` was added by the 2026-05-03 migration. Older
    # DBs may not have the column yet — fall back gracefully.
    try:
        rows = conn.execute("""
            SELECT id, quote_number, po_number, agency, institution,
                   total, status, created_at, buyer_email, is_test,
                   COALESCE(is_intentional_orphan, 0) AS is_intentional_orphan
            FROM orders
            WHERE COALESCE(is_test, 0) = 0
              AND COALESCE(is_intentional_orphan, 0) = 0
              AND (quote_number IS NULL OR TRIM(quote_number) = '')
        """).fetchall()
    except Exception:
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


# ─── Fuzzy candidate finder (orphan-review queue, 2026-05-03) ─────────
#
# The exact-PO matcher above is the autonomous-safe path. Everything below
# is for the operator-review queue: scores quote candidates per orphan,
# never auto-links. Operator picks via /orders/orphan-review.

# Confidence tiers — calibrated to the 64 known orphans (2026-05-03 memory).
TIER_PO_EXACT = 100         # quote.po_number == orders.po_number
TIER_TOTAL_AGENCY_60D = 80  # ±1% total + same agency + within 60 days
TIER_TOTAL_AGENCY_180D = 60 # ±1% total + same agency + within 180 days
TIER_TOTAL_AGENCY_LOOSE = 40 # ±5% total + same agency
TIER_TOTAL_ONLY = 20        # ±1% total only (no agency match) or aged >180d

DAYS_60 = 60
DAYS_180 = 180


def _coerce_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _agency_canonical(s) -> str:
    """Lightweight agency normalization for candidate matching only.

    Full canonicalization lives in `agency_config.match_agency`. The
    orphan list is small, so cheap lower+strip is enough — quote rows
    are written by paths where the canonical agency was already resolved
    upstream, so misspellings don't usually appear here.
    """
    return (s or "").strip().lower()


def _days_apart(order_created_at: str, quote_when: str) -> Optional[int]:
    """Return abs days between order.created_at and quote.sent_at (or
    created_at fallback), or None if either is missing/unparseable.
    """
    if not order_created_at or not quote_when:
        return None
    try:
        from datetime import datetime
        oa = datetime.fromisoformat(str(order_created_at)[:19])
        qa = datetime.fromisoformat(str(quote_when)[:19])
        return abs((oa - qa).days)
    except (ValueError, TypeError):
        return None


def find_quote_candidates(conn, orphan: dict, *, limit: int = 5) -> list[dict]:
    """Rank quote candidates that could match this orphan order.

    Returns up to `limit` candidates sorted by score desc, then by
    days-apart asc. Each candidate dict shape:

        {
            "quote_number": str,
            "score": int,                 # 0..100, see TIER_* constants
            "tier": str,                  # human label of which rule fired
            "total": float,
            "agency": str,
            "sent_at": str,
            "days_apart": int | None,
            "total_delta_pct": float | None,
        }

    Returns [] when the orphan has neither PO nor a positive total —
    nothing to score against. Exact-PO matches are included so the
    review UI shows them alongside fuzzy candidates instead of having
    to handle PO-match in one path and fuzzy in another.
    """
    order_total = _coerce_float(orphan.get("total"))
    order_agency = _agency_canonical(orphan.get("agency"))
    order_created_at = orphan.get("created_at") or ""
    po_canonical = orphan.get("po_canonical") or ""

    if order_total <= 0 and not po_canonical:
        return []

    # Pull all non-test quotes once. Orphan list is ~64 prod, quote table
    # is ~1k — N*M is fine and keeps tier scoring in Python rather than
    # forcing it through SQL.
    quote_rows = conn.execute("""
        SELECT quote_number, po_number, agency, total, sent_at, created_at
        FROM quotes
        WHERE COALESCE(is_test, 0) = 0
          AND quote_number IS NOT NULL
          AND TRIM(quote_number) != ''
    """).fetchall()

    candidates: list[dict] = []
    for q in quote_rows:
        qd = dict(q) if hasattr(q, "keys") else {
            "quote_number": q[0], "po_number": q[1], "agency": q[2],
            "total": q[3], "sent_at": q[4], "created_at": q[5],
        }
        q_po = (qd.get("po_number") or "").strip()
        q_agency = _agency_canonical(qd.get("agency"))
        q_total = _coerce_float(qd.get("total"))
        q_when = qd.get("sent_at") or qd.get("created_at") or ""
        days = _days_apart(order_created_at, q_when)

        score = 0
        tier_label = ""

        # Tier 1 — exact PO match. Authoritative business-key.
        if po_canonical and q_po == po_canonical:
            score = TIER_PO_EXACT
            tier_label = "po_exact"

        # Tier 2-5 — total + agency proximity. Total of 0 disqualifies.
        elif order_total > 0 and q_total > 0:
            delta_pct = abs(q_total - order_total) / order_total * 100
            same_agency = bool(order_agency) and q_agency == order_agency
            if delta_pct <= 1.0 and same_agency:
                if days is not None and days <= DAYS_60:
                    score = TIER_TOTAL_AGENCY_60D
                    tier_label = "total_agency_60d"
                elif days is not None and days <= DAYS_180:
                    score = TIER_TOTAL_AGENCY_180D
                    tier_label = "total_agency_180d"
                else:
                    # Same agency + tight total but outside 180d — still
                    # worth showing because SCPRS-stub orphans can be
                    # 1-2 years old. Rank low so the recent ones surface
                    # above it.
                    score = TIER_TOTAL_ONLY
                    tier_label = "total_agency_old"
            elif delta_pct <= 5.0 and same_agency:
                score = TIER_TOTAL_AGENCY_LOOSE
                tier_label = "total_agency_loose"
            elif delta_pct <= 1.0:
                # Cross-agency tight total — surface for review but rank low.
                score = TIER_TOTAL_ONLY
                tier_label = "total_only"

        if score == 0:
            continue

        delta_pct = (
            abs(q_total - order_total) / order_total * 100
            if order_total > 0 and q_total > 0 else None
        )
        candidates.append({
            "quote_number": qd.get("quote_number") or "",
            "score": score,
            "tier": tier_label,
            "total": q_total,
            "agency": qd.get("agency") or "",
            "sent_at": qd.get("sent_at") or "",
            "days_apart": days,
            "total_delta_pct": delta_pct,
        })

    def _sort_key(c):
        return (
            -c["score"],
            (c["days_apart"] if c["days_apart"] is not None else 99999),
            (c["total_delta_pct"] if c["total_delta_pct"] is not None else 99999),
        )
    candidates.sort(key=_sort_key)
    return candidates[:limit]


def mark_intentional_orphan(conn, order_id: str, *, actor: str) -> bool:
    """Flag an order as intentional (not-our-quote). Returns True when a
    row was actually flipped. Idempotent — re-flagging is a no-op.

    Caller owns the connection so the route layer can wrap multiple
    operations in one transaction. Audit-logged via `lifecycle_events`.
    """
    cur = conn.execute("""
        UPDATE orders
        SET is_intentional_orphan = 1
        WHERE id = ?
          AND COALESCE(is_intentional_orphan, 0) = 0
    """, (order_id,))
    flipped = cur.rowcount > 0
    if flipped:
        try:
            conn.execute("""
                INSERT INTO lifecycle_events (entity_type, entity_id, event,
                                              actor, payload_json, created_at)
                VALUES ('order', ?, 'mark_intentional_orphan', ?, '{}',
                        CURRENT_TIMESTAMP)
            """, (order_id, actor))
        except Exception as e:
            log.debug("mark_intentional_orphan: lifecycle insert failed: %s", e)
    return flipped


def link_orphan_to_quote(conn, order_id: str, quote_number: str,
                         *, actor: str) -> bool:
    """Link an orphan order to a chosen quote_number via save_order so
    the PR #664 hook fires + audit log writes automatically.

    Returns True on success, False if the order is no longer an orphan
    (race with another writer or already linked). Raises on
    unrecoverable errors so the route can surface them to the operator.
    """
    from src.core.order_dal import save_order
    row = conn.execute("""
        SELECT id, quote_number, po_number, agency, institution,
               total, status, items, created_at, buyer_name,
               buyer_email, ship_to, ship_to_address, total_cost,
               margin_pct, po_pdf_path, fulfillment_type, notes,
               is_test
        FROM orders WHERE id = ?
    """, (order_id,)).fetchone()
    if row is None:
        return False
    rec = dict(row) if hasattr(row, "keys") else {}
    if not _is_orphan(rec.get("quote_number")):
        return False
    rec["quote_number"] = quote_number
    save_order(order_id, rec, actor=actor)
    return True


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
