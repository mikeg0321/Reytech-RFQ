"""Idempotent quote-won materialization for any quote with a real order.

Mirror of src.core.orders_backfill (which handles the FORWARD direction:
quote marked won -> ensure orders row exists).

This handles the INVERSE direction: a PO arrives via the email watcher,
Drive watcher, or operator manual entry; save_order() writes the orders
row; but the paired quote stays in 'open' / 'pending' / 'priced' /
'sent' status because no path explicitly flips it. The recent-wins KPI
and the won-quote drilldowns then under-report.

PR #660 patched recent-wins to drive off the orders table as a workaround.
This is the architecture-layer fix: a real order with a quote_number
implies the quote was won, so flip it.

Idempotent: if the quote is already 'won', returns ok=True with
flipped=False. Safe to call from save_order's hot path.
"""
from __future__ import annotations

import logging

log = logging.getLogger("reytech.quotes_backfill")


# Statuses that an order's existence should ALWAYS upgrade to 'won'.
# Excludes: 'won' (already done), 'lost' (operator decided otherwise —
# don't second-guess), 'voided'/'cancelled'/'deleted' (bookkeeping).
_FLIPPABLE_QUOTE_STATUSES = frozenset({
    "open", "pending", "priced", "sent", "draft", "new", "",
})


def ensure_quote_won_for_order(quote_number: str, order_id: str = "",
                               po_number: str = "",
                               actor: str = "system") -> dict:
    """Make sure the quote paired with this order has status='won'.

    Args:
        quote_number: The quote that an order just landed for.
        order_id: The order id (for the audit log only).
        po_number: PO number (for the audit log only).
        actor: Who is making the implicit win attribution.

    Returns:
        {"ok": True/False, "flipped": True/False, "prev_status": "...",
         "error": "..."}

        - flipped=True if we changed the row.
        - flipped=False with ok=True if the quote was already won (or
          in a final state we mustn't override like 'lost').
        - ok=False with error set if the quote couldn't be loaded.
    """
    if not quote_number:
        return {"ok": False, "error": "quote_number is required",
                "flipped": False, "prev_status": ""}

    from src.core.db import get_db
    try:
        with get_db() as conn:
            qrow = conn.execute(
                "SELECT quote_number, status FROM quotes WHERE quote_number=? LIMIT 1",
                (quote_number,)
            ).fetchone()

            if not qrow:
                return {"ok": False, "error": f"quote {quote_number} not found",
                        "flipped": False, "prev_status": ""}

            prev_status = (qrow["status"] or "").strip().lower()

            if prev_status == "won":
                return {"ok": True, "flipped": False,
                        "prev_status": prev_status, "error": ""}

            if prev_status not in _FLIPPABLE_QUOTE_STATUSES:
                # 'lost' / 'voided' / 'cancelled' — operator decision, leave alone
                log.info(
                    "ensure_quote_won: quote %s status=%r is final, not flipping "
                    "(order=%s po=%s)",
                    quote_number, prev_status, order_id, po_number,
                )
                return {"ok": True, "flipped": False,
                        "prev_status": prev_status, "error": ""}

            from datetime import datetime
            now_iso = datetime.now().isoformat()
            conn.execute(
                "UPDATE quotes SET status='won', updated_at=? WHERE quote_number=?",
                (now_iso, quote_number),
            )

            # Audit trail: record that this win was inferred from order arrival.
            try:
                conn.execute("""
                    INSERT INTO quote_audit_log (quote_number, action, actor,
                                                 details, created_at)
                    VALUES (?, 'mark_won_from_order', ?, ?, ?)
                """, (
                    quote_number, actor,
                    f"Inferred won from order {order_id} (PO: {po_number or '-'})",
                    now_iso,
                ))
            except Exception as _e:
                # Audit table may not exist on older schemas — don't fail the flip.
                log.debug("quote_audit_log insert suppressed: %s", _e)

    except Exception as e:
        log.warning("ensure_quote_won failed for %s: %s", quote_number, e)
        return {"ok": False, "error": str(e),
                "flipped": False, "prev_status": ""}

    log.info(
        "ensure_quote_won: flipped quote %s %s->won (order=%s po=%s actor=%s)",
        quote_number, prev_status, order_id, po_number, actor,
    )
    return {"ok": True, "flipped": True,
            "prev_status": prev_status, "error": ""}


def backfill_orders_quotes_drift(*, dry_run: bool = False,
                                  actor: str = "backfill") -> dict:
    """One-time scan: every order with a quote_number where the quote is
    still in a flippable status, flip the quote to 'won'.

    Closes the gap that recent-wins shows orders for but quotes don't
    reflect.

    Args:
        dry_run: When True, walk the same query and classify what
            *would* be flipped, but never call `ensure_quote_won_for_order`
            so no rows mutate. Returned `flipped` lists the would-be
            flips. Use this on prod first to see scale before applying.
        actor: Audit-log actor string for the real run. Ignored on dry-run.

    Use the CLI wrapper:
        python scripts/backfill_orders_quotes_drift.py            # dry-run
        python scripts/backfill_orders_quotes_drift.py --apply    # apply
    """
    from src.core.db import get_db
    flipped = []
    skipped_final = []
    skipped_already_won = 0
    errors = []

    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT o.id AS order_id, o.quote_number, o.po_number,
                       q.status AS quote_status
                FROM orders o
                JOIN quotes q ON q.quote_number = o.quote_number
                WHERE COALESCE(o.quote_number, '') != ''
                  AND COALESCE(o.status, '') NOT IN ('cancelled', 'voided', 'deleted')
            """).fetchall()
    except Exception as e:
        return {"ok": False, "error": str(e), "flipped": [], "examined": 0}

    for r in rows:
        prev = (r["quote_status"] or "").strip().lower()
        if prev == "won":
            skipped_already_won += 1
            continue
        if prev not in _FLIPPABLE_QUOTE_STATUSES:
            skipped_final.append((r["quote_number"], prev))
            continue
        if dry_run:
            # Same row set ensure_quote_won_for_order would touch — no write.
            flipped.append(r["quote_number"])
            continue
        result = ensure_quote_won_for_order(
            r["quote_number"], order_id=r["order_id"],
            po_number=r["po_number"] or "", actor=actor,
        )
        if result["ok"] and result["flipped"]:
            flipped.append(r["quote_number"])
        elif not result["ok"]:
            errors.append((r["quote_number"], result["error"]))

    return {
        "ok": True, "examined": len(rows),
        "dry_run": dry_run,
        "flipped": flipped,
        "skipped_already_won": skipped_already_won,
        "skipped_final": skipped_final,
        "errors": errors,
    }
