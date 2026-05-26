"""
quote_lifecycle_shared.py — Unified win/loss/status logic for PC and RFQ.

Consolidates the duplicate mark-won/mark-lost implementations from
routes_pricecheck.py and routes_rfq.py into a single shared module.

Also provides set_quote_status_atomic() for the 4-writer race fence
(Phase 0.4 of PLAN_ONCE_AND_FOR_ALL.md): background agents (award_tracker,
email_poller, scprs_*_engine) must NOT overwrite an operator's manual
status mark made 200ms earlier. They now call this helper with
expected_prev so the UPDATE is conditional.

Manual operator paths (dashboard.py, routes_v1.py admin) keep using
direct UPDATEs — they should always win.
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.lifecycle")


def set_quote_status_atomic(
    quote_id: str,
    new_status: str,
    expected_prev: str | None = None,
    source: str = "",
    extra_columns: dict | None = None,
    forbidden_prev: list[str] | None = None,
) -> bool:
    """Atomically set a quote's status, optionally only if its current
    status equals expected_prev.

    Background agents (award_tracker, email_poller, scprs_*) MUST pass
    expected_prev so they don't clobber operator manual marks. Manual
    operator paths in dashboard.py and routes_v1.py admin should pass
    expected_prev=None so they always win.

    Args:
        quote_id: quote_number (the WHERE key)
        new_status: status to set
        expected_prev: if non-None, UPDATE only fires when current status
            matches. None = unconditional.
        source: free-form audit string ("award_tracker", "email_poller",
            "operator_manual", etc.) — logged on every successful update.
        extra_columns: optional {col: value} to include in the SET clause
            (for status_notes, po_number, etc.). Keys are interpolated
            into the SQL — pass only known column names from your code,
            never user input.
        forbidden_prev: optional list of statuses that BLOCK the update.
            Translates to `AND status NOT IN (?, ?, ...)`. Use when the
            caller wants to flip from "anything else" but specifically
            NOT clobber terminal states (e.g., dashboard order-creation
            flips quote→won but never undoes 'cancelled'). Mutually
            exclusive with expected_prev — caller chooses one or
            neither.

    Returns:
        True if the row was updated; False if no row matched (status was
        not expected_prev, or quote_id doesn't exist). Caller should NOT
        retry on False — that's the race-protection signal.
    """
    from src.core.db import get_db

    if expected_prev is not None and forbidden_prev:
        raise ValueError(
            "set_quote_status_atomic: expected_prev and forbidden_prev "
            "are mutually exclusive — pick one guard mechanism"
        )

    extra_columns = dict(extra_columns or {})
    set_parts = ["status = ?"]
    params: list = [new_status]
    for col, val in extra_columns.items():
        set_parts.append(f"{col} = ?")
        params.append(val)
    set_clause = ", ".join(set_parts)

    where_parts = ["quote_number = ?"]
    params.append(quote_id)
    if expected_prev is not None:
        where_parts.append("status = ?")
        params.append(expected_prev)
    elif forbidden_prev:
        placeholders = ", ".join("?" for _ in forbidden_prev)
        where_parts.append(f"status NOT IN ({placeholders})")
        params.extend(forbidden_prev)
    where_clause = " AND ".join(where_parts)

    sql = f"UPDATE quotes SET {set_clause} WHERE {where_clause}"

    try:
        with get_db() as conn:
            cur = conn.execute(sql, params)
            updated = bool(cur.rowcount)
        if updated:
            log.info(
                "QUOTE_STATUS: %s %s -> %s (source=%s, expected_prev=%s)",
                quote_id, expected_prev or "*", new_status, source or "?",
                expected_prev,
            )
        else:
            log.info(
                "QUOTE_STATUS_SKIP: %s -> %s blocked (source=%s, "
                "expected_prev=%s did not match)",
                quote_id, new_status, source or "?", expected_prev,
            )
        return updated
    except Exception as e:
        log.warning(
            "QUOTE_STATUS_ERR: %s -> %s (source=%s): %s",
            quote_id, new_status, source or "?", e,
        )
        return False


def mark_won(record, record_type, record_id, po_number="", notes=""):
    """Mark a PC or RFQ as won. Handles all side effects.

    Args:
        record: the PC or RFQ dict
        record_type: "pc" or "rfq"
        record_id: pcid or rfq_id
        po_number: optional PO number
        notes: optional notes

    Returns: dict with results
    """
    now = datetime.now().isoformat()
    result = {"ok": True, "record_type": record_type, "record_id": record_id}

    # Update status
    record["status"] = "won"
    record["outcome"] = "won"
    record["outcome_date"] = now
    record["closed_at"] = now
    record["closed_reason"] = f"Won — PO {po_number}" if po_number else "Won"
    if po_number:
        record["po_number"] = po_number

    items = record.get("items", record.get("line_items", []))
    inst = record.get("institution") or record.get("agency", "")
    quote_number = record.get("reytech_quote_number", "")

    # Compute revenue (no DB) — used inside the atomic block below.
    total = 0
    for it in items:
        if it.get("no_bid"):
            continue
        price = (it.get("unit_price") or it.get("price_per_unit")
                 or it.get("pricing", {}).get("recommended_price") or 0)
        qty = it.get("qty", 1) or 1
        try:
            total += float(price) * float(qty)
        except (ValueError, TypeError):
            pass

    # ── ATOMIC LIFECYCLE BOOKKEEPING (S-12, audit 2026-05-07 v2 §S-12) ──
    # Pre-fix mark_won had FIVE separate `with get_db() as conn:` blocks
    # (revenue_log, activity_log, recommendation_audit, award_tracker_log,
    # plus 2 external module calls). Each committed independently. A
    # process crash mid-flight (gunicorn worker SIGKILL, kernel panic)
    # could leave revenue logged but no calibration / no activity_log row /
    # no award_tracker_log entry. Audit named this concretely:
    #   "A crash between block 2 and 6 leaves revenue logged but no
    #    calibration, no activity_log row, no award_tracker_log entry."
    #
    # Fix: ONE connection holds ALL in-process writes. Per-block try/except
    # logs internal errors but does NOT raise — so a single broken site
    # (e.g., schema drift in one table) doesn't kill the others. The outer
    # `with get_db()` commits ALL successful writes atomically at __exit__,
    # so a crash before COMMIT rolls back EVERYTHING. The status flip
    # (set_quote_status_atomic) is upstream and independent — that fence
    # remains the durability anchor.
    #
    # The 2 external calls (record_winning_prices, calibrate_from_outcome)
    # open their own connections and stay best-effort intelligence work
    # outside the atomic block. They're idempotent / cache-class writes,
    # not lifecycle-critical.
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # 1. Revenue log
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO revenue_log
                    (logged_at, source, amount, category, description, po_number, agency)
                    VALUES (?, ?, ?, 'quote_won', ?, ?, ?)
                """, (now, f"{record_type}_{record_id}", total,
                      f"Won {record_type.upper()} {record_id}", po_number, inst))
                result["revenue_logged"] = total
            except Exception as e:
                log.debug("mark_won revenue: %s", e)

            # 2. CRM activity
            try:
                conn.execute("""
                    INSERT INTO activity_log (contact_id, event_type, event_detail, logged_at, metadata)
                    VALUES (?, 'quote_won', ?, ?, ?)
                """, (record.get("requestor_email", ""), f"Won — PO {po_number}",
                      now, f'{{"record_type":"{record_type}","record_id":"{record_id}"}}'))
            except Exception as e:
                log.debug("mark_won CRM activity: %s", e)

            # 3. recommendation_audit outcome
            try:
                conn.execute("""
                    UPDATE recommendation_audit SET outcome='won', updated_at=datetime('now')
                    WHERE (pc_id=? OR quote_number=?) AND outcome='pending'
                """, (record_id, quote_number))
            except Exception as e:
                log.debug("mark_won recommendation_audit: %s", e)

            # 4. award_tracker_log
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO award_tracker_log
                    (quote_number, checked_at, outcome, notes)
                    VALUES (?, datetime('now'), 'won_manual', ?)
                """, (quote_number or record_id, notes or f"PO {po_number}"))
            except Exception as e:
                log.debug("mark_won award_tracker_log: %s", e)
        # get_db __exit__ commits all 4 above atomically.
    except Exception as fatal:
        # The outer get_db itself failed (connection broken / DB locked
        # past retry). All 4 writes rolled back — log loud and continue
        # to external best-effort work below.
        log.error("MARK_WON atomic block failed: %s", fatal)

    # ── EXTERNAL BEST-EFFORT (intelligence/cache, NOT lifecycle-critical) ──
    # These open their own connections; they're outside the atomic block
    # by design. Failures here don't roll back the lifecycle bookkeeping.

    # Catalog write-back: record winning prices for future intel.
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        line_items = []
        for it in items:
            if it.get("no_bid"):
                continue
            price = (it.get("unit_price") or it.get("price_per_unit")
                     or (it.get("pricing") or {}).get("recommended_price") or 0)
            # PR mr-wolf #2: cost via the canonical reader. The previous
            # chain (vendor_cost → cost → pricing.unit_cost) had the
            # wrong priority — operator-typed supplier_cost was invisible.
            from src.core.pricing_math import cost_from_contract as _cfc_qls
            cost = _cfc_qls(it)
            if not price or not it.get("description"):
                continue
            line_items.append({
                "description": it.get("description", ""),
                "part_number": it.get("mfg_number", "") or it.get("part_number", ""),
                "sku": it.get("mfg_number", ""),
                "qty": it.get("qty", 1) or 1,
                "unit_price": float(price),
                "cost": float(cost),
                "supplier": it.get("item_supplier", "") or it.get("supplier", ""),
            })
        record_winning_prices({
            "quote_number": quote_number or record_id,
            "po_number": po_number,
            "agency": inst,
            "institution": record.get("institution", ""),
            "line_items": line_items,
        })
    except Exception as e:
        log.debug("mark_won catalog: %s", e)

    # V3: Calibrate Oracle from win outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        calibrate_from_outcome(items, "won", agency=inst)
    except Exception as e:
        log.warning("mark_won V3 calibration: %s", e)

    log.info("MARK_WON: %s %s — PO %s", record_type, record_id, po_number)
    return result


def mark_lost(record, record_type, record_id, competitor="", competitor_price=0, reason="", po_number=""):
    """Mark a PC or RFQ as lost. Handles all side effects."""
    now = datetime.now().isoformat()
    result = {"ok": True, "record_type": record_type, "record_id": record_id}

    record["status"] = "lost"
    record["outcome"] = "lost"
    record["outcome_date"] = now
    record["closed_at"] = now
    record["closed_reason"] = f"Lost to {competitor}" if competitor else reason or "Lost"

    # Log competitor intel
    if competitor:
        try:
            from src.core.db import get_db
            our_total = 0
            items = record.get("items", record.get("line_items", []))
            for it in items:
                if it.get("no_bid"):
                    continue
                price = it.get("unit_price") or it.get("price_per_unit") or 0
                qty = it.get("qty", 1) or 1
                our_total += float(price) * float(qty)

            delta = our_total - float(competitor_price) if competitor_price else 0
            delta_pct = round(delta / float(competitor_price) * 100, 1) if competitor_price else 0

            with get_db() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO competitor_intel
                    (found_at, quote_number, our_price, competitor_name, competitor_price,
                     price_delta, price_delta_pct, agency, institution, outcome, notes,
                     loss_reason_class)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (now, record.get("reytech_quote_number", record_id),
                      our_total, competitor, float(competitor_price) if competitor_price else 0,
                      delta, delta_pct,
                      record.get("institution") or record.get("agency", ""),
                      record.get("institution", ""),
                      "lost", reason or f"Lost to {competitor}",
                      "price_higher"))
            result["competitor_logged"] = True
        except Exception as e:
            log.debug("mark_lost competitor: %s", e)

    # V3: Calibrate Oracle from loss outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        items = record.get("items", record.get("line_items", []))
        loss_type = "price" if (competitor_price and float(competitor_price) > 0) else "other"
        calibrate_from_outcome(
            items, "lost",
            agency=record.get("institution") or record.get("agency", ""),
            loss_reason=loss_type,
        )
    except Exception as e:
        log.warning("mark_lost V3 calibration: %s", e)

    # Update recommendation_audit
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                UPDATE recommendation_audit SET outcome='lost',
                    outcome_price=?, updated_at=datetime('now')
                WHERE (pc_id=? OR quote_number=?) AND outcome='pending'
            """, (float(competitor_price) if competitor_price else 0,
                  record_id, record.get("reytech_quote_number", "")))
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # Generate action items from loss
    try:
        from src.agents.pricing_feedback import generate_action_items
        generate_action_items(
            {"loss_reason_class": "price_higher", "line_comparison": [], "margin_too_high_items": []},
            quote_number=record.get("reytech_quote_number", record_id),
            agency=record.get("institution") or record.get("agency", ""),
            institution=record.get("institution", ""),
        )
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    log.info("MARK_LOST: %s %s — competitor=%s price=%s", record_type, record_id, competitor, competitor_price)
    return result


def mark_sent_in_place(
    record: dict,
    *,
    sent_at: str | None = None,
    sent_to: str | None = None,
    sent_method: str | None = None,
    notes: str | None = None,
    source: str = "user",
    skip_transition: bool = False,
) -> dict:
    """Single substrate writer for the 'sent' state transition on a PC
    or RFQ entity dict.

    Before this helper (audit-flagged 2026-05-26 / PR #1078 follow-up):
    8 inline call sites across routes_rfq_admin / routes_rfq_gen /
    routes_pricecheck_admin / routes_pricecheck_pricing /
    routes_pricecheck_gen each did the same 4-step dance:

        _transition_status(record, "sent", actor=..., notes=...)
        record["sent_at"]     = <some iso>
        record["sent_to"]     = <some recipient>
        record["sent_method"] = <"manual"|"email"|"bundle"|...>
        # ... and later somewhere:
        propagate_sent_to_quote_row(record, source=...)

    8 inline writers = 8 places to drift. Per
    `[[feedback-kpi-substrate-singleness]]` this is the dominant
    defect class. ONE function owns the transition now; the inline
    writes are deleted in the same PR.

    Args:
        record: PC or RFQ dict (mutated in place).
        sent_at: ISO timestamp. Defaults to datetime.now().isoformat().
        sent_to: recipient email (e.g., buyer's procurement address).
        sent_method: "manual" | "email" | "bundle" | ... — free-form
            audit metadata; only written when truthy.
        notes: audit-trail note forwarded to _transition_status.
        source: actor for propagate_sent_to_quote_row audit
            ("user" for operator, "system" for daemons,
            "gmail_sent_watcher" / "bundle_send" / etc.).
        skip_transition: when True, the helper does NOT call
            _transition_status (caller already did). Use ONLY when
            interleaving with non-sent transitions; default False is
            the intended path.

    Returns:
        {transitioned: bool, propagated: bool}
        - transitioned: True if status was flipped to 'sent' here.
        - propagated: True if the linked `quotes` row was also flipped
          (via propagate_sent_to_quote_row).

    Best-effort: never raises. The status flip is authoritative; the
    propagate call is the analytics-substrate side-effect.
    """
    if sent_at is None:
        sent_at = datetime.now().isoformat()

    transitioned = False
    if not skip_transition:
        try:
            from src.api.modules.routes_rfq import _transition_status
            _transition_status(record, "sent", actor=source, notes=notes)
            transitioned = True
        except Exception as e:
            # Defensive: never let a transition-helper hiccup block the
            # sent-state flip the operator just requested.
            log.warning(
                "mark_sent_in_place: _transition_status failed (%s), "
                "falling back to direct status write", e,
            )
            record["status"] = "sent"
            transitioned = True

    record["sent_at"] = sent_at
    if sent_to:
        record["sent_to"] = sent_to
    if sent_method:
        record["sent_method"] = sent_method

    propagated = propagate_sent_to_quote_row(record, source=source)
    return {"transitioned": transitioned, "propagated": propagated}


def propagate_sent_to_quote_row(record: dict, source: str = "user") -> bool:
    """When a PC or RFQ is marked sent, also flip its linked row in the
    `quotes` table to status='sent' (which also stamps sent_at).

    Why this exists: the canonical mark-sent paths
    (`api_pricecheck_mark_sent`, `api_pricecheck_mark_sent_manually`,
    `api_rfq_mark_sent_manually`, `send_email_enhanced`) all flip the
    PC/RFQ ENTITY status + sent_at but did NOT update the `quotes` table
    that `award_tracker` reads. Result: PCs in status='sent' + matching
    `quotes` rows still in status='generated' or 'pending'. award_tracker's
    eligibility filter (`WHERE status='sent' AND total > 0`) sees zero
    work despite real operator activity → loss-detection pipeline silent
    → empty Oracle weekly. The 2026-05-25 screenshot
    (0 wins / 0 losses / 1991 calibration samples) was the visible
    symptom; this helper is the substrate fix.

    Args:
        record: PC or RFQ dict; must carry `reytech_quote_number`
            (the linkage key between entity and quotes table).
        source: actor name for the audit trail. Use "user" for operator
            paths, "system" for daemon/auto-generated sends.

    Returns:
        True  — quote row was found AND flipped to 'sent'.
        False — no `reytech_quote_number` on the record (entity has no
                linked quote PDF yet; legacy / pre-generation state) OR
                the linked row wasn't found in `quotes` (data drift).

    Best-effort — exceptions are caught and logged, never raised. The
    PC/RFQ entity flip remains the authoritative operator-console state;
    this helper only keeps the analytics substrate (award_tracker,
    oracle_weekly, calibration EMA) in sync with operator reality.
    """
    quote_number = (
        record.get("reytech_quote_number")
        or record.get("quote_number")
        or ""
    ).strip()
    if not quote_number:
        log.debug(
            "propagate_sent_to_quote_row: no reytech_quote_number on "
            "record, skipping — entity was mark-sent before a quote PDF "
            "was generated (legitimate for some flows)"
        )
        return False
    try:
        from src.forms.quote_generator import update_quote_status
        ok = update_quote_status(
            quote_number, "sent", actor=source,
            notes="Propagated from PC/RFQ mark-sent",
        )
        if ok:
            log.info(
                "QUOTE_SENT_PROPAGATED: %s (source=%s)",
                quote_number, source,
            )
        else:
            log.warning(
                "QUOTE_SENT_PROPAGATE_MISS: quote_number=%s not in quotes "
                "table — entity flipped but award_tracker won't see it. "
                "Likely cause: quote PDF was never generated, or "
                "reytech_quote_number is stale.",
                quote_number,
            )
        return bool(ok)
    except Exception as e:
        log.warning(
            "propagate_sent_to_quote_row(quote_number=%s) failed: %s",
            quote_number, e,
        )
        return False
