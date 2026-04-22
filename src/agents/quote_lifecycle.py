"""
src/agents/quote_lifecycle.py — Quote Lifecycle Automation (PRD-28 WI-1)

Closes the loop on quote status:
  1. Expiration engine — auto-expire quotes after 30 days
  2. Reply → status bridge — win/loss signals update quote status  
  3. Award monitor integration — SCPRS competitor wins close quotes
  4. Revision tracking — snapshot before any edit
  5. Follow-up triggers — pending quotes get nudged

Background scheduler runs every hour.
"""

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

log = logging.getLogger("quote_lifecycle")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    from contextlib import contextmanager
    @contextmanager
    def get_db():
        conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

# ── Config ────────────────────────────────────────────────────────────────────
EXPIRATION_DAYS = 30
FOLLOW_UP_INTERVAL_DAYS = 7      # First follow-up after 7 days
MAX_FOLLOW_UPS = 3                # Max auto follow-ups before expire
CHECK_INTERVAL = 3600             # Check every hour

_scheduler_running = False


# ── Expiration Engine ─────────────────────────────────────────────────────────

def check_expirations() -> dict:
    """Find and expire quotes past their expiration date."""
    now = datetime.now(timezone.utc).isoformat()
    expired = 0
    follow_ups_due = 0
    notified = []

    try:
        with get_db() as conn:
            # Set expires_at on quotes that don't have one
            conn.execute("""
                UPDATE quotes SET expires_at = datetime(created_at, '+%d days')
                WHERE expires_at IS NULL AND status IN ('pending', 'sent')
            """ % EXPIRATION_DAYS)

            # Find expired quotes
            rows = conn.execute("""
                SELECT quote_number, agency, institution, total, contact_email,
                       created_at, expires_at, follow_up_count
                FROM quotes
                WHERE is_test = 0 AND status IN ('pending', 'sent')
                  AND expires_at IS NOT NULL
                  AND expires_at < ?
            """, (now,)).fetchall()

            for r in rows:
                qn = r["quote_number"]
                conn.execute("""
                    UPDATE quotes
                    SET status = 'expired',
                        closed_by_agent = 'quote_lifecycle',
                        close_reason = 'Auto-expired after %d days with no response',
                        updated_at = ?
                    WHERE quote_number = ?
                """ % EXPIRATION_DAYS, (now, qn))
                expired += 1
                notified.append(qn)
                log.info("Expired quote %s (created %s)", qn, r["created_at"])

            # Find quotes needing follow-up (sent > 7 days ago, < max follow-ups)
            follow_up_cutoff = (datetime.now(timezone.utc) - timedelta(days=FOLLOW_UP_INTERVAL_DAYS)).isoformat()
            pending_rows = conn.execute("""
                SELECT quote_number, agency, institution, total, contact_email,
                       sent_at, last_follow_up, follow_up_count
                FROM quotes
                WHERE is_test = 0 AND status = 'sent'
                  AND (last_follow_up IS NULL OR last_follow_up < ?)
                  AND COALESCE(follow_up_count, 0) < ?
                  AND sent_at IS NOT NULL AND sent_at != ''
                  AND sent_at < ?
            """, (follow_up_cutoff, MAX_FOLLOW_UPS, follow_up_cutoff)).fetchall()

            for r in pending_rows:
                follow_ups_due += 1

        # Send notifications for expired quotes
        if notified:
            _notify_expirations(notified)

    except Exception as e:
        log.error("check_expirations failed: %s", e)
        return {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "expired": expired,
        "follow_ups_due": follow_ups_due,
        "checked_at": now,
    }


def get_expiring_soon(days: int = 7) -> list:
    """Get quotes expiring within N days."""
    cutoff = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, agency, institution, total, contact_email,
                       created_at, expires_at, status, follow_up_count
                FROM quotes
                WHERE is_test = 0 AND status IN ('pending', 'sent')
                  AND expires_at IS NOT NULL
                  AND expires_at BETWEEN ? AND ?
                ORDER BY expires_at ASC
            """, (now, cutoff)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_expiring_soon: %s", e)
        return []


# ── Reply → Quote Status Bridge ──────────────────────────────────────────────

def process_reply_signal(quote_number: str, signal: str, confidence: float = 0.0,
                         po_number: str = "", reason: str = "", source: str = "reply_analyzer") -> dict:
    """Update quote status based on reply analysis signal.
    
    Args:
        signal: 'win', 'loss', 'question', 'neutral'
        confidence: 0.0-1.0
        po_number: extracted PO number for wins
    """
    if not quote_number:
        return {"ok": False, "error": "no quote_number"}

    now = datetime.now(timezone.utc).isoformat()

    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT status, status_history FROM quotes WHERE quote_number = ?",
                (quote_number,)
            ).fetchone()

            if not row:
                return {"ok": False, "error": f"quote {quote_number} not found"}

            current_status = row["status"]
            if current_status in ("won", "lost", "expired", "cancelled"):
                return {"ok": False, "error": f"quote already {current_status}", "skipped": True}

            history = json.loads(row["status_history"] or "[]")
            action = "none"

            if signal == "win" and confidence >= 0.6:
                new_status = "won"
                history.append({"from": current_status, "to": new_status,
                                "at": now, "by": source, "reason": f"Win signal (conf={confidence:.0%})"})
                conn.execute("""
                    UPDATE quotes
                    SET status = 'won', po_number = COALESCE(NULLIF(?, ''), po_number),
                        closed_by_agent = ?, close_reason = ?,
                        status_history = ?, updated_at = ?
                    WHERE quote_number = ?
                """, (po_number, source, f"Win detected: {reason}",
                      json.dumps(history), now, quote_number))
                action = "won"
                _auto_create_order(conn, quote_number, po_number)
                log.info("Quote %s → WON (PO: %s, conf: %.0f%%)", quote_number, po_number, confidence * 100)

                # Learn item mappings + lock costs from won quote
                try:
                    from src.core.pricing_oracle_v2 import confirm_item_mapping, lock_cost
                    _items_raw = row["line_items"] or row["items_detail"] or "[]"
                    _items = json.loads(_items_raw) if isinstance(_items_raw, str) else _items_raw
                    for _it in (_items or []):
                        _desc = _it.get("description", "")
                        _cost = _it.get("cost") or _it.get("supplier_cost") or _it.get("unit_cost")
                        if _desc:
                            confirm_item_mapping(
                                original_description=_desc, canonical_description=_desc,
                                item_number=_it.get("part_number", _it.get("item_number", "")),
                                supplier=_it.get("supplier", ""),
                                cost=float(str(_cost or 0).replace("$", "").replace(",", "")) if _cost else None,
                            )
                            if _cost:
                                try:
                                    lock_cost(_desc, float(str(_cost).replace("$", "").replace(",", "")),
                                              supplier=_it.get("supplier", ""),
                                              source="won_quote_lifecycle", expires_days=60)
                                except Exception as _e:
                                    log.debug('suppressed in process_reply_signal: %s', _e)
                except Exception as _e:
                    log.debug('suppressed in process_reply_signal: %s', _e)

            elif signal == "loss" and confidence >= 0.6:
                new_status = "lost"
                history.append({"from": current_status, "to": new_status,
                                "at": now, "by": source, "reason": reason or "Loss signal detected"})
                conn.execute("""
                    UPDATE quotes
                    SET status = 'lost', closed_by_agent = ?, close_reason = ?,
                        status_history = ?, updated_at = ?
                    WHERE quote_number = ?
                """, (source, reason or "Loss detected from reply",
                      json.dumps(history), now, quote_number))
                action = "lost"
                log.info("Quote %s → LOST (reason: %s)", quote_number, reason)

            elif signal == "question":
                # Reset expiration clock — buyer is engaged
                new_expires = (datetime.now(timezone.utc) + timedelta(days=EXPIRATION_DAYS)).isoformat()
                conn.execute("""
                    UPDATE quotes SET expires_at = ?, updated_at = ?
                    WHERE quote_number = ?
                """, (new_expires, now, quote_number))
                action = "extended"
                log.info("Quote %s — question reply, extended expiration", quote_number)

            return {"ok": True, "action": action, "quote_number": quote_number, "signal": signal}

    except Exception as e:
        log.error("process_reply_signal: %s", e)
        return {"ok": False, "error": str(e)}


def _auto_create_order(conn, quote_number: str, po_number: str):
    """Create an order stub when a quote is won."""
    try:
        row = conn.execute("""
            SELECT agency, institution, total, subtotal, tax,
                   contact_name, contact_email, ship_to_name, ship_to_address,
                   line_items, is_test
            FROM quotes WHERE quote_number = ?
        """, (quote_number,)).fetchone()

        if not row:
            return

        order_id = f"ORD-{quote_number}"
        now = datetime.now(timezone.utc).isoformat()
        # BUILD-10: inherit is_test from the source quote so derived
        # orders/revenue rows don't pollute analytics aggregates.
        _is_test = 1 if (row["is_test"] if "is_test" in row.keys() else 0) else 0

        conn.execute("""
            INSERT OR IGNORE INTO orders
            (id, quote_number, agency, institution, po_number, status,
             total, items, created_at, updated_at, is_test)
            VALUES (?, ?, ?, ?, ?, 'new', ?, ?, ?, ?, ?)
        """, (order_id, quote_number, row["agency"], row["institution"],
              po_number, row["total"], row["line_items"] or "[]", now, now, _is_test))
        log.info("Auto-created order %s from won quote %s", order_id, quote_number)

        # Also log revenue
        conn.execute("""
            INSERT INTO revenue_log (id, logged_at, amount, description, source, quote_number, po_number, agency, is_test)
            VALUES (?, ?, ?, ?, 'quote_won', ?, ?, ?, ?)
        """, (f"rev-{quote_number}", now, row["total"] or 0,
              f"Quote {quote_number} won", quote_number, po_number, row["agency"], _is_test))

    except Exception as e:
        log.warning("_auto_create_order: %s", e)


# ── Competitor Close (from award_monitor) ─────────────────────────────────────

def close_lost_to_competitor(quote_number: str, competitor: str, competitor_price: float = 0,
                             po_number: str = "") -> dict:
    """Close a quote as lost to a specific competitor (from SCPRS data)."""
    return process_reply_signal(
        quote_number=quote_number,
        signal="loss",
        confidence=0.9,
        reason=f"Lost to {competitor}" + (f" at ${competitor_price:,.2f}" if competitor_price else ""),
        source="award_monitor"
    )


# ── Revision Tracking ─────────────────────────────────────────────────────────

def save_revision(quote_number: str, reason: str = "manual edit") -> dict:
    """Snapshot current quote state before editing."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM quotes WHERE quote_number = ?", (quote_number,)
            ).fetchone()
            if not row:
                return {"ok": False, "error": "quote not found"}

            snapshot = json.dumps(dict(row), default=str)
            rev_num = (row["revision_count"] or 0) + 1

            conn.execute("""
                INSERT INTO quote_revisions (quote_number, revision_num, revised_at, reason, snapshot_json)
                VALUES (?, ?, ?, ?, ?)
            """, (quote_number, rev_num, now, reason, snapshot))

            conn.execute("""
                UPDATE quotes SET revision_count = ?, updated_at = ?
                WHERE quote_number = ?
            """, (rev_num, now, quote_number))

            return {"ok": True, "revision": rev_num}
    except Exception as e:
        log.error("save_revision: %s", e)
        return {"ok": False, "error": str(e)}


def get_revisions(quote_number: str) -> list:
    """Get revision history for a quote."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT revision_num, revised_at, reason, changed_by
                FROM quote_revisions
                WHERE quote_number = ?
                ORDER BY revision_num DESC
            """, (quote_number,)).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


# ── Pipeline Summary ──────────────────────────────────────────────────────────

def get_pipeline_summary() -> dict:
    """Get full pipeline status for dashboard."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as cnt, COALESCE(SUM(total), 0) as value
                FROM quotes
                WHERE is_test = 0
                GROUP BY status
            """).fetchall()

            pipeline = {}
            for r in rows:
                pipeline[r["status"]] = {"count": r["cnt"], "value": r["value"]}

            # Expiring soon
            cutoff = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
            now = datetime.now(timezone.utc).isoformat()
            expiring = conn.execute("""
                SELECT COUNT(*) FROM quotes
                WHERE is_test = 0 AND status IN ('pending', 'sent')
                  AND expires_at BETWEEN ? AND ?
            """, (now, cutoff)).fetchone()[0]

            total_open = sum(d["value"] for s, d in pipeline.items() if s in ("pending", "sent"))
            total_won = pipeline.get("won", {}).get("value", 0)

            return {
                "ok": True,
                "pipeline": pipeline,
                "expiring_soon": expiring,
                "total_open_value": total_open,
                "total_won_value": total_won,
                "conversion_rate": round(
                    pipeline.get("won", {}).get("count", 0) * 100 /
                    max(sum(d["count"] for d in pipeline.values()), 1), 1
                ),
            }
    except Exception as e:
        log.error("get_pipeline_summary: %s", e)
        return {"ok": False, "error": str(e)}


# ── Background Scheduler ─────────────────────────────────────────────────────

def _lifecycle_loop():
    """Daemon loop for periodic lifecycle checks — shutdown-aware."""
    from src.core.scheduler import _shutdown_event, heartbeat
    _shutdown_event.wait(60)  # initial delay for app boot
    while not _shutdown_event.is_set():
        try:
            result = check_expirations()
            if result.get("expired", 0) > 0 or result.get("follow_ups_due", 0) > 0:
                log.info("Lifecycle check: expired=%d follow_ups_due=%d",
                         result.get("expired", 0), result.get("follow_ups_due", 0))
            # V5 Phase 6: Check for cost changes on pending quotes
            try:
                cost_alerts = check_cost_changes()
                if cost_alerts:
                    log.info("Cost change alerts: %d pending quotes affected", len(cost_alerts))
            except Exception as ce:
                log.debug("Cost change check: %s", ce)
            try:
                heartbeat("quote-lifecycle", success=True)
            except Exception as _e:
                log.debug('suppressed in _lifecycle_loop: %s', _e)
        except Exception as e:
            log.error("Lifecycle scheduler error: %s", e, exc_info=True)
            try:
                heartbeat("quote-lifecycle", success=False, error=str(e)[:200])
            except Exception as _e:
                log.debug('suppressed in _lifecycle_loop: %s', _e)
        _shutdown_event.wait(CHECK_INTERVAL)
    log.info("Quote lifecycle scheduler shutting down")


def start_lifecycle_scheduler():
    """Start the background lifecycle checker as a daemon thread."""
    global _scheduler_running
    if _scheduler_running:
        return
    _scheduler_running = True
    threading.Thread(target=_lifecycle_loop, daemon=True, name="quote-lifecycle").start()
    log.info("Quote lifecycle scheduler started (checks every %ds)", CHECK_INTERVAL)


def _notify_expirations(quote_numbers: list):
    """Send notification for expired quotes."""
    try:
        from src.agents.notify_agent import send_alert
        for qn in quote_numbers:
            send_alert(
                event_type="quote_expired",
                title=f"Quote {qn} expired",
                body=f"Quote {qn} auto-expired after {EXPIRATION_DAYS} days with no response.",
                urgency="warning",
                deep_link=f"/quote/{qn}"
            )
    except Exception as e:
        log.warning("_notify_expirations: %s", e)


# ── Agent Status ──────────────────────────────────────────────────────────────

def get_agent_status() -> dict:
    """Status for manager agent."""
    summary = get_pipeline_summary()
    expiring = get_expiring_soon(7)
    return {
        "name": "quote_lifecycle",
        "status": "ok",
        "scheduler_running": _scheduler_running,
        "pipeline": summary.get("pipeline", {}),
        "expiring_in_7_days": len(expiring),
        "conversion_rate": summary.get("conversion_rate", 0),
    }
