"""Persistence layer for the observed-send detector (PR-G2).

PR-G1 (#814) shipped the detector primitive — it scans Gmail Sent
folder + matches outbound messages back to existing RFQ/PC records,
returns a structured result, but doesn't persist anything. This module
is the storage half: convert detector results into rows in the
`observed_sends` table (migration 41), surface confirm/reject helpers,
and expose a list query for the admin UI (PR-G3).

When `confirm()` fires, the helper also appends the gmail_message_id
to the matched record's `gmail_message_ids` list (PR #808 column
shape) so the buyer-reply routing in PR-E (#813) and any future
operator-side display sees the same message-graph as the inbound side.

Doctrine:
* Confirm/reject are operator-driven for the first 8 weeks (Q5
  doctrine — see project_thread_aware_ingest_session_2026_05_07.md).
* Auto-attach (status='auto_attached') is reserved for PR-G4 and only
  fires when 8-week confirm rate ≥ 100%.
* Reytech Law 22 — never delete observations. `reject` flips status
  but the row + its gmail_message_id stay so the same message can't
  be re-imported as a "missed send" if Mike re-runs the scanner.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z")


def _row_to_dict(row) -> Dict:
    """sqlite3.Row → plain dict — convenient for tests + JSON."""
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}


# ─── Upsert from detector ─────────────────────────────────────────────


def upsert_from_detection(detection_result: Dict, *,
                          conn=None) -> Dict:
    """Take the dict returned by `detect_observed_sends` and persist
    each match as a pending observation. Idempotent — re-running the
    scanner against the same Sent folder doesn't create duplicate
    rows (UNIQUE constraint on gmail_message_id).

    `unmatched` items are NOT persisted. They're informational —
    appearing in the result but not the table — so the operator UI
    only shows actionable rows. (Future PR-G2.5 might surface unmatched
    in a separate review queue.)

    Returns:
      {
        "ok": bool,
        "inserted": int,
        "updated": int,        # already_attached or status drift
        "skipped_already_decided": int,
        "rows": [{id, gmail_message_id, status, ...}],
      }
    """
    if not detection_result.get("ok"):
        return {"ok": False,
                "error": "detection result not ok",
                "inserted": 0, "updated": 0,
                "skipped_already_decided": 0, "rows": []}

    own_conn = False
    if conn is None:
        from src.core.db import get_db
        conn = get_db().__enter__()  # type: ignore
        own_conn = True

    try:
        inserted = 0
        updated = 0
        skipped_already_decided = 0
        rows_out: List[Dict] = []

        for m in detection_result.get("matches", []):
            gid = (m.get("gmail_message_id") or "").strip()
            if not gid:
                continue

            # Lookup existing row.
            existing = conn.execute(
                "SELECT * FROM observed_sends WHERE gmail_message_id=?",
                (gid,),
            ).fetchone()

            now = _utc_iso()
            if existing:
                # Don't clobber decisions the operator already made.
                if existing["status"] in ("confirmed", "rejected",
                                          "auto_attached"):
                    skipped_already_decided += 1
                    rows_out.append(_row_to_dict(existing))
                    continue
                # Refresh the match details + bump updated_at.
                conn.execute("""
                    UPDATE observed_sends
                       SET thread_id=?, subject=?, to_email=?, sent_at=?,
                           matched_record_id=?, matched_record_kind=?,
                           match_signal=?, match_value=?, confidence=?,
                           updated_at=?
                     WHERE gmail_message_id=?
                """, (
                    m.get("thread_id", ""),
                    m.get("subject", "")[:200],
                    m.get("to", "")[:200],
                    m.get("date", ""),
                    m.get("matched_record_id", ""),
                    m.get("matched_record_kind", ""),
                    m.get("match_signal", ""),
                    m.get("match_value", ""),
                    float(m.get("confidence", 0)),
                    now,
                    gid,
                ))
                updated += 1
                row = conn.execute(
                    "SELECT * FROM observed_sends WHERE gmail_message_id=?",
                    (gid,),
                ).fetchone()
            else:
                conn.execute("""
                    INSERT INTO observed_sends (
                        gmail_message_id, thread_id, subject, to_email,
                        sent_at, matched_record_id, matched_record_kind,
                        match_signal, match_value, confidence,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """, (
                    gid,
                    m.get("thread_id", ""),
                    m.get("subject", "")[:200],
                    m.get("to", "")[:200],
                    m.get("date", ""),
                    m.get("matched_record_id", ""),
                    m.get("matched_record_kind", ""),
                    m.get("match_signal", ""),
                    m.get("match_value", ""),
                    float(m.get("confidence", 0)),
                    now, now,
                ))
                inserted += 1
                row = conn.execute(
                    "SELECT * FROM observed_sends WHERE gmail_message_id=?",
                    (gid,),
                ).fetchone()
            rows_out.append(_row_to_dict(row))

        if own_conn:
            conn.commit()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "skipped_already_decided": skipped_already_decided,
            "rows": rows_out,
        }
    finally:
        if own_conn:
            try:
                conn.__exit__(None, None, None)
            except Exception:
                pass


# ─── List ─────────────────────────────────────────────────────────────


def list_observed_sends(*, status: Optional[str] = None,
                        limit: int = 200,
                        conn=None) -> List[Dict]:
    """List observation rows, newest first. Filter by status when given.

    Caller passes `status='pending'` for the operator review queue,
    `status='confirmed'` for already-attached audit, etc.
    """
    own_conn = False
    if conn is None:
        from src.core.db import get_db
        conn = get_db().__enter__()  # type: ignore
        own_conn = True

    try:
        if status:
            rows = conn.execute(
                "SELECT * FROM observed_sends WHERE status=? "
                "ORDER BY created_at DESC LIMIT ?",
                (status, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM observed_sends "
                "ORDER BY created_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        if own_conn:
            try:
                conn.__exit__(None, None, None)
            except Exception:
                pass


# ─── Confirm / Reject ─────────────────────────────────────────────────


def _append_to_record_message_ids(matched_record_id: str,
                                  matched_record_kind: str,
                                  gmail_message_id: str) -> bool:
    """Append the gmail_message_id to the matched RFQ/PC's
    `gmail_message_ids` list (idempotent). Mirrors PR-E's behavior so
    that confirm fires the same forward-path stamping as buyer-reply
    routing — the message-graph stays the canonical source for "which
    Gmail messages belong to this record".
    """
    if not matched_record_id or not matched_record_kind \
            or not gmail_message_id:
        return False
    try:
        if matched_record_kind == "rfq":
            from src.api.data_layer import load_rfqs, save_rfqs
            recs = load_rfqs() or {}
        else:
            from src.api.data_layer import _load_price_checks, _save_price_checks
            recs = _load_price_checks() or {}
        rec = recs.get(matched_record_id)
        if not rec:
            log.warning("observed-send confirm: matched %s %s vanished",
                        matched_record_kind, matched_record_id)
            return False
        msgs = list(rec.get("gmail_message_ids") or [])
        if gmail_message_id not in msgs:
            msgs.append(gmail_message_id)
            rec["gmail_message_ids"] = msgs
            rec.setdefault("audit_log", []).append({
                "at": _utc_iso(),
                "actor": "observed_send.confirm",
                "action": "outbound-attached",
                "gmail_message_id": gmail_message_id,
            })
            recs[matched_record_id] = rec
            if matched_record_kind == "rfq":
                save_rfqs(recs)
            else:
                _save_price_checks(recs)
        return True
    except Exception as e:
        log.exception("observed-send confirm: failed to attach %s to "
                      "%s %s: %s", gmail_message_id,
                      matched_record_kind, matched_record_id, e)
        return False


def confirm(observation_id: int, *, by: str = "operator",
            notes: str = "", conn=None) -> Dict:
    """Mark an observation as confirmed and stamp its gmail_message_id
    onto the matched record's gmail_message_ids list."""
    own_conn = False
    if conn is None:
        from src.core.db import get_db
        conn = get_db().__enter__()  # type: ignore
        own_conn = True

    try:
        row = conn.execute(
            "SELECT * FROM observed_sends WHERE id=?",
            (int(observation_id),),
        ).fetchone()
        if not row:
            return {"ok": False, "error": "observation not found",
                    "id": observation_id}
        if row["status"] in ("confirmed", "auto_attached"):
            # Idempotent — re-confirming a confirmed obs is a no-op.
            return {"ok": True, "row": _row_to_dict(row),
                    "no_change": True}
        if row["status"] == "rejected":
            return {"ok": False,
                    "error": "cannot confirm a rejected observation",
                    "row": _row_to_dict(row)}

        attached = _append_to_record_message_ids(
            row["matched_record_id"],
            row["matched_record_kind"],
            row["gmail_message_id"],
        )
        now = _utc_iso()
        conn.execute("""
            UPDATE observed_sends
               SET status='confirmed', decided_by=?, decided_at=?,
                   notes=?, updated_at=?
             WHERE id=?
        """, (by, now, notes, now, int(observation_id)))
        if own_conn:
            conn.commit()

        new_row = conn.execute(
            "SELECT * FROM observed_sends WHERE id=?",
            (int(observation_id),),
        ).fetchone()
        return {"ok": True, "row": _row_to_dict(new_row),
                "attached_to_record": attached}
    finally:
        if own_conn:
            try:
                conn.__exit__(None, None, None)
            except Exception:
                pass


def reject(observation_id: int, *, by: str = "operator",
           reason: str = "", conn=None) -> Dict:
    """Mark an observation as rejected. Reytech Law 22 — the row
    itself stays in the table so a future scanner pass treats the
    same message as already-decided, not a fresh missed send."""
    own_conn = False
    if conn is None:
        from src.core.db import get_db
        conn = get_db().__enter__()  # type: ignore
        own_conn = True

    try:
        row = conn.execute(
            "SELECT * FROM observed_sends WHERE id=?",
            (int(observation_id),),
        ).fetchone()
        if not row:
            return {"ok": False, "error": "observation not found",
                    "id": observation_id}
        if row["status"] == "rejected":
            return {"ok": True, "row": _row_to_dict(row),
                    "no_change": True}
        if row["status"] in ("confirmed", "auto_attached"):
            return {"ok": False,
                    "error": "cannot reject a confirmed observation",
                    "row": _row_to_dict(row)}

        now = _utc_iso()
        conn.execute("""
            UPDATE observed_sends
               SET status='rejected', decided_by=?, decided_at=?,
                   notes=?, updated_at=?
             WHERE id=?
        """, (by, now, reason, now, int(observation_id)))
        if own_conn:
            conn.commit()

        new_row = conn.execute(
            "SELECT * FROM observed_sends WHERE id=?",
            (int(observation_id),),
        ).fetchone()
        return {"ok": True, "row": _row_to_dict(new_row)}
    finally:
        if own_conn:
            try:
                conn.__exit__(None, None, None)
            except Exception:
                pass
