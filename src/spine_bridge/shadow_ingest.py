"""Shadow-ingest helper — parent ingest_pipeline → Spine substrate.

The legacy ingest_pipeline (src/core/ingest_pipeline.py) writes the
canonical PC / RFQ row + items into the legacy JSON store. This module
provides a single best-effort call that ALSO builds an EmailContract
+ Spine Quote and persists them to spine.db so the substrate fills
with real data during the 30-day shadow window.

Architectural rules:
- Best-effort only. Never raises into the parent pipeline. Failures
  are logged + recorded in a return dict; the parent's ok-path is
  unaffected.
- Feature-flagged. `SPINE_SHADOW_INGEST_ENABLED` env var (default off)
  gates the entire helper. Flip on to begin the shadow window.
- Identity-driven idempotency. Re-running ingest on the same RFQ
  produces a NEW contract_id (immutable history); the Spine's
  write_email_contract enforces this. We never modify a prior row.
- One-way: shadow writes to Spine, never reads back to influence the
  legacy path. The two substrates stay decoupled until cannibalization.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("spine_bridge.shadow_ingest")


_FLAG_ENV = "SPINE_SHADOW_INGEST_ENABLED"


def _flag_on() -> bool:
    return str(os.environ.get(_FLAG_ENV, "0")).strip() in ("1", "true", "True", "yes", "on")


def _make_tax_resolver():
    """Wrap the prod CDTFA resolver into the bps-returning callable
    that ingest_email_contract expects.

    src.core.tax_resolver.resolve_tax returns a dict
    {"ok": bool, "rate": float, ...} where rate is decimal (0.0825).
    The Spine wants integer bps.
    """
    from src.core.tax_resolver import resolve_tax

    def _resolver(address: str) -> int | None:
        try:
            r = resolve_tax(address)
        except Exception as e:
            log.warning("shadow_ingest: resolve_tax raised: %s", e)
            return None
        if not isinstance(r, dict) or not r.get("ok"):
            return None
        rate = r.get("rate")
        if rate is None or rate <= 0:
            return None
        try:
            return int(round(float(rate) * 10000))
        except (TypeError, ValueError):
            return None

    return _resolver


def _spine_db_path() -> str:
    """Same resolution as routes_spine.py — env override or DATA_DIR."""
    p = os.environ.get("SPINE_DB_PATH")
    if p:
        return p
    try:
        from src.core.paths import DATA_DIR
        return str(os.path.join(str(DATA_DIR), "spine.db"))
    except Exception:
        return os.path.join(os.getcwd(), "data", "spine.db")


def _build_contract_dict(
    *,
    record_id: str,
    record_type: str,
    classification: Any,
    header: dict,
    items: list[dict],
    email_subject: str,
    email_sender: str,
    gmail_thread_id: str,
    gmail_message_id: str,
    email_received_at: str,
) -> dict:
    """Project the legacy ingest state into the Spine contract dict
    shape ingest_email_contract expects."""

    # classification may be a RequestClassification dataclass or a dict.
    def _c(name: str, default: Any = "") -> Any:
        if classification is None:
            return default
        if isinstance(classification, dict):
            return classification.get(name, default)
        return getattr(classification, name, default)

    agency_raw = _c("agency") or _c("agency_name") or ""
    # Spine v1 is CCHCS-only — anything else, ingest will reject. We
    # still try, the result.issues will record the reason.
    agency_upper = str(agency_raw).strip().upper()
    if agency_upper in ("CCHCS", "CCHCS-ACQ"):
        agency = "CCHCS"
    else:
        agency = agency_upper or "CCHCS"  # let Spine reject if not supported

    ship_to = (
        (header.get("ship_to") or "").strip()
        or (header.get("delivery_address") or "").strip()
        or (header.get("delivery_location") or "").strip()
    )

    facility = (
        (header.get("institution") or "").strip()
        or _c("institution")
        or ship_to.split("\n", 1)[0].strip()
        or "UNKNOWN"
    )

    sol = (
        _c("solicitation_number")
        or (header.get("solicitation_number") or "")
        or (header.get("pc_number") or "")
    )

    line_items = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        desc = (it.get("description") or "").strip()
        if not desc:
            continue
        line_items.append({
            "description": desc,
            "qty": it.get("qty") or it.get("quantity") or 1,
            "uom": (it.get("uom") or it.get("unit") or "EA"),
            "item_number": (
                it.get("item_number") or it.get("mfg_number") or it.get("mfg") or ""
            ),
        })

    return {
        "rfq_id": record_id,
        "agency": agency,
        "facility": facility,
        "ship_to": ship_to,
        "solicitation_number": sol,
        "line_items": line_items,
        "buyer": {
            "name": (header.get("buyer_name") or "").strip(),
            "email": email_sender or "",
        },
        "due_date": header.get("due_date") or "",
        "rfq_title": email_subject or header.get("rfq_title") or "",
        "source_email_id": gmail_message_id or "",
        "source_thread_id": gmail_thread_id or "",
        "pc_id": record_id if record_type == "pc" else "",
        "parser_version": _c("producer_signature") or "ingest_pipeline",
    }


def shadow_ingest_to_spine(
    *,
    record_id: str,
    record_type: str,
    classification: Any,
    header: dict,
    items: list[dict],
    email_subject: str = "",
    email_sender: str = "",
    gmail_thread_id: str = "",
    gmail_message_id: str = "",
    email_received_at: str = "",
    db_path: str | None = None,
) -> dict:
    """Best-effort write of one inbound RFQ into the Spine substrate.

    Returns a dict {ok, contract_id, quote_id, issues, reason}.
    Always returns; never raises into the parent pipeline.

    The caller wraps the entire call in try/except Exception anyway;
    this helper additionally catches every internal exception so a
    failed shadow ingest is a no-op for the parent.
    """
    out: dict = {
        "ok": False,
        "contract_id": None,
        "quote_id": None,
        "issues": [],
        "reason": None,
    }

    if not _flag_on():
        out["reason"] = "flag_off"
        return out

    if not record_id:
        out["reason"] = "no_record_id"
        return out

    try:
        contract = _build_contract_dict(
            record_id=record_id,
            record_type=record_type,
            classification=classification,
            header=header,
            items=items,
            email_subject=email_subject,
            email_sender=email_sender,
            gmail_thread_id=gmail_thread_id,
            gmail_message_id=gmail_message_id,
            email_received_at=email_received_at,
        )
    except Exception as e:
        log.exception("shadow_ingest: building contract dict failed for %s", record_id)
        out["reason"] = f"build_failed: {e}"
        return out

    try:
        from src.spine import (
            init_db, write_email_contract, write_quote,
        )
        from src.spine_bridge.ingest import ingest_email_contract

        tax_resolver = _make_tax_resolver()
        ingest_ts = datetime.now(timezone.utc)
        result = ingest_email_contract(
            contract, tax_resolver=tax_resolver, ingest_ts=ingest_ts,
        )
    except Exception as e:
        log.exception("shadow_ingest: ingest_email_contract failed for %s", record_id)
        out["reason"] = f"ingest_call_failed: {e}"
        return out

    if not result.ok:
        out["issues"] = [
            {"severity": i.severity, "field": i.field_path, "detail": i.detail}
            for i in result.issues
        ]
        out["reason"] = "ingest_rejected"
        return out

    try:
        path = db_path or _spine_db_path()
        # init_db is idempotent — costs us nothing to call here in case
        # the shadow path runs before routes_spine has registered.
        init_db(path)
        write_email_contract(path, result.email_contract)
        persisted_quote = write_quote(
            path, result.quote, actor="spine_shadow_ingest"
        )
    except Exception as e:
        log.exception("shadow_ingest: persist failed for %s", record_id)
        out["reason"] = f"persist_failed: {e}"
        return out

    out["ok"] = True
    out["contract_id"] = result.email_contract.contract_id
    out["quote_id"] = result.quote.quote_id
    out["reason"] = "shadow_written"

    # ── Auto-link PC predecessor (Mike 5/17 directive) ──────────────
    # Run the matcher against prior quotes at the same facility. If
    # confidence ≥ AUTO_LINK_THRESHOLD, persist the top match so the
    # editor + auto-price substrate can read it. Best-effort: a matcher
    # failure does NOT fail the ingest (the quote is already written).
    linked_pc_id: str | None = None
    try:
        link_info = _maybe_write_auto_link(path, persisted_quote)
        if link_info is not None:
            out["auto_link"] = link_info
            linked_pc_id = link_info["to_quote_id"]
    except Exception as e:
        log.exception(
            "shadow_ingest: auto-link failed for %s (non-fatal)",
            persisted_quote.quote_id,
        )
        out["auto_link_error"] = str(e)

    # ── Auto-price carry-forward (Mike 5/17 directive — "should be
    # auto priced"). Only fires when a link was just written: the link
    # is the substrate's evidence that the PC predecessor is the right
    # cost source. Best-effort: a carry failure does NOT fail ingest
    # and does NOT undo the link.
    if linked_pc_id is not None:
        try:
            carry_info = _maybe_carry_costs(path, persisted_quote, linked_pc_id)
            if carry_info is not None:
                out["auto_price"] = carry_info
        except Exception as e:
            log.exception(
                "shadow_ingest: auto-price failed for %s (non-fatal)",
                persisted_quote.quote_id,
            )
            out["auto_price_error"] = str(e)

    return out


def _maybe_write_auto_link(path, target_quote) -> dict | None:
    """Run the matcher and persist a link if it clears the threshold.

    Returns metadata dict on success, None when nothing matched.
    Isolated so the ingest function stays a straight line; testable
    independently with seeded DB state.
    """
    from src.spine import (
        find_pc_candidates,
        iter_quote_ids,
        read_quote,
        write_quote_link,
    )

    # Pull all prior quote IDs and resolve each. The Spine DB is
    # small in shadow mode (<1k rows); a full scan is fine. When
    # the substrate scales, add a facility-indexed reader to db.py
    # so this becomes a SQL filter instead of a Python filter.
    candidates = []
    for qid in iter_quote_ids(path):
        if qid == target_quote.quote_id:
            continue
        q = read_quote(path, qid)
        if q is None:
            continue
        candidates.append(q)

    matches = find_pc_candidates(target_quote, candidates)
    if not matches:
        return None

    top = matches[0]
    link = write_quote_link(
        path,
        from_quote_id=target_quote.quote_id,
        to_quote_id=top["quote_id"],
        match_method="auto_mfg_desc",
        confidence=top["confidence"],
        evidence=top["evidence"],
        actor="spine_auto_linker",
    )
    return {
        "to_quote_id": top["quote_id"],
        "confidence": top["confidence"],
        "match_method": "auto_mfg_desc",
        "link_id": link["link_id"],
        "candidates_considered": len(candidates),
        "matches_above_threshold": len(matches),
    }


def _maybe_carry_costs(path, target_quote, source_quote_id) -> dict | None:
    """If the linked PC has prior validated costs on MFG#-matched
    lines, carry them forward and write the enriched Quote back.

    Returns a summary dict on actual carry, None when nothing was
    carried (matcher linked on sol# alone with no overlapping MFG#,
    target already had costs, or source PC was itself unpriced).

    Best-effort sibling of _maybe_write_auto_link — same isolation
    discipline so the ingest function stays a straight line and so
    the helper is testable independently with seeded DB state.
    """
    from src.spine import (
        carry_forward_costs,
        read_quote,
        write_quote,
    )

    source = read_quote(path, source_quote_id)
    if source is None:
        # Linked PC vanished between link-write and carry — shouldn't
        # happen in single-process ingest, but defensive.
        return None

    new_target, summary = carry_forward_costs(target_quote, source)
    if not summary["carried"]:
        # Link existed (same sol# or partial MFG#) but no exact-MFG#
        # matches with non-zero source costs — nothing to carry.
        # Still report deltas if any so observability picks them up.
        if not summary["deltas"]:
            return None
        return {
            "source_quote_id": source_quote_id,
            "carried_count": 0,
            "delta_count": len(summary["deltas"]),
            "carried": [],
            "deltas": summary["deltas"],
        }

    # Persist the enriched Quote. This is a second write to the same
    # quote_id — the event log appends, the seq does NOT reassign
    # (already stamped on the first write).
    write_quote(
        path,
        new_target,
        actor="spine_auto_pricer",
        note=f"auto_price_carried from {source_quote_id}",
    )
    return {
        "source_quote_id": source_quote_id,
        "carried_count": len(summary["carried"]),
        "delta_count": len(summary["deltas"]),
        "carried": summary["carried"],
        "deltas": summary["deltas"],
    }
