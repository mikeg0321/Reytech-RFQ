"""Growth signals endpoint — surfaces buyer-last-won + SCPRS ceiling
inline on the quote detail page (rfq_detail.html / pc_detail.html).

PR-3 from the 2026-05-06 audit. The data both signals report on
already exists:

  * buyer-last-won — the same query that powers
    `/growth-intel/quote?id=...` cost-trace column "Last won (buyer)".
    Mike's stated job 1: "good data to win." Today it lives 2 clicks
    deep on `/growth-intel/buyer`. Nobody navigates there mid-quote.

  * SCPRS recent ceiling — what the state recently paid another
    vendor for the same item. Strict reference price (per
    CLAUDE.md "Pricing Guard Rails" — never our cost basis).

Surfacing both per-line at quote time is the substrate move from the
audit: bring the signals to the input box where the operator decides
the price, instead of forcing them to navigate to side-tabs.

Single endpoint: GET /api/quote/<doc_type>/<rid>/growth-signals
where doc_type ∈ {rfq, pc}. Returns:

  {
    "ok": True,
    "doc_type": "rfq" | "pc",
    "buyer_email": "...",
    "items": [
      {
        "line_no": 1,
        "description": "...",
        "part_number": "...",
        "last_won":   {"price": 12.34, "quote_number": "R26Q40",
                       "won_at": "2026-04-15"} | null,
        "scprs":      {"price": 18.00, "supplier": "Henry Schein",
                       "po_date": "2026-03-22"} | null
      },
      ...
    ]
  }

`null` for either signal means we found nothing in the lookup window.
The UI shows a dash. Per-line lookups are CHEAP — single SQLite
connection reused across all items.

Both lookups read-only; failures are swallowed to log and the line
returns null so a partially-broken signal can't kill the panel.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from flask import Blueprint, jsonify

log = logging.getLogger("reytech.growth_signals")

bp = Blueprint("growth_signals", __name__)


# ── helpers ───────────────────────────────────────────────────────


def _scprs_recent_for_item(conn, description: str, part_number: str = "",
                           days: int = 365) -> Dict[str, Any] | None:
    """Most recent SCPRS line that matches the description / part_number.

    Match priority:
      1. Exact part_number match (when both non-empty)
      2. Description first-3-significant-words LIKE match

    Returns {price, supplier, po_date} or None when nothing found in
    the window. Caller passes a shared `conn` so 100-line quotes
    don't open 100 connections.
    """
    desc = (description or "").strip()
    pn = (part_number or "").strip()
    if not desc and not pn:
        return None
    desc_words = [w for w in desc.split() if len(w) >= 3][:3]
    desc_pattern = "%" + "%".join(desc_words) + "%" if desc_words else ""

    # Try part_number first — high confidence when both sides have it.
    if pn:
        try:
            row = conn.execute(
                """
                SELECT l.unit_price, m.supplier, m.start_date
                FROM scprs_po_lines l
                JOIN scprs_po_master m ON l.po_number = m.po_number
                WHERE COALESCE(l.is_test, 0) = 0
                  AND COALESCE(m.is_test, 0) = 0
                  AND (l.part_number = ? OR l.mfg_number = ?)
                  AND l.unit_price > 0
                  AND m.start_date >= date('now', ?)
                ORDER BY m.start_date DESC
                LIMIT 1
                """,
                (pn, pn, f"-{int(days)} days"),
            ).fetchone()
            if row and row["unit_price"]:
                return {
                    "price": round(float(row["unit_price"]), 2),
                    "supplier": (row["supplier"] or "")[:40],
                    "po_date": (row["start_date"] or "")[:10],
                }
        except Exception as e:
            # part_number/mfg_number columns may not exist on all
            # schema variants — fall through to description match.
            log.debug("scprs part_number lookup: %s", e)

    if not desc_pattern:
        return None

    try:
        row = conn.execute(
            """
            SELECT l.unit_price, m.supplier, m.start_date
            FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE COALESCE(l.is_test, 0) = 0
              AND COALESCE(m.is_test, 0) = 0
              AND LOWER(l.description) LIKE LOWER(?)
              AND l.unit_price > 0
              AND m.start_date >= date('now', ?)
            ORDER BY m.start_date DESC
            LIMIT 1
            """,
            (desc_pattern, f"-{int(days)} days"),
        ).fetchone()
        if row and row["unit_price"]:
            return {
                "price": round(float(row["unit_price"]), 2),
                "supplier": (row["supplier"] or "")[:40],
                "po_date": (row["start_date"] or "")[:10],
            }
    except Exception as e:
        log.debug("scprs description lookup: %s", e)

    return None


def _load_record(doc_type: str, rid: str):
    """Returns (record_dict, items_list, buyer_email) or (None, [], '')."""
    if doc_type == "rfq":
        try:
            from src.api.modules.routes_rfq import load_rfqs
            rfqs = load_rfqs()
            r = rfqs.get(rid)
            if not r:
                return None, [], ""
            items = r.get("line_items") or []
            email = (r.get("requestor_email")
                     or r.get("buyer_email")
                     or r.get("contact_email")
                     or "")
            return r, items, email
        except Exception as e:
            log.warning("growth_signals load rfq %s: %s", rid, e)
            return None, [], ""
    if doc_type == "pc":
        try:
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            pc = pcs.get(rid)
            if not pc:
                return None, [], ""
            items = pc.get("items") or pc.get("line_items") or []
            email = (pc.get("requestor_email")
                     or pc.get("buyer_email")
                     or pc.get("contact_email")
                     or "")
            return pc, items, email
        except Exception as e:
            log.warning("growth_signals load pc %s: %s", rid, e)
            return None, [], ""
    return None, [], ""


# ── endpoint ──────────────────────────────────────────────────────


@bp.route("/api/quote/<doc_type>/<rid>/growth-signals")
def api_quote_growth_signals(doc_type, rid):
    """Per-line buyer-last-won + SCPRS ceiling for a quote.

    doc_type ∈ {rfq, pc}. Returns shape documented at the top of
    this module. Read-only; defensive on every lookup.
    """
    if doc_type not in ("rfq", "pc"):
        return jsonify({"ok": False, "error": "doc_type must be rfq or pc"}), 400

    record, items, buyer_email = _load_record(doc_type, rid)
    if record is None:
        return jsonify({"ok": False, "error": "not found"}), 404

    out_items: List[Dict[str, Any]] = []

    # Single connection shared across all per-line lookups.
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Reuse the existing buyer-last-won helper from
            # routes_growth_intel — DRY rather than duplicate the
            # match logic.
            try:
                from src.api.modules.routes_growth_intel import (
                    _last_won_price_for_buyer,
                )
            except Exception as e:
                log.warning("could not import last_won helper: %s", e)
                _last_won_price_for_buyer = None

            # Quote number to exclude from "last won" (avoid showing
            # current quote as its own history).
            exclude_qn = (record.get("reytech_quote_number")
                          or record.get("quote_number") or "")

            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                desc = (item.get("description") or "").strip()
                pn = (item.get("part_number")
                      or item.get("item_number")
                      or item.get("mfg_number")
                      or "").strip()
                line_no = item.get("line_number") or item.get("line_no") or (idx + 1)

                last_won: Dict[str, Any] | None = None
                if _last_won_price_for_buyer and buyer_email:
                    try:
                        result = _last_won_price_for_buyer(
                            conn, buyer_email, desc, pn,
                            exclude_quote_number=exclude_qn,
                        )
                        if result and result.get("price"):
                            last_won = {
                                "price": result.get("price"),
                                "quote_number": result.get("quote_number") or "",
                                "won_at": result.get("won_at") or "",
                            }
                    except Exception as e:
                        log.debug("last_won lookup line %d: %s", idx, e)

                scprs = _scprs_recent_for_item(conn, desc, pn)

                out_items.append({
                    "line_no": line_no,
                    "description": desc[:120],
                    "part_number": pn,
                    "last_won": last_won,
                    "scprs": scprs,
                })
    except Exception as e:
        log.error("growth_signals query failed for %s %s: %s", doc_type, rid, e)
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "doc_type": doc_type,
        "buyer_email": buyer_email,
        "items": out_items,
    })
