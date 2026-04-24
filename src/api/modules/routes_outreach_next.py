# routes_outreach_next.py — "Contact Today" decision card (§Option A from
# growth/outreach surface review). Answers in one glance:
#   1. Who should Mike contact today
#   2. Why (top gap item + top win-back item)
#   3. With what message (1-click draft, A/B strategies)
# Reuses signals already produced by prospect_scorer + scprs_po_lines.
# No new agents, no new tables.

from flask import request, jsonify
from src.api.shared import bp, auth_required
from src.api.render import render_page
import logging

log = logging.getLogger("reytech.outreach_next")


def _get_db():
    """Late-bind get_db so test monkeypatching against src.core.db.get_db
    actually reaches this module (the exec-loader copies module-level
    bindings into dashboard's namespace once, freezing them otherwise)."""
    from src.core.db import get_db
    return get_db()


def _top_items_for_dept(conn, dept_code: str, opportunity_flag: str, limit: int = 3):
    """Top N items by line_total for an agency, filtered by opportunity_flag.

    is_test=0 baked in (matches the rest of the SCPRS read-site filter
    discipline from PR #491).
    """
    rows = conn.execute("""
        SELECT l.description,
               COUNT(*) as times_ordered,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_unit_price,
               MAX(p.supplier) as last_supplier
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id = p.id
        WHERE p.dept_code = ?
          AND l.opportunity_flag = ?
          AND l.line_total > 0
          AND p.is_test = 0
          AND l.is_test = 0
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT ?
    """, (dept_code, opportunity_flag, limit)).fetchall()
    return [dict(r) for r in rows]


def _response_history_for_emails(conn, contact_emails):
    """Past outreach signal per prospect.

    Returns {email (lowercased): {sent, opened, clicked, last_sent, last_opened}}.
    Answers the "what will land" question: if we've emailed this buyer
    3 times with zero opens, email fatigue is real — operator should
    try phone. If opens are happening but no replies, relationship-angle
    template likely outperforms price-angle.

    Schema-tolerant: falls back to empty dict if email_outbox or
    email_engagement tables don't exist (fresh DB / test sandbox).
    """
    if not contact_emails:
        return {}
    try:
        # email_outbox may use `to_address` (current schema) or `recipient`
        # (older tests). Detect via sqlite_master.
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(email_outbox)").fetchall()}
        if not cols:
            return {}
        addr_col = "to_address" if "to_address" in cols else (
            "recipient" if "recipient" in cols else None)
        if not addr_col:
            return {}

        # Does email_engagement exist? If not, we still return sent counts.
        has_eng = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='email_engagement'"
        ).fetchone() is not None

        placeholders = ",".join(["?"] * len(contact_emails))
        lowered = [e.lower() for e in contact_emails]

        if has_eng:
            rows = conn.execute(f"""
                SELECT LOWER(o.{addr_col}) as email,
                       COUNT(DISTINCT o.id) as sent,
                       MAX(o.sent_at) as last_sent,
                       SUM(CASE WHEN e.event_type='open' THEN 1 ELSE 0 END) as opened,
                       SUM(CASE WHEN e.event_type='click' THEN 1 ELSE 0 END) as clicked,
                       MAX(CASE WHEN e.event_type='open' THEN e.event_at END) as last_opened
                FROM email_outbox o
                LEFT JOIN email_engagement e ON e.email_id = o.id
                WHERE o.status IN ('sent','delivered')
                  AND LOWER(o.{addr_col}) IN ({placeholders})
                GROUP BY LOWER(o.{addr_col})
            """, lowered).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT LOWER({addr_col}) as email,
                       COUNT(*) as sent,
                       MAX(sent_at) as last_sent,
                       0 as opened, 0 as clicked, NULL as last_opened
                FROM email_outbox
                WHERE status IN ('sent','delivered')
                  AND LOWER({addr_col}) IN ({placeholders})
                GROUP BY LOWER({addr_col})
            """, lowered).fetchall()
    except Exception as _e:
        log.debug("response_history suppressed: %s", _e)
        return {}

    out = {}
    for r in rows:
        d = dict(r)
        out[d["email"]] = {
            "sent": d.get("sent") or 0,
            "opened": d.get("opened") or 0,
            "clicked": d.get("clicked") or 0,
            "last_sent": d.get("last_sent"),
            "last_opened": d.get("last_opened"),
        }
    return out


def _response_signal(history: dict, has_phone: bool) -> dict:
    """Classify the per-prospect response history into an action hint.

    Returns {level, label, hint} where level ∈ {none, cold, warm, engaged, fatigued}.
    Drives the color + copy on each card so the operator sees which
    channel is likely to land before they click Draft.
    """
    if not history or not history.get("sent"):
        return {"level": "none", "label": "No prior contact",
                "hint": "🆕 First outreach — try the price-hook draft."}
    sent = history.get("sent", 0)
    opened = history.get("opened", 0)
    clicked = history.get("clicked", 0)
    open_rate = (opened / sent * 100) if sent else 0
    if clicked > 0 or opened >= 2:
        return {"level": "engaged",
                "label": f"{sent} sent · {opened} opened · {clicked} clicked",
                "hint": "🟢 Engaged — follow up with a specific quote or call."}
    if sent >= 3 and opened == 0:
        return {"level": "fatigued",
                "label": f"{sent} sent · 0 opened",
                "hint": ("📞 Email fatigue — try phone instead."
                         if has_phone else
                         "⚠ 3+ emails, no opens — find a phone number.")}
    if sent >= 1 and opened == 0:
        return {"level": "cold",
                "label": f"{sent} sent · 0 opened",
                "hint": "🟡 No opens yet — try a different subject angle (strategy B)."}
    if opened >= 1:
        return {"level": "warm",
                "label": f"{sent} sent · {opened} opened ({open_rate:.0f}%)",
                "hint": "🟡 Opening but not replying — try a specific ask."}
    return {"level": "none", "label": "No prior contact",
            "hint": "🆕 First outreach."}


def _existing_drafts_for_prospects(conn, contact_emails):
    """Map buyer_email → list of recent draft IDs for outbox-link surfacing."""
    if not contact_emails:
        return {}
    placeholders = ",".join(["?"] * len(contact_emails))
    try:
        rows = conn.execute(f"""
            SELECT id, recipient, subject, status, created_at
            FROM email_outbox
            WHERE recipient IN ({placeholders})
              AND status IN ('draft', 'pending', 'queued')
              AND created_at > date('now', '-30 days')
            ORDER BY created_at DESC
        """, list(contact_emails)).fetchall()
    except Exception as _e:
        # email_outbox may not exist on fresh DBs — fail soft.
        log.debug("outbox lookup suppressed: %s", _e)
        return {}
    out = {}
    for r in rows:
        rec = (r["recipient"] or "").lower()
        out.setdefault(rec, []).append({
            "id": r["id"], "subject": r["subject"],
            "status": r["status"], "created_at": r["created_at"],
        })
    return out


def _build_card_list(limit: int = 8):
    """Compose the decision-card list. Returns list of dicts ready for the
    template — one card per prospect, with reason inline."""
    from src.agents.prospect_scorer import score_prospects

    scored = score_prospects(limit=limit * 2)  # pull 2x so we have headroom
    if not scored.get("ok"):
        return [], scored.get("error") or "scoring failed", {}

    prospects = scored.get("prospects", []) or []
    cards = []
    summary = scored.get("summary", {}) or {}

    # Collect contact emails for one-shot outbox lookup.
    all_emails = []
    for p in prospects[:limit]:
        for c in (p.get("contacts") or [])[:1]:
            if c.get("email"):
                all_emails.append(c["email"].lower())

    with _get_db() as conn:
        outbox_map = _existing_drafts_for_prospects(conn, all_emails)
        response_map = _response_history_for_emails(conn, all_emails)
        for p in prospects[:limit]:
            dept_code = p.get("dept_code", "")
            gaps = _top_items_for_dept(conn, dept_code, "GAP_ITEM", limit=3)
            win_back = _top_items_for_dept(conn, dept_code, "WIN_BACK", limit=3)

            # Pick the headline contact — first buyer with an email.
            primary = next(
                (c for c in (p.get("contacts") or []) if c.get("email")),
                (p.get("contacts") or [None])[0] if p.get("contacts") else None,
            )

            # Compose the 1-line "why" — concrete dollars, not vibes.
            why_parts = []
            if win_back:
                top_wb = win_back[0]
                why_parts.append(
                    f"They buy {top_wb['description'][:60]} from "
                    f"{top_wb.get('last_supplier') or 'a competitor'} "
                    f"(${top_wb['total_spend']:,.0f}/yr) — we sell this."
                )
            if gaps:
                top_gap = gaps[0]
                why_parts.append(
                    f"They also buy {top_gap['description'][:60]} "
                    f"(${top_gap['total_spend']:,.0f}/yr) — gap item we could add."
                )
            if not why_parts:
                why_parts.append(
                    f"${p.get('total_spend', 0):,.0f} annual SCPRS spend, "
                    f"score {p.get('score')}."
                )

            email_lc = (primary.get("email") or "").lower() if primary else ""
            history = response_map.get(email_lc) if email_lc else None
            has_phone = bool(primary and primary.get("phone"))
            signal = _response_signal(history or {}, has_phone)
            cards.append({
                "dept_code": dept_code,
                "dept_name": p.get("dept_name") or dept_code,
                "agency_code": p.get("agency_code"),
                "score": p.get("score"),
                "score_breakdown": p.get("score_breakdown") or {},
                "relationship": p.get("relationship"),
                "total_spend": p.get("total_spend"),
                "gap_spend": p.get("gap_spend"),
                "winback_spend": p.get("winback_spend"),
                "po_count": p.get("po_count"),
                "last_po_date": p.get("last_po_date"),
                "primary_contact": primary,
                "extra_contacts": [c for c in (p.get("contacts") or []) if c is not primary][:2],
                "gap_items": gaps,
                "win_back_items": win_back,
                "why_lines": why_parts,
                "existing_drafts": outbox_map.get(email_lc, []) if email_lc else [],
                "response_history": history or {},
                "response_signal": signal,
            })

    return cards, None, summary


@bp.route("/outreach/next")
@auth_required
@safe_page
def page_outreach_next():
    """Contact Today — one-page operator decision view.
    Top N prospects ranked by score, each with concrete why + 1-click
    draft buttons. Cuts the multi-tab outreach loop to a single screen.
    """
    err = None
    cards = []
    summary = {}
    try:
        cards, err, summary = _build_card_list(limit=8)
    except Exception as e:
        log.exception("page_outreach_next: card build failed")
        err = f"{type(e).__name__}: {e}"

    return render_page(
        "outreach_next.html",
        active_page="Growth",
        cards=cards or [],
        summary=summary or {},
        error=err,
    )


@bp.route("/api/outreach/next/draft", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_draft():
    """Generate an outreach email draft for one prospect + strategy.

    Body: {"buyer_email": "...", "strategy": "A" | "B"}
    Returns the draft (subject, body, etc.) for inline display.
    The caller is responsible for sending — this is a draft generator,
    not an outbox writer (avoids accidental sends from the decision card).
    """
    try:
        from src.agents.outreach_agent import generate_outreach_email
        payload = request.get_json(force=True, silent=True) or {}
        email = (payload.get("buyer_email") or "").strip()
        strategy = (payload.get("strategy") or "A").upper()
        if strategy not in ("A", "B"):
            return jsonify({"ok": False, "error": "strategy must be A or B"}), 400
        if not email:
            return jsonify({"ok": False, "error": "buyer_email required"}), 400
        draft = generate_outreach_email(email, strategy=strategy)
        if draft.get("error"):
            return jsonify({"ok": False, "error": draft["error"]}), 404
        return jsonify({"ok": True, "draft": draft})
    except Exception as e:
        log.exception("api_outreach_next_draft failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500
