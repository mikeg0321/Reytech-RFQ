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
               MAX(p.supplier) as last_supplier,
               MAX(l.category) as category
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


def _parse_end_date(s: str):
    """Parse scprs_po_master.end_date (TEXT) into a date. Returns None on
    any malformed input — callers must skip the row, not crash.

    Seen prod shapes: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS...', '', None,
    garbage. Be defensive.
    """
    from datetime import datetime
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s[:10]).date()
    except (ValueError, TypeError):
        return None


def _expiring_contracts_by_dept(conn, dept_codes, window_days: int = 120,
                                 award_gap_days: int = 30, limit_per_dept: int = 5):
    """Batched rebid-window query — single SQL for all dept_codes.

    Returns {dept_code: [contracts]}. Each contract dict:
      description, supplier, end_date, days_until_expiry (may be negative
      for award-gap rows), grand_total, opportunity_flag, is_reytech,
      is_award_gap.

    Rules:
      - is_test=0 on both tables (matches §3e discipline)
      - Sorted soonest-expiry first within each dept
      - Future contracts: up to window_days out
      - Past contracts: up to award_gap_days ago (diagnostic "AWARD-GAP")
      - Includes Reytech-as-incumbent contracts, tagged is_reytech=True
        (different card treatment — RENEWAL, not rebid)
    """
    if not dept_codes:
        return {}
    from datetime import datetime, date
    today = date.today()

    try:
        placeholders = ",".join(["?"] * len(dept_codes))
        # Query all dept_codes in one shot; parse + filter in Python because
        # end_date is TEXT and SQLite date math on TEXT is fragile.
        rows = conn.execute(f"""
            SELECT p.dept_code, p.supplier, p.end_date, p.grand_total,
                   l.description, l.line_total, l.opportunity_flag
            FROM scprs_po_master p
            JOIN scprs_po_lines l ON l.po_id = p.id
            WHERE p.dept_code IN ({placeholders})
              AND p.is_test = 0
              AND l.is_test = 0
              AND p.end_date IS NOT NULL
              AND p.end_date != ''
              AND l.line_total > 0
            ORDER BY p.end_date ASC
        """, list(dept_codes)).fetchall()
    except Exception as _e:
        log.debug("expiring_contracts_by_dept suppressed: %s", _e)
        return {}

    by_dept = {dc: [] for dc in dept_codes}
    seen = {dc: set() for dc in dept_codes}  # dedup (supplier, desc) per dept

    for r in rows:
        dc = r["dept_code"]
        if dc not in by_dept:
            continue
        end = _parse_end_date(r["end_date"])
        if end is None:
            continue
        delta = (end - today).days
        if delta < -award_gap_days or delta > window_days:
            continue  # outside the window we care about
        supplier = (r["supplier"] or "").strip()
        description = (r["description"] or "").strip()
        if not description:
            continue
        # Dedup: one row per (supplier, description) per dept.
        key = (supplier.lower(), description.lower())
        if key in seen[dc]:
            continue
        seen[dc].add(key)
        is_reytech = "REYTECH" in supplier.upper() or "REY TECH" in supplier.upper()
        by_dept[dc].append({
            "description": description,
            "supplier": supplier or "Unknown supplier",
            "end_date": r["end_date"][:10] if r["end_date"] else "",
            "days_until_expiry": delta,
            "grand_total": r["grand_total"] or 0,
            "line_total": r["line_total"] or 0,
            "opportunity_flag": r["opportunity_flag"],
            "is_reytech": is_reytech,
            "is_award_gap": delta < 0,
        })

    # Cap per dept (soonest-expiry already first from SQL ORDER BY).
    for dc in by_dept:
        by_dept[dc] = by_dept[dc][:limit_per_dept]
    return by_dept


def _rebid_urgency(expiring_contracts):
    """Points to add to the prospect's base score based on contract timing.

    Curve flipped per 2026-04-24 product-engineer review: the 61-90d window
    is highest leverage (time to get onto the RFQ distribution list before
    the successor solicitation posts). <30d is mostly too late to onboard a
    new bidder; the right action there is a rebid memo, not a registration.

      61-90d  → +30 (peak — register NOW if not already)
      31-60d  → +25 (rebid memo window open)
      91-120d → +15 (early awareness)
      0-30d   → +10 (late — memo-only, often already too late)
      award-gap (days<0): no boost (diagnostic, not an urgency)
      Reytech-as-incumbent: no boost (RENEWAL, different card treatment)
    """
    urgency = 0
    for c in expiring_contracts:
        if c["is_reytech"] or c["is_award_gap"]:
            continue
        d = c["days_until_expiry"]
        if 61 <= d <= 90:     urgency = max(urgency, 30)
        elif 31 <= d <= 60:   urgency = max(urgency, 25)
        elif 91 <= d <= 120:  urgency = max(urgency, 15)
        elif 0 <= d <= 30:    urgency = max(urgency, 10)
    return urgency


def _rebid_summary(expiring_contracts):
    """Operator-facing summary describing the rebid state of each card.

    Returns {level, label, hint} where level drives UI color:
      red       — any competitor contract ≤ 60d out (rebid memo window)
      amber     — any competitor contract 61-120d out (register NOW)
      renewal   — we (Reytech) are the incumbent and expiring ≤ 120d
      award_gap — competitor contract expired in last 30d (diagnostic)
      none      — nothing in the window
    """
    if not expiring_contracts:
        return {"level": "none", "label": "", "hint": ""}
    # Precedence: red > amber > renewal > award_gap.
    has_red = any(not c["is_reytech"] and not c["is_award_gap"]
                  and 0 <= c["days_until_expiry"] <= 60
                  for c in expiring_contracts)
    has_amber = any(not c["is_reytech"] and not c["is_award_gap"]
                    and 61 <= c["days_until_expiry"] <= 120
                    for c in expiring_contracts)
    has_renewal = any(c["is_reytech"] and not c["is_award_gap"]
                      and 0 <= c["days_until_expiry"] <= 120
                      for c in expiring_contracts)
    has_gap = any(c["is_award_gap"] and not c["is_reytech"]
                  for c in expiring_contracts)

    # Find the soonest competitor expiry for the countdown label.
    comp = [c for c in expiring_contracts
            if not c["is_reytech"] and not c["is_award_gap"]]
    soonest = min(comp, key=lambda c: c["days_until_expiry"]) if comp else None
    if has_red and soonest:
        return {
            "level": "red",
            "label": f"REBID WINDOW — {soonest['days_until_expiry']}d",
            "hint": ("Prepare rebid memo. Confirm we're on their RFQ "
                     "distribution list for this category."),
        }
    if has_amber and soonest:
        return {
            "level": "amber",
            "label": f"REGISTER NOW — {soonest['days_until_expiry']}d to expiry",
            "hint": ("Register on their RFQ distribution list for this "
                     "category before the successor solicitation posts."),
        }
    if has_renewal:
        renewal = min((c for c in expiring_contracts
                       if c["is_reytech"] and not c["is_award_gap"]),
                      key=lambda c: c["days_until_expiry"])
        return {
            "level": "renewal",
            "label": f"RENEWAL — {renewal['days_until_expiry']}d (we are incumbent)",
            "hint": "Defend this account — confirm renewal process + capability.",
        }
    if has_gap:
        gap = max((c for c in expiring_contracts
                   if c["is_award_gap"] and not c["is_reytech"]),
                  key=lambda c: c["days_until_expiry"])
        return {
            "level": "award_gap",
            "label": f"AWARD GAP — expired {-gap['days_until_expiry']}d ago",
            "hint": "Confirm we received the successor RFQ, or verify award slipped.",
        }
    return {"level": "none", "label": "", "hint": ""}


# Canonical Reytech supplier patterns — matches ingest-side filter in
# scripts/run_scprs_harvest.py. Kept narrow to avoid dba-collision false
# positives (see product-engineer note on canonical-identity). V2-PR-8
# or later: promote to a canonical-supplier-id allowlist.
_REYTECH_SUPPLIER_PATTERNS = ("reytech", "rey tech", "rey-tech")


def _capability_credits_by_dept(conn, dept_category_map, limit_per=2,
                                 age_months=24):
    """Per-agency Reytech wins that credibly anchor an outreach message.

    Sources from scprs_po_lines JOIN scprs_po_master with:
      - supplier LIKE Reytech canonical patterns
      - is_test=0 on both tables
      - start_date within last age_months (default 24mo — credibility
        decays fast in procurement)
      - line_total > 0 and quantity > 0 (per-unit normalization must
        be computable; rows that violate this aren't shippable as
        citations per feedback_scprs_prices)

    Preference order per (prospect_dept, prospect_categories):
      1. same_dept_and_category — peer reference in the same agency
      2. category_only          — same category, different agency
      3. same_dept_only         — we've done business here

    Returns {prospect_dept_code: [credit_dict, ...]} where each dict:
      {po_number, item_description, credit_dept_code, credit_dept_name,
       category, quantity, line_total, per_unit_price, won_at,
       match_type}

    Schema-tolerant: missing tables → empty dict, no crash.
    """
    if not dept_category_map:
        return {}
    from datetime import datetime, timedelta

    try:
        cutoff = (datetime.now() - timedelta(days=int(age_months * 30.5))).date().isoformat()
        # One query for every Reytech win inside the age window across
        # every category any prospect cares about. Python-side grouping
        # applies the preference order.
        categories_set = set()
        for cats in dept_category_map.values():
            categories_set.update(cats or [])
        if not categories_set:
            return {}
        supplier_clause = " OR ".join([
            "LOWER(p.supplier) LIKE ?" for _ in _REYTECH_SUPPLIER_PATTERNS
        ])
        supplier_params = [f"%{pat}%" for pat in _REYTECH_SUPPLIER_PATTERNS]
        cat_placeholders = ",".join(["?"] * len(categories_set))
        rows = conn.execute(f"""
            SELECT p.po_number, p.dept_code AS credit_dept_code,
                   p.dept_name AS credit_dept_name, p.supplier,
                   p.start_date AS won_at,
                   l.description, l.category, l.quantity, l.line_total,
                   l.unit_price
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id = p.id
            WHERE ({supplier_clause})
              AND p.is_test = 0
              AND l.is_test = 0
              AND COALESCE(p.start_date, '') >= ?
              AND COALESCE(l.line_total, 0) > 0
              AND COALESCE(l.quantity, 0) > 0
              AND COALESCE(l.category, '') IN ({cat_placeholders})
            ORDER BY p.start_date DESC
        """, supplier_params + [cutoff] + list(categories_set)).fetchall()
    except Exception as _e:
        log.debug("capability_credits query suppressed: %s", _e)
        return {}

    all_rows = [dict(r) for r in rows]
    out = {}
    for prospect_dc, wanted_cats in dept_category_map.items():
        wanted_set = set(wanted_cats or [])
        if not wanted_set:
            out[prospect_dc] = []
            continue
        buckets = {
            "same_dept_and_category": [],
            "category_only": [],
            "same_dept_only": [],
        }
        for r in all_rows:
            cat = r.get("category") or ""
            same_dept = (r.get("credit_dept_code") or "") == prospect_dc
            cat_match = cat in wanted_set
            if same_dept and cat_match:
                mt = "same_dept_and_category"
            elif cat_match:
                mt = "category_only"
            elif same_dept:
                mt = "same_dept_only"
            else:
                continue
            qty = float(r.get("quantity") or 0) or 1
            line_total = float(r.get("line_total") or 0)
            # Per-unit normalization — ALWAYS divide line_total by qty.
            # SCPRS `unit_price` column is routinely a line total masquerading
            # as a per-unit; never trust it for credibility-sensitive display.
            per_unit = line_total / qty if qty else line_total
            buckets[mt].append({
                "po_number": r.get("po_number"),
                "item_description": r.get("description"),
                "credit_dept_code": r.get("credit_dept_code"),
                "credit_dept_name": r.get("credit_dept_name"),
                "category": cat,
                "quantity": qty,
                "line_total": round(line_total, 2),
                "per_unit_price": round(per_unit, 2),
                "won_at": (r.get("won_at") or "")[:10],
                "match_type": mt,
            })
        # Apply preference order + limit, dedup by po_number.
        seen = set()
        out[prospect_dc] = []
        for mt in ("same_dept_and_category", "category_only",
                   "same_dept_only"):
            for c in buckets[mt]:
                if c["po_number"] in seen:
                    continue
                seen.add(c["po_number"])
                out[prospect_dc].append(c)
                if len(out[prospect_dc]) >= limit_per:
                    break
            if len(out[prospect_dc]) >= limit_per:
                break
    return out


def _log_credits_shown(conn, dept_code, credits):
    """Write one row per (prospect_dept, credit_po) render event.

    Feedback signal for V2-PR-8: which credits correlate with prospects
    that later join the RFQ distribution list? Stateless credit blocks
    can't learn; this is the minimum write-path to enable learning.

    Best-effort only — log errors but don't fail the page render.
    """
    if not credits:
        return
    try:
        from datetime import datetime
        now = datetime.now().isoformat(timespec="seconds")
        conn.executemany(
            "INSERT INTO outreach_credit_shown "
            "(prospect_dept_code, credit_po_number, credit_dept_code, "
            "credit_category, match_type, shown_at) VALUES (?,?,?,?,?,?)",
            [(dept_code, c["po_number"], c.get("credit_dept_code"),
              c.get("category"), c.get("match_type"), now)
             for c in credits],
        )
    except Exception as _e:
        log.debug("credit_shown log suppressed: %s", _e)


def _capability_credits_enabled():
    """Feature flag lookup. Default: ON. Flip OFF via /api/admin/flags
    if the block surfaces a credibility problem in prod."""
    try:
        from src.core.flags import get_flag
        return bool(get_flag("FEATURE_CAPABILITY_CREDITS", True))
    except Exception:
        return True


def _bid_memory_for_depts(conn, dept_codes, limit_per=2):
    """V2-PR-6: most recent bid_memory entries per agency.

    Operator-curated record of every RFQ Reytech received and its
    outcome. Surfaces inline on each card as "Last bid: lost CDCR
    nitrile gloves $8.40 vs Medline $8.12 (2026-01-15) — their
    contract ends Jul 30, rebid window May 30."

    is_test=0 filter. Schema-tolerant.
    """
    if not dept_codes:
        return {}
    try:
        placeholders = ",".join(["?"] * len(dept_codes))
        rows = conn.execute(f"""
            SELECT rfq_id, received_at, dept_code, category,
                   summary_description, our_status, our_bid_amount,
                   our_bid_per_unit, outcome, winning_supplier,
                   winning_price, award_date, contract_end_date, notes
            FROM bid_memory
            WHERE dept_code IN ({placeholders}) AND is_test = 0
            ORDER BY received_at DESC, id DESC
        """, list(dept_codes)).fetchall()
    except Exception as _e:
        log.debug("bid_memory lookup suppressed: %s", _e)
        return {}
    out = {}
    for r in rows:
        d = dict(r)
        out.setdefault(d["dept_code"], [])
        if len(out[d["dept_code"]]) < limit_per:
            out[d["dept_code"]].append(d)
    return out


def _bid_memory_summary(memories):
    """One-line label per memory entry for inline card display.

    Format examples:
      "🔴 LOST CDCR nitrile gloves $8.40 vs Medline $8.12 (2026-01-15)"
      "✅ WON CCHCS first aid kits $58/each (2026-02-12)"
      "📨 RECEIVED CDCR PPE — bid not submitted"
    """
    out = []
    for m in memories or []:
        outcome = (m.get("outcome") or "").lower()
        cat = (m.get("category") or "").replace("_", " ")
        desc = (m.get("summary_description") or cat or "RFQ")[:50]
        award = (m.get("award_date") or m.get("received_at") or "")[:10]
        if outcome == "won":
            label = f"✅ WON {desc}"
            if m.get("our_bid_per_unit"):
                label += f" at ${m['our_bid_per_unit']:.2f}/unit"
            if award:
                label += f" ({award})"
        elif outcome == "lost":
            label = f"🔴 LOST {desc}"
            if m.get("our_bid_per_unit"):
                label += f" — we bid ${m['our_bid_per_unit']:.2f}"
            if m.get("winning_supplier"):
                label += f" vs {m['winning_supplier']}"
            if m.get("winning_price"):
                label += f" at ${m['winning_price']:.2f}"
            if award:
                label += f" ({award})"
        elif outcome == "pending":
            label = f"⏳ PENDING bid on {desc}"
            if award:
                label += f" (received {award})"
        else:
            label = f"📨 {(outcome or 'received').upper()} {desc}"
            if award:
                label += f" ({award})"
        out.append({
            "label": label,
            "outcome": outcome,
            "rfq_id": m.get("rfq_id"),
            "contract_end_date": m.get("contract_end_date") or "",
            "winning_supplier": m.get("winning_supplier") or "",
        })
    return out


def _bid_memory_urgency(memories):
    """Score boost from bid history.

    A recent LOST bid where the winning supplier's contract is about
    to expire = "we know the rebid window opens soon AND we know what
    price to beat" — +10 actionable signal.

    Recently lost (within 6mo) without contract-end visibility → +5
    (we have intel but no specific timing yet).

    Returns max boost across all memories for this dept.
    """
    if not memories:
        return 0
    from datetime import date
    today = date.today()
    boost = 0
    for m in memories:
        outcome = (m.get("outcome") or "").lower()
        if outcome != "lost":
            continue
        end_raw = m.get("contract_end_date") or ""
        end = _parse_end_date(end_raw) if end_raw else None
        if end is not None:
            days_to_end = (end - today).days
            if -30 <= days_to_end <= 120:
                boost = max(boost, 10)
            elif days_to_end > 120:
                boost = max(boost, 5)
        else:
            # No end date but we lost — recency matters.
            recv = _parse_end_date((m.get("award_date") or
                                    m.get("received_at") or "")[:10])
            if recv is not None and (today - recv).days <= 180:
                boost = max(boost, 5)
    return boost


def _cert_status_summary(conn):
    """V2-PR-4: aggregate Reytech certification health across all certs.

    Returns {certs: [...], summary: {level, label, hint, expiring_soon,
    expired, total}}.

    Levels:
      critical — any active cert past expiry → set-aside eligibility
                 lost silently on every affected bid
      warn     — any active cert ≤ 60d to expiry → renew now or risk
                 silent lapse
      ok       — all active certs >60d to expiry
      none     — no active certs in DB (operator hasn't populated)

    Schema-tolerant: missing table → none/empty, no crash.
    """
    from datetime import date
    out_certs = []
    try:
        rows = conn.execute("""
            SELECT cert_type, cert_number, issue_date, expires_at,
                   renewal_url, notes, is_active
            FROM reytech_certifications
            WHERE is_active = 1 AND is_test = 0
            ORDER BY cert_type
        """).fetchall()
    except Exception as _e:
        log.debug("cert_status_summary suppressed: %s", _e)
        return {"certs": [], "summary": {"level": "none", "label": "",
                "hint": "", "expiring_soon": 0, "expired": 0, "total": 0}}

    today = date.today()
    expired_count = 0
    soon_count = 0
    soonest_days = None
    soonest_cert = None
    for r in rows:
        d = dict(r)
        end = _parse_end_date(d.get("expires_at") or "")
        if end is not None:
            days = (end - today).days
            d["days_until_expiry"] = days
            d["is_expired"] = days < 0
            if days < 0:
                expired_count += 1
                if soonest_days is None or days < soonest_days:
                    soonest_days = days
                    soonest_cert = d
            elif days <= 60:
                soon_count += 1
                if soonest_days is None or days < soonest_days:
                    soonest_days = days
                    soonest_cert = d
        else:
            d["days_until_expiry"] = None
            d["is_expired"] = False
        out_certs.append(d)

    if not out_certs:
        return {"certs": [], "summary": {"level": "none",
                "label": "No certifications on file",
                "hint": "Add SB / MB / DVBE / OSDS cert details "
                        "to maintain set-aside eligibility.",
                "expiring_soon": 0, "expired": 0, "total": 0}}

    if expired_count > 0 and soonest_cert and soonest_cert.get("is_expired"):
        return {"certs": out_certs, "summary": {
            "level": "critical",
            "label": f"⚠ {soonest_cert['cert_type']} EXPIRED "
                     f"{-soonest_cert['days_until_expiry']}d ago",
            "hint": "Set-aside eligibility lost on bids requiring "
                    "this cert — re-register at portal urgently.",
            "expiring_soon": soon_count, "expired": expired_count,
            "total": len(out_certs),
        }}
    if soon_count > 0 and soonest_cert:
        return {"certs": out_certs, "summary": {
            "level": "warn",
            "label": f"⏳ {soonest_cert['cert_type']} expires in "
                     f"{soonest_cert['days_until_expiry']}d",
            "hint": "Renew before expiry to avoid silent loss of "
                    "set-aside eligibility.",
            "expiring_soon": soon_count, "expired": expired_count,
            "total": len(out_certs),
        }}
    return {"certs": out_certs, "summary": {
        "level": "ok",
        "label": f"All {len(out_certs)} certifications current",
        "hint": "",
        "expiring_soon": 0, "expired": 0, "total": len(out_certs),
    }}


def _registration_status_for_depts(conn, dept_codes):
    """Batched lookup against agency_vendor_registry.

    Returns {dept_code: record_dict} for every dept_code that has a row.
    Missing rows mean "unknown" — caller treats that as its own state.

    Schema-tolerant: if agency_vendor_registry doesn't exist (fresh DB
    before migration 24 fires), returns empty dict without raising.
    """
    if not dept_codes:
        return {}
    try:
        placeholders = ",".join(["?"] * len(dept_codes))
        rows = conn.execute(f"""
            SELECT dept_code, status, confirmed_at, expires_at, portal_url,
                   procurement_officer_name, procurement_officer_email,
                   procurement_officer_phone, vendor_id_at_agency,
                   categories_json, notes, source, updated_by,
                   created_at, updated_at
            FROM agency_vendor_registry
            WHERE dept_code IN ({placeholders}) AND is_test = 0
        """, list(dept_codes)).fetchall()
    except Exception as _e:
        log.debug("registry lookup suppressed: %s", _e)
        return {}
    return {r["dept_code"]: dict(r) for r in rows}


def _registration_summary(record):
    """Classify a registry record into a UI pill + action hint.

    Returns {level, label, hint, action_url, status_effective, expires_at}.

    Rule: `expires_at` trumps `status`. A row with status='registered' but
    expires_at in the past is effectively EXPIRED — never rely on the
    stored status alone. Catches the "operator forgot to flip status"
    class of silent failure flagged in product-engineer review.
    """
    if not record:
        return {
            "level": "unknown", "label": "UNKNOWN",
            "hint": "Verify registration status in agency portal.",
            "action_url": "", "status_effective": "unknown",
            "expires_at": "",
        }
    stored_status = (record.get("status") or "unknown").lower()
    expires_raw = record.get("expires_at") or ""
    portal = record.get("portal_url") or ""
    expires_date = _parse_end_date(expires_raw) if expires_raw else None

    # Expiry-overrides-stored-status logic.
    from datetime import date
    status = stored_status
    if expires_date is not None and expires_date < date.today():
        # Any row past expiry is effectively EXPIRED regardless of
        # stored status.
        status = "expired"

    if status == "registered":
        days_left = (expires_date - date.today()).days if expires_date else None
        if days_left is not None and days_left <= 60:
            # Registered but expiring soon — surface as a registered-but-renew hint.
            return {
                "level": "registered", "label": f"REGISTERED (renew in {days_left}d)",
                "hint": "Renewal due soon — confirm and re-up before it lapses.",
                "action_url": portal, "status_effective": "registered",
                "expires_at": expires_raw[:10] if expires_raw else "",
            }
        confirmed = (record.get("confirmed_at") or "")[:10]
        label = "REGISTERED"
        if confirmed:
            label = f"REGISTERED (confirmed {confirmed})"
        return {
            "level": "registered", "label": label,
            "hint": "On distribution list.", "action_url": portal,
            "status_effective": "registered",
            "expires_at": expires_raw[:10] if expires_raw else "",
        }
    if status == "expired":
        return {
            "level": "expired",
            "label": f"EXPIRED{(' ' + expires_raw[:10]) if expires_raw else ''}",
            "hint": "Re-register at agency portal to resume RFQ distribution.",
            "action_url": portal, "status_effective": "expired",
            "expires_at": expires_raw[:10] if expires_raw else "",
        }
    if status == "pending":
        return {
            "level": "pending", "label": "PENDING",
            "hint": "Request submitted — chase procurement officer for confirmation.",
            "action_url": portal, "status_effective": "pending",
            "expires_at": expires_raw[:10] if expires_raw else "",
        }
    if status == "not_registered":
        return {
            "level": "not_registered", "label": "NOT REGISTERED",
            "hint": "Register now to start receiving RFQs for this agency.",
            "action_url": portal, "status_effective": "not_registered",
            "expires_at": expires_raw[:10] if expires_raw else "",
        }
    return {
        "level": "unknown", "label": "UNKNOWN",
        "hint": "Verify registration status in agency portal.",
        "action_url": portal, "status_effective": "unknown",
        "expires_at": expires_raw[:10] if expires_raw else "",
    }


def _registration_urgency(summary):
    """Points to add to urgency_score from registration state.

    Weights (per 2026-04-24 product-engineer review):

      not_registered → +35 (absolute gate — rebid without registration is
                            useless; must outrank hottest rebid band)
      expired        → +35 (same — lapsed cert = same gate)
      pending        → +5  (in progress but chase)
      unknown        → +5  (probably fine; nudge operator to verify, but
                            avoid noise-flood since most rows start unknown)
      registered     → 0   (healthy)
    """
    level = (summary or {}).get("status_effective") or "unknown"
    if level in ("not_registered", "expired"):
        return 35
    if level == "pending":
        return 5
    if level == "unknown":
        return 5
    return 0


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

    # Pre-compute dept_codes for batched rebid-window query.
    dept_codes = [p.get("dept_code") for p in prospects[:limit]
                   if p.get("dept_code")]

    with _get_db() as conn:
        outbox_map = _existing_drafts_for_prospects(conn, all_emails)
        response_map = _response_history_for_emails(conn, all_emails)
        # Batched single-query lookup for expiring contracts across all
        # visible prospects (per 2026-04-24 product-engineer review).
        expiring_map = _expiring_contracts_by_dept(conn, dept_codes)
        # V2-PR-2: same-shape batched lookup for registration status.
        registry_map = _registration_status_for_depts(conn, dept_codes)
        # V2-PR-6: bid_memory batched lookup. Per-card "Last bid:" line
        # + urgency boost when the winner's contract is in the rebid
        # window (we know exactly what to beat AND when).
        bid_memory_map = _bid_memory_for_depts(conn, dept_codes)

        # V2-PR-3: capability credits. Batched — one SQL across all
        # prospects' categories → Reytech won quotes filtered by
        # supplier + age + is_test + per-unit-computable constraints.
        # Flag-gated for quick rollback if credibility issue surfaces.
        credits_map = {}
        if _capability_credits_enabled():
            dept_category_map = {}
            for p in prospects[:limit]:
                dc = p.get("dept_code") or ""
                # Top 3 categories for this prospect by line_total.
                try:
                    cat_rows = conn.execute("""
                        SELECT l.category, SUM(l.line_total) AS total
                        FROM scprs_po_lines l
                        JOIN scprs_po_master pm ON l.po_id = pm.id
                        WHERE pm.dept_code = ?
                          AND pm.is_test = 0
                          AND l.is_test = 0
                          AND COALESCE(l.category, '') != ''
                          AND l.line_total > 0
                        GROUP BY l.category
                        ORDER BY total DESC LIMIT 5
                    """, (dc,)).fetchall()
                    dept_category_map[dc] = [r["category"] for r in cat_rows]
                except Exception as _e:
                    log.debug("credit-categories suppressed for %s: %s", dc, _e)
                    dept_category_map[dc] = []
            credits_map = _capability_credits_by_dept(conn, dept_category_map)
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

            # V2-PR-1: rebid-window surveillance + urgency boost.
            expiring = expiring_map.get(dept_code, []) or []
            urgency = _rebid_urgency(expiring)
            rebid = _rebid_summary(expiring)
            base_score = p.get("score") or 0

            # V2-PR-2: registration status + urgency boost. Not_registered
            # / expired outrank any rebid-window band because a rebid we
            # can't receive is useless.
            reg_record = registry_map.get(dept_code)
            reg_summary = _registration_summary(reg_record)
            reg_urgency = _registration_urgency(reg_summary)

            # V2-PR-6: bid memory (operator-curated history of received
            # RFQs + outcomes). Both display + urgency boost.
            bid_memories_raw = bid_memory_map.get(dept_code, []) or []
            bid_memory_lines = _bid_memory_summary(bid_memories_raw)
            bid_urgency = _bid_memory_urgency(bid_memories_raw)

            # V2-PR-3: capability credits (credibility-only, no score boost).
            capability_credits = credits_map.get(dept_code, []) or []
            # Log render event — enables V2-PR-8 to learn which credits
            # correlate with actual registration / RFQ outcomes.
            if capability_credits:
                _log_credits_shown(conn, dept_code, capability_credits)
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
                "expiring_contracts": expiring,
                "rebid_urgency": urgency,
                "rebid_summary": rebid,
                "registration_record": reg_record or {},
                "registration_summary": reg_summary,
                "registration_urgency": reg_urgency,
                "capability_credits": capability_credits,
                "bid_memory_lines": bid_memory_lines,
                "bid_memory_urgency": bid_urgency,
                "urgency_score": base_score + urgency + reg_urgency + bid_urgency,
            })

    # V2-PR-8: per-card template auto-pick + which other templates are
    # renderable (UI dropdown enable/disable). Done in second pass so
    # we have the full card context. Schema-tolerant — failure here
    # leaves cards.template_pick = None.
    try:
        from src.agents.outreach_templates import (
            pick_template, template_is_renderable, TEMPLATES,
        )
        with _get_db() as conn:
            for c in cards:
                try:
                    pick = pick_template(c, conn)
                    c["template_pick"] = pick  # {template_key, template_name, reason}
                    c["template_options"] = [
                        {"key": k, "name": v["name"],
                         "renderable": template_is_renderable(k, c, conn)}
                        for k, v in TEMPLATES.items()
                    ]
                    log.debug("V2-PR-8 template pick for %s: %s",
                              c.get("dept_code"), pick.get("template_key"))
                except Exception as _e:
                    log.warning("template pick failed for %s: %s",
                                c.get("dept_code"), _e)
                    c["template_pick"] = None
                    c["template_options"] = []
    except Exception as _e:
        log.warning("template module import suppressed: %s", _e)

    # Re-sort by urgency_score (raw SCPRS score + rebid-window boost).
    # V2-PR-2..7 will each stack more boosts additively onto this field.
    # Keep the raw `score` untouched for transparency in the UI.
    cards.sort(key=lambda c: c.get("urgency_score") or 0, reverse=True)
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
    cert_status = {"certs": [], "summary": {"level": "none", "label": "",
                                             "hint": ""}}
    try:
        cards, err, summary = _build_card_list(limit=8)
    except Exception as e:
        log.exception("page_outreach_next: card build failed")
        err = f"{type(e).__name__}: {e}"

    # V2-PR-4: cert-expiry summary (global, top-of-page banner). Always
    # show — operators need the warning even if other queries fail.
    try:
        with _get_db() as conn:
            cert_status = _cert_status_summary(conn)
    except Exception as e:
        log.exception("page_outreach_next: cert_status load failed")

    # V2-PR-7: lightweight gap counter for top-of-page audit banner.
    # Cheap query — count, not list.
    gap_count = 0
    provisional_count = 0
    try:
        with _get_db() as conn:
            try:
                gap_count = conn.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT p.dept_code
                        FROM scprs_po_master p
                        JOIN scprs_po_lines l ON l.po_id = p.id
                        WHERE p.is_test = 0 AND l.is_test = 0
                          AND p.start_date >= date('now', '-365 days')
                        GROUP BY p.dept_code
                        ORDER BY SUM(l.line_total) DESC LIMIT 30
                    ) top
                    LEFT JOIN agency_vendor_registry r ON r.dept_code = top.dept_code
                    WHERE r.dept_code IS NULL
                       OR r.status IS NULL
                       OR r.status = ''
                       OR r.status = 'unknown'
                       OR (r.status = 'registered' AND r.is_provisional = 1)
                """).fetchone()[0]
            except Exception as _e:
                log.debug("gap count suppressed: %s", _e)
            try:
                provisional_count = conn.execute(
                    "SELECT COUNT(*) FROM agency_vendor_registry "
                    "WHERE source='agent' AND is_provisional=1 AND is_test=0"
                ).fetchone()[0]
            except Exception as _e:
                log.debug("provisional count suppressed: %s", _e)
    except Exception as e:
        log.exception("page_outreach_next: gap counts load failed")

    return render_page(
        "outreach_next.html",
        active_page="Growth",
        cards=cards or [],
        summary=summary or {},
        error=err,
        cert_status=cert_status,
        registration_gap_count=gap_count,
        registration_provisional_count=provisional_count,
    )


@bp.route("/api/outreach/next/bid", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_bid():
    """Upsert one bid_memory record by rfq_id.

    Body: {rfq_id, dept_code, dept_name?, category?, summary_description?,
           our_status? ('received'|'bid'|'no_bid'|'declined'),
           our_bid_amount?, our_bid_per_unit?,
           outcome? ('won'|'lost'|'pending'|'unknown'),
           winning_supplier?, winning_price?, award_date?,
           contract_end_date?, notes?}
    """
    try:
        from datetime import datetime
        payload = request.get_json(force=True, silent=True) or {}
        rfq_id = (payload.get("rfq_id") or "").strip()
        dept_code = (payload.get("dept_code") or "").strip()
        if not rfq_id:
            return jsonify({"ok": False, "error": "rfq_id required"}), 400
        if not dept_code:
            return jsonify({"ok": False, "error": "dept_code required"}), 400

        outcome = (payload.get("outcome") or "pending").lower()
        if outcome not in {"won", "lost", "pending", "unknown"}:
            return jsonify({
                "ok": False,
                "error": "outcome must be won|lost|pending|unknown"
            }), 400
        our_status = (payload.get("our_status") or "received").lower()
        if our_status not in {"received", "bid", "no_bid", "declined"}:
            return jsonify({
                "ok": False,
                "error": "our_status must be received|bid|no_bid|declined"
            }), 400

        for dt_field in ("received_at", "award_date", "contract_end_date"):
            val = payload.get(dt_field)
            if val and _parse_end_date(val) is None:
                return jsonify({
                    "ok": False,
                    "error": f"{dt_field} must be ISO date (YYYY-MM-DD)"
                }), 400

        now_iso = datetime.now().isoformat(timespec="seconds")
        with _get_db() as conn:
            existing = conn.execute(
                "SELECT id FROM bid_memory WHERE rfq_id = ?", (rfq_id,)
            ).fetchone()
            fields = {
                "received_at": payload.get("received_at") or "",
                "dept_code": dept_code,
                "dept_name": payload.get("dept_name") or "",
                "category": payload.get("category") or "",
                "summary_description": payload.get("summary_description") or "",
                "our_status": our_status,
                "our_bid_amount": float(payload.get("our_bid_amount") or 0),
                "our_bid_per_unit": float(payload.get("our_bid_per_unit") or 0),
                "outcome": outcome,
                "winning_supplier": payload.get("winning_supplier") or "",
                "winning_price": float(payload.get("winning_price") or 0),
                "award_date": payload.get("award_date") or "",
                "contract_end_date": payload.get("contract_end_date") or "",
                "notes": payload.get("notes") or "",
                "source": payload.get("source") or "operator",
                "updated_by": payload.get("updated_by") or "",
                "updated_at": now_iso,
            }
            if existing:
                set_clause = ", ".join(f"{k}=?" for k in fields)
                conn.execute(
                    f"UPDATE bid_memory SET {set_clause} WHERE rfq_id = ?",
                    list(fields.values()) + [rfq_id],
                )
            else:
                fields["rfq_id"] = rfq_id
                fields["created_at"] = now_iso
                cols = ", ".join(fields.keys())
                placeholders = ", ".join(["?"] * len(fields))
                conn.execute(
                    f"INSERT INTO bid_memory ({cols}) VALUES ({placeholders})",
                    list(fields.values()),
                )
            row = conn.execute(
                "SELECT * FROM bid_memory WHERE rfq_id = ?", (rfq_id,)
            ).fetchone()
            record = dict(row) if row else {}
        return jsonify({"ok": True, "record": record})
    except Exception as e:
        log.exception("api_outreach_next_bid failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/outreach/next/cert", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_cert():
    """Upsert one Reytech certification record by cert_type.

    Body: {cert_type, cert_number?, issue_date?, expires_at?,
           renewal_url?, notes?, is_active?}
    Validates: cert_type required, dates ISO if provided.
    """
    try:
        from datetime import datetime
        payload = request.get_json(force=True, silent=True) or {}
        cert_type = (payload.get("cert_type") or "").strip().upper()
        if not cert_type:
            return jsonify({"ok": False, "error": "cert_type required"}), 400
        for dt_field in ("issue_date", "expires_at"):
            val = payload.get(dt_field)
            if val and _parse_end_date(val) is None:
                return jsonify({
                    "ok": False,
                    "error": f"{dt_field} must be ISO date (YYYY-MM-DD)"
                }), 400

        now_iso = datetime.now().isoformat(timespec="seconds")
        is_active = 1 if payload.get("is_active", True) else 0
        with _get_db() as conn:
            existing = conn.execute(
                "SELECT id FROM reytech_certifications WHERE cert_type = ?",
                (cert_type,)
            ).fetchone()
            fields = {
                "cert_number": payload.get("cert_number") or "",
                "issue_date": payload.get("issue_date") or "",
                "expires_at": payload.get("expires_at") or "",
                "renewal_url": payload.get("renewal_url") or "",
                "notes": payload.get("notes") or "",
                "is_active": is_active,
                "updated_at": now_iso,
            }
            if existing:
                set_clause = ", ".join(f"{k}=?" for k in fields)
                conn.execute(
                    f"UPDATE reytech_certifications SET {set_clause} "
                    "WHERE cert_type = ?",
                    list(fields.values()) + [cert_type],
                )
            else:
                fields["cert_type"] = cert_type
                fields["created_at"] = now_iso
                cols = ", ".join(fields.keys())
                placeholders = ", ".join(["?"] * len(fields))
                conn.execute(
                    f"INSERT INTO reytech_certifications ({cols}) "
                    f"VALUES ({placeholders})",
                    list(fields.values()),
                )
            row = conn.execute(
                "SELECT * FROM reytech_certifications WHERE cert_type = ?",
                (cert_type,)
            ).fetchone()
            record = dict(row) if row else {}
            summary = _cert_status_summary(conn)
        return jsonify({"ok": True, "record": record,
                        "cert_status": summary})
    except Exception as e:
        log.exception("api_outreach_next_cert failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/outreach/next/registry", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_registry():
    """Upsert a registration-status record for one dept_code.

    Narrow inline-edit path for the operator (per 2026-04-24 product-eng
    review — Option A, narrowed to status + portal_url + notes only).
    Body: {dept_code, status, portal_url?, notes?, confirmed_at?,
           expires_at?, vendor_id_at_agency?, categories_json?}
    """
    try:
        from datetime import datetime
        payload = request.get_json(force=True, silent=True) or {}
        dept_code = (payload.get("dept_code") or "").strip()
        status = (payload.get("status") or "").strip().lower()
        if not dept_code:
            return jsonify({"ok": False, "error": "dept_code required"}), 400
        allowed_statuses = {"registered", "not_registered", "pending",
                            "expired", "unknown"}
        if status not in allowed_statuses:
            return jsonify({
                "ok": False,
                "error": f"status must be one of {sorted(allowed_statuses)}"
            }), 400

        # Validate expires_at / confirmed_at if provided — must be ISO date.
        for dt_field in ("confirmed_at", "expires_at"):
            val = payload.get(dt_field)
            if val and _parse_end_date(val) is None:
                return jsonify({
                    "ok": False,
                    "error": f"{dt_field} must be ISO date (YYYY-MM-DD)"
                }), 400

        now_iso = datetime.now().isoformat(timespec="seconds")
        with _get_db() as conn:
            existing = conn.execute(
                "SELECT dept_code FROM agency_vendor_registry WHERE dept_code = ?",
                (dept_code,)
            ).fetchone()
            fields = {
                "status": status,
                "confirmed_at": payload.get("confirmed_at") or "",
                "expires_at": payload.get("expires_at") or "",
                "portal_url": payload.get("portal_url") or "",
                "notes": payload.get("notes") or "",
                "vendor_id_at_agency": payload.get("vendor_id_at_agency") or "",
                "categories_json": payload.get("categories_json") or "[]",
                "source": payload.get("source") or "operator",
                "updated_by": payload.get("updated_by") or "",
                "updated_at": now_iso,
            }
            if existing:
                set_clause = ", ".join(f"{k}=?" for k in fields)
                conn.execute(
                    f"UPDATE agency_vendor_registry SET {set_clause} "
                    "WHERE dept_code = ?",
                    list(fields.values()) + [dept_code],
                )
            else:
                fields["dept_code"] = dept_code
                fields["created_at"] = now_iso
                cols = ", ".join(fields.keys())
                placeholders = ", ".join(["?"] * len(fields))
                conn.execute(
                    f"INSERT INTO agency_vendor_registry ({cols}) "
                    f"VALUES ({placeholders})",
                    list(fields.values()),
                )
            # Re-fetch so the caller gets the canonical persisted shape.
            row = conn.execute(
                "SELECT * FROM agency_vendor_registry WHERE dept_code = ?",
                (dept_code,)
            ).fetchone()
            record = dict(row) if row else {}
        summary = _registration_summary(record)
        return jsonify({"ok": True, "record": record, "summary": summary})
    except Exception as e:
        log.exception("api_outreach_next_registry failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/outreach/next/draft-v2", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_draft_v2():
    """V2-PR-8: render a canned procurement-template draft for one card.

    Body: {dept_code, template_key?}
      - template_key omitted → auto-pick the highest-priority template
      - template_key provided → render that specific template (operator override)

    NEVER returns placeholder copy. If a template's required_vars are
    missing, returns ok=false + missing_vars so the UI can disable the
    option instead of shipping junk to a procurement officer.
    """
    try:
        from src.agents.outreach_templates import (
            pick_template, render_template, template_is_renderable, TEMPLATES,
        )
        payload = request.get_json(force=True, silent=True) or {}
        dept_code = (payload.get("dept_code") or "").strip()
        template_key = (payload.get("template_key") or "").strip() or None
        if not dept_code:
            return jsonify({"ok": False, "error": "dept_code required"}), 400

        cards, err, _summary = _build_card_list(limit=50)
        if err:
            return jsonify({"ok": False, "error": err}), 500
        card = next((c for c in (cards or [])
                     if (c.get("dept_code") or "") == dept_code), None)
        if card is None:
            return jsonify({
                "ok": False,
                "error": f"dept_code '{dept_code}' not in current top "
                         "prospect set"
            }), 404

        with _get_db() as conn:
            if template_key is None:
                pick = pick_template(card, conn)
                template_key = pick["template_key"]
                pick_reason = pick["reason"]
                if template_key is None:
                    return jsonify({
                        "ok": False,
                        "template_key": None,
                        "reason": pick_reason,
                        "available_templates": [
                            {"key": k, "name": v["name"],
                             "renderable": template_is_renderable(k, card, conn)}
                            for k, v in TEMPLATES.items()
                        ],
                    })
                rendered = render_template(template_key, card, conn)
                rendered["pick_reason"] = pick_reason
                rendered["auto_picked"] = True
            else:
                if template_key not in TEMPLATES:
                    return jsonify({
                        "ok": False,
                        "error": f"unknown template_key '{template_key}'"
                    }), 400
                rendered = render_template(template_key, card, conn)
                rendered["auto_picked"] = False
        return jsonify(rendered)
    except Exception as e:
        log.exception("api_outreach_next_draft_v2 failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/outreach/next/save-draft", methods=["POST"])
@auth_required
@safe_route
def api_outreach_next_save_draft():
    """Persist a rendered draft to email_outbox status='draft' (matches
    existing card-surfaced vocabulary; do NOT invent 'pending_approval').
    Body: {dept_code, template_key, subject, body, recipient_email}"""
    try:
        import json as _json
        import uuid
        from datetime import datetime as _dt
        payload = request.get_json(force=True, silent=True) or {}
        for required in ("dept_code", "template_key", "subject",
                         "body", "recipient_email"):
            if not (payload.get(required) or "").strip():
                return jsonify({
                    "ok": False, "error": f"{required} required"
                }), 400
        outbox_id = "out_" + uuid.uuid4().hex[:12]
        now_iso = _dt.now().isoformat(timespec="seconds")
        with _get_db() as conn:
            try:
                conn.execute(
                    "INSERT INTO email_outbox (id, created_at, status, "
                    "type, to_address, subject, body, intent, metadata) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        outbox_id, now_iso, "draft", "outreach",
                        payload["recipient_email"],
                        payload["subject"], payload["body"],
                        f"outreach:{payload['template_key']}",
                        _json.dumps({
                            "dept_code": payload["dept_code"],
                            "template_key": payload["template_key"],
                            "rendered_at": now_iso,
                        }),
                    ),
                )
            except Exception as _e:
                log.debug("save-draft fallback: %s", _e)
                conn.execute(
                    "INSERT INTO email_outbox (id, created_at, status, "
                    "recipient, subject, body) VALUES (?,?,?,?,?,?)",
                    (
                        outbox_id, now_iso, "draft",
                        payload["recipient_email"],
                        payload["subject"], payload["body"],
                    ),
                )
        return jsonify({"ok": True, "outbox_id": outbox_id})
    except Exception as e:
        log.exception("api_outreach_next_save_draft failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


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


# ════════════════════════════════════════════════════════════════════════════
# V2-PR-7: Registration-Gap Detector + Gmail Bulk-Seed
# ════════════════════════════════════════════════════════════════════════════

@bp.route("/admin/registration-gaps")
@auth_required
@safe_page
def page_registration_gaps():
    """Operator audit + bulk-action UI for V2-PR-7.

    Shows two sections:
      1. Detector punch list — top-N agencies by spend with no registry
         row (or status=unknown).
      2. Agent-seeded provisional rows awaiting operator confirm/reject.
    """
    gaps_result = {"ok": True, "gaps": []}
    provisional = []
    pending_aliases = []
    err = None
    try:
        from src.agents.registration_gap_detector import detect_registration_gaps
        gaps_result = detect_registration_gaps(top_n=30)
        with _get_db() as conn:
            try:
                rows = conn.execute("""
                    SELECT dept_code, status, source, is_provisional,
                           confirmed_at, expires_at, notes,
                           evidence_message_ids, updated_by, updated_at
                    FROM agency_vendor_registry
                    WHERE source = 'agent' AND is_provisional = 1
                      AND is_test = 0
                    ORDER BY updated_at DESC LIMIT 50
                """).fetchall()
                provisional = [dict(r) for r in rows]
            except Exception as _e:
                log.debug("provisional query suppressed: %s", _e)
            try:
                rows = conn.execute("""
                    SELECT domain, seen_count, first_seen, last_seen,
                           example_subject FROM agency_pending_aliases
                    ORDER BY seen_count DESC, last_seen DESC LIMIT 50
                """).fetchall()
                pending_aliases = [dict(r) for r in rows]
            except Exception as _e:
                log.debug("pending aliases query suppressed: %s", _e)
    except Exception as e:
        log.exception("page_registration_gaps failed")
        err = f"{type(e).__name__}: {e}"

    return render_page(
        "registration_gaps.html",
        active_page="Growth",
        gaps=gaps_result.get("gaps") or [],
        provisional=provisional,
        pending_aliases=pending_aliases,
        scanned_top_n=gaps_result.get("scanned_top_n", 0),
        error=err,
    )


@bp.route("/api/admin/registration-gaps/detect", methods=["POST"])
@auth_required
@safe_route
def api_registration_gaps_detect():
    """Run detector synchronously, return JSON gap list."""
    try:
        from src.agents.registration_gap_detector import detect_registration_gaps
        payload = request.get_json(force=True, silent=True) or {}
        top_n = int(payload.get("top_n", 30))
        return jsonify(detect_registration_gaps(top_n=top_n))
    except Exception as e:
        log.exception("api_registration_gaps_detect failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/admin/registration-gaps/gmail-seed", methods=["POST"])
@auth_required
@safe_route
def api_registration_gaps_gmail_seed():
    """Bulk-seed agency_vendor_registry from Gmail RFQ archive.

    Body: {dry_run?, limit?, since_days?, inbox_name?}.
    dry_run defaults TRUE — operator must explicitly send dry_run=false
    to actually write to the registry.
    """
    try:
        from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
        payload = request.get_json(force=True, silent=True) or {}
        dry_run = bool(payload.get("dry_run", True))
        limit = int(payload.get("limit", 200))
        since_days = int(payload.get("since_days", 540))
        inbox_name = (payload.get("inbox_name") or "sales").strip()
        return jsonify(gmail_bulk_seed_registrations(
            dry_run=dry_run, limit=limit, since_days=since_days,
            inbox_name=inbox_name,
        ))
    except Exception as e:
        log.exception("api_registration_gaps_gmail_seed failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/admin/registration-gaps/confirm", methods=["POST"])
@auth_required
@safe_route
def api_registration_gaps_confirm():
    """Operator confirms an agent-seeded row → source='operator'."""
    try:
        from src.agents.registration_gap_detector import confirm_agent_registration
        payload = request.get_json(force=True, silent=True) or {}
        dept_code = (payload.get("dept_code") or "").strip()
        if not dept_code:
            return jsonify({"ok": False, "error": "dept_code required"}), 400
        return jsonify(confirm_agent_registration(dept_code))
    except Exception as e:
        log.exception("api_registration_gaps_confirm failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@bp.route("/api/admin/registration-gaps/reject", methods=["POST"])
@auth_required
@safe_route
def api_registration_gaps_reject():
    """Operator rejects an agent-seeded row → status='not_registered',
    source='operator'."""
    try:
        from src.agents.registration_gap_detector import reject_agent_registration
        payload = request.get_json(force=True, silent=True) or {}
        dept_code = (payload.get("dept_code") or "").strip()
        if not dept_code:
            return jsonify({"ok": False, "error": "dept_code required"}), 400
        return jsonify(reject_agent_registration(dept_code))
    except Exception as e:
        log.exception("api_registration_gaps_reject failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500
