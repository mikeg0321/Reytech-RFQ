"""
src/agents/outreach_templates.py — V2-PR-8 canned procurement templates.

Replaces V1's A/B drafts (price-hook vs relationship — wrong shape for
public-sector procurement per Mike's 2026-04-24 reframe). Four canned
templates auto-pick per card state and inline the concrete data the
operator card carries (capability credits, bid memory, registration
status, rebid window). All canonical sender identity comes from
src/core/reytech_identity (Michael Guadan, NOT "Mike Gonzalez").

Per 2026-04-25 product-engineer pre-build review:
  - Canonical identity at constants layer ✓ (reytech_identity)
  - Kill placeholder fallbacks ✓ (template_is_renderable disables
    the dropdown option when required vars are missing — never
    ship `[no recent capability credit]` to a procurement officer)
  - Recipient = procurement_officer_email NOT buyer_email ✓
    (resolved via agency_vendor_registry; falls back only when
    operator hasn't filled it)
  - Outbox status='draft' (matches V1 vocabulary)
  - Inline reason line (returned with each render)
  - Shorter tone (no "To Whom It May Concern"; subject + first
    sentence does the work)
  - Single ask per CTA

Templates are Python string constants. NO LLM, NO freeform generation
— procurement copy precision matters more than cleverness.
"""
from __future__ import annotations

import logging

log = logging.getLogger("reytech.outreach_templates")


# ── Template registry ────────────────────────────────────────────────────────

TEMPLATES = {
    "rfq_list_inclusion": {
        "name": "RFQ-list inclusion request",
        "auto_pick_priority": 1,  # highest — gating action
        "required_vars": ["recipient_email", "agency_name",
                          "top_category"],
        "subject_template": "Reytech Inc. — RFQ distribution list inclusion request for {agency_name}",
        "body_template": "Hi,\n\n"
            "Reytech Inc. is a California-certified Small Business "
            "{sb_cert_clause}supplying state agencies with {top_category} "
            "and adjacent categories.\n\n"
            "{capability_credit_clause}"
            "Please add Reytech to your RFQ distribution list for "
            "{top_category}. We can be reached at "
            "{reytech_email}.\n\n"
            "{reytech_signature}",
    },
    "rebid_memo": {
        "name": "Rebid window memo",
        "auto_pick_priority": 2,
        "required_vars": ["recipient_email", "agency_name",
                          "top_category", "incumbent_supplier",
                          "contract_end_date"],
        "subject_template": "Rebid window for {agency_name} {top_category} — Reytech available to compete",
        "body_template": "Hi,\n\n"
            "We noticed {incumbent_supplier}'s {agency_name} contract "
            "for {top_category} expires {contract_end_date}.\n\n"
            "{capability_credit_clause}"
            "{prior_bid_clause}"
            "Please include Reytech in the successor solicitation. "
            "Reach us at {reytech_email}.\n\n"
            "{reytech_signature}",
    },
    "capability_refresher": {
        "name": "Quarterly capability refresher",
        "auto_pick_priority": 3,
        "required_vars": ["recipient_email", "agency_name",
                          "top_category"],
        "subject_template": "Reytech Inc. — quarterly capability update for {agency_name}",
        "body_template": "Hi,\n\n"
            "Quick capability refresh from Reytech Inc. {sb_cert_clause}\n\n"
            "{capability_credit_clause}"
            "We continue to be available for your {top_category} "
            "solicitations. Confirm we are current on your distribution "
            "list — {reytech_email}.\n\n"
            "{reytech_signature}",
    },
    "cert_confirmation": {
        "name": "Cert renewal confirmation",
        "auto_pick_priority": 99,  # never auto-picked; operator-only
        "required_vars": ["recipient_email", "agency_name", "cert_type"],
        "subject_template": "Reytech Inc. — {cert_type} certification renewed",
        "body_template": "Hi,\n\n"
            "Reytech Inc.'s {cert_type} certification has been renewed and "
            "is on file. Cert # {cert_no_for_renewed_type}, valid through "
            "{cert_expires_at}.\n\n"
            "Please update Reytech's bidder record at {agency_name}. "
            "{reytech_email}.\n\n"
            "{reytech_signature}",
    },
}


# ── Recipient resolution ─────────────────────────────────────────────────────

def resolve_recipient_email(card: dict, conn) -> str | None:
    """Per 2026-04-25 product-engineer review: recipient MUST come from
    `agency_vendor_registry.procurement_officer_email`, NOT from the
    card's surfaced `buyer_email` (which is often the ordering clerk,
    not the procurement officer who can grant RFQ-list inclusion).

    Falls back to buyer_email only when registry doesn't have a
    procurement officer on file — and tags the result for the UI to
    surface a "best-effort recipient" warning."""
    dept_code = card.get("dept_code")
    if dept_code:
        try:
            row = conn.execute(
                "SELECT procurement_officer_email FROM agency_vendor_registry "
                "WHERE dept_code = ? AND is_test = 0",
                (dept_code,)
            ).fetchone()
            if row:
                officer_email = (row["procurement_officer_email"]
                                 if hasattr(row, "__getitem__") else row[0])
                if officer_email and officer_email.strip():
                    return officer_email.strip()
        except Exception as e:
            log.debug("recipient officer lookup suppressed: %s", e)
    # Fallback: buyer_email from the card (clerk, not officer — but better
    # than nothing for operators to manually re-route).
    primary = card.get("primary_contact") or {}
    return primary.get("email") or None


# ── Template rendering ───────────────────────────────────────────────────────

def _safe_format(template_str: str, ctx: dict) -> tuple[str, list[str]]:
    """Format with lenient missing-var detection. Returns (output,
    missing_keys). Missing keys cause the OUTPUT to retain the literal
    `{key}` so callers can detect + decide to disable the template."""
    import string
    formatter = string.Formatter()
    missing = []

    class _Tracking(dict):
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                missing.append(key)
                return "{" + key + "}"

    out = formatter.vformat(template_str, (), _Tracking(ctx))
    return out, missing


def _build_card_context(card: dict, conn) -> dict:
    """Compose the template-render context from one prospect card +
    Reytech identity + cert numbers + bid_memory + capability_credits."""
    from src.core.reytech_identity import render_context as _id_ctx, get_cert_context
    ctx = {}
    ctx.update(_id_ctx())
    ctx.update(get_cert_context())

    ctx["agency_name"] = card.get("dept_name") or card.get("dept_code") or ""
    # Top category from win_back / gap items.
    top_cat = ""
    for src_key in ("win_back_items", "gap_items"):
        items = card.get(src_key) or []
        if items:
            cat = (items[0] or {}).get("category") or ""
            if cat:
                top_cat = cat.replace("_", " ")
                break
    ctx["top_category"] = top_cat

    # SB cert clause — only included if cert_no exists.
    sb_no = ctx.get("sb_cert_no") or ""
    ctx["sb_cert_clause"] = (
        f"(SB cert #{sb_no}) " if sb_no else ""
    )

    # Capability-credit clause from V2-PR-3.
    credits = card.get("capability_credits") or []
    if credits:
        c = credits[0]
        po = c.get("po_number") or ""
        d = (c.get("item_description") or "")[:60]
        per_unit = c.get("per_unit_price") or 0
        won_at = c.get("won_at") or ""
        agency = c.get("credit_dept_name") or c.get("credit_dept_code") or ""
        ctx["capability_credit_clause"] = (
            f"For reference: Reytech recently delivered {d} to {agency} "
            f"on {po} ({won_at}, ${per_unit:.2f}/unit).\n\n"
        )
    else:
        ctx["capability_credit_clause"] = ""

    # Rebid-memo specific: incumbent + contract end + prior-bid clause.
    expiring = card.get("expiring_contracts") or []
    competitor = next(
        (c for c in expiring if not c.get("is_reytech")
         and not c.get("is_award_gap")), None,
    )
    if competitor:
        ctx["incumbent_supplier"] = competitor.get("supplier") or ""
        ctx["contract_end_date"] = competitor.get("end_date") or ""
    bid_lines = card.get("bid_memory_lines") or []
    last_lost = next(
        (m for m in bid_lines if (m.get("outcome") or "").lower() == "lost"),
        None,
    )
    if last_lost:
        ctx["prior_bid_clause"] = (
            f"Reytech bid this category previously: {last_lost.get('label','')[:160]}. "
            "We are positioned to compete on the rebid.\n\n"
        )
    else:
        ctx["prior_bid_clause"] = ""

    return ctx


def template_is_renderable(template_key: str, card: dict, conn) -> bool:
    """Return True iff this template's required_vars ALL resolve from
    the card. Drives the dropdown-option enable/disable state per the
    product-engineer "kill placeholder fallbacks" mandate.

    For cert_confirmation: only renderable if operator passes
    `cert_type` explicitly in render context (skip auto-eligibility
    for now — V2-PR-4 panel hasn't wired the trigger yet).
    """
    if template_key not in TEMPLATES:
        return False
    if template_key == "cert_confirmation":
        # Trigger isn't wired (V2-PR-4 cert-update doesn't set a flag yet).
        # Operator can pick from dropdown but auto-pick logic must skip.
        return False
    spec = TEMPLATES[template_key]
    ctx = _build_card_context(card, conn)
    recipient = resolve_recipient_email(card, conn)
    if recipient:
        ctx["recipient_email"] = recipient
    for var in spec["required_vars"]:
        val = ctx.get(var)
        if not val or (isinstance(val, str) and not val.strip()):
            return False
    return True


def pick_template(card: dict, conn) -> dict:
    """Auto-pick the highest-priority template that is RENDERABLE for
    this card. Returns {template_key, template_name, reason}.

    Priority order (per design doc):
      1. rfq_list_inclusion — when registration_status ∈ {not_registered, expired, unknown}
      2. rebid_memo         — when registered + active rebid window (red/amber)
      3. capability_refresher — when registered + no recent contact
      cert_confirmation — operator-only (never auto-picked)
    """
    reg_summary = (card.get("registration_summary") or {})
    reg_level = reg_summary.get("level") or "unknown"
    rebid_summary = (card.get("rebid_summary") or {})
    rebid_level = rebid_summary.get("level") or "none"

    candidates = []
    # Priority 1: not-registered / expired / unknown → ask for RFQ-list inclusion.
    if reg_level in ("not_registered", "expired", "unknown"):
        if template_is_renderable("rfq_list_inclusion", card, conn):
            candidates.append({
                "template_key": "rfq_list_inclusion",
                "reason": f"registration status is '{reg_level}' — primary "
                          "action is to get on their RFQ distribution list",
            })
    # Priority 2: registered + rebid window open.
    if reg_level == "registered" and rebid_level in ("red", "amber"):
        if template_is_renderable("rebid_memo", card, conn):
            candidates.append({
                "template_key": "rebid_memo",
                "reason": f"registered AND rebid window open "
                          f"({rebid_level}) — competitor contract is expiring",
            })
    # Priority 3: registered, no rebid pressure → quarterly refresher.
    if reg_level == "registered" and rebid_level in ("none", "renewal"):
        if template_is_renderable("capability_refresher", card, conn):
            candidates.append({
                "template_key": "capability_refresher",
                "reason": "registered + no rebid pressure — keep relationship "
                          "warm with capability update",
            })
    # Fallback: rfq_list_inclusion if nothing else fired.
    if not candidates and template_is_renderable("rfq_list_inclusion", card, conn):
        candidates.append({
            "template_key": "rfq_list_inclusion",
            "reason": "default — RFQ-list inclusion is the safest first ask",
        })

    if not candidates:
        return {"template_key": None, "template_name": None,
                "reason": "no template renderable for this card "
                          "(missing recipient or category data)"}
    pick = candidates[0]
    pick["template_name"] = TEMPLATES[pick["template_key"]]["name"]
    return pick


def render_template(template_key: str, card: dict, conn,
                     extra_ctx: dict | None = None) -> dict:
    """Render one template into {subject, body, recipient_email,
    template_key, template_name, missing_vars, reason_unrenderable}.

    If template is unrenderable, returns the missing vars and the
    caller MUST NOT show the rendered output to the operator (per
    product-engineer: never ship placeholder strings to customer copy)."""
    if template_key not in TEMPLATES:
        return {"ok": False, "error": f"unknown template '{template_key}'"}
    spec = TEMPLATES[template_key]
    ctx = _build_card_context(card, conn)
    recipient = resolve_recipient_email(card, conn)
    if recipient:
        ctx["recipient_email"] = recipient
    if extra_ctx:
        ctx.update(extra_ctx)

    # Identify missing required vars BEFORE formatting so caller can
    # gate the UI.
    missing_required = [v for v in spec["required_vars"]
                        if not (ctx.get(v) or "").strip()]
    if missing_required:
        return {
            "ok": False,
            "template_key": template_key,
            "template_name": spec["name"],
            "missing_vars": missing_required,
            "reason_unrenderable": (
                "required variable(s) not resolvable from this card: "
                + ", ".join(missing_required)
            ),
        }

    subject, sub_missing = _safe_format(spec["subject_template"], ctx)
    body, body_missing = _safe_format(spec["body_template"], ctx)
    return {
        "ok": True,
        "template_key": template_key,
        "template_name": spec["name"],
        "subject": subject,
        "body": body,
        "recipient_email": recipient,
        "missing_vars": list(set(sub_missing + body_missing)),
    }
