"""fill_plan_builder.py — Phase 1.6 PR3a: bind email-contract → fill-plan.

The buyer email is the contract: it (and any attached "Bid Instructions"
PDF) tells us *which forms* to submit. The agency_config tells us *which
forms apply by default for that buyer*. The profile registry tells us
*which YAML profile will fill each form*.

Today these three sources are consulted in different code paths and
never joined. The fill-plan builder produces a single read-only view
of the join — answering "for this quote, what's going to fill correctly,
what's going to fill empty, and what doesn't have a profile at all?"

Read-only. No mutations. Safe to call on any quote.
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Optional

log = logging.getLogger("reytech.fill_plan")


# ─── Friendly form names (UI display) ──────────────────────────────────────
FORM_DISPLAY_NAMES = {
    "703b": "AMS 703B Quote Worksheet",
    "703c": "AMS 703C Fair & Reasonable",
    "704a": "AMS 704A Quote Form",
    "704b": "AMS 704B Quote Worksheet",
    "dvbe843": "STD 843 DVBE Declaration",
    "darfur_act": "Darfur Contracting Act",
    "cv012_cuf": "CV 012 Commercially Useful Function",
    "barstow_cuf": "Barstow CUF Supplement",
    "calrecycle74": "CalRecycle 74 Recycled Content",
    "bidder_decl": "Bidder Declaration GSPD-05-105",
    "std204": "STD 204 Payee Data Record",
    "std205": "STD 205 Payee Supplemental",
    "std1000": "STD 1000 GenAI Disclosure",
    "sellers_permit": "Seller's Permit",
    "w9": "W-9 Tax Form",
    "drug_free": "Drug-Free Workplace",
    "obs_1600": "OBS 1600 Food Certification",
    "quote": "Reytech Quote Letterhead",
    "cchcs_it_rfq": "CCHCS IT Goods/Services RFQ",
    "bidpkg": "CCHCS Bid Package",
    "dsh_attA": "DSH Attachment A — Bidder Identity",
    "dsh_attB": "DSH Attachment B — Pricing",
    "dsh_attC": "DSH Attachment C — Forms Checklist",
}


# ─── Critical concepts: alias lists per real semantic vocabulary ──────────
# Enhancement C (2026-04-27): chrome-verify revealed the panel was
# falsely flagging profiles that DO map equivalent fields under
# different semantic names. Survey of all 17 committed profiles
# informed these alias lists.
CRITICAL_CONCEPTS = {
    "vendor_name": [
        "vendor.business_name", "vendor.name", "supplier.name",
    ],
    "vendor_fein": [
        "vendor.fein", "vendor.fein_2", "vendor.tax_id",
    ],
    "signature": [
        "vendor.signature", "signatures.primary", "signatures.owner1",
        "signer.name", "signer.printed_name",
        "signer.printed_name_and_title",
    ],
    "items_unit_price": [
        "items[n].unit_price",  # canonical row template; see _profile_satisfies_concept
    ],
}

# Per-form_type required CONCEPTS. 703B/703C are header/cert forms — no
# item rows live there (those go on 704A/704B); reflect that here.
CRITICAL_CONCEPTS_PER_FORM = {
    "703b":         ["vendor_name", "signature"],
    "703c":         ["vendor_name", "signature"],
    "704a":         ["vendor_name", "signature", "items_unit_price"],
    "704b":         ["vendor_name", "signature", "items_unit_price"],
    "dvbe843":      ["vendor_name", "signature"],
    "darfur_act":   ["vendor_name", "signature"],
    "cv012_cuf":    ["vendor_name", "signature"],
    "calrecycle74": ["vendor_name", "signature"],
    "bidder_decl":  ["vendor_name", "signature"],
    "std204":       ["vendor_name", "vendor_fein", "signature"],
    "std205":       ["vendor_name", "signature"],
    "std1000":      ["vendor_name", "signature"],
    "sellers_permit": ["vendor_name"],
    "w9":           ["vendor_name", "vendor_fein", "signature"],
    "drug_free":    ["vendor_name", "signature"],
    "obs_1600":     ["vendor_name", "signature"],
    "quote":        ["vendor_name", "items_unit_price"],
    "cchcs_it_rfq": ["vendor_name", "signature", "items_unit_price"],
}
_DEFAULT_CRITICAL_CONCEPTS = ["signature"]


# Backwards-compat shim — older callers import CRITICAL_SEMANTICS
CRITICAL_SEMANTICS = CRITICAL_CONCEPTS_PER_FORM
_DEFAULT_CRITICAL = _DEFAULT_CRITICAL_CONCEPTS


def _profile_satisfies_concept(profile, concept: str) -> bool:
    """Does the profile cover this critical concept by any alias?

    Three branches:
      1. signature — also satisfied by signature_field set OR by
         raw_yaml.signature being a non-empty dict (overlay-mode
         signatures don't use a form field but ARE handled at fill time)
      2. items_* — match any items[*] semantic with the concept's suffix
      3. all other concepts — direct alias equality
    """
    aliases = CRITICAL_CONCEPTS.get(concept, [])
    mapped = {fm.semantic for fm in getattr(profile, "fields", [])}

    if concept == "signature":
        if (getattr(profile, "signature_field", "") or "").strip():
            return True
        sig_decl = (getattr(profile, "raw_yaml", {}) or {}).get("signature")
        if isinstance(sig_decl, dict) and sig_decl:
            return True
        # Fall through to alias check

    if concept.startswith("items_"):
        suffix = concept.replace("items_", "")
        return any(
            ("items[" in m) and (suffix in m) for m in mapped
        )

    return any(alias in mapped for alias in aliases)


# ─── Status taxonomy ───────────────────────────────────────────────────────
STATUS_READY            = "ready"               # buyer template found, profile matched, all critical fields covered
STATUS_GENERIC_FALLBACK = "generic_fallback"    # profile matched but it's the standard, not buyer-specific
STATUS_MISSING_FIELDS   = "missing_critical"    # profile matched but critical fields unmapped
STATUS_UNKNOWN_VARIANT  = "unknown_variant"     # buyer attached a blank we can't fingerprint
STATUS_FLAT_PDF         = "flat_pdf"            # buyer's PDF has 0 form fields — needs overlay mode
STATUS_NO_TEMPLATE      = "no_template"         # required form, no buyer-attached blank, no committed profile blank
STATUS_NO_PROFILE       = "no_profile"          # no FormProfile registered for this form_id at all


@dataclass
class FillPlanItem:
    form_id: str
    form_name: str
    required_by: list  # ["agency_config", "email_contract", "attachment_contract"]
    status: str
    matched_profile_id: str = ""
    profile_kind: str = ""           # "buyer_specific" | "generic" | "none"
    fingerprint: str = ""
    buyer_template_filename: str = ""
    critical_fields: list = field(default_factory=list)
    missing_critical: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    # Enhancement B: candidate-badge surface. When the buyer attached a
    # blank PDF whose fingerprint isn't covered by an existing FormProfile,
    # the panel renders a "🆕 NEW VARIANT" badge linking to a promotion
    # workflow. Populated only when buyer_template_capture has registered
    # the fingerprint as a candidate.
    candidate_id: int = 0            # buyer_template_candidates.id (0 = none)
    candidate_fingerprint: str = ""  # truncated to 16 hex chars for display
    candidate_seen_count: int = 0    # how many times we've seen this variant


@dataclass
class FillPlan:
    quote_id: str
    quote_type: str               # "pc" | "rfq"
    agency_key: str
    agency_name: str
    contract_source: str          # "email+attachment" | "email" | "agency_only"
    contract_summary: dict        # subset of RFQRequirements for display
    items: list = field(default_factory=list)
    total_required: int = 0
    total_ready: int = 0
    total_warning: int = 0
    total_blocked: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


# ═══════════════════════════════════════════════════════════════════════════
# CORE
# ═══════════════════════════════════════════════════════════════════════════

def build_fill_plan(quote_id: str, quote_type: str,
                    quote_data: Optional[dict] = None) -> FillPlan:
    """Build a fill-plan for a quote.

    Args:
        quote_id: PC or RFQ id
        quote_type: "pc" or "rfq"
        quote_data: optional pre-loaded quote dict (skips DB lookup)
    """
    if quote_data is None:
        quote_data = _load_quote(quote_id, quote_type)

    if not quote_data:
        return FillPlan(quote_id=quote_id, quote_type=quote_type,
                        agency_key="", agency_name="",
                        contract_source="agency_only", contract_summary={})

    # 1. Email contract
    contract = _load_contract(quote_data)

    # 2. Agency config → required forms
    agency_key, agency_cfg = _resolve_agency(quote_data)
    agency_required = list(agency_cfg.get("required_forms", []))

    # 3. Profile registry + attached files
    profiles = _load_profiles_safe()
    attached = _list_attachments(quote_id, quote_type)

    # 3b. PR3b: parse PDF attachments for additional contract data.
    # Often the buyer's "Bid Instructions" PDF is the real forms list;
    # the email body just says "see attached." Without this, the
    # contract is incomplete and the panel under-reports requirements.
    attachment_required = _attachment_required_forms(attached)
    if attachment_required:
        existing = list(contract.get("forms_required", []))
        for f in attachment_required:
            if f and f not in existing:
                existing.append(f)
        contract["forms_required"] = existing
        contract["forms_required_from_attachments"] = attachment_required

    # 4. Merge required forms — agency baseline + contract additions
    contract_required = list(contract.get("forms_required", []))
    effective_required = _merge_required(agency_required, contract_required)

    # 5. Build per-form items
    attachment_required_set = set(
        contract.get("forms_required_from_attachments") or []
    )
    items = []
    for form_id in effective_required:
        rb = _required_by_for(form_id, agency_required, contract_required)
        if form_id in attachment_required_set and "attachment_contract" not in rb:
            rb = list(rb) + ["attachment_contract"]
        item = _build_item(
            form_id=form_id,
            agency_key=agency_key,
            required_by=rb,
            profiles=profiles,
            attached=attached,
        )
        items.append(item)

    # 6. Roll up
    plan = FillPlan(
        quote_id=quote_id,
        quote_type=quote_type,
        agency_key=agency_key,
        agency_name=agency_cfg.get("name", agency_key),
        contract_source=_contract_source_label(contract, attached),
        contract_summary={
            "due_date": contract.get("due_date", ""),
            "due_time": contract.get("due_time", ""),
            "solicitation_number": contract.get("solicitation_number", ""),
            "food_items_present": bool(contract.get("food_items_present", False)),
            "extraction_method": contract.get("extraction_method", "none"),
        },
        items=items,
        total_required=len(items),
    )
    for it in items:
        if it.status == STATUS_READY:
            plan.total_ready += 1
        elif it.status in (STATUS_GENERIC_FALLBACK, STATUS_MISSING_FIELDS):
            plan.total_warning += 1
        else:
            plan.total_blocked += 1

    return plan


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _load_quote(quote_id: str, quote_type: str) -> Optional[dict]:
    """Load quote row as a dict. Returns None if not found."""
    try:
        from src.core.db import get_db
        table = "price_checks" if quote_type == "pc" else "rfqs"
        with get_db() as conn:
            row = conn.execute(
                f"SELECT * FROM {table} WHERE id = ?", (quote_id,)
            ).fetchone()
            return dict(row) if row else None
    except Exception as e:
        log.debug("_load_quote(%s, %s) error: %s", quote_id, quote_type, e)
        return None


def _load_contract(quote_data: dict) -> dict:
    """Parse requirements_json column → dict. Returns {} if absent/invalid."""
    raw = quote_data.get("requirements_json") or ""
    if not raw:
        return {}
    try:
        return json.loads(raw) or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _resolve_agency(quote_data: dict) -> tuple:
    """Match agency from quote → (agency_key, config_dict)."""
    try:
        from src.core.agency_config import match_agency
        return match_agency(quote_data)
    except Exception as e:
        log.debug("_resolve_agency error: %s", e)
        try:
            from src.core.agency_config import get_agency_config
            return ("other", get_agency_config("other"))
        except Exception:
            return ("other", {"name": "Other", "required_forms": []})


def _merge_required(agency_required: list, contract_required: list) -> list:
    """Union, preserving agency order then appending contract-only additions."""
    seen = set()
    out = []
    for f in agency_required:
        if f and f not in seen:
            seen.add(f); out.append(f)
    for f in contract_required:
        if f and f not in seen:
            seen.add(f); out.append(f)
    return out


def _required_by_for(form_id: str, agency_required: list,
                     contract_required: list) -> list:
    sources = []
    if form_id in agency_required:
        sources.append("agency_config")
    if form_id in contract_required:
        sources.append("email_contract")
    return sources or ["unknown"]


def _attachment_required_forms(attached: list) -> list:
    """Run the regex extractor over PDF attachments and return the
    union of required form_ids. Defensive — never raises."""
    if not attached:
        return []
    try:
        from src.agents.attachment_contract_parser import (
            parse_attachments_for_requirements,
        )
        merged = parse_attachments_for_requirements(attached)
        return list(getattr(merged, "forms_required", []) or [])
    except Exception as e:
        log.debug("_attachment_required_forms suppressed: %s", e)
        return []


def _load_profiles_safe() -> dict:
    try:
        from src.forms.profile_registry import load_profiles
        return load_profiles() or {}
    except Exception as e:
        log.debug("_load_profiles_safe error: %s", e)
        return {}


def _list_attachments(quote_id: str, quote_type: str) -> list:
    """List the buyer's attached PDFs for the quote.

    Returns: [{filename, file_path_or_id, file_type}] (no BLOB data).
    """
    try:
        if quote_type == "rfq":
            from src.api.data_layer import list_rfq_files
            files = list_rfq_files(quote_id, category="template") or []
            return [{
                "filename": f.get("filename", ""),
                "file_id": f.get("id"),
                "file_type": (f.get("file_type") or "").lower(),
            } for f in files]
        else:
            # PC: source_file column on price_checks
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT source_file FROM price_checks WHERE id = ?",
                    (quote_id,),
                ).fetchone()
                src = (dict(row).get("source_file") or "") if row else ""
                if src:
                    return [{
                        "filename": os.path.basename(src),
                        "file_id": None,
                        "file_path": src,
                        "file_type": "pdf",
                    }]
                return []
    except Exception as e:
        log.debug("_list_attachments(%s, %s) error: %s", quote_id, quote_type, e)
        return []


def _build_item(form_id: str, agency_key: str, required_by: list,
                profiles: dict, attached: list) -> FillPlanItem:
    """Resolve a single required form to a FillPlanItem."""
    name = FORM_DISPLAY_NAMES.get(form_id, form_id)
    concepts = list(CRITICAL_CONCEPTS_PER_FORM.get(
        form_id, _DEFAULT_CRITICAL_CONCEPTS,
    ))
    # critical_fields is the user-facing label list (UI uses it as-is)
    critical = list(concepts)

    # Find candidate profiles for this form_id
    candidates = [p for p in profiles.values()
                  if getattr(p, "form_type", "") == form_id]

    if not candidates:
        return FillPlanItem(
            form_id=form_id, form_name=name, required_by=required_by,
            status=STATUS_NO_PROFILE, critical_fields=critical,
            notes=[f"No FormProfile registered for form_type={form_id!r}"],
        )

    # Prefer buyer-specific over generic
    buyer_specific = [p for p in candidates if getattr(p, "agency_match", [])
                      and _agency_matches_any(p, agency_key)]
    generic = [p for p in candidates if not getattr(p, "agency_match", [])]

    if buyer_specific:
        chosen = buyer_specific[0]
        kind = "buyer_specific"
        base_status = STATUS_READY
    elif generic:
        chosen = generic[0]
        kind = "generic"
        base_status = STATUS_GENERIC_FALLBACK
    else:
        # candidates exist for this form_type but none apply (all are buyer-
        # specific for OTHER buyers) — surface as no usable profile
        return FillPlanItem(
            form_id=form_id, form_name=name, required_by=required_by,
            status=STATUS_NO_PROFILE, critical_fields=critical,
            notes=["Profiles exist for this form but none apply to this buyer"],
        )

    # Detect missing critical CONCEPTS via alias-aware satisfaction check.
    # Enhancement C: handles vendor.business_name/vendor.name, signer.* as
    # signature, raw_yaml.signature dict for overlay-mode signatures, etc.
    # static_attach profiles ship the pre-printed PDF verbatim — no fields
    # to map → no critical-field check needed.
    fill_mode = (getattr(chosen, "fill_mode", "") or "").lower()
    if fill_mode == "static_attach":
        missing = []
    else:
        missing = [c for c in concepts
                   if not _profile_satisfies_concept(chosen, c)]

    status = base_status if not missing else STATUS_MISSING_FIELDS

    # Was the buyer's blank for this form attached? (heuristic: filename
    # contains form_id or a known token)
    buyer_pdf = _find_buyer_blank(form_id, attached)

    # Enhancement B: candidate-badge surface. If the buyer attached a
    # blank for this form AND we have a candidate row for its
    # fingerprint, surface the candidate metadata so the panel can
    # render "🆕 NEW VARIANT" with a promote link.
    cand_id, cand_fp, cand_seen = _candidate_for_attachment(
        form_id, agency_key, attached, buyer_pdf,
    )

    return FillPlanItem(
        form_id=form_id, form_name=name, required_by=required_by,
        status=status,
        matched_profile_id=getattr(chosen, "id", ""),
        profile_kind=kind,
        fingerprint=getattr(chosen, "fingerprint", "")[:12],
        buyer_template_filename=buyer_pdf,
        critical_fields=critical,
        missing_critical=missing,
        candidate_id=cand_id,
        candidate_fingerprint=cand_fp,
        candidate_seen_count=cand_seen,
    )


def _agency_matches_any(profile, agency_key: str) -> bool:
    """Reuse the resolver's matching logic if available."""
    try:
        from src.forms.profile_registry import _agency_matches
        return _agency_matches(getattr(profile, "agency_match", []), agency_key)
    except Exception:
        # fallback: simple substring check
        agency = (agency_key or "").lower().replace(" ", "_")
        return any(t and (t in agency or agency in t)
                   for t in (getattr(profile, "agency_match", None) or []))


def _find_buyer_blank(form_id: str, attached: list) -> str:
    """Heuristic: does any attached file look like the buyer's blank for this form?"""
    if not attached:
        return ""
    needles = [form_id, form_id.replace("_", "-"), form_id.replace("_", " ")]
    for f in attached:
        name = (f.get("filename", "") or "").lower()
        if any(n in name for n in needles):
            return f["filename"]
    return ""


def _candidate_for_attachment(form_id: str, agency_key: str,
                              attached: list, buyer_pdf_filename: str) -> tuple:
    """Look up a buyer_template_candidates row for the attached blank.

    Returns (candidate_id, fingerprint_truncated, seen_count). Empty
    tuple equivalents (0, "", 0) when no candidate found, or when the
    table doesn't exist (older deploys). Defensive — never raises.
    """
    if not attached or not buyer_pdf_filename:
        return (0, "", 0)
    try:
        from src.agents.buyer_template_capture import _fingerprint_attachment
        from src.core.db import get_db
    except ImportError:
        return (0, "", 0)

    # Find the attachment dict matching this filename
    target = None
    for a in attached:
        if (a.get("filename") or "") == buyer_pdf_filename:
            target = a
            break
    if not target:
        return (0, "", 0)

    try:
        fp, _, _ = _fingerprint_attachment(target)
        if not fp:
            return (0, "", 0)
        agency = (agency_key or "").lower().strip()
        with get_db() as conn:
            row = conn.execute(
                """SELECT id, seen_count FROM buyer_template_candidates
                   WHERE fingerprint = ? AND agency_key = ?
                   LIMIT 1""",
                (fp, agency),
            ).fetchone()
            if row:
                return (row["id"], fp[:16], row["seen_count"])
    except Exception as e:
        log.debug("_candidate_for_attachment suppressed: %s", e)
    return (0, "", 0)


def _contract_source_label(contract: dict, attached: list) -> str:
    has_email = bool(contract.get("forms_required") or contract.get("due_date"))
    has_attach_extracted = bool(contract.get("forms_required_from_attachments"))
    has_attach_files = bool(attached)
    if has_email and has_attach_extracted:
        return "email+attachment"
    if has_attach_extracted:
        return "attachment"
    if has_email:
        return "email"
    if has_attach_files:
        # attachments present but parser found nothing parseable
        return "email" if contract else "agency_only"
    return "agency_only"
