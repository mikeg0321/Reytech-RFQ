"""Compliance Validator — deterministic + LLM checks before a quote advances
from PRICED → QA_PASS.

This is called by `QuoteOrchestrator._run_transition` at the qa_pass gate.
It is intentionally split into:

  1. Deterministic checks (fast, cheap, no network). These are the ones
     that MUST pass — failure populates `blockers` and blocks the stage
     advance.
  2. LLM gap check (optional, network). Reads the buyer email and the set
     of forms filled, asks Claude to flag things the buyer asked for that
     we did NOT address. This returns `warnings` — it never blocks, to
     avoid LLM hallucinations kicking good packages into a failure loop.

Contract (called from quote_orchestrator.py):
    validate_package(*, quote, per_form_reports, buyer_email_text)
        -> {"blockers": [...], "warnings": [...], "checks": [...], ...}
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

log = logging.getLogger("reytech.compliance_validator")

MODEL = "claude-sonnet-4-6"

REYTECH_VENDOR_NAME = "Reytech Inc."
REYTECH_REPRESENTATIVE = "Michael Guadan"
REYTECH_EMAIL = "sales@reytechinc.com"

# Quote numbers are R<YY>Q<NNNN> — e.g. R26Q0321. Enforced because the
# quote counter has produced bad numbers in past incidents.
_QUOTE_NUM_RE = re.compile(r"^R\d{2}Q\d{4}$")


def _check_required_forms(
    quote: Any, per_form_reports: list[dict]
) -> tuple[list[str], list]:
    """Every `agency_config.required_forms` entry must show up filled.

    Returns `(blockers, skips)`. Skips carry SkipReason objects for any
    dependency the check needed but couldn't reach (failed import of
    agency_config or the orchestrator's _FORM_ID_TO_PROFILE_ID map). A
    BLOCKER skip means the check did NOT run truthfully — the orchestrator
    routes it via add_skip() so the operator sees the import failure
    instead of a silently-empty blockers list.
    """
    from src.core.dependency_check import Severity, try_import

    skips: list = []
    where = "compliance_validator._check_required_forms"

    agency_config, ac_skip = try_import(
        "src.core.agency_config", severity=Severity.BLOCKER, where=where,
    )
    if ac_skip is not None:
        skips.append(ac_skip)
        return [], skips

    blockers: list[str] = []
    agency_key = getattr(quote.header, "agency_key", "") or ""
    if not agency_key:
        return blockers, skips

    cfg = agency_config.DEFAULT_AGENCY_CONFIGS.get(agency_key, {})
    required = cfg.get("required_forms") or []
    if not required:
        return blockers, skips

    # A profile is only "filled" if it actually produced bytes. Earlier this
    # check accepted filled=True/qa_passed=True even when bytes=0, which let
    # pass-through profiles with a missing source PDF (e.g. sellers_permit)
    # silently advance to GENERATED with an incomplete package.
    filled_profile_ids = {
        r.get("profile_id", "") for r in per_form_reports
        if r.get("filled") and r.get("qa_passed") and int(r.get("bytes") or 0) > 0
    }

    # Translate required form_ids to profile_ids via the orchestrator map.
    qo_mod, qo_skip = try_import(
        "src.core.quote_orchestrator", severity=Severity.BLOCKER, where=where,
    )
    if qo_skip is not None:
        skips.append(qo_skip)
        return blockers, skips
    _FORM_ID_TO_PROFILE_ID = getattr(qo_mod, "_FORM_ID_TO_PROFILE_ID", {})

    for form_id in required:
        if form_id == "bidpkg":
            continue  # container, not a per-form profile
        expected_pid = _FORM_ID_TO_PROFILE_ID.get(form_id, "")
        if not expected_pid:
            # Unmapped form — surfaced as a warning by the orchestrator, not a blocker here.
            continue
        if expected_pid not in filled_profile_ids:
            blockers.append(
                f"required form '{form_id}' (profile {expected_pid}) not filled/qa_passed for agency '{agency_key}'"
            )
    return blockers, skips


def _check_quote_number(quote: Any) -> list[str]:
    """Quote number lives on `header.solicitation_number` (e.g. R26Q0321)."""
    qn = getattr(quote.header, "solicitation_number", "") or ""
    if not qn:
        return ["quote_number is empty"]
    if not _QUOTE_NUM_RE.match(qn):
        return [f"quote_number '{qn}' does not match R<YY>Q<NNNN> pattern"]
    return []


def _check_vendor_identity(per_form_reports: list[dict]) -> list[str]:
    """Placeholder — the per-form reports do not yet carry raw field values.

    Today this is a no-op because `quote_engine.draft()` doesn't surface
    filled field values in its QA report. When it does, this check enforces
    Reytech canonical identity (name, representative, email) across every
    filled form.
    """
    return []


def _invoke_llm_gap_check(
    *, api_key: str, buyer_email_text: str, filled_profiles: list[str], quote: Any
) -> list[str]:
    """Make the actual Anthropic call. Split out so tests can patch just this
    boundary while letting the surrounding setup checks (buyer_email, api_key)
    run normally."""
    import anthropic

    user = json.dumps({
        "buyer_email": buyer_email_text[:8000],
        "forms_we_filled": filled_profiles,
        "agency": getattr(quote.header, "agency_key", "") or "",
        "solicitation_number": getattr(quote.header, "solicitation_number", "") or "",
    }, indent=2)

    tool = {
        "name": "record_gaps",
        "description": "Record items the buyer's email mentions that the filled forms may not address.",
        "input_schema": {
            "type": "object",
            "properties": {
                "gaps": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "concern":    {"type": "string"},
                            "severity":   {"type": "string", "enum": ["info", "warning"]},
                        },
                        "required": ["concern", "severity"],
                    },
                },
            },
            "required": ["gaps"],
        },
    }

    system = (
        "You are an RFQ compliance reviewer. Given a buyer's email and the list "
        "of forms the vendor filled, surface concerns about things the buyer "
        "explicitly asked for that the vendor may not have included. Do NOT flag "
        "general politeness, standard boilerplate, or things outside form scope. "
        "If nothing concerning, return gaps=[]."
    )

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=system,
        tools=[tool],
        tool_choice={"type": "tool", "name": "record_gaps"},
        messages=[{"role": "user", "content": user}],
    )

    gaps: list[str] = []
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "record_gaps":
            for gap in (block.input or {}).get("gaps", []) or []:
                concern = (gap.get("concern") or "").strip()
                if concern:
                    gaps.append(concern[:240])
    return gaps


def _run_llm_gap_check(
    quote: Any, per_form_reports: list[dict], buyer_email_text: str
) -> tuple[list[str], str | None]:
    """Ask Claude what the buyer asked for that the package may not address.

    Returns `(gaps, skipped_reason)`. `skipped_reason` is None when the LLM
    actually ran; otherwise it carries a short human-readable reason so the
    operator sees that the LLM portion was not exercised. Returning a real
    reason instead of silently producing `[]` is what lets validate_package()
    surface the skip as a warning.
    """
    if not (buyer_email_text or "").strip():
        return [], "no buyer email text available"
    try:
        import anthropic  # noqa: F401
    except Exception:
        return [], "anthropic SDK not installed"
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return [], "ANTHROPIC_API_KEY not set"

    filled_profiles = [r.get("profile_id", "") for r in per_form_reports if r.get("filled")]
    try:
        gaps = _invoke_llm_gap_check(
            api_key=api_key,
            buyer_email_text=buyer_email_text,
            filled_profiles=filled_profiles,
            quote=quote,
        )
    except Exception as e:
        log.debug("LLM gap check failed: %s", e)
        return [], f"LLM call failed: {type(e).__name__}: {e}"
    return gaps, None


def validate_package(
    *,
    quote: Any,
    per_form_reports: list[dict],
    buyer_email_text: str = "",
) -> dict:
    """Run the compliance gate. Returns a dict consumed by QuoteOrchestrator.

    Shape:
        {
            "checked": bool,
            "blockers": [str],          # if non-empty, qa_pass is refused
            "warnings": [str],          # advisory only
            "skips":    [SkipReason],   # routed by orchestrator.add_skip()
            "checks": [                 # per-check trace for the dashboard
                {"name": ..., "ok": bool, "detail": str},
                ...
            ],
        }
    """
    blockers: list[str] = []
    warnings: list[str] = []
    skips: list = []
    checks: list[dict] = []

    # 1) Required forms all filled + QA-passed
    rf, rf_skips = _check_required_forms(quote, per_form_reports)
    skips.extend(rf_skips)
    rf_problems = rf + [f"{s.name}: {s.reason}" for s in rf_skips]
    checks.append({"name": "required_forms", "ok": not rf_problems, "detail": "; ".join(rf_problems) or "OK"})
    blockers.extend(rf)

    # 2) Quote number shape
    qn = _check_quote_number(quote)
    checks.append({"name": "quote_number", "ok": not qn, "detail": "; ".join(qn) or "OK"})
    blockers.extend(qn)

    # 3) Vendor identity (best-effort today)
    vi = _check_vendor_identity(per_form_reports)
    checks.append({"name": "vendor_identity", "ok": not vi, "detail": "; ".join(vi) or "OK"})
    blockers.extend(vi)

    # 4) LLM gap check — warnings only. A skipped LLM run is itself a
    #    warning so the dashboard reflects when this layer didn't actually
    #    weigh in (no buyer email, no API key, SDK not installed, LLM 5xx).
    gaps, skipped_reason = _run_llm_gap_check(quote, per_form_reports, buyer_email_text)
    if skipped_reason:
        checks.append({"name": "llm_gap", "ok": False, "detail": f"skipped: {skipped_reason}"})
        warnings.append(f"LLM gap check skipped: {skipped_reason}")
    else:
        checks.append({"name": "llm_gap", "ok": True, "detail": f"{len(gaps)} gap(s)"})
    warnings.extend(gaps)

    return {
        "checked": True,
        "blockers": blockers,
        "warnings": warnings,
        "skips": skips,
        "checks": checks,
    }
