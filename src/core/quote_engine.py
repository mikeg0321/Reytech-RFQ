"""Unified Quote Engine — single entry point for the RFQ/PC workflow.

This module is the "one workflow" the design called for (see Q3 in
project_quote_rebuild_decisions): PC and RFQ are labels on a Quote, not
forks in the code. Routes call this module and nothing else.

Pipeline (each step is independently callable):

    1. ingest(source)        -> Quote               # PDF | dict | URL
    2. enrich_pricing(quote) -> Quote               # oracle hook
    3. fill_one(quote, ...)  -> bytes               # one form
    4. validate(...)         -> ValidationReport    # readback gate
    5. assemble(quote, ...)  -> PackageResult       # full package
    6. sign(pdf_bytes)       -> bytes               # signature + flatten

One-shot helpers wrap the common flows:

    draft(quote, profile_id=None) -> DraftResult
        # fill + QA on a single profile, returns editable PDF + report

    finalize(quote, profile_ids=None) -> PackageResult
        # fill all + QA all + merge + sign + flatten

The legacy routes still call into the old fill paths. New routes import
from here. Migration is feature-flagged via QUOTE_MODEL_V2; the adapter
already handles dict↔Quote round-tripping.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional, Union

from src.core.quote_model import Quote, DocType
from src.forms.profile_registry import (
    FormProfile,
    load_profiles,
    match_profile,
)
from src.forms.parse_engine import parse, ParseWarning
from src.forms.fill_engine import fill, approve_and_sign
from src.forms.qa_engine import validate as qa_validate, ValidationReport
from src.forms.package_engine import assemble, PackageResult

log = logging.getLogger(__name__)

# Default profile by doc_type. Routes can override per call.
_DEFAULT_PROFILE_BY_DOC_TYPE = {
    DocType.PC: "704a_reytech_standard",
    DocType.RFQ: "704a_reytech_standard",  # Falls back until 704b profile lands
}

# Cached profile registry (load once per process).
_PROFILES_CACHE: Optional[dict[str, FormProfile]] = None


def get_profiles(refresh: bool = False) -> dict[str, FormProfile]:
    """Return the loaded profile registry (cached after first call)."""
    global _PROFILES_CACHE
    if refresh or _PROFILES_CACHE is None:
        _PROFILES_CACHE = load_profiles()
        log.info("quote_engine: loaded %d profiles", len(_PROFILES_CACHE))
    return _PROFILES_CACHE


def pick_profile(
    quote: Quote,
    profile_id: Optional[str] = None,
    pdf_hint: Optional[str] = None,
) -> FormProfile:
    """Resolve which profile to use for this quote.

    Resolution order:
      1. Explicit profile_id (if provided and known)
      2. Content fingerprint of pdf_hint (if provided and the file exists)
      3. Fingerprint of quote.provenance.parsed_from_files[0]
      4. Doc-type default

    Raises KeyError if no profile can be resolved.
    """
    profiles = get_profiles()

    if profile_id and profile_id in profiles:
        return profiles[profile_id]

    candidate_pdfs = []
    if pdf_hint:
        candidate_pdfs.append(pdf_hint)
    candidate_pdfs.extend(quote.provenance.parsed_from_files)

    for pdf in candidate_pdfs:
        if pdf and os.path.exists(pdf):
            matched = match_profile(pdf, profiles)
            if matched:
                return matched

    default_id = _DEFAULT_PROFILE_BY_DOC_TYPE.get(quote.doc_type)
    if default_id and default_id in profiles:
        return profiles[default_id]

    raise KeyError(
        f"No profile resolvable for doc_type={quote.doc_type}, "
        f"profile_id={profile_id!r}, pdf_hint={pdf_hint!r}"
    )


# ── Step 1: Ingest ─────────────────────────────────────────────────────────

def ingest(
    source: Union[str, dict],
    doc_type: str = "pc",
) -> tuple[Quote, list[ParseWarning]]:
    """Bring a source into a Quote.

    Source may be:
      - dict  : legacy PC/RFQ dict (round-tripped through Quote.from_legacy_dict)
      - str   : path to a PDF (parsed via parse_engine)
      - other : currently raises ValueError (URL ingest is Phase 5)

    Returns (quote, warnings). Never raises on parse failure — the warning
    list carries the failure reason so callers can surface it.
    """
    if isinstance(source, dict):
        quote = Quote.from_legacy_dict(source, doc_type=doc_type)
        return quote, []

    if isinstance(source, str):
        if not os.path.exists(source):
            return Quote(doc_type=DocType(doc_type)), [
                ParseWarning("source", f"Source not found: {source}", "error")
            ]
        quote, warnings = parse(source, profiles=get_profiles())
        # parse() defaults to PC; honor caller's doc_type
        try:
            quote.doc_type = DocType(doc_type)
        except ValueError:
            pass
        return quote, warnings

    raise ValueError(f"Unsupported source type: {type(source).__name__}")


# ── Step 2: Pricing enrichment ─────────────────────────────────────────────

def _oracle_recommendation_to_flat(raw: dict) -> dict:
    """Flatten pricing_oracle_v2.get_pricing() output into the shape
    enrich_pricing expects. get_pricing returns nested dicts (cost, market,
    matched_item, recommendation); we project the fields the quote engine
    populates on each LineItem."""
    if not raw:
        return {}
    matched = raw.get("matched_item") or {}
    cost = raw.get("cost") or {}
    market = raw.get("market") or {}
    rec = raw.get("recommendation") or {}

    flat = {
        "asin": matched.get("asin") or "",
        "supplier": matched.get("supplier") or cost.get("supplier") or "",
        "source_url": matched.get("product_url") or matched.get("supplier_url") or "",
        "confidence": raw.get("confidence") or matched.get("confidence") or 0,
        "catalog_cost": (
            cost.get("locked_cost")
            or matched.get("last_cost")
            or 0
        ),
        "scprs_price": market.get("competitor_low") or market.get("low") or 0,
        "unit_cost": cost.get("locked_cost") or 0,
        "source": (raw.get("sources_used") or ["oracle"])[0],
    }
    # recommendation price becomes a reference ceiling (non-cost-basis)
    if rec.get("recommended_price"):
        flat["recommended_price"] = rec["recommended_price"]
    return flat


def enrich_pricing(quote: Quote, *, apply: bool = False) -> Quote:
    """Populate price recommendations on each line item.

    With apply=False (default): only fills reference fields
    (amazon_price, scprs_price, catalog_cost, confidence, source_url, supplier).
    The operator still has to confirm before unit_cost is set.

    With apply=True: also writes unit_cost from the highest-confidence
    cost-basis source (catalog/supplier > scprs ceiling). Subject to the
    3x sanity guard in Quote.set_price.

    Pricing oracle is imported lazily so the engine works in test/CI
    environments where the oracle DB isn't seeded.
    """
    try:
        from src.core.pricing_oracle_v2 import get_pricing
    except Exception as e:
        log.info("quote_engine.enrich_pricing: oracle unavailable (%s) — skipping", e)
        return quote

    for item in quote.line_items:
        if item.no_bid:
            continue
        try:
            raw = get_pricing(
                description=item.description,
                quantity=float(item.qty or 1),
                item_number=item.item_no or "",
            )
            rec = _oracle_recommendation_to_flat(raw)
        except Exception as e:
            log.warning("oracle lookup failed for line %d: %s", item.line_no, e)
            continue

        if not rec:
            continue

        # Reference fields (always populate; never overwrite an explicit value)
        if item.amazon_price == 0 and rec.get("amazon_price"):
            item.amazon_price = _to_decimal(rec["amazon_price"])
        if item.scprs_price == 0 and rec.get("scprs_price"):
            item.scprs_price = _to_decimal(rec["scprs_price"])
        if item.catalog_cost == 0 and rec.get("catalog_cost"):
            item.catalog_cost = _to_decimal(rec["catalog_cost"])
        if not item.source_url and rec.get("source_url"):
            item.source_url = rec["source_url"]
        if not item.supplier and rec.get("supplier"):
            item.supplier = rec["supplier"]
        if not item.asin and rec.get("asin"):
            item.asin = rec["asin"]
        if rec.get("confidence") is not None:
            item.confidence = float(rec["confidence"])

        if apply and item.unit_cost == 0:
            cost_basis = (
                rec.get("unit_cost")
                or rec.get("catalog_cost")
                or rec.get("supplier_cost")
            )
            if cost_basis:
                # Use set_price so the 3x sanity guard runs.
                quote.set_price(
                    line_no=item.line_no,
                    unit_cost=_to_decimal(cost_basis),
                    markup_pct=item.markup_pct,
                )
                item.price_source = rec.get("source", "oracle")

    return quote


# ── Step 3-5: Fill / Validate / Assemble (thin wrappers) ───────────────────

def fill_one(
    quote: Quote,
    profile_id: Optional[str] = None,
    pdf_hint: Optional[str] = None,
) -> bytes:
    """Fill a single form for the quote."""
    profile = pick_profile(quote, profile_id=profile_id, pdf_hint=pdf_hint)
    return fill(quote, profile)


def validate(
    pdf_bytes: bytes,
    quote: Quote,
    profile_id: Optional[str] = None,
    pdf_hint: Optional[str] = None,
) -> ValidationReport:
    """Read the filled PDF back through the same profile and diff."""
    profile = pick_profile(quote, profile_id=profile_id, pdf_hint=pdf_hint)
    return qa_validate(pdf_bytes, quote, profile)


# ── Step 6: Sign ───────────────────────────────────────────────────────────

def sign(pdf_bytes: bytes, signature_image_path: str = "") -> bytes:
    """Apply signature stamp + date and lock the PDF."""
    return approve_and_sign(pdf_bytes, signature_image_path=signature_image_path)


# ── One-shot helpers ───────────────────────────────────────────────────────

@dataclass
class DraftResult:
    """Editable single-form draft + its QA report."""
    pdf_bytes: bytes
    qa_report: ValidationReport
    profile_id: str

    @property
    def ok(self) -> bool:
        return self.qa_report.passed


def draft(
    quote: Quote,
    profile_id: Optional[str] = None,
    pdf_hint: Optional[str] = None,
    run_qa: bool = True,
) -> DraftResult:
    """Fill + QA a single form. Returns the editable draft.

    Use this for the operator-review step. The returned PDF is *not* signed
    or flattened — fields stay editable so the operator can tweak before
    sending. Call sign() on the bytes when ready to lock.
    """
    profile = pick_profile(quote, profile_id=profile_id, pdf_hint=pdf_hint)
    pdf_bytes = fill(quote, profile)
    if run_qa:
        report = qa_validate(pdf_bytes, quote, profile)
    else:
        report = ValidationReport(passed=True, profile_id=profile.id)
    return DraftResult(pdf_bytes=pdf_bytes, qa_report=report, profile_id=profile.id)


def finalize(
    quote: Quote,
    profile_ids: Optional[list[str]] = None,
    *,
    sign_after: bool = True,
    signature_image_path: str = "",
    run_qa: bool = True,
    merge: bool = True,
) -> PackageResult:
    """Full package assembly: fill all forms, QA all, merge, optionally sign.

    Args:
        profile_ids: explicit list of profiles to include. If None, uses the
            doc-type default (single 704A for PC; same for RFQ until 704B
            and 703B profiles land).
        sign_after: if True, applies the signature stamp to the merged PDF
            and replaces it in-place. Individual artifacts are left editable.

    The returned PackageResult.merged_pdf is what the route serves to the
    operator.
    """
    profiles_dict = get_profiles()

    if not profile_ids:
        default_id = _DEFAULT_PROFILE_BY_DOC_TYPE.get(quote.doc_type)
        profile_ids = [default_id] if default_id else []

    selected: list[FormProfile] = []
    missing: list[str] = []
    for pid in profile_ids:
        if pid in profiles_dict:
            selected.append(profiles_dict[pid])
        else:
            missing.append(pid)

    if missing:
        log.warning("quote_engine.finalize: skipping unknown profiles: %s", missing)

    if not selected:
        raise KeyError(
            f"No usable profiles for doc_type={quote.doc_type.value}, "
            f"requested={profile_ids}"
        )

    result = assemble(quote, selected, run_qa=run_qa, merge=merge)

    if missing:
        for pid in missing:
            result.warnings.append(f"Profile not found, skipped: {pid}")

    if sign_after and result.merged_pdf:
        try:
            result.merged_pdf = sign(result.merged_pdf, signature_image_path)
        except Exception as e:
            log.error("quote_engine.finalize: sign failed: %s", e, exc_info=True)
            result.warnings.append(f"Signing failed: {e}")
            result.ok = False

    return result


# ── Helpers ────────────────────────────────────────────────────────────────

def _to_decimal(val) -> "Decimal":  # noqa: F821 — annotation deferred
    from decimal import Decimal, InvalidOperation
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


# ── Boot wiring ────────────────────────────────────────────────────────────

def boot_validate_profiles(strict: bool = False) -> dict[str, list[str]]:
    """Validate every profile against its blank PDF.

    Called at app startup (see startup_checks.py). When strict=True, raises
    RuntimeError on any failure so the app refuses to serve traffic with
    a known-bad profile (per DESIGN_QUOTE_MODEL_V2 §"Boot-Time Validator").
    """
    from src.forms.profile_registry import validate_all_profiles
    results = validate_all_profiles()
    bad = {pid: issues for pid, issues in results.items() if issues}
    if bad and strict:
        lines = []
        for pid, issues in bad.items():
            lines.append(f"  {pid}:")
            for issue in issues:
                lines.append(f"    - {issue}")
        raise RuntimeError(
            "Profile validation failed — refusing to start:\n" + "\n".join(lines)
        )
    return results
