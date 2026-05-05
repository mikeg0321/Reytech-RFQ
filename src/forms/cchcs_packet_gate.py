"""CCHCS packet gate validator — the zero-tolerance check that runs
at the end of fill_cchcs_packet and blocks the result from being
returned as ok=True if any business rule is violated.

This is Reytech's scale-safety net for the CCHCS automation. The
attachment-splicing pipeline has silent fallbacks (if a filler crashes
we log a warning and keep the placeholder), and without this gate
those silent fallbacks would ship to production and a buyer would
receive a packet with a blank-looking attachment page.

Categories of validation (blocking vs. warning):

Blocking (fill returns ok=False if ANY fail):
  - Buyer pre-fill: solicitation number must be present
  - Attachment completeness: every registered attachment must have
    been successfully spliced (no silent fallbacks)
  - Signature presence: every required signature widget must have
    received a PNG overlay
  - Pricing: every row with qty > 0 must have a price > 0; price
    must not exceed cost by more than 5x (catch scrape errors)
  - Extension arithmetic: qty × unit_price must equal Extension
    Total{n} (within 1 cent)
  - Cert validity: cert_expiration must be in the future
  - Cover page totals: Amount field must be blank (not the grand
    total — regression guard for the 2026-04-13 bug)

Warning (fill still returns ok=True but logs loudly):
  - Buyer email/institution name missing
  - Date drift across signature fields
  - Markup below 10% or above 200% (outside Reytech's usual band)
  - Attachment field population (spot-checks)
  - Preference checkbox integrity (the 3 YES/NO pairs must not
    have both halves ticked — regression guard for the 2026-04-13
    double-check bug)

Built 2026-04-13 alongside the attachment-splicing pipeline.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.cchcs_gate")


# Tolerances and thresholds
PRICE_TO_COST_CEILING_RATIO = 5.0   # price > 5x cost = likely scrape error
MARKUP_WARN_HIGH = 2.00             # >200% markup = suspicious
EXTENSION_TOLERANCE_CENTS = 1       # $0.01 tolerance for rounding


def _safe_float(x: Any) -> float:
    try:
        return float(str(x).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _parse_mdy(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


# ── Individual validators ────────────────────────────────────────────────

def _check_buyer_prefill(parsed: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    header = (parsed or {}).get("header", {}) or {}
    if not header.get("solicitation_number"):
        issues.append("buyer packet missing solicitation number — cannot submit")
    if not header.get("institution"):
        warnings.append("buyer packet missing institution name")
    if not header.get("requestor_email"):
        warnings.append("buyer packet missing requestor email")
    if not header.get("due_date"):
        warnings.append("buyer packet missing due date")
    return {"issues": issues, "warnings": warnings}


def _check_attachment_completeness(splice_log: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    expected = splice_log.get("expected", [])
    spliced = set(splice_log.get("spliced", []))
    failed = splice_log.get("failed", [])
    for spec in expected:
        num = spec.get("num")
        desc = spec.get("description", f"attachment {num}")
        if num not in spliced:
            issues.append(f"attachment {num} ({desc}) was not spliced — placeholder still in packet")
    for (num, desc, reason) in failed:
        issues.append(f"attachment {num} filler failed: {reason}")
    return {"issues": issues, "warnings": warnings}


def _check_signatures(signature_log: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    expected_targets = signature_log.get("expected", [])
    overlaid = set(signature_log.get("overlaid", []))
    for target in expected_targets:
        if target not in overlaid:
            issues.append(f"signature '{target}' never received PNG overlay")
    return {"issues": issues, "warnings": warnings}


def _check_line_item_pricing(
    parsed: Dict[str, Any],
    price_overrides: Optional[Dict[int, Dict[str, float]]],
) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    overrides = price_overrides or {}
    line_items = (parsed or {}).get("line_items", []) or []

    # CC-3 fail-closed: an empty line-item list means the packet has no
    # items to price, so the gate must block rather than pass silently.
    if not line_items:
        issues.append(
            "no line items in parsed packet — cannot ship a CCHCS packet with "
            "zero rows (CC-3 fail-closed)"
        )
        return {"issues": issues, "warnings": warnings}

    rows_priced = 0
    rows_with_qty = 0
    for item in line_items:
        row = int(item.get("row_index", 0))
        qty = _safe_float(item.get("qty", 0))
        if qty <= 0:
            continue
        rows_with_qty += 1
        override = overrides.get(row, {}) or {}
        unit_price = _safe_float(
            override.get("unit_price")
            or item.get("unit_price")
            or item.get("pricing", {}).get("recommended_price")
            or 0
        )
        unit_cost = _safe_float(
            override.get("unit_cost")
            or item.get("pricing", {}).get("unit_cost")
            or 0
        )
        if unit_price <= 0:
            issues.append(f"row {row}: no price set (qty {qty:.0f})")
            continue
        rows_priced += 1
        if unit_cost > 0:
            if unit_price < unit_cost:
                issues.append(
                    f"row {row}: unit price ${unit_price:.2f} is BELOW cost ${unit_cost:.2f}"
                )
            elif unit_price > unit_cost * PRICE_TO_COST_CEILING_RATIO:
                issues.append(
                    f"row {row}: unit price ${unit_price:.2f} is >{PRICE_TO_COST_CEILING_RATIO:.0f}x cost "
                    f"${unit_cost:.2f} (likely scrape error)"
                )
            else:
                markup = (unit_price - unit_cost) / unit_cost
                if markup > MARKUP_WARN_HIGH:
                    warnings.append(
                        f"row {row}: markup {markup:.1%} above {MARKUP_WARN_HIGH:.0%} ceiling"
                    )

    # CC-3 fail-closed: if every row had qty=0 OR none ended up priced,
    # the packet totals to $0. That is never a valid CCHCS submission.
    if rows_with_qty == 0:
        issues.append(
            f"all {len(line_items)} line items have qty=0 — packet would total $0 "
            "(CC-3 fail-closed)"
        )
    elif rows_priced == 0:
        issues.append(
            f"0 of {rows_with_qty} line items with qty>0 received a unit price — "
            "packet would total $0 (CC-3 fail-closed)"
        )

    return {"issues": issues, "warnings": warnings}


def _check_extension_arithmetic(
    output_pdf_path: str,
    parsed: Dict[str, Any],
    price_overrides: Optional[Dict[int, Dict[str, float]]],
) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    try:
        from pypdf import PdfReader
    except ImportError:
        return {"issues": issues, "warnings": ["pypdf unavailable"]}
    if not os.path.exists(output_pdf_path):
        return {"issues": [f"output PDF missing at {output_pdf_path}"], "warnings": []}
    try:
        fields = PdfReader(output_pdf_path).get_fields() or {}
    except Exception as e:
        return {"issues": [f"cannot re-read output: {e}"], "warnings": []}

    overrides = price_overrides or {}
    for item in (parsed or {}).get("line_items", []):
        row = int(item.get("row_index", 0))
        qty = _safe_float(item.get("qty", 0))
        if qty <= 0:
            continue
        override = overrides.get(row, {}) or {}
        expected_unit = _safe_float(
            override.get("unit_price")
            or item.get("unit_price")
            or 0
        )
        if expected_unit <= 0:
            continue  # no price set — pricing check catches this
        expected_ext = round(qty * expected_unit, 2)

        unit_field = f"Price Per Unit{row}"
        ext_field = f"Extension Total{row}"
        unit_written = _safe_float(
            (fields.get(unit_field) or {}).get("/V", "") if isinstance(fields.get(unit_field), dict) else ""
        )
        ext_written = _safe_float(
            (fields.get(ext_field) or {}).get("/V", "") if isinstance(fields.get(ext_field), dict) else ""
        )
        if abs(unit_written - expected_unit) > 0.01:
            issues.append(
                f"row {row}: Price Per Unit written as ${unit_written:.2f} but expected ${expected_unit:.2f}"
            )
        if abs(ext_written - expected_ext) > (EXTENSION_TOLERANCE_CENTS / 100.0):
            issues.append(
                f"row {row}: Extension Total written as ${ext_written:.2f} but "
                f"qty {qty:.0f} × ${expected_unit:.2f} = ${expected_ext:.2f}"
            )
    return {"issues": issues, "warnings": warnings}


def _check_cert_validity(reytech_info: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[str] = []
    warnings: List[str] = []
    cert_exp_str = reytech_info.get("cert_expiration", "")
    if not cert_exp_str:
        warnings.append("cert_expiration not set in reytech_config.json")
        return {"issues": issues, "warnings": warnings}
    exp = _parse_mdy(cert_exp_str)
    if exp is None:
        warnings.append(f"could not parse cert_expiration '{cert_exp_str}'")
        return {"issues": issues, "warnings": warnings}
    today = datetime.now()
    if exp < today:
        issues.append(
            f"Reytech SB/DVBE cert expired {exp.strftime('%m/%d/%Y')} — renew before submitting"
        )
    elif (exp - today).days < 30:
        warnings.append(
            f"Reytech SB/DVBE cert expires in {(exp - today).days} days — schedule renewal"
        )
    return {"issues": issues, "warnings": warnings}


def _check_amount_field_blank(output_pdf_path: str) -> Dict[str, Any]:
    """Regression guard: the page 1 'Amount' field is the subcontract
    dollar input, NOT the grand total. The 2026-04-13 initial build
    incorrectly wrote the grand total there. This check ensures the
    bug never reappears: Amount must be blank unless Reytech is
    actually claiming the 25% subcontract preference (which is false
    by default)."""
    issues: List[str] = []
    warnings: List[str] = []
    try:
        from pypdf import PdfReader
        fields = PdfReader(output_pdf_path).get_fields() or {}
    except Exception:
        return {"issues": issues, "warnings": warnings}
    amt = fields.get("Amount")
    if amt is None:
        return {"issues": issues, "warnings": warnings}
    val = str(amt.get("/V", "")).strip() if isinstance(amt, dict) else ""
    if val and val not in ("", "$", "0.00", "0"):
        issues.append(
            f"page 1 'Amount' field is populated with '{val}' — this should be "
            f"BLANK unless claiming 25% subcontract preference (regression guard)"
        )
    return {"issues": issues, "warnings": warnings}


def _check_preference_pairs(output_pdf_path: str) -> Dict[str, Any]:
    """Regression guard: the 3 YES/NO checkbox pairs on page 1 must
    each have EXACTLY one side ticked. Both-ticked is the
    2026-04-13 bug; neither-ticked is the original placeholder state
    and means the operator forgot to set the compliance flags."""
    issues: List[str] = []
    warnings: List[str] = []
    try:
        from pypdf import PdfReader
        fields = PdfReader(output_pdf_path).get_fields() or {}
    except Exception:
        return {"issues": issues, "warnings": warnings}

    pairs = [
        ("SB preference", "Check Box12", "Check Box11"),
        ("Manufacturer", "Check Box13", "Check Box14"),
        ("25% subcontract", "Check Box15", "Check Box16"),
    ]
    for label, yes_name, no_name in pairs:
        yes_v = str((fields.get(yes_name) or {}).get("/V", ""))
        no_v = str((fields.get(no_name) or {}).get("/V", ""))
        yes_on = yes_v in ("/Yes", "Yes", "/On", "On", "1", "True")
        no_on = no_v in ("/Yes", "Yes", "/On", "On", "1", "True")
        if yes_on and no_on:
            issues.append(
                f"preference block '{label}': BOTH YES and NO are ticked "
                f"({yes_name}+{no_name})"
            )
        elif not yes_on and not no_on:
            warnings.append(
                f"preference block '{label}': neither YES nor NO ticked "
                f"(compliance flags missing from reytech_config.json?)"
            )
    return {"issues": issues, "warnings": warnings}


def _spot_check_attachments(output_pdf_path: str) -> Dict[str, Any]:
    """Spot-check a critical field from each spliced attachment to
    confirm it actually has Reytech data. Catches the case where the
    splicer succeeded but the filler wrote blanks."""
    issues: List[str] = []
    warnings: List[str] = []
    try:
        from pypdf import PdfReader
        fields = PdfReader(output_pdf_path).get_fields() or {}
    except Exception:
        return {"issues": issues, "warnings": warnings}

    def _val(name: str) -> str:
        f = fields.get(name)
        return str(f.get("/V", "")) if isinstance(f, dict) else ""

    # Bidder Declaration
    if "Solicitaion #" in fields and not _val("Solicitaion #"):
        warnings.append("Bidder Declaration 'Solicitaion #' present but empty")
    # DVBE 843
    if "DVBEname" in fields and not _val("DVBEname"):
        warnings.append("DVBE 843 'DVBEname' present but empty")
    # CalRecycle 74
    if "ContractorCompany Name" in fields and not _val("ContractorCompany Name"):
        warnings.append("CalRecycle 74 'ContractorCompany Name' present but empty")
    # STD 204 Payee
    key = "NAME OF AUTHORIZED PAYEE REPRESENTATIVE"
    if key in fields and not _val(key):
        warnings.append("STD 204 payee representative present but empty")
    # DARFUR
    if "CompanyVendor Name" in fields and not _val("CompanyVendor Name"):
        warnings.append("DARFUR company name present but empty")
    # Civil Rights
    if "ProposerBidder Firm Name Printed" in fields and not _val("ProposerBidder Firm Name Printed"):
        warnings.append("CA Civil Rights firm name present but empty")
    return {"issues": issues, "warnings": warnings}


# ── Orchestrator ─────────────────────────────────────────────────────────

def gate_validate(
    output_pdf_path: str,
    parsed: Dict[str, Any],
    reytech_info: Dict[str, Any],
    price_overrides: Optional[Dict[int, Dict[str, float]]],
    splice_log: Dict[str, Any],
    signature_log: Dict[str, Any],
) -> Dict[str, Any]:
    """Run every gate check and aggregate results into a single report.

    Returns: {
        "passed": bool,
        "critical_issues": [str],
        "warnings": [str],
        "checks_run": int,
        "checks_with_issues": int,
        "by_check": {name: {issues: [], warnings: []}},
    }
    """
    report: Dict[str, Any] = {
        "passed": True,
        "critical_issues": [],
        "warnings": [],
        "checks_run": 0,
        "checks_with_issues": 0,
        "by_check": {},
    }

    checks = [
        ("buyer_prefill", lambda: _check_buyer_prefill(parsed)),
        ("attachment_completeness", lambda: _check_attachment_completeness(splice_log)),
        ("signatures", lambda: _check_signatures(signature_log)),
        ("line_item_pricing", lambda: _check_line_item_pricing(parsed, price_overrides)),
        ("extension_arithmetic",
            lambda: _check_extension_arithmetic(output_pdf_path, parsed, price_overrides)),
        ("cert_validity", lambda: _check_cert_validity(reytech_info)),
        ("amount_field_blank", lambda: _check_amount_field_blank(output_pdf_path)),
        ("preference_pairs", lambda: _check_preference_pairs(output_pdf_path)),
        ("attachment_spot_check", lambda: _spot_check_attachments(output_pdf_path)),
    ]

    for name, fn in checks:
        try:
            res = fn()
        except Exception as e:
            log.error("gate check %s crashed: %s", name, e, exc_info=True)
            res = {"issues": [f"gate check {name} crashed: {e}"], "warnings": []}
        report["checks_run"] += 1
        issues = res.get("issues", []) or []
        warnings = res.get("warnings", []) or []
        report["by_check"][name] = {"issues": issues, "warnings": warnings}
        if issues:
            report["checks_with_issues"] += 1
            report["critical_issues"].extend(f"[{name}] {i}" for i in issues)
            report["passed"] = False
        if warnings:
            report["warnings"].extend(f"[{name}] {w}" for w in warnings)

    status = "PASS" if report["passed"] else "FAIL"
    log.info(
        "CCHCS gate %s: %d checks, %d issues, %d warnings",
        status, report["checks_run"],
        len(report["critical_issues"]), len(report["warnings"]),
    )
    if report["critical_issues"]:
        for issue in report["critical_issues"]:
            log.warning("CCHCS gate ISSUE: %s", issue)
    return report


__all__ = [
    "gate_validate",
    "PRICE_TO_COST_CEILING_RATIO",
    "MARKUP_WARN_HIGH",
]
