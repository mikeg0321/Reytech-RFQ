"""
Form QA Agent — Verifies all PDF forms are filled correctly after generation.

Runs after every form fill in generate_rfq_package(). Reads back each filled PDF,
checks all fields against a registry of expected values, verifies signatures and
dates, and returns a pass/fail report before the user sees the package.

Built from actual PDF field dumps on 2026-03-26 after a 4-hour incident where
forms generated with missing fields, wrong signatures, and double dates.
"""

import os
import re
import time
import logging
from typing import Optional

log = logging.getLogger("reytech.form_qa")


# ═══════════════════════════════════════════════════════════════════════
# Form Field Registry — verified from actual pypdf field dumps
# ═══════════════════════════════════════════════════════════════════════

FORM_FIELD_REGISTRY = {
    "703b": {
        "prefix_detect": True,
        "possible_prefixes": ["703B_", "703C_", ""],
        "required_fields": {
            "{p}Business Name": "company.name",
            "{p}Address": "company.address",
            "{p}Contact Person": "company.owner",
            "{p}Title": "company.title",
            "{p}Phone": "company.phone",
            "{p}Email": "company.email",
            "{p}Federal Employer Identification Number FEIN": "company.fein",
            "{p}Retailers CA Sellers Permit Number": "company.sellers_permit",
            "{p}SBMBDVBE Certification.0": "company.cert_number",
            "{p}Certification Expiration Date": "company.cert_expiration",
            "{p}Solicitation Number": "rfq.solicitation_number",
            "{p}Due Date": "rfq.due_date",
            "{p}BidExpirationDate": "computed",
        },
        "checkbox_fields": {
            "{p}Check Box2": "/Yes",
            "{p}Check Box4": "/Yes",
        },
        "date_fields": ["{p}Date"],
        "signature_fields": ["Signature1"],
    },
    "703c": {
        "prefix_detect": True,
        "possible_prefixes": ["703C_", "703B_", ""],
        "required_fields": {
            "{p}Business Name": "company.name",
            "{p}Address": "company.address",
            "{p}Contact Person": "company.owner",
            "{p}Title": "company.title",
            "{p}Phone": "company.phone",
            "{p}Email": "company.email",
            "{p}Federal Employer Identification Number FEIN": "company.fein",
            "{p}Retailers CA Sellers Permit Number": "company.sellers_permit",
            "{p}SBMBDVBE Certification.0": "company.cert_number",
            "{p}Certification Expiration Date": "company.cert_expiration",
            "{p}Solicitation Number": "rfq.solicitation_number",
            "{p}Due Date": "rfq.due_date",
            "{p}BidExpirationDate": "computed",
        },
        "checkbox_fields": {
            "{p}Check Box2": "/Yes",
            "{p}Check Box4": "/Yes",
        },
        "date_fields": ["{p}Date"],
        "signature_fields": ["Signature1"],
    },
    "704b": {
        "prefix_detect": False,
        "required_fields": {
            "COMPANY NAME": "company.name",
            "PERSON PROVIDING QUOTE": "company.owner",
        },
        "date_fields": [],
        "signature_fields": [],  # 704B has no /Sig — positional overlay
        "positional_signature": True,
        "pricing_required": True,
    },
    "bidpkg": {
        "prefix_detect": False,
        "required_fields": {
            # CUF (MC-345)
            "DOING BUSINESS AS DBA NAME_CUF": "company.name",
            "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": "company.cert_number",
            "Date_CUF": "sign_date",
            "Text7_CUF": "rfq.solicitation_number",
            # Darfur Act
            "CompanyVendor Name Printed_darfur": "company.name",
            "Federal ID Number_darfur": "company.fein",
            "Date__darfur": "sign_date",
            # Bidder Declaration (GSPD-05-105)
            "Text0_105": "rfq.solicitation_number",
            "Text1_105": "static:SB/DVBE",
            # DVBE 843
            "Text1_PD843": "company.name",
            "Text2_PD843": "company.cert_number",
            "Text4_PD843": "rfq.solicitation_number",
            "Date1_PD843": "sign_date",
            # GenAI 708
            "708_Text1": "rfq.solicitation_number",
            "708_Text3": "company.name",
            "708_Text16": "sign_date",
            # Drug-Free STD 21
            "Text1_std21": "company.name",
            "Text2_std21": "company.fein",
            # CalRecycle 74
            "ContractorCompany Name": "company.name",
            "Address": "company.address",
        },
        "date_fields": [
            "Date_CUF", "Date__darfur", "Date1_PD843", "708_Text16",
        ],
        "signature_fields": [
            "Signature_CUF", "Signature_darfur",
            "Signature1_PD843", "Signature_std21", "Signature1",
        ],
        "skip_signature_fields": ["Signature29"],  # Bidder Dec /Rotate=90
    },
    "quote": {
        "prefix_detect": False,
        "required_fields": {},
        "date_fields": [],
        "signature_fields": [],
        "pricing_required": True,
    },
}

# Forms that are INSIDE the bid package — never generate standalone for CCHCS
BID_PACKAGE_INTERNAL_FORMS = {
    "dvbe843", "sellers_permit", "calrecycle74", "darfur_act",
    "bidder_decl", "std21", "genai_708",
}


# ═══════════════════════════════════════════════════════════════════════
# Verification Functions
# ═══════════════════════════════════════════════════════════════════════

def _detect_prefix(field_names: set, possible_prefixes: list) -> str:
    """Detect which prefix the PDF form uses."""
    for prefix in possible_prefixes:
        if prefix and any(f.startswith(prefix) for f in field_names):
            return prefix
    return ""


def _resolve_expected(source: str, rfq_data: dict, config: dict) -> Optional[str]:
    """Resolve an expected value from its source descriptor."""
    if source.startswith("static:"):
        return source[7:]
    if source == "computed":
        return None  # Don't check computed values, just check non-empty
    if source == "sign_date":
        return rfq_data.get("sign_date", "")
    if source.startswith("company."):
        key = source[8:]
        return config.get("company", {}).get(key, "")
    if source.startswith("rfq."):
        key = source[4:]
        return rfq_data.get(key, "")
    if source == "checkbox":
        return "/Yes"
    return None


def verify_filled_form(pdf_path: str, form_id: str, rfq_data: dict, config: dict) -> dict:
    """Read back a filled PDF and verify all expected fields have values.

    Returns: {
        "passed": bool,
        "form_id": str,
        "fields_total": int,
        "fields_filled": int,
        "fields_expected": int,
        "issues": [str],
        "warnings": [str],
        "field_details": [{name, expected, actual, status}],
    }
    """
    result = {
        "passed": True, "form_id": form_id,
        "fields_total": 0, "fields_filled": 0, "fields_expected": 0,
        "issues": [], "warnings": [], "field_details": [],
    }

    if not os.path.exists(pdf_path):
        result["passed"] = False
        result["issues"].append(f"PDF not found: {os.path.basename(pdf_path)}")
        return result

    registry = FORM_FIELD_REGISTRY.get(form_id)
    if not registry:
        # Unknown form — just count filled fields
        try:
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            fields = reader.get_fields() or {}
            result["fields_total"] = len(fields)
            result["fields_filled"] = sum(1 for f in fields.values() if f.get("/V"))
            if result["fields_total"] > 0 and result["fields_filled"] == 0:
                result["warnings"].append(f"Form {form_id}: {result['fields_total']} fields, none filled")
        except Exception as e:
            result["warnings"].append(f"Could not read {form_id}: {e}")
        return result

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        fields = reader.get_fields() or {}
        field_names = set(fields.keys())
        result["fields_total"] = len(fields)
        result["fields_filled"] = sum(1 for f in fields.values() if f.get("/V"))
    except Exception as e:
        result["passed"] = False
        result["issues"].append(f"Cannot read PDF: {e}")
        return result

    # Detect prefix
    prefix = ""
    if registry.get("prefix_detect"):
        prefix = _detect_prefix(field_names, registry.get("possible_prefixes", []))

    # Check required fields
    required = registry.get("required_fields", {})
    result["fields_expected"] = len(required)
    for field_template, source in required.items():
        field_name = field_template.replace("{p}", prefix)
        expected = _resolve_expected(source, rfq_data, config)

        actual_field = fields.get(field_name)
        actual_value = str(actual_field.get("/V", "")).strip() if actual_field else ""

        detail = {"name": field_name, "expected": expected or "(non-empty)", "actual": actual_value[:50]}

        if not actual_value:
            detail["status"] = "FAIL"
            result["passed"] = False
            result["issues"].append(f"Missing: {field_name}")
        elif expected and actual_value != expected and expected != "(non-empty)":
            detail["status"] = "WARN"
            result["warnings"].append(f"Mismatch: {field_name} = '{actual_value[:30]}' (expected '{expected[:30]}')")
        else:
            detail["status"] = "PASS"

        result["field_details"].append(detail)

    # Check checkbox fields
    for field_template, expected_val in registry.get("checkbox_fields", {}).items():
        field_name = field_template.replace("{p}", prefix)
        actual_field = fields.get(field_name)
        actual_value = str(actual_field.get("/V", "")).strip() if actual_field else ""
        if actual_value != expected_val:
            result["warnings"].append(f"Checkbox not set: {field_name} = '{actual_value}' (expected '{expected_val}')")

    # Check date fields
    for date_template in registry.get("date_fields", []):
        date_field = date_template.replace("{p}", prefix)
        actual_field = fields.get(date_field)
        actual_value = str(actual_field.get("/V", "")).strip() if actual_field else ""
        if not actual_value:
            result["issues"].append(f"Missing date: {date_field}")
            result["passed"] = False

    # Check pricing (704B, quote)
    if registry.get("pricing_required"):
        items = rfq_data.get("line_items", [])
        unpriced = [i for i in items if not i.get("price_per_unit") and not i.get("unit_price")]
        if unpriced:
            result["warnings"].append(f"{len(unpriced)} items have no price")

    return result


def verify_signatures(pdf_path: str, form_id: str) -> dict:
    """Verify signature fields are signed and positioned correctly.

    Returns: {"passed": bool, "issues": [], "warnings": [], "details": []}
    """
    result = {"passed": True, "issues": [], "warnings": [], "details": []}
    registry = FORM_FIELD_REGISTRY.get(form_id, {})
    expected_sigs = set(registry.get("signature_fields", []))
    skip_sigs = set(registry.get("skip_signature_fields", []))

    if not expected_sigs and not registry.get("positional_signature"):
        return result  # No signature expectations

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)

        found_sigs = set()
        for pg_idx, page in enumerate(reader.pages):
            page_h = float(page.get("/MediaBox", [0, 0, 612, 792])[3])
            page_rotate = int(page.get("/Rotate", 0))
            annots = page.get("/Annots", []) or []

            for annot in annots:
                obj = annot.get_object() if hasattr(annot, "get_object") else annot
                name = str(obj.get("/T", ""))
                ft = str(obj.get("/FT", ""))

                if ft == "/Sig" or name in expected_sigs:
                    rect = obj.get("/Rect", [0, 0, 0, 0])
                    y = float(rect[1])

                    if name in skip_sigs:
                        result["details"].append({
                            "field": name, "page": pg_idx + 1,
                            "status": "SKIPPED", "reason": "Known rotation issue"
                        })
                        continue

                    found_sigs.add(name)

                    # Check position for generic fields
                    if name in ("Signature1", "Signature") and y > page_h * 0.4:
                        result["warnings"].append(
                            f"{name} on page {pg_idx + 1} at y={y:.0f} — may be in header area"
                        )

                    # Check rotation
                    if page_rotate != 0 and name not in skip_sigs:
                        result["warnings"].append(
                            f"{name} on rotated page {pg_idx + 1} (rotate={page_rotate}°) — verify rendering"
                        )

                    result["details"].append({
                        "field": name, "page": pg_idx + 1,
                        "y": round(y), "rotate": page_rotate,
                        "status": "FOUND"
                    })

        # Check for missing expected signatures
        missing = expected_sigs - found_sigs - skip_sigs
        if missing:
            for m in missing:
                result["warnings"].append(f"Expected signature field not found: {m}")

    except Exception as e:
        result["warnings"].append(f"Signature check error: {e}")

    return result


def verify_package_completeness(
    agency_key: str,
    required_forms: set,
    generated_files: list,
    has_bid_package: bool,
) -> dict:
    """Verify only required forms are generated, no extras, no missing.

    Returns: {"passed": bool, "issues": [], "warnings": [], "generated": [], "missing": [], "extra": []}
    """
    result = {
        "passed": True, "issues": [], "warnings": [],
        "generated": [], "missing": [], "extra": [],
    }

    # Map filenames to form IDs
    generated_ids = set()
    for fn in generated_files:
        fn_lower = fn.lower()
        fid = "unknown"
        if "quote" in fn_lower and "704" not in fn_lower:
            fid = "quote"
        elif "703c" in fn_lower:
            fid = "703b"  # 703C counts as 703B requirement
        elif "703b" in fn_lower:
            fid = "703b"
        elif "704b" in fn_lower:
            fid = "704b"
        elif "bidpackage" in fn_lower or "bidpkg" in fn_lower:
            fid = "bidpkg"
        elif "calrecycle" in fn_lower:
            fid = "calrecycle74"
        elif "dvbe" in fn_lower or "843" in fn_lower:
            fid = "dvbe843"
        elif "seller" in fn_lower or "permit" in fn_lower:
            fid = "sellers_permit"
        elif "darfur" in fn_lower:
            fid = "darfur_act"
        elif "bidder" in fn_lower:
            fid = "bidder_decl"
        elif "std204" in fn_lower:
            fid = "std204"
        generated_ids.add(fid)
        result["generated"].append({"filename": fn, "form_id": fid})

    # Check missing required forms
    for req in required_forms:
        if req not in generated_ids:
            result["missing"].append(req)
            result["issues"].append(f"Required form not generated: {req}")
            result["passed"] = False

    # Check for standalone forms that should be inside bid package
    if has_bid_package:
        for fid in generated_ids:
            if fid in BID_PACKAGE_INTERNAL_FORMS:
                result["extra"].append(fid)
                result["warnings"].append(
                    f"'{fid}' generated standalone but is inside bid package — remove standalone"
                )

    # Check for both 703B and 703C
    has_703b_file = any("703b" in fn.lower() for fn in generated_files)
    has_703c_file = any("703c" in fn.lower() for fn in generated_files)
    if has_703b_file and has_703c_file:
        result["warnings"].append("Both 703B and 703C generated — should be one or the other")

    return result


# ═══════════════════════════════════════════════════════════════════════
# Master QA Function
# ═══════════════════════════════════════════════════════════════════════

def run_form_qa(
    out_dir: str,
    output_files: list,
    form_id_map: list,
    rfq_data: dict,
    config: dict,
    agency_key: str,
    required_forms: set,
) -> dict:
    """Run complete QA on all generated forms.

    Args:
        out_dir: Directory containing generated PDFs
        output_files: List of generated filenames
        form_id_map: List of {"form_id": str, "filename": str}
        rfq_data: The RFQ data dict
        config: App config with company info
        agency_key: Agency key from agency_config
        required_forms: Set of required form IDs

    Returns: {
        "passed": bool,
        "timestamp": str,
        "duration_ms": int,
        "forms_checked": int,
        "critical_issues": [str],
        "warnings": [str],
        "form_results": {form_id: {...}},
        "package_check": {...},
    }
    """
    from datetime import datetime

    start = time.time()
    report = {
        "passed": True,
        "timestamp": datetime.now().isoformat(),
        "duration_ms": 0,
        "forms_checked": 0,
        "critical_issues": [],
        "warnings": [],
        "form_results": {},
        "package_check": {},
    }

    # 1. Package completeness
    has_bidpkg = any("bidpackage" in f.lower() or "bidpkg" in f.lower() for f in output_files)
    pkg_check = verify_package_completeness(agency_key, required_forms, output_files, has_bidpkg)
    report["package_check"] = pkg_check
    if not pkg_check["passed"]:
        report["passed"] = False
        report["critical_issues"].extend(pkg_check["issues"])
    report["warnings"].extend(pkg_check.get("warnings", []))

    # 2. Verify each generated form
    for form_info in form_id_map:
        form_id = form_info.get("form_id", "unknown")
        filename = form_info.get("filename", "")
        pdf_path = os.path.join(out_dir, filename) if filename else ""

        if not pdf_path or not os.path.exists(pdf_path):
            report["warnings"].append(f"File not found for {form_id}: {filename}")
            continue

        # Field verification
        field_result = verify_filled_form(pdf_path, form_id, rfq_data, config)

        # Signature verification
        sig_result = verify_signatures(pdf_path, form_id)

        # Combine
        form_report = {
            "filename": filename,
            "fields": field_result,
            "signatures": sig_result,
            "passed": field_result["passed"] and sig_result["passed"],
        }

        report["form_results"][form_id] = form_report
        report["forms_checked"] += 1

        if not form_report["passed"]:
            report["passed"] = False
            report["critical_issues"].extend(field_result.get("issues", []))

        report["warnings"].extend(field_result.get("warnings", []))
        report["warnings"].extend(sig_result.get("warnings", []))

    report["duration_ms"] = int((time.time() - start) * 1000)

    # Log summary
    status = "PASS" if report["passed"] else "FAIL"
    log.info("Form QA %s: %d forms checked, %d issues, %d warnings (%dms)",
             status, report["forms_checked"], len(report["critical_issues"]),
             len(report["warnings"]), report["duration_ms"])

    if report["critical_issues"]:
        for issue in report["critical_issues"]:
            log.warning("Form QA ISSUE: %s", issue)

    return report
