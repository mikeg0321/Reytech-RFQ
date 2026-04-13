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
    # ── Standalone forms (field names from actual template PDF dumps) ──
    "calrecycle74": {
        "prefix_detect": False,
        "required_fields": {
            "ContractorCompany Name": "company.name",
            "Address": "company.address",
            "Phone": "company.phone",
            "Email": "company.email",
            "Print Name": "company.owner",
            "Title": "company.title",
            "State Agency": "rfq.agency",
        },
        "date_fields": ["Date"],
        "signature_fields": ["Signature"],
    },
    "darfur": {
        "prefix_detect": False,
        "required_fields": {
            "CompanyVendor Name": "company.name",
            "Federal ID Number": "company.fein",
            "Printed Name and Title of Person Signing": "company.owner",
        },
        "date_fields": ["Date of signature"],
        "signature_fields": ["Authorized Signature"],
    },
    "cv012_cuf": {
        "prefix_detect": False,
        "required_fields": {
            "form1[0].#subform[0].DoingBusinessAs[0]": "company.name",
            "form1[0].#subform[0].OSDSRefNumber[0]": "company.cert_number",
            "form1[0].#subform[0].SolicitationNumber[0]": "rfq.solicitation_number",
            "form1[0].#subform[1].PrintedName[0]": "company.owner",
            "form1[0].#subform[1].Title[0]": "company.title",
        },
        "date_fields": ["form1[0].#subform[1].Date[0]"],
        "signature_fields": ["form1[0].#subform[1].AuthorizedRepresentative[0]"],
    },
    "std204": {
        "prefix_detect": False,
        "required_fields": {
            "NAME (This is required. Do not leave this line blank. Must match the payee\u2019s federal tax return)": "company.name",
            "Federal Employer Identification Number (FEIN)": "company.fein",
            "MAILING ADDRESS (number, street, apt. or suite no.) (See instructions on Page 2)": "company.address",
            "NAME OF AUTHORIZED PAYEE REPRESENTATIVE": "company.owner",
            "TITLE": "company.title",
        },
        "date_fields": ["DATE"],
        "signature_fields": ["Signature4"],
    },
    "std1000": {
        "prefix_detect": False,
        "required_fields": {
            "Business Name": "company.name",
            "Business Address": "company.address",
            "Business Telephone Number": "company.phone",
            "Solicitation  Contract Number": "rfq.solicitation_number",
        },
        "date_fields": ["Date"],
        "signature_fields": ["Signature"],
    },
    "bidder_decl": {
        "prefix_detect": False,
        "required_fields": {
            "Solicitaion #": "rfq.solicitation_number",
            "Text1": "static:SB/DVBE",
        },
        "date_fields": [],
        "signature_fields": [],  # Sig is on rotated page — skip for QA
    },
    # CCHCS Non-IT RFQ Packet — consolidated 22-page output from the
    # cchcs_packet_filler. The registry here only checks the cover-page
    # supplier info block; deep attachment validation is handled by
    # src/forms/cchcs_packet_gate.py which runs first and blocks on
    # business-rule violations. This registry entry gives Reytech's
    # existing Form QA Agent a second opinion on the supplier fields
    # using the same code path as every other form in the system.
    "cchcs_packet": {
        "prefix_detect": False,
        "required_fields": {
            "Supplier Name": "company.name",
            "Contact Name": "company.owner",
            "Phone": "company.phone",
            "Supplier Email": "company.email",
            "SBMBDVBE Certification  if applicable": "company.cert_number",
            "Expiration Date": "company.cert_expiration",
            "Solicitation No": "rfq.solicitation_number",
        },
        "checkbox_fields": {
            # SB preference YES side must be ticked
            "Check Box12": "/Yes",
            # Manufacturer NO side must be ticked
            "Check Box14": "/Yes",
            # 25% subcontract NO side must be ticked
            "Check Box16": "/Yes",
        },
        "date_fields": ["Date_es_:date"],
        # Signatures are handled by the gate's signature_log check,
        # not here — form_qa's signature verifier doesn't understand
        # PNG overlays merged into page content streams.
        "signature_fields": [],
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

    # Per-page field tracking
    try:
        page_stats = []
        for pg_idx, page in enumerate(reader.pages):
            annots = page.get("/Annots", []) or []
            pg_total = 0
            pg_filled = 0
            for annot in annots:
                obj = annot.get_object() if hasattr(annot, "get_object") else annot
                ft = str(obj.get("/FT", ""))
                if ft in ("/Tx", "/Btn", "/Ch", "/Sig"):
                    pg_total += 1
                    val = obj.get("/V")
                    if val and str(val).strip():
                        pg_filled += 1
            page_stats.append({
                "page": pg_idx + 1,
                "total_fields": pg_total,
                "filled_fields": pg_filled,
            })
            # Warn if page has fields but none filled
            if pg_total > 0 and pg_filled == 0:
                result["warnings"].append(
                    f"Page {pg_idx + 1} has {pg_total} form fields but none are filled"
                )
        result["page_stats"] = page_stats

        # 704B-specific: if items overflow to page 2, verify page 2 has pricing
        if form_id == "704b" and len(page_stats) >= 2:
            items = rfq_data.get("line_items", rfq_data.get("items", []))
            if isinstance(items, list) and len(items) > 11 and page_stats[1]["filled_fields"] == 0:
                result["warnings"].append(
                    f"704B has {len(items)} items (overflow to page 2) but page 2 has no filled fields"
                )
    except Exception as _e:
        result["warnings"].append(f"Per-page tracking error: {_e}")

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

        # Check for actual signature IMAGE on pages that should be signed
        if expected_sigs or registry.get("positional_signature"):
            for pg_idx, page in enumerate(reader.pages):
                resources = page.get("/Resources", {})
                xobjects = resources.get("/XObject", {})
                has_image = False
                if hasattr(xobjects, "get_object"):
                    xobjects = xobjects.get_object()
                for xname, xobj in (xobjects.items() if isinstance(xobjects, dict) else []):
                    try:
                        obj = xobj.get_object() if hasattr(xobj, "get_object") else xobj
                        if str(obj.get("/Subtype", "")) == "/Image":
                            has_image = True
                            break
                    except Exception:
                        pass

                # Only flag pages in the lower half where signatures live
                page_h = float(page.get("/MediaBox", [0, 0, 612, 792])[3])
                # Check if this page has a signature annotation
                page_has_sig = any(
                    d.get("page") == pg_idx + 1 and d.get("status") == "FOUND"
                    for d in result["details"]
                )
                if page_has_sig and not has_image:
                    result["warnings"].append(
                        f"Page {pg_idx + 1} has signature field but no image XObject — "
                        f"signature may not have rendered"
                    )

    except Exception as e:
        result["warnings"].append(f"Signature check error: {e}")

    return result


def verify_signature_file_exists(config: dict) -> dict:
    """Pre-flight: check that the signature image file exists.

    Returns: {"passed": bool, "path": str, "issue": str or None}
    """
    sig_path = config.get("signature_image", "")
    if not sig_path:
        # Try common locations
        from src.core.paths import DATA_DIR
        for candidate in ["signature_transparent.png", "signature.png"]:
            p = os.path.join(DATA_DIR, candidate)
            if os.path.exists(p):
                return {"passed": True, "path": p, "issue": None}
            # Check project root
            root_p = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), candidate)
            if os.path.exists(root_p):
                return {"passed": True, "path": root_p, "issue": None}
        return {"passed": False, "path": "", "issue": "Signature image file not found"}

    if os.path.exists(sig_path):
        return {"passed": True, "path": sig_path, "issue": None}
    return {"passed": False, "path": sig_path, "issue": f"Signature image not found: {sig_path}"}


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
# Requirements Validation (Email = Contract)
# ═══════════════════════════════════════════════════════════════════════

def validate_against_requirements(
    generated_files: list,
    requirements_json: str,
    rfq_data: dict = None,
    config: dict = None,
    strict: bool = False,
) -> dict:
    """Check that generated package satisfies email-extracted requirements.

    This is Phase 2 of the Email-as-Contract pipeline: the buyer email
    is treated as a binding spec, and we verify the generated package
    covers every requirement they listed (forms, certifications, due
    dates, delivery constraints) BEFORE allowing the package to ship.

    Args:
        generated_files: List of generated PDF filenames.
        requirements_json: JSON string or dict of RFQRequirements.to_dict().
        rfq_data: Full RFQ dict (for due_date, agency, etc.).
        config: App config with company info (cert number, expiration, etc.).
            When present, enables cert validity checks.
        strict: If True, upgrade certain gaps from "warning" to "critical"
            severity so `passed=False` and callers block the send. When
            False (default), all gaps stay as warnings and the caller
            sees them in the report but isn't blocked.

    Returns:
        {
            "passed": bool,
            "gaps": [{type, form_id, msg, severity}],
            "confirmed": [str],
            "confidence": float,
        }

    Severity taxonomy:
        - critical: MUST fix before send (cert expired, sol# mismatch)
        - warning:  should review (missing form the email expects)
        - info:     FYI only (low-confidence extraction, edge hints)
    """
    import json as _json
    from datetime import datetime

    result = {"passed": True, "gaps": [], "confirmed": [], "confidence": 0.0}

    if not requirements_json or requirements_json == "{}":
        return result

    try:
        reqs = _json.loads(requirements_json) if isinstance(requirements_json, str) else requirements_json
    except (ValueError, TypeError):
        return result
    if not isinstance(reqs, dict):
        return result

    confidence = reqs.get("confidence", 0.0)
    result["confidence"] = confidence
    forms_required = reqs.get("forms_required", []) or []
    gen_lower = [fn.lower() for fn in (generated_files or [])]
    rfq_data = rfq_data or {}
    config = config or {}

    def _add_gap(type_, form_id, msg, severity):
        result["gaps"].append({
            "type": type_,
            "form_id": form_id,
            "msg": msg,
            "severity": severity,
        })
        if strict and severity == "critical":
            result["passed"] = False

    # ── Required forms ──────────────────────────────────────────────
    for form_id in forms_required:
        found = any(form_id.replace("_", "") in fn.replace("_", "") for fn in gen_lower)
        if found:
            result["confirmed"].append(form_id)
        else:
            # Missing a form the buyer explicitly asked for is CRITICAL
            # in strict mode — they told us what they need.
            _add_gap(
                "missing_form",
                form_id,
                f"Email requires {form_id.upper()} but none generated",
                "critical" if strict else "warning",
            )

    # ── Food items → OBS-1600 heuristic ─────────────────────────────
    if reqs.get("food_items_present") and "obs_1600" not in forms_required:
        if not any("obs" in fn or "1600" in fn for fn in gen_lower):
            _add_gap(
                "missing_form",
                "obs_1600",
                "Email mentions food items — OBS 1600 certification may be required",
                "warning",
            )

    # ── Due date deadline check ─────────────────────────────────────
    req_due = reqs.get("due_date", "")
    rfq_due = rfq_data.get("due_date", "")
    if req_due and rfq_due in ("TBD", "", None):
        _add_gap(
            "due_date_missing",
            "",
            f"Email due date is {req_due} but RFQ due date not set",
            "warning",
        )
    # If both are set, also verify we're still BEFORE the deadline
    if req_due:
        parsed_due = _parse_requirement_date(req_due)
        if parsed_due:
            if parsed_due < datetime.now():
                _add_gap(
                    "deadline_passed",
                    "",
                    f"Email due date {req_due} has already passed — "
                    f"do not submit without buyer confirmation",
                    "critical",
                )
            elif (parsed_due - datetime.now()).days == 0:
                _add_gap(
                    "deadline_today",
                    "",
                    f"Email due date {req_due} is TODAY — ensure submission before cutoff time",
                    "warning",
                )

    # ── Cert validity check ────────────────────────────────────────
    # If the email requires DVBE/SB/MB certs and the package is using
    # Reytech's cert, verify it isn't expired.
    co = config.get("company", {}) or {}
    cert_expiration = co.get("cert_expiration") or config.get("cert_expiration", "")
    dvbe_or_sb_required = any(
        f in forms_required for f in ("dvbe843", "std843", "sb_dvbe_cert")
    ) or bool(reqs.get("requires_sb_dvbe"))
    if cert_expiration and dvbe_or_sb_required:
        parsed_cert = _parse_requirement_date(cert_expiration)
        if parsed_cert and parsed_cert < datetime.now():
            _add_gap(
                "cert_expired",
                "",
                f"Reytech SB/DVBE cert expired {cert_expiration} — "
                f"renew before bidding on DVBE solicitations",
                "critical",
            )

    # ── Solicitation number match ───────────────────────────────────
    req_sol = (reqs.get("solicitation_number") or "").strip()
    rfq_sol = (rfq_data.get("solicitation_number") or "").strip()
    if req_sol and rfq_sol and req_sol != rfq_sol:
        _add_gap(
            "solicitation_mismatch",
            "",
            f"Email says solicitation {req_sol} but RFQ is set to {rfq_sol} — "
            f"verify before sending",
            "critical",
        )

    # ── Delivery location constraint ───────────────────────────────
    delivery_location = (reqs.get("delivery_location") or "").strip()
    if delivery_location:
        # FYI only — we can't verify the package contains the right
        # ship-to without parsing the PDF, but we surface it so the
        # operator eyeballs the cover page
        _add_gap(
            "delivery_reminder",
            "",
            f"Email specifies delivery to: {delivery_location}",
            "info",
        )

    # ── Low-confidence extraction warning ──────────────────────────
    if confidence and confidence < 0.5:
        _add_gap(
            "low_confidence_extraction",
            "",
            f"Requirements extraction confidence is only {confidence:.0%} — "
            f"review extracted requirements manually",
            "info",
        )

    return result


def _parse_requirement_date(date_str: str):
    """Best-effort parse of a date string in one of the formats the
    requirement extractor emits. Returns a datetime or None."""
    from datetime import datetime
    if not date_str:
        return None
    s = str(date_str).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# ═══════════════════════════════════════════════════════════════════════
# Computed Field & Value Range Validation
# ═══════════════════════════════════════════════════════════════════════

def _parse_currency(val: str) -> Optional[float]:
    """Parse a currency string like '$1,234.56' or '1234.56' into float."""
    if not val:
        return None
    cleaned = re.sub(r"[$ ,]", "", str(val).strip())
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def verify_704b_computations(pdf_path: str, rfq_data: dict) -> dict:
    """Verify 704B math: qty × unit_price == extension, sum of extensions == subtotal.

    Returns: {"passed": bool, "issues": [str], "warnings": [str]}
    """
    result = {"passed": True, "issues": [], "warnings": []}

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        fields = reader.get_fields() or {}
    except Exception as e:
        result["warnings"].append(f"Cannot read PDF for computation check: {e}")
        return result

    def _get(name):
        f = fields.get(name)
        return str(f.get("/V", "")).strip() if f else ""

    # Check each row on page 1 (unsuffixed) and page 2 (_2 suffix)
    suffixes = [("", range(1, 20)), ("_2", range(1, 9))]
    computed_total = 0.0
    rows_checked = 0

    for suffix, row_range in suffixes:
        for n in row_range:
            row_key = f"Row{n}" if suffix == "" else f"Row{n}{suffix}"
            qty_str = _get(f"QTYRow{n}{suffix}") or _get(f"QTY{row_key}")
            price_str = _get(f"PRICE PER UNITRow{n}{suffix}") or _get(f"PRICE PER UNIT{row_key}")
            ext_str = _get(f"EXTENSIONRow{n}{suffix}") or _get(f"EXTENSION{row_key}")

            qty = _parse_currency(qty_str)
            price = _parse_currency(price_str)
            ext = _parse_currency(ext_str)

            # Skip empty rows
            if price is None and ext is None:
                continue

            rows_checked += 1

            if qty is not None and price is not None and ext is not None:
                expected_ext = round(qty * price, 2)
                if abs(expected_ext - ext) > 0.01:
                    result["passed"] = False
                    result["issues"].append(
                        f"Row {n}{suffix}: {qty} × ${price:.2f} = ${expected_ext:.2f}, "
                        f"but extension shows ${ext:.2f}"
                    )
                computed_total += ext
            elif ext is not None:
                computed_total += ext
                if qty is None and price is not None:
                    result["warnings"].append(f"Row {n}{suffix}: quantity missing")

    # Check merchandise subtotal
    subtotal_str = _get("fill_154") or _get("MERCHANDISE SUBTOTAL")
    subtotal = _parse_currency(subtotal_str)
    if subtotal is not None and rows_checked > 0:
        if abs(computed_total - subtotal) > 0.01:
            result["passed"] = False
            result["issues"].append(
                f"Subtotal mismatch: sum of extensions = ${computed_total:.2f}, "
                f"but MERCHANDISE SUBTOTAL = ${subtotal:.2f}"
            )

    if rows_checked == 0:
        result["warnings"].append("No pricing rows found to verify")

    return result


def verify_value_ranges(pdf_path: str, form_id: str) -> dict:
    """Check date formats and price ranges for sanity.

    Returns: {"passed": bool, "issues": [str], "warnings": [str]}
    """
    result = {"passed": True, "issues": [], "warnings": []}
    registry = FORM_FIELD_REGISTRY.get(form_id)
    if not registry:
        return result

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        fields = reader.get_fields() or {}
    except Exception as e:
        result["warnings"].append(f"Cannot read PDF for range check: {e}")
        return result

    # Detect prefix
    field_names = set(fields.keys())
    prefix = ""
    if registry.get("prefix_detect"):
        prefix = _detect_prefix(field_names, registry.get("possible_prefixes", []))

    # Validate date fields
    date_pattern = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
    for date_template in registry.get("date_fields", []):
        date_field = date_template.replace("{p}", prefix)
        actual_field = fields.get(date_field)
        val = str(actual_field.get("/V", "")).strip() if actual_field else ""
        if not val:
            continue
        m = date_pattern.match(val)
        if not m:
            result["warnings"].append(f"Date '{date_field}' has unexpected format: '{val}'")
        else:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if year < 2024 or year > 2030:
                result["warnings"].append(f"Date '{date_field}' has implausible year: {year}")
            if month < 1 or month > 12:
                result["issues"].append(f"Date '{date_field}' has invalid month: {month}")
                result["passed"] = False
            if day < 1 or day > 31:
                result["issues"].append(f"Date '{date_field}' has invalid day: {day}")
                result["passed"] = False

    # Validate pricing fields (704b: check for negative prices)
    if registry.get("pricing_required"):
        suffixes = [("", range(1, 20)), ("_2", range(1, 9))]
        for suffix, row_range in suffixes:
            for n in row_range:
                price_str = fields.get(f"PRICE PER UNITRow{n}{suffix}")
                if price_str:
                    val = _parse_currency(str(price_str.get("/V", "")))
                    if val is not None and val < 0:
                        result["passed"] = False
                        result["issues"].append(f"Negative price in row {n}{suffix}: ${val:.2f}")

    return result


# ═══════════════════════════════════════════════════════════════════════
# Buyer Field Contamination Check (704B)
# ═══════════════════════════════════════════════════════════════════════

# 704B fields that belong to the BUYER — Reytech must never overwrite these
_BUYER_OWNED_HEADER_FIELDS = {
    "DEPARTMENT", "PHONE", "EMAIL", "PHONEEMAIL",
    "SOLICITATION NUMBER", "CONTACT NAME",
}

# Per-row buyer fields (description, qty, UOM, item number)
_BUYER_ROW_PREFIXES = ("Row", "QTYRow", "UOMRow", "ITEM NUMBERRow")


def _is_buyer_field(name: str) -> bool:
    """Check if a field name is buyer-owned on the 704B."""
    upper = name.upper().strip()
    if upper in _BUYER_OWNED_HEADER_FIELDS:
        return True
    # Row fields: "Row1", "QTYRow3", "UOMRow1_2", "ITEM NUMBERRow5"
    for prefix in _BUYER_ROW_PREFIXES:
        if upper.startswith(prefix.upper()):
            return True
    return False


def verify_buyer_fields_untouched(pdf_path: str, template_path: str) -> dict:
    """Compare buyer-owned fields between template and filled PDF.

    If a buyer field had a non-empty value in the template and the filled PDF
    has a DIFFERENT value, that's contamination — Reytech overwrote buyer data.

    Returns: {"passed": bool, "issues": [str], "contaminated": [{name, template, filled}]}
    """
    result = {"passed": True, "issues": [], "contaminated": []}

    if not template_path or not os.path.exists(template_path):
        return result  # Can't check without template
    if not os.path.exists(pdf_path):
        result["issues"].append("Filled PDF not found")
        result["passed"] = False
        return result

    try:
        from pypdf import PdfReader
        tmpl_fields = PdfReader(template_path).get_fields() or {}
        filled_fields = PdfReader(pdf_path).get_fields() or {}
    except Exception as e:
        result["issues"].append(f"Cannot read PDFs for buyer check: {e}")
        return result

    for name, tmpl_field in tmpl_fields.items():
        if not _is_buyer_field(name):
            continue
        tmpl_val = str(tmpl_field.get("/V", "")).strip()
        if not tmpl_val:
            continue  # Empty in template — not a contamination risk

        filled_field = filled_fields.get(name)
        filled_val = str(filled_field.get("/V", "")).strip() if filled_field else ""

        if filled_val != tmpl_val:
            result["passed"] = False
            result["contaminated"].append({
                "field": name,
                "template_value": tmpl_val[:50],
                "filled_value": filled_val[:50],
            })
            result["issues"].append(
                f"Buyer field '{name}' was overwritten: "
                f"'{tmpl_val[:30]}' → '{filled_val[:30]}'"
            )

    return result


# ═══════════════════════════════════════════════════════════════════════
# Overlay Bounds Check (DOCX-converted 704 regression guard)
# ═══════════════════════════════════════════════════════════════════════

# Maximum allowed drift between an overlay text object's position and the
# cell rect it is supposed to sit inside. 5pt is tight enough to catch
# the 20-40pt drift seen on LibreOffice-converted 704 forms but loose
# enough to tolerate sub-pixel rounding from pdfplumber/reportlab.
OVERLAY_DRIFT_TOLERANCE_PT = 5.0


def verify_overlay_bounds(
    pdf_path: str,
    expected_cells: dict,
    tolerance_pt: float = OVERLAY_DRIFT_TOLERANCE_PT,
) -> dict:
    """Verify that overlay text drawn onto a flat 704 (or similar)
    lands inside the expected cell rectangles. Catches the DOCX→
    LibreOffice→overlay regression where text lands 20-40pt off.

    Args:
        pdf_path: path to the filled output PDF
        expected_cells: {field_id: (x0, y0_bot, x1, y1_top)} in
            reportlab coordinates (Y grows upward). Example:
                {
                    "Row1_price": (637, 292, 686, 311),
                    "Row1_ext":   (691, 292, 754, 311),
                    "COMPANY NAME": (33, 421, 278, 441),
                }
        tolerance_pt: max allowed drift from each cell edge

    Returns: {
        "passed": bool,
        "issues": [str],
        "warnings": [str],
        "drift_details": [{field, text, expected, actual, drift}],
    }

    The check reads overlay text via pdfplumber's word extraction and
    finds the word(s) that correspond to each expected field by nearest
    cell-center match. For each match, it then checks whether the
    word's bounding box is within tolerance_pt of the cell rect edges.
    """
    result = {
        "passed": True,
        "issues": [],
        "warnings": [],
        "drift_details": [],
    }
    if not pdf_path or not os.path.exists(pdf_path):
        result["issues"].append(f"overlay bounds: PDF not found: {pdf_path}")
        result["passed"] = False
        return result
    if not expected_cells:
        return result

    try:
        import pdfplumber
    except ImportError:
        result["warnings"].append("overlay bounds: pdfplumber unavailable — skipped")
        return result

    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        result["issues"].append(f"overlay bounds: cannot open PDF: {e}")
        result["passed"] = False
        return result

    try:
        # Walk every page, extract words with coordinates, and for each
        # expected cell find the closest word by center distance.
        # Words are returned in pdfplumber coordinates (Y grows downward);
        # convert to reportlab (Y grows upward) for comparison with the
        # expected cell rects.
        all_words = []  # (page_idx, rl_x0, rl_y0, rl_x1, rl_y1, text)
        for pg_idx, page in enumerate(pdf.pages):
            ph = float(page.height)
            for w in page.extract_words(keep_blank_chars=False,
                                         x_tolerance=2, y_tolerance=2):
                rl_y0 = ph - w["bottom"]  # bottom in reportlab = top in pdfplumber
                rl_y1 = ph - w["top"]
                all_words.append(
                    (pg_idx, float(w["x0"]), rl_y0, float(w["x1"]), rl_y1, w["text"])
                )

        for field_id, (ex0, ey0, ex1, ey1) in expected_cells.items():
            ecx = (ex0 + ex1) / 2
            ecy = (ey0 + ey1) / 2

            # Find the word whose center is closest to the cell center
            # AND is within a generous proximity window (3x tolerance)
            # so we don't match unrelated words on the far side of the
            # page when the target cell is empty.
            proximity = tolerance_pt * 6
            best = None
            best_dist = float("inf")
            for (pg_idx, wx0, wy0, wx1, wy1, wtxt) in all_words:
                wcx = (wx0 + wx1) / 2
                wcy = (wy0 + wy1) / 2
                if abs(wcx - ecx) > (ex1 - ex0) / 2 + proximity:
                    continue
                if abs(wcy - ecy) > (ey1 - ey0) / 2 + proximity:
                    continue
                d = ((wcx - ecx) ** 2 + (wcy - ecy) ** 2) ** 0.5
                if d < best_dist:
                    best_dist = d
                    best = (pg_idx, wx0, wy0, wx1, wy1, wtxt)

            if best is None:
                # Cell is empty in the output. Not an error — just skip.
                continue

            pg_idx, wx0, wy0, wx1, wy1, wtxt = best
            # Compute signed drift at each edge. Positive means the word
            # extends OUTSIDE the cell on that side.
            drift_left = max(0.0, ex0 - wx0)
            drift_right = max(0.0, wx1 - ex1)
            drift_bot = max(0.0, ey0 - wy0)
            drift_top = max(0.0, wy1 - ey1)
            max_drift = max(drift_left, drift_right, drift_bot, drift_top)

            detail = {
                "field": field_id,
                "text": wtxt[:40],
                "expected": (round(ex0, 1), round(ey0, 1),
                             round(ex1, 1), round(ey1, 1)),
                "actual": (round(wx0, 1), round(wy0, 1),
                           round(wx1, 1), round(wy1, 1)),
                "drift": round(max_drift, 2),
                "page": pg_idx + 1,
            }
            result["drift_details"].append(detail)

            if max_drift > tolerance_pt:
                result["passed"] = False
                result["issues"].append(
                    f"[overlay drift] {field_id} pg{pg_idx + 1}: "
                    f"text '{wtxt[:20]}' is {max_drift:.1f}pt outside its "
                    f"cell (tolerance {tolerance_pt:.0f}pt)"
                )
    finally:
        try:
            pdf.close()
        except Exception:
            pass

    return result


def verify_704_overlay_self_check(pdf_path: str) -> dict:
    """Run the 704 overlay self-check: read the detector's output for
    the filled PDF, then verify that price/extension overlay text
    actually lands inside those detected cells. Returns an empty-pass
    result when the form doesn't look like a 704 overlay target
    (so the check is safe to call from run_form_qa for any form).

    This closes the loop on the DOCX 704 overlay bug: even if a new
    buyer format causes the detector to return wrong cell positions,
    this self-check will fail loudly instead of shipping a silently-
    broken PDF.
    """
    result = {
        "passed": True,
        "issues": [],
        "warnings": [],
        "drift_details": [],
        "rows_checked": 0,
    }
    if not pdf_path or not os.path.exists(pdf_path):
        return result

    try:
        from src.forms.price_check import _detect_ams704_overlay_positions
    except ImportError:
        result["warnings"].append(
            "overlay self-check: _detect_ams704_overlay_positions unavailable"
        )
        return result

    try:
        detected = _detect_ams704_overlay_positions(pdf_path)
    except Exception as e:
        result["warnings"].append(f"overlay self-check: detector crashed: {e}")
        return result

    if not detected:
        # No detection result at all — this is a form-field 704, not an
        # overlay 704. Skip.
        return result
    if not any(d is not None for d in detected):
        return result

    expected_cells = {}
    row_num = 0
    for pg_idx, d in enumerate(detected):
        if d is None:
            continue
        for (yb, yt) in d.get("item_rows", []):
            row_num += 1
            px = d.get("price_x") or ()
            ex = d.get("ext_x") or ()
            if len(px) == 2:
                expected_cells[f"price_row{row_num}"] = (px[0], yb, px[1], yt)
            if len(ex) == 2:
                expected_cells[f"ext_row{row_num}"] = (ex[0], yb, ex[1], yt)
    result["rows_checked"] = row_num

    if not expected_cells:
        return result

    bounds = verify_overlay_bounds(
        pdf_path,
        expected_cells,
        tolerance_pt=OVERLAY_DRIFT_TOLERANCE_PT,
    )
    result["passed"] = bounds["passed"]
    result["issues"] = bounds["issues"]
    result["warnings"].extend(bounds.get("warnings", []))
    result["drift_details"] = bounds.get("drift_details", [])
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
    requirements_json: str = "",
    strict_requirements: bool = False,
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

    # 1b. Email-as-Contract requirements check — the buyer's email is
    # a binding spec. Verify the generated package satisfies every
    # requirement they listed (forms, certs, due dates) before allowing
    # the send. Only runs if the caller provided extracted requirements.
    if requirements_json:
        try:
            req_check = validate_against_requirements(
                generated_files=output_files,
                requirements_json=requirements_json,
                rfq_data=rfq_data,
                config=config,
                strict=strict_requirements,
            )
            report["requirements_check"] = req_check
            if not req_check.get("passed", True):
                report["passed"] = False
            for gap in req_check.get("gaps", []):
                msg = f"[requirements] {gap.get('msg','')}"
                if gap.get("severity") == "critical":
                    report["critical_issues"].append(msg)
                    if strict_requirements:
                        report["passed"] = False
                elif gap.get("severity") == "warning":
                    report["warnings"].append(msg)
                else:  # info
                    report["warnings"].append(msg)
        except Exception as e:
            log.warning("run_form_qa: requirements check crashed: %s", e)
            report["warnings"].append(f"requirements check crashed: {e}")

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

        # Computation check (704B only)
        comp_result = None
        if form_id == "704b":
            comp_result = verify_704b_computations(pdf_path, rfq_data)

        # Buyer field contamination check (704B with template)
        buyer_result = None
        template_path = form_info.get("template_path", "")
        if form_id == "704b" and template_path:
            buyer_result = verify_buyer_fields_untouched(pdf_path, template_path)

        # Value range check
        range_result = verify_value_ranges(pdf_path, form_id)

        # Overlay self-check: for flat/overlay-based 704 forms, confirm
        # that the detector's reported cells match where overlay text
        # actually landed. This is the scale-safety net that would have
        # caught the 20-40pt DOCX 704 drift if it had ever shipped.
        overlay_result = None
        if form_id in ("704", "704a", "704b", "price_check_overlay"):
            overlay_result = verify_704_overlay_self_check(pdf_path)

        # Combine
        form_report = {
            "filename": filename,
            "fields": field_result,
            "signatures": sig_result,
            "passed": field_result["passed"] and sig_result["passed"],
        }
        if comp_result:
            form_report["computations"] = comp_result
            if not comp_result["passed"]:
                form_report["passed"] = False
        if buyer_result:
            form_report["buyer_fields"] = buyer_result
            if not buyer_result["passed"]:
                form_report["passed"] = False
        if not range_result["passed"]:
            form_report["passed"] = False
        form_report["value_ranges"] = range_result
        if overlay_result is not None:
            form_report["overlay_self_check"] = overlay_result
            if not overlay_result["passed"]:
                form_report["passed"] = False

        report["form_results"][form_id] = form_report
        report["forms_checked"] += 1

        if not form_report["passed"]:
            report["passed"] = False
            report["critical_issues"].extend(field_result.get("issues", []))
            if comp_result:
                report["critical_issues"].extend(comp_result.get("issues", []))
            if buyer_result:
                report["critical_issues"].extend(buyer_result.get("issues", []))
            report["critical_issues"].extend(range_result.get("issues", []))
            if overlay_result:
                report["critical_issues"].extend(overlay_result.get("issues", []))
        if overlay_result:
            report["warnings"].extend(overlay_result.get("warnings", []))

        report["warnings"].extend(field_result.get("warnings", []))
        report["warnings"].extend(sig_result.get("warnings", []))
        if comp_result:
            report["warnings"].extend(comp_result.get("warnings", []))
        report["warnings"].extend(range_result.get("warnings", []))

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


def verify_single_form(pdf_path: str, form_id: str, data: dict = None, config: dict = None) -> dict:
    """Lightweight QA for a single generated PDF (used by PC generation paths).

    Args:
        pdf_path: Path to the generated PDF
        form_id: Registry key (e.g. "704b", "quote")
        data: RFQ/PC data dict (optional — used for expected-value checks)
        config: App config with company info (optional)

    Returns: {
        "passed": bool,
        "form_id": str,
        "issues": [str],
        "warnings": [str],
    }
    """
    result = {"passed": True, "form_id": form_id, "issues": [], "warnings": []}

    if not pdf_path or not os.path.exists(pdf_path):
        result["passed"] = False
        result["issues"].append(f"PDF file not found: {pdf_path}")
        return result

    # Field verification (skipped if form_id not in registry — just warns)
    if form_id in FORM_FIELD_REGISTRY:
        field_check = verify_filled_form(pdf_path, form_id, data or {}, config or {})
        sig_check = verify_signatures(pdf_path, form_id)

        if not field_check["passed"]:
            result["passed"] = False
            result["issues"].extend(field_check.get("issues", []))
        result["warnings"].extend(field_check.get("warnings", []))

        if not sig_check["passed"]:
            result["passed"] = False
            result["issues"].extend(sig_check.get("issues", []))
        result["warnings"].extend(sig_check.get("warnings", []))
    else:
        result["warnings"].append(f"No QA registry for form_id '{form_id}' — skipping field checks")

    # Overlay self-check — runs for any 704-family form, independent
    # of whether the registry has an entry. Catches cell-drift
    # regressions on flat/DocuSign/DOCX-converted 704 PDFs.
    if form_id in ("704", "704a", "704b", "price_check_overlay"):
        overlay_result = verify_704_overlay_self_check(pdf_path)
        if not overlay_result["passed"]:
            result["passed"] = False
            result["issues"].extend(overlay_result.get("issues", []))
        result["warnings"].extend(overlay_result.get("warnings", []))
        result["overlay_self_check"] = overlay_result

    status = "PASS" if result["passed"] else "FAIL"
    log.info("Form QA [single] %s: %s — %d issues, %d warnings",
             status, form_id, len(result["issues"]), len(result["warnings"]))

    return result
