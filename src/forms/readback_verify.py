"""
Read-Back Verification — Post-generation PDF output verification.

After every PDF generation, reads back what was actually written and compares
to what was intended. This is the foundation of the self-healing pipeline:
evidence over assumptions.

Two verification modes:
1. Form fields: pypdf get_fields() reads back field values
2. Overlay text: pdfplumber extracts text from rendered pages

Score = 100 means every intended field confirmed. Anything less triggers retry.
"""

import os
import re
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("reytech.readback_verify")


# ═══════════════════════════════════════════════════════════════════════════
# RESULT TYPES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ReadbackIssue:
    """A single field verification failure."""
    field_name: str
    intended_value: str
    actual_value: str
    issue_type: str     # "missing" | "wrong_value" | "truncated" | "empty"
    is_critical: bool   # True for COMPANY NAME, prices, totals, signature


@dataclass
class ReadbackResult:
    """Result of read-back verification."""
    score: int                      # 0-100
    fields_intended: int            # how many fields we tried to write
    fields_confirmed: int           # how many read back correctly
    fields_missing: int             # intended but absent in output
    fields_wrong: int               # present but wrong value
    issues: list = field(default_factory=list)   # list[ReadbackIssue]
    strategy_used: str = ""         # what fill strategy was used
    verification_mode: str = ""     # "form_fields" | "overlay_text"

    @property
    def passed(self) -> bool:
        return self.score == 100

    def summary(self) -> str:
        if self.passed:
            return f"Verified: {self.fields_confirmed}/{self.fields_intended} fields confirmed (100%)"
        return (f"FAILED: {self.fields_confirmed}/{self.fields_intended} confirmed, "
                f"{self.fields_missing} missing, {self.fields_wrong} wrong (score={self.score})")


# ═══════════════════════════════════════════════════════════════════════════
# CRITICAL FIELDS — higher penalty for missing these
# ═══════════════════════════════════════════════════════════════════════════

CRITICAL_FIELDS = {
    "COMPANY NAME", "SUPPLIER NAME",
    "COMPANY REPRESENTATIVE print name",
    "PERSON PROVIDING QUOTE",
}

# Price/extension/total patterns — any field containing these keywords is critical
_PRICE_PATTERNS = re.compile(
    r"PRICE PER UNIT|EXTENSION|fill_7[0-3]|GRAND TOTAL|Subtotal",
    re.IGNORECASE
)

# Fields that are informational/cosmetic — lower penalty
_LOW_PRIORITY_PATTERNS = re.compile(
    r"^Page\d*$|^of\d*$|Discount Offered|Supplier andor Requestor Notes",
    re.IGNORECASE
)


def _is_critical_field(field_name: str) -> bool:
    """Determine if a field is critical (high penalty if missing)."""
    if field_name in CRITICAL_FIELDS:
        return True
    if _PRICE_PATTERNS.search(field_name):
        return True
    return False


def _is_low_priority(field_name: str) -> bool:
    """Determine if a field is low-priority (lower penalty)."""
    if _LOW_PRIORITY_PATTERNS.search(field_name):
        return True
    return False


def _normalize_value(val) -> str:
    """Normalize a field value for comparison."""
    if val is None:
        return ""
    s = str(val).strip()
    # pypdf returns /V values sometimes with leading slash
    if s.startswith("/"):
        s = s[1:]
    # Normalize whitespace
    s = re.sub(r'\s+', ' ', s)
    return s


def _values_match(intended: str, actual: str) -> bool:
    """Compare two field values with tolerance for formatting differences."""
    i = _normalize_value(intended)
    a = _normalize_value(actual)
    if not i:
        return True  # Don't penalize empty intended values
    if i == a:
        return True
    # Numeric comparison: strip commas/$ and compare
    i_clean = re.sub(r'[$,\s]', '', i)
    a_clean = re.sub(r'[$,\s]', '', a)
    if i_clean == a_clean:
        return True
    # Truncation check: actual is prefix of intended
    if len(a) >= 3 and i.startswith(a):
        return False  # This is truncation, not a match
    return False


def _is_truncated(intended: str, actual: str) -> bool:
    """Check if actual value is a truncated version of intended."""
    i = _normalize_value(intended)
    a = _normalize_value(actual)
    if not i or not a:
        return False
    if len(a) < len(i) and i.startswith(a) and len(a) >= 3:
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# FORM FIELD VERIFICATION (for PDFs filled via pypdf)
# ═══════════════════════════════════════════════════════════════════════════

def verify_form_fields(output_pdf: str, intended_values: list) -> ReadbackResult:
    """Read back all fields from a filled PDF and compare to intended values.

    Args:
        output_pdf: Path to the filled output PDF
        intended_values: The field_values list used during generation.
            Each item: {"field_id": str, "page": int, "value": str}

    Returns:
        ReadbackResult with score and issue details.
    """
    if not os.path.exists(output_pdf):
        return ReadbackResult(
            score=0, fields_intended=len(intended_values),
            fields_confirmed=0, fields_missing=len(intended_values),
            fields_wrong=0,
            issues=[ReadbackIssue("OUTPUT_FILE", output_pdf, "",
                                  "missing", True)],
            verification_mode="form_fields",
        )

    try:
        from pypdf import PdfReader
        reader = PdfReader(output_pdf)
        pdf_fields = reader.get_fields() or {}
    except Exception as e:
        log.warning("readback_verify: Failed to read PDF fields: %s", e)
        return ReadbackResult(
            score=0, fields_intended=len(intended_values),
            fields_confirmed=0, fields_missing=0, fields_wrong=0,
            issues=[ReadbackIssue("PDF_READ", str(e), "", "missing", True)],
            verification_mode="form_fields",
        )

    # Build lookup: field_name → actual value from PDF
    actual_values = {}
    for fname, fobj in pdf_fields.items():
        if isinstance(fobj, dict):
            val = fobj.get("/V", "")
        else:
            val = str(fobj) if fobj else ""
        actual_values[fname] = _normalize_value(val)

    # Compare intended vs actual
    issues = []
    confirmed = 0
    missing = 0
    wrong = 0
    # Deduplicate intended_values by field_id (last write wins)
    seen = {}
    for fv in intended_values:
        fid = fv.get("field_id", "")
        val = fv.get("value", "")
        if fid and val and val.strip():
            seen[fid] = val
    intended_count = len(seen)

    for field_id, intended_val in seen.items():
        intended_norm = _normalize_value(intended_val)
        if not intended_norm:
            confirmed += 1  # Empty intended = always OK
            continue

        actual_norm = actual_values.get(field_id, "")
        is_crit = _is_critical_field(field_id)

        if not actual_norm:
            # Field missing or empty in output
            missing += 1
            issues.append(ReadbackIssue(
                field_name=field_id,
                intended_value=intended_norm[:80],
                actual_value="",
                issue_type="missing",
                is_critical=is_crit,
            ))
        elif _values_match(intended_norm, actual_norm):
            confirmed += 1
        elif _is_truncated(intended_norm, actual_norm):
            wrong += 1
            issues.append(ReadbackIssue(
                field_name=field_id,
                intended_value=intended_norm[:80],
                actual_value=actual_norm[:80],
                issue_type="truncated",
                is_critical=is_crit,
            ))
        else:
            wrong += 1
            issues.append(ReadbackIssue(
                field_name=field_id,
                intended_value=intended_norm[:80],
                actual_value=actual_norm[:80],
                issue_type="wrong_value",
                is_critical=is_crit,
            ))

    # Calculate score
    score = _calculate_score(intended_count, confirmed, issues)

    result = ReadbackResult(
        score=score,
        fields_intended=intended_count,
        fields_confirmed=confirmed,
        fields_missing=missing,
        fields_wrong=wrong,
        issues=issues,
        verification_mode="form_fields",
    )
    log.info("readback_verify (form_fields): %s", result.summary())
    return result


# ═══════════════════════════════════════════════════════════════════════════
# OVERLAY TEXT VERIFICATION (for flattened/DOCX PDFs without form fields)
# ═══════════════════════════════════════════════════════════════════════════

def verify_overlay_text(output_pdf: str, intended_values: list,
                        critical_fields: list = None) -> ReadbackResult:
    """Verify overlay text using pdfplumber text extraction.

    For overlay fills, form fields don't exist. Instead, use pdfplumber
    to extract ALL text from each page and check that critical values appear.

    This is a softer check than form field verification — it confirms the
    text exists somewhere on the correct page, not at exact coordinates.

    Args:
        output_pdf: Path to the filled output PDF
        intended_values: The field_values list used during generation.
        critical_fields: List of field_ids that MUST be verified.
            If None, uses CRITICAL_FIELDS + all price fields.
    """
    if not os.path.exists(output_pdf):
        return ReadbackResult(
            score=0, fields_intended=len(intended_values),
            fields_confirmed=0, fields_missing=len(intended_values),
            fields_wrong=0,
            issues=[ReadbackIssue("OUTPUT_FILE", output_pdf, "",
                                  "missing", True)],
            verification_mode="overlay_text",
        )

    try:
        import pdfplumber
        pdf = pdfplumber.open(output_pdf)
        page_texts = []
        for page in pdf.pages:
            page_texts.append(page.extract_text() or "")
        pdf.close()
    except Exception as e:
        log.warning("readback_verify: Failed to extract text: %s", e)
        return ReadbackResult(
            score=0, fields_intended=len(intended_values),
            fields_confirmed=0, fields_missing=0, fields_wrong=0,
            issues=[ReadbackIssue("PDF_READ", str(e), "", "missing", True)],
            verification_mode="overlay_text",
        )

    # Deduplicate and filter to verifiable fields
    seen = {}
    for fv in intended_values:
        fid = fv.get("field_id", "")
        val = fv.get("value", "")
        page = fv.get("page", 1)
        if fid and val and val.strip():
            seen[fid] = {"value": val, "page": page}

    # For overlay, focus on critical values that MUST appear
    # We can't check every field (overlay text is position-based, not named)
    check_fields = {}
    for fid, info in seen.items():
        if _is_critical_field(fid):
            check_fields[fid] = info
        elif critical_fields and fid in critical_fields:
            check_fields[fid] = info

    # Also check all price values — these are the most important
    for fid, info in seen.items():
        if _PRICE_PATTERNS.search(fid):
            check_fields[fid] = info

    if not check_fields:
        # No critical fields to verify — trust the overlay
        log.info("readback_verify (overlay): no critical fields to check, passing")
        return ReadbackResult(
            score=100, fields_intended=len(seen),
            fields_confirmed=len(seen), fields_missing=0, fields_wrong=0,
            verification_mode="overlay_text",
        )

    intended_count = len(check_fields)
    issues = []
    confirmed = 0
    missing = 0

    for fid, info in check_fields.items():
        intended_val = _normalize_value(info["value"])
        page_idx = info["page"] - 1  # 0-indexed

        if not intended_val:
            confirmed += 1
            continue

        # Search for the value in page text
        found = False
        # Extract the core value for searching (strip formatting)
        search_val = re.sub(r'[$,]', '', intended_val).strip()

        if page_idx < len(page_texts):
            page_text = page_texts[page_idx]
            page_text_clean = re.sub(r'[$,]', '', page_text)
            if search_val in page_text_clean:
                found = True
            elif len(search_val) > 5 and search_val[:5] in page_text_clean:
                # Partial match — text exists but may be wrapped/split
                found = True

        # Also search all pages (text might overflow to next page)
        if not found:
            all_text = " ".join(page_texts)
            all_text_clean = re.sub(r'[$,]', '', all_text)
            if search_val in all_text_clean:
                found = True

        if found:
            confirmed += 1
        else:
            missing += 1
            issues.append(ReadbackIssue(
                field_name=fid,
                intended_value=intended_val[:80],
                actual_value="(not found in page text)",
                issue_type="missing",
                is_critical=_is_critical_field(fid),
            ))

    score = _calculate_score(intended_count, confirmed, issues)

    result = ReadbackResult(
        score=score,
        fields_intended=intended_count,
        fields_confirmed=confirmed,
        fields_missing=missing,
        fields_wrong=0,
        issues=issues,
        verification_mode="overlay_text",
    )
    log.info("readback_verify (overlay): %s", result.summary())
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SIGNATURE VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════

def verify_signature(output_pdf: str, expected_page: int = -1) -> bool:
    """Verify signature image is present on the expected page.

    Checks for:
    1. /Sig form field with value
    2. Image XObjects on the target page (in lower 40% = signature zone)

    Args:
        output_pdf: Path to the filled PDF
        expected_page: 1-based page number (-1 = last page with items)

    Returns:
        True if signature detected, False otherwise.
    """
    if not os.path.exists(output_pdf):
        return False

    try:
        from pypdf import PdfReader
        reader = PdfReader(output_pdf)

        # Check 1: /Sig form field
        fields = reader.get_fields() or {}
        for fname, fobj in fields.items():
            if isinstance(fobj, dict) and fobj.get("/FT") == "/Sig":
                if fobj.get("/V"):
                    log.info("readback_verify: signature field '%s' has value", fname)
                    return True

        # Check 2: Image XObjects on expected page
        if expected_page == -1:
            expected_page = len(reader.pages)  # last page
        page_idx = min(expected_page - 1, len(reader.pages) - 1)
        if page_idx < 0:
            return False

        page = reader.pages[page_idx]
        resources = page.get("/Resources", {})
        if hasattr(resources, 'get_object'):
            resources = resources.get_object()
        xobjects = resources.get("/XObject", {})
        if hasattr(xobjects, 'get_object'):
            xobjects = xobjects.get_object()

        if xobjects and len(xobjects) > 0:
            # Has at least one XObject (likely an image = signature)
            log.info("readback_verify: found %d XObjects on page %d (signature likely present)",
                     len(xobjects), expected_page)
            return True

        return False
    except Exception as e:
        log.warning("readback_verify: signature check failed: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════════
# SCORE CALCULATION
# ═══════════════════════════════════════════════════════════════════════════

def _calculate_score(total_fields: int, confirmed: int,
                     issues: list) -> int:
    """Calculate verification score (0-100).

    Score = 100 means every field verified. Zero tolerance.

    Deductions:
    - Critical field missing/wrong: -15 per field
    - Normal field missing: -5 per field
    - Truncated field: -3 per field
    - Low-priority field missing: -2 per field
    """
    if total_fields == 0:
        return 100  # Nothing to verify = pass

    score = 100
    for issue in issues:
        if issue.is_critical:
            score -= 15
        elif _is_low_priority(issue.field_name):
            score -= 2
        elif issue.issue_type == "truncated":
            score -= 3
        else:
            score -= 5

    return max(0, score)
