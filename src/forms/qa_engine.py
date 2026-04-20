"""QA Engine — verify filled PDF using the same profile the filler used.

Single entry point: validate(filled_pdf_bytes, quote, profile) → ValidationReport.
The critical design: QA reads fields using the SAME profile YAML the filler
wrote to. Parser, filler, and QA all share one source of truth for field names.
No more 704A/704B convention mismatch.

Usage:
    from src.forms.qa_engine import validate

    report = validate(filled_pdf_bytes, quote, profile)
    if not report.passed:
        for issue in report.issues:
            print(f"  {issue.severity}: {issue.field} — {issue.message}")
"""
import io
import logging
from dataclasses import dataclass, field
from decimal import Decimal

from src.core.quote_model import Quote
from src.forms.profile_registry import FormProfile

log = logging.getLogger(__name__)


@dataclass
class QAIssue:
    field: str
    severity: str    # "error" | "warning" | "info"
    message: str
    expected: str = ""
    actual: str = ""


@dataclass
class ValidationReport:
    passed: bool
    issues: list[QAIssue] = field(default_factory=list)
    fields_checked: int = 0
    fields_matched: int = 0
    fields_missing: int = 0
    fields_wrong: int = 0
    profile_id: str = ""

    @property
    def match_rate(self) -> float:
        if self.fields_checked == 0:
            return 0.0
        return round(self.fields_matched / self.fields_checked * 100, 1)

    @property
    def summary(self) -> str:
        return (f"{self.fields_matched}/{self.fields_checked} fields match "
                f"({self.match_rate}%), {self.fields_missing} missing, "
                f"{self.fields_wrong} wrong — {'PASS' if self.passed else 'FAIL'}")

    @property
    def errors(self) -> list[str]:
        return [i.message for i in self.issues if i.severity == "error"]


def validate(filled_pdf_bytes: bytes, quote: Quote, profile: FormProfile) -> ValidationReport:
    """Validate a filled PDF against the Quote that generated it.

    Reads every field the profile declares, compares against the Quote's data.
    Returns a ValidationReport with per-field issues.
    """
    issues: list[QAIssue] = []
    checked = 0
    matched = 0
    missing = 0
    wrong = 0

    # Read fields from the filled PDF
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(filled_pdf_bytes))
        pdf_fields = reader.get_fields() or {}
    except Exception as e:
        return ValidationReport(
            passed=False, profile_id=profile.id,
            issues=[QAIssue("pdf", "error", f"Failed to read PDF: {e}")],
        )

    def _get(pdf_field_name: str) -> str:
        f = pdf_fields.get(pdf_field_name)
        if f is None:
            return ""
        if isinstance(f, dict):
            val = f.get("/V", "")
        else:
            val = str(f)
        return str(val).strip() if val else ""

    # ── Check static fields ──
    static_checks = {
        "vendor.name": quote.vendor.name,
        "vendor.supplier_name": quote.vendor.name,
        "vendor.representative": quote.vendor.representative,
        "vendor.email": quote.vendor.email,
        "vendor.phone": quote.vendor.phone,
        "vendor.sb_cert": quote.vendor.sb_cert,
        "vendor.dvbe_cert": quote.vendor.dvbe_cert,
        "buyer.requestor_name": quote.buyer.requestor_name,
        "buyer.institution": quote.header.institution_key,
        "ship_to.zip_code": quote.ship_to.zip_code,
        "header.solicitation_number": quote.header.solicitation_number,
    }

    for fm in profile.fields:
        if "[n]" in fm.semantic:
            continue  # Row fields checked separately
        if fm.semantic not in static_checks:
            continue

        expected = static_checks[fm.semantic]
        if not expected:
            continue  # Don't check fields we didn't fill

        actual = _get(fm.pdf_field)
        checked += 1

        if not actual:
            missing += 1
            issues.append(QAIssue(
                field=fm.semantic, severity="error",
                message=f"Field blank in output (expected: {expected[:50]})",
                expected=expected, actual="",
            ))
        elif actual != expected:
            # Fuzzy match — allow minor formatting differences
            if _normalize(actual) == _normalize(expected):
                matched += 1
            else:
                wrong += 1
                issues.append(QAIssue(
                    field=fm.semantic, severity="warning",
                    message=f"Value mismatch",
                    expected=expected[:50], actual=actual[:50],
                ))
        else:
            matched += 1

    # ── Check item rows ──
    active_items = [it for it in quote.line_items if not it.no_bid]
    capacities = profile.page_row_capacities
    item_idx = 0

    for page_num, capacity in enumerate(capacities, start=1):
        for row in range(1, capacity + 1):
            if item_idx >= len(active_items):
                break

            item = active_items[item_idx]
            row_fields = profile.get_row_fields(row, page=page_num)

            for sem, pdf_field in row_fields.items():
                field_part = sem.split(".")[-1]

                # Only check fields we care about
                if field_part == "unit_price" and item.unit_price > 0:
                    expected_price = f"{float(item.unit_price):.2f}"
                    actual_price = _get(pdf_field)
                    checked += 1
                    if not actual_price:
                        missing += 1
                        issues.append(QAIssue(
                            field=f"item[{item.line_no}].unit_price",
                            severity="error",
                            message=f"Price blank (expected ${expected_price})",
                            expected=expected_price, actual="",
                        ))
                    elif _normalize_number(actual_price) != _normalize_number(expected_price):
                        wrong += 1
                        issues.append(QAIssue(
                            field=f"item[{item.line_no}].unit_price",
                            severity="error",
                            message=f"Price mismatch",
                            expected=expected_price, actual=actual_price,
                        ))
                    else:
                        matched += 1

                elif field_part == "extension" and item.extension > 0:
                    expected_ext = f"{float(item.extension):.2f}"
                    actual_ext = _get(pdf_field)
                    checked += 1
                    if not actual_ext:
                        missing += 1
                        issues.append(QAIssue(
                            field=f"item[{item.line_no}].extension",
                            severity="error",
                            message=f"Extension blank (expected ${expected_ext})",
                            expected=expected_ext, actual="",
                        ))
                    elif _normalize_number(actual_ext) != _normalize_number(expected_ext):
                        wrong += 1
                        issues.append(QAIssue(
                            field=f"item[{item.line_no}].extension",
                            severity="error",
                            message=f"Extension mismatch",
                            expected=expected_ext, actual=actual_ext,
                        ))
                    else:
                        matched += 1

                elif field_part == "description" and item.description:
                    actual_desc = _get(pdf_field)
                    checked += 1
                    if not actual_desc:
                        missing += 1
                        issues.append(QAIssue(
                            field=f"item[{item.line_no}].description",
                            severity="warning",
                            message="Description blank",
                            expected=item.description[:40], actual="",
                        ))
                    else:
                        matched += 1

            item_idx += 1

    # ── Determine pass/fail ──
    errors = [i for i in issues if i.severity == "error"]
    passed = len(errors) == 0

    report = ValidationReport(
        passed=passed,
        issues=issues,
        fields_checked=checked,
        fields_matched=matched,
        fields_missing=missing,
        fields_wrong=wrong,
        profile_id=profile.id,
    )

    log.info("qa_engine: %s — %s", profile.id, report.summary)
    return report


def _normalize(s: str) -> str:
    """Normalize a string for fuzzy comparison."""
    return s.lower().strip().replace(",", "").replace(".", "").replace("  ", " ")


def _normalize_number(s: str) -> str:
    """Normalize a number string — strip $, commas, leading zeros."""
    s = s.strip().replace("$", "").replace(",", "").strip()
    try:
        return f"{float(s):.2f}"
    except (ValueError, TypeError):
        return s
