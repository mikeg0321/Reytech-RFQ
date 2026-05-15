"""PR-AV-QC — QA failure categorizer for the review-package rollup.

The QA pipeline (src/forms/form_qa.py) emits one flat list of critical
issue strings. The rollup banner used to display "Form QA failed — 26
critical issue(s)" — opaque. Operators had to drop into devtools (or
read the JSON of /api/rfq/<rid>/package-diag) to see which 26 things
failed and decide ship-anyway vs hand-fill.

This module adds `categorize_qa_issues` and `format_qa_breakdown` to
group issues by their emit-site failure class. The rollup banner now
reads:

    Form QA failed — 26 critical issue(s) (21 overlay drift, 5 field
    missing)

The categorization is anchored on the literal prefixes used by the QA
emitters in form_qa.py — not on free-text content — so it can't drift
when an unrelated message mentions "overlay" or "missing".

Tests pin:
  - Each known emit-site pattern lands in the right category
  - Unknown / new patterns fall through to "other" (visible, unlabeled)
  - Empty / None / non-string entries skip cleanly
  - Counts sort descending so the highest-impact class shows first
  - format_qa_breakdown returns parenthetical-ready text
  - Rollup banner wire-up surfaces the breakdown for multi-category fail
"""
from __future__ import annotations


# ── Pattern matching ────────────────────────────────────────────────────────

def test_overlay_drift_pattern_matches_emit_site():
    """[overlay drift] is the literal prefix in
    form_qa.verify_overlay_bounds (line ~1205)."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "[overlay drift] price_row7 pg1: text '12.50' is 8.3pt outside "
        "its cell (tolerance 3pt)",
    ])
    assert cats == {"overlay drift": 1}


def test_overlay_bounds_pdf_open_failure_categorizes_as_overlay():
    """overlay bounds: cannot open PDF — same class (overlay-side
    failure surface)."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "overlay bounds: cannot open PDF: ...",
    ])
    assert cats == {"overlay drift": 1}


def test_computation_row_mismatch_pattern():
    """form_qa.verify_704b_computations emits 'Row N: qty × $price = $X,
    but extension shows $Y'."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Row 3: 12 × $14.55 = $174.60, but extension shows $0.00",
        "Row 7_2: 1 × $99.99 = $99.99, but extension shows $899.91",
    ])
    assert cats == {"computation": 2}


def test_computation_subtotal_mismatch():
    """Subtotal-side of verify_704b_computations."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Subtotal mismatch: sum of extensions = $21685.80, but "
        "MERCHANDISE SUBTOTAL = $0.00",
    ])
    assert cats == {"computation": 1}


def test_value_range_negative_price():
    """verify_value_ranges line ~988."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Negative price in row 4: $-12.50",
    ])
    assert cats == {"value-range": 1}


def test_value_range_invalid_date():
    """verify_value_ranges date-side."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Date 'Date' has invalid month: 13",
        "Date '703B_Date' has invalid day: 32",
    ])
    assert cats == {"value-range": 2}


def test_field_missing_pattern():
    """verify_filled_form line ~356 (required field) + line ~379 (date)."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Missing: COMPANY NAME",
        "Missing: PERSON PROVIDING QUOTE",
        "Missing date: 703C_Date",
    ])
    assert cats == {"field missing": 3}


def test_pdf_read_error_patterns():
    """Multiple emit-sites surface as one class."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "PDF not found: 10847262_704B_Reytech.pdf",
        "Cannot read PDF: <PdfReadError>",
        "PDF file not found: ...",
    ])
    assert cats == {"pdf read error": 3}


def test_buyer_contamination_pattern():
    """verify_buyer_fields_untouched line ~1061."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Buyer field 'COMMENTS' was overwritten: "
        "'Original buyer text' → 'Reytech text'",
    ])
    assert cats == {"buyer-field contamination": 1}


def test_missing_form_patterns():
    """verify_package_completeness line ~621 + the forms_on_disk
    issue from review_alignment itself."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Required form not generated: 703B",
        "Missing required forms: 704B, AMS 703B",
    ])
    assert cats == {"missing form": 2}


def test_requirements_gap_pattern():
    """form_qa.run_form_qa line ~1369 — '[requirements] <msg>'."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "[requirements] Buyer asked for STD 204 — not in package",
    ])
    assert cats == {"buyer-email requirement": 1}


def test_unknown_pattern_falls_through_to_other():
    """A new emit-site we don't know about should still be visible
    in the bucket counts — just unlabeled as 'other'."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Some new failure class we haven't classified yet",
    ])
    assert cats == {"other": 1}


# ── Aggregate / sort ────────────────────────────────────────────────────────


def test_counts_sort_descending_by_count():
    """Highest-count class first so operator sees the biggest failure
    mode at a glance."""
    from src.api.review_alignment import categorize_qa_issues
    issues = (
        ["[overlay drift] r1"] * 21
        + ["Missing: COMPANY NAME"] * 5
        + ["Subtotal mismatch: ..."] * 2
    )
    cats = categorize_qa_issues(issues)
    keys = list(cats.keys())
    assert keys[0] == "overlay drift"   # 21
    assert keys[1] == "field missing"   # 5
    assert keys[2] == "computation"     # 2


def test_format_breakdown_human_readable():
    """The text format the rollup banner consumes."""
    from src.api.review_alignment import format_qa_breakdown
    breakdown = format_qa_breakdown(
        ["[overlay drift] x"] * 21 + ["Missing: A"] * 5
    )
    assert breakdown == "21 overlay drift, 5 field missing"


def test_format_breakdown_empty_input():
    """No issues → no breakdown text."""
    from src.api.review_alignment import format_qa_breakdown
    assert format_qa_breakdown([]) == ""
    assert format_qa_breakdown(None) == ""


def test_categorize_handles_none_and_non_strings():
    """Defensive: a stray None/int/dict in the list must not raise."""
    from src.api.review_alignment import categorize_qa_issues
    cats = categorize_qa_issues([
        "Missing: A",
        None,
        42,
        {"not": "a string"},
        "[overlay drift] x",
    ])
    assert cats == {"field missing": 1, "overlay drift": 1}


# ── Rollup wire-up ──────────────────────────────────────────────────────────


def test_rollup_banner_shows_breakdown_for_multi_category_qa_fail():
    """End-to-end: when QA fails with mixed-class issues, the rollup
    `issues` list contains the categorized banner string AND the
    structured `qa_categories` dict is exposed for the template."""
    from src.api.review_alignment import compute_review_alignment

    rfq = {
        "id": "rfq_x", "agency": "cchcs",
        "requestor_name": "Test", "requestor_email": "x@y.z",
        "due_date": "2026-05-15",
        "line_items": [{"description": "A", "qty": 1, "unit_price": 1.0}],
    }
    manifest = {
        "agency_name": "CCHCS",
        "field_audit": {
            "_qa_passed": False,
            "_qa_summary": {
                "critical_issues": (
                    ["[overlay drift] r1 pg1: text '...' is 8pt outside"] * 21
                    + ["Missing: COMPANY NAME"] * 5
                ),
            },
        },
        "source_validation": {},
    }
    out = compute_review_alignment(
        rfq=rfq, manifest=manifest, agency_cfg={"name": "CCHCS"},
    )
    rollup = out["rollup"]
    # qa_categories surfaces as structured data
    assert rollup["qa_categories"] == {"overlay drift": 21, "field missing": 5}
    # Banner string includes the breakdown
    qa_issue = next(
        (i for i in rollup["issues"] if "Form QA failed" in i),
        "",
    )
    assert "26 critical" in qa_issue
    assert "21 overlay drift" in qa_issue
    assert "5 field missing" in qa_issue


def test_rollup_banner_single_category_compact_form():
    """When all 7 failures are the same class, banner reads cleaner."""
    from src.api.review_alignment import compute_review_alignment

    rfq = {
        "id": "rfq_x", "agency": "cchcs",
        "requestor_name": "Test", "requestor_email": "x@y.z",
        "due_date": "2026-05-15",
        "line_items": [{"description": "A", "qty": 1, "unit_price": 1.0}],
    }
    manifest = {
        "agency_name": "CCHCS",
        "field_audit": {
            "_qa_passed": False,
            "_qa_summary": {
                # Match the literal shape from verify_704b_computations:
                # "Row N: qty × $price = $X, but extension shows $Y"
                "critical_issues": [
                    f"Row {i}: 12 × $14.55 = $174.60, but extension shows $0.00"
                    for i in range(1, 8)
                ],
            },
        },
        "source_validation": {},
    }
    out = compute_review_alignment(
        rfq=rfq, manifest=manifest, agency_cfg={"name": "CCHCS"},
    )
    rollup = out["rollup"]
    qa_issue = next(
        (i for i in rollup["issues"] if "Form QA failed" in i),
        "",
    )
    assert "7 computation" in qa_issue
    # Single-category form drops the redundant total count
    assert qa_issue == "Form QA failed — 7 computation"


def test_rollup_qa_passed_no_categories():
    """When QA passes (or hasn't run), qa_categories is an empty dict."""
    from src.api.review_alignment import compute_review_alignment

    rfq = {
        "id": "rfq_x", "agency": "cchcs",
        "requestor_name": "Test", "requestor_email": "x@y.z",
        "due_date": "2026-05-15",
        "line_items": [{"description": "A", "qty": 1, "unit_price": 1.0}],
    }
    manifest = {
        "agency_name": "CCHCS",
        "field_audit": {
            "_qa_passed": True,
            "_qa_summary": {"critical_issues": []},
        },
        "source_validation": {},
    }
    out = compute_review_alignment(
        rfq=rfq, manifest=manifest, agency_cfg={"name": "CCHCS"},
    )
    rollup = out["rollup"]
    assert rollup["qa_categories"] == {}
