"""Fail-closed contract validator on quote render.

## Why

Tonight's golden 1-item-quote E2E (PR #523) caught two real renderer
bugs that produced a $0.00 PDF (`['no items', '$0 total']`). The
contract validator already detected them but only LOGGED a warning —
the rendered PDF still landed on disk and the function returned
`ok=True` with a path. An operator could attach that $0 quote to a
buyer email.

This PR converts the validator to fail-closed: when a contract violation
is detected, the just-written PDF is unlinked, the allocated quote
number is rolled back, and the renderer returns
`{ok: False, error: "contract_violations", violations: [...]}`. The
caller (route layer / operator UI) can then surface a clear error
instead of a $0 quote masquerading as a successful render.

## What this test pins

* A render call that produces a $0 / no-items quote returns ok=False
* The PDF file is NOT on disk after the failed render
* `violations` is in the result so the caller can show specifics
* The quote number is rolled back (next valid render reuses it, no gap)
* Successful renders are unaffected (positive case)
"""
from __future__ import annotations

import os

import pytest


def _build_invalid_rfq_no_items(tmp_path) -> dict:
    """RFQ that resolves cleanly to canonical Barstow but carries
    items in a shape the renderer wrapper doesn't read (the exact
    shape mismatch the golden test caught). Renders $0.00."""
    return {
        "id": "test_invalid_no_items",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "department": "Skilled Nursing Unit",
        "requestor_name": "Test Buyer",
        "requestor_email": "buyer@calvet.ca.gov",
        "delivery_location": "Calipatria State Prison ship-to placeholder",
        "ship_to": "CAL",
        "due_date": "2026-04-30",
        # Items in `items` only — NOT in `line_items`. Renderer wrapper
        # reads `rfq.line_items` so this triggers the $0 / no-items
        # contract failure that this test asserts is now fail-closed.
        "items": [{
            "description": "Stanley RoamAlert Wrist Strap",
            "qty": 1,
            "uom": "EA",
            "mfg_number": "WRS-100",
            "unit_price": 540.00,
        }],
        "status": "ready_to_send",
    }


def _build_valid_rfq(tmp_path) -> dict:
    """The same RFQ in the shape the renderer wrapper actually reads
    (line_items). This MUST render successfully — the positive case."""
    return {
        "id": "test_valid_one_item",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "department": "Skilled Nursing Unit",
        "requestor_name": "Test Buyer",
        "requestor_email": "buyer@calvet.ca.gov",
        "due_date": "2026-04-30",
        "line_items": [{
            "description": "Stanley RoamAlert Wrist Strap",
            "qty": 1,
            "uom": "EA",
            "mfg_number": "WRS-100",
            "unit_price": 540.00,
            "price_per_unit": 540.00,
        }],
        "status": "ready_to_send",
    }


@pytest.mark.timeout(60)
def test_invalid_quote_returns_ok_false(tmp_path):
    """The headline assertion. A render that fails the contract MUST
    return ok=False so the calling route doesn't silently surface a
    $0 PDF as a successful quote."""
    from src.forms.quote_generator import generate_quote_from_rfq
    out = str(tmp_path / "invalid_quote.pdf")
    result = generate_quote_from_rfq(_build_invalid_rfq_no_items(tmp_path), out)
    assert isinstance(result, dict)
    assert result.get("ok") is False, (
        f"Expected ok=False on contract violation, got {result!r}. "
        "The fail-closed gate isn't firing — operator could attach a "
        "$0 quote to a buyer email."
    )


@pytest.mark.timeout(60)
def test_invalid_quote_pdf_is_unlinked(tmp_path):
    """The just-written invalid PDF MUST be deleted so the operator
    physically cannot attach it. Disk-state is the actual gate; an
    ok=False that leaves the file behind is exploitable."""
    from src.forms.quote_generator import generate_quote_from_rfq
    out = str(tmp_path / "invalid_quote.pdf")
    generate_quote_from_rfq(_build_invalid_rfq_no_items(tmp_path), out)
    assert not os.path.exists(out), (
        f"Invalid quote PDF still on disk at {out!r} after fail-closed "
        "render. The unlink step didn't run."
    )


@pytest.mark.timeout(60)
def test_invalid_quote_returns_violations_for_caller(tmp_path):
    """The caller (route handler / UI) needs the specific violations
    so it can show the operator WHY the render failed — 'no items',
    '$0 total', etc. — not just a generic error."""
    from src.forms.quote_generator import generate_quote_from_rfq
    out = str(tmp_path / "invalid_quote.pdf")
    result = generate_quote_from_rfq(_build_invalid_rfq_no_items(tmp_path), out)
    assert "violations" in result, (
        f"Result missing 'violations' key: {result!r}"
    )
    assert isinstance(result["violations"], list)
    assert len(result["violations"]) > 0, (
        f"Empty violations list on a fail-closed render: {result!r}"
    )
    # Specific violations the validator emits for $0/no-items quotes
    joined = " ".join(result["violations"]).lower()
    assert "items" in joined or "total" in joined, (
        f"Violations don't mention items/total: {result['violations']!r}"
    )


@pytest.mark.timeout(60)
def test_invalid_quote_carries_error_marker(tmp_path):
    """Distinct error code so the caller can branch on it instead of
    parsing free-text. `contract_violations` is the marker."""
    from src.forms.quote_generator import generate_quote_from_rfq
    out = str(tmp_path / "invalid_quote.pdf")
    result = generate_quote_from_rfq(_build_invalid_rfq_no_items(tmp_path), out)
    assert result.get("error") == "contract_violations", (
        f"Expected error='contract_violations', got {result.get('error')!r}"
    )


@pytest.mark.timeout(60)
def test_valid_quote_still_renders(tmp_path):
    """Positive case — a contract-valid quote MUST still render
    successfully. The fail-closed gate cannot break the happy path."""
    from src.forms.quote_generator import generate_quote_from_rfq
    out = str(tmp_path / "valid_quote.pdf")
    result = generate_quote_from_rfq(_build_valid_rfq(tmp_path), out)
    assert result.get("ok") is True, (
        f"Valid quote render returned ok=False — fail-closed gate is "
        f"over-triggering. Result: {result!r}"
    )
    rendered = result.get("path") or out
    assert os.path.exists(rendered), (
        f"Valid quote PDF not on disk at {rendered!r}"
    )
    assert os.path.getsize(rendered) > 1000  # non-empty


@pytest.mark.timeout(60)
def test_invalid_quote_rolls_back_quote_number(tmp_path):
    """Quote numbers are sequential (R26Q1, R26Q2, ...) and represent
    audit trail. A failed render should roll back the allocation so the
    next successful render reuses the number — no gap that looks like
    a missing quote in the sequence."""
    from src.forms.quote_generator import generate_quote_from_rfq, peek_next_quote_number
    next_before = peek_next_quote_number()
    out = str(tmp_path / "invalid_quote.pdf")
    generate_quote_from_rfq(_build_invalid_rfq_no_items(tmp_path), out)
    next_after = peek_next_quote_number()
    assert next_before == next_after, (
        f"Quote number advanced {next_before} → {next_after} despite "
        "fail-closed render. Rollback didn't fire — next operator quote "
        "will get a gap in the sequence."
    )
