"""Inspector gate — math + identity + coverage + cost-basis reconcile.

Per §0 Inspector duty + Job #1 acceptance: every Spine quote shipped
through the 3-quote send gate runs ``reconcile_quote_to_package``
first; a non-clean report blocks the send. These tests verify the
reconciler catches real-shape drift.

End-to-end: build a Spine ``Quote`` + ``EmailContract``, render the
package via the appropriate adapter, then re-parse the rendered output
and assert the Inspector either passes (happy path) or fails with the
right issue kind + severity (negative path).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.spine.email_contract import ContractLineItem, EmailContract
from src.spine.inspector import (
    InspectorReport,
    reconcile_format_a,
    reconcile_format_b,
    reconcile_quote_to_package,
)
from src.spine.model import LineItem, Quote

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T703B = "tests/fixtures/703b_blank.pdf"
_T704B = "tests/fixtures/704b_blank.pdf"
_TBIDPKG = "tests/fixtures/cchcs_bidpkg_blank.pdf"
_TPACKET = "tests/fixtures/unified_ingest/cchcs_packet_preq.pdf"

_FORMAT_B_REFS = (_T703B, _T704B, _TBIDPKG)
_B_FIXTURES_PRESENT = all((_REPO_ROOT / p).is_file() for p in _FORMAT_B_REFS)
_A_FIXTURE_PRESENT = (_REPO_ROOT / _TPACKET).is_file()

_needs_b = pytest.mark.skipif(
    not _B_FIXTURES_PRESENT, reason="Format-B template fixtures missing")
_needs_a = pytest.mark.skipif(
    not _A_FIXTURE_PRESENT, reason="Format-A packet fixture missing")


# ── builders ──────────────────────────────────────────────────────────


def _line(line_no, description, *, mfg=None, qty=4, unit_price_cents=12500,
          cost_cents=8000):
    return LineItem(
        line_no=line_no,
        description=description,
        mfg_number=mfg,
        qty=qty,
        uom="EA",
        cost_cents=cost_cents,
        cost_source_url="https://example.com/item",
        cost_validated_at=datetime.now(timezone.utc),
        unit_price_cents=unit_price_cents,
    )


def _quote(line_items=None, *, quote_id="Q-insp-001", sol="10848901",
           tax_rate_bps=775):
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number=sol,
        line_items=line_items or [
            _line(1, "Nitrile Exam Gloves, Large", mfg="N-GLV-L",
                  qty=6, unit_price_cents=11400, cost_cents=7850),
            _line(2, "Isolation Gowns, yellow", mfg="ISO-GWN",
                  qty=4, unit_price_cents=26500, cost_cents=18200),
        ],
        tax_rate_bps=tax_rate_bps,
    )


def _contract_b(*, quote_id="Q-insp-001", sol="10848901",
                buyer_name="Grace Pfost", attachment_refs=_FORMAT_B_REFS,
                required_forms=None):
    kwargs = dict(
        contract_id=f"contract_{quote_id}_b",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number=sol,
        buyer_name=buyer_name,
        buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        release_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
        due_date=datetime(2026, 5, 25, tzinfo=timezone.utc),
        line_items=[
            ContractLineItem(line_no=1, description="Nitrile Exam Gloves", qty=6, uom="EA"),
            ContractLineItem(line_no=2, description="Isolation Gowns", qty=4, uom="EA"),
        ],
        attachment_refs=list(attachment_refs),
        response_packaging="separate_pdfs",
    )
    if required_forms is not None:
        kwargs["required_forms"] = required_forms
    return EmailContract(**kwargs)


def _contract_a(*, quote_id="Q-insp-pkt", sol="10843276",
                attachment_refs=(_TPACKET,)):
    return EmailContract(
        contract_id=f"contract_{quote_id}_a",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number=sol,
        line_items=[
            ContractLineItem(line_no=1, description="Handheld Scanner", qty=15, uom="EA"),
        ],
        attachment_refs=list(attachment_refs),
        response_packaging="single_pdf",
    )


# ── Report shape ──────────────────────────────────────────────────────


def test_report_severity_derives_from_counts():
    rep = InspectorReport(
        ok=True, quote_id="Q", response_packaging="separate_pdfs",
        blocking_count=0, warning_count=0,
    )
    assert rep.severity == "ok"
    rep2 = rep.model_copy(update={"warning_count": 1})
    assert rep2.severity == "warning"
    rep3 = rep.model_copy(update={"blocking_count": 1, "ok": False})
    assert rep3.severity == "blocking"


def test_no_contract_blocks_immediately():
    rep = reconcile_quote_to_package(_quote(), None)
    assert rep.ok is False
    assert rep.blocking_count == 1
    assert rep.issues[0].kind == "render"
    assert "EmailContract" in rep.issues[0].detail


# ── Format B — happy + negatives ─────────────────────────────────────


@_needs_b
def test_format_b_happy_path_reconciles_clean(tmp_path):
    rep = reconcile_format_b(_quote(), _contract_b(), output_dir=str(tmp_path))
    # Print issues to aid debugging if this regresses.
    assert rep.ok, [(i.kind, i.location, i.detail) for i in rep.issues]
    assert rep.blocking_count == 0
    assert rep.line_items_checked == 2
    # 703 + 704b + bidpkg should all be checked.
    assert {"703b", "704b", "bidpkg"} <= set(rep.forms_checked)


@_needs_b
def test_format_b_dispatcher_picks_format_b_for_separate_pdfs(tmp_path):
    rep = reconcile_quote_to_package(
        _quote(), _contract_b(), output_dir=str(tmp_path))
    assert rep.response_packaging == "separate_pdfs"
    assert "704b" in rep.forms_checked


def _render_b_and_collect(quote, contract, out_dir):
    """Helper — render Format B once and return the per-form output paths
    dict in the shape ``reconcile_format_b(forms_paths=...)`` expects."""
    from src.spine.forms_render import render_cchcs_forms_via_legacy
    res = render_cchcs_forms_via_legacy(
        quote, contract, output_dir=str(out_dir), strict=False)
    forms = res.get("forms") or {}
    return {
        "703": (forms.get("703") or {}).get("output_path", ""),
        "704b": (forms.get("704b") or {}).get("output_path", ""),
        "bidpkg": (forms.get("bidpkg") or {}).get("output_path", ""),
    }


@_needs_b
def test_format_b_identity_mismatch_caught_against_pre_rendered(tmp_path):
    """Render the package for the real contract; reconcile that SAME
    rendered output against a quote that claims a different sol# — the
    703B + 704B identity checks must flag the drift."""
    real_quote = _quote(sol="10848901")
    contract = _contract_b(sol="10848901")
    paths = _render_b_and_collect(real_quote, contract, tmp_path)
    # Drift quote claims a different sol; the rendered output still
    # reads 10848901 from the real_quote.
    drift_quote = _quote(sol="99999999")
    rep = reconcile_format_b(drift_quote, contract, forms_paths=paths)
    assert rep.ok is False
    identity = [i for i in rep.issues if i.kind == "identity"]
    locs = [i.location for i in identity]
    assert any("703B" in loc for loc in locs), locs
    assert any("704B" in loc for loc in locs), locs


@_needs_b
def test_format_b_math_mismatch_caught_against_pre_rendered(tmp_path):
    """Render with real prices; reconcile the same output against a quote
    whose per-line prices differ — every row must flag a math mismatch."""
    real_quote = _quote()
    contract = _contract_b()
    paths = _render_b_and_collect(real_quote, contract, tmp_path)
    drift_quote = _quote(line_items=[
        _line(1, "Nitrile", qty=6, unit_price_cents=99900),
        _line(2, "Isolation", qty=4, unit_price_cents=88800),
    ])
    rep = reconcile_format_b(drift_quote, contract, forms_paths=paths)
    assert rep.ok is False
    math = [i for i in rep.issues if i.kind == "math"]
    assert math, "expected math issues for diverged unit prices"
    # Both lines should flag — unit price + subtotal + the merchandise
    # subtotal field overall.
    assert any("PRICE PER UNIT" in i.location for i in math)
    assert any("fill_154" in i.location for i in math)


@_needs_b
def test_format_b_coverage_fails_when_a_required_form_missing(tmp_path):
    """A contract that declares 703b/704b/bidpkg but ships only the 703B
    template — the bidpkg won't render; coverage must flag every missing
    declared form."""
    contract = _contract_b(attachment_refs=(_T703B,))  # only 703B
    rep = reconcile_format_b(_quote(), contract, output_dir=str(tmp_path))
    assert rep.ok is False
    coverage = [i for i in rep.issues if i.kind == "coverage"]
    assert coverage, "expected coverage issues when forms missing"
    locs = " ".join(i.location for i in coverage)
    assert "704" in locs or "bidpkg" in locs


@_needs_b
def test_format_b_cost_basis_blocks_stale_high_cost_line(tmp_path):
    """A line with cost ≥ $100 whose cost_validated_at is > 30 days old
    must be flagged by the Inspector (mirrors the model's finalized
    precondition; double-checks for any code path that skipped it)."""
    stale_when = datetime.now(timezone.utc) - timedelta(days=45)
    expensive_line = LineItem(
        line_no=1, description="Vital Signs Monitor", mfg_number="VSM-1",
        qty=2, uom="EA",
        cost_cents=42000,       # $420 — over the $100 threshold
        cost_source_url="https://example.com/monitor",
        cost_validated_at=stale_when,
        unit_price_cents=58000,
    )
    q = _quote(line_items=[expensive_line])
    rep = reconcile_format_b(q, _contract_b(), output_dir=str(tmp_path))
    cb = [i for i in rep.issues if i.kind == "cost_basis"]
    assert cb, "expected cost_basis issue for stale validation"
    assert rep.ok is False


# ── Format A — happy ──────────────────────────────────────────────────


@_needs_a
def test_format_a_happy_path_reconciles_clean(tmp_path):
    """Single-PDF packet — packet adapter renders, inspector verifies
    the match_report covers every priced line at the operator's typed
    unit price."""
    q = Quote(
        quote_id="Q-insp-pkt", agency="CCHCS", facility="CHCF",
        solicitation_number="10843276",
        line_items=[
            LineItem(
                line_no=1,
                description="Handheld Scanner w/ USB cable and standard cradle",
                mfg_number="DS8178",
                qty=15, uom="EA",
                cost_cents=29500,
                cost_source_url="https://example.com/scanner",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=39500,
            )
        ],
        tax_rate_bps=775,
    )
    rep = reconcile_format_a(q, _contract_a(), output_dir=str(tmp_path))
    assert rep.ok, [(i.kind, i.location, i.detail) for i in rep.issues]
    assert rep.response_packaging == "single_pdf"
    assert "packet" in rep.forms_checked
    assert rep.line_items_checked >= 1


@_needs_a
def test_format_a_dispatcher_picks_format_a_for_single_pdf(tmp_path):
    q = Quote(
        quote_id="Q-insp-pkt", agency="CCHCS", facility="CHCF",
        solicitation_number="10843276",
        line_items=[
            LineItem(
                line_no=1, description="Handheld Scanner", mfg_number="DS8178",
                qty=15, uom="EA",
                cost_cents=29500,
                cost_source_url="https://example.com/scanner",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=39500,
            )
        ],
        tax_rate_bps=775,
    )
    rep = reconcile_quote_to_package(q, _contract_a(), output_dir=str(tmp_path))
    assert rep.response_packaging == "single_pdf"
    assert "packet" in rep.forms_checked


# ── Cost-basis pure (no render) ──────────────────────────────────────


def test_cost_basis_skips_low_cost_lines():
    """Lines with cost below the $100 threshold need no source/freshness."""
    # cheap_line: cost $50, no source, no validation — fine.
    cheap = LineItem(
        line_no=1, description="Cheap Item", qty=10, uom="EA",
        cost_cents=5000, unit_price_cents=7500,
    )
    q = Quote(
        quote_id="Q-cheap", agency="CCHCS", facility="SAC",
        solicitation_number="X", line_items=[cheap], tax_rate_bps=775,
    )
    # Inspector directly: no contract → blocks on contract, but the
    # cost_basis path inside is exercised before that check returns.
    from src.spine.inspector import _check_cost_basis
    assert _check_cost_basis(q) == []


# ── _to_cents safety rails — parser bound + ceiling ──────────────────


def test_to_cents_accepts_normal_us_format():
    """US format — grouping commas, decimal point, bare integers — must
    keep working exactly as today (no regression in the happy path)."""
    from src.spine.inspector import _to_cents

    assert _to_cents("1,060.00") == 106000
    assert _to_cents("684") == 68400
    assert _to_cents("0.05") == 5
    assert _to_cents(" 5955.25") == 595525
    assert _to_cents("$1,234.56") == 123456
    assert _to_cents("0") == 0
    assert _to_cents("0.00") == 0


def test_to_cents_rejects_eu_format():
    """``_to_cents("1234,56")`` must return None — NOT 12345600 cents
    ($123,456). Stripping commas before checking for ``.`` decimal would
    100x the value. US-only today; this closes the class before a non-US
    buyer template ever lands."""
    from src.spine.inspector import _to_cents

    assert _to_cents("1234,56") is None
    assert _to_cents("12,5") is None
    assert _to_cents("0,99") is None
    # Edge: trailing zeros — still EU format, still rejected.
    assert _to_cents("100,00") is None


def test_to_cents_rejects_implausible_ceiling():
    """A cell value over $10B is almost certainly a parse artifact
    (concatenated sol#, mangled scrape). Return None so the caller flags
    it as a math issue rather than computing off junk."""
    from src.spine.inspector import _to_cents

    # 10 trillion dollars + change — way past any realistic line item.
    assert _to_cents("99999999999999.99") is None
    # Just over $10B.
    assert _to_cents("10000000000.01") is None
    # Just under $10B — accepted (still implausible but the ceiling is
    # for catching mangled bytes, not for editorializing on real prices).
    assert _to_cents("9999999999.99") == 999999999999


def test_to_cents_handles_garbage_input():
    """Empty / whitespace / non-numeric still returns None (no regression)."""
    from src.spine.inspector import _to_cents

    assert _to_cents(None) is None
    assert _to_cents("") is None
    assert _to_cents("   ") is None
    assert _to_cents("not a number") is None
    assert _to_cents("$") is None


# ── 704B page-mapping discovery — survives 15 vs 23 page-1 capacity ──


@_needs_b
def test_reconcile_format_b_handles_15_items_page1_only(tmp_path):
    """Fifteen items — fills page 1 (current filler capacity), no page 2.
    Discovery must find every QTYRow* the filler populated and reconcile
    each line clean."""
    items = [
        _line(i, f"Item {i}", mfg=f"MFG-{i}",
              qty=2 + (i % 3),
              unit_price_cents=10000 + i * 250,
              cost_cents=5000 + i * 100)
        for i in range(1, 16)  # 15 items
    ]
    q = _quote(line_items=items)
    contract = _contract_b()
    rep = reconcile_format_b(q, contract, output_dir=str(tmp_path))
    assert rep.ok, [(i.kind, i.location, i.detail) for i in rep.issues]
    assert rep.line_items_checked == 15


@_needs_b
def test_reconcile_format_b_handles_30_items_across_two_pages(tmp_path):
    """Thirty items — 15 on page 1 + 15 on page 2 (filled into Row1_2..).
    Discovery must walk page 1 then page 2 in order and reconcile every
    line. This is the case the hardcoded ``page_size = 15`` got right by
    coincidence; the discovery rewrite must match that behavior AND
    survive a future filler change to 23-row page-1 capacity."""
    items = [
        _line(i, f"Item {i}", mfg=f"MFG-{i}",
              qty=1 + (i % 5),
              unit_price_cents=5000 + i * 100,
              cost_cents=2500 + i * 50)
        for i in range(1, 31)  # 30 items
    ]
    q = _quote(line_items=items)
    contract = _contract_b()
    rep = reconcile_format_b(q, contract, output_dir=str(tmp_path))
    # Filler capacity on the standard 704B template is 15 page-1 + 16
    # page-2 = 31 form-field slots. 30 items fits without overflow, so
    # every line should be field-level checked + clean.
    assert rep.ok, [(i.kind, i.location, i.detail) for i in rep.issues]
    assert rep.line_items_checked == 30


@_needs_b
def test_reconcile_format_b_discovery_flags_page2_drift(tmp_path):
    """Render 30 items with real prices; reconcile the SAME output against
    a drift quote whose page-2 line prices differ. Discovery must walk
    into the ``_2``-suffixed rows and surface the per-line math drift
    (the old hardcoded mapping happened to do this too; the new
    discovery code must preserve it)."""
    items = [
        _line(i, f"Item {i}", mfg=f"MFG-{i}",
              qty=2, unit_price_cents=10000 + i * 100,
              cost_cents=5000)
        for i in range(1, 31)
    ]
    real_q = _quote(line_items=items)
    contract = _contract_b()
    paths = _render_b_and_collect(real_q, contract, tmp_path)
    # Drift only on the page-2 lines (16+).
    drift_items = [
        _line(i, f"Item {i}", mfg=f"MFG-{i}",
              qty=2,
              unit_price_cents=(10000 + i * 100) if i <= 15 else 77700,
              cost_cents=5000)
        for i in range(1, 31)
    ]
    drift_q = _quote(line_items=drift_items)
    rep = reconcile_format_b(drift_q, contract, forms_paths=paths)
    assert rep.ok is False
    math = [i for i in rep.issues if i.kind == "math"]
    # Every page-2 line should flag PRICE PER UNIT*_2 + SUBTOTAL*_2 mismatch.
    page2_price_issues = [
        i for i in math
        if "PRICE PER UNIT" in i.location and i.location.endswith("_2")
    ]
    assert page2_price_issues, (
        "expected per-line page-2 (_2 suffix) PRICE PER UNIT drift, "
        f"got {[i.location for i in math]}"
    )
