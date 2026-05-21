"""Spine → legacy CCHCS packet adapter — src/spine/packet_render.py.

The adapter delegates to the verified legacy filler. These tests prove
its three jobs — locate the buyer's packet PDF, map Spine quote prices
onto its rows, surface the filled bytes — plus loud failure when no
contract / no packet PDF is in hand.

Fixture: tests/fixtures/unified_ingest/cchcs_packet_preq.pdf — a real
CCHCS Non-Cloud RFQ packet, sol# 10843276, one line item (DS8178
handheld scanner, qty 15).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.spine.email_contract import ContractLineItem, EmailContract
from src.spine.model import LineItem, Quote
from src.spine.packet_render import (
    _match_quote_lines_to_packet,
    _resolve_source_pdf,
    render_cchcs_packet_via_legacy,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_REL = "tests/fixtures/unified_ingest/cchcs_packet_preq.pdf"
_FIXTURE_ABS = _REPO_ROOT / _FIXTURE_REL


# ── builders ──────────────────────────────────────────────────────────


def _line(line_no, description, *, mfg=None, qty=15, unit_price_cents=39500):
    return LineItem(
        line_no=line_no,
        description=description,
        mfg_number=mfg,
        qty=qty,
        uom="EA",
        cost_cents=29500,
        cost_source_url="https://example.com/scanner",
        cost_validated_at=datetime.now(timezone.utc),
        unit_price_cents=unit_price_cents,
    )


def _quote(line_items, *, quote_id="Q-pkt-001", tax_rate_bps=775):
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="10843276",
        line_items=line_items,
        tax_rate_bps=tax_rate_bps,
    )


def _contract(attachment_refs, *, quote_id="Q-pkt-001"):
    return EmailContract(
        contract_id=f"contract_{quote_id}_1747000000",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="10843276",
        line_items=[
            ContractLineItem(
                line_no=1,
                description="Handheld Scanner w/ USB cable and standard cradle",
                qty=15,
                uom="EA",
            )
        ],
        attachment_refs=list(attachment_refs),
    )


def _parsed(items):
    """Minimal parse_cchcs_packet-shaped dict for matcher unit tests."""
    return {"ok": True, "line_items": items}


# ── fixture sanity ────────────────────────────────────────────────────


def test_fixture_packet_present():
    assert _FIXTURE_ABS.is_file(), f"missing test fixture {_FIXTURE_ABS}"


# ── _resolve_source_pdf ───────────────────────────────────────────────


def test_resolve_source_pdf_finds_packet_by_relative_ref():
    src = _resolve_source_pdf(_contract([_FIXTURE_REL]))
    assert src is not None
    assert Path(src).is_file()
    assert Path(src).name == "cchcs_packet_preq.pdf"


def test_resolve_source_pdf_finds_packet_by_absolute_ref():
    src = _resolve_source_pdf(_contract([str(_FIXTURE_ABS)]))
    assert src is not None and Path(src).is_file()


def test_resolve_source_pdf_none_when_no_refs():
    assert _resolve_source_pdf(_contract([])) is None


def test_resolve_source_pdf_none_when_refs_bogus():
    assert _resolve_source_pdf(_contract(["nope/not_here.pdf"])) is None


# ── _match_quote_lines_to_packet ──────────────────────────────────────


def test_match_by_mfg_number_wins_over_description():
    parsed = _parsed([
        {"row_index": 1, "description": "totally unrelated wording",
         "mfg_number": "DS8178", "qty": 15},
    ])
    q = _quote([_line(1, "scanner unit", mfg="ds8178", unit_price_cents=39500)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    assert overrides == {1: {"unit_price": 395.0}}
    assert report[0]["strategy"] == "mfg_number"
    assert report[0]["confidence"] == 1.0


def test_match_by_description_jaccard():
    parsed = _parsed([
        {"row_index": 1, "description": "Handheld Scanner USB cable cradle",
         "mfg_number": "", "qty": 15},
    ])
    q = _quote([_line(1, "Handheld Scanner USB cable cradle", mfg=None,
                      unit_price_cents=40000)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    assert overrides == {1: {"unit_price": 400.0}}
    assert report[0]["strategy"] == "description"


def test_match_positional_fallback():
    parsed = _parsed([
        {"row_index": 2, "description": "zzz qqq", "mfg_number": "", "qty": 1},
    ])
    q = _quote([_line(2, "completely different alpha beta", mfg=None,
                      unit_price_cents=12300)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    assert overrides == {2: {"unit_price": 123.0}}
    assert report[0]["strategy"] == "positional"


def test_match_unmatched_row_omitted_from_overrides():
    parsed = _parsed([
        {"row_index": 1, "description": "xyz", "mfg_number": "", "qty": 1},
    ])
    # quote line_no 5 ≠ row_index 1 → no positional match either.
    q = _quote([_line(5, "alpha beta gamma", mfg=None)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    assert overrides == {}
    assert report[0]["strategy"] == "unmatched"


def test_match_unpriced_line_recorded_but_not_overridden():
    parsed = _parsed([
        {"row_index": 1, "description": "scanner", "mfg_number": "DS8178",
         "qty": 15},
    ])
    q = _quote([_line(1, "scanner", mfg="DS8178", unit_price_cents=0)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    assert overrides == {}  # zero-price row left blank for operator review
    assert "unpriced_skip" in report[0]["strategy"]


def test_match_each_quote_line_consumed_once():
    parsed = _parsed([
        {"row_index": 1, "description": "scanner", "mfg_number": "DS8178",
         "qty": 1},
        {"row_index": 2, "description": "scanner", "mfg_number": "DS8178",
         "qty": 1},
    ])
    q = _quote([_line(1, "scanner", mfg="DS8178", unit_price_cents=10000)])
    overrides, report = _match_quote_lines_to_packet(q, parsed)
    # Only one quote line exists — row 1 claims it, row 2 cannot reuse it.
    assert set(overrides.keys()) == {1}


# ── render_cchcs_packet_via_legacy ────────────────────────────────────


def test_render_happy_path_against_real_packet():
    q = _quote([_line(1, "Handheld Scanner w/ USB cable and standard cradle",
                      mfg="DS8178", qty=15, unit_price_cents=39500)])
    res = render_cchcs_packet_via_legacy(q, _contract([_FIXTURE_REL]),
                                         strict=False)
    assert res["source_pdf"].endswith("cchcs_packet_preq.pdf")
    assert res["pdf_bytes"][:5] == b"%PDF-", "adapter did not return PDF bytes"
    assert Path(res["output_path"]).is_file()
    fill = res["fill_result"]
    assert fill.get("rows_priced") == 1, "the priced row did not reach the packet"
    assert fill.get("grand_total", 0) > 0


def test_render_no_contract_fails_loudly():
    q = _quote([_line(1, "scanner", mfg="DS8178")])
    res = render_cchcs_packet_via_legacy(q, None)
    assert res["ok"] is False
    assert not res["pdf_bytes"]
    assert "EmailContract" in res["error"]


def test_render_no_source_pdf_fails_loudly():
    q = _quote([_line(1, "scanner", mfg="DS8178")])
    res = render_cchcs_packet_via_legacy(q, _contract(["bogus/missing.pdf"]))
    assert res["ok"] is False
    assert not res["pdf_bytes"]
    assert "packet PDF" in res["error"]


def test_render_operator_tax_rate_flows_into_packet_totals():
    """The Spine quote's tax_rate_bps must drive the packet totals — not
    a zip-derived CDTFA lookup. Two renders, same lines, different rate."""
    line_args = dict(mfg="DS8178", qty=15, unit_price_cents=39500)
    q_notax = _quote([_line(1, "Handheld Scanner w/ USB cable and standard "
                            "cradle", **line_args)],
                     quote_id="Q-tax-0", tax_rate_bps=0)
    q_tax = _quote([_line(1, "Handheld Scanner w/ USB cable and standard "
                          "cradle", **line_args)],
                   quote_id="Q-tax-10", tax_rate_bps=1000)
    r0 = render_cchcs_packet_via_legacy(q_notax, _contract([_FIXTURE_REL],
                                        quote_id="Q-tax-0"), strict=False)
    r10 = render_cchcs_packet_via_legacy(q_tax, _contract([_FIXTURE_REL],
                                         quote_id="Q-tax-10"), strict=False)
    g0 = r0["fill_result"]["grand_total"]
    g10 = r10["fill_result"]["grand_total"]
    # 10% tax on a 15 × $395.00 = $5,925.00 subtotal → +$592.50.
    assert g10 == pytest.approx(g0 + 592.50, abs=0.01)
