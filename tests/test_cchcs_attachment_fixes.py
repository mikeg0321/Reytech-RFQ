"""Regression guards for the CCHCS attachment-filler fidelity fixes
(2026-05-21, from Mike's rendered-packet review).

The bugs these lock down:
  - DVBE 843 Section 2 broker radio ticked "is a broker" (must be "not
    a broker"); both radio widgets ended up checked.
  - DVBE 843 SCPRS Ref. Number filled with the solicitation # (it is
    "FOR STATE USE ONLY" — must be blank).
  - DVBE 843 2nd-owner / manager / principal lines stuffed with "N/A".
  - DVBE 843 Section 3 (equipment-rental) boxes checked when Reytech
    rents no equipment.
  - STD 204 FEIN truncated by a 9-cell comb ("47-4588061" → "47-458806").
  - STD 204 Section 6 (state-use-only) UNIT/SECTION filled.
  - Signatures rendered micro-sized (fit-to-thin-rect).
"""
from __future__ import annotations

import io

import pytest

pypdf = pytest.importorskip("pypdf")

from src.forms.cchcs_attachment_fillers import (
    _signature_draw_box,
    fill_dvbe_843,
    fill_std204,
)

_REYTECH = {
    "company_name": "Reytech Inc.",
    "cert_number": "2002605",
    "representative": "Michael Guadan",
    "title": "Owner",
    "phone": "949-229-1575",
    "address": "30 Carnoustie Way Trabuco Canyon, CA 92679",
    "email": "sales@reytechinc.com",
    "fein": "47-4588061",
    "street": "30 Carnoustie Way",
    "city": "Trabuco Canyon",
    "state": "CA",
    "zip": "92679",
}
_PARSED = {
    "header": {"solicitation_number": "10843276"},
    "line_items": [{"row_index": 1, "description": "Handheld Scanner", "qty": 15}],
}


def _fields(bytes_io):
    return pypdf.PdfReader(bytes_io).get_fields() or {}


def _v(fields, name):
    spec = fields.get(name)
    return ("" if spec is None else str(spec.get("/V") or "")).strip()


# ── DVBE 843 ──────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def dvbe_fields():
    out = fill_dvbe_843(_REYTECH, _PARSED)
    if out is None:
        pytest.skip("dvbe_843_blank.pdf template not available")
    return _fields(out)


def test_dvbe_843_broker_radio_declares_not_a_broker(dvbe_fields):
    """Section 2 radio MUST resolve to /1 — the top box, 'the DVBE is
    NOT a broker or agent'. /0 is the 'is a broker' box."""
    assert _v(dvbe_fields, "YNagent") == "/1", (
        "DVBE 843 Section 2 must declare NOT a broker (YNagent=/1)"
    )


def test_dvbe_843_scprs_reference_number_is_blank(dvbe_fields):
    """SCPRS Ref. Number is 'FOR STATE USE ONLY' — Reytech leaves it blank."""
    assert _v(dvbe_fields, "SCPRS Reference Number") == ""


def test_dvbe_843_solicitation_number_is_filled(dvbe_fields):
    """The solicitation # belongs in Solicitation/Contract Number (SCno)."""
    assert _v(dvbe_fields, "SCno") == "10843276"


def test_dvbe_843_no_gratuitous_na_filler(dvbe_fields):
    """2nd-owner / manager / principal lines stay blank — never 'N/A'."""
    for field in ("DVBEowner2", "DVBEmgr", "Principal",
                  "PrincipalPhone", "PrincipalAddress"):
        assert _v(dvbe_fields, field) == "", f"{field} should be blank, not N/A"


def test_dvbe_843_section3_equipment_boxes_unchecked(dvbe_fields):
    """Reytech rents no equipment — Section 3 boxes must not be ticked."""
    for field in ("OwnBusiness", "OwnEquipment"):
        assert _v(dvbe_fields, field) in ("", "/Off"), (
            f"{field} must be unchecked — Reytech is a supply reseller"
        )


# ── STD 204 ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def std204_fields():
    out = fill_std204(_REYTECH, _PARSED)
    if out is None:
        pytest.skip("std204_blank.pdf template not available")
    return _fields(out)


def test_std204_fein_keeps_every_digit(std204_fields):
    """The 9-cell FEIN comb drops the last digit if fed the dashed form.
    Digits-only ('474588061') keeps all 9."""
    fein = _v(std204_fields, "Federal Employer Identification Number (FEIN)")
    assert fein == "474588061", f"FEIN lost a digit: {fein!r}"


def test_std204_section6_unit_section_is_blank(std204_fields):
    """Section 6 ('Paying State Agency') is state-use-only — blank."""
    assert _v(std204_fields, "UNITSECTION") == ""


# ── Signature sizing ──────────────────────────────────────────────────


def test_signature_draw_box_sizes_off_field_width_not_thin_height():
    """A thin signature widget must not shrink the image to a micro mark.
    The draw box is driven by field WIDTH; height is derived + capped."""
    # Thin field: 220 wide, only 9 tall.
    x, y, w, h = _signature_draw_box((100.0, 500.0, 320.0, 509.0), img_aspect=3.0)
    assert w > 50.0, "signature width collapsed — still micro-sized"
    assert h > 9.0, "signature height clamped to the thin field rect"
    assert h <= 30.0 + 0.01, "signature height exceeded the cap"
    # Aspect preserved.
    assert abs(w / h - 3.0) < 0.05


def test_signature_draw_box_caps_height_on_wide_field():
    """A very wide field must not balloon the signature past the cap."""
    x, y, w, h = _signature_draw_box((0.0, 0.0, 500.0, 14.0), img_aspect=3.0)
    assert h <= 30.0 + 0.01
    assert abs(w / h - 3.0) < 0.05


# ── Signature overlay actually lands ──────────────────────────────────


def test_packet_signatures_actually_land(tmp_path):
    """fill_cchcs_packet must report drawn signatures.

    The signature overlay catches its own exceptions, so a crash inside
    it ships an UNSIGNED packet with no test failure (exactly how the
    2026-05-21 `fl` unbound-variable bug slipped through). This asserts
    the overlay reported at least one drawn signature target.
    """
    import os

    from src.forms.cchcs_packet_filler import fill_cchcs_packet
    from src.forms.cchcs_packet_parser import parse_cchcs_packet

    fixture = os.path.join(
        os.path.dirname(__file__),
        "fixtures", "unified_ingest", "cchcs_packet_preq.pdf",
    )
    if not os.path.exists(fixture):
        pytest.skip("cchcs packet fixture not available")

    parsed = parse_cchcs_packet(fixture)
    if not parsed.get("ok"):
        pytest.skip(f"fixture parse failed: {parsed.get('error')}")

    result = fill_cchcs_packet(
        source_pdf=fixture, parsed=parsed,
        output_dir=str(tmp_path), strict=False,
    )
    overlaid = (result.get("signature_log") or {}).get("overlaid") or []
    assert overlaid, (
        "signature overlay drew nothing — it failed silently and the "
        "packet would ship unsigned"
    )
