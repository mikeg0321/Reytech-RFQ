"""Regression test for 703B bidder-info fill across template prefix variants.

Incident 2026-04-20 (quote R26Q36): buyer-supplied 703B template had
unprefixed AcroForm field names ("Business Name", "Check Box2", ...)
but fill_703b hardcoded the "703B_" prefix, so every vendor field and
checkbox silently missed. Output PDF had a blank Bidder Information
section, signature present, and QA flagging 15 missing fields.

This test builds two blank 703B templates — one with the stock
"703B_" prefix, one with every field renamed to unprefixed — and
asserts fill_703b populates the same logical bidder fields in both.
"""
from __future__ import annotations

import os
import shutil

import pytest
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject

from src.forms import reytech_filler_v4


FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "703b_blank.pdf"
)

# Mirror the production config (subset used by fill_703b).
_CONFIG = {
    "company": {
        "name": "Reytech Inc.",
        "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
        "owner": "Michael Guadan",
        "title": "Owner",
        "phone": "949-229-1575",
        "email": "sales@reytechinc.com",
        "fein": "47-4588061",
        "sellers_permit": "245652416 - 00001",
        "cert_number": "2002605",
        "cert_expiration": "5/31/2027",
    }
}

_RFQ = {
    "solicitation_number": "10844466",
    "release_date": "04/16/2026",
    "due_date": "04/20/2026",
    "delivery_days": "30",
    "sign_date": "04/20/2026",
    "requestor_name": "Jane Buyer",
    "requestor_email": "buyer@cchcs.ca.gov",
    "requestor_phone": "916-555-0100",
}


# ── Template builders ─────────────────────────────────────────────────────

def _rename_fields(src_pdf: str, dst_pdf: str, rename: dict[str, str]) -> None:
    """Clone src_pdf to dst_pdf renaming AcroForm /T field titles in place.

    Rewrites every widget annotation's /T entry — that's the name pypdf
    reports via get_fields() and what update_page_form_field_values matches
    on. Field values, appearance, and layout are preserved; only the key
    used to address the field changes.
    """
    reader = PdfReader(src_pdf)
    writer = PdfWriter()
    writer.append(reader)
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        for annot_ref in page["/Annots"]:
            annot = annot_ref.get_object()
            t_name = str(annot.get("/T", ""))
            if t_name in rename:
                annot[NameObject("/T")] = TextStringObject(rename[t_name])
    # Also rewrite the AcroForm /Fields tree so reader.get_fields() sees the
    # new names (widgets may inherit /T from a parent field).
    root = writer._root_object  # noqa: SLF001 — pypdf doesn't expose a getter
    if "/AcroForm" in root and "/Fields" in root["/AcroForm"]:
        for field_ref in root["/AcroForm"]["/Fields"]:
            field = field_ref.get_object()
            t_name = str(field.get("/T", ""))
            if t_name in rename:
                field[NameObject("/T")] = TextStringObject(rename[t_name])
    with open(dst_pdf, "wb") as fh:
        writer.write(fh)


@pytest.fixture
def prefixed_blank(tmp_path):
    """Standard Reytech 703B blank with '703B_' prefix."""
    dst = tmp_path / "703b_prefixed.pdf"
    shutil.copy(FIXTURE, dst)
    return str(dst)


@pytest.fixture
def unprefixed_blank(tmp_path):
    """Buyer variant: every field renamed to drop the '703B_' prefix.

    Emulates the CCHCS-buyer-supplied 703B that caused R26Q36 to generate
    with a blank Bidder Information section.
    """
    src_fields = list((PdfReader(FIXTURE).get_fields() or {}).keys())
    rename = {
        name: name[len("703B_"):]
        for name in src_fields if name.startswith("703B_")
    }
    dst = tmp_path / "703b_unprefixed.pdf"
    _rename_fields(FIXTURE, str(dst), rename)
    # Sanity: the rewrite worked and there's no residual 703B_ prefix.
    names = set((PdfReader(str(dst)).get_fields() or {}).keys())
    assert not any(n.startswith("703B_") for n in names), (
        "rename did not strip the 703B_ prefix"
    )
    assert "Business Name" in names and "Check Box2" in names
    return str(dst)


# ── Assertions ────────────────────────────────────────────────────────────

_BIDDER_LOGICAL_FIELDS = [
    ("Business Name", "Reytech Inc."),
    ("Address", "30 Carnoustie Way Trabuco Canyon CA 92679"),
    ("Contact Person", "Michael Guadan"),
    ("Title", "Owner"),
    ("Phone", "949-229-1575"),
    ("Email", "sales@reytechinc.com"),
    ("Federal Employer Identification Number FEIN", "47-4588061"),
    ("Retailers CA Sellers Permit Number", "245652416 - 00001"),
    ("SBMBDVBE Certification.0", "2002605"),
    ("Certification Expiration Date", "5/31/2027"),
]


def _assert_bidder_filled(pdf_path: str, prefix: str) -> None:
    fields = PdfReader(pdf_path).get_fields() or {}
    missing = []
    for suffix, expected in _BIDDER_LOGICAL_FIELDS:
        key = f"{prefix}{suffix}"
        actual = str((fields.get(key) or {}).get("/V", "")).strip()
        if not actual:
            missing.append(key)
        else:
            assert actual == expected, (
                f"{key}: expected {expected!r}, got {actual!r}"
            )
    assert not missing, (
        f"Bidder fields blank after fill (prefix={prefix!r}): {missing}"
    )

    # Checkboxes — these were silently misaddressed in the R26Q36 bug.
    for cb in ("Check Box2", "Check Box4"):
        key = f"{prefix}{cb}"
        actual = str((fields.get(key) or {}).get("/V", "")).strip()
        assert actual == "/Yes", (
            f"{key}: expected '/Yes', got {actual!r}"
        )

    # Bid expiration must be computed and filled.
    bid_exp_key = f"{prefix}BidExpirationDate"
    bid_exp = str((fields.get(bid_exp_key) or {}).get("/V", "")).strip()
    assert bid_exp, f"{bid_exp_key} empty — 45-day bid expiry not filled"


# ── Tests ─────────────────────────────────────────────────────────────────

def test_fill_703b_prefixed_template_populates_bidder_info(
    prefixed_blank, tmp_path
):
    out = tmp_path / "out_prefixed.pdf"
    reytech_filler_v4.fill_703b(
        prefixed_blank, dict(_RFQ), _CONFIG, str(out)
    )
    _assert_bidder_filled(str(out), prefix="703B_")


def test_fill_703b_unprefixed_template_populates_bidder_info(
    unprefixed_blank, tmp_path
):
    """The R26Q36 regression. Buyer sent an unprefixed 703B; fill_703b
    must detect the empty prefix and populate the bidder fields instead
    of silently missing every key."""
    out = tmp_path / "out_unprefixed.pdf"
    reytech_filler_v4.fill_703b(
        unprefixed_blank, dict(_RFQ), _CONFIG, str(out)
    )
    _assert_bidder_filled(str(out), prefix="")


def test_fill_703b_reads_top_buyer_fields_on_both_variants(
    prefixed_blank, unprefixed_blank, tmp_path
):
    """Top-of-form buyer fields (Solicitation Number, Release Date, Due Date)
    rendered correctly on R26Q36 — guard against a fix regressing them."""
    for src, prefix, tag in (
        (prefixed_blank, "703B_", "prefixed"),
        (unprefixed_blank, "", "unprefixed"),
    ):
        out = tmp_path / f"out_top_{tag}.pdf"
        reytech_filler_v4.fill_703b(src, dict(_RFQ), _CONFIG, str(out))
        fields = PdfReader(str(out)).get_fields() or {}
        for suffix, expected in (
            ("Solicitation Number", "10844466"),
            ("Release Date", "04/16/2026"),
            ("Due Date", "04/20/2026"),
        ):
            key = f"{prefix}{suffix}"
            actual = str((fields.get(key) or {}).get("/V", "")).strip()
            assert expected in actual, (
                f"{tag}: {key} expected to contain {expected!r}, got {actual!r}"
            )
