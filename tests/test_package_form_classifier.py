"""Regression: filename → form_id classification for package QA.

Bug shipped to prod (2026-04-23): both `routes_rfq_gen.py` and
`routes_rfq.py` had inline `elif` chains where `"cuf" in name`
came before any check for `"barstow"`. So a generated
`RFQ_BarstowCUF_Reytech.pdf` matched "cuf" first and got labelled
`cv012_cuf` — leaving `barstow_cuf` reported "missing" on every
CalVet Barstow package even though the PDF was on disk and bundled
into the combined package PDF.

These tests pin the specific-first ordering so a future edit can't
re-break the same mapping.
"""
from __future__ import annotations

import pytest

from src.forms.package_form_classifier import classify_package_filename


# ── The bug that triggered this module (do NOT remove these) ────

class TestBarstowVsCv012:
    """The exact regression: real prod filenames that broke 2026-04-23."""

    def test_barstow_cuf_classified_as_barstow_not_cv012(self):
        assert classify_package_filename(
            "RFQ_BarstowCUF_Reytech.pdf"
        ) == "barstow_cuf"

    def test_cv012_cuf_classified_as_cv012(self):
        assert classify_package_filename(
            "RFQ_CV012_CUF_Reytech.pdf"
        ) == "cv012_cuf"

    def test_lowercase_barstow_cuf(self):
        assert classify_package_filename(
            "rfq_barstowcuf_reytech.pdf"
        ) == "barstow_cuf"

    def test_both_barstow_and_cv012_in_same_package(self):
        """Real CalVet Barstow package emits BOTH PDFs — the QA gate
        must see both ids in `gen_ids`, not collapsed to one cv012."""
        files = [
            "RFQ_CV012_CUF_Reytech.pdf",
            "RFQ_BarstowCUF_Reytech.pdf",
        ]
        ids = {classify_package_filename(f) for f in files}
        assert ids == {"cv012_cuf", "barstow_cuf"}, (
            "BOTH ids must appear — bug collapsed both filenames into "
            "cv012_cuf, hiding the missing barstow_cuf."
        )


# ── Coverage for the rest of the chain ──────────────────────────

@pytest.mark.parametrize("filename, expected", [
    # Quote (must lose to 704 if both present in name)
    ("RFQ_Quote_Reytech.pdf", "quote"),
    # 703 family
    ("RFQ_703B_Reytech.pdf", "703b"),
    ("RFQ_703C_Reytech.pdf", "703b"),  # 703C also classifies as 703b family
    # 704
    ("RFQ_704B_Reytech.pdf", "704b"),
    # Compliance forms
    ("RFQ_CalRecycle74_Reytech.pdf", "calrecycle74"),
    ("RFQ_BidderDecl_Reytech.pdf", "bidder_decl"),
    ("RFQ_DVBE843_Reytech.pdf", "dvbe843"),
    ("RFQ_DarfurAct_Reytech.pdf", "darfur_act"),
    ("RFQ_STD204_Reytech.pdf", "std204"),
    ("RFQ_STD205_Reytech.pdf", "std205"),
    ("RFQ_STD1000_Reytech.pdf", "std1000"),
    ("RFQ_SellersPermit_Reytech.pdf", "sellers_permit"),
    ("RFQ_OBS1600_Reytech.pdf", "obs_1600"),
    ("RFQ_DrugFree_Reytech.pdf", "drug_free"),
    ("RFQ_BidPkg_Reytech.pdf", "bidpkg"),
    # Unknown / garbage
    ("random_attachment.pdf", "unknown"),
    ("", "unknown"),
])
def test_known_filenames_classify_correctly(filename, expected):
    assert classify_package_filename(filename) == expected


def test_never_raises_on_garbage_input():
    for v in [None, "", "    ", "no_extension", "wrong.txt", "🦄.pdf"]:
        # Should not raise — return "unknown" or a best-guess id
        result = classify_package_filename(v)
        assert isinstance(result, str)


# ── Property: ordering invariant ─────────────────────────────────

def test_specific_facility_cuf_wins_over_generic_cuf():
    """Property: any filename that names a known facility (barstow,
    future facility-CUFs) MUST resolve to the facility-specific id,
    not the generic cv012_cuf bucket. Lock this so future filename
    formats can't accidentally regress."""
    barstow_variants = [
        "RFQ_BarstowCUF_Reytech.pdf",
        "RFQ_Barstow_CUF.pdf",
        "BARSTOW-CUF-2026.pdf",
        "out_dir/sol_BarstowCUF_Reytech.pdf",
    ]
    for f in barstow_variants:
        assert classify_package_filename(f) == "barstow_cuf", (
            f"{f!r} should resolve to barstow_cuf, not cv012_cuf"
        )
