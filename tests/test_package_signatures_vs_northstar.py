"""Audit BB: package-signature regression guard.

Mike's 2026-04-23 complaint against the generated RFQ 10840486
package:

    "missing some signatures and in some places has double signatures."

The 3-strikes guardrail per `project_2026_04_22_session_audit.md`
item BB: **do NOT patch signature placement heuristics without a
regression fixture proving the fix + preserving good pages.**

This test file IS the regression fixture. It:
  1. Pins the signature audit of the canonical north-star PDF —
     the 20-page PDF Mike actually submitted for RFQ 10840486.
  2. Gives a helper (`compare_to_northstar`) any future test can
     use to assert a generated package matches the north star.
  3. Protects against regressions in `package_signatures.py` itself
     — if the detection heuristics stop firing on real AcroForm
     fields or image XObjects, these tests fail loudly.

### North-star signature contract (20 pages total)
Detected via `audit_package_signatures`:
  page 1  — 2 image XObjects + 'signature' text (LPA supplier sig)
  page 8  — 1 image XObject + 'signature' text (Attachment 6 / GenAI)
  page 9  — 2 image XObjects + 'signature' text (CCHCS certs)
  page 10 — AcroForm /Sig + Widget + 'signature' text
  page 15 — AcroForm /Sig + Widget + 1 image + 'signature' text
  page 16 — AcroForm /Sig + Widget + 3 images + 'signature' text
  page 17 — 4 AcroForm /Sig fields + 4 widgets + 1 image
  page 18 — AcroForm /Sig + Widget + 2 images

Pages 2-7, 11-14, 19-20 have NO signature evidence and MUST stay
signature-free.

The original audit memo (item BB) said "Signatures detected via text
scan in north star: pages 1, 8, 9, 10, 15, 16" — that missed pages
17 and 18 because they contain form-field signatures without the
word "signature" on the page. This module's detection (AcroForm +
Widget + images + text) is more rigorous.
"""
from __future__ import annotations

import os

import pytest


NORTH_STAR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "rfq_packages",
    "10840486_rfq_package_NORTHSTAR.pdf",
)


# Frozen expected state — derived from an actual audit run of the
# north star on 2026-04-23. If any of these values change, EITHER
# (a) the north star has genuinely been updated — refresh this map
# from `audit_package_signatures` output, OR (b) detection has
# regressed — fix `package_signatures.py`.
EXPECTED_SIG_PAGES = {1, 8, 9, 10, 15, 16, 17, 18}
EXPECTED_NON_SIG_PAGES = {2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 19, 20}
EXPECTED_PAGE_COUNT = 20


class TestNorthStarSignatureContract:
    """Lock in the canonical state of the north star. If this file
    drifts, the regression fixtures that depend on it also drift."""

    def test_north_star_has_expected_page_count(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        assert len(result) == EXPECTED_PAGE_COUNT, (
            f"north-star page count changed: expected "
            f"{EXPECTED_PAGE_COUNT}, got {len(result)}"
        )

    def test_sig_pages_match_canonical(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        actual_sig_pages = {
            p for p, d in result.items() if d["has_any_signature"]
        }
        assert actual_sig_pages == EXPECTED_SIG_PAGES, (
            f"signature-page set drift: expected "
            f"{sorted(EXPECTED_SIG_PAGES)}, got "
            f"{sorted(actual_sig_pages)}. Either north star changed "
            f"or detection regressed."
        )

    def test_non_sig_pages_carry_no_signatures(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        for p in EXPECTED_NON_SIG_PAGES:
            assert result[p]["has_any_signature"] is False, (
                f"page {p} used to be signature-free; now detection "
                f"sees {result[p]}"
            )

    def test_acroform_pages_have_form_field_sigs(self):
        """Pages 10, 15, 16, 17, 18 carry AcroForm /Sig fields.
        If detection stops seeing them, the /Sig counting path in
        `package_signatures.py` has broken."""
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        for p in (10, 15, 16, 17, 18):
            d = result[p]
            assert d["acroform_sigs"] >= 1, (
                f"page {p} should have AcroForm /Sig; got {d}"
            )

    def test_image_overlay_pages_detected(self):
        """Pages 1, 8, 9 carry image-overlay signatures (no AcroForm).
        Detection must still flag them via the image+text heuristic."""
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        for p in (1, 8, 9):
            d = result[p]
            assert d["image_xobjects"] >= 1, (
                f"page {p} should have image XObject; got {d}"
            )
            assert d["has_any_signature"] is True, (
                f"page {p} image-overlay should flag has_any_signature"
            )


class TestCompareHelper:
    """The `compare_to_northstar` helper is what future regression
    tests will call. These tests pin its return shape + behavior so
    a caller can rely on `missing_on` / `extra_on` / `matches`."""

    def test_compare_self_matches(self):
        """Comparing the north star to itself MUST report a perfect
        match. If this test fails, the helper itself is broken."""
        from src.core.package_signatures import compare_to_northstar
        diff = compare_to_northstar(NORTH_STAR, NORTH_STAR)
        assert diff["matches"] is True
        assert diff["missing_on"] == []
        assert diff["extra_on"] == []
        assert diff["page_count_gen"] == diff["page_count_ns"]
        assert diff["page_count_gen"] == EXPECTED_PAGE_COUNT

    def test_compare_shape_has_required_keys(self):
        from src.core.package_signatures import compare_to_northstar
        diff = compare_to_northstar(NORTH_STAR, NORTH_STAR)
        for key in ("page_count_gen", "page_count_ns", "matches",
                    "per_page", "missing_on", "extra_on"):
            assert key in diff, f"missing key {key!r}"
        for entry in diff["per_page"]:
            for key in ("page", "expected_sig", "actual_sig",
                        "match", "ns_counts", "gen_counts"):
                assert key in entry, f"per_page missing {key!r}"


class TestAuditReadsAllThreeDetectionSignals:
    """Regression guard on the detection heuristics themselves. All
    three signals (AcroForm, Widget, Image) must fire at least once
    across the north star — if any single detection code path
    regresses to always-0, the others mask it and the test suite
    wouldn't notice."""

    def test_acroform_count_fires_somewhere(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        total_acroform = sum(
            d["acroform_sigs"] for d in result.values()
        )
        assert total_acroform > 0, (
            "acroform_sigs never fired across the north star — "
            "AcroForm /Sig detection has regressed"
        )

    def test_widget_count_fires_somewhere(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        total_widget = sum(
            d["widget_sigs"] for d in result.values()
        )
        assert total_widget > 0, (
            "widget_sigs never fired across the north star — "
            "Widget /FT=/Sig detection has regressed"
        )

    def test_image_xobjects_fire_somewhere(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        total_images = sum(
            d["image_xobjects"] for d in result.values()
        )
        assert total_images > 0, (
            "image_xobjects never fired across the north star — "
            "image-XObject scan has regressed"
        )

    def test_text_markers_fire_somewhere(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures(NORTH_STAR)
        pages_with_markers = sum(
            1 for d in result.values() if d["text_markers"]
        )
        assert pages_with_markers > 0, (
            "no page had a text marker — extract_text may be broken"
        )


class TestEmptyAndBadInput:
    def test_missing_file_returns_empty_dict(self):
        from src.core.package_signatures import audit_package_signatures
        result = audit_package_signatures("/tmp/nonexistent-12345.pdf")
        assert result == {}

    def test_compare_missing_file_does_not_raise(self):
        from src.core.package_signatures import compare_to_northstar
        diff = compare_to_northstar(
            "/tmp/nonexistent-12345.pdf", NORTH_STAR,
        )
        # Generated side has 0 pages, north star has 20
        assert diff["page_count_gen"] == 0
        assert diff["page_count_ns"] == EXPECTED_PAGE_COUNT
        assert diff["matches"] is False
