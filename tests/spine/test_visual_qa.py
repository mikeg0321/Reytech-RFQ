"""Visual-QA — Tier-1 deterministic detectors for the PR-10 bug class.

The math/identity/coverage Inspector (``inspector.py``) can pass clean
on output that still LOOKS wrong (comb-spaced, clipped, fragmented).
``visual_qa.py`` catches those visual-fidelity defects. These tests
pin its behavior end-to-end.
"""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pytest
from pypdf import PdfReader, PdfWriter

from src.spine.visual_qa import VisualIssue, VisualQAReport, inspect_pdf_visual

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T704B = _REPO_ROOT / "tests/fixtures/704b_blank.pdf"
_T703B = _REPO_ROOT / "tests/fixtures/703b_blank.pdf"


_needs_fixtures = pytest.mark.skipif(
    not (_T704B.is_file() and _T703B.is_file()),
    reason="form template fixtures missing",
)

# The vision_classifier extension point rasterizes each page to PNG before
# invoking the caller's classifier — that rasterization needs PyMuPDF
# (`fitz`), which is NOT a production dependency. When fitz is absent
# visual_qa.py degrades to Tier-1 only (visual_qa.py:305 "vision_classifier
# failed ... Tier-1 only") and the classifier is never run, so
# `vision_classifier` is correctly absent from detectors_run. This test
# asserts the extension-point CAPABILITY and is therefore skipped, not
# failed, when fitz is unavailable. Whether to make fitz a test/prod dep is
# an Architect/Mike decision, not a blind fix.
_HAS_FITZ = importlib.util.find_spec("fitz") is not None
_needs_fitz = pytest.mark.skipif(
    not _HAS_FITZ,
    reason="PyMuPDF (fitz) not installed — vision_classifier degrades to Tier-1 in prod",
)


# ── pass-through cases ───────────────────────────────────────────────


def test_visual_qa_empty_input_is_ok():
    """Empty / non-PDF bytes return ok=True with zero issues (the
    upstream Inspector / send-prep handles 'nothing to send' separately)."""
    r = inspect_pdf_visual(b"")
    assert r.ok is True
    assert r.pdf_pages == 0
    assert r.issues == []

    r2 = inspect_pdf_visual(b"not a pdf")
    assert r2.ok is True
    assert r2.issues == []


@_needs_fixtures
def test_visual_qa_clean_blank_template_passes():
    """A pristine blank form has no comb-spacing and no (cid:N) runs
    in extracted text — the gate should pass clean."""
    data = _T704B.read_bytes()
    r = inspect_pdf_visual(data)
    assert r.ok is True
    cid = [i for i in r.issues if i.kind == "cid_glyph_artifacts"]
    comb = [i for i in r.issues if i.kind == "comb_class_spacing"]
    assert cid == [], f"clean template flagged cid: {cid}"
    assert comb == [], f"clean template flagged comb: {comb}"


# ── comb-class detector ──────────────────────────────────────────────


def test_visual_qa_comb_pattern_regex_matches_demidenko_signature():
    """The literal Demidenko PC failure string MUST trigger the comb
    detector — this is the pin against the PR-10 bug class.
    """
    from src.spine.visual_qa import _COMB_RUN

    # Pre-fix Demidenko render produced this exact extraction:
    fragment = "s a l e s @ r e y t e c h i n c ."
    m = _COMB_RUN.search(fragment)
    assert m is not None, "comb regex must match the Demidenko failure"
    run = m.group(0)
    assert len(run) >= 8


def test_visual_qa_comb_pattern_regex_does_not_match_normal_text():
    """Normal English text — even short — must NOT trigger the comb
    detector. False positives here would erode operator trust."""
    from src.spine.visual_qa import _COMB_RUN

    normals = [
        "Reytech Inc.",
        "30 Carnoustie Way Trabuco Canyon, CA 92679",
        "Total Price $ 3,138.89",
        "Net 45  PAYMENT TERMS",
        "Midmark Manual Exam Table base only",
        "I am ok",
        "a b c",  # 3-char single-tokens — under the 4-min threshold
    ]
    for s in normals:
        m = _COMB_RUN.search(s)
        if m and len(m.group(0)) >= 8:
            raise AssertionError(
                f"comb regex falsely matched normal text: "
                f"{s!r} -> {m.group(0)!r}"
            )


# ── cid-glyph detector ──────────────────────────────────────────────


def test_visual_qa_cid_pattern_matches_post_bake_signature():
    """fitz emits (cid:N) when the font subset's ToUnicode map can't
    resolve glyphs — the post-bake-with-stale-/AP signature."""
    from src.spine.visual_qa import _CID_GLYPH

    sample = (
        "1 (cid:0)1 (cid:0)E(cid:0)A(cid:0)C(cid:0)H "
        "Midmark Manual Exam Table, base only"
    )
    hits = _CID_GLYPH.findall(sample)
    assert len(hits) >= 3, f"cid regex found only {len(hits)} hits"


# ── empty-required-field detector ────────────────────────────────────


@_needs_fixtures
def test_visual_qa_flags_required_field_left_blank():
    """Pre-flatten: if any AcroForm widget has /Ff bit 2 (Required)
    and no /V, the gate must flag it."""
    from pypdf.generic import NameObject, NumberObject

    reader = PdfReader(str(_T704B))
    writer = PdfWriter(clone_from=reader)
    # Mark the first Tx widget as required, leave /V unset.
    tagged = False
    for page in writer.pages:
        for annot in page.get("/Annots") or []:
            a = annot.get_object()
            if (a.get("/Subtype") != "/Widget"
                    or str(a.get("/FT") or "") != "/Tx"):
                continue
            existing_ff = int(a.get("/Ff") or 0)
            a[NameObject("/Ff")] = NumberObject(existing_ff | 2)
            tagged = True
            break
        if tagged:
            break
    assert tagged, "no Text widget found to mark required"

    buf = io.BytesIO()
    writer.write(buf)
    r = inspect_pdf_visual(buf.getvalue())
    empties = [i for i in r.issues if i.kind == "empty_required_field"]
    assert empties, "required-but-blank field not flagged"


def test_visual_qa_post_flatten_no_required_check():
    """After flatten, AcroForm fields are gone, so the empty-required
    detector returns nothing for that class. (This is task-#20 v2
    territory: visual reasoning on the baked content stream.)"""
    if not _T704B.is_file():
        pytest.skip("fixture missing")
    from src.spine.flatten import flatten_pdf_bytes

    flat = flatten_pdf_bytes(_T704B.read_bytes())
    r = inspect_pdf_visual(flat)
    empties = [i for i in r.issues if i.kind == "empty_required_field"]
    assert empties == []


# ── public-API shape ─────────────────────────────────────────────────


def test_visual_qa_report_shape_and_counts():
    """The report carries the structured info the operator UI needs."""
    r = inspect_pdf_visual(b"")
    assert isinstance(r, VisualQAReport)
    assert r.detectors_run  # at least one detector listed
    assert r.blocking_count == 0
    assert r.warning_count == 0


@_needs_fitz
def test_visual_qa_vision_classifier_extension_point():
    """A caller-provided vision_classifier merges its findings into
    the report and is listed in detectors_run."""
    if not _T704B.is_file():
        pytest.skip("fixture missing")

    def _fake_classifier(page_pngs):
        # Should receive at least 1 page PNG and return list[VisualIssue].
        assert len(page_pngs) >= 1 and page_pngs[0][:4] == b"\x89PNG"
        return [VisualIssue(
            kind="vision_finding",
            severity="warning",
            page=1,
            location="page 1",
            message="(fake) signature box may overlap label",
            evidence="",
        )]

    r = inspect_pdf_visual(
        _T704B.read_bytes(), vision_classifier=_fake_classifier
    )
    assert "vision_classifier" in r.detectors_run
    findings = [i for i in r.issues if i.kind == "vision_finding"]
    assert len(findings) == 1
    assert "overlap" in findings[0].message
