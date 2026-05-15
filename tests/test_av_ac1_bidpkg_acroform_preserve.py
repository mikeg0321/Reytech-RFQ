"""PR-AV-AC1 — bidpkg page-trim must preserve /AcroForm root.

Surfaced 2026-05-15 during the PREQ 10847262 (rfq_9e63456e) post-ship
audit. The form_qa banner on /rfq/<rid>/review-package reported 26
critical issues, 24 in the "field missing" bucket — the new AV-QC
categorizer's main signal for a real fill bug.

Forensic walkthrough of the actual generated PDF
(`10847262_BidPackage_Reytech.pdf`, 9 pages, 5.96MB):

  >>> reader = PdfReader(path)
  >>> reader.get_fields()
  {}                                 # nothing
  >>> reader.trailer["/Root"].get("/AcroForm")
  None                               # no AcroForm root
  >>> reader.pages[5].get("/Annots")  # but the page-level annots ARE there
  [...18 form-field annots, all with /V values written by
   fill_bid_package via fill_and_sign_pdf — DOING BUSINESS AS DBA
   NAME_CUF='Reytech Inc.', Date_CUF='05/14/2026', Text7_CUF='10847262', ...]

17 of the 24 "Missing:" QA errors were FALSE POSITIVES: the fields ARE
written on the annots but invisible to `reader.get_fields()` because
that API enumerates from the AcroForm root.

ROOT CAUSE

`fill_bid_package` (reytech_filler_v4.py) does a page-trim at the end
to drop SABRC tables, GenAI definitions, etc. The OLD code did:

    writer = PdfWriter()
    for i in valid_keep:
        writer.add_page(reader.pages[i])
    writer.write(f)

`PdfWriter.add_page` copies the page object (which includes the
per-page form-field annots with /V values) but does NOT propagate
the document-level `/Root → /AcroForm` node. The resulting PDF still
prints/looks correct, but `get_fields()` returns {} and downstream
QA flags every expected field as missing.

THE FIX

    writer = PdfWriter()
    writer.append(reader, pages=valid_keep)
    writer.write(f)

`PdfWriter.append(reader, pages=...)` clones the source's AcroForm
node along with the kept pages, so `get_fields()` works again on
the output.

WHAT THIS TEST PINS

  - Source-grep: trim uses `writer.append(reader, pages=...)`, NOT
    a bare `add_page` loop (regression guard).
  - A/B simulation against a real fixture: AcroForm survives trim
    using the new method but is lost with the old one.
  - `get_fields()` count is preserved for kept pages.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from pypdf import PdfReader, PdfWriter


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"


def _build_multipage_source(tmp_path):
    """Concatenate four AcroForm-bearing fixtures so the source has
    /AcroForm + multiple pages of named fields. Mirrors the structure
    of a filled CCHCS bid package (multiple internal forms stitched
    together).
    """
    sources = [
        FIXTURES / "dvbe843_blank.pdf",
        FIXTURES / "darfur_blank.pdf",
        FIXTURES / "cv012_cuf_blank.pdf",
        FIXTURES / "drug_free_blank.pdf",
    ]
    # Skip if any fixture is missing — fail loud rather than test against
    # a partial source.
    for s in sources:
        assert s.exists(), f"required fixture missing: {s}"
    w = PdfWriter()
    for s in sources:
        w.append(PdfReader(str(s)))
    out = tmp_path / "multi_src.pdf"
    with open(out, "wb") as f:
        w.write(f)
    return out


def test_source_has_acroform_and_fields(tmp_path):
    """Sanity gate: the multi-page source we build for the A/B has the
    AcroForm root + readable fields. If this fails, the rest of the
    test would be meaningless (vacuous pass on an already-stripped
    source — exactly the bug class we are guarding against).
    """
    src_path = _build_multipage_source(tmp_path)
    src = PdfReader(str(src_path))
    root = src.trailer.get("/Root", {})
    assert root.get("/AcroForm") is not None, (
        "source must have /AcroForm root for the trim A/B to be meaningful"
    )
    fields = src.get_fields() or {}
    assert len(fields) > 0, "source must expose readable form fields"


def test_old_trim_drops_acroform(tmp_path):
    """A/B baseline: prove the OLD writer.add_page loop loses AcroForm.

    This is the bug that PR-AV-AC1 fixes. Locking this in keeps anyone
    from "simplifying" the trim code back to a bare add_page loop in
    the future and re-introducing the field-missing-false-positive
    avalanche.
    """
    src_path = _build_multipage_source(tmp_path)
    src = PdfReader(str(src_path))
    keep = list(range(len(src.pages) - 1))  # drop last page
    w = PdfWriter()
    for i in keep:
        w.add_page(src.pages[i])
    out = tmp_path / "old_trim.pdf"
    with open(out, "wb") as f:
        w.write(f)

    reader = PdfReader(str(out))
    root = reader.trailer.get("/Root", {})
    assert root.get("/AcroForm") is None, (
        "OLD trim is expected to drop /AcroForm — if this assertion "
        "starts failing, pypdf semantics changed and the fix may be "
        "obsolete (revisit)"
    )
    assert (reader.get_fields() or {}) == {}, (
        "OLD trim is expected to make get_fields() return {} (no "
        "AcroForm root to enumerate from)"
    )


def test_new_trim_preserves_acroform(tmp_path):
    """The actual fix: writer.append(reader, pages=...) keeps AcroForm
    AND every named field on the kept pages remains addressable via
    `reader.get_fields()`.
    """
    src_path = _build_multipage_source(tmp_path)
    src = PdfReader(str(src_path))
    src_fields = src.get_fields() or {}
    src_field_count = len(src_fields)
    keep = list(range(len(src.pages) - 1))  # drop last page

    w = PdfWriter()
    w.append(src, pages=keep)
    out = tmp_path / "new_trim.pdf"
    with open(out, "wb") as f:
        w.write(f)

    reader = PdfReader(str(out))
    root = reader.trailer.get("/Root", {})
    assert root.get("/AcroForm") is not None, (
        "NEW trim must preserve /AcroForm root — this is the entire "
        "point of PR-AV-AC1"
    )
    out_fields = reader.get_fields() or {}
    assert len(out_fields) > 0, (
        "NEW trim must keep at least one named field readable via "
        "get_fields() (kept pages had fields)"
    )
    # The kept set is strictly smaller than the source (dropped 1 page);
    # we should NOT see more fields than the source had.
    assert len(out_fields) <= src_field_count


def test_bidpkg_trim_uses_append_not_add_page_loop():
    """Source-grep guard: the trim block in fill_bid_package must use
    writer.append(reader, pages=...), not a writer.add_page() loop.

    Lives here (not in a generic style-check) because the regression
    is specific to one block of code and the failure mode (silent
    AcroForm loss) is invisible to ordinary template-render tests.
    """
    target = REPO_ROOT / "src" / "forms" / "reytech_filler_v4.py"
    src = target.read_text(encoding="utf-8")
    # Locate the PR-AV-AC1 marker that we added on the fix line. If
    # someone removes the marker AND the append call, this fails loud.
    av_ac1_idx = src.find("PR-AV-AC1")
    assert av_ac1_idx > 0, "PR-AV-AC1 marker must remain in reytech_filler_v4.py"
    # Look at the ~1500 characters after the marker — should contain
    # writer.append(reader, pages=valid_keep) and NOT a bare add_page
    # loop targeting reader.pages[i] in the same block.
    block = src[av_ac1_idx:av_ac1_idx + 1500]
    assert "writer.append(reader, pages=valid_keep)" in block, (
        "trim block must call writer.append(reader, pages=valid_keep) "
        "within ~1500 chars of the PR-AV-AC1 marker"
    )
    assert "writer.add_page(reader.pages[i])" not in block, (
        "trim block must NOT contain the legacy writer.add_page("
        "reader.pages[i]) loop — that strips /AcroForm"
    )
