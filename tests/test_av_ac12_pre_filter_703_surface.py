"""PR-AV-AC12 — surface buyer-attachment 703 to the shape filter.

CONTEXT

generate_rfq_package calls `filter_required_forms_by_shape(
required_forms, shape, uploaded_templates=_uploaded_tmpls)` to drop
buyer-template forms the shape doesn't apply. `_uploaded_tmpls` is
built from `tmpl` keys with on-disk files at filter time.

The AV-4 promote step (PR #1007-era) copies buyer-uploaded 703B /
703C / 704B blobs from the rfq_files ledger to tmpl — BUT that
block executes AFTER this filter call. So for a buyer who attached
both a 703C AND a pre-fillable 704 (shape=pc_704_pdf_fillable),
the filter ran with tmpl missing 703c entirely → "703b" was not
in `uploaded` set → for SHAPE_PC_704_PDF_FILLABLE which
shape_allowed = frozenset({"704b"}), "703b" got dropped from
_req_forms → the 703 fill block (gated on `_include("703b") or
_include("703c")`) never ran.

Symptom on rfq_9e63456e 5/15: buyer attached `AMS 703C - RFQ -
F_R - 03-25.pdf`. Manifest.generated_forms = ['704b','bidpkg',
'sellers_permit','dvbe843','quote']. NO 703 form. Review-page
banner read "Missing required forms: AMS 703B". A signed 703 is
contractually required for any CCHCS bid — Mike couldn't ship
without one even though the buyer-provided template was right
there in the ledger waiting to be filled.

THE FIX

Before the filter call, scan rfq_files for any buyer_attachment
whose filename contains "703B", "703C", or "FAIR_AND_REASONABLE"
(the canonical CCHCS 703C filename pattern) and surface "703c" into
the `_uploaded_tmpls` list. Same for "704B" / "QUOTE_WORKSHEET".

The filter's own substitution logic (L116-117) does the rest:

    if "703b" in uploaded or "703c" in uploaded:
        uploaded.update({"703b", "703c"})

So surfacing "703c" alone is enough to keep both 703b AND 703c
in the filtered required_forms.

WHAT THIS TEST PINS
===================

Source-grep only — wiring the live AV-4-style ledger scan into
this code path requires a stubbed `list_rfq_files` mock plus DB
fixtures, which is more setup than the substrate gain justifies.
The wiring is exercised end-to-end on prod via the rfq_9e63456e
acceptance walk.

  - PR-AV-AC12 marker present in routes_rfq_gen.py
  - The buyer-attachment scan iterates rfq_files with category=
    "buyer_attachment" BEFORE the filter call
  - The filename match patterns include 703B / 703C /
    FAIR_AND_REASONABLE
  - Surface mutation appends "703c" (which the filter expands to
    {703b, 703c})
"""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"


def test_ac12_marker_present():
    src = TARGET.read_text(encoding="utf-8")
    assert "PR-AV-AC12" in src, "PR-AV-AC12 marker must remain"


def test_ac12_scan_runs_before_filter_call():
    """Source-order check: the buyer-attachment surface must execute
    BEFORE the filter_required_forms_by_shape call, otherwise the
    filter still sees an empty uploaded set.
    """
    src = TARGET.read_text(encoding="utf-8")
    ac12_idx = src.find("PR-AV-AC12")
    filter_idx = src.find(
        "filter_required_forms_by_shape(\n                _req_forms_raw"
    )
    assert ac12_idx > 0 and filter_idx > 0
    assert ac12_idx < filter_idx, (
        f"AC12 buyer-attachment surface (line ~{ac12_idx}) must "
        f"execute before the filter call (line ~{filter_idx})"
    )


def test_ac12_filename_patterns_present():
    """AC12 filename ladder must admit ALL three CCHCS 703 revisions.

    Extended 2026-05-27 (Coleman sol# 10842771): the prior ladder
    matched only 703B/703C/FAIR_AND_REASONABLE. CCHCS shipped 703A
    (Rev. 03/2025) as the current revision on Coleman 10842771 — the
    buyer's PDF filename was "PR 10842771 - AMS 703A - REQUEST FOR
    QUOTATION.pdf". The renderer then fell back to an empty 703B
    because the AC12 surface didn't recognize 703A.
    """
    src = TARGET.read_text(encoding="utf-8")
    # Anchor at the actual code block (not the explanatory comment) by
    # looking for the first occurrence of the surface flag init.
    code_idx = src.find("_av_703_attached = False")
    assert code_idx > 0, "AC12 surface code block not found"
    block = src[code_idx:code_idx + 2400]
    # The filename patterns we match against
    assert '"703A" in _fn' in block, (
        "AC12 must match buyer attachments named *703A* (current revision)"
    )
    assert '"703B" in _fn' in block, (
        "AC12 must match buyer attachments named *703B* (prior revision)"
    )
    assert '"703C" in _fn' in block, (
        "AC12 must match buyer attachments named *703C* (IT-RFQ variant)"
    )
    assert '"FAIR_AND_REASONABLE" in _fn' in block, (
        "AC12 must match buyer attachments named *FAIR_AND_REASONABLE*"
        " (the canonical CCHCS 703C filename pattern)"
    )


def test_ac12_appends_present_revision_to_uploaded():
    """AC12 must surface the actually-present 703 revision to
    uploaded_templates — not a hardcoded fallback. The rev-aware
    filter at the render seam (added 2026-05-27 for Coleman sol#
    10842771) picks `_present_703` from `_uploaded_tmpls`; if AC12
    surfaces the wrong revision the renderer drops the right one.
    """
    src = TARGET.read_text(encoding="utf-8")
    # Anchor at the actual code block (not the explanatory comment) by
    # looking for the first occurrence of the surface flag init.
    code_idx = src.find("_av_703_attached = False")
    assert code_idx > 0, "AC12 surface code block not found"
    block = src[code_idx:code_idx + 2400]
    # The surface must read the detected slot (703a / 703b / 703c)
    # rather than hardcode 703c.
    assert "_av_703_slot" in block, (
        "AC12 must detect which 703 revision the buyer attached, "
        "not hardcode a fallback slot"
    )
    # Must still append SOMETHING to _uploaded_tmpls so the filter sees it.
    assert "_uploaded_tmpls.append" in block, (
        "AC12 must append the detected slot to _uploaded_tmpls so "
        "the rev-aware filter sees the buyer-attached revision"
    )


def test_ac12_reads_both_attachment_categories():
    """The scan must read BOTH category='buyer_attachment' (AV-4
    promote convention) AND category='attachment' (manual-upload
    writer convention). Coleman 10842771 surfaced the constant
    drift: writer uses 'attachment', reader was matching only
    'buyer_attachment' → manually-uploaded 703A never reached the
    surface block.
    """
    src = TARGET.read_text(encoding="utf-8")
    # Anchor at the actual code block (not the explanatory comment) by
    # looking for the first occurrence of the surface flag init.
    code_idx = src.find("_av_703_attached = False")
    assert code_idx > 0, "AC12 surface code block not found"
    block = src[code_idx:code_idx + 2400]
    assert '"buyer_attachment"' in block, (
        "AC12 must read category='buyer_attachment' for ingest-pipeline rows"
    )
    assert '"attachment"' in block, (
        "AC12 must ALSO read category='attachment' for manual-upload rows"
    )
