"""PR-AV-AC8 — bundle two related substrate fixes surfaced during the
5/15 DevTools-MCP walk of rfq_9e63456e:

  1. `_bidpkg_page_skip_reason` must ALWAYS keep any page whose field
     names contain a `708_` prefixed widget. The AMS 708 GenAI
     Disclosure form is required for CCHCS Under-100k bids; on prod
     5/15 the trim was dropping it because another rule (OBS 1600
     field-name fingerprint or GenAI-defs text rule) false-fired on
     the same page. AMS 708 form pages (1 of 4 / 2 of 4) are the
     only places that carry `708_Text*` widgets, so this guard is
     precisely scoped — the definitions pages (3 of 4 / 4 of 4)
     have NO `708_` widgets and stay subject to the GenAI-defs
     text-content rule.

  2. `/rfq/<rid>/generate-package` must refuse to run when any line
     item has a non-positive unit_price. Mike: "we should never get
     to this step without a line item being filled" — the prior flow
     paid the 80-second package-gen cost and surfaced the unpriced
     row on the review-page banner, but at that point the operator
     had already burned the regen budget. The hard gate sends them
     back to /rfq/<rid> to fix pricing first. `?force=1` query param
     is the diagnostic escape hatch.

WHAT THIS TEST PINS
===================

708 keep-guard (in `src.forms.reytech_filler_v4`):

  - A synthetic page carrying `708_Text1` as a /Tx widget is KEPT
    (returns None) even when text content contains "OBS 1600" or
    "GENAI definitions" markers
  - A synthetic page with NO 708_-prefixed widgets is still subject
    to the existing OBS / GenAI / SABRC / VSDS / blank rules
  - Order: the 708 guard fires BEFORE every other skip rule
  - PR-AV-AC8 marker present in the source

Unpriced-row gate (in `routes_rfq_gen.generate_rfq_package`):

  - Source-grep: gate sits between Step-1 form-price application
    and the orchestrator observer
  - The PR-AV-AC8 marker is present in the gate's comment block
  - The gate respects `?force=1` so a diagnostic regen can bypass
  - `redirect(f"/rfq/{rid}")` is the failure outcome (operator
    is sent back to fix pricing)
"""
from __future__ import annotations

import ast
import io
from pathlib import Path

import pytest
from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    NameObject, TextStringObject, ArrayObject, DictionaryObject,
    NumberObject,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_one_page_pdf_with_fields(tmp_path, name_to_value, text=""):
    """Build a 1-page PDF with named form-field widgets. Returns the
    PdfReader.pages[0] object so we can hand it straight to
    `_bidpkg_page_skip_reason` (which takes a single Page, not a
    full reader).
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter

    raw = tmp_path / f"raw_{abs(hash(text))%10000}.pdf"
    c = canvas.Canvas(str(raw), pagesize=letter)
    if text:
        c.drawString(72, 600, text)
    c.showPage()
    c.save()
    reader = PdfReader(str(raw))
    writer = PdfWriter()
    writer.append(reader)
    page = writer.pages[0]

    annots = []
    for name, value in name_to_value.items():
        widget = DictionaryObject({
            NameObject("/Type"): NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"): NameObject("/Tx"),
            NameObject("/T"): TextStringObject(name),
            NameObject("/V"): TextStringObject(str(value)) if value is not None else TextStringObject(""),
            NameObject("/Rect"): ArrayObject([
                NumberObject(0), NumberObject(0),
                NumberObject(0), NumberObject(0),
            ]),
            NameObject("/F"): NumberObject(4),
        })
        annots.append(writer._add_object(widget))
    page[NameObject("/Annots")] = ArrayObject(annots)

    out = tmp_path / f"synth_{abs(hash(text))%10000}.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    # Re-read so the page returned is from an immutable reader
    return PdfReader(str(out)).pages[0]


# ── 708 keep-guard tests ────────────────────────────────────────────


def test_708_page_kept_when_obs_field_also_present(tmp_path):
    """The headline scenario: a page carries `708_Text1` AND another
    field whose name contains "obs 1600" (the false-positive trigger).
    The 708 guard must override.
    """
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    page = _make_one_page_pdf_with_fields(tmp_path, {
        "708_Text1": "10847262",
        "708_Text3": "Reytech Inc.",
        "obs 1600 leftover field": "",
    })
    assert _bidpkg_page_skip_reason(page) is None, (
        "708 page must be KEPT even if a sibling field name "
        "matches OBS 1600"
    )


def test_708_page_kept_when_genai_defs_text_present(tmp_path):
    """A 708-fillable page may have 'GENAI DISCLOSURE & FACTSHEET'
    in its header text. The GenAI-defs skip rule keys off
    `("3 of 4" in text or "4 of 4" in text) AND (genai/definition in
    text)` — but in practice the text-extraction layer can leak
    a stray "3 of 4" footer into the wrong page. The 708 keep
    guard must override.
    """
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    page = _make_one_page_pdf_with_fields(tmp_path, {
        "708_Text1": "10847262",
    }, text="1 of 4 GENAI DISCLOSURE & FACTSHEET 3 of 4 definitions")
    assert _bidpkg_page_skip_reason(page) is None


def test_non_708_obs_page_still_skipped(tmp_path):
    """Defense: don't regress the actual OBS 1600 skip. A page with
    only OBS 1600 field names (no 708_) is STILL skipped.
    """
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    page = _make_one_page_pdf_with_fields(tmp_path, {
        "OBS 1600 % OF PRODUCT PG 1 - ROW 1": "",
        "OBS 1600 % OF PRODUCT PG 1 - ROW 2": "",
    })
    reason = _bidpkg_page_skip_reason(page)
    assert reason is not None and "OBS 1600" in reason


def test_blank_page_still_skipped(tmp_path):
    """Defense: blank pages still skip.

    Synthesizing a truly text-empty page through reportlab + pypdf is
    fiddly (the writer header bytes can leak into extracted text on
    some pypdf builds). The substrate check that matters is structural:
    a page with no field widgets and no 708_-prefixed widget should NOT
    be force-kept by the AC8 guard — it must fall through to the
    downstream "blank (no text, no fields)" rule. Pin the negative
    intent rather than the exact return value.
    """
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    page = _make_one_page_pdf_with_fields(tmp_path, {}, text="")
    reason = _bidpkg_page_skip_reason(page)
    # AC8 must NOT keep this page — only 708 pages get the keep-guard.
    # Whatever rule fires downstream (blank-text, OBS, GenAI defs) is
    # fine; the negative intent is what we pin.
    if reason is None:
        # If kept, that's only correct if AC8 keep-guard fired. Confirm
        # it didn't — the page has no 708_ fields.
        assert False, (
            "AC8 guard must not keep a page lacking 708_ widgets; "
            "downstream skip rules should have caught it"
        )


def test_708_guard_marker_in_source():
    src = (REPO_ROOT / "src" / "forms" / "reytech_filler_v4.py").read_text(encoding="utf-8")
    assert "PR-AV-AC8" in src, "PR-AV-AC8 marker must remain in reytech_filler_v4.py"
    # The actual implementation line
    assert 'f.startswith("708_")' in src, (
        "the 708 keep-guard must check field names startswith '708_'"
    )


# ── Unpriced-row gate tests ─────────────────────────────────────────


def _find_function(target_path: Path, name: str) -> ast.FunctionDef:
    src = target_path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {target_path}")


def test_unpriced_gate_present_in_generate_rfq_package():
    """Source-grep the unpriced gate. We check that the function
    body contains:
      - the PR-AV-AC8 marker
      - a check for price <= 0
      - the redirect to /rfq/<rid>
      - the ?force=1 escape hatch
    """
    target = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"
    src = target.read_text(encoding="utf-8")
    ac8_idx = src.find("PR-AV-AC8")
    assert ac8_idx > 0
    # Look in the ~2500 char window around the marker
    block = src[ac8_idx:ac8_idx + 2500]
    assert "unpriced" in block.lower(), (
        "AC8 gate block must mention 'unpriced' in its diagnostic "
        "message / comments"
    )
    assert "_price <= 0" in block or "<= 0" in block, (
        "AC8 gate must compare price <= 0 to flag empties + negatives"
    )
    assert 'request.args.get("force")' in block, (
        "AC8 gate must honor ?force=1 escape hatch"
    )
    assert f'redirect(f"/rfq/{{rid}}")' in block, (
        "AC8 gate failure must redirect back to /rfq/<rid> so the "
        "operator can fix pricing"
    )


def test_unpriced_gate_runs_after_form_price_application():
    """Order matters: gate runs AFTER the Step-1 loop that applies
    form-submitted price overrides. Otherwise an operator typing a
    price in the same POST would be falsely rejected.
    """
    target = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"
    src = target.read_text(encoding="utf-8")
    fn = _find_function(target, "generate_rfq_package")
    # Locate the form-loop and AC8 gate by string markers
    step1_marker = src.find("Step 1: Save ALL fields from form", fn.col_offset)
    ac8_marker = src.find("PR-AV-AC8", fn.col_offset)
    assert step1_marker > 0 and ac8_marker > 0
    assert step1_marker < ac8_marker, (
        "AC8 unpriced gate must come AFTER the Step-1 form-price "
        "application loop"
    )
