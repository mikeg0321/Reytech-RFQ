"""Pins for the 703A signature overlay.

Coleman 10842771 punch-list 2026-05-28: the AMS 703A Rev. 03/2025 form
shipped UNSIGNED. `fill_703a` calls `mirror_fill_from_prior_pdf` which
copies form-field /V values only — /Sig form fields and signature
graphics from the prior 703B are not carried over. The 703A blank
template has a /Sig field named ``703A_Signature`` on page 0 which
needs an explicit overlay pass.

Fix: add ``703A_Signature`` to ``SIGN_FIELDS`` + branch in fill_703a
to call ``_overlay_signature`` (writer-based, draws onto /Sig field
/Rect) when present, falling back to ``_703b_overlay_signature``
(pdfminer-located "Bidder Signature" label, for legacy variants
without the /Sig field).

These tests pin both halves of the contract.
"""
from __future__ import annotations

import os
import tempfile

import pytest
from pypdf import PdfReader

_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "templates",
)
_703A_BLANK = os.path.join(_TEMPLATE_DIR, "703a_blank.pdf")
_FIX_BLANK = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "703b_blank.pdf",
)

pytestmark = pytest.mark.skipif(
    not (os.path.exists(_703A_BLANK) and os.path.exists(_FIX_BLANK)),
    reason="703A/703B blank templates not available",
)


def _build_prior_703b(cfg) -> bytes:
    """Generate a filled+signed 703B to act as the prior submission
    fill_703a's mirror-fill pulls from."""
    from src.forms.reytech_filler_v4 import fill_703b
    out = tempfile.mktemp(suffix=".pdf")
    fill_703b(
        _FIX_BLANK,
        {
            "solicitation_number": "703A-SIG-TEST",
            "sign_date": "05/28/2026",
            "due_date": "06/15/2026",
        },
        cfg,
        out,
    )
    with open(out, "rb") as f:
        return f.read()


# ── SIGN_FIELDS membership pin ────────────────────────────────────────


def test_703a_signature_in_sign_fields():
    """703A_Signature must be in SIGN_FIELDS — otherwise
    `_overlay_signature` skips the /Sig field silently and the 703A
    ships unsigned (the Coleman 10842771 punch-list bug).
    """
    from src.forms.reytech_filler_v4 import SIGN_FIELDS
    assert "703A_Signature" in SIGN_FIELDS, (
        f"703A_Signature missing from SIGN_FIELDS — overlay will skip "
        f"the field silently. Add it or rename the field on the blank "
        f"template if intentional. Current entries: {sorted(SIGN_FIELDS)}"
    )


def test_703a_blank_template_has_sig_field():
    """Floor — the 703A blank template must still have the /Sig form
    field this PR's overlay path targets. If a future template rev
    removes the field (e.g. switching to a printed signature line),
    flip the fallback overlay path on by adjusting the dispatch in
    fill_703a.
    """
    reader = PdfReader(_703A_BLANK)
    sig_field_names = []
    for pg in reader.pages:
        for annot in (pg.get("/Annots") or []):
            obj = annot.get_object()
            if str(obj.get("/FT", "")) == "/Sig":
                sig_field_names.append(str(obj.get("/T", "")))
    assert "703A_Signature" in sig_field_names, (
        f"703A blank template no longer has /Sig field "
        f"'703A_Signature'. Found {sig_field_names}. Update the "
        f"fallback path in fill_703a (`_has_sig_field` branch)."
    )


# ── fill_703a end-to-end pin ──────────────────────────────────────────


def test_fill_703a_signs_the_sig_field(monkeypatch):
    """End-to-end pin — `fill_703a` with a real prior 703B produces a
    703A where page 0's content stream has GROWN vs the blank
    template. Growth indicates the signature draw operators (cm + Do)
    were added to the page. Without the overlay (the pre-fix bug),
    delta is ~0 and the 703A ships visibly unsigned.
    """
    from src.forms.reytech_filler_v4 import load_config, fill_703a
    import src.forms.prior_submissions as prior_submissions

    cfg = load_config()
    prior_bytes = _build_prior_703b(cfg)
    # Inject the prior so fill_703a's mirror-fill finds it without
    # needing a populated prior_submissions DB table.
    monkeypatch.setattr(
        prior_submissions, "latest_for", lambda *a, **k: prior_bytes,
    )

    out = tempfile.mktemp(suffix=".pdf")
    fill_703a(_703A_BLANK, {
        "solicitation_number": "703A-SIG-TEST",
        "sign_date": "05/28/2026",
        "due_date": "06/15/2026",
    }, cfg, out)

    def _content_stream_len(page) -> int:
        out_bytes = b""
        for c in (page.get("/Contents") or []):
            try:
                obj = c.get_object() if hasattr(c, "get_object") else c
                if hasattr(obj, "get_data"):
                    out_bytes += obj.get_data()
            except Exception:
                pass
        return len(out_bytes)

    blank = PdfReader(_703A_BLANK)
    filled = PdfReader(out)
    blank_len = _content_stream_len(blank.pages[0])
    filled_len = _content_stream_len(filled.pages[0])
    growth = filled_len - blank_len

    # The signature overlay adds image draw operators (cm + Do +
    # state save/restore) — typically ~100-300 bytes net even when
    # the underlying image is shared via XObject reuse. Pre-fix the
    # 703A page 0 content stream was identical to the blank (overlay
    # never ran) so growth was 0.
    assert growth >= 50, (
        f"703A page 0 content stream did not grow vs blank "
        f"(blank={blank_len}, filled={filled_len}, delta={growth}). "
        f"The signature overlay either didn't run or didn't add draw "
        f"operators. Check that:\n"
        f"  * fill_703a's /Sig dispatch hits _overlay_signature\n"
        f"  * SIGN_FIELDS includes 703A_Signature\n"
        f"  * SIGNATURE_PATH exists and is readable\n"
    )


def test_fill_703a_does_not_break_on_missing_prior(monkeypatch):
    """When no prior 703B exists at all, fill_703a must copy the input
    unchanged WITHOUT raising — the existing behavior. The new
    signature overlay code path runs ONLY in the mirror-fill success
    case, so this regression-pin guarantees a clean fallback.
    """
    from src.forms.reytech_filler_v4 import load_config, fill_703a
    import src.forms.prior_submissions as prior_submissions

    cfg = load_config()
    monkeypatch.setattr(
        prior_submissions, "latest_for", lambda *a, **k: None,
    )

    out = tempfile.mktemp(suffix=".pdf")
    fill_703a(_703A_BLANK, {
        "solicitation_number": "NO-PRIOR",
        "sign_date": "05/28/2026",
        "due_date": "06/15/2026",
    }, cfg, out)

    assert os.path.exists(out)
    assert os.path.getsize(out) > 0
