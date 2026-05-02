"""Tests for src/forms/double_sig_scanner.py — pre-flight detector that
hard-blocks Send when an output PDF carries two signatures on the same
page band. The scan is deliberately conservative (over-flag rather than
under-flag) because buyers reject double-signed packages.

PR-B1.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.forms.double_sig_scanner import scan_package_for_double_sigs


def _make_page(*, height=792, sig_rects=(), tokens=()):
    """Build a fake pdfplumber page that the scanner can introspect.

    sig_rects: iterable of (y_low, y_high) tuples — each becomes one /Sig
    AcroForm widget annotation (FT=/Sig).
    tokens: iterable of (text, top_y) tuples returned by extract_words.
    """
    page = MagicMock()
    page.height = height
    annots = []
    for y_low, y_high in sig_rects:
        annots.append({
            "rect": [0, y_low, 100, y_high],
            "data": {"FT": "/Sig", "Subtype": "/Widget"},
        })
    page.annots = annots
    page.extract_words = MagicMock(return_value=[
        {"text": t, "top": y, "x0": 100, "x1": 200, "bottom": y + 12}
        for t, y in tokens
    ])
    return page


def _patch_pdfplumber(pages):
    """Build a context-manager-friendly pdfplumber.open mock."""
    pdf = MagicMock()
    pdf.pages = pages
    cm = MagicMock()
    cm.__enter__.return_value = pdf
    cm.__exit__.return_value = False
    return cm


def test_clean_pdf_returns_empty():
    # Single /Sig in band, no overlay name tokens → clean
    page = _make_page(sig_rects=[(700, 720)], tokens=[])
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs([("704b", "/tmp/clean.pdf")])
    assert issues == []


def test_acroform_plus_overlay_flags():
    # /Sig field in lower band + "Michael Guadan" overlay in same band → flag
    page = _make_page(
        sig_rects=[(700, 720)],
        tokens=[("Michael", 730), ("Guadan", 730)],
    )
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs([("704b", "/tmp/p.pdf")])
    assert len(issues) == 1
    assert issues[0]["kind"] == "acroform_plus_overlay"
    assert issues[0]["form_id"] == "704b"
    assert issues[0]["page"] == 1


def test_two_overlay_signatures_flag():
    # No /Sig field, but TWO "Michael Guadan" tokens-pairs in the lower band
    page = _make_page(
        sig_rects=[],
        tokens=[
            ("Michael", 720), ("Guadan", 720),
            ("Michael", 750), ("Guadan", 750),
        ],
    )
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs([("703b", "/tmp/p.pdf")])
    assert len(issues) == 1
    assert issues[0]["kind"] == "overlay_pair_same_band"


def test_two_acroform_sigs_flag():
    page = _make_page(sig_rects=[(700, 720), (740, 760)], tokens=[])
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs([("cv012", "/tmp/p.pdf")])
    assert len(issues) == 1
    assert issues[0]["kind"] == "double_acroform_sig"


def test_overlay_outside_band_is_ignored():
    # "Michael Guadan" in TOP of page (e.g. header logo text) shouldn't trip
    page = _make_page(
        sig_rects=[(700, 720)],
        tokens=[("Michael", 50), ("Guadan", 50)],
    )
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs([("704b", "/tmp/p.pdf")])
    assert issues == []


def test_missing_file_is_skipped_silently():
    # Path doesn't exist — scanner shouldn't raise, just skip
    with patch("os.path.exists", return_value=False):
        issues = scan_package_for_double_sigs([("704b", "/tmp/nope.pdf")])
    assert issues == []


def test_pdfplumber_missing_returns_empty():
    # If pdfplumber import fails, scanner degrades to no-op (logs warning)
    with patch.dict("sys.modules", {"pdfplumber": None}):
        # Force ImportError by removing the module from sys.modules + re-import
        # Simpler: patch the import inside the function via builtins
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pdfplumber":
                raise ImportError("simulated")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            issues = scan_package_for_double_sigs([("704b", "/tmp/p.pdf")])
    assert issues == []


def test_accepts_bare_path_entries():
    # Older callers may pass plain paths instead of (form_id, path) tuples
    page = _make_page(sig_rects=[(700, 720)], tokens=[])
    with patch("pdfplumber.open", return_value=_patch_pdfplumber([page])):
        with patch("os.path.exists", return_value=True):
            issues = scan_package_for_double_sigs(["/tmp/clean.pdf"])
    assert issues == []
