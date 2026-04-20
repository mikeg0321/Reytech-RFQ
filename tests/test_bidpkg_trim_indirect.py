"""Regression: _bidpkg_page_skip_reason handles IndirectObject /Annots.

Incident 2026-04-20 (R26Q36): BidPackage page trim failed with
"object of type 'IndirectObject' has no len()" because /Annots was
stored as an IndirectObject referencing the array, not the array
itself. The exception was caught by a bare `except Exception`
wrapping the trim block, so the full untrimmed PDF (16 pages
including SABRC/GenAI/VSDS skip pages) was sent to QA — silently
breaking the package.

This test simulates a page whose /Annots entry is an object with
a `get_object()` method (the IndirectObject protocol pypdf uses)
and asserts _bidpkg_page_skip_reason returns without raising.
"""
from __future__ import annotations

from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason


class _FakeIndirectObject:
    """Mimics pypdf's IndirectObject: no __len__, but has get_object()."""

    def __init__(self, resolved):
        self._resolved = resolved

    def get_object(self):
        return self._resolved

    # Explicit: no __len__, no __iter__ at this level.


class _FakePage:
    """Pypdf-page-like: dict __contains__ + .get() + extract_text()."""

    def __init__(self, annots_value=None, text=""):
        self._annots = annots_value
        self._text = text

    def __contains__(self, key):
        return key == "/Annots" and self._annots is not None

    def get(self, key, default=None):
        if key == "/Annots" and self._annots is not None:
            return self._annots
        return default

    def extract_text(self):
        return self._text


def test_skip_reason_resolves_indirect_annots_array():
    """IndirectObject wrapping an empty list must not raise len()."""
    page = _FakePage(annots_value=_FakeIndirectObject([]), text="")
    # Empty text + zero fields → skip reason "blank (no text, no fields)"
    reason = _bidpkg_page_skip_reason(page)
    assert reason == "blank (no text, no fields)", reason


def test_skip_reason_resolves_indirect_annots_with_fields():
    """IndirectObject wrapping an array with field refs must iterate cleanly."""

    class _FakeField:
        def __init__(self, t):
            self._t = t

        def get_object(self):
            return {"/T": self._t}

    annots = [_FakeField("OBS 1600 Row1"), _FakeField("OBS 1600 Row2")]
    page = _FakePage(annots_value=_FakeIndirectObject(annots), text="")
    reason = _bidpkg_page_skip_reason(page)
    assert reason == "OBS 1600 food entry form", reason


def test_skip_reason_plain_list_annots_still_works():
    """Direct list (non-indirect) must continue to work — no regression."""
    page = _FakePage(annots_value=[], text="")
    reason = _bidpkg_page_skip_reason(page)
    assert reason == "blank (no text, no fields)", reason


def test_skip_reason_no_annots_key_still_works():
    """Page with no /Annots at all must not raise."""
    page = _FakePage(annots_value=None, text="some normal content")
    # Non-empty text + no fields + no skip pattern → KEEP (None)
    reason = _bidpkg_page_skip_reason(page)
    assert reason is None, reason
