"""BidPackage page-trim respects agency required_forms.

Canonical incident — RFQ e02b7fa6 PVSP 2026-05-13 (CCHCS, missed COB
to Mohammad@CDCR): the page-trim assumed bidder_decl + darfur_act
standalones would always be generated alongside the BidPackage, so it
silently stripped the inline GSPD-05-105 + Darfur pages from CCHCS
output. CCHCS `required_forms = ["703b","704b","bidpkg","quote"]` —
no standalones generated → those inline pages had to STAY.

Fix (PR mr-wolf substrate-pivot 2026-05-13): the trim accepts a
`replaced_by_standalone` frozenset derived from `match_agency()`.
The 6 "standalone used" rules only fire when their token is in the
set. Always-skip rules (CalRecycle SABRC reference table, OBS 1600,
GenAI defs, VSDS instruction, blank pages) ignore the set.

This test pins both directions so a future "just trim everything"
regression can't sneak past.
"""
from __future__ import annotations

from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason


# ── Page mocks ──────────────────────────────────────────────────────


class _FakeField:
    def __init__(self, t: str):
        self._t = t

    def get_object(self):
        return {"/T": self._t}


class _FakePage:
    """Pypdf-page-like: dict __contains__ + .get() + extract_text()."""

    def __init__(self, annots=None, text: str = ""):
        self._annots = list(annots) if annots else []
        self._text = text

    def __contains__(self, key):
        return key == "/Annots" and self._annots is not None

    def get(self, key, default=None):
        if key == "/Annots":
            return self._annots
        return default

    def extract_text(self):
        return self._text


# ── Test fixtures: standalone-replaced sets ────────────────────────


CCHCS_REPLACED = frozenset()  # CCHCS generates no bidder_decl / darfur_act standalones
CALVET_REPLACED = frozenset({"bidder_decl", "darfur_act"})


# ── GSPD-05-105 Bidder Declaration (field fingerprint) ─────────────


def _gspd_page() -> _FakePage:
    """A BidPackage page whose field names match the GSPD-05-105
    Bidder Declaration inline form: Text0_105, Check3_105, Page1_105."""
    return _FakePage(
        annots=[_FakeField("Text0_105"), _FakeField("Check3_105"),
                _FakeField("Page1_105"), _FakeField("Text4_105")],
        text="GSPD-05-105 Bidder Declaration\nSubcontractor info...",
    )


def test_cchcs_keeps_inline_gspd_bidder_declaration():
    """CCHCS does not generate a bidder_decl standalone — the inline
    page MUST stay in the trimmed BidPackage output."""
    reason = _bidpkg_page_skip_reason(_gspd_page(), replaced_by_standalone=CCHCS_REPLACED)
    assert reason is None, (
        f"CCHCS BidPackage trim must KEEP the inline GSPD page; got skip={reason!r}"
    )


def test_calvet_skips_inline_gspd_bidder_declaration():
    """CalVet generates a bidder_decl standalone — the inline page
    SHOULD be skipped (replaced by the standalone)."""
    reason = _bidpkg_page_skip_reason(_gspd_page(), replaced_by_standalone=CALVET_REPLACED)
    assert reason is not None, (
        "CalVet BidPackage trim must SKIP the inline GSPD page when "
        "bidder_decl standalone will replace it"
    )
    assert "Bidder Declaration" in reason or "GSPD" in reason, reason


# ── Darfur pg1 (text fingerprint) ───────────────────────────────────


def _darfur_pg1() -> _FakePage:
    return _FakePage(
        annots=[],
        text=(
            "Darfur Contracting Act\n"
            "Public Contract Code § 10475\n"
            "Scrutinized Companies List\n"
            "Bidder certifies..."
        ),
    )


def test_cchcs_keeps_darfur_pg1():
    reason = _bidpkg_page_skip_reason(_darfur_pg1(), replaced_by_standalone=CCHCS_REPLACED)
    assert reason is None, f"CCHCS should KEEP Darfur pg1; got {reason!r}"


def test_calvet_skips_darfur_pg1():
    reason = _bidpkg_page_skip_reason(_darfur_pg1(), replaced_by_standalone=CALVET_REPLACED)
    assert reason == "Darfur pg1 (standalone used)", reason


# ── Darfur pg2 ──────────────────────────────────────────────────────


def _darfur_pg2() -> _FakePage:
    return _FakePage(
        annots=[],
        text=(
            "Public Contract Code § 10476\n"
            "Scrutinized Company written permission..."
        ),
    )


def test_cchcs_keeps_darfur_pg2():
    reason = _bidpkg_page_skip_reason(_darfur_pg2(), replaced_by_standalone=CCHCS_REPLACED)
    assert reason is None, f"CCHCS should KEEP Darfur pg2; got {reason!r}"


def test_calvet_skips_darfur_pg2():
    reason = _bidpkg_page_skip_reason(_darfur_pg2(), replaced_by_standalone=CALVET_REPLACED)
    assert reason == "Darfur pg2 (standalone used)", reason


# ── Always-skip rules: ignore the set ───────────────────────────────


def _calrecycle_sabrc_reference() -> _FakePage:
    return _FakePage(
        annots=[],
        text="SABRC@calrecycle.ca.gov\nProduct category reference table",
    )


def test_calrecycle_sabrc_reference_table_always_skipped_cchcs():
    """The CalRecycle SABRC reference table is pure documentation —
    always skip, never form-replaces, regardless of agency."""
    reason = _bidpkg_page_skip_reason(
        _calrecycle_sabrc_reference(), replaced_by_standalone=CCHCS_REPLACED
    )
    assert reason == "CalRecycle SABRC reference table", reason


def test_calrecycle_sabrc_reference_table_always_skipped_calvet():
    reason = _bidpkg_page_skip_reason(
        _calrecycle_sabrc_reference(), replaced_by_standalone=CALVET_REPLACED
    )
    assert reason == "CalRecycle SABRC reference table", reason


# ── OBS 1600 always-skip (field fingerprint) ───────────────────────


def test_obs_1600_food_entry_always_skipped():
    """OBS 1600 food entry form is filled inline by Reytech logic
    (`fill_obs1600_fields`) but the FOOD CODES reference table and
    footnotes are always skipped — that's an unchanged behavior."""
    page = _FakePage(
        annots=[_FakeField("OBS 1600 Row1 Pounds"),
                _FakeField("OBS 1600 Row1 Code")],
        text="",
    )
    for replaced in (CCHCS_REPLACED, CALVET_REPLACED):
        reason = _bidpkg_page_skip_reason(page, replaced_by_standalone=replaced)
        assert reason == "OBS 1600 food entry form", (replaced, reason)


# ── Default arg (backwards-compat) ─────────────────────────────────


def test_default_arg_treats_set_as_empty_keeps_inline_pages():
    """Callers that don't pass `replaced_by_standalone` get the
    CCHCS-safe default (empty set → inline pages preserved). This pins
    the backwards-compat shim so the existing
    `routes_rfq_admin.py` / `routes_rfq_gen.py` callers don't have to
    change in this PR."""
    reason = _bidpkg_page_skip_reason(_gspd_page())  # no kwarg
    assert reason is None, (
        f"Default arg must be CCHCS-safe (keep); got {reason!r}"
    )
