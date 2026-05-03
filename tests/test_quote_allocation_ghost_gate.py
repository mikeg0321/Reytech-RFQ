"""Regression: quote-counter allocation must refuse to burn a real seq
on a ghost RFQ (placeholder sol#, zero items, Reytech-internal buyer).

Incident 2026-05-01 (rfq_7813c4e1, R26Q45):
  Keith Alsing CalVet RFQ parsed with `solicitation_number="WORKSHEET"`
  (placeholder fallback when neither subject nor PDF surfaced a real
  sol#). Mike clicked Generate Package on the review screen. The route
  saw no `locked_qn`, called `_next_quote_number()` unconditionally,
  and burned R26Q45 on the placeholder RFQ — polluting the QuoteWerks-
  synced sequence with a phantom assignment.

  Mike's escalation: *"there is no way this is R26Q45, this is a constant
  issue, fix ghost data and regenerate with correct number"* — and on
  follow-up *"look into system wide fix, check memory this happens way
  too frequenlty and is a P0 hold up when quoting"*.

Fix shape: gate the allocation site in routes_rfq_gen.py with a new
helper `is_ready_for_quote_allocation(rfq)` that reuses the existing
`_is_placeholder_number` infrastructure. On ghost detection, refuse
allocation, surface reasons to the operator, and never silent-burn.
"""
from __future__ import annotations

import os
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.api.dashboard import (
    is_ready_for_quote_allocation,
    _is_placeholder_number,
)


# ── Helper: minimum-viable RFQ shape that passes all gates ─────────────

def _clean_rfq() -> dict:
    """A minimum-shape RFQ that should pass every ghost gate."""
    return {
        "id": "rfq_test_clean",
        "solicitation_number": "8955-00001234",   # real CalVet PO format
        "requestor_email": "buyer@calvet.ca.gov",
        "line_items": [
            {"qty": 1, "description": "Real product", "price_per_unit": 100.0},
        ],
    }


# ── Hard rule: placeholder sol# blocks allocation ──────────────────────

class TestPlaceholderSolicitationBlocks:
    """The exact incident shape: sol# = 'WORKSHEET'. Must block."""

    def test_worksheet_sol_blocks(self):
        rfq = _clean_rfq()
        rfq["solicitation_number"] = "WORKSHEET"
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons), (
            f"Expected a 'placeholder' reason naming WORKSHEET; got {reasons!r}"
        )
        assert any("WORKSHEET" in r for r in reasons), (
            "The blocking message should name the offending sol# verbatim "
            "so the operator can grep for it on the detail page"
        )

    @pytest.mark.parametrize("placeholder", [
        "WORKSHEET", "GOOD", "RFQ", "QUOTE", "TEST", "TBD",
        "unknown", "", "   ",
    ])
    def test_known_placeholders_block(self, placeholder: str):
        rfq = _clean_rfq()
        rfq["solicitation_number"] = placeholder
        ok, _reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False, (
            f"placeholder sol# {placeholder!r} should block allocation"
        )

    def test_blank_sol_blocks(self):
        rfq = _clean_rfq()
        rfq["solicitation_number"] = ""
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons)

    def test_missing_sol_key_blocks(self):
        rfq = _clean_rfq()
        del rfq["solicitation_number"]
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons)


# ── Hard rule: zero items blocks allocation ────────────────────────────

class TestZeroItemsBlocks:
    """Nothing to quote → don't burn a seq."""

    def test_empty_line_items_blocks(self):
        rfq = _clean_rfq()
        rfq["line_items"] = []
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False
        assert any("zero line items" in r.lower() for r in reasons)

    def test_missing_line_items_key_blocks(self):
        rfq = _clean_rfq()
        del rfq["line_items"]
        ok, _reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False

    def test_falls_back_to_items_key(self):
        # Some legacy paths use `items` instead of `line_items`. Honor either.
        rfq = _clean_rfq()
        del rfq["line_items"]
        rfq["items"] = [{"qty": 1, "description": "Legacy items[]"}]
        ok, _reasons = is_ready_for_quote_allocation(rfq)
        assert ok is True


# ── Hard rule: Reytech buyer blocks allocation ─────────────────────────

class TestReytechBuyerBlocks:
    """Reytech is the seller — never the buyer. If parser tagged a Reytech
    address as the requestor, it misclassified the sender direction."""

    @pytest.mark.parametrize("addr", [
        "sales@reytechinc.com",
        "mike@reytechinc.com",
        "Mike@ReytechInc.Com",  # case-insensitive
        " mike@reytechinc.com ",  # trim whitespace
    ])
    def test_reytech_email_blocks(self, addr: str):
        rfq = _clean_rfq()
        rfq["requestor_email"] = addr
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is False, f"{addr!r} should block (Reytech is never the buyer)"
        assert any("reytech" in r.lower() for r in reasons)

    def test_real_calvet_buyer_passes(self):
        rfq = _clean_rfq()
        rfq["requestor_email"] = "keith.alsing@calvet.ca.gov"
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is True, f"Real CalVet buyer must pass; reasons={reasons!r}"


# ── Happy path: clean RFQ allocates ────────────────────────────────────

class TestCleanRfqPasses:
    """Belt-and-suspenders: positive case so a future tightening doesn't
    silently start blocking real RFQs."""

    def test_clean_calvet_rfq_passes(self):
        ok, reasons = is_ready_for_quote_allocation(_clean_rfq())
        assert ok is True, f"Clean RFQ must pass; reasons={reasons!r}"
        assert reasons == []

    def test_auto_id_sol_passes(self):
        # ingest_v2 generates AUTO_<id> as a deterministic placeholder for
        # records the parser couldn't extract a real sol# from. The blocker
        # is for buyer-content junk (WORKSHEET / GOOD), NOT for the
        # canonical AUTO_ format which is a known-good placeholder.
        rfq = _clean_rfq()
        rfq["solicitation_number"] = "AUTO_7813c4e1"
        ok, reasons = is_ready_for_quote_allocation(rfq)
        assert ok is True, f"AUTO_<id> sol# must pass; reasons={reasons!r}"


# ── Multiple reasons surface together ──────────────────────────────────

def test_multiple_ghost_markers_all_surface():
    """Don't short-circuit on the first failure — the operator should see
    every reason at once so they can fix in one pass instead of N
    error→fix→error→fix cycles."""
    rfq = {
        "id": "rfq_ghost",
        "solicitation_number": "WORKSHEET",
        "requestor_email": "mike@reytechinc.com",
        "line_items": [],
    }
    ok, reasons = is_ready_for_quote_allocation(rfq)
    assert ok is False
    assert len(reasons) >= 3, (
        f"Expected all three blockers (placeholder sol, zero items, "
        f"Reytech buyer); got {len(reasons)}: {reasons!r}"
    )


# ── _is_placeholder_number coverage on the gate's vocabulary ───────────

class TestPlaceholderHelperVocabulary:
    """Pin the existing helper's behavior so a refactor doesn't drift away
    from what the gate trusts."""

    def test_worksheet_is_placeholder(self):
        assert _is_placeholder_number("WORKSHEET") is True

    def test_auto_prefix_is_not_placeholder(self):
        # AUTO_ format is the canonical placeholder ingest_v2 emits — not
        # buyer-content junk. Must pass.
        assert _is_placeholder_number("AUTO_7813c4e1") is False

    def test_real_sol_is_not_placeholder(self):
        assert _is_placeholder_number("8955-00001234") is False
        assert _is_placeholder_number("4500750017") is False
        assert _is_placeholder_number("4440-RFQ-2026-001") is False

    def test_short_numeric_sol_is_placeholder(self):
        """Pure-digit 1-2 char sol#s are parser artifacts.

        Authorised 2026-05-03 after RFQ `ba4d3457` got sol="3" through the
        existing gate. Real CA gov solicitations are 10+ chars with agency
        prefixes (CalVet `8955-`, CCHCS `4500…`, DSH `4440-`), so a bare
        single or double digit is junk.
        """
        for s in ("3", "1", "8", "9", "12", "42", "00", "99"):
            assert _is_placeholder_number(s) is True, (
                f"short numeric {s!r} should be flagged as placeholder")

    def test_three_plus_digit_numeric_still_passes(self):
        """Don't tighten beyond what we can defend. 3+ digit pure numerics
        could be internal codes or short county refs — leave them alone
        until we see a concrete miss."""
        for s in ("123", "1234", "12345"):
            assert _is_placeholder_number(s) is False, (
                f"3+ digit numeric {s!r} should NOT be flagged "
                "(would block real short-form sol#s)")
