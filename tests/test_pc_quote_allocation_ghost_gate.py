"""Regression: PC-side quote-counter allocation must refuse to burn a
real seq on a ghost PC (placeholder pc_number, zero items, Reytech-
internal buyer).

PR #675 (2026-05-01) shipped the RFQ-side gate — `is_ready_for_quote_
allocation(rfq)` — at routes_rfq_gen.py:2057. The PC side (per the
session memo `project_session_2026_05_01_ghost_quote_arc.md`) has
parallel allocation paths that were left ungated and could burn ghost
seqs the same way:

  - `routes_pricecheck.py:3661`  — direct `_next_quote_number()` call
  - `routes_crm.py:1914`         — passes `quote_number=None` to
                                    `generate_quote_from_pc`, so
                                    allocation falls through to
                                    `quote_generator.py:913`
  - `routes_simple_submit.py`    — direct `generate_quote()` call
                                    with no quote_number, same fall-through

This file exercises the parallel helper `is_ready_for_pc_quote_allocation`
and pins the same three hard rules — placeholder pc_number, zero
items, Reytech buyer — so the PC side stays in lockstep with the RFQ
side as Mike adds new placeholder vocabulary.
"""
from __future__ import annotations

import os
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.api.dashboard import (
    is_ready_for_pc_quote_allocation,
    is_ready_for_quote_allocation,
    _is_placeholder_number,
)


# ── Helper: minimum-viable PC shape that passes all gates ──────────────

def _clean_pc() -> dict:
    """A minimum-shape PC that should pass every ghost gate."""
    return {
        "id": "pc_test_clean",
        "pc_number": "PC-2026-0001",   # operator-style PC number
        "requestor_email": "buyer@calvet.ca.gov",
        "items": [
            {"qty": 1, "description": "Real product", "unit_price": 100.0},
        ],
    }


# ── Hard rule: placeholder pc_number blocks allocation ─────────────────

class TestPlaceholderPcNumberBlocks:

    def test_worksheet_pc_blocks(self):
        pc = _clean_pc()
        pc["pc_number"] = "WORKSHEET"
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons), (
            f"Expected a 'placeholder' reason naming WORKSHEET; got {reasons!r}"
        )
        assert any("WORKSHEET" in r for r in reasons), (
            "The blocking message should name the offending pc# verbatim"
        )

    @pytest.mark.parametrize("placeholder", [
        "WORKSHEET", "GOOD", "RFQ", "QUOTE", "TEST", "TBD",
        "unknown", "", "   ",
    ])
    def test_known_placeholders_block(self, placeholder: str):
        pc = _clean_pc()
        pc["pc_number"] = placeholder
        ok, _reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False, (
            f"placeholder pc# {placeholder!r} should block allocation"
        )

    def test_blank_pc_number_blocks(self):
        pc = _clean_pc()
        pc["pc_number"] = ""
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons)

    def test_missing_pc_number_key_blocks(self):
        pc = _clean_pc()
        del pc["pc_number"]
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False
        assert any("placeholder" in r.lower() for r in reasons)

    def test_auto_id_pc_number_passes(self):
        """ingest_v2 generates AUTO_<id> for PCs missing a real PC# —
        same canonical placeholder shape as RFQs. Must pass the gate."""
        pc = _clean_pc()
        pc["pc_number"] = "AUTO_5063d1cd"
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is True, (
            f"AUTO_<id> pc# must pass; reasons={reasons!r}"
        )


# ── Hard rule: zero items blocks allocation ────────────────────────────

class TestZeroItemsBlocks:

    def test_empty_items_blocks(self):
        pc = _clean_pc()
        pc["items"] = []
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False
        assert any("zero line items" in r.lower() for r in reasons)

    def test_missing_items_key_blocks(self):
        pc = _clean_pc()
        del pc["items"]
        ok, _reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False

    def test_falls_back_to_line_items_key(self):
        """Some PC paths use `line_items` instead of `items`. Honor
        either, parallel to the RFQ helper."""
        pc = _clean_pc()
        del pc["items"]
        pc["line_items"] = [{"qty": 1, "description": "Legacy line_items[]"}]
        ok, _reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is True


# ── Hard rule: Reytech buyer blocks allocation ─────────────────────────

class TestReytechBuyerBlocks:

    @pytest.mark.parametrize("addr", [
        "sales@reytechinc.com",
        "mike@reytechinc.com",
        "Mike@ReytechInc.Com",
        " mike@reytechinc.com ",
    ])
    def test_reytech_email_blocks(self, addr: str):
        pc = _clean_pc()
        pc["requestor_email"] = addr
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is False, (
            f"{addr!r} should block (Reytech is never the buyer)"
        )
        assert any("reytech" in r.lower() for r in reasons)

    def test_real_calvet_buyer_passes(self):
        pc = _clean_pc()
        pc["requestor_email"] = "keith.alsing@calvet.ca.gov"
        ok, reasons = is_ready_for_pc_quote_allocation(pc)
        assert ok is True, f"Real buyer must pass; reasons={reasons!r}"


# ── Happy path ─────────────────────────────────────────────────────────

class TestCleanPcPasses:

    def test_clean_pc_passes(self):
        ok, reasons = is_ready_for_pc_quote_allocation(_clean_pc())
        assert ok is True, f"Clean PC must pass; reasons={reasons!r}"
        assert reasons == []


# ── Multiple reasons surface together ──────────────────────────────────

def test_multiple_ghost_markers_all_surface():
    pc = {
        "id": "pc_ghost",
        "pc_number": "WORKSHEET",
        "requestor_email": "mike@reytechinc.com",
        "items": [],
    }
    ok, reasons = is_ready_for_pc_quote_allocation(pc)
    assert ok is False
    assert len(reasons) >= 3, (
        f"Expected all three blockers (placeholder pc#, zero items, "
        f"Reytech buyer); got {len(reasons)}: {reasons!r}"
    )


# ── PC and RFQ helpers stay in lockstep on shared vocabulary ───────────

class TestPcAndRfqHelpersAgreeOnPlaceholderVocab:
    """If Mike adds a new placeholder string to `_is_placeholder_number`
    (e.g. flagging single-digit sol# per the session memo's 'sol="3" on
    ba4d3457' open question), both helpers must see it. These tests
    pin both to the same shared `_is_placeholder_number` so they
    cannot drift apart."""

    @pytest.mark.parametrize("placeholder", [
        "WORKSHEET", "GOOD", "RFQ", "TBD", "",
    ])
    def test_both_helpers_block_same_placeholder(self, placeholder: str):
        # PC side
        pc = _clean_pc()
        pc["pc_number"] = placeholder
        pc_ok, _ = is_ready_for_pc_quote_allocation(pc)

        # RFQ side
        rfq = {
            "id": "rfq_t",
            "solicitation_number": placeholder,
            "requestor_email": "buyer@calvet.ca.gov",
            "line_items": [{"qty": 1, "description": "x", "price_per_unit": 1.0}],
        }
        rfq_ok, _ = is_ready_for_quote_allocation(rfq)

        assert pc_ok == rfq_ok == False, (
            f"Both helpers must agree {placeholder!r} is a placeholder; "
            f"pc_ok={pc_ok}, rfq_ok={rfq_ok}"
        )


# ── Route gates: each call site refuses ghost data ─────────────────────


class TestRouteGatesBlockGhostAllocation:
    """End-to-end: hit each PC route with a ghost PC, assert no quote
    number is allocated. We don't care about the exact response shape
    here — we care that the counter sequence isn't burned.

    This is the regression-detection layer that catches a future copy-
    paste of a new PC route that forgets to call the gate helper."""

    def _seed_ghost_pc(self, temp_data_dir, pc_id: str) -> dict:
        """Seed a PC that the gate must block on every rule."""
        import json
        pc = {
            "id": pc_id,
            "pc_number": "WORKSHEET",            # placeholder
            "items": [],                         # zero items
            "requestor_email": "sales@reytechinc.com",  # Reytech buyer
            "institution": "CalVet-Yountville",
            "agency": "CalVet",
        }
        path = os.path.join(temp_data_dir, "price_checks.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({pc_id: pc}, f)
        return pc

    def test_pricecheck_generate_quote_route_blocks_ghost(
        self, auth_client, temp_data_dir
    ):
        from src.forms.quote_generator import _next_quote_number
        pc_id = "pc_ghost_route_1"
        self._seed_ghost_pc(temp_data_dir, pc_id)

        seq_before = _next_quote_number()  # peek (this DOES burn one)
        # We compare against the next call to confirm only +1 happened
        # (our peek), not a route allocation on top.
        resp = auth_client.post(f"/pricecheck/{pc_id}/generate-quote")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body.get("ok") is False, (
            f"PC route should refuse ghost allocation; got {body}"
        )
        # The seq should have advanced exactly once (our peek above),
        # not twice (peek + route allocation).
        seq_after = _next_quote_number()
        assert seq_after.split("Q")[1] == str(
            int(seq_before.split("Q")[1]) + 1
        ), (
            f"Seq advanced more than once — route allocated despite "
            f"ghost data. before={seq_before!r}, after={seq_after!r}"
        )
