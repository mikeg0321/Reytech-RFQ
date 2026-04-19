"""Wiring tests for DSH packet dispatch (PR3).

PR2 shipped the AttA/B/C overlay fillers. PR3 wires them in:

  1. `identify_attachments()` registers the buyer's per-solicitation
     AttA/B/C source PDFs under tmpl["dsh_attA"|"dsh_attB"|"dsh_attC"].
  2. The DSH agency_config requires those three IDs so `_include()`
     in routes_rfq_gen returns True for a DSH-matched RFQ.
  3. The FILLERS dispatcher in dsh_attachment_fillers exposes the
     three callable names the dispatch block looks up.

These tests pin the dispatch contract — if any of the three pieces
drift, packet generation for DSH would silently skip the attachments.
"""
from __future__ import annotations

from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
from src.forms.dsh_attachment_fillers import FILLERS as DSH_FILLERS


class TestDshAgencyConfigContract:

    def test_dsh_required_forms_include_attachments(self):
        cfg = DEFAULT_AGENCY_CONFIGS["dsh"]
        req = set(cfg["required_forms"])
        assert {"dsh_attA", "dsh_attB", "dsh_attC"}.issubset(req), (
            f"DSH required_forms missing AttA/B/C — has: {sorted(req)}"
        )

    def test_dsh_still_requires_baseline_forms(self):
        """Adding the attachments shouldn't have dropped any baseline form."""
        req = set(DEFAULT_AGENCY_CONFIGS["dsh"]["required_forms"])
        for baseline in ("quote", "std204", "sellers_permit", "dvbe843",
                         "calrecycle74"):
            assert baseline in req, f"DSH lost baseline form: {baseline}"


class TestDshFillerDispatcher:

    def test_fillers_dict_has_three_callables(self):
        for name in ("fill_dsh_attachment_a",
                     "fill_dsh_attachment_b",
                     "fill_dsh_attachment_c"):
            assert name in DSH_FILLERS, f"FILLERS missing {name}"
            assert callable(DSH_FILLERS[name])

    def test_filler_names_match_dispatch_block(self):
        """The dispatch in routes_rfq_gen.py looks these up by string —
        a rename in dsh_attachment_fillers without updating the dispatch
        would silently skip the form. Pin the exact names."""
        assert sorted(DSH_FILLERS.keys()) == [
            "fill_dsh_attachment_a",
            "fill_dsh_attachment_b",
            "fill_dsh_attachment_c",
        ]
