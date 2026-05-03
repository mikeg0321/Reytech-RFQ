"""Tests for PR-B: AMS 704 ingest drift surfaces #2 (qty/quantity dual-write)
and #4 (requestor_name email-sender fallback).

Triggered by 2026-05-03 finding (Mike: "this is a good example of things
not working as expected"). Companion to PR-A (PR #707) which closed
surfaces #3, #5, #6.
"""
from __future__ import annotations

import pytest


# ─── Drift #2: LineItem dual-writes qty + quantity ──────────────────────

class TestLineItemQtyMirror:
    """`LineItem.to_dict()` is the canonical serialization of a parsed
    item. Most readers in the codebase use `qty`, but a few use
    `quantity`. Mirror both keys at write time so reading either is
    consistent."""

    def test_to_dict_writes_both_qty_and_quantity(self):
        from src.forms.ams704_helpers import LineItem
        li = LineItem(
            line_number=1,
            description="Heel Donut Cushions",
            qty=12,
            uom="EA",
            unit_price=0.0,
        )
        d = li.to_dict()
        assert d["qty"] == 12
        assert d["quantity"] == 12

    def test_dual_write_with_zero_qty(self):
        """Edge: qty=0 (no_bid scenario) must still mirror, not skip."""
        from src.forms.ams704_helpers import LineItem
        li = LineItem(line_number=1, description="x", qty=0, uom="EA")
        d = li.to_dict()
        assert d["qty"] == 0
        assert d["quantity"] == 0

    def test_from_dict_then_to_dict_round_trip(self):
        """When LineItem.from_dict reads `qty` only and to_dict writes both,
        the round trip must yield consistent output regardless of which
        key the input used."""
        from src.forms.ams704_helpers import LineItem
        # Input has qty only
        a = LineItem.from_dict({"description": "A", "qty": 5})
        assert a.to_dict()["qty"] == 5
        assert a.to_dict()["quantity"] == 5
        # Input has quantity only
        b = LineItem.from_dict({"description": "B", "quantity": 7})
        assert b.to_dict()["qty"] == 7
        assert b.to_dict()["quantity"] == 7


# ─── Drift #4: requestor_name email-sender fallback ─────────────────────

class TestRequestorNameFallback:
    """When the PDF header didn't capture a Requestor (blank field,
    body-only RFQ, etc), derive a display name from the email sender
    address. Priority: PDF header → RFC822 display → local-part heuristic."""

    def test_helper_derives_from_local_part(self):
        from src.core.ingest_pipeline import _derive_requestor_name
        assert _derive_requestor_name("valentina.demidenko@cdcr.ca.gov") == "Valentina Demidenko"
        assert _derive_requestor_name("keith.alsing@calvet.ca.gov") == "Keith Alsing"
        assert _derive_requestor_name("k_underscore_name@x.com") == "K Underscore Name"

    def test_helper_uses_rfc822_display_name_when_present(self):
        from src.core.ingest_pipeline import _derive_requestor_name
        result = _derive_requestor_name('"Valentina Demidenko" <valentina.demidenko@cdcr.ca.gov>')
        assert result == "Valentina Demidenko"

    def test_helper_handles_unquoted_display(self):
        from src.core.ingest_pipeline import _derive_requestor_name
        result = _derive_requestor_name("Keith Alsing <keith.alsing@calvet.ca.gov>")
        assert result == "Keith Alsing"

    def test_helper_returns_empty_for_garbage(self):
        from src.core.ingest_pipeline import _derive_requestor_name
        assert _derive_requestor_name("") == ""
        assert _derive_requestor_name("not-an-email") == ""

    def test_pdf_header_takes_priority_over_email_derived(self, temp_data_dir):
        """When the AMS 704 PDF Requestor field captured a real name,
        ingest must use it rather than the email-derived fallback."""
        from src.core.ingest_pipeline import _create_record
        from src.core.request_classifier import RequestClassification

        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={"requestor": "Real Name From PDF"},
            classification=RequestClassification(
                shape="pc_704_pdf_fillable",
                agency="cchcs",
                is_quote_only=True,
            ),
            primary_path=None,
            email_subject="t",
            email_sender="some.other.person@cdcr.ca.gov",
            email_uid="test-uid-priority",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        assert pc["requestor_name"] == "Real Name From PDF"

    def test_email_fallback_lands_when_pdf_blank(self, temp_data_dir):
        """When PDF header is blank (the Demidenko case), the email-
        derived display name lands on the persisted record."""
        from src.core.ingest_pipeline import _create_record
        from src.core.request_classifier import RequestClassification

        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=RequestClassification(
                shape="pc_704_pdf_fillable",
                agency="cchcs",
                is_quote_only=True,
            ),
            primary_path=None,
            email_subject="t",
            email_sender="valentina.demidenko@cdcr.ca.gov",
            email_uid="test-uid-fallback",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        assert pc["requestor_name"] == "Valentina Demidenko"
