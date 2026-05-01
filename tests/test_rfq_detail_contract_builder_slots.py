"""Regression guards for the agency-aware Contract Builder slot row.

Incident 2026-05-01: Mike opened a CalVet RFQ and saw the slot row
showing `703B / 703C / 704B / BidPkg`. CalVet RFQs never include those
forms — they ship the buyer's RFQ PDF and a few compliance forms
Reytech generates. The CCHCS-only slot list was hardcoded into
rfq_detail.html, so every agency saw the CCHCS layout regardless.

Fix: derive the slot list from `agency_key` at template-render time.
Each agency surfaces only the buyer-issued slots the form_classifier
can route into.

These tests lock that:
  - CCHCS still gets the original 5 slots (no regression)
  - DSH gets Email + AttA + AttB + AttC
  - CalVet variants + DGS + CalFire get only Email
  - The 'Drop files…' helper text matches the active agency
"""
from __future__ import annotations

import json
import os

import pytest


def _seed_rfq_with_agency(temp_data_dir, sample_rfq: dict, agency: str) -> str:
    """Write the sample_rfq fixture with the given agency; return its id.

    Uses the resolver-friendly agency strings — `match_agency` maps
    "CalVet" → calvet_key, "CDCR" → cchcs, etc. We seed BOTH `agency`
    and `institution` so resolution lands on the agency we want."""
    rfq = dict(sample_rfq)
    rfq["agency"] = agency
    rfq["institution"] = agency
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _fetch_detail(client, rid: str) -> str:
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200, f"/rfq/{rid} returned {resp.status_code}"
    return resp.get_data(as_text=True)


def _slot_strip(html: str) -> str:
    """Slice out just the slot-row block of the Contract Builder card so
    asserting "704B is absent" doesn't false-fire on the More dropdown's
    Replace 704B button or any 704B-related JS strings further down."""
    start = html.find("📎 Contract Builder")
    assert start != -1, "Contract Builder card missing from detail page"
    end = html.find("Drop files or click to upload", start)
    assert end != -1, "Contract Builder upload area missing"
    return html[start:end]


class TestCchcsKeepsFiveSlots:
    """No regression for the existing CCHCS workflow — all 5 slots stay."""

    def test_cchcs_shows_703b_703c_704b_bidpkg(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CDCR")
        slot = _slot_strip(_fetch_detail(client, rid))
        for label in ("Email", "703B", "703C", "704B", "BidPkg"):
            assert label in slot, f"CCHCS slot row missing {label!r}"


class TestCalVetHidesCchcsSlots:
    """CalVet doesn't ship 703B/704B/etc. The slot row must hide them."""

    @pytest.mark.parametrize("agency_input", ["CalVet", "VHC-Yountville", "CALVET.CA.GOV"])
    def test_calvet_no_703_or_704_in_slot_row(
        self, client, temp_data_dir, sample_rfq, agency_input
    ):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, agency_input)
        slot = _slot_strip(_fetch_detail(client, rid))
        for forbidden in ("703B", "703C", "704B", "BidPkg"):
            assert forbidden not in slot, (
                f"CalVet slot row should not surface {forbidden!r} "
                f"(agency_input={agency_input!r}); got: {slot}"
            )

    def test_calvet_keeps_email_slot(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CalVet")
        slot = _slot_strip(_fetch_detail(client, rid))
        assert "Email" in slot, "CalVet slot row must still surface the Email slot"


class TestDshGetsAttachmentSlots:
    """DSH ships AttA/B/C as buyer-issued PDFs; classifier already routes them."""

    def test_dsh_shows_attA_B_C(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "DSH-Atascadero")
        slot = _slot_strip(_fetch_detail(client, rid))
        assert "Email" in slot
        for label in ("AttA", "AttB", "AttC"):
            assert label in slot, f"DSH slot row missing {label!r}"
        for forbidden in ("703B", "704B", "BidPkg"):
            assert forbidden not in slot, (
                f"DSH slot row should not surface {forbidden!r}"
            )


class TestUploadHintMatchesAgency:
    """The 'Drop files...' helper text under the dropzone names the
    formats the agency actually ships, not the CCHCS list."""

    def test_calvet_hint_does_not_mention_703_or_704(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CalVet")
        html = _fetch_detail(client, rid)
        # Slice the hint area
        start = html.find("Drop files or click to upload")
        end = html.find("</div>", start + 50)
        hint = html[start:end + 6]
        for forbidden in ("703B", "704B", "bid package"):
            assert forbidden not in hint, (
                f"CalVet upload hint should not mention {forbidden!r}; got: {hint}"
            )

    def test_cchcs_hint_still_lists_704b(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CDCR")
        html = _fetch_detail(client, rid)
        start = html.find("Drop files or click to upload")
        end = html.find("</div>", start + 50)
        hint = html[start:end + 6]
        assert "704B" in hint, "CCHCS upload hint must still mention 704B"
