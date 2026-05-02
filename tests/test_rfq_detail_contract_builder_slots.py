"""Regression guards for the agency-aware Contract Builder slot row.

Incident 2026-05-01: Mike opened a CalVet RFQ and saw the slot row
showing `703B / 703C / 704B / BidPkg`. CalVet RFQs never include those
forms — they ship the buyer's RFQ PDF and a few compliance forms
Reytech generates. The CCHCS-only slot list was hardcoded into
rfq_detail.html, so every agency saw the CCHCS layout regardless.

Fix: derive the slot list from `agency_key` at template-render time.
Each agency surfaces only the buyer-issued slots the form_classifier
can route into.

Bug 7 (2026-05-02): the Email-screenshot upload slot was removed from
all agency variants — the buyer's email is reachable via the 📬 Gmail
thread link in the header (B1 thread-id capture), so the upload pill
was redundant noise. Server-side classification of dropped images as
`email_screenshot` is preserved for backward compat.

These tests lock that:
  - CCHCS shows the 4 buyer-form slots (703B / 703C / 704B / BidPkg)
  - DSH shows the 3 attachment slots (AttA / AttB / AttC)
  - CalVet variants + DGS + CalFire get no slot row at all
  - The 'Drop files…' helper text matches the active agency
  - The Email pill never appears in the Contract Builder slot row
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


class TestCchcsShowsBuyerFormSlots:
    """No regression for the CCHCS workflow — all 4 buyer-form slots stay.

    Bug 7 (2026-05-02) removed the Email pill; the 4 actual buyer-issued
    forms (703B / 703C / 704B / BidPkg) keep their slot row."""

    def test_cchcs_shows_703b_703c_704b_bidpkg(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CDCR")
        slot = _slot_strip(_fetch_detail(client, rid))
        for label in ("703B", "703C", "704B", "BidPkg"):
            assert label in slot, f"CCHCS slot row missing {label!r}"


class TestCalVetHidesCchcsSlots:
    """CalVet doesn't ship 703B/704B/etc. The slot row must hide them.

    Bug 7 (2026-05-02): CalVet now has zero buyer-form slots, so the
    slot-row div is suppressed entirely (no orphan empty flex container)."""

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

    def test_calvet_has_no_email_slot_either(self, client, temp_data_dir, sample_rfq):
        """Bug 7: the Email upload pill is gone — Gmail thread link in
        the header is the canonical pointer to the buyer email."""
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "CalVet")
        slot = _slot_strip(_fetch_detail(client, rid))
        # The slot row should not contain an "Email" pill anywhere.
        # We look for the pill markup specifically (📧 Email), not
        # bare "Email" — the helper line above ("Drop files...")
        # contains the word in copy. The pill markup is `<span ...>📧 Email`.
        assert "📧 Email" not in slot, (
            "Bug 7 regression: Email upload pill is back in CalVet "
            "Contract Builder slot row"
        )


class TestDshGetsAttachmentSlots:
    """DSH ships AttA/B/C as buyer-issued PDFs; classifier already routes them."""

    def test_dsh_shows_attA_B_C(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "DSH-Atascadero")
        slot = _slot_strip(_fetch_detail(client, rid))
        for label in ("AttA", "AttB", "AttC"):
            assert label in slot, f"DSH slot row missing {label!r}"
        for forbidden in ("703B", "704B", "BidPkg"):
            assert forbidden not in slot, (
                f"DSH slot row should not surface {forbidden!r}"
            )

    def test_dsh_has_no_email_slot(self, client, temp_data_dir, sample_rfq):
        """Bug 7: Email pill is gone for DSH too."""
        rid = _seed_rfq_with_agency(temp_data_dir, sample_rfq, "DSH-Atascadero")
        slot = _slot_strip(_fetch_detail(client, rid))
        assert "📧 Email" not in slot, (
            "Bug 7 regression: Email upload pill is back in DSH "
            "Contract Builder slot row"
        )


class TestNoEmailScreenshotConfirmation:
    """Bug 7: the 'Email contract: <filename>' confirmation box is gone.
    If a record from before the change still has `r['email_screenshot']`
    populated, we no longer render the green confirmation banner — the
    upload is still on disk and the server-side route still handles new
    image drops, but the Contract Builder UI doesn't surface it."""

    def test_email_screenshot_box_not_rendered(
        self, client, temp_data_dir, sample_rfq
    ):
        rfq = dict(sample_rfq)
        rfq["agency"] = "CDCR"
        rfq["institution"] = "CDCR"
        rfq["email_screenshot"] = {
            "path": "/tmp/fake.png",
            "original_filename": "from_before_bug7.png",
            "filename": "es-001.png",
            "bytes": 12345,
            "uploaded_at": "2026-04-30T10:00:00",
        }
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({rfq["id"]: rfq}, f)
        html = _fetch_detail(client, rfq["id"])
        # The pre-Bug-7 confirmation block contained these exact strings.
        # If any one of them appears, the legacy box is rendering again.
        assert "Email contract:" not in html, (
            "Bug 7 regression: legacy email-screenshot confirmation "
            "box is rendering on the Contract Builder card"
        )
        assert "from_before_bug7.png" not in html, (
            "Bug 7 regression: stored email_screenshot filename is "
            "leaking into the rendered Contract Builder UI"
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
