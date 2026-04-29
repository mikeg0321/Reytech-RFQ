"""Regression guard: the duplicate-signature bug Mike flagged
2026-04-23 ("missing some signatures and in some places has double
signatures.") and recalled 2026-04-29 ("i want to make sure the app
doesnt duplicate signatures, was an issue before").

### The bug pattern

When a form template has a /Sig field whose name is in `SIGN_FIELDS`,
two signing code paths can fire:

1. `fill_and_sign_pdf` writes the signature image into the /Sig field's
   appearance stream (the AcroForm path).
2. `_703b_overlay_signature` ALSO draws a signature PNG via reportlab
   at the positional location of the printed "Bidder Signature" line.

If both run, the same page ends up with TWO visible signatures stacked
on the signature line. The fix lives at `reytech_filler_v4.py:737` —
the overlay path is gated on `not _has_sig_field`.

### What this test pins

These tests mock `_703b_overlay_signature` to count its invocations
when `fill_703b` is called against:

- A 703B-style template with NO /Sig field → overlay MUST run once.
- A template with a /Sig field whose name is in `SIGN_FIELDS` → overlay
  MUST NOT run (otherwise signatures stack).

If anyone weakens the guard (renames `_has_sig_field`, removes the
`if not _has_sig_field` check, or starts unconditionally calling the
overlay), one of these tests fails and CI blocks the regression.

See: `feedback_form_filling.md` Signature Placement Rules.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

_FIX = os.path.join(os.path.dirname(__file__), "fixtures")
_703B_NO_SIG = os.path.join(_FIX, "703b_blank.pdf")  # no /Sig fields
_DARFUR_HAS_SIG = os.path.join(_FIX, "darfur_blank.pdf")  # /Sig "Authorized Signature" in SIGN_FIELDS


def _rfq_data():
    return {
        "solicitation_number": "TEST10000000",
        "rfq_number": "TEST10000000",
        "agency_name": "CCHCS",
        "ship_to": "California Health Care Facility",
        "due_date": "01/15/2026",
        "sign_date": "01/01/2026",
        "delivery_days": "30",
        "delivery_location": "",
        "requestor_name": "",
        "requestor_email": "",
        "requestor_phone": "",
        "items": [],
    }


def _cfg():
    return {
        "company": {
            "name": "Reytech Inc.",
            "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
            "street": "30 Carnoustie Way",
            "city": "Trabuco Canyon",
            "state": "CA",
            "zip": "92679",
            "county": "Orange",
            "owner": "Michael Guadan",
            "title": "Owner",
            "phone": "949-229-1575",
            "email": "sales@reytechinc.com",
            "fein": "47-4588061",
            "sellers_permit": "245652416 - 00001",
            "cert_number": "2002605",
            "cert_expiration": "5/31/2027",
            "cert_type": "SB/DVBE",
            "description_of_goods": "Medical/Office and other supplies",
            "drug_free_expiration": "7/1/2028",
        },
    }


@pytest.fixture(autouse=True)
def _require_fixtures():
    """Skip the module if either fixture template is missing — these
    tests are guard-rail tests, not E2E generation tests."""
    for p in (_703B_NO_SIG, _DARFUR_HAS_SIG):
        if not os.path.exists(p):
            pytest.skip(f"fixture not found: {os.path.basename(p)}")


def _has_sig_field_named_in_allowlist(pdf_path):
    """Mirror of the production guard at reytech_filler_v4.py:723-731 —
    used in tests to verify the fixture itself satisfies the precondition
    we expect it to."""
    from pypdf import PdfReader

    from src.forms.reytech_filler_v4 import SIGN_FIELDS

    reader = PdfReader(pdf_path)
    for page in reader.pages:
        annots = page.get("/Annots") or []
        for ann in annots:
            try:
                obj = ann.get_object() if hasattr(ann, "get_object") else ann
            except Exception:
                continue
            if str(obj.get("/FT", "")) != "/Sig":
                continue
            if str(obj.get("/T", "")) in SIGN_FIELDS:
                return True
    return False


class TestFixturePreconditions:
    """Sanity-check the fixtures match this test's assumptions. If the
    fixtures themselves drift, the regression tests below would silently
    pass for the wrong reason."""

    def test_703b_blank_has_no_allowlisted_sig_field(self):
        assert not _has_sig_field_named_in_allowlist(_703B_NO_SIG), (
            "703b_blank.pdf gained a /Sig field whose name is in "
            "SIGN_FIELDS — the duplicate-sig regression test now uses the "
            "wrong fixture for the 'no /Sig' branch. Either update the "
            "test or use a different template."
        )

    def test_darfur_blank_has_allowlisted_sig_field(self):
        assert _has_sig_field_named_in_allowlist(_DARFUR_HAS_SIG), (
            "darfur_blank.pdf no longer has a /Sig field with a name "
            "in SIGN_FIELDS — the regression test now uses the wrong "
            "fixture for the 'has /Sig' branch."
        )


class TestNoDuplicateSignaturesGuard:
    """Pin the guard at `reytech_filler_v4.py:737` that prevents two
    signing paths from both firing on the same page. The guard reads:

        if not _has_sig_field:
            _703b_overlay_signature(output_path, sign_date)

    Mike's recalled regression (2026-04-29): "duplicate signatures got
    placed when reusing forms." The mechanism is when the guard fails
    open and the overlay runs alongside the AcroForm signing path.
    """

    def test_overlay_runs_when_template_has_no_sig_field(self, tmp_path):
        """703B Rev 03/2025 has no /Sig field — the positional overlay
        is the ONLY signing path, so it must run exactly once. If this
        test fails to 0, the overlay path is broken; if it fails to >1,
        the overlay is being called multiple times."""
        from src.forms.reytech_filler_v4 import fill_703b

        out = str(tmp_path / "out_703b.pdf")
        with patch(
            "src.forms.reytech_filler_v4._703b_overlay_signature"
        ) as mock_overlay:
            fill_703b(_703B_NO_SIG, _rfq_data(), _cfg(), out)

        assert mock_overlay.call_count == 1, (
            f"Expected _703b_overlay_signature to run exactly once for a "
            f"703B template with no /Sig field; got {mock_overlay.call_count}. "
            f"If 0, the overlay-path is broken (no signature renders). "
            f"If >1, the overlay is being invoked multiple times "
            f"(duplicate signatures will stack)."
        )

    def test_overlay_skipped_when_template_has_sig_field_in_allowlist(
        self, tmp_path
    ):
        """REGRESSION: when the template carries a /Sig field whose name
        is in `SIGN_FIELDS`, `fill_and_sign_pdf` will write the signature
        into the /Sig appearance stream. The positional overlay must
        then SKIP, otherwise we end up with two signatures on the same
        line — exactly the bug Mike flagged.

        We use darfur_blank.pdf (which has 'Authorized Signature' /Sig
        listed in SIGN_FIELDS) as a stand-in for any 703B-style template
        that grew a /Sig field. The guard logic at
        `reytech_filler_v4.py:723-737` doesn't care which form it is —
        it only checks whether ANY allowlisted /Sig field exists.
        """
        from src.forms.reytech_filler_v4 import fill_703b

        out = str(tmp_path / "out_with_sig.pdf")
        with patch(
            "src.forms.reytech_filler_v4._703b_overlay_signature"
        ) as mock_overlay:
            fill_703b(_DARFUR_HAS_SIG, _rfq_data(), _cfg(), out)

        assert mock_overlay.call_count == 0, (
            f"DUPLICATE-SIG REGRESSION RESURFACED. "
            f"_703b_overlay_signature was called {mock_overlay.call_count} "
            f"time(s) on a template that has a /Sig field in SIGN_FIELDS. "
            f"Per feedback_form_filling.md the overlay path MUST skip when "
            f"the AcroForm signing path will handle the signature, "
            f"otherwise both signatures render on the same line. See the "
            f"guard at reytech_filler_v4.py:737 — it is failing open."
        )


class TestSignFieldsAllowlistShape:
    """The duplicate-sig guard hinges on `SIGN_FIELDS` containing
    form-specific names so generic widgets don't accidentally trigger
    the AcroForm signing path. Pin the shape so a future "just add
    Signature5 to be safe" PR can't silently weaken the guard.

    Generic names that ARE in the allowlist
    (Signature1, Signature, Signature3, Signature4) are gated by the
    lower-40%-of-page check at reytech_filler_v4.py:511-514. Anything
    new added to the allowlist must either be form-specific (e.g.
    ends with `_<formid>`) or accept the page-position constraint.
    """

    KNOWN_GENERIC_ALLOWED = frozenset({
        "Signature1",   # 703B (most versions) + 704B + CalRecycle 74 — gated by lower-40%
        "Signature",    # CalRecycle 74 standalone + STD 1000 — gated by lower-40%
        "Signature3",   # STD 205 Payee Data Record Supplement
        "Signature4",   # STD 204 Payee Data Record
        "Bidder Signature",       # 703B Rev 03/2025 alternate name
        "703B_Bidder Signature",  # 703B prefixed variant
        "BidderSignature",        # 703B no-space variant
        "Authorized Signature",   # Darfur Act DGS PD 1
        "Signature29",  # GSPD-05-105 — hardcoded to skip at reytech_filler_v4.py:509
    })

    def test_no_unrecognized_generic_names_in_allowlist(self):
        """Catches "Signature2", "Signature5", "Sig", etc. being added
        without thought. Form-specific names are fine; the generic
        ones must be in the known list above."""
        from src.forms.reytech_filler_v4 import SIGN_FIELDS

        suspicious = []
        for name in SIGN_FIELDS:
            if name in self.KNOWN_GENERIC_ALLOWED:
                continue
            # Form-specific names contain an underscore, brackets, or
            # are obviously form-tied (e.g., end with a form id).
            looks_form_specific = (
                "_" in name
                or "[" in name
                or any(tag in name for tag in (
                    "PD843", "CUF", "darfur", "std21", "AMS", "OBS",
                    "DVBE", "DGS", "GenAI", "708",
                ))
            )
            if not looks_form_specific:
                suspicious.append(name)
        assert not suspicious, (
            f"SIGN_FIELDS gained unrecognized generic names: {suspicious}. "
            f"Generic-named /Sig fields are dangerous because the "
            f"duplicate-sig guard gives them only a lower-40% page-position "
            f"protection. If these are intentional, add them to "
            f"KNOWN_GENERIC_ALLOWED here AND verify they're either "
            f"form-specific in spirit or always at the bottom of the page."
        )
