"""Profile `defaults:` block — YAML-declared fallback values for text fields.

Problem this addresses: profiles for forms like cchcs_it_rfq need the same
invariant Reytech identity on every quote (vendor name, DBA, SB/DVBE cert
number, CA reseller permit, OSDS ref, vendor ID, phone, contact name, etc.).
Requiring every Quote-upstream caller to stamp these on each instance was
leaky — any path that forgot to populate them (v2 orders, observer runs,
future fill callers) shipped a blank form.

Fix: a `defaults:` block in the profile YAML keyed by semantic name. The
fill engine merges those values as a FLOOR after Quote-derived accessors
run, so Quote values still win when present but the profile-level invariants
fill in where Quote is silent.

Scope of this PR: TEXT fields only. Checkbox defaults ("/Yes" values in
compliance.* semantics) need field_type metadata and a broader checkbox
path refactor — tracked as a follow-up.
"""
from __future__ import annotations

import io
import os
import tempfile

import pytest
import yaml
from pypdf import PdfReader

from src.core.quote_model import Quote, LineItem, QuoteHeader, BuyerInfo, Address
from src.forms.fill_engine import _build_static_field_map, fill
from src.forms.profile_registry import FormProfile, load_profile, load_profiles


class TestLoadDefaults:
    def test_defaults_extracted_from_yaml(self, tmp_path):
        yml = tmp_path / "x.yaml"
        yml.write_text(
            "id: test\n"
            "form_type: test\n"
            "blank_pdf: nonexistent.pdf\n"
            "fill_mode: acroform\n"
            "fields:\n"
            "  vendor.name: {pdf_field: 'Supplier'}\n"
            "defaults:\n"
            "  vendor.name: Reytech Inc.\n"
            "  cert.osds_ref: '2002605'\n",
            encoding="utf-8",
        )
        profile = load_profile(str(yml))
        assert profile.defaults == {
            "vendor.name": "Reytech Inc.",
            "cert.osds_ref": "2002605",
        }

    def test_missing_defaults_is_empty_dict(self, tmp_path):
        """Profiles without a defaults block must still load cleanly."""
        yml = tmp_path / "x.yaml"
        yml.write_text(
            "id: bare\n"
            "form_type: test\n"
            "blank_pdf: nonexistent.pdf\n"
            "fill_mode: acroform\n"
            "fields:\n"
            "  vendor.name: {pdf_field: 'Supplier'}\n",
            encoding="utf-8",
        )
        profile = load_profile(str(yml))
        assert profile.defaults == {}

    def test_defaults_coerce_values_to_strings(self, tmp_path):
        """YAML may parse numbers/booleans; fill engine writes text only."""
        yml = tmp_path / "x.yaml"
        yml.write_text(
            "id: t\n"
            "form_type: t\n"
            "blank_pdf: n.pdf\n"
            "fill_mode: acroform\n"
            "fields:\n"
            "  cert.number: {pdf_field: 'N'}\n"
            "defaults:\n"
            "  cert.number: 2002605\n",
            encoding="utf-8",
        )
        profile = load_profile(str(yml))
        assert profile.defaults["cert.number"] == "2002605"
        assert isinstance(profile.defaults["cert.number"], str)


class TestDefaultsMergeAsFloor:
    """Defaults fill where Quote is silent; Quote values still override."""

    def _profile_with(self, defaults: dict, *, with_quote_accessor: bool = False):
        """Build an in-memory profile with the given defaults. If
        with_quote_accessor=True, include vendor.name in fields so the
        Quote accessor path runs for it."""
        fields = {
            "cert.osds_ref": {"pdf_field": "OSDS_REF_PDF"},
            "cert.bidder_vendor_id": {"pdf_field": "BIDDER_ID_PDF"},
        }
        if with_quote_accessor:
            fields["vendor.name"] = {"pdf_field": "SUPPLIER_PDF"}

        from src.forms.profile_registry import FieldMapping
        fms = [
            FieldMapping(semantic=sem, pdf_field=spec["pdf_field"])
            for sem, spec in fields.items()
        ]
        return FormProfile(
            id="t", form_type="t", blank_pdf="", fill_mode="acroform",
            fields=fms, defaults=defaults,
        )

    def _quote(self, **header_kw) -> Quote:
        return Quote(
            doc_type="pc", doc_id="t",
            header=QuoteHeader(**header_kw),
        )

    def test_defaults_fill_when_quote_silent(self):
        profile = self._profile_with({
            "cert.osds_ref": "2002605",
            "cert.bidder_vendor_id": "47-4588061",
        })
        values = _build_static_field_map(self._quote(), profile)
        assert values["OSDS_REF_PDF"] == "2002605"
        assert values["BIDDER_ID_PDF"] == "47-4588061"

    def test_quote_value_wins_over_default(self):
        """A Quote that populates vendor.name must override any profile
        default for vendor.name — defaults are the floor, not a ceiling."""
        profile = self._profile_with(
            {"vendor.name": "Default Co."},
            with_quote_accessor=True,
        )
        quote = self._quote()
        quote.vendor.name = "Quote-Level Co."
        values = _build_static_field_map(quote, profile)
        assert values["SUPPLIER_PDF"] == "Quote-Level Co."

    def test_default_fills_when_quote_field_blank(self):
        """Empty Quote accessor falls through to default. Quote.vendor.name
        defaults to the Reytech identity (per canonical-identity memory), so
        we must explicitly blank it to prove the fallback path."""
        profile = self._profile_with(
            {"vendor.name": "Fallback Co."},
            with_quote_accessor=True,
        )
        quote = self._quote()
        quote.vendor.name = ""  # explicit blank → accessor is empty → default wins
        values = _build_static_field_map(quote, profile)
        assert values["SUPPLIER_PDF"] == "Fallback Co."

    def test_empty_defaults_no_op(self):
        """No defaults = same behavior as before this PR."""
        profile = self._profile_with({})
        values = _build_static_field_map(self._quote(), profile)
        # No OSDS or BIDDER fields set — Quote has no accessor for them.
        assert "OSDS_REF_PDF" not in values
        assert "BIDDER_ID_PDF" not in values

    def test_default_for_unmapped_semantic_is_ignored(self):
        """A defaults entry pointing at a semantic that has no FieldMapping
        is a YAML mistake, not a crash — silently skip."""
        profile = self._profile_with({"this.does.not.exist": "oops"})
        # Should not raise.
        values = _build_static_field_map(self._quote(), profile)
        assert "oops" not in values.values()

    def test_row_field_semantic_default_ignored(self):
        """Defaults for row-field semantics (containing [n]) must not leak
        into the static field map — row fields use a separate path."""
        from src.forms.profile_registry import FieldMapping
        profile = FormProfile(
            id="t", form_type="t", blank_pdf="", fill_mode="acroform",
            fields=[FieldMapping(semantic="items[n].qty", pdf_field="Qty{n}")],
            defaults={"items[n].qty": "should-not-appear"},
        )
        values = _build_static_field_map(self._quote(), profile)
        assert "should-not-appear" not in values.values()


class TestCchcsItRfqDefaultsApplied:
    """End-to-end: the real cchcs_it_rfq profile, which has 14 text defaults
    we expect to see in the rendered PDF output."""

    def test_cchcs_it_rfq_defaults_render_into_pdf(self):
        """Using the real profile + blank PDF fixture, confirm the key
        identity fields are actually written to the filled PDF's AcroForm
        values. This is the acceptance test for the whole PR."""
        profiles = load_profiles()
        profile = profiles.get("cchcs_it_rfq_reytech_standard")
        if profile is None:
            pytest.skip("cchcs_it_rfq profile not present in registry")
        if not profile.blank_pdf or not os.path.exists(profile.blank_pdf):
            pytest.skip(f"blank PDF not present: {profile.blank_pdf}")

        # A Quote with zero vendor / cert info — defaults must supply it.
        quote = Quote(
            doc_type="rfq", doc_id="test-cchcs-it",
            header=QuoteHeader(solicitation_number="PREQ10843276"),
        )
        pdf_bytes = fill(quote, profile)
        assert len(pdf_bytes) > 5000

        reader = PdfReader(io.BytesIO(pdf_bytes))
        fields = reader.get_fields() or {}

        # Key identity fields from the profile defaults — per
        # project_reytech_canonical_identity memory.
        supplier_name = str(fields.get("Supplier Name", {}).get("/V", ""))
        contact_name = str(fields.get("Contact Name", {}).get("/V", ""))
        phone = str(fields.get("Phone", {}).get("/V", ""))
        email = str(fields.get("Supplier Email", {}).get("/V", ""))
        osds_ref = str(fields.get("OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY", {}).get("/V", ""))

        assert "Reytech" in supplier_name, f"supplier name: {supplier_name!r}"
        assert "Michael Guadan" in contact_name, f"contact: {contact_name!r}"
        assert "949" in phone, f"phone: {phone!r}"
        assert "reytechinc.com" in email, f"email: {email!r}"
        assert "2002605" in osds_ref, f"osds: {osds_ref!r}"
