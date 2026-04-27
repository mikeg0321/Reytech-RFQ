"""Phase 1.6 Enhancement C: CRITICAL_CONCEPTS alias resolution tests.

Chrome-verify on prod (pc_cd910ef2) revealed the panel was falsely
flagging `vendor.name` as missing on profiles that actually map
`vendor.business_name`. These tests pin the new alias-aware concept
satisfaction logic against real profile vocabulary.
"""

import pytest

from src.agents.fill_plan_builder import (
    CRITICAL_CONCEPTS,
    CRITICAL_CONCEPTS_PER_FORM,
    _profile_satisfies_concept,
)
from src.forms.profile_registry import FormProfile, FieldMapping


def _profile(form_type="703b", fields=None, signature_field="",
             raw_yaml=None):
    return FormProfile(
        id="t", form_type=form_type, blank_pdf="",
        fill_mode="acroform", fingerprint="fp",
        fields=list(fields or []),
        signature_field=signature_field,
        raw_yaml=raw_yaml or {},
    )


def _f(semantic, pdf="X"):
    return FieldMapping(semantic=semantic, pdf_field=pdf)


class TestVendorNameAlias:
    def test_vendor_business_name_satisfies(self):
        # Real 703b_reytech_standard uses vendor.business_name
        p = _profile(fields=[_f("vendor.business_name")])
        assert _profile_satisfies_concept(p, "vendor_name") is True

    def test_vendor_name_satisfies(self):
        # Some profiles use the bare vendor.name
        p = _profile(fields=[_f("vendor.name")])
        assert _profile_satisfies_concept(p, "vendor_name") is True

    def test_supplier_name_satisfies(self):
        p = _profile(fields=[_f("supplier.name")])
        assert _profile_satisfies_concept(p, "vendor_name") is True

    def test_no_vendor_name_alias_unsatisfied(self):
        p = _profile(fields=[_f("buyer.name"), _f("vendor.address")])
        assert _profile_satisfies_concept(p, "vendor_name") is False


class TestSignatureAlias:
    def test_signature_field_set_satisfies(self):
        p = _profile(signature_field="Sig1")
        assert _profile_satisfies_concept(p, "signature") is True

    def test_overlay_mode_signature_dict_satisfies(self):
        # Real 703b_reytech_standard has YAML: signature: {mode: overlay,
        # page: 1, field: ''} — overlay-mode draws sig without form field
        p = _profile(raw_yaml={"signature": {"mode": "overlay",
                                              "page": 1, "field": ""}})
        assert _profile_satisfies_concept(p, "signature") is True

    def test_signer_name_satisfies(self):
        p = _profile(fields=[_f("signer.printed_name")])
        assert _profile_satisfies_concept(p, "signature") is True

    def test_signer_printed_name_and_title_satisfies(self):
        p = _profile(fields=[_f("signer.printed_name_and_title")])
        assert _profile_satisfies_concept(p, "signature") is True

    def test_no_signature_at_all_unsatisfied(self):
        p = _profile(fields=[_f("vendor.business_name")])
        assert _profile_satisfies_concept(p, "signature") is False

    def test_empty_signature_dict_falls_through_to_aliases(self):
        # Empty dict shouldn't satisfy via raw_yaml.signature shortcut
        p = _profile(raw_yaml={"signature": {}})
        assert _profile_satisfies_concept(p, "signature") is False


class TestItemsUnitPriceAlias:
    def test_indexed_items_unit_price_satisfies(self):
        p = _profile(fields=[_f("items[0].unit_price")])
        assert _profile_satisfies_concept(p, "items_unit_price") is True

    def test_canonical_items_n_unit_price_satisfies(self):
        p = _profile(fields=[_f("items[n].unit_price")])
        assert _profile_satisfies_concept(p, "items_unit_price") is True

    def test_items_without_unit_price_fails(self):
        p = _profile(fields=[_f("items[0].description")])
        assert _profile_satisfies_concept(p, "items_unit_price") is False

    def test_no_items_at_all_fails(self):
        p = _profile(fields=[_f("vendor.business_name")])
        assert _profile_satisfies_concept(p, "items_unit_price") is False


class TestPerFormConceptLists:
    def test_703b_does_not_require_items(self):
        # 703B is header/cert form — items live on 704B
        assert "items_unit_price" not in CRITICAL_CONCEPTS_PER_FORM["703b"]
        assert "vendor_name" in CRITICAL_CONCEPTS_PER_FORM["703b"]
        assert "signature" in CRITICAL_CONCEPTS_PER_FORM["703b"]

    def test_704b_requires_items(self):
        assert "items_unit_price" in CRITICAL_CONCEPTS_PER_FORM["704b"]

    def test_std204_requires_fein(self):
        assert "vendor_fein" in CRITICAL_CONCEPTS_PER_FORM["std204"]


class TestRealProfileSatisfaction:
    """End-to-end: load real committed YAMLs, verify concept satisfaction."""

    def test_703b_reytech_standard_satisfies_all_critical_concepts(self):
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles() or {}
        p = profiles.get("703b_reytech_standard")
        if not p:
            pytest.skip("703b_reytech_standard not present in test env")
        for c in CRITICAL_CONCEPTS_PER_FORM["703b"]:
            assert _profile_satisfies_concept(p, c), \
                f"703b_reytech_standard fails concept: {c}"

    def test_static_attach_profile_skips_critical_checks(self):
        # sellers_permit_reytech is fill_mode=static_attach — the pre-printed
        # PDF is attached verbatim; no fields need mapping. _build_item must
        # short-circuit critical checks for static_attach.
        from src.agents.fill_plan_builder import build_fill_plan
        from src.forms.profile_registry import load_profiles
        from unittest.mock import patch
        import json

        profiles = load_profiles() or {}
        if "sellers_permit_reytech" not in profiles:
            pytest.skip("sellers_permit_reytech not present")

        quote = {
            "id": "PC-SP", "agency": "CCHCS",
            "institution": "CCHCS",
            "requirements_json": json.dumps(
                {"forms_required": ["sellers_permit"]}),
            "source_file": "",
        }
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value=profiles), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cchcs",
                                 {"name": "CCHCS", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[]):
            plan = build_fill_plan("PC-SP", "pc", quote_data=quote)
        item = next(it for it in plan.items if it.form_id == "sellers_permit")
        # static_attach should produce a clean status, not MISSING_FIELDS
        assert item.missing_critical == [], \
            f"static_attach mis-flagged: {item.missing_critical}"
        assert item.status in ("ready", "generic_fallback"), \
            f"unexpected status: {item.status}"
