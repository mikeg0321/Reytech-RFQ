"""Phase 1.6 PR3a: fill_plan_builder unit tests.

Asserts the email-contract → fill-plan join produces the right per-form
status (ready / generic_fallback / missing_critical / no_profile) given
synthetic FormProfile + Quote inputs.
"""

import json
from unittest.mock import patch

import pytest

from src.agents.fill_plan_builder import (
    build_fill_plan,
    FillPlan,
    FillPlanItem,
    FORM_DISPLAY_NAMES,
    STATUS_READY,
    STATUS_GENERIC_FALLBACK,
    STATUS_MISSING_FIELDS,
    STATUS_NO_PROFILE,
)
from src.forms.profile_registry import FormProfile, FieldMapping


def _profile(pid: str, form_type: str = "703b",
             agency_match: list = None,
             fields: list = None,
             signature_field: str = "Signature1") -> FormProfile:
    return FormProfile(
        id=pid,
        form_type=form_type,
        blank_pdf="",
        fill_mode="acroform",
        fingerprint="fp_" + pid,
        agency_match=list(agency_match or []),
        fields=list(fields or []),
        signature_field=signature_field,
    )


def _f(semantic: str, pdf_field: str = "X") -> FieldMapping:
    return FieldMapping(semantic=semantic, pdf_field=pdf_field)


def _quote(agency: str = "CDCR Folsom",
           requirements: dict = None,
           qid: str = "PC-1",
           qtype: str = "pc"):
    return {
        "id": qid,
        "agency": agency,
        "institution": agency,
        "requirements_json": json.dumps(requirements or {}),
        "source_file": "",
    }


class TestRequiredFormsMerge:
    def test_agency_baseline_only_when_no_contract(self):
        # CDCR config requires 703b + 704b + bidpkg + quote
        q = _quote(requirements={})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        ids = [it.form_id for it in plan.items]
        assert "703b" in ids and "704b" in ids
        # All marked agency_config-sourced
        for it in plan.items:
            assert "agency_config" in it.required_by

    def test_contract_adds_forms_not_in_agency(self):
        # CDCR baseline doesn't include obs_1600; contract requires it
        q = _quote(requirements={"forms_required": ["obs_1600"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        obs = [it for it in plan.items if it.form_id == "obs_1600"]
        assert obs
        assert "email_contract" in obs[0].required_by


class TestStatusResolution:
    def test_ready_when_buyer_specific_profile_covers_critical_fields(self):
        # 703b critical: vendor.name, signature, items[n].unit_price
        spec = _profile("703b_cdcr_folsom", "703b",
                        agency_match=["cdcr_folsom"],
                        fields=[_f("vendor.name"), _f("items[0].unit_price")])
        # Mock minimal agency to avoid pulling 4 forms
        q = _quote(agency="CDCR Folsom",
                   requirements={"forms_required": ["703b"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_cdcr_folsom": spec}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        items_703 = [it for it in plan.items if it.form_id == "703b"]
        assert items_703 and items_703[0].status == STATUS_READY
        assert items_703[0].profile_kind == "buyer_specific"
        assert items_703[0].matched_profile_id == "703b_cdcr_folsom"

    def test_generic_fallback_when_only_standard_exists(self):
        std = _profile("703b_std", "703b",
                       fields=[_f("vendor.name"), _f("items[0].unit_price")])
        q = _quote(requirements={"forms_required": ["703b"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        it = next(it for it in plan.items if it.form_id == "703b")
        assert it.status == STATUS_GENERIC_FALLBACK
        assert it.profile_kind == "generic"

    def test_missing_critical_when_profile_lacks_required_field(self):
        # Profile has no vendor.name mapping AND no signature_field
        std = _profile("703b_std", "703b",
                       fields=[_f("items[0].unit_price")],
                       signature_field="")
        q = _quote(requirements={"forms_required": ["703b"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("other",
                                 {"name": "Other", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        it = next(it for it in plan.items if it.form_id == "703b")
        assert it.status == STATUS_MISSING_FIELDS
        # Enhancement C: missing list now uses concept names not raw semantics
        assert "vendor_name" in it.missing_critical
        assert "signature" in it.missing_critical

    def test_no_profile_when_form_id_unregistered(self):
        q = _quote(requirements={"forms_required": ["bogus_form"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("other",
                                 {"name": "Other", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        it = next(it for it in plan.items if it.form_id == "bogus_form")
        assert it.status == STATUS_NO_PROFILE


class TestRollup:
    def test_totals_match_item_buckets(self):
        # Enhancement C: 703b critical = vendor_name + signature only.
        # 704b critical = vendor_name + signature + items_unit_price.
        # ready: 703b_cdcr_folsom buyer-specific with vendor.name + signature_field
        # generic: 704b_std with vendor.name + signature_field + items[0].unit_price
        # no_profile: bogus form
        ready = _profile("703b_cdcr_folsom", "703b",
                         agency_match=["cdcr_folsom"],
                         fields=[_f("vendor.name")])  # sig via signature_field default
        generic = _profile("704b_std", "704b",
                           fields=[_f("vendor.name"), _f("items[0].unit_price")])
        q = _quote(requirements={"forms_required": ["703b", "704b", "bogus"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_cdcr_folsom": ready, "704b_std": generic}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        assert plan.total_required == 3
        assert plan.total_ready == 1     # 703b_cdcr_folsom
        assert plan.total_warning == 1   # 704b_std generic_fallback
        assert plan.total_blocked == 1   # bogus → no_profile


class TestContractSourceLabel:
    def test_email_only_when_no_attachments(self):
        q = _quote(requirements={"forms_required": ["703b"], "due_date": "2026-05-01"})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("other",
                                 {"name": "Other", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[]):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        assert plan.contract_source == "email"

    def test_agency_only_when_no_contract_no_attachments(self):
        q = _quote(requirements={})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("other",
                                 {"name": "Other", "required_forms": ["703b"]})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[]):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        assert plan.contract_source == "agency_only"


class TestQuoteNotFound:
    def test_returns_empty_plan_gracefully(self):
        with patch("src.agents.fill_plan_builder._load_quote",
                   return_value=None):
            plan = build_fill_plan("ghost-id", "pc")
        assert plan.total_required == 0
        assert plan.items == []


class TestFormDisplayNames:
    def test_known_forms_have_friendly_names(self):
        # Sanity that the name table covers all the common ones
        for fid in ("703b", "704b", "dvbe843", "darfur_act", "calrecycle74",
                    "std204", "obs_1600", "quote"):
            assert fid in FORM_DISPLAY_NAMES
            assert FORM_DISPLAY_NAMES[fid] != fid


class TestEndpointSerialization:
    def test_plan_to_dict_is_jsonable(self):
        std = _profile("703b_std", "703b",
                       fields=[_f("vendor.name"), _f("items[0].unit_price")])
        q = _quote(requirements={"forms_required": ["703b"]})
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("other",
                                 {"name": "Other", "required_forms": []})):
            plan = build_fill_plan("PC-1", "pc", quote_data=q)
        # Must round-trip through json
        s = json.dumps(plan.to_dict())
        roundtrip = json.loads(s)
        assert roundtrip["total_required"] == 1
        assert roundtrip["items"][0]["form_id"] == "703b"
