"""Regression guards for form_classifier._fingerprint_pdf after it moved
from hardcoded prefix checks to data-driven `classifier_hints` in profile
YAMLs (PR-4 of the declarative fillers track).

Covers:
  - Prefix-based routing (703A_, 703B_)
  - Substring-based routing (704B — no prefix, matches on field name)
  - Priority ordering — higher priority wins first
  - Fallback to hardcoded 703C check (no profile exists for 703c)
  - No match returns None
  - All profile YAMLs with classifier_hints declare a slot in TEMPLATE_SLOTS
"""
from __future__ import annotations

import pytest
from unittest.mock import patch

from src.forms import form_classifier


class _FakeReader:
    """Stand-in for pypdf.PdfReader — returns the field dict we seed."""
    def __init__(self, fields: dict):
        self._fields = fields

    def get_fields(self):
        return self._fields


def _patch_reader(fields: dict):
    """Patch pypdf.PdfReader so _fingerprint_pdf sees our synthetic fields
    without needing to build a real PDF byte stream."""
    return patch("pypdf.PdfReader", return_value=_FakeReader(fields))


class TestHintsDrivenRouting:
    def test_prefix_match_703a(self):
        fields = {"703A_Name": None, "703A_Phone": None}
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "703a"

    def test_prefix_match_703b(self):
        fields = {"703B_Business Name": None, "703B_Email": None}
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "703b"

    def test_contains_match_704b_primary(self):
        # Primary 704B marker: "ITEM DESCRIPTION PRODUCT SPECIFICATIONRow"
        fields = {
            "Row1": None,
            "ITEM DESCRIPTION PRODUCT SPECIFICATIONRow1": None,
            "Contract_Number": None,
        }
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "704b"

    def test_contains_match_704b_backup(self):
        # Backup 704B marker: "COMPANY REPRESENTATIVE"
        fields = {
            "COMPANY REPRESENTATIVE print name": None,
            "Date of Request": None,
        }
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "704b"

    def test_no_match_returns_none(self):
        fields = {"Some_Unrelated_Field": None, "Another_Field": None}
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") is None

    def test_empty_pdf_returns_none(self):
        with _patch_reader({}):
            assert form_classifier._fingerprint_pdf(b"x") is None


class TestHardcodedFallback:
    def test_703c_still_routes_without_profile(self):
        """703c has no profile YAML yet — the hardcoded fallback must catch
        it. If someone adds a 703c profile later and removes the fallback,
        the prefix match still works via classifier_hints."""
        fields = {"703C_Name": None, "703C_Phone": None}
        with _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "703c"


class TestHintCollection:
    def test_hints_load_from_profiles(self):
        hints = form_classifier._collect_classifier_hints()
        slots = {h["slot"] for h in hints}
        assert "703a" in slots
        assert "703b" in slots
        assert "704b" in slots

    def test_hints_sorted_by_priority_desc(self):
        hints = form_classifier._collect_classifier_hints()
        priorities = [h["priority"] for h in hints]
        assert priorities == sorted(priorities, reverse=True)

    def test_all_hint_slots_are_known(self):
        """Every slot declared in a profile YAML must be a recognized
        template slot — otherwise downstream routing breaks."""
        hints = form_classifier._collect_classifier_hints()
        for h in hints:
            assert h["slot"] in form_classifier.TEMPLATE_SLOTS, (
                f"Profile {h['profile_id']} declares unknown slot {h['slot']}"
            )

    def test_higher_priority_wins_first(self):
        """Inject a higher-priority 704b-style hint that claims a 703A
        prefix; it must beat the stock 703A hint."""
        fake_hints = [
            {
                "profile_id": "fake_high",
                "slot": "704b",
                "priority": 999,
                "field_prefixes": ["703A_"],
                "field_contains": [],
            },
            {
                "profile_id": "703a_reytech_standard",
                "slot": "703a",
                "priority": 100,
                "field_prefixes": ["703A_"],
                "field_contains": [],
            },
        ]
        fields = {"703A_Name": None}
        with patch.object(
            form_classifier, "_collect_classifier_hints", return_value=fake_hints
        ), _patch_reader(fields):
            assert form_classifier._fingerprint_pdf(b"x") == "704b"
