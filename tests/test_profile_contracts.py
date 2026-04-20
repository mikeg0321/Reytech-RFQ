"""Contract tests per form profile.

These are the per-profile golden-path contracts — every profile must honor
them before it can ship. Failures here mean the profile YAML drifted from
the declared semantic contract, or a new form was added without meeting
the minimum schema.

Covered contracts:
  1. No duplicate semantic names inside a single profile.
  2. page_row_capacities (when declared) is consistent with row templates.
  3. Every form_type family provides its minimum required semantics.
  4. Declared signature field (if mode stamps into an AcroForm field)
     actually exists in the blank PDF.
  5. Row-templated pdf_field strings contain the {n} placeholder.

These tests are cheap and static — no filled PDF is generated. For a live
fill → read-back contract, see tests/test_readback.py.
"""
from __future__ import annotations

import os
import sys

import pytest
import yaml

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.forms.profile_registry import (  # noqa: E402
    PROFILES_DIR,
    load_profile,
    load_profiles,
)


# Minimum semantic coverage per form_type. Missing any entry here = bug
# waiting to happen, because fill_engine silently drops unmapped semantics.
# Only the form_types whose fill_mode is acroform (field-level fill) need
# coverage. generated/static_attach/pass_through emit PDFs without field fills.
REQUIRED_SEMANTICS_BY_FORM_TYPE: dict[str, set[str]] = {
    "703a": {"vendor.business_name", "vendor.signature_date"},
    "703b": {"vendor.business_name", "vendor.signature_date"},
    "704a": {
        "items[n].description",
        "items[n].qty",
        "items[n].unit_price",
    },
    "704b": {
        "items[n].description",
        "items[n].qty",
        "items[n].unit_price",
    },
}


def _all_profile_ids() -> list[str]:
    return sorted(load_profiles().keys())


def _all_profile_paths() -> list[str]:
    return sorted(
        os.path.join(PROFILES_DIR, n)
        for n in os.listdir(PROFILES_DIR)
        if n.endswith((".yaml", ".yml")) and n != "registry.yml"
    )


@pytest.mark.parametrize("profile_id", _all_profile_ids())
def test_semantic_names_unique_per_profile(profile_id: str) -> None:
    """Each semantic key must appear at most once per profile.

    YAML dicts silently collapse duplicate keys — the loader keeps the last
    value. This test parses the raw file to detect duplicates that would be
    hidden by yaml.safe_load.
    """
    # Find the YAML path for this profile id by scanning.
    target_path = None
    for p in _all_profile_paths():
        with open(p, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if raw and raw.get("id") == profile_id:
            target_path = p
            break
    assert target_path, f"no YAML file declares id={profile_id}"

    # Walk the fields map text to count duplicate semantic keys.
    with open(target_path, "r", encoding="utf-8") as f:
        text = f.read()
    seen: dict[str, int] = {}
    in_fields = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("fields:"):
            in_fields = True
            continue
        if in_fields and stripped and not stripped.startswith("#"):
            if not line.startswith(" "):
                in_fields = False
                continue
            if ":" in stripped and not stripped.startswith("-"):
                indent = len(line) - len(line.lstrip())
                if indent == 2:
                    key = stripped.split(":", 1)[0].strip()
                    seen[key] = seen.get(key, 0) + 1
    dupes = {k: n for k, n in seen.items() if n > 1}
    assert not dupes, f"{profile_id}: duplicate semantic keys: {dupes}"


@pytest.mark.parametrize("profile_id", _all_profile_ids())
def test_page_capacities_declared_when_row_templates_exist(profile_id: str) -> None:
    """If a profile has items[n].* row fields, page_row_capacities must be set."""
    profile = load_profiles()[profile_id]
    has_row_templates = any("[n]" in fm.semantic for fm in profile.fields)
    if has_row_templates:
        assert profile.page_row_capacities, (
            f"{profile_id}: has row-templated fields but page_row_capacities is empty"
        )
        assert all(c > 0 for c in profile.page_row_capacities), (
            f"{profile_id}: page_row_capacities contains non-positive values: "
            f"{profile.page_row_capacities}"
        )


@pytest.mark.parametrize("profile_id", _all_profile_ids())
def test_row_templates_contain_placeholder(profile_id: str) -> None:
    """Every row-semantic pdf_field must contain {n} so the fill engine can
    substitute the row number. A missing placeholder means row 2+ writes
    land on top of row 1."""
    profile = load_profiles()[profile_id]
    missing = []
    for fm in profile.fields:
        if "[n]" in fm.semantic and "{n}" not in fm.pdf_field:
            missing.append((fm.semantic, fm.pdf_field))
    assert not missing, (
        f"{profile_id}: row-semantic(s) have pdf_field without {{n}} placeholder: {missing}"
    )


@pytest.mark.parametrize(
    "form_type,required",
    sorted(REQUIRED_SEMANTICS_BY_FORM_TYPE.items()),
)
def test_required_semantics_by_form_type(form_type: str, required: set[str]) -> None:
    """Every acroform profile of a given form_type must expose the minimum
    required semantics. This is the contract that fill_engine relies on —
    fill code written against the form_type can assume these are mappable."""
    profiles = [
        p for p in load_profiles().values()
        if p.form_type == form_type and p.fill_mode == "acroform"
    ]
    assert profiles, f"no acroform profile found for form_type={form_type}"
    for p in profiles:
        declared = {fm.semantic for fm in p.fields}
        missing = required - declared
        assert not missing, (
            f"{p.id} ({form_type}) missing required semantics: {sorted(missing)}"
        )


@pytest.mark.parametrize("profile_id", _all_profile_ids())
def test_signature_field_exists_in_blank_when_acroform_stamp(profile_id: str) -> None:
    """If a profile stamps the signature into a named AcroForm field, the
    field must exist in the blank PDF. Overlay-mode signatures (positional
    draws with empty field) are exempt."""
    profile = load_profiles()[profile_id]
    # Profiles that don't use an AcroForm field for signatures are exempt.
    if profile.fill_mode != "acroform":
        pytest.skip(f"{profile_id}: fill_mode={profile.fill_mode}")
    if not profile.signature_field:
        return  # legitimate: overlay or positional signature
    if not profile.blank_pdf or not os.path.exists(profile.blank_pdf):
        pytest.skip(f"{profile_id}: blank PDF unavailable")

    from pypdf import PdfReader
    reader = PdfReader(profile.blank_pdf)
    field_names = set((reader.get_fields() or {}).keys())
    assert profile.signature_field in field_names, (
        f"{profile_id}: signature.field='{profile.signature_field}' not found in blank PDF"
    )


def test_every_profile_id_matches_file_stem() -> None:
    """Profile id == YAML filename stem. Catches rename drift."""
    drifts = []
    for path in _all_profile_paths():
        profile = load_profile(path)
        stem = os.path.splitext(os.path.basename(path))[0]
        if profile.id != stem:
            drifts.append((path, profile.id, stem))
    assert not drifts, f"profile id / filename drift: {drifts}"
