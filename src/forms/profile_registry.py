"""Form Profile Registry — content-based form detection + field mapping.

One registry for all form variants (PC and RFQ alike). Each buyer variant
is a YAML profile declaring field mappings. The fill engine reads the
profile — no hardcoded field names in Python.

Usage:
    from src.forms.profile_registry import load_profiles, match_profile, validate_profile

    # Load all profiles at boot
    profiles = load_profiles()

    # Match a PDF to its profile by field fingerprint
    profile = match_profile("path/to/uploaded.pdf", profiles)

    # Validate a profile against its blank PDF
    issues = validate_profile(profile)
"""
import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

log = logging.getLogger(__name__)

PROFILES_DIR = os.path.join(os.path.dirname(__file__), "profiles")


@dataclass
class FieldMapping:
    """Single field mapping from semantic name to PDF field name."""
    semantic: str       # e.g., "vendor.name" or "items[n].unit_price"
    pdf_field: str      # e.g., "COMPANY NAME" or "PRICE PER UNITRow{n}"
    field_type: str = "text"  # text | checkbox | signature
    default: str = ""


@dataclass
class FormProfile:
    """Complete form profile loaded from YAML."""
    id: str
    form_type: str
    blank_pdf: str
    fill_mode: str          # acroform | overlay | hybrid | html_to_pdf
    page_row_capacities: list[int] = field(default_factory=list)
    fields: list[FieldMapping] = field(default_factory=list)
    signature_mode: str = "image_stamp"
    signature_page: int = 1
    signature_field: str = ""
    fingerprint: str = ""   # SHA-256 of sorted field names from blank PDF
    raw_yaml: dict = field(default_factory=dict)

    @property
    def total_row_capacity(self) -> int:
        return sum(self.page_row_capacities)

    def get_field(self, semantic: str) -> Optional[FieldMapping]:
        """Look up a field mapping by semantic name."""
        for fm in self.fields:
            if fm.semantic == semantic:
                return fm
        return None

    def get_row_fields(self, row_num: int, page: int = 1) -> dict[str, str]:
        """Expand templated row fields for a specific row number.

        Returns dict of {semantic: resolved_pdf_field_name}.
        """
        suffix = "" if page == 1 else f"_{page}"
        result = {}
        for fm in self.fields:
            if "[n]" not in fm.semantic:
                continue
            sem = fm.semantic.replace("[n]", f"[{row_num}]")
            pdf = fm.pdf_field.replace("{n}", f"{row_num}{suffix}")
            result[sem] = pdf
        return result


def _parse_fields(fields_dict: dict) -> list[FieldMapping]:
    """Parse the fields section of a YAML profile."""
    mappings = []
    for semantic, spec in fields_dict.items():
        if isinstance(spec, str):
            mappings.append(FieldMapping(semantic=semantic, pdf_field=spec))
        elif isinstance(spec, dict):
            mappings.append(FieldMapping(
                semantic=semantic,
                pdf_field=spec.get("pdf_field", ""),
                field_type=spec.get("type", "text"),
                default=spec.get("default_if_blank", ""),
            ))
    return mappings


def _compute_fingerprint(pdf_path: str) -> str:
    """Compute SHA-256 of sorted AcroForm field names from a PDF."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        fields = reader.get_fields()
        if not fields:
            return ""
        names = sorted(fields.keys())
        return hashlib.sha256("\n".join(names).encode()).hexdigest()
    except Exception as e:
        log.warning("Fingerprint failed for %s: %s", pdf_path, e)
        return ""


def load_profile(yaml_path: str) -> FormProfile:
    """Load a single profile from a YAML file."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    fields = _parse_fields(raw.get("fields", {}))
    sig = raw.get("signature", {})

    profile = FormProfile(
        id=raw.get("id", ""),
        form_type=raw.get("form_type", ""),
        blank_pdf=raw.get("blank_pdf", ""),
        fill_mode=raw.get("fill_mode", "acroform"),
        page_row_capacities=raw.get("page_row_capacities", []),
        fields=fields,
        signature_mode=sig.get("mode", "image_stamp"),
        signature_page=sig.get("page", 1),
        signature_field=sig.get("field", ""),
        raw_yaml=raw,
    )

    # Compute fingerprint from blank PDF if it exists
    if profile.blank_pdf and os.path.exists(profile.blank_pdf):
        profile.fingerprint = _compute_fingerprint(profile.blank_pdf)

    return profile


def load_profiles(profiles_dir: str = PROFILES_DIR) -> dict[str, FormProfile]:
    """Load all YAML profiles from the profiles directory.

    Returns dict of {profile_id: FormProfile}.
    """
    profiles = {}
    if not os.path.isdir(profiles_dir):
        log.warning("Profiles directory not found: %s", profiles_dir)
        return profiles

    for fname in sorted(os.listdir(profiles_dir)):
        if not fname.endswith((".yaml", ".yml")):
            continue
        path = os.path.join(profiles_dir, fname)
        try:
            profile = load_profile(path)
            if profile.id:
                profiles[profile.id] = profile
                log.debug("Loaded profile: %s (%d fields, fingerprint=%s)",
                          profile.id, len(profile.fields), profile.fingerprint[:12] if profile.fingerprint else "none")
            else:
                log.warning("Profile %s has no id, skipping", fname)
        except Exception as e:
            log.error("Failed to load profile %s: %s", fname, e)

    return profiles


def match_profile(pdf_path: str, profiles: dict[str, FormProfile]) -> Optional[FormProfile]:
    """Match an uploaded PDF to a profile by field fingerprint.

    1. Compute fingerprint of the uploaded PDF
    2. Look for exact match in profiles
    3. If no match, return None (caller should fall back to Simple Submit)
    """
    fingerprint = _compute_fingerprint(pdf_path)
    if not fingerprint:
        return None

    for profile in profiles.values():
        if profile.fingerprint and profile.fingerprint == fingerprint:
            log.info("Matched %s to profile %s by fingerprint", pdf_path, profile.id)
            return profile

    log.info("No profile match for %s (fingerprint=%s)", pdf_path, fingerprint[:12])
    return None


def validate_profile(profile: FormProfile) -> list[str]:
    """Validate a profile against its blank PDF.

    Returns list of issues (empty = valid). This runs at boot time
    and in the pre-push hook — any issue = loud failure.
    """
    issues = []

    # generated mode synthesizes the PDF from scratch — no blank, no AcroForm.
    # Validate the generator spec instead and return.
    if profile.fill_mode == "generated":
        spec = (profile.raw_yaml or {}).get("generator", "")
        if not spec or ":" not in spec:
            issues.append(
                f"{profile.id}: fill_mode=generated requires a 'generator: module:function' entry"
            )
        return issues

    if not profile.blank_pdf:
        issues.append(f"{profile.id}: blank_pdf not specified")
        return issues

    if not os.path.exists(profile.blank_pdf):
        issues.append(f"{profile.id}: blank_pdf not found: {profile.blank_pdf}")
        return issues

    # pass_through and static_attach both emit the source PDF verbatim —
    # no field map to validate.
    if profile.fill_mode in ("pass_through", "static_attach"):
        return issues

    try:
        from pypdf import PdfReader
        reader = PdfReader(profile.blank_pdf)
        pdf_fields = reader.get_fields()
        if not pdf_fields:
            issues.append(f"{profile.id}: blank PDF has no AcroForm fields")
            return issues

        pdf_field_names = set(pdf_fields.keys())

        for fm in profile.fields:
            if "[n]" in fm.semantic:
                # Templated row field — check row 1 exists
                resolved = fm.pdf_field.replace("{n}", "1")
                if resolved not in pdf_field_names:
                    issues.append(f"{profile.id}: row field '{fm.semantic}' → '{resolved}' not found in PDF")
            else:
                if fm.pdf_field not in pdf_field_names:
                    issues.append(f"{profile.id}: field '{fm.semantic}' → '{fm.pdf_field}' not found in PDF")

    except Exception as e:
        issues.append(f"{profile.id}: validation error: {e}")

    return issues


def validate_all_profiles(profiles_dir: str = PROFILES_DIR) -> dict[str, list[str]]:
    """Validate all profiles. Returns {profile_id: [issues]}.

    Called at boot time. If any profile has issues, app should refuse traffic.
    """
    profiles = load_profiles(profiles_dir)
    results = {}
    for pid, profile in profiles.items():
        issues = validate_profile(profile)
        results[pid] = issues
        if issues:
            for issue in issues:
                log.error("PROFILE VALIDATION FAILED: %s", issue)
        else:
            log.info("Profile %s: validated OK (%d fields)", pid, len(profile.fields))
    return results
