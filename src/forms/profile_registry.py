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
    # Profile-level default values, keyed by semantic name. Used by the fill
    # engine as a floor when the Quote has no value for a field — e.g. the
    # Reytech canonical identity (vendor.name, cert.sb_dvbe_number, etc.) is
    # invariant across quotes and belongs on the profile, not on every Quote.
    # Quote-derived values still take precedence.
    defaults: dict = field(default_factory=dict)
    # Overflow config — what to do when item count exceeds total_row_capacity.
    # Structure: {"mode": "duplicate_page", "source_page": 1,
    #             "row_field_suffix_pattern": "_{page}"} (all optional).
    # Empty dict = no overflow declared; fill engine raises to prevent silent
    # item drop past the row-field limit.
    overflow: dict = field(default_factory=dict)
    raw_yaml: dict = field(default_factory=dict)

    @property
    def total_row_capacity(self) -> int:
        return sum(self.page_row_capacities)

    def effective_page_capacities(self, item_count: int) -> list[int]:
        """Compute the per-page row capacities needed to fit `item_count` items.

        Returns `page_row_capacities` unchanged when items fit. When items
        exceed the base capacity AND `overflow.mode == "duplicate_page"`, the
        list is extended with copies of the duplicable row page's capacity
        until it accommodates every item.

        `overflow.source_page` is interpreted first as a 1-indexed reference
        into `page_row_capacities`; if that index is out of range (common
        when source_page points at an absolute PDF page number rather than
        a row-page index — e.g., cchcs_it_rfq's row widgets live on PDF
        page 5 even though there's only one logical row-page), the last
        non-zero capacity is used as the duplicable row-page capacity.

        For any other overflow mode (or no overflow), returns the base list —
        callers should detect the shortfall and act (raise, truncate, log).
        """
        base = list(self.page_row_capacities)
        if not base or item_count <= sum(base):
            return base
        if (self.overflow or {}).get("mode") != "duplicate_page":
            return base
        src_page_1indexed = int(self.overflow.get("source_page", 1) or 1)
        src_idx = src_page_1indexed - 1
        if 0 <= src_idx < len(base):
            # In-range index is authoritative: an explicit 0 here means
            # "this page has no rows, don't extend" (profile bug signal).
            per_page = base[src_idx]
        else:
            # Out-of-range: source_page is a PDF-page reference that doesn't
            # index into page_row_capacities (common for single-row-page
            # profiles like cchcs_it_rfq). Fall back to last non-zero.
            non_zero = [c for c in base if c > 0]
            per_page = non_zero[-1] if non_zero else 0
        if per_page <= 0:
            return base
        while sum(base) < item_count:
            base.append(per_page)
        return base

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

    defaults_raw = raw.get("defaults", {}) or {}
    # Coerce every value to a string; the fill engine writes text-mode
    # AcroForm fields only for now (checkbox defaults are a follow-up).
    defaults = {str(k): str(v) for k, v in defaults_raw.items()}

    overflow_raw = raw.get("overflow", {}) or {}
    overflow = dict(overflow_raw) if isinstance(overflow_raw, dict) else {}

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
        defaults=defaults,
        overflow=overflow,
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
        if fname == "registry.yml":
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


def check_template_profile_matches(
    templates: dict[str, str],
    profiles: Optional[dict[str, FormProfile]] = None,
) -> dict[str, dict]:
    """Inspect each uploaded buyer template and report profile-match status.

    Args:
        templates: mapping of slot -> on-disk PDF path (e.g. {"703b": "/path/703b.pdf"}).
            Keys that are not form-template slots (e.g. "bidpkg") and paths
            that do not exist are still reported, with matched=False.
        profiles: preloaded registry. If None, loads from disk.

    Returns:
        {slot: {"path": str, "fingerprint": str, "matched": bool,
                "profile_id": str | None, "reason": str | None}}

        reason is set when matched=False and explains why
        (missing_file, unreadable_pdf, no_registered_profile).
    """
    if profiles is None:
        profiles = load_profiles()

    fingerprint_index = {
        p.fingerprint: p.id
        for p in profiles.values()
        if p.fingerprint
    }

    report: dict[str, dict] = {}
    for slot, path in templates.items():
        entry: dict = {
            "path": path,
            "fingerprint": "",
            "matched": False,
            "profile_id": None,
            "reason": None,
        }
        if not path or not os.path.exists(path):
            entry["reason"] = "missing_file"
            report[slot] = entry
            continue

        fp = _compute_fingerprint(path)
        entry["fingerprint"] = fp
        if not fp:
            entry["reason"] = "unreadable_pdf"
            report[slot] = entry
            continue

        pid = fingerprint_index.get(fp)
        if pid:
            entry["matched"] = True
            entry["profile_id"] = pid
        else:
            entry["reason"] = "no_registered_profile"
        report[slot] = entry

    return report


def build_manifest_payload(profiles: dict[str, FormProfile]) -> dict:
    """Serialize the in-memory profile set into the registry.yml schema.

    Output schema:
        version: 1
        profiles:
          - id: <profile_id>
            form_type: <form_type>
            fill_mode: <acroform|overlay|hybrid|generated|pass_through|static_attach>
            blank_pdf: <relative path or ''>
            fingerprint: <sha256 hex or ''>
            field_count: <int>
    """
    entries = []
    for pid in sorted(profiles.keys()):
        p = profiles[pid]
        entries.append({
            "id": p.id,
            "form_type": p.form_type,
            "fill_mode": p.fill_mode,
            "blank_pdf": p.blank_pdf or "",
            "fingerprint": p.fingerprint or "",
            "field_count": len(p.fields),
        })
    return {"version": 1, "profiles": entries}


def load_manifest(manifest_path: Optional[str] = None) -> dict[str, dict]:
    """Load registry.yml into a {profile_id: entry} dict.

    Returns empty dict if the manifest file is missing — callers should treat
    absence as 'no registered profiles' rather than raise.
    """
    if manifest_path is None:
        manifest_path = os.path.join(PROFILES_DIR, "registry.yml")
    if not os.path.exists(manifest_path):
        return {}
    with open(manifest_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    out: dict[str, dict] = {}
    for entry in raw.get("profiles", []) or []:
        pid = entry.get("id", "")
        if pid:
            out[pid] = entry
    return out


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
