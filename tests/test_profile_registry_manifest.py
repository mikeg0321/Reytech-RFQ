"""Drift tests for src/forms/profiles/registry.yml.

The manifest is a data snapshot of the on-disk profile set. CI enforces that
it stays in sync with the YAML sources; these tests catch the most common
drifts: missing entries, stale fingerprints, wrong field counts.

If a test fails here, run:
    python scripts/build_profile_registry.py
"""
from __future__ import annotations

import os
import sys

# Make repo root importable when tests are invoked from tests/ cwd.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.forms.profile_registry import (  # noqa: E402
    PROFILES_DIR,
    build_manifest_payload,
    load_manifest,
    load_profiles,
)


class TestManifestShape:
    def test_registry_yml_exists(self):
        assert os.path.exists(os.path.join(PROFILES_DIR, "registry.yml")), (
            "registry.yml is missing — run scripts/build_profile_registry.py"
        )

    def test_manifest_lists_every_profile(self):
        profiles = load_profiles()
        manifest = load_manifest()
        missing = sorted(set(profiles.keys()) - set(manifest.keys()))
        extra = sorted(set(manifest.keys()) - set(profiles.keys()))
        assert not missing and not extra, (
            f"manifest/profile drift — missing={missing}, extra={extra}. "
            "Regenerate: python scripts/build_profile_registry.py"
        )

    def test_entry_fields_are_present(self):
        manifest = load_manifest()
        required = {"id", "form_type", "fill_mode", "blank_pdf", "fingerprint", "field_count"}
        for pid, entry in manifest.items():
            missing = required - set(entry.keys())
            assert not missing, f"profile {pid} missing manifest keys: {missing}"


class TestManifestFreshness:
    def test_fingerprints_match_current_blank_pdfs(self):
        profiles = load_profiles()
        manifest = load_manifest()
        mismatches = []
        for pid, p in profiles.items():
            entry = manifest.get(pid)
            if not entry:
                continue
            if p.fingerprint != entry.get("fingerprint", ""):
                mismatches.append((pid, entry.get("fingerprint", "")[:12], p.fingerprint[:12]))
        assert not mismatches, (
            f"fingerprint drift — manifest vs computed: {mismatches}. "
            "Regenerate: python scripts/build_profile_registry.py"
        )

    def test_field_counts_match(self):
        profiles = load_profiles()
        manifest = load_manifest()
        mismatches = []
        for pid, p in profiles.items():
            entry = manifest.get(pid)
            if not entry:
                continue
            if len(p.fields) != entry.get("field_count"):
                mismatches.append((pid, entry.get("field_count"), len(p.fields)))
        assert not mismatches, (
            f"field_count drift — manifest vs computed: {mismatches}. "
            "Regenerate: python scripts/build_profile_registry.py"
        )

    def test_build_matches_on_disk_manifest(self):
        """Round-trip check: building the payload from current profiles
        produces the same entries (ids, fingerprints, counts) as registry.yml."""
        profiles = load_profiles()
        live = build_manifest_payload(profiles)["profiles"]
        on_disk = load_manifest()
        diffs = []
        for entry in live:
            pid = entry["id"]
            saved = on_disk.get(pid)
            if saved != entry:
                diffs.append(pid)
        assert not diffs, (
            f"manifest is stale for: {diffs}. "
            "Regenerate: python scripts/build_profile_registry.py"
        )


class TestFingerprintUniqueness:
    def test_no_two_profiles_share_a_fingerprint(self):
        """Two profiles with the same fingerprint break match_profile() —
        the first one wins nondeterministically. Blank-value profiles
        (generated, static_attach) are exempt."""
        manifest = load_manifest()
        seen: dict[str, str] = {}
        collisions = []
        for pid, entry in manifest.items():
            fp = entry.get("fingerprint", "") or ""
            if not fp:
                continue
            if fp in seen:
                collisions.append((seen[fp], pid, fp[:12]))
            else:
                seen[fp] = pid
        assert not collisions, f"fingerprint collisions: {collisions}"
