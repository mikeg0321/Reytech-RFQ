#!/usr/bin/env python3
"""Regenerate src/forms/profiles/registry.yml from the YAML profile sources.

registry.yml is the single authoritative manifest of registered form profiles.
It lists every profile by id with its form_type, fill_mode, blank_pdf, field
count, and the SHA-256 fingerprint of its blank PDF's sorted AcroForm field
names. Fingerprints are what routes match against uploaded buyer PDFs.

Usage:
    python scripts/build_profile_registry.py           # regenerate
    python scripts/build_profile_registry.py --check   # exit 3 if stale

CI runs with --check so any profile change that does not regenerate the
manifest is caught before merge.
"""
from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import yaml  # noqa: E402

from src.forms.profile_registry import (  # noqa: E402
    PROFILES_DIR,
    build_manifest_payload,
    load_profiles,
)

REGISTRY_PATH = os.path.join(PROFILES_DIR, "registry.yml")


def _render(payload: dict) -> str:
    header = (
        "# Profile registry manifest — regenerate with:\n"
        "#   python scripts/build_profile_registry.py\n"
        "# CI enforces freshness via --check.\n"
    )
    return header + yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate profile registry.yml.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 3 if registry.yml is out of date (no write).",
    )
    args = parser.parse_args()

    profiles = load_profiles(PROFILES_DIR)
    payload = build_manifest_payload(profiles)
    rendered = _render(payload)

    if args.check:
        existing = ""
        if os.path.exists(REGISTRY_PATH):
            with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
                existing = f.read()
        if existing.strip() != rendered.strip():
            print(
                "[stale] registry.yml is out of date. "
                "Regenerate: python scripts/build_profile_registry.py",
                file=sys.stderr,
            )
            return 3
        print(f"[ok] registry.yml is fresh ({len(payload['profiles'])} profiles)")
        return 0

    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        f.write(rendered)
    print(f"[ok] wrote {REGISTRY_PATH} ({len(payload['profiles'])} profiles)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
