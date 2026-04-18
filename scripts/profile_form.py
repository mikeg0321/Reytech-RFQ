#!/usr/bin/env python3
"""CLI: profile a blank PDF into a FormProfile YAML draft.

Usage:
    python scripts/profile_form.py \
        --blank tests/fixtures/calvet_rfq_briefs_blank.pdf \
        --form-id calvet_rfq_briefs \
        --out src/forms/profiles/calvet_rfq_briefs_reytech_draft.yaml

After it writes the YAML, it runs `validate_profile` and reports remaining
issues. Zero issues = the profile is wired and can be loaded at boot.
Non-zero issues = the operator edits the draft and re-validates.

The script never overwrites an existing YAML — use `--force` if you mean it.
"""
from __future__ import annotations

import argparse
import os
import sys

# Repo root → sys.path
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.agents.form_profiler import profile_blank_pdf  # noqa: E402
from src.forms.profile_registry import load_profile, validate_profile  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a FormProfile YAML draft from a blank PDF.")
    p.add_argument("--blank", required=True, help="Path to the blank government form PDF.")
    p.add_argument("--form-id", required=True, help="agency_config form id (e.g., calvet_rfq_briefs).")
    p.add_argument("--profile-id", default="", help="Override profile id; default is <form-id>_reytech_draft.")
    p.add_argument("--out", required=True, help="Output YAML path.")
    p.add_argument("--force", action="store_true", help="Overwrite if output already exists.")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    if os.path.exists(args.out) and not args.force:
        print(f"[abort] {args.out} already exists. Pass --force to overwrite.", file=sys.stderr)
        return 2

    result = profile_blank_pdf(
        args.blank,
        form_id=args.form_id,
        profile_id=args.profile_id,
    )

    if not result.yaml_text:
        print("[error] profiler produced no output:", file=sys.stderr)
        for issue in result.issues:
            print(f"  - {issue}", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(result.yaml_text)

    print(f"[ok] wrote {args.out}")
    print(f"     mapped: {len(result.fields_mapped)} semantics, "
          f"unknown: {len(result.fields_unknown)} fields, "
          f"page_row_capacities: {result.page_row_capacities}")

    if result.issues:
        print("[profiler issues]")
        for i in result.issues:
            print(f"  - {i}")

    # Validate what we wrote against the blank PDF.
    try:
        profile = load_profile(args.out)
        vissues = validate_profile(profile)
    except Exception as e:
        print(f"[error] profile reload/validate failed: {e}", file=sys.stderr)
        return 1

    if vissues:
        print(f"[validate] {len(vissues)} issue(s) — fix before shipping:")
        for i in vissues:
            print(f"  - {i}")
        return 3

    print("[validate] clean — profile is wired.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
