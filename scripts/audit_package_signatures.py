"""CLI for package-signature audit vs. the north-star fixture.

Usage:
    python scripts/audit_package_signatures.py <package.pdf>
    python scripts/audit_package_signatures.py <package.pdf> --north-star path/to/other.pdf

Output: page-by-page table + a summary of missing/extra sig pages.

Use when:
  - Operator says "a sig is in the wrong spot" — run this against
    the generated package; compare to north-star pages
  - Before patching any sig-placement heuristic in
    `reytech_filler_v4.py` or `quote_generator.py` — confirm what's
    actually different first (per the audit-BB 3-strikes guardrail)
"""
from __future__ import annotations

import argparse
import os
import sys

# When run as a standalone script the package root isn't on sys.path.
# Insert it so `from src.core.package_signatures import ...` resolves.
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _default_north_star() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(
        here, "..", "tests", "fixtures", "rfq_packages",
        "10840486_rfq_package_NORTHSTAR.pdf",
    ))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Audit sigs in a bid-package PDF vs. north star."
    )
    ap.add_argument("package", help="path to the generated package PDF")
    ap.add_argument(
        "--north-star", default=_default_north_star(),
        help="path to the canonical north-star PDF (default: "
             "tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf)",
    )
    ap.add_argument(
        "--verbose", "-v", action="store_true",
        help="dump the per-page audit dicts (ns + gen) for mismatches",
    )
    args = ap.parse_args()

    if not os.path.exists(args.package):
        print(f"ERROR: package not found: {args.package}", file=sys.stderr)
        return 2
    if not os.path.exists(args.north_star):
        print(
            f"ERROR: north star not found: {args.north_star}",
            file=sys.stderr,
        )
        return 2

    from src.core.package_signatures import compare_to_northstar
    diff = compare_to_northstar(args.package, args.north_star)

    print(f"Pages: generated={diff['page_count_gen']} "
          f"north_star={diff['page_count_ns']}")
    print()
    print(f"{'PAGE':<6} {'EXPECTED':<10} {'ACTUAL':<10} {'MATCH':<8} DETAIL")
    print("-" * 80)
    for entry in diff["per_page"]:
        p = entry["page"]
        exp = "sig" if entry["expected_sig"] else "-"
        act = "sig" if entry["actual_sig"] else "-"
        # ASCII markers — Windows cp1252 can't print check/cross glyphs
        match = "OK" if entry["match"] else "FAIL"
        ns = entry["ns_counts"]
        gen = entry["gen_counts"]
        if entry["match"]:
            detail = (
                f"af={ns.get('acroform_sigs',0)} "
                f"w={ns.get('widget_sigs',0)} "
                f"img={ns.get('image_xobjects',0)}"
            )
        else:
            detail = (
                f"NS[af={ns.get('acroform_sigs',0)} "
                f"w={ns.get('widget_sigs',0)} "
                f"img={ns.get('image_xobjects',0)}] "
                f"GEN[af={gen.get('acroform_sigs',0)} "
                f"w={gen.get('widget_sigs',0)} "
                f"img={gen.get('image_xobjects',0)}]"
            )
        print(f"{p:<6} {exp:<10} {act:<10} {match:<8} {detail}")

    print()
    if diff["matches"]:
        print("RESULT: package matches north star signature-wise.")
        return 0
    print(f"RESULT: MISMATCH")
    if diff["missing_on"]:
        print(f"  missing signatures on pages: {diff['missing_on']}")
    if diff["extra_on"]:
        print(f"  extra signatures on pages:   {diff['extra_on']}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
