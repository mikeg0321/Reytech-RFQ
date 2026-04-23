"""One-shot finalize script for RFQ 10840486 (CSP-Sacramento LPA IT).

Written 2026-04-22 to unblock submission while the three audit-logged
systemic bugs stay deferred:
  - W: institution resolver canonicalizes to wrong facility (FSP vs CSP-SAC)
  - X: tax-lookup parser fails on facility-led / zip-less addresses
  - M: fill engine not wired for form_type=cchcs_it_rfq (703B filler can't
       name-match LPA fields)

What this script does (in order):
  1. Loads RFQ 9ad8a0ac.
  2. Overrides ship-to / tax fields on the record with canonical CSP-SAC
     values so the quote PDF renders correctly AND the saved record is
     self-consistent (same rate everywhere).
  3. Saves the record.
  4. Calls the internal quote generator directly to produce a corrected
     Quote PDF — bypassing the resolver overwrite path that mis-maps
     "Folsom" to FSP.
  5. Prints the output paths + download URLs.

What it does NOT do:
  - Touch the 703B/LPA fill. That still requires hand-fill in Acrobat
    (Attachment 2 per Mike's plan).
  - Mutate other records. Scoped to rid=9ad8a0ac only.

Usage:
  # Dry-run — print what it would change, no writes:
  railway ssh /opt/venv/bin/python scripts/finalize_rfq_10840486.py

  # Actually apply:
  railway ssh /opt/venv/bin/python scripts/finalize_rfq_10840486.py --apply

  # Regenerate quote PDF only (after --apply has been run once):
  railway ssh /opt/venv/bin/python scripts/finalize_rfq_10840486.py --apply --regen-only
"""
import argparse
import json
import os
import sys
from datetime import datetime

# Make the project importable when run via `railway ssh /opt/venv/bin/python`.
sys.path.insert(0, "/app")

RID = "9ad8a0ac"
SOL = "10840486"

# Canonical CSP-Sacramento (California State Prison — Sacramento / New Folsom)
# overrides. These are the values that SHOULD have resolved from the buyer's
# email "CA State Prison Sacramento, 100 Prison Road, Folsom CA 95671".
# NOTE: buyer email stated 100 Prison Road; AGENCY_CONFIGS has CSP-SAC at
# 300 Prison Road (data bug logged as audit item W). Trusting the buyer's
# stated address here — this is the address the quote must print.
OVERRIDES = {
    "delivery_location": "CSP-Sacramento, 100 Prison Road, Folsom, CA 95671",
    "ship_to_name": "CSP Sacramento - New Folsom",
    "ship_to_address": ["100 Prison Road", "Folsom, CA 95671"],
    "institution": "CSP Sacramento - New Folsom",
    "agency": "CCHCS",
    "agency_name": "CCHCS / CDCR",
    # Tax — 95671 Folsom area = 7.75% per CDTFA (matches the hit from
    # generate-package at 04:07:15Z).
    "tax_rate": 7.75,
    "tax_validated": True,
    "tax_source": "cdtfa_api",
    "tax_jurisdiction": "FOLSOM",
    # Audit trace — what the operator observed.
    "_override_note": (
        f"Finalized 2026-04-22 via scripts/finalize_rfq_10840486.py. "
        f"Buyer email specified CSP-Sacramento / 100 Prison Rd / Folsom 95671; "
        f"default resolver had mapped to FSP. See audit W/X/M."
    ),
    "_override_at": datetime.utcnow().isoformat() + "Z",
}


def _print_current_state(r):
    print("── Current record state ──")
    for k in ("id", "solicitation_number", "institution", "agency",
              "delivery_location", "ship_to", "ship_to_name",
              "ship_to_address", "tax_rate", "tax_validated",
              "tax_source", "tax_jurisdiction", "status"):
        print(f"  {k}: {json.dumps(r.get(k))}")
    print()


def _apply_overrides(r):
    print("── Overrides to apply ──")
    for k, v in OVERRIDES.items():
        before = r.get(k)
        print(f"  {k}: {json.dumps(before)}  →  {json.dumps(v)}")
    print()
    for k, v in OVERRIDES.items():
        r[k] = v


def _regenerate_quote(r, output_dir):
    """Bypass generate_quote_from_rfq() — that path re-runs facility resolution
    and maps 'Folsom' to FSP (audit W). Instead, build quote_data explicitly
    with the correct canonical CSP-SAC values and call generate_quote()
    directly. This is also why institution != ship_to_name: the resolver-
    overwrite at quote_generator.py:908 triggers when `ship_name == to_name`,
    and we set them different so it stays skipped.
    """
    from src.forms import quote_generator
    out_path = os.path.join(output_dir, f"{SOL}_Quote_Reytech_CORRECTED.pdf")

    # Build quote_data explicitly — no resolver, no facility lookup.
    quote_data = {
        # institution != ship_to_name → resolver-overwrite skipped
        "institution": "Dept. of Corrections and Rehabilitation",
        "ship_to_name": OVERRIDES["ship_to_name"],
        "ship_to_address": list(OVERRIDES["ship_to_address"]),
        "to_address": list(OVERRIDES["ship_to_address"]),
        # Buyer's solicitation number
        "rfq_number": SOL,
        "source_rfq_id": r.get("id", RID),
        "requestor_email": r.get("requestor_email", ""),
        # Line items from the RFQ — quote generator normalizes each item
        "line_items": list(r.get("line_items", []) or []),
    }

    # Tax rate: generate_quote expects decimal (0.0775), record stores percent (7.75).
    tax_decimal = float(OVERRIDES["tax_rate"]) / 100.0

    result = quote_generator.generate_quote(
        quote_data,
        output_path=out_path,
        agency="CCHCS",
        tax_rate=tax_decimal,
        shipping=0.0,
    )
    return out_path, result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually mutate the record. Default is dry-run.")
    ap.add_argument("--regen-only", action="store_true",
                    help="Skip the override step and only regenerate the "
                         "quote PDF from the CURRENT record state. Use after "
                         "--apply has been run once.")
    ap.add_argument("--skip-regen", action="store_true",
                    help="Apply overrides but skip quote regeneration.")
    args = ap.parse_args()

    from src.api.data_layer import load_rfqs, _save_single_rfq
    rfqs = load_rfqs()
    r = rfqs.get(RID)
    if not r:
        print(f"ERROR: RFQ {RID} not found.")
        sys.exit(1)

    print(f"=== Finalize RFQ {RID} (solicitation {SOL}) ===\n")
    _print_current_state(r)

    if not args.regen_only:
        _apply_overrides(r)
        if not args.apply:
            print("DRY RUN — pass --apply to persist the overrides above.")
            print("DRY RUN — no files written.")
            return
        _save_single_rfq(RID, r)
        print(f"✓ Persisted override on {RID}\n")

    if args.skip_regen:
        print("Skipping quote regeneration (--skip-regen).")
        return

    output_dir = os.path.join("/data", "output", SOL)
    os.makedirs(output_dir, exist_ok=True)
    out_path, result = _regenerate_quote(r, output_dir)

    print(f"✓ Quote PDF regenerated: {out_path}")
    if isinstance(result, dict):
        print(f"  quote_number: {result.get('quote_number')}")
        print(f"  total: {result.get('total')}")
        print(f"  ship_to_name: {result.get('ship_to_name')}")
        print(f"  ship_to_address: {result.get('ship_to_address')}")
    print()
    print("Next steps:")
    print(f"  1. Download: https://web-production-dcee9.up.railway.app"
          f"/api/download/{SOL}/{SOL}_Quote_Reytech_CORRECTED.pdf")
    print(f"  2. Open in Acrobat — verify ship-to reads "
          f"'CSP Sacramento - New Folsom / 100 Prison Road / Folsom, CA 95671' "
          f"and TAX line is 7.75%.")
    print(f"  3. Hand-fill the buyer's LPA template separately "
          f"(Attachment 2 per finalize plan). Send both as email attachments.")


if __name__ == "__main__":
    main()
