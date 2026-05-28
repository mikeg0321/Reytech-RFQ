"""Map a generated package PDF filename to its canonical `form_id`.

This was inlined as a 12-line `elif` chain at two sites
(`routes_rfq_gen.py` + `routes_rfq.py`). Both copies were missing a
specific `barstow` check before the generic `cuf` substring — so
`RFQ_BarstowCUF_Reytech.pdf` matched `"cuf"` first and got
mislabelled as `cv012_cuf`. The QA gate then reported `barstow_cuf`
missing on every CalVet Barstow package even though the PDF was
written to disk and included in the combined package PDF.

Extracting both call sites onto this single helper:
  - prevents the two copies drifting again,
  - gives us one place to add new facility-specific CUFs,
  - is unit-testable on its own.

The order of checks below is significant: more specific patterns
must come before generic substrings (e.g. `barstow` before `cuf`,
each 703 revision before plain `703`). When in doubt: list the
facility-specific or solicitation-specific token first.

703 revision identity (added 2026-05-27 after the Coleman 10842771
incident): `703a`, `703b`, and `703c` are THREE DISTINCT form_ids in
`AVAILABLE_FORMS` (`src/core/agency_config.py`). Pre-2026-05-27 this
file collapsed `703c → 703b` and had NO `703a` check at all — the
buyer's 703A Rev. 03/2025 PDF was written to disk but tagged
`"unknown"`, and Form QA reported `703a` missing on every Coleman
package even though the file was bundled into the merged Package PDF.
Keep these as three distinct return values so the next 703 revision
(703D, etc.) lands in one obvious place.
"""
from __future__ import annotations


def classify_package_filename(filename: str) -> str:
    """Return the canonical `form_id` for a generated package filename.

    Returns the literal string `"unknown"` when no rule matches —
    callers can decide whether to treat unknown as a hard error or
    a warning. Never raises.
    """
    name = (filename or "").lower()
    if "quote" in name and "704" not in name:
        return "quote"
    # 703 family: each revision is a distinct form_id. List most-specific
    # variants explicitly so the next revision adds a single new branch.
    if "703a" in name:
        return "703a"
    if "703b" in name:
        return "703b"
    if "703c" in name:
        return "703c"
    if "704b" in name:
        return "704b"
    if "calrecycle" in name:
        return "calrecycle74"
    if "bidderdecl" in name or "bidder" in name:
        return "bidder_decl"
    if "dvbe" in name or "843" in name:
        return "dvbe843"
    if "darfur" in name:
        return "darfur_act"
    # Facility-specific CUFs MUST be tested before the generic `cuf`
    # substring. Add new facility CUFs here as they're built.
    if "barstow" in name:
        return "barstow_cuf"
    if "cuf" in name or "cv012" in name:
        return "cv012_cuf"
    if "std205" in name:
        return "std205"
    if "std204" in name or "payee" in name:
        return "std204"
    if "std1000" in name:
        return "std1000"
    if "seller" in name or "permit" in name:
        return "sellers_permit"
    if "bidpkg" in name or "bidpackage" in name:
        return "bidpkg"
    if "obs" in name or "1600" in name:
        return "obs_1600"
    if "drug" in name:
        return "drug_free"
    return "unknown"
