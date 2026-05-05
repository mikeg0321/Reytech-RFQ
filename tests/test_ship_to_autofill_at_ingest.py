"""Ship-to auto-fill from facility_registry at ingest (Surface #15).

Mike's complaint 2026-05-04 (PC pc_08583a68): Auto-Classification card said
`Institution: CSP-Sacramento, agency: CCHCS / CDCR, confidence: 65%`. But
the Ship-to field had "CA" — Mike was retyping `100 Prison Road, Represa,
CA 95671` every PC even though facility_registry.py:223-227 already had it.

Doctrine: feedback_app_is_source_of_truth + feedback_canonical_not_verbatim
+ feedback_global_fix_not_one_off. The registry IS the canonical address.
The buyer's free-form text in the parsed PDF/email is honored ONLY when
substantive (>3 chars, not just "CA"); otherwise the registry wins.
"""
from __future__ import annotations

import re
from pathlib import Path


def test_ingest_pipeline_resolves_ship_to_from_facility_registry_source():
    """Source-level guard: ingest_pipeline.py must call into facility_registry
    to derive ship_to AND emit it on the persisted record."""
    src = Path("src/core/ingest_pipeline.py").read_text(encoding="utf-8")
    # The facility resolve block must compute canonical_ship_to.
    assert "canonical_ship_to" in src, (
        "ingest_pipeline must compute canonical_ship_to from the FacilityRecord "
        "during institution canonicalization. Surface #15 fix."
    )
    # The record dict must include ship_to.
    assert re.search(r'"ship_to":\s*resolved_ship_to', src), (
        "ingest_pipeline record dict must include 'ship_to': resolved_ship_to. "
        "Without this, the persisted PC has no ship_to and Mike retypes the "
        "address on every PC."
    )


def test_facility_registry_csp_sac_ship_to_unchanged():
    """Pin the exact CSP-SAC address that fed Mike's complaint. If a future
    PR changes 'Prison Road' back to 'Folsom Prison Rd' or '300 Prison Road'
    (Audit W history), this test catches it."""
    from src.core.facility_registry import FACILITIES_BY_CODE

    csp_sac = FACILITIES_BY_CODE.get("CSP-SAC")
    assert csp_sac is not None, "CSP-SAC facility entry missing"
    assert csp_sac.address_line1 == "100 Prison Road", (
        f"Audit W's '300 Prison Road' → '100 Prison Road' correction reverted. "
        f"Got: {csp_sac.address_line1!r}"
    )
    assert "Represa" in csp_sac.address_line2 and "95671" in csp_sac.address_line2


def test_ship_to_resolves_for_csp_sac_via_quote_contract():
    """Behavioral check: the public ship_to_for_text helper returns the
    canonical CSP-SAC address for several alias spellings Mike uses."""
    from src.core.quote_contract import ship_to_for_text

    for alias in ["CSP-Sacramento", "CSP-SAC", "California State Prison Sacramento",
                  "New Folsom"]:
        addr = ship_to_for_text(alias)
        assert "Prison Road" in addr, (
            f"ship_to_for_text({alias!r}) → {addr!r} — expected canonical "
            f"CSP-SAC address with 'Prison Road'"
        )
        assert "95671" in addr


def test_buyer_explicit_ship_to_overrides_canonical():
    """When the buyer's parsed header carries a substantive ship_to (>3 chars),
    it wins over the canonical registry. Mike's intent: a buyer who explicitly
    says 'building C' should be honored."""
    src = Path("src/core/ingest_pipeline.py").read_text(encoding="utf-8")
    # The override logic must check header.ship_to / header.delivery_address.
    assert 'header.get("ship_to")' in src and 'header.get("delivery_address")' in src
    # And it must check len > 3 to filter "CA"/empty/whitespace.
    assert re.search(r"len\(_hdr_ship_to\)\s*>\s*3", src), (
        "Header ship_to override must filter 2-char/'CA'/whitespace values. "
        "Otherwise a stray 'CA' from the parser silently disables the auto-fill."
    )
