"""Spine-native canonical agency constants.

This module holds the canonical agency-level constants the Spine ingest
needs to satisfy §0 LAW 6 ("the EmailContract carries every answer at
ingest") WITHOUT importing from the legacy substrate that Job #1 is
deleting.

Why it exists
=============
`src/spine_bridge/ingest.py::_resolve_canonical_bill_to(agency)` used to
read CCHCS bill-to from `src.forms.quote_generator.AGENCY_CONFIGS`. Job #1
plans to DELETE `AGENCY_CONFIGS["CCHCS"]`; doing that today would silently
strip `bill_to_*` from every new CCHCS `EmailContract` — a LAW 6
violation. The fix is to promote the canonical CCHCS values to a
Spine-native source of truth (this module) BEFORE the legacy entry is
deleted.

The values below are the byte-for-byte CDCR Accounts Payable address
that has shipped on every CCHCS quote since the legacy AGENCY_CONFIGS
table landed (verified 2026-05-27 against
`src/forms/quote_generator.py::AGENCY_CONFIGS["CCHCS"]`).

Scope discipline (§0 LAW 4)
===========================
This module is intentionally narrow: it carries the constants the Spine
ingest needs to populate `EmailContract.bill_to_*` for CCHCS. It is NOT
a generic "agency registry" or "config substrate" — those would be a new
substrate and would need Architect + Closer sign-off (LAW 1 / LAW 4).
Adding non-CCHCS agency entries here is OUT OF SCOPE for this ticket;
the other agencies (CDCR, CalVet, DSH, DGS) continue to read from the
legacy table until their own migration tickets follow Job #1's pattern.

Architect approval recorded in the PR introducing this module per §0
LAW 4 (2026-05-27 — Job #1 prerequisite, ticket PR-Job1-A0).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CchcsCanonicalBillTo:
    """Frozen CCHCS canonical bill-to (CDCR Accounts Payable).

    Frozen dataclass — instances are immutable, which matches the §0
    LAW 6 invariant ("the EmailContract carries every answer at
    ingest" → the answer must not mutate underneath us). The shape is
    pinned by `tests/spine/test_agency_constants.py`; any drift breaks
    the test.
    """

    name: str = "Dept. of Corrections and Rehabilitation"
    email: str = "APA.Invoices@cdcr.ca.gov"
    address_lines: tuple[str, ...] = (
        "Attn: Accounts Payable",
        "P.O. BOX 187021",
        "Sacramento, CA 95818-7021",
    )


# Module-level singleton — callers use this, never instantiate.
CCHCS_CANONICAL_BILL_TO = CchcsCanonicalBillTo()


def cchcs_bill_to_tuple() -> tuple[str, str, tuple[str, ...]]:
    """Return CCHCS canonical bill-to as `(name, email, address_lines)`.

    Helper for the ingest seam in `src/spine_bridge/ingest.py`, which
    expects a 3-tuple matching the legacy `_resolve_canonical_bill_to`
    return shape so the contract construction call site doesn't have to
    change. Caller is responsible for joining `address_lines` with
    "\\n" if it needs a single-string `bill_to_address`.
    """
    return (
        CCHCS_CANONICAL_BILL_TO.name,
        CCHCS_CANONICAL_BILL_TO.email,
        CCHCS_CANONICAL_BILL_TO.address_lines,
    )
