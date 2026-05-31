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
This module is intentionally narrow: it carries the per-agency canonical
bill-to constants the Spine ingest needs to populate
`EmailContract.bill_to_*` as each agency migrates (Job #1: CCHCS; Job #2:
CalVet). It is NOT a generic "agency registry" or "config substrate" —
those would be a new substrate and would need Architect + Closer sign-off
(LAW 1 / LAW 4). An agency's bill-to entry is added here ONLY by its own
migration ticket, sourced byte-for-byte from the legacy
`quote_generator.AGENCY_CONFIGS` entry (cited in a code comment at the
entry). The remaining unmigrated agencies (CDCR, DSH, DGS) continue to
read from the legacy table until their own migration tickets follow this
pattern.

Architect approval recorded in the PR introducing this module per §0
LAW 4 (2026-05-27 — Job #1 prerequisite, ticket PR-Job1-A0). CalVet
bill-to added per §0 Job #2, ticket J2-1 (2026-05-31, Architect-
authorized).
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


# ──────────────────────────────────────────────────────────────────────
# CalVet — §0 Job #2 (J2-1). Same pattern as CCHCS above: promote the
# canonical CalVet bill-to to this Spine-native source of truth so the
# upcoming `synthesize_calvet_email_contract` (J2-3) can populate
# `EmailContract.bill_to_*` for CalVet WITHOUT importing the legacy
# `src/forms/quote_generator.py::AGENCY_CONFIGS`.
#
# SOURCE OF TRUTH (cited per §0 LAW 4, mirroring the CCHCS block above):
# `src/forms/quote_generator.py::AGENCY_CONFIGS["CalVet"]` (lines 93-98),
# verified 2026-05-31:
#     "bill_to_name": "California Department of Veterans Affairs"
#     "bill_to_lines": ["APinvoices@calvet.ca.gov",
#                       "1227 \"O\" Street, Room 403",
#                       "Sacramento, CA 95814", "United States"]
#
# Shape note: unlike CCHCS (where the AP email is the LAST legacy line),
# CalVet's AP email is the FIRST legacy line. In BOTH cases the email is
# hoisted out of the physical address into `.email`, and the remaining
# physical address lines are kept in `address_lines` (CalVet retains the
# "United States" line that the legacy entry carried). The ingest seam
# reconstructs the legacy `bill_to_lines` ordering when it needs it.
#
# Distinct from CCHCS: CalVet bills to its OWN central AP (CDVA, 1227 "O"
# Street, Sacramento) — NOT a parent department's AP (CCHCS routes through
# CDCR Accounts Payable). This is a real central bill-to, not a
# facility ship-to.


@dataclass(frozen=True)
class CalVetCanonicalBillTo:
    """Frozen CalVet canonical bill-to (CDVA central Accounts Payable).

    Frozen dataclass — immutable, matching the §0 LAW 6 invariant ("the
    EmailContract carries every answer at ingest" → the answer must not
    mutate underneath us). The shape is pinned by
    `tests/spine/test_agency_constants.py`; any drift breaks the test.
    """

    name: str = "California Department of Veterans Affairs"
    email: str = "APinvoices@calvet.ca.gov"
    address_lines: tuple[str, ...] = (
        "1227 \"O\" Street, Room 403",
        "Sacramento, CA 95814",
        "United States",
    )


# Module-level singleton — callers use this, never instantiate.
CALVET_CANONICAL_BILL_TO = CalVetCanonicalBillTo()


def calvet_bill_to_tuple() -> tuple[str, str, tuple[str, ...]]:
    """Return CalVet canonical bill-to as `(name, email, address_lines)`.

    Helper for the upcoming `synthesize_calvet_email_contract` ingest
    seam (J2-3), mirroring `cchcs_bill_to_tuple()`. Returns the same
    3-tuple shape so the CalVet contract-construction call site reuses
    the CCHCS seam logic. Caller joins `address_lines` with "\\n" if it
    needs a single-string `bill_to_address`.
    """
    return (
        CALVET_CANONICAL_BILL_TO.name,
        CALVET_CANONICAL_BILL_TO.email,
        CALVET_CANONICAL_BILL_TO.address_lines,
    )
