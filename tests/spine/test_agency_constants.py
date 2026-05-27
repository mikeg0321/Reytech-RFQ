"""Pin the Spine-native CCHCS canonical bill-to constants.

`src/spine/agency_constants.py` carries the CCHCS bill-to (CDCR
Accounts Payable) that ingest writes into `EmailContract.bill_to_*` per
§0 LAW 6. These tests pin the byte-for-byte values so future drift
breaks loudly — the CCHCS canonical address is on every quote that
Reytech ships to CCHCS, so a silent change here would corrupt
production output.

The values must also match the legacy `AGENCY_CONFIGS["CCHCS"]` entry
in `src/forms/quote_generator.py` byte-for-byte until Job #1 deletes
that entry. After deletion, this module is the single source of truth
and these tests are the only pinning that survives.

Architect-approved per §0 LAW 4 (ticket PR-Job1-A0, 2026-05-27).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, is_dataclass

import pytest

from src.spine.agency_constants import (
    CCHCS_CANONICAL_BILL_TO,
    CchcsCanonicalBillTo,
    cchcs_bill_to_tuple,
)


# ──────────────────────────────────────────────────────────────────────
# Byte-for-byte values — these are operator-facing canonical strings.
# ──────────────────────────────────────────────────────────────────────


def test_cchcs_canonical_bill_to_name_is_cdcr_dept():
    """The bill-to name is the CDCR department string (CCHCS routes
    Accounts Payable through the parent department)."""
    assert CCHCS_CANONICAL_BILL_TO.name == "Dept. of Corrections and Rehabilitation"


def test_cchcs_canonical_bill_to_email_is_cdcr_apa():
    """The CDCR Accounts Payable inbox — required on the Reytech Quote
    PDF for CCHCS."""
    assert CCHCS_CANONICAL_BILL_TO.email == "APA.Invoices@cdcr.ca.gov"


def test_cchcs_canonical_bill_to_address_lines_are_cdcr_ap_po_box():
    """3-line CDCR Accounts Payable address — must match legacy
    AGENCY_CONFIGS["CCHCS"]["bill_to_lines"] byte-for-byte (minus the
    email line which is carried in .email)."""
    assert CCHCS_CANONICAL_BILL_TO.address_lines == (
        "Attn: Accounts Payable",
        "P.O. BOX 187021",
        "Sacramento, CA 95818-7021",
    )


# ──────────────────────────────────────────────────────────────────────
# Shape & immutability — pin the dataclass contract.
# ──────────────────────────────────────────────────────────────────────


def test_cchcs_canonical_bill_to_is_frozen_dataclass():
    """Instances must be immutable — §0 LAW 6 invariant ("the answer at
    ingest must not mutate underneath us"). Frozen=True is enforced at
    construction; this test pins it so a future refactor that drops
    frozen= breaks loudly."""
    assert is_dataclass(CchcsCanonicalBillTo)
    with pytest.raises(FrozenInstanceError):
        CCHCS_CANONICAL_BILL_TO.name = "tampered"  # type: ignore[misc]


def test_cchcs_canonical_bill_to_shape_pinned():
    """Exactly three fields: name, email, address_lines. Any drift
    (added field, renamed field) breaks this test — caller code in
    `_resolve_canonical_bill_to` assumes the 3-field shape via the
    `cchcs_bill_to_tuple()` helper."""
    field_names = {f.name for f in fields(CchcsCanonicalBillTo)}
    assert field_names == {"name", "email", "address_lines"}


def test_address_lines_is_a_tuple_not_a_list():
    """A frozen dataclass with a list default is hashability-broken and
    mutable in practice; we declared tuple intentionally. Pin it."""
    assert isinstance(CCHCS_CANONICAL_BILL_TO.address_lines, tuple)


# ──────────────────────────────────────────────────────────────────────
# Helper shape — `cchcs_bill_to_tuple()` is the seam ingest uses.
# ──────────────────────────────────────────────────────────────────────


def test_cchcs_bill_to_tuple_returns_three_part_shape():
    """`cchcs_bill_to_tuple()` is the seam `_resolve_canonical_bill_to`
    uses — it must return (name, email, address_lines) in that order."""
    name, email, address_lines = cchcs_bill_to_tuple()
    assert name == CCHCS_CANONICAL_BILL_TO.name
    assert email == CCHCS_CANONICAL_BILL_TO.email
    assert address_lines == CCHCS_CANONICAL_BILL_TO.address_lines


def test_cchcs_bill_to_tuple_address_is_tuple_of_strings():
    """The 3rd element is a tuple of address line strings — the ingest
    seam joins them with '\\n' to produce the single-string
    `bill_to_address` the Spine model holds."""
    _, _, address_lines = cchcs_bill_to_tuple()
    assert isinstance(address_lines, tuple)
    assert all(isinstance(line, str) for line in address_lines)
    assert len(address_lines) >= 1
