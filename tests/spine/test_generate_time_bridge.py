"""Tests for the J1-1 generate-time CCHCS on-ramp.

src.spine_bridge.ingest.synthesize_cchcs_email_contract — the thin
bridge that, given a legacy RFQ dict and a rfq_id, synthesizes a
transient Spine EmailContract at generate time.

Key invariants tested:
- Returns an EmailContract whose bill-to == cchcs_bill_to_tuple() output.
- Does NOT create any persisted state (no spine.db writes, no JSON
  mutations, no side-effects).
- Raises NotCchcsError for non-CCHCS agencies.
- Raises ValueError when tax_resolver returns no usable rate.
- Solicitation prefix stripped (PREQ / EREQ / IREQ).
- Required fields propagated (solicitation, facility, line_items).
- Default required_forms == CCHCS_DEFAULT_REQUIRED_FORMS.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.spine.agency_constants import cchcs_bill_to_tuple
from src.spine.email_contract import CCHCS_DEFAULT_REQUIRED_FORMS, EmailContract
from src.spine_bridge.ingest import NotCchcsError, synthesize_cchcs_email_contract


# ──────────────────────────────────────────────────────────────────────
# Stubs
# ──────────────────────────────────────────────────────────────────────


def _tax_825(_addr: str) -> int:
    """Deterministic stub: every address resolves to 8.25% (825 bps)."""
    return 825


def _tax_zero(_addr: str) -> int:
    """Stub that returns zero — invalid per Charter rule #6."""
    return 0


def _tax_none(_addr: str) -> int | None:
    """Stub that returns None — no usable rate."""
    return None


def _tax_raises(_addr: str) -> int:
    raise RuntimeError("CDTFA network timeout")


# ──────────────────────────────────────────────────────────────────────
# Minimal realistic RFQ dict (matches legacy load_rfqs() shape)
# ──────────────────────────────────────────────────────────────────────


def _minimal_rfq(**overrides) -> dict:
    base = {
        "agency": "CCHCS",
        "institution": "SATF Corcoran",
        "ship_to": "California Substance Abuse Treatment Facility, 900 Quebec Ave, Corcoran CA 93212",
        "solicitation_number": "PREQ 10847262",
        "line_items": [
            {
                "description": "GLOVES, NITRILE, LARGE, 100/BX",
                "qty": 10,
                "uom": "BX",
                "item_number": "MK-2103L",
            },
            {
                "description": "MASKS, SURGICAL, 50/BX",
                "qty": 12,
                "uom": "BX",
                "item_number": "PRM-1820",
            },
        ],
        "requestor_email": "jdoe@cchcs.ca.gov",
        "due_date": "2026-06-15",
    }
    base.update(overrides)
    return base


_RFQ_ID = "rfq_test_j1_onramp"
_PINNED_TS = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)


# ──────────────────────────────────────────────────────────────────────
# Happy path — returns a valid EmailContract
# ──────────────────────────────────────────────────────────────────────


def test_returns_email_contract_instance():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
        synthesis_ts=_PINNED_TS,
    )
    assert isinstance(result, EmailContract)


def test_agency_is_cchcs():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.agency == "CCHCS"


def test_rfq_id_propagated():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.rfq_id == _RFQ_ID


def test_solicitation_prefix_stripped():
    """PREQ prefix must be stripped, matching ingest_email_contract behaviour."""
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(solicitation_number="PREQ 10847262"),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.solicitation_number == "10847262"


def test_solicitation_no_prefix_left_intact():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(solicitation_number="10847262"),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.solicitation_number == "10847262"


def test_line_items_count_matches_rfq():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert len(result.line_items) == 2


def test_tax_rate_bps_carried():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.tax_rate_bps == 825


def test_synthesis_ts_propagates_to_ingested_at():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
        synthesis_ts=_PINNED_TS,
    )
    assert result.ingested_at == _PINNED_TS


# ──────────────────────────────────────────────────────────────────────
# Bill-to math reconciliation — the core LAW 6 + J1-1 invariant
#
# The contract MUST carry the canonical CCHCS bill-to from
# src.spine.agency_constants.cchcs_bill_to_tuple().  This is the
# math-reconcile the Inspector will check: "bill_to_name on the
# synthesized contract == CCHCS_CANONICAL_BILL_TO.name".
# ──────────────────────────────────────────────────────────────────────


def test_bill_to_name_equals_cchcs_canonical():
    expected_name, expected_email, expected_addr_lines = cchcs_bill_to_tuple()
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.bill_to_name == expected_name, (
        f"bill_to_name mismatch: got {result.bill_to_name!r}, "
        f"expected {expected_name!r} from cchcs_bill_to_tuple()"
    )


def test_bill_to_email_equals_cchcs_canonical():
    expected_name, expected_email, expected_addr_lines = cchcs_bill_to_tuple()
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.bill_to_email == expected_email, (
        f"bill_to_email mismatch: got {result.bill_to_email!r}, "
        f"expected {expected_email!r} from cchcs_bill_to_tuple()"
    )


def test_bill_to_address_equals_cchcs_canonical_joined():
    expected_name, expected_email, expected_addr_lines = cchcs_bill_to_tuple()
    expected_address = "\n".join(expected_addr_lines)
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.bill_to_address == expected_address, (
        f"bill_to_address mismatch: got {result.bill_to_address!r}, "
        f"expected {expected_address!r} from cchcs_bill_to_tuple()"
    )


# ──────────────────────────────────────────────────────────────────────
# Default form set
# ──────────────────────────────────────────────────────────────────────


def test_required_forms_defaults_to_cchcs_standard():
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.required_forms == CCHCS_DEFAULT_REQUIRED_FORMS


# ──────────────────────────────────────────────────────────────────────
# No persisted state — the "no DB writes" contract
#
# We verify this by confirming: (a) the function does NOT call
# write_quote or write_email_contract, and (b) the rfq_row dict is
# unchanged after the call (no side-effects on the caller's dict).
# ──────────────────────────────────────────────────────────────────────


def test_rfq_row_not_mutated():
    """The legacy rfq_row dict must not be modified by the on-ramp."""
    import copy
    rfq = _minimal_rfq()
    rfq_before = copy.deepcopy(rfq)

    synthesize_cchcs_email_contract(
        rfq_row=rfq,
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )

    assert rfq == rfq_before, (
        "synthesize_cchcs_email_contract must not mutate the rfq_row dict"
    )


def test_no_spine_db_write(monkeypatch, tmp_path):
    """Verify that no write_quote / write_email_contract is called."""
    calls = []

    def _spy_write(*args, **kwargs):
        calls.append(("write", args, kwargs))

    # Patch at the spine module level so any import path is covered.
    import src.spine as spine_pkg
    monkeypatch.setattr(spine_pkg, "write_quote", _spy_write)
    monkeypatch.setattr(spine_pkg, "write_email_contract", _spy_write)

    synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )

    assert calls == [], (
        f"synthesize_cchcs_email_contract must not write to spine.db; "
        f"got {calls!r}"
    )


# ──────────────────────────────────────────────────────────────────────
# Error handling
# ──────────────────────────────────────────────────────────────────────


def test_raises_not_cchcs_error_for_cdcr():
    with pytest.raises(NotCchcsError):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(agency="CDCR"),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_825,
        )


def test_raises_not_cchcs_error_for_calvet():
    with pytest.raises(NotCchcsError):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(agency="CalVet"),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_825,
        )


def test_raises_not_cchcs_error_for_empty_agency():
    with pytest.raises(NotCchcsError):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(agency=""),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_825,
        )


def test_cchcs_acq_variant_accepted():
    """CCHCS-ACQ is an acceptable agency alias (same procurement unit)."""
    result = synthesize_cchcs_email_contract(
        rfq_row=_minimal_rfq(agency="CCHCS-ACQ"),
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert isinstance(result, EmailContract)
    assert result.agency == "CCHCS"


def test_raises_value_error_when_tax_resolver_returns_none():
    with pytest.raises(ValueError, match="no usable rate"):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_none,
        )


def test_raises_value_error_when_tax_resolver_returns_zero():
    with pytest.raises(ValueError, match="no usable rate"):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_zero,
        )


def test_raises_value_error_when_tax_resolver_raises():
    with pytest.raises(ValueError, match="tax_resolver raised"):
        synthesize_cchcs_email_contract(
            rfq_row=_minimal_rfq(),
            rfq_id=_RFQ_ID,
            tax_resolver=_tax_raises,
        )


# ──────────────────────────────────────────────────────────────────────
# Agency field alias — legacy dicts sometimes use agency_key
# ──────────────────────────────────────────────────────────────────────


def test_agency_key_field_accepted():
    """Some legacy RFQ dicts store 'agency_key' instead of 'agency'."""
    rfq = _minimal_rfq()
    rfq.pop("agency")
    rfq["agency_key"] = "CCHCS"
    result = synthesize_cchcs_email_contract(
        rfq_row=rfq,
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.agency == "CCHCS"


# ──────────────────────────────────────────────────────────────────────
# facility / institution field aliases
# ──────────────────────────────────────────────────────────────────────


def test_institution_field_used_for_facility():
    rfq = _minimal_rfq(institution="VSPW Chowchilla")
    rfq.pop("facility", None)
    result = synthesize_cchcs_email_contract(
        rfq_row=rfq,
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.facility == "VSPW Chowchilla"


def test_facility_field_used_when_institution_absent():
    rfq = _minimal_rfq()
    rfq.pop("institution", None)
    rfq["facility"] = "CIW Chino"
    result = synthesize_cchcs_email_contract(
        rfq_row=rfq,
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert result.facility == "CIW Chino"


# ──────────────────────────────────────────────────────────────────────
# Line items — items key alias
# ──────────────────────────────────────────────────────────────────────


def test_items_key_alias_accepted():
    """Some legacy RFQ dicts use 'items' instead of 'line_items'."""
    rfq = _minimal_rfq()
    items = rfq.pop("line_items")
    rfq["items"] = items
    result = synthesize_cchcs_email_contract(
        rfq_row=rfq,
        rfq_id=_RFQ_ID,
        tax_resolver=_tax_825,
    )
    assert len(result.line_items) == 2


# ──────────────────────────────────────────────────────────────────────
# Public export sanity
# ──────────────────────────────────────────────────────────────────────


def test_public_export_from_spine_bridge():
    """synthesize_cchcs_email_contract and NotCchcsError are exported
    from src.spine_bridge so J1-2 callers need only one import."""
    from src.spine_bridge import (
        NotCchcsError as _NCE,
        synthesize_cchcs_email_contract as _fn,
    )
    assert _fn is synthesize_cchcs_email_contract
    assert _NCE is NotCchcsError
