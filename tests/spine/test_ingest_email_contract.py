"""Tests for the EmailContract construction inside ingest_email_contract.

Companion to test_ingest.py. Those tests cover Quote translation; these
cover the immutable EmailContract built alongside (the master-substrate
projection of what the buyer asked for).

Architectural invariants:
- IngestResult.email_contract is populated iff IngestResult.quote is.
- The contract captures buyer-side fields that Quote intentionally
  drops (buyer name/email, due_date, attachment refs, parser_version).
- The contract is independently writable into spine_email_contracts.
- Re-ingesting the SAME source produces a NEW contract_id (immutable
  history; rebids never modify the prior contract).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.spine import (
    EmailContract,
    QuoteStatus,
    init_db,
    write_email_contract,
    read_email_contract,
    write_quote,
    find_contract_for_quote,
)
from src.spine_bridge import IngestResult, ingest_email_contract


def _tax_825(_addr: str) -> int:
    return 825


def _minimal_contract(**overrides) -> dict:
    base = {
        "rfq_id": "rfq_contract_test",
        "agency": "CCHCS",
        "facility": "SATF Corcoran 93212",
        "ship_to": "CA Substance Abuse TF, 900 Quebec Ave, Corcoran CA 93212",
        "solicitation_number": "PREQ 10847262",
        "line_items": [
            {"description": "GLOVES, NITRILE, LARGE, 100/BX",
             "item_number": "MK-2103L", "qty": 10, "uom": "BX"},
            {"description": "MASKS, SURGICAL, 50/BX",
             "item_number": "PRM-1820", "qty": 12, "uom": "BX"},
        ],
        "buyer": {
            "name": "Robert Buyer",
            "email": "rbuyer@cchcs.ca.gov",
            "phone": "555-111-2222",
            "title": "Procurement Officer",
        },
        "due_date": "2026-05-20T17:00:00+00:00",
        "rfq_title": "RFQ for Examination Supplies",
        "attachment_refs": ["s3://cchcs-rfq/abc/raw.pdf"],
        "parser_version": "vision-v3",
    }
    base.update(overrides)
    return base


# ──────────────────────────────────────────────────────────────────────
# IngestResult shape
# ──────────────────────────────────────────────────────────────────────


def test_ok_implies_both_quote_and_contract_present():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.ok
    assert result.quote is not None
    assert result.email_contract is not None


def test_failure_returns_neither_quote_nor_contract():
    result = ingest_email_contract(
        _minimal_contract(rfq_id=""), tax_resolver=_tax_825,
    )
    assert not result.ok
    assert result.quote is None
    assert result.email_contract is None


# ──────────────────────────────────────────────────────────────────────
# EmailContract content — captures buyer-side fields Quote drops
# ──────────────────────────────────────────────────────────────────────


def test_contract_captures_buyer_identity():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    c = result.email_contract
    assert c.buyer_name == "Robert Buyer"
    assert c.buyer_email == "rbuyer@cchcs.ca.gov"
    assert c.buyer_phone == "555-111-2222"
    assert c.buyer_title == "Procurement Officer"


def test_contract_captures_due_date_parsed_to_datetime():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    c = result.email_contract
    assert isinstance(c.due_date, datetime)
    assert c.due_date.year == 2026 and c.due_date.month == 5 and c.due_date.day == 20


def test_contract_captures_attachment_refs():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.email_contract.attachment_refs == ["s3://cchcs-rfq/abc/raw.pdf"]


def test_contract_captures_parser_version():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.email_contract.ingest_parser_version == "vision-v3"


def test_contract_solicitation_uses_stripped_form():
    """The contract's solicitation_number must match what Quote stores —
    i.e. the PREQ-prefix-stripped form. Otherwise diff against the
    Quote would always show a spurious "operator changed sol#"."""
    result = ingest_email_contract(
        _minimal_contract(solicitation_number="PREQ 10847262"),
        tax_resolver=_tax_825,
    )
    assert result.email_contract.solicitation_number == "10847262"
    assert result.quote.solicitation_number == "10847262"


def test_contract_inherits_tax_rate_from_resolver():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.email_contract.tax_rate_bps == 825
    assert result.quote.tax_rate_bps == 825


def test_contract_line_items_carry_buyer_mfg_suggestions():
    """Buyer's item_number lands on ContractLineItem.mfg_number_suggested.
    Quote.line_items[*].mfg_number may diverge later if operator
    overrides; the contract preserves what the buyer originally
    suggested."""
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    cli = result.email_contract.line_items
    assert cli[0].mfg_number_suggested == "MK-2103L"
    assert cli[1].mfg_number_suggested == "PRM-1820"


# ──────────────────────────────────────────────────────────────────────
# contract_id construction — stable but rebid-safe
# ──────────────────────────────────────────────────────────────────────


def test_contract_id_includes_rfq_id_and_timestamp():
    ts = datetime(2026, 5, 16, 12, 30, 0, tzinfo=timezone.utc)
    result = ingest_email_contract(
        _minimal_contract(rfq_id="rfq_abc123"),
        tax_resolver=_tax_825, ingest_ts=ts,
    )
    cid = result.email_contract.contract_id
    assert cid.startswith("contract_rfq_abc123_")
    assert str(int(ts.timestamp())) in cid


def test_reingest_produces_different_contract_id():
    """Two ingest calls 1 second apart on the same RFQ produce two
    distinct contracts — the substrate stores history, not state."""
    ts1 = datetime(2026, 5, 16, 12, 30, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 16, 12, 30, 1, tzinfo=timezone.utc)
    r1 = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825, ingest_ts=ts1)
    r2 = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825, ingest_ts=ts2)
    assert r1.email_contract.contract_id != r2.email_contract.contract_id


# Note: Quote.quote_id has its own pattern validation [A-Za-z0-9_-] so
# rfq_ids with unsafe chars are rejected at the Quote level, never
# reaching the contract-build step. _sanitize_for_cid stays as
# defense-in-depth but is not unit-tested via the public ingest path.


# ──────────────────────────────────────────────────────────────────────
# DB integration — both writes succeed; substrate links by rfq_id
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_ingest_contract.db"
    init_db(str(p))
    return str(p)


def test_ingest_result_writes_both_contract_and_quote(db_path: str):
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.ok

    write_email_contract(db_path, result.email_contract)
    write_quote(db_path, result.quote, actor="spine_ingest")

    # Round-trip the contract.
    c = read_email_contract(db_path, result.email_contract.contract_id)
    assert c is not None
    assert c.solicitation_number == result.email_contract.solicitation_number
    assert c.buyer_email == "rbuyer@cchcs.ca.gov"

    # The Quote can find its contract by rfq_id.
    found = find_contract_for_quote(db_path, result.quote.quote_id)
    assert found is not None
    assert found.contract_id == result.email_contract.contract_id


# ──────────────────────────────────────────────────────────────────────
# Minimal-field contract — optional buyer fields tolerated as None
# ──────────────────────────────────────────────────────────────────────


def test_contract_built_when_buyer_block_missing():
    contract = _minimal_contract()
    contract.pop("buyer", None)
    result = ingest_email_contract(contract, tax_resolver=_tax_825)
    assert result.ok
    c = result.email_contract
    assert c.buyer_name is None
    assert c.buyer_email is None


def test_contract_built_when_due_date_missing():
    contract = _minimal_contract()
    contract.pop("due_date", None)
    result = ingest_email_contract(contract, tax_resolver=_tax_825)
    assert result.ok
    assert result.email_contract.due_date is None


def test_contract_built_when_attachment_refs_missing():
    contract = _minimal_contract()
    contract.pop("attachment_refs", None)
    result = ingest_email_contract(contract, tax_resolver=_tax_825)
    assert result.ok
    assert result.email_contract.attachment_refs == []


# ──────────────────────────────────────────────────────────────────────
# Canonical bill-to / ship-to resolution at ingest (LAW 6)
# ──────────────────────────────────────────────────────────────────────
# Mike caught this on Duffey rfq_89bb9a3e (2026-05-26): the Quote PDF
# rendered "Bill to: CCHCS" + "Ship to: CA STATE PRISON SACRAMENTO"
# with no street, no PO Box, no email. Substrate cause: the EmailContract
# fields bill_to_name/email/address/ship_to_facility/ship_to_address
# were all null because ingest never looked them up. Per §0 LAW 6 the
# canonical answers MUST be on the contract AT INGEST — these tests pin
# that invariant for CCHCS quotes shipped through Spine. The renderer
# already supports the rich fields; ingest is the seam.


def test_contract_populates_canonical_bill_to_for_cchcs():
    """A CCHCS-class quote ingested with no raw bill-to in the dict
    must inherit the canonical AGENCY_CONFIGS bill-to (name + email +
    multi-line address). Without this, every CCHCS Quote PDF shows
    'Bill to: CCHCS' with no street."""
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    c = result.email_contract
    assert c.bill_to_name == "Dept. of Corrections and Rehabilitation"
    assert c.bill_to_email == "APA.Invoices@cdcr.ca.gov"
    assert c.bill_to_address is not None
    assert "P.O. BOX 187021" in c.bill_to_address
    assert "Sacramento, CA 95818-7021" in c.bill_to_address


def test_contract_populates_canonical_ship_to_from_facility_registry():
    """A facility free-text string that resolves cleanly via
    facility_registry must yield a populated ship_to_facility
    (canonical_name) and ship_to_address (line1+line2 joined)."""
    # SATF resolves unambiguously per facility_registry seed data.
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    c = result.email_contract
    assert c.ship_to_facility is not None
    assert "SATF" in c.ship_to_facility or "California Substance Abuse" in c.ship_to_facility
    assert c.ship_to_address is not None
    # The resolved registry address overrides the raw "ship_to" string
    # only when the raw is absent; the _minimal_contract above passes
    # ship_to which WINS — so this test must use the dict-supplied value.
    # Re-test with no raw ship_to to prove canonical lookup fills the gap.
    contract = _minimal_contract()
    contract.pop("ship_to", None)
    contract.pop("ship_to_address", None)
    result2 = ingest_email_contract(contract, tax_resolver=_tax_825)
    c2 = result2.email_contract
    assert c2.ship_to_address is not None
    assert "Corcoran" in c2.ship_to_address  # SATF is in Corcoran, CA


def test_raw_dict_bill_to_overrides_canonical_lookup():
    """If the raw ingest dict carries a bill_to_name (operator override
    path, richer parser), it wins over the canonical lookup. Canonical
    is fallback-only, never stomp."""
    contract = _minimal_contract()
    contract["bill_to_name"] = "Override Buyer LLC"
    contract["bill_to_email"] = "ap@override.example.com"
    contract["bill_to_address"] = "1 Override Way\nOverride, CA 90000"
    result = ingest_email_contract(contract, tax_resolver=_tax_825)
    c = result.email_contract
    assert c.bill_to_name == "Override Buyer LLC"
    assert c.bill_to_email == "ap@override.example.com"
    assert c.bill_to_address == "1 Override Way\nOverride, CA 90000"


def test_unrecognized_agency_leaves_bill_to_null_gracefully():
    """A non-CCHCS, non-AGENCY_CONFIGS agency leaves bill-to None
    (status quo) — never raises, never blocks ingest."""
    contract = _minimal_contract()
    contract["agency"] = "UnknownAgency"
    # Quote-construction will reject the unknown agency, so we test the
    # resolver function directly to confirm graceful degradation.
    from src.spine_bridge.ingest import _resolve_canonical_bill_to
    name, email, addr = _resolve_canonical_bill_to("UnknownAgency")
    assert (name, email, addr) == (None, None, None)


def test_unresolvable_facility_leaves_ship_to_null_gracefully():
    """A facility string that facility_registry can't resolve returns
    (None, None) — caller's raw-dict fallback preserves today's
    behavior. Never raises."""
    from src.spine_bridge.ingest import _resolve_canonical_ship_to
    fac, addr = _resolve_canonical_ship_to("Nonexistent Facility XYZ")
    assert (fac, addr) == (None, None)
