"""Email contract → Spine Quote ingest tests.

Mandatory tax-at-ingest (Charter rule #6), agency gate, line item
validation, no-prices-required-at-parsed-status, single-agency
literal still enforced.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.spine import QuoteStatus
from src.spine_bridge import IngestResult, ingest_email_contract


# ──────────────────────────────────────────────────────────────────────
# Stubs
# ──────────────────────────────────────────────────────────────────────


def _tax_825(_addr: str) -> int:
    """Stub: every address resolves to 8.25%."""
    return 825


def _tax_none(_addr: str) -> int | None:
    """Stub: CDTFA call fails for every address."""
    return None


def _tax_raises(_addr: str) -> int:
    raise RuntimeError("CDTFA network timeout")


def _minimal_contract(**overrides) -> dict:
    base = {
        "rfq_id": "rfq_ingest_test",
        "agency": "CCHCS",
        "facility": "SATF Corcoran 93212",
        "ship_to": "California Substance Abuse Treatment Facility "
                    "900 Quebec Ave, Corcoran CA 93212",
        "solicitation_number": "PREQ 10847262",
        "line_items": [
            {"description": "GLOVES, NITRILE, LARGE, 100/BX",
             "item_number": "MK-2103L", "qty": 10, "uom": "BX"},
            {"description": "MASKS, SURGICAL, 50/BX",
             "item_number": "PRM-1820", "qty": 12, "uom": "BX"},
        ],
        "buyer": {"name": "Test Buyer", "email": "test@example.com"},
        "due_date": "2026-05-20",
    }
    base.update(overrides)
    return base


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_minimal_contract_ingests_to_parsed_quote():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.ok, "\n".join(
        f"  {i.field_path}: {i.detail}" for i in result.errors()
    )
    q = result.quote
    assert q is not None
    assert q.status == QuoteStatus.PARSED
    assert q.agency == "CCHCS"
    assert q.facility == "SATF Corcoran 93212"
    assert q.solicitation_number == "10847262"  # PREQ stripped
    assert q.tax_rate_bps == 825
    assert len(q.line_items) == 2
    # No prices at ingest — that's expected.
    assert all(li.unit_price_cents == 0 for li in q.line_items)
    assert all(li.cost_cents == 0 for li in q.line_items)


def test_quote_id_is_taken_from_rfq_id_field():
    result = ingest_email_contract(
        _minimal_contract(rfq_id="rfq_abc_001"),
        tax_resolver=_tax_825,
    )
    assert result.ok
    assert result.quote.quote_id == "rfq_abc_001"


def test_quote_id_falls_back_to_id_field():
    contract = _minimal_contract()
    contract.pop("rfq_id")
    contract["id"] = "rfq_via_id_key"
    result = ingest_email_contract(contract, tax_resolver=_tax_825)
    assert result.ok
    assert result.quote.quote_id == "rfq_via_id_key"


def test_ingest_ts_propagates_to_provenance():
    pinned = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = ingest_email_contract(
        _minimal_contract(),
        tax_resolver=_tax_825,
        ingest_ts=pinned,
    )
    assert result.ok
    assert result.quote.created_at == pinned
    assert result.quote.updated_at == pinned
    # cost_validated_at on every line item matches the pinned timestamp.
    for li in result.quote.line_items:
        assert li.cost_validated_at == pinned


# ──────────────────────────────────────────────────────────────────────
# MANDATORY tax-at-ingest — Charter rule #6.
# ──────────────────────────────────────────────────────────────────────


def test_ingest_fails_when_tax_resolver_returns_none():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_none)
    assert not result.ok
    tax_errors = [i for i in result.errors() if "tax" in i.field_path]
    assert tax_errors, "expected a tax_rate_bps error"
    assert any("mandatory at ingest" in i.detail.lower() or "no usable rate" in i.detail.lower()
               for i in tax_errors)


def test_ingest_fails_when_tax_resolver_returns_zero():
    result = ingest_email_contract(
        _minimal_contract(),
        tax_resolver=lambda _: 0,
    )
    assert not result.ok
    assert any("tax" in i.field_path for i in result.errors())


def test_ingest_fails_when_tax_resolver_raises():
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_raises)
    assert not result.ok
    raise_errors = [
        i for i in result.errors()
        if "tax_resolver raised" in i.detail
    ]
    assert len(raise_errors) == 1


# ──────────────────────────────────────────────────────────────────────
# Required fields
# ──────────────────────────────────────────────────────────────────────


def test_ingest_fails_without_rfq_id():
    c = _minimal_contract()
    c.pop("rfq_id")
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any(i.field_path == "rfq_id" for i in result.errors())


def test_ingest_fails_without_solicitation_number():
    c = _minimal_contract(solicitation_number="")
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any(i.field_path == "solicitation_number" for i in result.errors())


def test_ingest_fails_without_facility_or_ship_to():
    c = _minimal_contract(facility="", ship_to="")
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any(i.field_path == "facility" for i in result.errors())


def test_ingest_fails_with_no_line_items():
    c = _minimal_contract(line_items=[])
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any(i.field_path == "line_items" for i in result.errors())


def test_ingest_fails_with_line_item_missing_description():
    c = _minimal_contract(line_items=[{
        "item_number": "X", "qty": 1, "uom": "EA",
    }])
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any("description" in i.field_path for i in result.errors())


# ──────────────────────────────────────────────────────────────────────
# Agency gate — v1 CCHCS only.
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("agency", ["CalVet", "DGS", "DSH", "CalRecycle"])
def test_ingest_rejects_non_cchcs_agency(agency):
    c = _minimal_contract(agency=agency)
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert not result.ok
    assert any(i.field_path == "agency" for i in result.errors())


def test_ingest_defaults_to_cchcs_when_agency_absent():
    """If the email contract omits agency, assume CCHCS (v1 default)."""
    c = _minimal_contract()
    c.pop("agency")
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert result.ok
    assert result.quote.agency == "CCHCS"


# ──────────────────────────────────────────────────────────────────────
# PREQ prefix normalization mirrors translator
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("prefix", ["PREQ ", "PREQ-", "preq ", "PREQ"])
def test_preq_prefix_stripped(prefix):
    c = _minimal_contract(solicitation_number=f"{prefix}10847262")
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert result.ok
    assert result.quote.solicitation_number == "10847262"


# ──────────────────────────────────────────────────────────────────────
# Misc
# ──────────────────────────────────────────────────────────────────────


def test_shipping_fields_dropped_with_info_note():
    c = _minimal_contract(
        shipping_option="included",
        shipping_amount=42,
        delivery_option="fob_dest",
    )
    result = ingest_email_contract(c, tax_resolver=_tax_825)
    assert result.ok
    info_paths = {i.field_path for i in result.issues if i.severity == "info"}
    assert "shipping_option" in info_paths
    assert "shipping_amount" in info_paths
    assert "delivery_option" in info_paths


def test_parsed_quote_can_advance_to_priced_after_pricing():
    """End-to-end: ingest → operator adds prices → advance to priced.

    This is the operator workflow that ingest is meant to feed.
    """
    result = ingest_email_contract(_minimal_contract(), tax_resolver=_tax_825)
    assert result.ok
    parsed = result.quote
    assert parsed.status == QuoteStatus.PARSED

    # Operator adds prices (mutating the model is fine before write).
    priced_lines = [
        li.model_copy(update={
            "cost_cents": 500,
            "unit_price_cents": 750,
            "cost_source_url": "https://supplier.example.com/x",
        })
        for li in parsed.line_items
    ]
    priced = parsed.model_copy(update={
        "line_items": priced_lines,
    }).with_status(QuoteStatus.PRICED)

    assert priced.status == QuoteStatus.PRICED
    # Tax math now produces a real value.
    assert priced.tax_cents > 0


def test_ingest_does_not_mutate_input_contract():
    import copy
    c = _minimal_contract()
    before = copy.deepcopy(c)
    _ = ingest_email_contract(c, tax_resolver=_tax_825)
    assert c == before
