"""Golden acceptance — the `QuoteContract` assembled from Mike's real
PC `f81c4e9b` (CalVet Barstow) must resolve to canonical Barstow,
NOT Calipatria, NOT CDCR.

This is the test the product-engineer review asked for: not a
regression net for the next manifestation, but the ACCEPTANCE GATE
that proves the contract is the single source of truth. Every
renderer-migration PR must keep this green.

## Contract under test

`src.core.quote_contract.assemble_from_rfq(rfq)` — the ONLY assembler
that feeds PDF renderers. If this returns the wrong facility, every
downstream generator will too. That's the point of the contract.

## Why contract-level, not PDF-level

A PDF-text assertion catches the symptom (wrong address on the
rendered page). A contract-level assertion catches the CAUSE (wrong
facility in the contract) before any renderer has a chance to
honor or ignore it. Once the contract is right, renderers reading
from `contract.facility` / `contract.agency_code` / `contract.
ship_to_address_lines` cannot diverge.

Golden PDF-text tests live alongside the renderer migration PRs —
each renderer that moves to `fn(contract, output_path)` gets a
pair assertion at the end of its render that the output contains
`contract.ship_to_name`. This file pins the contract layer itself.

## Two-input test

Tests the real prod failing case (`f81c4e9b` CalVet Barstow) and a
CSP-SAC case so the structural contract covers more than one agency.
Prevents a migration from accidentally honoring CalVet but breaking
CDCR (or vice versa).
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core.quote_contract import (
    assemble_from_rfq,
    LineItem,
    QuoteContract,
)


# ── The real prod incident: PC f81c4e9b / RFQ 8a1dcf77 ───────────


def _barstow_rfq_fixture() -> dict:
    """Shape-matches what the app would produce after Convert for
    Mike's CalVet Barstow PC. Field names per
    `generate_quote_from_rfq`'s own reader in src/forms/quote_generator.py:1796."""
    return {
        "id": "rfq_8a1dcf77",
        "source_pc_id": "pc_f81c4e9b",
        "agency_name": "California Department of Veterans Affairs",
        "department": "CalVet",
        "requestor_name": "Tarrna Solis",
        "delivery_location": "",  # often blank in this agency's RFQs
        "ship_to": "",
        "ship_to_name": "Veterans Home of California - Barstow",
        "institution_name": "California Department of Veterans Affairs - Barstow Division, Skilled Nursing Unit",
        "items": [
            {
                "description": "Stanley RoamAlert Wrist Strap: 6 in Overall Lg, Gray",
                "quantity": 1,
                "unit_price": 33.74,
                "mfg_number": "WL085P",
                "uom": "EA",
            },
        ],
    }


def _csp_sac_rfq_fixture() -> dict:
    """Audit W sibling case — CSP-SAC at 100 Prison Road, not FSP's
    300. Guards against a migration that honors CalVet but breaks
    the CDCR Folsom-ambiguity fix."""
    return {
        "id": "rfq_csp_sac_golden",
        "source_pc_id": "pc_csp_sac_golden",
        "agency_name": "California Department of Corrections and Rehabilitation",
        "department": "CDCR",
        "delivery_location": "CSP-SAC (CA State Prison Sacramento)",
        "ship_to_name": "CSP Sacramento - New Folsom",
        "institution_name": "CSP-SAC",
        "items": [
            {
                "description": "Medical grade supplies",
                "quantity": 2,
                "unit_price": 50.00,
                "mfg_number": "W14100",
                "uom": "EA",
            },
        ],
    }


# ── CalVet Barstow — the exact f81c4e9b regression ───────────────


def test_barstow_rfq_resolves_to_calvethome_bf():
    c = assemble_from_rfq(_barstow_rfq_fixture())
    assert c.facility is not None, (
        "Contract failed to resolve any facility for the Barstow RFQ. "
        "This reproduces the f81c4e9b incident — operator-review "
        "required state was silently shipped as empty."
    )
    assert c.facility.code == "CALVETHOME-BF", (
        f"Barstow RFQ resolved to {c.facility.code!r} ({c.facility.canonical_name!r}) "
        f"— expected CALVETHOME-BF. Regression of the f81c4e9b Calipatria "
        f"bug: some resolver priority is matching the wrong facility."
    )


def test_barstow_contract_has_canonical_address_not_calipatria():
    c = assemble_from_rfq(_barstow_rfq_fixture())
    lines = c.ship_to_address_lines
    joined = " ".join(lines)
    assert "100 E Veterans Pkwy" in joined
    assert "Barstow, CA 92311" in joined
    assert "Calipatria" not in joined, (
        f"Ship-to leaked Calipatria text: {joined!r}. Exact f81c4e9b "
        f"incident signature — fail immediately."
    )
    assert "7018 Blair Rd" not in joined, (
        "Ship-to leaked Calipatria street address (7018 Blair Rd)."
    )


def test_barstow_contract_carries_calvet_agency_not_cdcr():
    c = assemble_from_rfq(_barstow_rfq_fixture())
    assert c.agency_code == "CalVet", (
        f"agency_code={c.agency_code!r} — expected CalVet. The "
        f"f81c4e9b PDF shipped with 'Dept. of Corrections and "
        f"Rehabilitation' — that string must never appear for a "
        f"Barstow contract."
    )
    assert "Veterans Affairs" in c.agency_full
    assert "Corrections" not in c.agency_full, (
        "agency_full leaked a CDCR string for a CalVet contract."
    )


def test_barstow_contract_tax_rate_from_canonical_barstow():
    """Barstow has an operator-verified 8.75% override in the
    canonical registry (district add-on the CDTFA API misses).
    Contract must pick it up."""
    c = assemble_from_rfq(_barstow_rfq_fixture())
    assert c.tax_rate_bps == 875, (
        f"Expected 8.75% (875 bps) for Barstow; got {c.tax_rate_bps} bps. "
        f"The operator-verified rate override isn't flowing through "
        f"the contract."
    )
    assert c.tax_jurisdiction == "BARSTOW"
    assert c.tax_source == "facility_registry"
    assert c.tax_validated is True


def test_barstow_contract_prices_come_from_operator_not_default():
    """Mike's f81c4e9b had $2,252.21 medical-grade state restored by
    hand; the quote PDF shipped with $329.92 stale Amazon-retail. The
    contract must carry what the operator set — the only way a stale
    price can reach the PDF now is if the RFQ row itself has stale
    values, which is a different (and visible) bug."""
    c = assemble_from_rfq(_barstow_rfq_fixture())
    assert len(c.line_items) == 1
    li = c.line_items[0]
    assert li.unit_price_cents == 3374, (
        f"Line unit price became {li.unit_price_cents}¢ — operator "
        f"set $33.74. The assembler lost precision or swapped a value."
    )
    assert li.quantity == 1
    assert li.mfg_number == "WL085P"
    # Subtotal math in integer cents — no float drift
    assert c.subtotal_cents == 3374
    # Tax @ 8.75% on $33.74 = $2.95 (2953¢ pre-round); banker's round → 295¢
    assert c.tax_cents == 295
    assert c.total_cents == 3374 + 295


# ── Sibling coverage: CSP-SAC audit-W case ───────────────────────


def test_csp_sac_rfq_resolves_to_100_prison_road_not_300():
    c = assemble_from_rfq(_csp_sac_rfq_fixture())
    assert c.facility is not None
    assert c.facility.code == "CSP-SAC"
    lines = c.ship_to_address_lines
    assert lines[0] == "100 Prison Road", (
        f"CSP-SAC contract shipped with address {lines!r} — "
        f"audit W regression: should be 100 Prison Road, not 300."
    )
    assert "300 Prison Road" not in " ".join(lines), (
        "FSP's 300 Prison Road leaked into CSP-SAC contract."
    )


def test_csp_sac_contract_agency_is_cdcr():
    c = assemble_from_rfq(_csp_sac_rfq_fixture())
    assert c.agency_code == "CDCR"
    assert "Corrections" in c.agency_full


# ── Empty-input safety ───────────────────────────────────────────


def test_empty_rfq_returns_contract_with_facility_none():
    """Missing everything → empty contract, NOT a default-guess.
    Renderers should see `facility is None` and refuse to emit a PDF
    (that's what the operator-review gate is for)."""
    c = assemble_from_rfq({})
    assert c.facility is None
    assert c.agency_code == ""
    assert c.agency_full == ""
    assert c.line_items == ()
    assert c.subtotal_cents == 0
    assert c.ship_to_address_lines == ()


def test_unresolvable_ship_to_keeps_raw_for_operator_review():
    """Some wacky string the registry can't resolve → facility=None,
    but ship_to_raw captures what the operator will need to fix.
    Renderer should flag + prompt, not guess."""
    c = assemble_from_rfq({
        "id": "rfq_unresolvable",
        "ship_to_name": "some handwritten address that's not in registry",
    })
    assert c.facility is None
    assert "some handwritten address" in c.ship_to_raw


# ── Structural invariants (frozen-contract guarantees) ───────────


def test_contract_is_frozen():
    """Renderers cannot mutate contract fields mid-render."""
    c = assemble_from_rfq(_barstow_rfq_fixture())
    with pytest.raises((AttributeError, Exception)):
        c.tax_rate_bps = 0  # type: ignore[misc]
    with pytest.raises((AttributeError, Exception)):
        c.facility = None  # type: ignore[misc]


def test_line_items_is_immutable_tuple():
    c = assemble_from_rfq(_barstow_rfq_fixture())
    assert isinstance(c.line_items, tuple), (
        "line_items must be a tuple so renderers can't accidentally "
        ".append to it and corrupt a shared contract."
    )
