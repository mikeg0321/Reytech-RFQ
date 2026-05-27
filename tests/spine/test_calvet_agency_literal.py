"""Pin: CalVet admitted as a valid agency on Spine Quote + EmailContract.

Chrome MCP audit 2026-05-27 / G12: forward declaration of CalVet as
the next-after-Job-#1 agency. This PR doesn't migrate the legacy
CalVet path — it just expands the agency literal so a CalVet-tagged
EmailContract and Quote can be constructed + persisted through the
Spine substrate. The full migration is gated on Job #1 (CCHCS
legacy deletion) completing first.

Tests pin:
  1. EmailContract accepts agency='CalVet'.
  2. Quote accepts agency='CalVet'.
  3. Both literals are kept IN SYNC — a future PR that widens one
     without the other gets caught.
  4. Unknown agency values still rejected (no silent any-agency).
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest


def test_email_contract_accepts_calvet():
    from src.spine.email_contract import EmailContract, ContractLineItem
    c = EmailContract(
        contract_id="c-calvet-1",
        agency="CalVet",
        facility="CALVET-HQ",
        solicitation_number="CV-SOL-1",
        line_items=[ContractLineItem(
            line_no=1, description="x", qty=1, uom="EA",
        )],
    )
    assert c.agency == "CalVet"


def test_quote_accepts_calvet():
    from src.spine.model import Quote, LineItem
    q = Quote(
        quote_id="q-calvet-1",
        agency="CalVet",
        facility="CALVET-HQ",
        solicitation_number="CV-SOL-1",
        tax_rate_bps=775,
        line_items=[LineItem(
            line_no=1, description="x", mfg_number="M1",
            qty=1, uom="EA",
            cost_cents=0, unit_price_cents=0,
        )],
        status="parsed",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    assert q.agency == "CalVet"


def test_literals_stay_in_sync():
    """If a future PR widens one literal without the other (e.g.,
    adds 'DSH' to model.py but forgets email_contract.py), this test
    catches it. Both must have identical agency sets."""
    from src.spine.model import Quote
    from src.spine.email_contract import EmailContract
    from typing import get_args, get_type_hints
    q_agency = set(get_args(get_type_hints(Quote)["agency"]))
    c_agency = set(get_args(get_type_hints(EmailContract)["agency"]))
    assert q_agency == c_agency, (
        f"Spine agency literals drifted: Quote={q_agency} "
        f"EmailContract={c_agency}. Update both together."
    )


def test_unknown_agency_still_rejected():
    from src.spine.email_contract import EmailContract, ContractLineItem
    with pytest.raises(Exception):  # pydantic ValidationError
        EmailContract(
            contract_id="c-unknown-1",
            agency="UnknownAgency",  # not in literal
            facility="X",
            solicitation_number="X",
            line_items=[ContractLineItem(
                line_no=1, description="x", qty=1, uom="EA",
            )],
        )
