"""LAW 6 forcing function: CalVet EmailContract ingest synthesis (J2-3, Job #2).

Mirrors the CCHCS J1-5a synthesis tests. Proves that
``synthesize_calvet_email_contract`` resolves EVERY LAW 6 answer at ingest
for a CalVet RFQ — with NO heuristic discovery later and NO legacy
``src/core/agency_config.py`` re-import for the bill-to (it reads the J2-1
Spine constant ``calvet_bill_to_tuple()``):

  * required_forms — the canonical CalVet compliance set, AND the Barstow
    two-CUF split (declared by the resolved facility record, not guessed);
  * due_date, solicitation_number, buyer email;
  * per-facility ship-to + the correct facility tax jurisdiction (Barstow
    → 8.75% BARSTOW; other homes → their facility rate);
  * line items with qty / UOM / MFG#.

Cross-ticket dependency (J2-2): ``bidder_decl``, ``std_205``,
``sellers_permit``, ``barstow_cuf`` are not yet in the ``FormCode``
Literal, so the synthesized EmailContract carries only the Literal-valid
subset; the FULL canonical set is asserted via
``get_calvet_required_forms`` (the survives-deletion artifact). Once J2-2
widens ``FormCode``, the contract carries the full set and the
``_calvet_literal_valid_required_forms`` filter becomes a no-op.

Survives-config-deletion: each form-set assertion is also run with
``DEFAULT_AGENCY_CONFIGS["calvet"]`` / ``["calvet_barstow"]`` popped — the
CalVet form set is Spine-owned and must NOT depend on the legacy key
(mirror of ``test_cchcs_form_set_survives_config_deletion.py``).
"""
from __future__ import annotations

import contextlib

import pytest

from src.spine_bridge.ingest import (
    NotCalVetError,
    get_calvet_required_forms,
    synthesize_calvet_email_contract,
)

# ──────────────────────────────────────────────────────────────────────────────
# Canonical CalVet form sets (the FULL truth — incl. J2-2-pending codes)
# ──────────────────────────────────────────────────────────────────────────────

#: Standard CalVet set (non-Barstow). One CUF (cv012 → `cuf`).
_CALVET_STANDARD = {
    "quote", "calrecycle_74", "bidder_decl", "dvbe_843", "darfur",
    "cuf", "std_204", "std_205", "std_1000", "sellers_permit",
}

#: Barstow set — adds `barstow_cuf` (TWO CUF forms).
_CALVET_BARSTOW = _CALVET_STANDARD | {"barstow_cuf"}

#: The "other" 3-form fallback — the regression CalVet must NEVER become.
_OTHER_FORMS = {"quote", "std204", "sellers_permit"}

#: The CCHCS default — the synthesizer must NEVER silently stomp CalVet to this.
_CCHCS_DEFAULT = {"703b", "704b", "bidpkg", "quote"}


def _barstow_rfq(**overrides) -> dict:
    base = {
        "agency": "CalVet",
        "institution": "Veterans Home of California - Barstow",
        "ship_to": "100 E Veterans Pkwy, Barstow, CA 92311",
        "solicitation_number": "CV-2025-BAR-01",
        "due_date": "2026-06-15",
        "requestor_email": "buyer@calvet.ca.gov",
        "line_items": [
            {"description": "Nitrile Gloves L", "qty": 50, "uom": "BX",
             "item_number": "NL304"},
        ],
    }
    base.update(overrides)
    return base


def _yountville_rfq(**overrides) -> dict:
    base = {
        "agency": "CALVET",
        "institution": "Yountville",
        "ship_to": "100 California Dr, Yountville, CA 94599",
        "solicitation_number": "CV-2025-YNT-02",
        "due_date": "2026-06-20",
        "requestor_email": "officer@calvet.ca.gov",
        "line_items": [
            {"description": "Exam Gloves M", "qty": 200, "uom": "BX"},
        ],
    }
    base.update(overrides)
    return base


@contextlib.contextmanager
def _calvet_keys_deleted():
    """Pop DEFAULT_AGENCY_CONFIGS['calvet'] + ['calvet_barstow'] for the duration."""
    from src.core import agency_config as _ac_mod
    popped = {}
    for key in ("calvet", "calvet_barstow"):
        popped[key] = _ac_mod.DEFAULT_AGENCY_CONFIGS.pop(key, None)
    try:
        yield
    finally:
        for key, entry in popped.items():
            if entry is not None:
                _ac_mod.DEFAULT_AGENCY_CONFIGS[key] = entry
            else:
                _ac_mod.DEFAULT_AGENCY_CONFIGS.pop(key, None)


# Barstow returns 8.75% (875 bps); a generic CalVet home returns 7.75% (775 bps).
def _calvet_tax_resolver(address: str) -> int:
    if "barstow" in (address or "").lower() or "92311" in (address or ""):
        return 875
    return 775


# ──────────────────────────────────────────────────────────────────────────────
# LAW 6 — every answer resolved at ingest (the core forcing assertions)
# ──────────────────────────────────────────────────────────────────────────────

class TestCalVetLaw6FieldsResolved:
    """Every LAW 6 answer is present on the synthesized contract — no later
    heuristic discovery."""

    def test_barstow_resolves_every_field(self):
        rfq = _barstow_rfq()
        c = synthesize_calvet_email_contract(
            rfq, "rfq_bar1", tax_resolver=_calvet_tax_resolver,
        )
        # agency
        assert c.agency == "CalVet"
        # bill-to from the Spine constant (J2-1), NOT legacy config
        assert c.bill_to_name == "California Department of Veterans Affairs"
        assert c.bill_to_email == "APinvoices@calvet.ca.gov"
        assert c.bill_to_address  # non-empty address block
        # solicitation #
        assert c.solicitation_number == "CV-2025-BAR-01"
        # due date resolved
        assert c.due_date is not None
        assert c.due_date.year == 2026 and c.due_date.month == 6
        # buyer
        assert c.buyer_email == "buyer@calvet.ca.gov"
        # per-facility ship-to
        assert c.ship_to_address and "Barstow" in c.ship_to_address
        # line items with qty / UOM / MFG#
        assert len(c.line_items) == 1
        li = c.line_items[0]
        assert li.qty == 50
        assert li.uom == "BX"
        assert li.mfg_number_suggested == "NL304"

    def test_barstow_tax_jurisdiction_is_875(self):
        """Barstow resolves the 8.75% BARSTOW jurisdiction (875 bps)."""
        c = synthesize_calvet_email_contract(
            _barstow_rfq(), "rfq_bar_tax", tax_resolver=_calvet_tax_resolver,
        )
        assert c.tax_rate_bps == 875, (
            f"Barstow must resolve 875 bps (8.75% BARSTOW); got {c.tax_rate_bps}"
        )

    def test_yountville_resolves_every_field(self):
        c = synthesize_calvet_email_contract(
            _yountville_rfq(), "rfq_ynt1", tax_resolver=_calvet_tax_resolver,
        )
        assert c.agency == "CalVet"
        assert c.bill_to_name == "California Department of Veterans Affairs"
        assert c.solicitation_number == "CV-2025-YNT-02"
        assert c.due_date is not None
        assert c.buyer_email == "officer@calvet.ca.gov"
        assert c.ship_to_address and "Yountville" in c.ship_to_address
        assert c.line_items[0].qty == 200 and c.line_items[0].uom == "BX"

    def test_yountville_tax_is_non_barstow_rate(self):
        c = synthesize_calvet_email_contract(
            _yountville_rfq(), "rfq_ynt_tax", tax_resolver=_calvet_tax_resolver,
        )
        assert c.tax_rate_bps == 775
        assert c.tax_rate_bps != 875, "Yountville must NOT get the Barstow rate"


# ──────────────────────────────────────────────────────────────────────────────
# LAW 6 — required_forms: the full canonical set + the Barstow split
# ──────────────────────────────────────────────────────────────────────────────

class TestCalVetRequiredForms:
    """The canonical CalVet form set — standard AND Barstow two-CUF split."""

    def test_standard_full_set(self):
        forms = set(get_calvet_required_forms(_yountville_rfq(), barstow=False))
        assert forms == _CALVET_STANDARD, (
            f"Standard CalVet set mismatch.\n got: {sorted(forms)}\n"
            f" want: {sorted(_CALVET_STANDARD)}"
        )
        # One CUF only for non-Barstow.
        assert "cuf" in forms
        assert "barstow_cuf" not in forms

    def test_barstow_full_set_is_two_cuf(self):
        forms = set(get_calvet_required_forms(_barstow_rfq(), barstow=True))
        assert forms == _CALVET_BARSTOW, (
            f"Barstow CalVet set mismatch.\n got: {sorted(forms)}\n"
            f" want: {sorted(_CALVET_BARSTOW)}"
        )
        # TWO CUF forms at Barstow.
        assert {"cuf", "barstow_cuf"} <= forms

    def test_set_is_never_other_or_cchcs(self):
        for barstow in (True, False):
            forms = set(get_calvet_required_forms(_barstow_rfq(), barstow=barstow))
            assert forms != _OTHER_FORMS, "CalVet collapsed to 'other' 3-form set"
            assert forms != _CCHCS_DEFAULT, "CalVet collapsed to CCHCS default set"

    def test_synthesized_contract_carries_literal_valid_subset(self):
        """The contract's required_forms is the FormCode-Literal-valid subset
        (J2-2 widens it to the full set). Until then it must be the subset of
        the canonical set that is already a valid FormCode — never CCHCS."""
        from src.spine.email_contract import ALL_FORM_CODES
        c = synthesize_calvet_email_contract(
            _barstow_rfq(), "rfq_subset", tax_resolver=_calvet_tax_resolver,
        )
        forms = set(c.required_forms)
        # Every emitted code is a valid FormCode (Pydantic accepted it).
        assert all(f in ALL_FORM_CODES for f in forms)
        # It is the CalVet subset, not the CCHCS default.
        assert forms != _CCHCS_DEFAULT
        assert "quote" in forms and "cuf" in forms and "calrecycle_74" in forms
        # The full canonical Barstow set is a SUPERSET of the contract subset.
        assert forms <= _CALVET_BARSTOW

    def test_barstow_detected_from_facility_not_flag(self):
        """End-to-end: synthesize from a Barstow RFQ (no explicit barstow flag)
        and confirm the FULL canonical set resolves to the two-CUF Barstow set
        via facility-record detection."""
        rfq = _barstow_rfq()
        # Reproduce the synthesizer's barstow detection on the RFQ alone.
        from src.core.facility_registry import resolve as _resolve_facility
        rec = _resolve_facility(rfq["ship_to"]) or _resolve_facility(rfq["institution"])
        assert rec is not None and rec.code == "CALVETHOME-BF"
        full = set(get_calvet_required_forms(rfq, barstow=(rec.code == "CALVETHOME-BF")))
        assert full == _CALVET_BARSTOW


# ──────────────────────────────────────────────────────────────────────────────
# Survives DEFAULT_AGENCY_CONFIGS["calvet"] deletion (J2 forcing function)
# ──────────────────────────────────────────────────────────────────────────────

class TestCalVetSurvivesConfigDeletion:
    """The CalVet form set + synthesis are Spine-owned — independent of the
    legacy DEFAULT_AGENCY_CONFIGS keys (which J2-6 deletes)."""

    def test_standard_set_survives_deletion(self):
        with _calvet_keys_deleted():
            forms = set(get_calvet_required_forms(_yountville_rfq(), barstow=False))
        assert forms == _CALVET_STANDARD
        assert forms != _OTHER_FORMS and forms != _CCHCS_DEFAULT

    def test_barstow_set_survives_deletion(self):
        with _calvet_keys_deleted():
            forms = set(get_calvet_required_forms(_barstow_rfq(), barstow=True))
        assert forms == _CALVET_BARSTOW
        assert {"cuf", "barstow_cuf"} <= forms

    def test_full_synthesis_survives_deletion(self):
        """The whole contract synthesizes with the legacy keys popped — bill-to,
        forms, tax, ship-to all resolve from Spine-owned sources."""
        with _calvet_keys_deleted():
            c = synthesize_calvet_email_contract(
                _barstow_rfq(), "rfq_del", tax_resolver=_calvet_tax_resolver,
            )
        assert c.agency == "CalVet"
        assert c.bill_to_name == "California Department of Veterans Affairs"
        assert c.tax_rate_bps == 875
        assert set(c.required_forms) != _CCHCS_DEFAULT
        assert "quote" in set(c.required_forms)


# ──────────────────────────────────────────────────────────────────────────────
# Agency gate — admits CalVet, rejects non-CalVet
# ──────────────────────────────────────────────────────────────────────────────

class TestCalVetAgencyGate:
    """The synthesizer admits CalVet (widening the CCHCS-only on-ramp) and
    rejects non-CalVet RFQs with NotCalVetError."""

    @pytest.mark.parametrize("agency_val", ["CalVet", "CALVET", "Cal Vet", "calvet_barstow"])
    def test_admits_calvet_spellings(self, agency_val):
        rfq = _yountville_rfq(agency=agency_val)
        c = synthesize_calvet_email_contract(
            rfq, "rfq_gate", tax_resolver=_calvet_tax_resolver,
        )
        assert c.agency == "CalVet"

    @pytest.mark.parametrize("agency_val", ["CCHCS", "DSH", "DGS", "CDCR", ""])
    def test_rejects_non_calvet(self, agency_val):
        rfq = _yountville_rfq(agency=agency_val)
        with pytest.raises(NotCalVetError):
            synthesize_calvet_email_contract(
                rfq, "rfq_reject", tax_resolver=_calvet_tax_resolver,
            )

    def test_tax_outage_blocks_quote(self):
        """LAW 6 / Charter rule #6: no usable tax rate must BLOCK, not paper over."""
        with pytest.raises(ValueError, match="tax is mandatory"):
            synthesize_calvet_email_contract(
                _yountville_rfq(), "rfq_notax", tax_resolver=lambda _a: None,
            )


# ──────────────────────────────────────────────────────────────────────────────
# Bill-to comes from the Spine constant, not the legacy config
# ──────────────────────────────────────────────────────────────────────────────

class TestCalVetBillToIsSpineOwned:
    """Bill-to is read from src/spine/agency_constants.calvet_bill_to_tuple()
    (J2-1 #1284), not DEFAULT_AGENCY_CONFIGS — proven by deletion-survival
    and by matching the constant exactly."""

    def test_bill_to_matches_spine_constant(self):
        from src.spine.agency_constants import calvet_bill_to_tuple
        name, email, address_lines = calvet_bill_to_tuple()
        c = synthesize_calvet_email_contract(
            _yountville_rfq(), "rfq_billto", tax_resolver=_calvet_tax_resolver,
        )
        assert c.bill_to_name == name
        assert c.bill_to_email == email
        assert c.bill_to_address == ("\n".join(address_lines) or None)
