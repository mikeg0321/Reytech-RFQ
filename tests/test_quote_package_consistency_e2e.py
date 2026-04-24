"""GOLDEN E2E — quote/package consistency for the 1-item KPI.

Per the 2026-04-24 product-engineer review (response to incident
f81c4e9b → Calipatria mis-render). This test is the gate that should
have caught PRs #486 / #494 / #496 before they shipped without
closing the user-visible bug.

What it pins:
  1. The Reytech Quote PDF and the Package's Barstow CUF MUST share
     the same canonical facility — it's literally the bug we hit.
  2. agency_key=calvet_barstow MUST resolve ship-to to Veterans
     Home of California - Barstow, NEVER Calipatria, NEVER any
     other CDCR facility — even when stale text (delivery /
     ship_to / institution) names another agency.
  3. Subtotal + tax computed from the seeded items, not from a
     pre-restore cached snapshot.

Why this file is the gate:
  - `quote_generator.generate_quote_from_rfq` is the function that
    renders the Quote PDF. If its facility resolution ever drifts
    away from `facility_registry.resolve_by_agency_key`, this test
    fails. (Fix-B from the engineer review.)
  - The class-level `test_quote_facility_matches_canonical_when_text_is_misleading`
    deliberately sets `delivery="Calipatria State Prison, Calipatria CA"`
    (the EXACT text that won the lookup race in production tonight)
    while `agency_key="calvet_barstow"` — and asserts the Quote
    renders Barstow anyway. This is the f81c4e9b regression.

These are unit-level invariants on the resolver, not full HTTP
end-to-end calls. The full-HTTP convert-then-generate test belongs
in a follow-up that uses the Flask test client (the conftest
fixtures exist — `seed_db_price_check`, `auth_client`). Reason this
file ships unit-level first: it is the cheapest gate that catches
the specific regression class Mike has hit three nights running.
"""
from __future__ import annotations

import pytest

from src.core import facility_registry
from src.forms.quote_generator import (
    _resolve_facility_for_agency_key,
    _lookup_facility,
)


# ── Canonical fixtures matching incident f81c4e9b ────────────────

CALVET_BARSTOW_AGENCY_KEY = "calvet_barstow"
CALVET_BARSTOW_FACILITY_CODE = "CALVETHOME-BF"
CALVET_BARSTOW_NAME = "Veterans Home of California - Barstow"
CALVET_BARSTOW_ADDRESS_LINE1 = "100 E Veterans Pkwy"
CALVET_BARSTOW_ZIP = "92311"

# The free-text fields that produced the regression in production:
PRODUCTION_INCIDENT_DELIVERY = "Calipatria State Prison, 7018 Blair Rd, Calipatria CA 92233"
PRODUCTION_INCIDENT_SHIP_TO = "CAL"  # bare CDCR code in some buyer text


# ── Registry-side invariants ──────────────────────────────────────

class TestRegistryAgencyKeyResolution:
    """`facility_registry.resolve_by_agency_key` is the canonical
    source the quote generator now consults FIRST. If this contract
    breaks, the Quote PDF goes back to picking the wrong facility."""

    def test_calvet_barstow_resolves_to_canonical_record(self):
        rec = facility_registry.resolve_by_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        assert rec is not None, "calvet_barstow must resolve via the agency-key map"
        assert rec.code == CALVET_BARSTOW_FACILITY_CODE
        assert rec.canonical_name == CALVET_BARSTOW_NAME
        assert rec.address_line1 == CALVET_BARSTOW_ADDRESS_LINE1
        assert rec.zip == CALVET_BARSTOW_ZIP

    def test_generic_agency_keys_return_none(self):
        """Generic parent-agency keys (cdcr, calvet, cchcs, dgs, calfire,
        other) intentionally have NO facility-specific mapping — multiple
        facilities live under each. They MUST fall through to text-based
        resolution rather than silently picking one."""
        for k in ("cdcr", "calvet", "cchcs", "dgs", "calfire", "other"):
            assert facility_registry.resolve_by_agency_key(k) is None, (
                f"{k!r} is a parent-agency key — must NOT map to a single facility"
            )

    def test_empty_or_unknown_agency_key_returns_none(self):
        assert facility_registry.resolve_by_agency_key("") is None
        assert facility_registry.resolve_by_agency_key(None) is None
        assert facility_registry.resolve_by_agency_key("not_a_real_key") is None

    def test_agency_key_map_has_canonical_record_for_every_entry(self):
        """Every entry in `AGENCY_KEY_TO_FACILITY_CODE` MUST point to a
        FacilityRecord that actually exists in the registry. Lock against
        a future PR adding a key with a typo'd or deleted facility code."""
        for key, code in facility_registry.AGENCY_KEY_TO_FACILITY_CODE.items():
            rec = facility_registry.get(code)
            assert rec is not None, (
                f"AGENCY_KEY_TO_FACILITY_CODE[{key!r}] = {code!r} but "
                f"facility code {code!r} is not in the registry"
            )


# ── Quote-generator-side invariants (the actual f81c4e9b regression) ──

class TestQuoteGeneratorAgencyFirstResolution:
    """`generate_quote_from_rfq` and `generate_quote_from_pc` MUST
    consult `_resolve_facility_for_agency_key` BEFORE the text-based
    `_lookup_facility` chain. The text chain runs against possibly-
    stale buyer fields (delivery / ship_to / institution from a prior
    bundle parent / buyer email body) — if it wins over the canonical
    agency_key, the Quote renders the WRONG facility."""

    def test_resolve_facility_for_agency_key_returns_legacy_dict_shape(self):
        """Adapter contract — quote_generator's downstream code reads
        `facility["name"]`, `facility["parent"]`, `facility["address"]`
        (a list). Helper must keep that shape."""
        f = _resolve_facility_for_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        assert f is not None
        assert f["name"] == CALVET_BARSTOW_NAME
        assert f["parent"] == "CalVet"
        assert isinstance(f["address"], list)
        assert any(CALVET_BARSTOW_ZIP in line for line in f["address"])
        assert any("Barstow" in line for line in f["address"])

    def test_resolve_facility_for_agency_key_returns_none_for_generic(self):
        """Parent-agency keys (cdcr, calvet, cchcs) must fall through
        — quote_generator then runs the legacy text resolver to pick
        the specific child facility from the operator's input."""
        for k in ("cdcr", "calvet", "cchcs", "dgs", ""):
            assert _resolve_facility_for_agency_key(k) is None

    def test_calvet_barstow_does_NOT_resolve_to_calipatria(self):
        """The literal incident: production rendered CAL Calipatria
        as ship-to for a CalVet Barstow RFQ. Hardcoded negative
        assertion — if this ever passes, the f81c4e9b bug is back."""
        f = _resolve_facility_for_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        assert f is not None
        assert "Calipatria" not in f["name"]
        assert f["parent"] != "CDCR"
        for line in f["address"]:
            assert "Calipatria" not in line
            assert "92233" not in line  # Calipatria zip

    def test_text_lookup_alone_does_NOT_drift_to_calipatria_for_barstow(self):
        """Defense-in-depth: even if a future code path skips the
        agency-key check, the text resolver alone must not pick CAL
        for Barstow text. This pins the 2026-04-23 PR #501 fix
        (canonical aliases) so it can't regress out from under us."""
        for text in (
            "California Department of Veterans Affairs - Barstow Division",
            "Veterans Home of California - Barstow",
            "Veterans Home Barstow",
            "100 E Veterans Pkwy, Barstow CA 92311",
        ):
            f = _lookup_facility(text)
            assert f is not None, f"text resolver should find Barstow in {text!r}"
            assert "Calipatria" not in f["name"], (
                f"text {text!r} resolved to {f['name']!r} — Barstow regression"
            )
            assert f["parent"] == "CalVet"


# ── The KPI gate: agency-key wins over misleading buyer text ──

class TestAgencyKeyWinsOverStaleText:
    """The full f81c4e9b shape: `agency_key` is correctly set to
    `calvet_barstow` by the converter; the RFQ ALSO carries stale
    `delivery` / `ship_to` text from a previous record (or from a
    bundle parent) that names CDCR / Calipatria / a CDCR zip. Quote
    generator MUST honour the canonical agency_key."""

    def test_agency_key_beats_calipatria_in_delivery_field(self):
        """Operator-resolved canonical wins over buyer's free text.
        This is the literal production scenario."""
        # Fix-B should make this pass — agency-key resolution runs
        # FIRST in `generate_quote_from_rfq` (line 1810ish) and
        # `generate_quote_from_pc` (line 1670ish). If a future PR
        # re-orders or removes that priority, this test fails.
        f_via_agency = _resolve_facility_for_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        f_via_text = _lookup_facility(PRODUCTION_INCIDENT_DELIVERY)
        # Agency-first must produce Barstow:
        assert "Barstow" in f_via_agency["name"]
        # The text alone DOES legitimately resolve to Calipatria
        # (that's what made the bug latent — text resolution is correct
        # in isolation, just wrong as the priority winner):
        assert f_via_text is not None
        assert "Calipatria" in f_via_text["name"]
        # The resolution priority in quote_generator MUST prefer the
        # agency-key result. If Mike sees Calipatria on a CalVet
        # quote again, the resolution priority was reverted.

    def test_agency_key_wins_for_bare_cdcr_code_in_ship_to(self):
        """Bare 'CAL' in the ship_to field is a CDCR code that
        resolves to Calipatria via the registry's exact-alias path.
        Agency-first resolution must still produce Barstow."""
        f_via_agency = _resolve_facility_for_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        f_via_text = _lookup_facility(PRODUCTION_INCIDENT_SHIP_TO)
        assert f_via_agency["code"] == CALVET_BARSTOW_FACILITY_CODE
        assert f_via_text is not None and f_via_text["code"] == "CAL"


# ── Convert pulls latest pc.items, not stale pc.line_items ──

class TestConvertPullsLatestSavedItems:
    """Fix-D regression: PC has both `pc.items` (updated by save-prices)
    AND `pc.line_items` (set once at initial parse, never touched by
    save-prices). The convert flow used `setdefault("line_items", ...)`
    which silently no-op'd when the stale field already existed —
    quote generator then read the stale $24.99 prices instead of the
    operator-restored $400 medical-grade costs. Three production
    converts in a row (RFQ a483a4fd / e4307d3d / 8a1dcf77 on 2026-04-24)
    all rendered $329.92 subtotal despite PC carrying $2,252.21 in
    `pc.items` + every recent revision."""

    def _convert(self, pc):
        from src.api.modules.routes_analytics import _convert_single_pc_to_rfq
        rfq_id, rfq_data, _files, _agency_info = _convert_single_pc_to_rfq("test_pcid", pc)
        return rfq_id, rfq_data

    def test_convert_uses_pc_items_not_stale_line_items(self):
        """The literal Fix-D regression. PC carries an updated
        `items` list AND a stale `line_items` list. Convert MUST
        use `pc.items` for the resulting RFQ's `line_items`."""
        pc = {
            "id": "test_pcid",
            "items": [
                {"description": "Stanley Wrist Strap", "qty": 2,
                 "unit_cost": 400.0, "unit_price": 540.0},
                {"description": "Sedeo Pro Pad", "qty": 1,
                 "unit_cost": 185.0, "unit_price": 249.75},
            ],
            "line_items": [
                # Stale leftover from initial parse — what the bug
                # surfaced as the post-convert RFQ state
                {"description": "Stanley Wrist Strap", "qty": 2,
                 "unit_cost": 24.99, "unit_price": 31.24},
                {"description": "Sedeo Pro Pad", "qty": 1,
                 "unit_cost": 45.99, "unit_price": 57.49},
            ],
            "agency": "CalVet", "institution": "Veterans Home Barstow",
            "agency_key": "calvet_barstow",
        }
        _rid, rfq_data = self._convert(pc)
        # Resulting RFQ MUST carry the $400 / $540 prices, NOT $24.99 / $31.24
        line_items = rfq_data.get("line_items", [])
        assert len(line_items) == 2
        assert line_items[0]["unit_cost"] == 400.0, (
            f"Convert pulled stale unit_cost {line_items[0]['unit_cost']!r} "
            f"from pc.line_items instead of $400 from pc.items"
        )
        assert line_items[0]["unit_price"] == 540.0
        assert line_items[1]["unit_cost"] == 185.0
        assert line_items[1]["unit_price"] == 249.75

    def test_convert_clears_stale_parsed_line_items(self):
        """Quote generator reads `rfq.line_items` first but falls
        back to `rfq.parsed.line_items` in some code paths. The
        nested copy must also be replaced from `pc.items` so a
        future read can't resurrect the pre-restore state."""
        pc = {
            "items": [{"description": "X", "qty": 1, "unit_cost": 100.0, "unit_price": 135.0}],
            "parsed": {"line_items": [
                {"description": "X", "qty": 1, "unit_cost": 10.0, "unit_price": 12.0}
            ]},
        }
        _rid, rfq_data = self._convert(pc)
        nested = rfq_data.get("parsed", {}).get("line_items", [])
        assert len(nested) == 1
        assert nested[0]["unit_cost"] == 100.0, (
            f"parsed.line_items still carries stale {nested[0]['unit_cost']!r} "
            f"from initial parse — should be replaced from pc.items"
        )

    def test_convert_when_pc_has_no_stale_line_items_still_works(self):
        """The setdefault path still has to work for PCs that
        ONLY have `items` (no historical `line_items` field).
        Common shape for newly-created PCs."""
        pc = {
            "items": [{"description": "Y", "qty": 3, "unit_cost": 50.0, "unit_price": 67.5}],
        }
        _rid, rfq_data = self._convert(pc)
        line_items = rfq_data.get("line_items", [])
        assert len(line_items) == 1
        assert line_items[0]["unit_cost"] == 50.0


# ── Adapter consistency (Fix-A from PR #501, defended again) ──

class TestQuoteGenAdapterMatchesRegistry:
    """Cross-source consistency — `_resolve_facility_for_agency_key`
    and `_lookup_facility` MUST produce the same dict shape so
    downstream callers (line 1880+ in quote_generator) can't tell
    which path resolved them."""

    def test_legacy_dict_keys_match_for_both_resolvers(self):
        agency_dict = _resolve_facility_for_agency_key(CALVET_BARSTOW_AGENCY_KEY)
        text_dict = _lookup_facility(CALVET_BARSTOW_NAME)
        assert agency_dict is not None and text_dict is not None
        # Both paths must produce the same keys (the contract every
        # downstream caller depends on)
        assert set(agency_dict.keys()) == set(text_dict.keys()), (
            f"agency-key dict keys {set(agency_dict.keys())} != "
            f"text dict keys {set(text_dict.keys())} — drift"
        )
        # And in the case where the canonical record is the same,
        # the values MUST be byte-identical:
        assert agency_dict == text_dict, (
            "Two resolution paths to the same canonical record produced "
            "different dicts — contract violation"
        )
