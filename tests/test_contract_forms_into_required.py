"""Contract-extracted forms must flow into the generated package.

P0 incident 2026-05-04 (Mike, "for the 10thousanth time"): operator
uploaded a CalVet email contract that listed all 10 required forms,
auto-extract OCR'd it correctly and saved `requirements_json` with the
full `forms_required` list, but the generate-package path only read
`agency_cfg.required_forms` — so contract-listed forms beyond the
agency default were silently dropped. Mike saw 4 forms in the package
when the buyer asked for 10.

These tests pin the structural fix: `extract_contract_forms()` accepts
both a JSON string and a parsed dict, validates against the canonical
form_id allowlist, and never raises. The generate path unions its
output into `_req_forms` so the contract becomes load-bearing.
"""
from __future__ import annotations

import json

from src.api.modules.routes_rfq_gen import (
    extract_contract_forms, _CANONICAL_FORM_IDS,
)


class TestExtractContractForms:
    def test_empty_input_returns_empty_set(self):
        assert extract_contract_forms(None) == set()
        assert extract_contract_forms("") == set()
        assert extract_contract_forms({}) == set()

    def test_canonical_ids_pass_through(self):
        """The recurring CalVet shape: contract lists the full set."""
        payload = {"forms_required": [
            "std204", "std205", "dvbe843", "darfur_act", "cv012_cuf",
            "calrecycle74", "bidder_decl", "std1000", "sellers_permit",
        ]}
        out = extract_contract_forms(payload)
        assert out == {
            "std204", "std205", "dvbe843", "darfur_act", "cv012_cuf",
            "calrecycle74", "bidder_decl", "std1000", "sellers_permit",
        }

    def test_accepts_json_string(self):
        """requirements_json is persisted as JSON text in r['requirements_json'];
        the helper must handle both shapes so callers don't need to pre-parse."""
        payload = json.dumps({"forms_required": ["std204", "dvbe843"]})
        assert extract_contract_forms(payload) == {"std204", "dvbe843"}

    def test_unknown_form_ids_dropped(self):
        """OCR noise / future LLM hallucinations can't smuggle in unsupported
        forms that would 500 the generation path. Allowlist is authoritative."""
        payload = {"forms_required": [
            "std204",          # canonical → keeps
            "std999_fake",     # not in allowlist → drops
            "<script>",        # garbage → drops
            "",                # empty → drops
        ]}
        assert extract_contract_forms(payload) == {"std204"}

    def test_case_insensitive(self):
        """Operator-typed contracts often vary case ('STD204' / 'Std204')."""
        payload = {"forms_required": ["STD204", "DVBE843", "Cv012_Cuf"]}
        assert extract_contract_forms(payload) == {"std204", "dvbe843", "cv012_cuf"}

    def test_whitespace_stripped(self):
        payload = {"forms_required": ["  std204  ", "\tdvbe843\n"]}
        assert extract_contract_forms(payload) == {"std204", "dvbe843"}

    def test_malformed_json_returns_empty(self):
        """Never raise — a corrupt requirements_json should fall through to
        agency defaults, not crash the generate-package route."""
        assert extract_contract_forms("{not valid json}") == set()
        assert extract_contract_forms("[]") == set()      # wrong shape
        assert extract_contract_forms('"string"') == set()

    def test_missing_forms_required_key_returns_empty(self):
        """Old extractor records may have requirements_json without forms_required."""
        payload = {"due_date": "2026-05-10", "buyer_name": "Stefanie"}
        assert extract_contract_forms(payload) == set()

    def test_forms_required_wrong_type_returns_empty(self):
        """Defensive: forms_required must be a list. A dict or string drops."""
        assert extract_contract_forms({"forms_required": "std204,std205"}) == set()
        assert extract_contract_forms({"forms_required": {"std204": True}}) == set()

    def test_canonical_set_covers_agency_config_shapes(self):
        """Regression guard: every form_id used as a key in
        DEFAULT_AGENCY_CONFIGS[*]['required_forms'] must be in the
        canonical allowlist. Otherwise a contract that legitimately
        names a form the agency requires would be dropped here."""
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        used_in_configs = set()
        for cfg in DEFAULT_AGENCY_CONFIGS.values():
            for f in cfg.get("required_forms", []):
                used_in_configs.add(str(f).lower())
        # bidpkg is internal-only, but every other form_id in any agency
        # config must be allowlisted so contract uploads can union them.
        missing = used_in_configs - set(_CANONICAL_FORM_IDS)
        assert not missing, (
            f"Agency configs reference form_ids not in _CANONICAL_FORM_IDS: "
            f"{sorted(missing)}. Add them or contract uploads will drop them."
        )
