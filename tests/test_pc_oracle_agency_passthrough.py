"""
Guards for the 2026-04-19 Oracle detail-panel fix: the per-item oracle
route (/api/pricecheck/<pcid>/oracle/<idx>) must pass the PC's institution
through to get_pricing() as `department`. Without it, V3 calibration,
V5 institution_profile, and V5.5 buyer_curve never activate — the engine
keys all three off department. Silent-degrade bug: UI shows "Oracle data"
but the rich agency-specific signals are blank.
"""
from unittest.mock import patch


def _fake_pricing_result():
    return {
        "description": "x", "quantity": 1,
        "matched_item": None, "confidence": 0,
        "cost": {}, "market": {"data_points": 0},
        "recommendation": {"quote_price": None, "confidence": "low"},
        "strategies": [], "tiers": [], "competitors": [],
        "cross_sell": [], "sources_used": [],
    }


class TestOracleAgencyPassthrough:
    def test_institution_passed_as_department(self, auth_client, sample_pc):
        """PC with institution='CCHCS' → get_pricing called with department='CCHCS'."""
        from src.api.dashboard import _save_single_pc

        pc = dict(sample_pc)
        pc["id"] = "pc_inst_pass"
        pc["institution"] = "CCHCS"
        pc["items"] = [{"description": "Nitrile gloves L 100ct",
                        "qty": 5, "vendor_cost": 8.50}]
        _save_single_pc(pc["id"], pc)

        with patch("src.core.pricing_oracle_v2.get_pricing",
                   return_value=_fake_pricing_result()) as mock_get:
            resp = auth_client.get("/api/pricecheck/pc_inst_pass/oracle/0")
            assert resp.status_code == 200
            assert resp.get_json().get("ok")
            assert mock_get.called
            kwargs = mock_get.call_args.kwargs
            assert kwargs.get("department") == "CCHCS", \
                f"Expected department=CCHCS, got {kwargs.get('department')!r}"

    def test_agency_fallback_when_no_institution(self, auth_client, sample_pc):
        """If institution missing but agency set, use agency."""
        from src.api.dashboard import _save_single_pc

        pc = dict(sample_pc)
        pc["id"] = "pc_agency_fb"
        pc.pop("institution", None)
        pc["agency"] = "CalVet"
        pc["items"] = [{"description": "item", "qty": 1}]
        _save_single_pc(pc["id"], pc)

        with patch("src.core.pricing_oracle_v2.get_pricing",
                   return_value=_fake_pricing_result()) as mock_get:
            auth_client.get("/api/pricecheck/pc_agency_fb/oracle/0")
            assert mock_get.call_args.kwargs.get("department") == "CalVet"

    def test_response_echoes_agency(self, auth_client, sample_pc):
        """UI can show "analysis for CCHCS" — response includes the agency used."""
        from src.api.dashboard import _save_single_pc

        pc = dict(sample_pc)
        pc["id"] = "pc_echo_inst"
        pc["institution"] = "CDCR"
        pc["items"] = [{"description": "item", "qty": 1}]
        _save_single_pc(pc["id"], pc)

        with patch("src.core.pricing_oracle_v2.get_pricing",
                   return_value=_fake_pricing_result()):
            resp = auth_client.get("/api/pricecheck/pc_echo_inst/oracle/0")
            body = resp.get_json()
            assert body.get("agency") == "CDCR"
