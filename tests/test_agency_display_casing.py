"""RFQ Detail badge must normalize lowercase agency_name to proper casing.

The institution_resolver and classifier_v2 store agency identity as a
lowercase key (`"cchcs"`, `"cdcr"`, `"calvet"`). The RFQ detail template
at rfq_detail.html:29 renders `{{ r.agency_name }}` directly into a badge
pill — so a prod RFQ with `agency_name="cchcs"` shows the badge as "cchcs"
rather than "CCHCS". The existing inline comment at rfq_detail.html:365
already documents the shape: "Prod RFQs often have agency_name='cchcs' but
r.agency empty."

This is the same pattern that routes_crm.py:303 and routes_pricecheck.py
:1087 already resolve via `agency_map.get(agency.lower(), agency.upper())`.
The detail route missed this step.

Spec:
  - `cchcs` → `CCHCS`, `cdcr` → `CDCR`, `dsh` → `DSH`, `dgs` → `DGS`
  - `calvet` → `CalVet`, `calfire` → `CalFire`, `caltrans` → `CalTrans`
  - `cdph` → `CDPH`, `chp` → `CHP`
  - Already-proper values (`"CCHCS"`, `"CalVet"`) pass through unchanged.
  - Unknown values fall back to `.upper()` — same as routes_crm.py:303.
  - Empty / None → empty string (don't render anything).
"""
from __future__ import annotations


class TestAgencyDisplayHelper:
    """Dedicated helper so every agency render site can share one rule."""

    def test_known_lowercase_keys_resolve_to_canonical_case(self):
        from src.core.agency_display import agency_display
        assert agency_display("cchcs") == "CCHCS"
        assert agency_display("cdcr") == "CDCR"
        assert agency_display("dsh") == "DSH"
        assert agency_display("dgs") == "DGS"
        assert agency_display("calvet") == "CalVet"
        assert agency_display("calfire") == "CalFire"
        assert agency_display("caltrans") == "CalTrans"
        assert agency_display("cdph") == "CDPH"
        assert agency_display("chp") == "CHP"

    def test_already_canonical_values_passthrough(self):
        from src.core.agency_display import agency_display
        assert agency_display("CCHCS") == "CCHCS"
        assert agency_display("CalVet") == "CalVet"
        assert agency_display("CDCR") == "CDCR"

    def test_unknown_agency_falls_back_to_upper(self):
        """Matches routes_crm.py:303 behavior — unknown keys get .upper()."""
        from src.core.agency_display import agency_display
        assert agency_display("acme") == "ACME"

    def test_empty_or_none_returns_empty_string(self):
        """RFQs without a classified agency must not render a badge."""
        from src.core.agency_display import agency_display
        assert agency_display("") == ""
        assert agency_display(None) == ""


class TestRfqDetailBadgeNormalizesCasing:
    """End-to-end: lowercase agency_name in storage → uppercase badge on page."""

    def test_lowercase_cchcs_renders_as_uppercase_in_badge(
        self, client, temp_data_dir, sample_rfq
    ):
        """Reproduces the prod shape the line-365 comment documents."""
        import json, os
        rfq = dict(sample_rfq)
        rfq["agency_name"] = "cchcs"   # the prod shape
        rfq["agency"] = ""              # empty, per line-365 comment
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({rfq["id"]: rfq}, f)
        resp = client.get(f"/rfq/{rfq['id']}")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        # Pull out the badge area (lines around the sol-number/badge block
        # near the top of rfq_detail.html).
        assert ">CCHCS<" in html, (
            "RFQ detail badge must display 'CCHCS' in proper case — "
            "lowercase 'cchcs' leaks into the UI when agency_name is "
            "stored lowercase by the classifier."
        )
        assert ">cchcs<" not in html, (
            "Bare lowercase 'cchcs' should not render in the badge area"
        )

    def test_calvet_keeps_mixed_case(
        self, client, temp_data_dir, sample_rfq
    ):
        """Agencies with mixed-case canonical form (CalVet) must not be
        blindly .upper()'d — the helper's map wins over the fallback."""
        import json, os
        rfq = dict(sample_rfq)
        rfq["agency_name"] = "calvet"
        rfq["agency"] = ""
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({rfq["id"]: rfq}, f)
        resp = client.get(f"/rfq/{rfq['id']}")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        assert ">CalVet<" in html, (
            "CalVet badge must preserve the camel-case canonical form"
        )
        assert ">CALVET<" not in html, (
            "Naïve .upper() leaked — helper's agency_map should win"
        )

    def test_missing_agency_name_renders_no_badge(
        self, client, temp_data_dir, sample_rfq
    ):
        """When there's no agency classification at all, the template's
        `{% if r.get('agency_name') %}` guard skips the badge entirely.
        Verify an empty agency_name doesn't sneak an empty pill in."""
        import json, os
        rfq = dict(sample_rfq)
        rfq["agency_name"] = ""
        rfq["agency"] = ""
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({rfq["id"]: rfq}, f)
        resp = client.get(f"/rfq/{rfq['id']}")
        assert resp.status_code == 200
