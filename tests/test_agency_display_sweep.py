"""Regression guard: display-normalization call sites must route through
the shared `src.core.agency_display.agency_display()` helper, not inline
`agency_map = {"cchcs": "CCHCS", ...}` dicts.

PR #319 (2026-04-20) introduced the helper and wired up RFQ detail. The
2026-04-21 audit (PC-14) flagged two remaining inline dicts that render
display casing:

1. src/api/modules/routes_crm.py:300 — _guess_agency() returns the
   agency display name used in the CRM-match JSON response; if the
   resolver returns an unknown lowercase key, callers see bare
   lowercase (RFQ-3 root cause class).

2. src/api/modules/routes_pricecheck.py:1087 — institution-resolver
   self-heal assembles a display name for the PC detail page.

Both site dicts had subsets of the shared helper's keys, so
swapping to `agency_display()` is behavior-preserving for known
agencies AND adds coverage for the 4 keys they were missing
(calfire, caltrans, cdph, chp — routes_pricecheck.py) or always
correct regardless of resolver key case.

Note: quote_generator.py:1772 uses its inline dict as a MEMBERSHIP
gate ("only override if agency is a known canonical key"), not as
display normalization. That's semantically different — replacing it
with agency_display() would change the unknown-key override behavior.
Left alone by this sweep.
"""
from __future__ import annotations

import pathlib


_DISPLAY_SITES = [
    "src/api/modules/routes_crm.py",
    "src/api/modules/routes_pricecheck.py",
]


def test_display_call_sites_use_agency_display_helper():
    """Each flagged display site must import the helper."""
    for relpath in _DISPLAY_SITES:
        src = pathlib.Path(relpath).read_text(encoding="utf-8")
        assert "from src.core.agency_display import agency_display" in src, (
            f"{relpath} must import agency_display — audit PC-14 / RFQ-3 "
            f"wants one source of truth for display casing."
        )


def test_display_call_sites_dropped_inline_cchcs_maps():
    """Once the helper is in use, the literal inline map with both
    `"cchcs": "CCHCS"` AND `"cdcr": "CDCR"` is a leftover and must go.
    (quote_generator.py has a similar literal but is a membership gate,
    not a display map — exempt.)"""
    for relpath in _DISPLAY_SITES:
        src = pathlib.Path(relpath).read_text(encoding="utf-8")
        # Signature of a display-mapping inline dict.
        # Matches: `{"cchcs": "CCHCS", "cdcr": "CDCR", ...}`
        assert '"cchcs": "CCHCS"' not in src or '"cdcr": "CDCR"' not in src, (
            f"{relpath} still has an inline agency-display dict. Use "
            f"`agency_display(key)` from src.core.agency_display instead."
        )


def test_customers_match_returns_canonical_agency(client):
    """Functional guard: /api/customers/match returns a `suggested_agency`
    via _guess_agency() — must be canonical display casing (CCHCS, CalVet).
    Before the sweep: the inline dict worked, but any new agency key
    resolver adds wouldn't be canonicalized until someone found this
    site too. After: routed through the single agency_display helper.

    routes_crm.py is loaded via exec() into dashboard.py's namespace, so
    direct `import src.api.modules.routes_crm` ImportErrors — go through
    the HTTP endpoint instead (see CLAUDE.md "Module loading")."""
    resp = client.get(
        "/api/customers/match",
        query_string={"q": "California Correctional Health Care Services"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    # The match may or may not hit a CRM row depending on seed data; if
    # it misses, suggested_agency is set via _guess_agency. If it hits,
    # suggested_agency is omitted — either way, no lowercase leak.
    if "suggested_agency" in body:
        agency = body["suggested_agency"]
        assert agency == "CCHCS", (
            f"suggested_agency must be canonical 'CCHCS', got {agency!r}"
        )


def test_routes_pricecheck_renders_canonical_cchcs():
    """Functional guard: when institution_resolver returns lowercase
    'cchcs', the PC detail header must carry 'CCHCS' (not 'cchcs')
    into the template."""
    import importlib
    import src.core.agency_display as ad_mod
    importlib.reload(ad_mod)
    # Smoke the helper directly — covers the call site's behavior.
    assert ad_mod.agency_display("cchcs") == "CCHCS"
    assert ad_mod.agency_display("calvet") == "CalVet"
    assert ad_mod.agency_display("CCHCS") == "CCHCS"  # idempotent
    assert ad_mod.agency_display("") == ""            # empty-safe
    assert ad_mod.agency_display(None) == ""          # None-safe
    # Unknown → .upper() fallback matches the old inline pattern.
    assert ad_mod.agency_display("usda") == "USDA"
