"""One place to canonicalize agency display casing.

Problem this solves: the institution_resolver and classifier_v2 store the
agency identity as a lowercase key (`"cchcs"`, `"calvet"`). Render sites
throughout the app need the canonical display form (`"CCHCS"`, `"CalVet"`).
The route handlers used to carry their own private `agency_map` dicts
(routes_crm.py:303, routes_pricecheck.py:1087, routes_pricecheck_admin.py
:1933, dashboard.py:1137). Any render site that missed the step showed
bare lowercase in the UI — which is exactly what the RFQ detail badge
at rfq_detail.html:29 does today.

Use this helper anywhere an agency string is about to be rendered or
passed into a template.
"""
from __future__ import annotations

_CANONICAL = {
    "cchcs": "CCHCS",
    "cdcr": "CDCR",
    "dsh": "DSH",
    "dgs": "DGS",
    "calvet": "CalVet",
    "calfire": "CalFire",
    "caltrans": "CalTrans",
    "cdph": "CDPH",
    "chp": "CHP",
}


def agency_display(value: str | None) -> str:
    """Return the canonical display casing for an agency string.

    - Known lowercase keys (`cchcs`, `calvet`, …) resolve to their canonical
      form (`CCHCS`, `CalVet`, …).
    - Already-canonical values pass through unchanged.
    - Unknown values fall back to `.upper()` — matching the existing pattern
      at routes_crm.py:303 (`agency_map.get(agency.lower(), agency.upper())`).
    - Empty / None → empty string, so templates' `{% if agency %}` guards
      keep working unchanged.
    """
    if not value:
        return ""
    key = value.strip().lower()
    if not key:
        return ""
    return _CANONICAL.get(key, value.strip().upper())
