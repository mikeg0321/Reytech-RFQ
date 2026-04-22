"""Cross-module canonical identity sweep.

Per `project_reytech_canonical_identity`: every outbound communication
uses **Michael Guadan / sales@reytechinc.com / (949) 229-1575**, no variants.

The Orders audit 2026-04-21 fixed O-4 (`delivery-update` signed as "Mike
Gonzales / mike@reytechinc.com") and O-3 (app-level signature in reply-all).
A follow-up sweep across `src/api/modules/routes_*.py` found three more
identity variants embedded in email templates:

  - `routes_crm.py`            "Mike Guzman / (916) 995-4713 / mike@reytechinc.com"
  - `routes_growth_intel.py`   "Mike Gorzell / (916) 548-9484 / mike@reytechinc.com"
  - `routes_rfq_admin.py`      "(949) 872-8676 / mike@reytechinc.com" (signature HTML)

None of those name/phone/email triples are canonical. They would all have
gone to real buyers in outreach/capability/compose-window emails.

This grep-invariant locks the route files to the canonical identity. Any
re-introduction fails pre-push.
"""
from __future__ import annotations

import os
import pathlib


FORBIDDEN_IDENTITY_TOKENS = [
    # Wrong names
    "Mike Guzman",
    "Mike Gorzell",
    "Mike Gonzales",
    # Wrong email (canonical is sales@reytechinc.com)
    "mike@reytechinc.com",
    # Wrong phone variants spotted in the sweep (canonical is 949-229-1575)
    "(949) 872-8676",
    "(916) 995-4713",
    "(916) 548-9484",
    "9498728676",
    "9169954713",
    "9165489484",
]


def _routes_files():
    routes_dir = pathlib.Path(__file__).resolve().parents[1] / "src" / "api" / "modules"
    # Every route module is eligible — identity can leak from any email
    # template, not just orders-related ones.
    return sorted(routes_dir.glob("routes_*.py"))


def test_no_wrong_identity_in_any_route_module():
    """Grep-invariant: no route module in src/api/modules/ contains a
    non-canonical identity token. Canonical: Michael Guadan /
    sales@reytechinc.com / (949) 229-1575."""
    offenders = []
    for path in _routes_files():
        src = path.read_text(encoding="utf-8", errors="replace")
        for token in FORBIDDEN_IDENTITY_TOKENS:
            if token in src:
                # Locate the first hit to make the failure actionable
                line_no = src[: src.index(token)].count("\n") + 1
                offenders.append(f"{path.name}:{line_no}  token={token!r}")
    assert not offenders, (
        "Non-canonical identity string(s) found in route modules — every "
        "outbound email/draft must use Michael Guadan / sales@reytechinc.com "
        "/ (949) 229-1575. Offenders:\n  " + "\n  ".join(offenders))


def test_canonical_identity_is_still_referenced():
    """Sanity: at least one route module references the canonical sales
    address. Catches a search-and-replace that accidentally wipes the
    canonical string alongside the wrong variants."""
    hits = 0
    for path in _routes_files():
        src = path.read_text(encoding="utf-8", errors="replace")
        if "sales@reytechinc.com" in src:
            hits += 1
    assert hits >= 1, (
        "No route module references sales@reytechinc.com — canonical "
        "identity may have been stripped. Restore explicit references.")
