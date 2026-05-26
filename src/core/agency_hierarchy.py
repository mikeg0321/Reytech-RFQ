"""Parent-agency registry — hierarchy data + parent detection.

EXTRACTED FROM `agency_config.py` (audit Item 4 / PR #13 / 2026-05-26).

Why this module exists separately
─────────────────────────────────
`agency_config.py` is the legacy per-agency configuration store —
its `DEFAULT_AGENCY_CONFIGS["cchcs"]` entry is on the §0 Job #1
deletion schedule (due 2026-06-18), and its other entries (CalVet,
DSH, DGS, CalFire) follow as each agency migrates to the Spine.

PARENT_AGENCIES + OVERLAP_PATTERNS + `_detect_parent` are a different
concern: agency *hierarchy* — which child agencies live under which
parent organization. The hierarchy data is reusable (and arguably
permanent — CDCR being the parent of CCHCS is just a fact), but it
was inlined inside `agency_config.py` because that's where the
consumer lived. Adding 172 LOC of hierarchy data to a deletion-tracked
file (PR #1080, 2026-05-25) is anti-discipline per §0 LAW 2 — the
moment the auditor flagged.

Migration path
──────────────
- Phase 1 (this PR): physically move the data + helper here. The
  `agency_config.py` module re-imports for back-compat — every existing
  caller (DEFAULT_AGENCY_CONFIGS internal lookups, line ~601) keeps
  working unchanged.
- Phase 2 (post-Job #1): when the per-agency entries in
  `DEFAULT_AGENCY_CONFIGS` are deleted, this module SURVIVES because
  the hierarchy is shared infrastructure — operator KPI queries,
  parent-domain match in the email parser, future Spine-side
  agency-router code all need it.

History of the substrate fix this data drives (kept here so a future
reader doesn't have to dig into agency_config.py to learn the why):

  PVSP (Pleasant Valley State Prison) is a CCHCS facility in COALINGA, CA.
  DSH's match_patterns include "COALINGA" because Coalinga State Hospital
  is also in Coalinga. The pre-2026-05-25 `match_agency()` loop checked
  DSH BEFORE CCHCS and returned DSH for any quote shipping to PVSP,
  producing the wrong Fill Plan with NO PROFILE for CCHCS Bid Package.

  Substrate fix:
    1. Detect parent via email domain (deterministic) OR strong text
       patterns ("CCHCS", "STATE HOSPITAL", etc.).
    2. Once parent is known, match only the parent's children.
    3. Patterns flagged as OVERLAP (e.g. "COALINGA") never fire without
       a parent context — they need parent disambiguation first.
    4. Mike's mental model: CCHCS and DSH are sibling child branches of
       the correctional/healthcare CDCR-family parent, each with its
       own addresses, processes, forms, bill-to.
       See [[architectural-cdcr-hierarchy]].
"""
from __future__ import annotations


# Parent-agency registry — two-tier hierarchy.
# Each parent has strong signals (unambiguous patterns / domains that
# prove this parent) and a children list (the legacy agency-config keys
# that live under this parent).
PARENT_AGENCIES = {
    "CDCR": {
        "name": "California Department of Corrections and Rehabilitation",
        "strong_patterns": [
            "CCHCS", "CDCR", "CDCR.CA.GOV", "CCHCS.CA.GOV",
            "CORRECTIONAL HEALTH CARE",
            "CALIFORNIA CORRECTIONAL",
            "STATE PRISON",  # CCHCS prison naming; DSH doesn't use "State Prison"
        ],
        "domains": ["cdcr.ca.gov", "cchcs.ca.gov"],
        "children": ["cchcs"],
    },
    "DSH": {
        "name": "Department of State Hospitals",
        "strong_patterns": [
            "DSH", "DSH.CA.GOV",
            "STATE HOSPITAL", "DEPARTMENT OF STATE HOSPITALS",
            "ATASCADERO STATE", "NAPA STATE", "METROPOLITAN STATE", "PATTON STATE",
        ],
        "domains": ["dsh.ca.gov"],
        "children": ["dsh"],
    },
    "CALVET": {
        "name": "California Department of Veterans Affairs",
        "strong_patterns": [
            "CALVET", "CAL VET", "CVA",
            "VETERANS HOME", "VETERANS AFFAIRS",
            "VHC", "CALVET.CA.GOV",
        ],
        "domains": ["calvet.ca.gov"],
        # Order matters: calvet_barstow is more specific; checked first.
        "children": ["calvet_barstow", "calvet"],
    },
    "DGS": {
        "name": "Department of General Services",
        "strong_patterns": ["DGS", "GENERAL SERVICES", "DGS.CA.GOV"],
        "domains": ["dgs.ca.gov"],
        "children": ["dgs"],
    },
    "CALFIRE": {
        "name": "California Department of Forestry and Fire Protection",
        "strong_patterns": ["CALFIRE", "CAL FIRE", "FORESTRY", "FIRE PROTECTION"],
        "domains": ["fire.ca.gov", "calfire.ca.gov"],
        "children": ["calfire"],
    },
}


# Patterns that previously lived in agency `match_patterns` lists but are
# AMBIGUOUS — they appear in multiple parents' facility universes and must
# not fire without a parent signal. The canonical example: "COALINGA"
# matches both PVSP (CCHCS) and Coalinga State Hospital (DSH).
#
# Patterns listed here are SKIPPED during the no-parent fallback scan.
# Parent-scoped matching ignores this list (the parent context resolves
# the ambiguity).
OVERLAP_PATTERNS = {
    "COALINGA",  # PVSP (cchcs) + Coalinga State Hospital (dsh)
}


def _detect_parent(search_text: str, email_domain: str | None = None) -> str | None:
    """Detect parent organization from strong signals.

    Order: email domain (deterministic) → strong text patterns. Returns
    parent_id ("CDCR", "DSH", "CALVET", "DGS", "CALFIRE") or None.

    Pure helper — no side effects.
    """
    text = (search_text or "").upper()
    # Domain signal wins.
    if email_domain:
        d = email_domain.lower().strip().rstrip(">").rstrip(".")
        for parent_id, info in PARENT_AGENCIES.items():
            for known in sorted(info.get("domains", []), key=lambda x: -len(x)):
                if d == known or d.endswith("." + known):
                    return parent_id
    # Strong text patterns — longest pattern wins to favor specificity.
    candidates = []
    for parent_id, info in PARENT_AGENCIES.items():
        for pattern in info.get("strong_patterns", []):
            if pattern.upper() in text:
                candidates.append((len(pattern), parent_id, pattern))
    if candidates:
        candidates.sort(reverse=True)  # longest first
        return candidates[0][1]
    return None


# Public alias for callers outside agency_config.py (the leading
# underscore on _detect_parent signals "internal to agency_config" —
# this is now a public surface).
detect_parent = _detect_parent


__all__ = ["PARENT_AGENCIES", "OVERLAP_PATTERNS", "_detect_parent", "detect_parent"]
