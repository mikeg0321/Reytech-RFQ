"""Regression test pinning the §0 Job #1 deletion of CCHCS from the
legacy `DEFAULT_AGENCY_CONFIGS` dict.

The original tests in this file pinned Mike's hard package rules for
the CCHCS entry in `src/core/agency_config.py`:

  1. CCHCS required_forms = 703B + 704B + Bid Package + Quote.
  2. DVBE 843 + seller's permit are INSIDE the bid package (optional only).
  3. 703C is an optional alternative to 703B.
  4. primary_response_form is 704b.

Those rules now belong to the Spine. CCHCS routes through `src/spine/`
per §0 LAW 1; the legacy entry was deleted per §0 Job #1 acceptance
2026-05-27. This file becomes the negative pin: the "cchcs" key must
NOT come back into `DEFAULT_AGENCY_CONFIGS`. Sibling entries
(calvet/dsh/dgs/calfire/other) survive.

If a future change re-adds "cchcs" to the legacy dict, this test fails
loudly — the migration completion gate is preserved.
"""
from __future__ import annotations


def test_cchcs_not_in_default_agency_configs():
    """§0 LAW 2 / Job #1: CCHCS routes through the Spine. The legacy
    `DEFAULT_AGENCY_CONFIGS["cchcs"]` entry was DELETED — re-adding it
    would resurrect the dead substrate. Sibling agencies survive.
    """
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    assert "cchcs" not in DEFAULT_AGENCY_CONFIGS, (
        "DEFAULT_AGENCY_CONFIGS['cchcs'] must NOT exist — CCHCS routes "
        "through `src/spine/` per §0 LAW 1 / Job #1 acceptance 2026-05-27. "
        "If you need to re-add a CCHCS legacy config, open a §0 PR first."
    )


def test_sibling_agency_entries_survive():
    """The Job #1 deletion is CCHCS-only. Multi-agency entries are
    deferred to per-agency migrations (CalVet, DSH, DGS, CalFire)."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    for key in ("calvet", "dsh", "dgs", "calfire", "other"):
        assert key in DEFAULT_AGENCY_CONFIGS, (
            f"Sibling agency key {key!r} must survive the CCHCS deletion — "
            f"per-agency migrations are tracked separately."
        )
