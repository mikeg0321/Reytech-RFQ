"""Guard rail: SCPRS/won_quotes prices are NEVER seeded as unit_cost.

CLAUDE.md Pricing Guard Rails: "SCPRS Prices Are NOT Supplier Costs. SCPRS
prices are what the STATE paid another vendor. They are reference ceilings
for your bid price, NEVER your cost basis."

2026-05-30 incident (rfq_fca653f6): `pc_enrichment_pipeline` seeded
`it["pricing"]["unit_cost"] = per_unit` from a SCPRS/won_quotes KB match
(guarded only by `per_unit < 5000`). A $2 composition notebook matched a $130
SCPRS record → cost locked at $130 → bogus $149 bid tiers. The fix deletes
that assignment; items with no real catalog/supplier cost correctly fall to
NEEDS COST. This source-scan ratchet stops the anti-pattern from returning.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINE = REPO_ROOT / "src" / "agents" / "pc_enrichment_pipeline.py"

# Variables that hold a SCPRS / won_quotes competitor price (a ceiling, not a
# cost) inside the enrichment pipeline's SCPRS-lookup step.
_SCPRS_PRICE_VARS = ("per_unit", "scprs_price", "scprs_line_total")


def _collapse(text: str) -> str:
    out = text.replace("\\\n", " ")
    out = re.sub(r"\(\s*\n\s*", "(", out)
    return out


def test_enrichment_never_seeds_unit_cost_from_scprs_price():
    text = _collapse(PIPELINE.read_text(encoding="utf-8", errors="replace"))
    # Any assignment of unit_cost whose RHS is a SCPRS/won_quotes price var.
    bad = []
    for var in _SCPRS_PRICE_VARS:
        # e.g.  it["pricing"]["unit_cost"] = per_unit
        pat = re.compile(r'unit_cost["\']\s*\]\s*=\s*' + re.escape(var) + r'\b')
        for m in pat.finditer(text):
            bad.append(m.group(0).strip())
    assert not bad, (
        "SCPRS/won_quotes price seeded as unit_cost (guard-rail violation — "
        "SCPRS is a ceiling, never a cost). Remove the assignment; let the "
        "item fall through to NEEDS COST.\nFound: " + "; ".join(bad)
    )


def test_scprs_price_is_still_kept_as_reference():
    """The fix must keep SCPRS as a reference ceiling (scprs_price), only
    stop using it as cost — so we don't lose the market signal."""
    text = PIPELINE.read_text(encoding="utf-8", errors="replace")
    assert 'scprs_price' in text, (
        "scprs_price reference was removed entirely — keep it as the market "
        "ceiling; only the unit_cost seeding should go."
    )
