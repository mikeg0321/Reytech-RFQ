"""CR-1 regression guard: api_customers_add must not default new contacts
to the 'DEFAULT' agency bucket.

Audited 2026-04-22 — the old code set `agency=data.get("agency", "DEFAULT")`.
Every downstream agency filter in the app excludes `DEFAULT`, so the new
contact never showed up in searches, buyer briefs, or expansion outreach.
Per CLAUDE.md (Agency & Institution Rules) and the canonical identity
memory, CDCR is the correct default — never `DEFAULT` or `UPPER()`.
"""
from pathlib import Path


ROUTES_CRM = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_crm.py"
)


def test_customers_add_does_not_default_to_DEFAULT():
    src = ROUTES_CRM.read_text(encoding="utf-8")
    assert 'data.get("agency", "DEFAULT")' not in src, (
        "CR-1 regression: api_customers_add defaults new contacts to the "
        "DEFAULT agency bucket. Filters exclude DEFAULT → ghost contact. "
        "Use CDCR as the fallback per CLAUDE.md Agency rules."
    )


def test_customers_add_defaults_to_cdcr():
    """The default fallback for a new contact agency should be CDCR."""
    src = ROUTES_CRM.read_text(encoding="utf-8")
    assert (
        'data.get("agency") or "CDCR"' in src
        or 'data.get("agency", "CDCR")' in src
    ), (
        "CR-1 positive check: api_customers_add should fall back to CDCR "
        "when the caller doesn't supply an agency — that's the documented "
        "default per the canonical-identity memory and CLAUDE.md."
    )
