"""CR-2 regression guard: fix-hint in api_email_health must name the
canonical Reytech domain, not the typo `raytechinc`.

Per project_reytech_canonical_identity memory: sales@reytechinc.com is
the only legitimate Reytech identity. A fix-hint pointing to a misspelled
domain sends operators down a dead-end credentials loop.
"""
from pathlib import Path


ROUTES_CRM = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_crm.py"
)


def test_email_health_no_raytechinc_typo():
    src = ROUTES_CRM.read_text(encoding="utf-8")
    assert "raytechinc" not in src, (
        "CR-2 regression: `raytechinc` (typo: ray vs rey) appears in "
        "routes_crm.py. Canonical domain is reytechinc.com — the typo "
        "sends operators chasing creds for a domain that doesn't exist."
    )


def test_email_health_uses_canonical_domain():
    """Positive: the canonical reytechinc.com address should be present
    so the fix-hint still gives useful guidance."""
    src = ROUTES_CRM.read_text(encoding="utf-8")
    assert "sales@reytechinc.com" in src, (
        "CR-2 positive check: fix-hint should name sales@reytechinc.com "
        "(the canonical Reytech identity) when GMAIL_ADDRESS is unset."
    )
