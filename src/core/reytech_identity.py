"""
src/core/reytech_identity.py — Single source of truth for Reytech sender
identity in any outbound communication.

Per memory `project_reytech_canonical_identity`: every form, email, and
signature block must use **Michael Guadan** + sales@reytechinc.com — no
variants even if reference packs show others.

Created 2026-04-25 in V2-PR-8 to eliminate the "Mike Gonzalez" hardcode
in src/agents/outreach_agent.py:118,155 (caught by product-engineer
review). All future outreach copy must read from here.
"""
from __future__ import annotations

import logging

log = logging.getLogger("reytech.identity")


# ── Canonical identity ────────────────────────────────────────────────────────
NAME = "Michael Guadan"
TITLE = "President"
COMPANY = "Reytech Inc."
EMAIL = "sales@reytechinc.com"
PHONE = ""  # No public phone documented; leave empty rather than fabricate.

SIGNATURE_BLOCK = "\n".join([
    "Best regards,",
    NAME,
    TITLE + ", " + COMPANY,
    EMAIL,
])


def signature() -> str:
    """Standard email-signature block. Use at the bottom of every
    outbound message body."""
    return SIGNATURE_BLOCK


def render_context() -> dict:
    """Variables for template interpolation."""
    return {
        "reytech_name": NAME,
        "reytech_title": TITLE,
        "reytech_company": COMPANY,
        "reytech_email": EMAIL,
        "reytech_phone": PHONE,
        "reytech_signature": SIGNATURE_BLOCK,
    }


# ── Cert lookups (data-driven, not hardcoded) ────────────────────────────────

def get_active_cert_number(cert_type: str) -> str | None:
    """Read SB / MB / DVBE / OSDS cert numbers from
    `reytech_certifications` (V2-PR-4). Returns None if cert isn't on
    file or has expired.

    Schema-tolerant: returns None if table is missing.
    """
    try:
        from datetime import date
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT cert_number, expires_at FROM reytech_certifications "
                "WHERE cert_type = ? AND is_active = 1 AND is_test = 0",
                (cert_type.upper(),)
            ).fetchone()
            if not row:
                return None
            num = (row["cert_number"] if hasattr(row, "__getitem__") else row[0]) or ""
            ex_raw = (row["expires_at"] if hasattr(row, "__getitem__") else row[1]) or ""
            if ex_raw:
                try:
                    if date.fromisoformat(ex_raw[:10]) < date.today():
                        return None  # expired — cert is not legally valid
                except (ValueError, TypeError):
                    pass
            return num.strip() or None
    except Exception as e:
        log.debug("get_active_cert_number suppressed: %s", e)
        return None


def get_cert_context() -> dict:
    """Cert numbers for template interpolation. Missing certs map to
    empty string (template render code MUST check for empty before
    citing a cert # in the copy)."""
    return {
        "sb_cert_no": get_active_cert_number("SB") or "",
        "mb_cert_no": get_active_cert_number("MB") or "",
        "dvbe_cert_no": get_active_cert_number("DVBE") or "",
        "osds_cert_no": get_active_cert_number("OSDS") or "",
    }
