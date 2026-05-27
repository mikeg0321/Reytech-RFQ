"""Darfur Contracting Act Certification.

Spine-shaped wrapper around the legacy `fill_darfur_act()` in
`src.forms.cchcs_attachment_fillers`. Same architectural shape as
the calrecycle_74 + std_204 + dvbe_843 adapters.

Pillar 4 / G10: 4th deferred renderer registered. Required by
CalVet + DGS + most other CA agency bids — Reytech is not a
scrutinized company per CA Government Code §10477, so we sign the
non-scrutinized declaration on every bid.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms._identity import (
    ReytechIdentity,
    SpineFormFillError,
)

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    """Map ReytechIdentity to the legacy `reytech_info` keys
    `fill_darfur_act` reads (lines 674-678)."""
    return {
        "company_name": identity.business_name,
        "fein": identity.fein,
        "representative": identity.contact_person,
        "title": identity.title,
    }


def fill_darfur_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render Darfur Act Certification. Form is purely vendor identity
    + non-scrutinized declaration — no bid-specific data. Quote and
    contract are accepted-and-ignored for uniform registry signature."""
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    parsed: dict = {}

    from src.forms.cchcs_attachment_fillers import fill_darfur_act

    result = fill_darfur_act(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "Darfur Act template missing — "
            "darfur_act_blank.pdf not found in template search paths"
        )

    if isinstance(result, io.BytesIO):
        data = result.getvalue()
    else:
        data = bytes(result)

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"Darfur Act filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )

    return data


__all__ = ["fill_darfur_pdf"]
