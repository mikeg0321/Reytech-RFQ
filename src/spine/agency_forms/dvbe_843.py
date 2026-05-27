"""DVBE 843 — CA Disabled Veteran Business Enterprise Declaration.

Spine-shaped wrapper around the legacy `fill_dvbe_843()` in
`src.forms.cchcs_attachment_fillers`. Same architectural shape as
the calrecycle_74 + std_204 adapters.

Pillar 4 / G10: 3rd deferred renderer registered. Required by
CalVet + DGS bids. CCHCS bidpkg already fires it internally via
fill_bid_package.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms.cchcs_703b import (
    ReytechIdentity,
    SpineFormFillError,
)

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    """Map ReytechIdentity to the legacy `reytech_info` keys
    `fill_dvbe_843` reads (lines 460-467)."""
    return {
        "company_name": identity.business_name,
        "cert_number": identity.cert_number or "2002605",
        "representative": identity.contact_person,
        "phone": identity.phone,
        "address": identity.address,
        # The legacy filler defaults description_of_goods if absent.
        "description_of_goods": "Medical/Office supplies",
    }


def _quote_to_legacy_parsed(quote: "Quote",
                            contract: "EmailContract | None") -> dict:
    """DVBE 843 reads solicitation_number via _sol_number(parsed)."""
    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number
    return {"solicitation_number": sol or ""}


def fill_dvbe_843_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render DVBE 843. Same uniform signature as the rest of
    FORM_REGISTRY. Raises SpineFormFillError on missing template /
    non-PDF output."""
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    parsed = _quote_to_legacy_parsed(quote, contract)

    from src.forms.cchcs_attachment_fillers import fill_dvbe_843

    result = fill_dvbe_843(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "DVBE 843 template missing — "
            "dvbe_843_blank.pdf not found in template search paths"
        )

    if isinstance(result, io.BytesIO):
        data = result.getvalue()
    else:
        data = bytes(result)

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"DVBE 843 filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )

    return data


__all__ = ["fill_dvbe_843_pdf"]
