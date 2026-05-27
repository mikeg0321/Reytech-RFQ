"""CV 012 — Commercially Useful Function attestation (DVBE).

Spine-shaped wrapper around the new `fill_cuf` filler in
`src.forms.cchcs_attachment_fillers`. Reytech is a DVBE-certified
supplier of goods that performs the function directly — purchase
inventory, manage logistics, deliver. All 6 CUF questions answered
"Yes".

Pillar 4 / G10: 6th deferred renderer registered.
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
    return {
        "company_name": identity.business_name,
        "representative": identity.contact_person,
        "title": identity.title,
        "cert_number": identity.cert_number or "2002605",
        "cert_expiration": identity.cert_expiration or "",
    }


def fill_cuf_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number
    parsed = {"header": {"solicitation_number": sol or ""}}

    from src.forms.cchcs_attachment_fillers import fill_cuf

    result = fill_cuf(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "CV 012 CUF template missing — "
            "cv012_cuf_blank.pdf not found in template search paths"
        )

    data = result.getvalue() if isinstance(result, io.BytesIO) else bytes(result)
    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"CV 012 CUF filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_cuf_pdf"]
