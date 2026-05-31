"""Seller's Permit — static pre-filled Reytech attachment.

Spine-shaped wrapper around the legacy `splice_static()` in
`src.forms.cchcs_attachment_fillers`. The seller's permit is an already-
completed Reytech document (`data/templates/sellers_permit_reytech.pdf`);
there is nothing to fill. The adapter loads the static template and
returns its bytes — no quote, identity, or solicitation inputs touch the
output.

J2-2 (CalVet migration, 2026-05-31): one of the 3 CalVet required forms
with no Spine FormCode/adapter yet. Trivial pass-through. The quote /
identity / contract kwargs are accepted for the uniform FORM_REGISTRY
call shape and ignored — the rendered PDF is byte-identical regardless
of input.

Architect-authorized: J2-2 new src/spine/agency_forms adapter +
FormCode per CLAUDE.md §0 Job #2 LAW 4.
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


def fill_sellers_permit_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Return the static Reytech seller's permit PDF bytes.

    Args:
        quote / identity / today / flatten / contract:
            All accepted for uniform FORM_REGISTRY call shape and
            ignored. The seller's permit is a completed static document;
            no input influences the output.

    Returns:
        The static seller's permit PDF bytes.

    Raises:
        SpineFormFillError: if the static template is unreachable
            (filler returns None) or returns non-PDF output.
    """
    from src.forms.cchcs_attachment_fillers import splice_static

    # splice_static reads only its template_filename default
    # (sellers_permit_reytech.pdf); reytech_info + parsed are unused but
    # required positionally.
    result = splice_static({}, {})
    if result is None:
        raise SpineFormFillError(
            "Seller's Permit template missing — "
            "sellers_permit_reytech.pdf not found in template search paths"
        )

    data = result.getvalue() if isinstance(result, io.BytesIO) else bytes(result)
    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"Seller's Permit static splice returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_sellers_permit_pdf"]
