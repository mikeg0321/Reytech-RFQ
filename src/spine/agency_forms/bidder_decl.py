"""GSPD-05-105 — Bidder Declaration (CalVet + most CA agency bids).

Spine-shaped wrapper around the legacy `fill_bidder_declaration()` in
`src.forms.cchcs_attachment_fillers`. Flat-shape adapter — same shape
as `cuf.py` / `std_204.py`: convert Quote + ReytechIdentity into the
legacy `reytech_info` + `parsed` dict shape, call the existing filler,
return bytes.

J2-2 (CalVet migration, 2026-05-31): one of the 3 CalVet required forms
with no Spine FormCode/adapter yet. Reytech is a prime SB/DVBE supplier
with no subcontractors — the legacy filler ticks Check Box 3/5/8 and
leaves the 3 subcontractor blocks blank. The form's only bid-specific
field is the solicitation number (which the legacy filler reads from the
`parsed` header / `_sol_number`).

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


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    """Map ReytechIdentity → the legacy `reytech_info` dict keys that
    `fill_bidder_declaration` reads. The filler defaults cert_type to
    'SB/DVBE' and description_of_goods to a generic supplies string when
    absent; we pass the identity-derived values where we have them and
    let the filler's documented defaults stand otherwise."""
    return {
        "company_name": identity.business_name,
        "representative": identity.contact_person,
        "title": identity.title,
        "cert_number": identity.cert_number or "2002605",
        # Reytech is a prime supplier with no subcontractors — leave the
        # compliance dict empty so the legacy filler's defaults (claiming
        # SB preference, no subcontractors) stand.
        "compliance": {},
    }


def fill_bidder_decl_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render the GSPD-05-105 Bidder Declaration.

    Args:
        quote:    Spine Quote. Provides solicitation_number for the
                  declaration's "Solicitation #" field.
        identity: ReytechIdentity. Defaults to `from_env()` if omitted —
                  matches the convention every FORM_REGISTRY renderer
                  uses so callers can pass `None`.
        today:    Accepted for uniform signature; the legacy filler does
                  not stamp a date on this form.
        flatten:  Accepted-and-currently-ignored (legacy filler returns
                  the editable BytesIO).
        contract: Optional EmailContract for solicitation_number fallback
                  when the Quote doesn't carry one yet.

    Returns:
        Filled PDF bytes.

    Raises:
        SpineFormFillError: if the template is unreachable (filler
            returns None) or the filler returns non-PDF output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number
    parsed = {"header": {"solicitation_number": sol or ""}}

    from src.forms.cchcs_attachment_fillers import fill_bidder_declaration

    result = fill_bidder_declaration(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "Bidder Declaration template missing — "
            "bidder_declaration_blank.pdf not found in template search paths"
        )

    data = result.getvalue() if isinstance(result, io.BytesIO) else bytes(result)
    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"Bidder Declaration filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_bidder_decl_pdf"]
