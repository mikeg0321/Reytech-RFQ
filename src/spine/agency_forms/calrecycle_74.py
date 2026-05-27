"""CalRecycle 74 — Postconsumer Recycled-Content Certification.

Spine-shaped wrapper around the legacy `fill_calrecycle_74()` filler
in `src.forms.cchcs_attachment_fillers`. Same architecture as the
existing `cchcs_703b.py` adapter: convert Quote + EmailContract into
the legacy `reytech_info` + `parsed` dict shape, call the existing
filler, return bytes.

Pillar 4 / G10 (chrome MCP audit 2026-05-26): FORM_REGISTRY had 4
entries (703b/704b/bidpkg/quote) but the FormCode literal declares
12 — CalVet bids alone need calrecycle_74 + std_204 + dvbe_843 +
darfur. This is the first of the 8 deferred renderers, registering
calrecycle_74. Unblocks the path toward multi-agency expansion
beyond CCHCS once Job #1 deletion gate closes.

The legacy `fill_calrecycle_74` already works inside `fill_bidpkg`
(packet path) — this adapter just exposes it as a standalone Spine
renderer with the uniform `(quote, identity, *, today, flatten,
contract) -> bytes` signature that FORM_REGISTRY expects.
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms.cchcs_703b import (
    ReytechIdentity,
    SpineFormFillError,
)
from src.spine.model import SpineValidationError

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    """Map ReytechIdentity fields to the legacy `reytech_info` dict
    keys that `fill_calrecycle_74` reads. Mirrors the field mapping
    inside `src.forms.cchcs_attachment_fillers.fill_calrecycle_74`
    (lines 529-534)."""
    return {
        "company_name": identity.business_name,
        "address": identity.address,
        "phone": identity.phone,
        "representative": identity.contact_person,
        "title": identity.title,
        # The legacy filler defaults `compliance` to 0% / N/A if absent.
        # Operator can override per-bid by editing the rendered PDF if
        # they have an actual SABRC-compliant product mix; the default
        # is the conservative non-compliant claim.
        "compliance": {},
    }


def _quote_to_legacy_parsed(quote: "Quote",
                            contract: "EmailContract | None") -> dict:
    """Build the minimal `parsed` dict CalRecycle 74 reads — solicitation
    number + line_items[].description. The full `_build_legacy_rfq_dict`
    in forms_render is heavier than needed here; this is the slim
    subset that maps to what the filler actually consumes."""
    line_items: list[dict] = []
    for li in quote.line_items:
        line_items.append({
            "row_index": li.line_no,
            "description": li.description,
        })

    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number

    return {
        "solicitation_number": sol or "",
        "line_items": line_items,
    }


def fill_calrecycle_74_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render CalRecycle 74 Postconsumer Recycled-Content Certification.

    Args:
        quote:    Spine Quote. Provides solicitation_number + line_items.
        identity: ReytechIdentity. Defaults to `from_env()` if omitted —
                  matches the convention `cchcs_703b.fill_703b_pdf`
                  uses so callers can pass `None` for env-driven
                  defaults.
        today:    Currently unused by the legacy filler (it computes
                  `_today_mmddyyyy()` internally). Accepted for
                  signature uniformity with the rest of FORM_REGISTRY
                  so the registry's adapter contract stays flat.
        flatten:  Accepted-and-currently-ignored. The legacy filler
                  returns the editable BytesIO; a follow-on PR can
                  add bake() if a flat variant is needed. Keeping the
                  kwarg in the signature so the registry's uniform
                  call shape holds.
        contract: Optional EmailContract for solicitation_number
                  fallback when the Quote doesn't carry one yet.

    Returns:
        Filled PDF bytes.

    Raises:
        SpineFormFillError: if the template is unreachable or the
            legacy filler returns None (template not found).
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    parsed = _quote_to_legacy_parsed(quote, contract)

    # Import here so the module's import-time cost is just the dataclass
    # — large filler module isn't loaded unless this renderer runs.
    from src.forms.cchcs_attachment_fillers import fill_calrecycle_74

    result = fill_calrecycle_74(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "CalRecycle 74 template missing — "
            "calrecycle_74_blank.pdf not found in template search paths"
        )

    if isinstance(result, io.BytesIO):
        data = result.getvalue()
    else:
        data = bytes(result)

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"CalRecycle 74 filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )

    return data


__all__ = ["fill_calrecycle_74_pdf"]
