"""STD 204 — CA Payee Data Record.

Spine-shaped wrapper around the legacy `fill_std204()` in
`src.forms.cchcs_attachment_fillers`. Same architectural shape as
the `calrecycle_74` adapter — convert Quote + ReytechIdentity into
the legacy `reytech_info` + `parsed` dict shape, call the existing
filler, return bytes.

Pillar 4 / G10 (chrome MCP audit 2026-05-26): 2nd of 8 deferred
renderers registered. STD 204 is the **most universal** of the
deferred set — required by:
  - CalVet (DEFAULT_AGENCY_CONFIGS["calvet"].required_forms)
  - DGS (DEFAULT_AGENCY_CONFIGS["dgs"].required_forms)
  - DSH (DEFAULT_AGENCY_CONFIGS["dsh"].required_forms)
  - CCHCS bidpkg (already fired inside fill_bid_package)

Registering it as a standalone Spine renderer unblocks every non-
CCHCS agency response path.

The legacy filler reads the Reytech address as separate
street/city/state/zip fields. ReytechIdentity stores the full
address as one string; this adapter parses it on the way in.
"""
from __future__ import annotations

import io
import re
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms.cchcs_703b import (
    ReytechIdentity,
    SpineFormFillError,
)

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# Match "Street, City, ST 12345" with optional ZIP+4. The Reytech
# canonical address ("30 Carnoustie Way, Trabuco Canyon, CA 92679")
# fits this; if a future identity uses a non-canonical form the
# adapter falls back to passing the full string as the street.
_ADDRESS_RE = re.compile(
    r"^\s*(?P<street>.+?)\s*,\s*"
    r"(?P<city>[^,]+?)\s*,\s*"
    r"(?P<state>[A-Z]{2})\s+"
    r"(?P<zip>\d{5}(?:-\d{4})?)\s*$"
)


def _parse_address(address: str) -> dict:
    """Split a single-string address into street/city/state/zip parts.
    Returns a dict with empty strings for parts the regex can't
    determine — the legacy filler tolerates missing fields."""
    if not address:
        return {"street": "", "city": "", "state": "CA", "zip": ""}
    m = _ADDRESS_RE.match(address.strip())
    if not m:
        # Best-effort: dump the whole string into street, default the rest.
        return {
            "street": address.strip(),
            "city": "",
            "state": "CA",
            "zip": "",
        }
    return {
        "street": m.group("street").strip(),
        "city": m.group("city").strip(),
        "state": m.group("state").strip(),
        "zip": m.group("zip").strip(),
    }


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    """Map ReytechIdentity fields to the legacy `reytech_info` dict
    keys that `fill_std204` reads. Mirrors the field mapping inside
    `src.forms.cchcs_attachment_fillers.fill_std204` (lines 579-595)."""
    addr_parts = _parse_address(identity.address)
    return {
        "company_name": identity.business_name,
        "representative": identity.contact_person,
        "title": identity.title,
        "street": addr_parts["street"],
        "city": addr_parts["city"],
        "state": addr_parts["state"],
        "zip": addr_parts["zip"],
        "email": identity.email,
        "phone": identity.phone,
        "fein": identity.fein,
    }


def fill_std_204_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render STD 204 Payee Data Record.

    Args:
        quote:    Spine Quote. Currently unused by STD 204 (the form is
                  vendor-only — solicitation number isn't on it). Kept
                  in the signature so the registry's uniform call shape
                  holds. Future per-bid metadata (e.g., a paying-agency
                  note Reytech might inscribe) can be wired through
                  later without changing the registry call shape.
        identity: ReytechIdentity. Defaults to `from_env()` if omitted.
        today:    Currently unused by the legacy filler (it computes
                  `_today_mmddyyyy()` internally). Accepted for
                  uniform signature.
        flatten:  Accepted-and-currently-ignored (legacy filler returns
                  editable BytesIO).
        contract: Accepted-and-currently-unused — see `quote` rationale
                  above.

    Returns:
        Filled PDF bytes.

    Raises:
        SpineFormFillError: if template missing or filler returns
            non-PDF output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    reytech_info = _identity_to_reytech_info(identity)
    # STD 204 doesn't read bid-specific data from `parsed` — pass an
    # empty dict. We intentionally accept the kwarg for uniform
    # registry contract.
    parsed: dict = {}

    from src.forms.cchcs_attachment_fillers import fill_std204

    result = fill_std204(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "STD 204 template missing — "
            "std204_blank.pdf not found in template search paths"
        )

    if isinstance(result, io.BytesIO):
        data = result.getvalue()
    else:
        data = bytes(result)

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"STD 204 filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )

    return data


__all__ = ["fill_std_204_pdf"]
