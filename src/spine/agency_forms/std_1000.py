"""STD 1000 — CA GenAI Disclosure Form.

Spine-shaped wrapper around the new `fill_std_1000` filler in
`src.forms.cchcs_attachment_fillers`. Reytech does not use GenAI
in the products supplied — fills the identity block, ticks "No",
signs + dates. Per the form's instruction, items 1-6 are skipped
when "No" is selected.

Pillar 4 / G10: 5th deferred renderer registered.
"""
from __future__ import annotations

import io
import re
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms._identity import (
    ReytechIdentity,
    SpineFormFillError,
)

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


_ADDRESS_RE = re.compile(
    r"^\s*(?P<street>.+?)\s*,\s*"
    r"(?P<city>[^,]+?)\s*,\s*"
    r"(?P<state>[A-Z]{2})\s+"
    r"(?P<zip>\d{5}(?:-\d{4})?)\s*$"
)


def _parse_address(address: str) -> dict:
    if not address:
        return {"street": "", "city": "", "state": "CA", "zip": ""}
    m = _ADDRESS_RE.match(address.strip())
    if not m:
        return {
            "street": address.strip(), "city": "",
            "state": "CA", "zip": "",
        }
    return {
        "street": m.group("street").strip(),
        "city": m.group("city").strip(),
        "state": m.group("state").strip(),
        "zip": m.group("zip").strip(),
    }


def _identity_to_reytech_info(identity: ReytechIdentity) -> dict:
    addr = _parse_address(identity.address)
    return {
        "company_name": identity.business_name,
        "phone": identity.phone,
        "street": addr["street"],
        "city": addr["city"],
        "state": addr["state"],
        "zip": addr["zip"],
        "description_of_goods": "Medical/Office supplies",
    }


def fill_std_1000_pdf(
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

    from src.forms.cchcs_attachment_fillers import fill_std_1000

    result = fill_std_1000(reytech_info, parsed)
    if result is None:
        raise SpineFormFillError(
            "STD 1000 template missing — "
            "std1000_blank.pdf not found in template search paths"
        )

    data = result.getvalue() if isinstance(result, io.BytesIO) else bytes(result)
    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"STD 1000 filler returned non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_std_1000_pdf"]
