"""STD 205 — CA Payee Data Record Supplement.

Spine-shaped wrapper around the legacy path-based `fill_std205()` in
`src.forms.reytech_filler_v4`. Unlike the flat-shape attachment fillers
(`cuf`, `std_204`, `bidder_decl`), `fill_std205` has the file-writing
signature `(input_path, rfq_data, config, output_path)` and reads a
NESTED `config["company"]{...}` dict. This adapter uses the same
path-based temp-file bridge that `src/spine/forms_render.py` uses for the
AMS 703/704 fillers: resolve the blank template path, build a
`config["company"]` dict from ReytechIdentity (NOT from the legacy
reytech_config.json — the Spine carries its own identity boundary), write
the filled PDF to a tempfile, read the bytes back, clean up.

J2-2 (CalVet migration, 2026-05-31): one of the 3 CalVet required forms
with no Spine FormCode/adapter yet. STD 205 supplements STD 204 with
additional remittance addresses / contacts; Reytech fills the canonical
authorized-rep row.

Architect-authorized: J2-2 new src/spine/agency_forms adapter +
FormCode per CLAUDE.md §0 Job #2 LAW 4.
"""
from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms._identity import (
    ReytechIdentity,
    SpineFormFillError,
)

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# Same address splitter shape as std_204.py — "Street, City, ST 12345".
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
        return {"street": address.strip(), "city": "", "state": "CA", "zip": ""}
    return {
        "street": m.group("street").strip(),
        "city": m.group("city").strip(),
        "state": m.group("state").strip(),
        "zip": m.group("zip").strip(),
    }


def _identity_to_company(identity: ReytechIdentity) -> dict:
    """Build the legacy `config["company"]` dict from ReytechIdentity.
    `fill_std205` reads: name, fein, address, city, state, zip, owner,
    title, phone, email. We keep `address` as the full string (the filler
    splits the street off the first comma itself) AND pass the parsed
    city/state/zip so the filler doesn't fall back to its Trabuco Canyon
    hardcodes for a future non-canonical identity."""
    parts = _parse_address(identity.address)
    return {
        "name": identity.business_name,
        "owner": identity.contact_person,
        "title": identity.title,
        "address": identity.address,
        "city": parts["city"],
        "state": parts["state"],
        "zip": parts["zip"],
        "phone": identity.phone,
        "email": identity.email,
        "fein": identity.fein,
    }


def _resolve_blank_template() -> str:
    """Resolve the std205_blank.pdf path via the same search the legacy
    attachment fillers use. Raises SpineFormFillError if unreachable."""
    from src.forms.cchcs_attachment_fillers import _template_path
    path = _template_path("std205_blank.pdf")
    if not path:
        raise SpineFormFillError(
            "STD 205 template missing — "
            "std205_blank.pdf not found in template search paths"
        )
    return path


def fill_std_205_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render STD 205 Payee Data Record Supplement.

    Args:
        quote:    Spine Quote. STD 205 is vendor-only (no bid data on the
                  form); kept for the uniform registry call shape.
        identity: ReytechIdentity. Defaults to `from_env()` if omitted.
        today:    Accepted for uniform signature. The legacy filler
                  stamps `get_pst_date()` itself; we leave sign_date
                  unset so it uses today.
        flatten:  Accepted-and-currently-ignored.
        contract: Accepted-and-currently-unused (vendor-only form).

    Returns:
        Filled PDF bytes.

    Raises:
        SpineFormFillError: if the template is unreachable or the filler
            writes nothing / non-PDF output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    template_path = _resolve_blank_template()
    config = {"company": _identity_to_company(identity)}
    rfq_data: dict = {}

    from src.forms.reytech_filler_v4 import fill_std205

    tmp_dir = tempfile.mkdtemp(prefix="spine_std205_")
    out_path = os.path.join(tmp_dir, "std205_filled.pdf")
    try:
        fill_std205(template_path, rfq_data, config, out_path)
        if not os.path.isfile(out_path):
            raise SpineFormFillError(
                f"STD 205 filler returned without writing {out_path}"
            )
        with open(out_path, "rb") as fh:
            data = fh.read()
    finally:
        try:
            os.remove(out_path)
            os.rmdir(tmp_dir)
        except OSError:
            pass

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"STD 205 filler produced non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_std_205_pdf"]
