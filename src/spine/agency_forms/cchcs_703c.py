"""CCHCS 703C — alternate of 703B (Fair and Reasonable / Exempt).

Spine-shaped wrapper around the legacy `fill_703c` in
`src.forms.reytech_filler_v4`. Unlike 703B (bundled blank in
spine/agency_forms/templates/703b_blank.pdf), 703C templates ship
WITH the buyer's email — every CCHCS bid that uses the 703C
variant attaches its own blank.

Pillar 4 / G10 (Architect approval 2026-05-27): 7th deferred
renderer registered. Resolves template via
`_template_resolver.resolve_template_path` — env override first,
then `contract.attachment_refs`, then raises.

The legacy fill_703c takes file paths (input + output) and writes
to disk; this adapter writes to a tempfile, reads bytes back, and
cleans up. Same pattern used by forms_render.py's Format-B path
(which also calls fill_703c) — the difference is that path bundles
multiple forms; this is a standalone render.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms._identity import (
    ReytechIdentity,
    SpineFormFillError,
)
from src.spine.agency_forms._template_resolver import resolve_template_path

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


def _build_legacy_rfq_dict(
    quote: "Quote",
    identity: ReytechIdentity,
    contract: "EmailContract | None",
) -> dict:
    """Convert Quote + Identity + Contract → the legacy rfq_data dict
    that fill_703c reads. Mirror of forms_render._build_legacy_rfq_dict
    but slimmer — 703C doesn't read line_items per-row, just identity +
    sol# + dates.
    """
    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number

    # Sign date — frozen at finalize when present.
    if quote.sign_date_pst is not None:
        sign_date_str = quote.sign_date_pst.strftime("%m/%d/%Y")
    else:
        from src.forms.reytech_filler_v4 import get_pst_date
        sign_date_str = get_pst_date()

    return {
        "solicitation_number": sol or "",
        "sign_date": sign_date_str,
        "agency": quote.agency,
        "facility": quote.facility,
        "institution": quote.facility,
        "line_items": [],  # 703C is cover-sheet only
        # Optional requestor block from the contract (buyer-side procurement)
        "requestor_name": getattr(contract, "buyer_name", "") if contract else "",
        "requestor_email": getattr(contract, "buyer_email", "") if contract else "",
        "requestor_phone": getattr(contract, "buyer_phone", "") if contract else "",
    }


def fill_703c_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render CCHCS 703C cover sheet. Template comes from the buyer's
    email (contract.attachment_refs) or the SPINE_703C_TEMPLATE_PATH
    env override.

    Raises SpineFormFillError if no template path is resolvable or
    the legacy filler returns non-PDF output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    template_path = resolve_template_path(
        "703c", contract, "SPINE_703C_TEMPLATE_PATH",
    )

    rfq_data = _build_legacy_rfq_dict(quote, identity, contract)

    from src.forms.reytech_filler_v4 import fill_703c, load_config
    try:
        config = load_config()
    except Exception as e:
        raise SpineFormFillError(f"could not load reytech_config.json: {e}")

    # The legacy filler writes to a file path. Use a tempfile so the
    # adapter's contract (return bytes) holds; clean up after read.
    tmp_dir = tempfile.mkdtemp(prefix="spine_703c_")
    out_path = os.path.join(tmp_dir, "703c_filled.pdf")
    try:
        fill_703c(template_path, rfq_data, config, out_path)
        if not os.path.isfile(out_path):
            raise SpineFormFillError(
                f"fill_703c returned without writing {out_path}"
            )
        with open(out_path, "rb") as fh:
            data = fh.read()
    finally:
        # Best-effort cleanup; ignore errors so a transient disk issue
        # doesn't mask a successful render.
        try:
            os.remove(out_path)
            os.rmdir(tmp_dir)
        except Exception:
            pass

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"703C filler produced non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_703c_pdf"]
