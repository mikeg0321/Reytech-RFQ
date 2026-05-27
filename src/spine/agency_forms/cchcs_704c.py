"""CCHCS 704C — alternate of 704B (line-item form).

Spine-shaped wrapper around the legacy `fill_704b` in
`src.forms.reytech_filler_v4`. The legacy filler auto-detects
field-name prefixes (via `template_registry.get_profile` +
`detect_field_prefix`) so the same function handles 704B and 704C
templates as long as the template is well-formed.

Pillar 4 / G10 (Architect approval 2026-05-27): 8th and final
deferred renderer. Like 703C, the 704C template ships with the
buyer's email — resolved via attachment_refs or env override.
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


def _build_legacy_rfq_dict(quote: "Quote",
                           contract: "EmailContract | None") -> dict:
    """Convert Quote → legacy rfq_data shape fill_704b reads.

    704 forms iterate line_items so this is the heavier conversion:
    each line emits line_number / description / qty / uom /
    unit_price / supplier_cost / mfg_number for the row builder.
    Mirror of forms_render._build_legacy_rfq_dict.
    """
    line_items: list[dict] = []
    for li in quote.line_items:
        unit_price = li.unit_price_cents / 100.0
        mfg = (li.mfg_number or "").strip()
        line_items.append({
            "line_number": li.line_no,
            "description": li.description,
            "qty": li.qty,
            "uom": li.uom,
            "unit_price": unit_price,
            "price_per_unit": unit_price,
            "supplier_cost": li.cost_cents / 100.0,
            "mfg_number": mfg,
            "part_number": mfg,
        })

    sol = quote.solicitation_number
    if not sol and contract is not None:
        sol = contract.solicitation_number

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
        "line_items": line_items,
    }


def fill_704c_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render CCHCS 704C line-item form. Template resolved from
    contract.attachment_refs or SPINE_704C_TEMPLATE_PATH env.

    Calls the legacy `fill_704b` (which auto-detects field prefixes
    and works for 704B + 704C templates alike). Raises
    SpineFormFillError on missing template or non-PDF output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    template_path = resolve_template_path(
        "704c", contract, "SPINE_704C_TEMPLATE_PATH",
    )
    rfq_data = _build_legacy_rfq_dict(quote, contract)

    from src.forms.reytech_filler_v4 import fill_704b, load_config
    try:
        config = load_config()
    except Exception as e:
        raise SpineFormFillError(f"could not load reytech_config.json: {e}")

    tmp_dir = tempfile.mkdtemp(prefix="spine_704c_")
    out_path = os.path.join(tmp_dir, "704c_filled.pdf")
    try:
        fill_704b(template_path, rfq_data, config, out_path)
        if not os.path.isfile(out_path):
            raise SpineFormFillError(
                f"fill_704b returned without writing {out_path}"
            )
        with open(out_path, "rb") as fh:
            data = fh.read()
    finally:
        try:
            os.remove(out_path)
            os.rmdir(tmp_dir)
        except Exception:
            pass

    if not data or len(data) < 1024 or data[:5] != b"%PDF-":
        raise SpineFormFillError(
            f"704C filler produced non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_704c_pdf"]
