"""Barstow CUF — Veterans Home of California, Barstow CUF documentation.

Spine-shaped wrapper around the legacy path-based `generate_barstow_cuf()`
in `src.forms.reytech_filler_v4`. This form has NO blank template — it is
generated from scratch with ReportLab — so the adapter only supplies the
output tempfile (no input template path) and a `config["company"]` dict
built from ReytechIdentity. `generate_barstow_cuf` reads exactly one
company key: `name`.

J2-2 (CalVet migration, 2026-05-31): required ONLY by the
`calvet_barstow` form set (the Veterans Home of California - Barstow adds
this Barstow-specific CUF on top of the standard CalVet set). Same
path-based temp-file bridge as `std_205.py`, minus the input template.

Architect-authorized: J2-2 new src/spine/agency_forms adapter +
FormCode per CLAUDE.md §0 Job #2 LAW 4.
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

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


def fill_barstow_cuf_pdf(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Render the Barstow Veterans Home CUF documentation (ReportLab).

    Args:
        quote:    Spine Quote. Not read by the generator (the Barstow CUF
                  is a fixed Yes/No questionnaire + company name); kept
                  for the uniform registry call shape.
        identity: ReytechIdentity. Defaults to `from_env()` if omitted.
                  Supplies the company name printed on the form.
        today / flatten / contract:
                  Accepted for uniform signature; unused by the generator.

    Returns:
        Generated PDF bytes.

    Raises:
        SpineFormFillError: if the generator writes nothing / non-PDF
            output.
    """
    if identity is None:
        identity = ReytechIdentity.from_env()

    config = {"company": {"name": identity.business_name}}
    rfq_data: dict = {}

    from src.forms.reytech_filler_v4 import generate_barstow_cuf

    tmp_dir = tempfile.mkdtemp(prefix="spine_barstow_cuf_")
    out_path = os.path.join(tmp_dir, "barstow_cuf.pdf")
    try:
        generate_barstow_cuf(rfq_data, config, out_path)
        if not os.path.isfile(out_path):
            raise SpineFormFillError(
                f"Barstow CUF generator returned without writing {out_path}"
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
            f"Barstow CUF generator produced non-PDF bytes "
            f"(len={len(data)}, head={data[:8]!r})"
        )
    return data


__all__ = ["fill_barstow_cuf_pdf"]
