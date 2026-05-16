"""Agency-form fillers under the Spine matching gate.

Today the Spine renders only Reytech's own Quote PDF (src/spine/
quote_pdf.py). CCHCS bid responses require additional agency-
mandated forms — 703B (cover sheet + certifications), 704B (line-
item form), bid package, STD 204 / 1000, DVBE 843, CalRecycle 74,
Darfur Act, etc.

Each form lives in its own module here and exposes:

    fill_<form>_pdf(quote, identity, today=None) -> bytes

The function:
  1. Loads the blank template (src/spine/agency_forms/templates/).
  2. Fills the AcroForm fields from the Quote model + Reytech identity.
  3. Flattens the PDF (government convention; per 2026-05-15 web
     research — federal grants reject non-flattened, federal courts
     mandate flattening). Operator can request a fillable variant
     via the route's `?fillable=1` query param if needed.
  4. Runs a per-form matching gate that asserts every identity +
     model-derived value the operator depends on is present in the
     filled bytes. Same architectural pattern as
     render_quote_pdf's _verify_render_matches_model.
  5. Returns bytes. Caller decides whether to write to disk, attach
     to email, or stream to an HTTP response.

The fillers are stateless and pure — same (quote, identity, today)
input always produces logically-equivalent bytes (PDF metadata
timestamps vary, but the form-field values + visible text don't).

Architectural commitment:
  - The Spine substrate stores only the Quote model's fields. No
    `vendor_*` or `buyer_*` substrate columns. Identity comes from
    operator-config (env / config file), not from spine_quotes rows.
  - The matching gate is the same shape as quote_pdf: re-extract,
    compare cent-for-cent + identifier strings, raise on mismatch.
  - Government convention: send flat, support fillable as escape.
"""

from src.spine.agency_forms.cchcs_703b import (
    ReytechIdentity,
    SpineFormFillError,
    fill_703b_pdf,
)
from src.spine.agency_forms.cchcs_704b import (
    fill_704b_pdf,
)
from src.spine.agency_forms.cchcs_bidpkg import (
    fill_bidpkg_pdf,
)

__all__ = [
    "ReytechIdentity",
    "SpineFormFillError",
    "fill_703b_pdf",
    "fill_704b_pdf",
    "fill_bidpkg_pdf",
]
