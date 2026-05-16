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

from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional

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

if TYPE_CHECKING:
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Quote-PDF adapter — uniform Renderer signature.
# ──────────────────────────────────────────────────────────────────────
#
# render_quote_pdf(quote) lives in src/spine/quote_pdf.py and does NOT
# take identity/today/flatten because the Quote PDF is the Spine's own
# (not an agency-mandated form). We wrap it here so FORM_REGISTRY can
# present a uniform call signature: (quote, identity=None, *, today=None,
# flatten=True) -> bytes. The kwargs are accepted-then-ignored.


def _render_quote_pdf_adapter(
    quote: "Quote",
    identity: Optional[ReytechIdentity] = None,
    *,
    today: Optional[datetime] = None,
    flatten: bool = True,
) -> bytes:
    """Adapter so render_quote_pdf can sit in FORM_REGISTRY beside the
    fill_*_pdf functions. identity/today/flatten are accepted-and-ignored
    — the Quote PDF derives everything from the Quote model alone, has
    no AcroForm to flatten, and uses datetime.now() for any timestamps
    it cares about (which the matching gate doesn't compare against)."""
    from src.spine.quote_pdf import render_quote_pdf
    return render_quote_pdf(quote)


# ──────────────────────────────────────────────────────────────────────
# FORM_REGISTRY — single source of truth for FormCode → renderer.
# ──────────────────────────────────────────────────────────────────────
#
# Keys MUST be members of src.spine.email_contract.FormCode literal.
# `test_every_registered_form_code_is_in_form_code_literal` enforces
# the forward direction (no orphans here); `test_cchcs_default_set_
# fully_registered` enforces the backward direction for the CCHCS
# required_forms set (CCHCS bids cannot ship without a renderer for
# every required form).
#
# Adding a new agency form:
#   1. Implement fill_<form>_pdf in its own module here.
#   2. Add the FormCode literal in src/spine/email_contract.py.
#   3. Register the renderer in FORM_REGISTRY below.
#   4. The architecture test will fail until all three exist —
#      that's the consumer-driven-contract gate at build time.
#
# Codes in the FormCode literal but NOT in FORM_REGISTRY (today: 703c,
# 704c, calrecycle_74, std_204, std_1000, dvbe_843, darfur, cuf) are
# *known-deferred* — they live in the legacy app or aren't yet needed.
# An EmailContract that declares them in required_forms causes the
# /package endpoint to refuse 409 with "renderer not registered" — the
# substrate gate that the legacy app never had.

Renderer = Callable[..., bytes]

FORM_REGISTRY: dict[str, Renderer] = {
    "703b":   fill_703b_pdf,
    "704b":   fill_704b_pdf,
    "bidpkg": fill_bidpkg_pdf,
    "quote":  _render_quote_pdf_adapter,
}


__all__ = [
    "ReytechIdentity",
    "SpineFormFillError",
    "fill_703b_pdf",
    "fill_704b_pdf",
    "fill_bidpkg_pdf",
    "FORM_REGISTRY",
    "Renderer",
]
