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

from src.spine.agency_forms._identity import (
    ReytechIdentity,
    SpineFormFillError,
)
from src.spine.agency_forms.std_204 import (
    fill_std_204_pdf,
)
from src.spine.agency_forms.dvbe_843 import (
    fill_dvbe_843_pdf,
)
from src.spine.agency_forms.darfur import (
    fill_darfur_pdf,
)
from src.spine.agency_forms.calrecycle_74 import (
    fill_calrecycle_74_pdf,
)
from src.spine.agency_forms.std_1000 import (
    fill_std_1000_pdf,
)
from src.spine.agency_forms.cuf import (
    fill_cuf_pdf,
)
# PR-Job1-D (2026-05-27): the five per-form CCHCS adapter shims that
# previously lived under ``src/spine/agency_forms/cchcs_*.py`` were
# folded into ``src/spine/forms_render.py`` — same delegation, one home.
# FORM_REGISTRY entries for 703b/703c/704b/704c/bidpkg now point at the
# ``render_*_pdf`` functions there.
from src.spine.forms_render import (
    render_703b_pdf,
    render_703c_pdf,
    render_704b_pdf,
    render_704c_pdf,
    render_bidpkg_pdf,
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
    contract=None,
) -> bytes:
    """Adapter so render_quote_pdf can sit in FORM_REGISTRY beside the
    fill_*_pdf functions. identity/flatten are accepted-and-ignored —
    the Quote PDF derives identity from its own constants and has no
    AcroForm to flatten. `today` and `contract` ARE passed through:
    `today` for deterministic test rendering, `contract` for buyer-side
    Bill-to / Ship-to / RFQ-title (template-match PR #1052)."""
    from src.spine.quote_pdf import render_quote_pdf
    return render_quote_pdf(quote, contract=contract, today=today)


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
# Every FormCode literal member is currently registered: 703b / 703c /
# 704b / 704c / bidpkg / quote / std_204 / std_1000 / dvbe_843 / darfur /
# calrecycle_74 / cuf. An EmailContract that declares an unregistered
# code (the substrate gate against a future typo-only code addition)
# would cause /package to refuse 409 with "renderer not registered".

Renderer = Callable[..., bytes]

FORM_REGISTRY: dict[str, Renderer] = {
    # PR-Job1-D (2026-05-27): the CCHCS-specific entries point at
    # ``forms_render.py``'s ``render_*_pdf`` functions. 703b/704b/bidpkg
    # delegate through the bundled standalone-form-set renderer
    # (``render_cchcs_forms_via_legacy``) which classifies the buyer's
    # template PDFs and runs the verified legacy fillers in
    # ``src.forms.reytech_filler_v4``. 703c/704c go through the
    # ``_template_resolver`` (env override or attachment_refs match)
    # and call the legacy fillers directly. Same delegation, single home.
    "703b":          render_703b_pdf,
    "704b":          render_704b_pdf,
    "bidpkg":        render_bidpkg_pdf,
    "703c":          render_703c_pdf,
    "704c":          render_704c_pdf,
    "quote":         _render_quote_pdf_adapter,
    # Pillar 4 / G10: STD 204 Payee Data Record. Most universal of
    # the deferred renderers — required by CalVet + DGS + DSH and
    # already fires inside the CCHCS bidpkg via fill_bid_package.
    # Standalone registration unblocks every non-CCHCS agency
    # response path.
    "std_204":       fill_std_204_pdf,
    # Pillar 4 / G10: DVBE 843 declaration. Required by CalVet + DGS
    # for every prime-DVBE bid. CCHCS bidpkg already fires it
    # internally; standalone here unblocks non-CCHCS paths.
    "dvbe_843":      fill_dvbe_843_pdf,
    # Pillar 4 / G10: Darfur Contracting Act certification. Required
    # by CalVet + DGS + most CA agency bids. Reytech is not a
    # scrutinized company per CA Gov Code §10477 — fills the
    # non-scrutinized declaration on page 1.
    "darfur":        fill_darfur_pdf,
    # Pillar 4 / G10: CalRecycle 74 Postconsumer Recycled-Content
    # Certification. Required by CalVet + DGS; also fires inside the
    # CCHCS bidpkg via fill_bid_package.
    "calrecycle_74": fill_calrecycle_74_pdf,
    # Pillar 4 / G10: STD 1000 GenAI Disclosure. Required by most CA
    # agency bids. Reytech does not use GenAI in supplied products —
    # ticks "No" + skips items 1-6 per the form's instruction.
    "std_1000":      fill_std_1000_pdf,
    # Pillar 4 / G10: CV 012 Commercially Useful Function (DVBE
    # attestation). Reytech is DVBE-certified + performs the function
    # directly (inventory + logistics + delivery) — all 6 questions
    # answered "Yes".
    "cuf":           fill_cuf_pdf,
}


__all__ = [
    "ReytechIdentity",
    "SpineFormFillError",
    "render_703b_pdf",
    "render_704b_pdf",
    "render_bidpkg_pdf",
    "render_703c_pdf",
    "render_704c_pdf",
    "fill_std_204_pdf",
    "fill_dvbe_843_pdf",
    "fill_darfur_pdf",
    "fill_calrecycle_74_pdf",
    "fill_std_1000_pdf",
    "fill_cuf_pdf",
    "FORM_REGISTRY",
    "Renderer",
]
