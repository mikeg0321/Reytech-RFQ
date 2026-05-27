"""Pin: fill_std_204_pdf — Spine-shaped renderer wraps the legacy
fill_std204 + is registered in FORM_REGISTRY.

Pillar 4 / G10 (chrome MCP audit 2026-05-26): 2nd deferred renderer
registered. STD 204 is the most universal of the deferred set —
CalVet + DGS + DSH all require it; CCHCS bidpkg already fires it
internally.

Tests pin:
  1. Renderer is in FORM_REGISTRY under canonical 'std_204' key.
  2. Renderer accepts the uniform Renderer signature.
  3. Address parser splits the canonical Reytech address into
     street/city/state/zip parts correctly.
  4. Address parser fall-back: when the string doesn't match the
     expected pattern, the whole string goes into 'street' and the
     defaults stand.
  5. End-to-end: render produces valid PDF bytes.
  6. SpineFormFillError on missing template / non-PDF output.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote():
    from src.spine.model import Quote, LineItem
    return Quote(
        quote_id="q-std204-test",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="SOL-STD204-1",
        tax_rate_bps=775,
        line_items=[LineItem(
            line_no=1, description="x", mfg_number="M1",
            qty=1, uom="EA",
            cost_cents=0, unit_price_cents=0,
        )],
        status="parsed",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


# ─── Registry + signature ────────────────────────────────────────────


def test_std_204_registered_in_form_registry():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "std_204" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["std_204"])


def test_renderer_accepts_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_std_204_pdf
    sig = inspect.signature(fill_std_204_pdf)
    for kw in ("today", "flatten", "contract"):
        assert kw in sig.parameters, f"missing kwarg {kw!r}, sig {sig}"


# ─── Address parser ──────────────────────────────────────────────────


def test_address_parser_canonical_reytech():
    """The Reytech default address parses cleanly into 4 fields."""
    from src.spine.agency_forms.std_204 import _parse_address
    parts = _parse_address("30 Carnoustie Way, Trabuco Canyon, CA 92679")
    assert parts["street"] == "30 Carnoustie Way"
    assert parts["city"] == "Trabuco Canyon"
    assert parts["state"] == "CA"
    assert parts["zip"] == "92679"


def test_address_parser_handles_zip_plus_4():
    from src.spine.agency_forms.std_204 import _parse_address
    parts = _parse_address("100 Main St, Sacramento, CA 94203-1234")
    assert parts["zip"] == "94203-1234"


def test_address_parser_falls_back_on_unparseable():
    """When the address doesn't match the expected pattern, the whole
    string lands in `street` with safe defaults for the rest."""
    from src.spine.agency_forms.std_204 import _parse_address
    parts = _parse_address("PO Box 123 Trabuco Canyon CA")
    assert "PO Box 123" in parts["street"]
    # State defaults to CA (the legacy filler's default behavior).
    assert parts["state"] == "CA"


def test_address_parser_empty_string():
    from src.spine.agency_forms.std_204 import _parse_address
    parts = _parse_address("")
    assert parts == {"street": "", "city": "", "state": "CA", "zip": ""}


# ─── Render output ───────────────────────────────────────────────────


def test_renderer_returns_real_pdf_bytes():
    from src.spine.agency_forms import fill_std_204_pdf
    data = fill_std_204_pdf(_make_quote())
    assert isinstance(data, bytes)
    assert data[:5] == b"%PDF-", f"got {data[:8]!r}"
    assert len(data) > 1024


def test_raises_on_missing_template(monkeypatch):
    import pytest
    from src.spine.agency_forms import fill_std_204_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_std204",
        lambda r, p: None,
    )
    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_std_204_pdf(_make_quote())


def test_raises_on_non_pdf_output(monkeypatch):
    import io
    import pytest
    from src.spine.agency_forms import fill_std_204_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_std204",
        lambda r, p: io.BytesIO(b"junk"),
    )
    with pytest.raises(SpineFormFillError, match="non-PDF"):
        fill_std_204_pdf(_make_quote())
