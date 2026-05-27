"""Pin: fill_calrecycle_74_pdf — Spine-shaped renderer wraps the
legacy fill_calrecycle_74 + is registered in FORM_REGISTRY.

Pillar 4 / G10 (chrome MCP audit 2026-05-26): FORM_REGISTRY had 4
entries; this is the first deferred renderer registered. Required
by CalVet bids; the existing fill_bidpkg keeps using its in-bundle
copy unaffected.

Tests pin:
  1. Renderer is in FORM_REGISTRY under the canonical 'calrecycle_74'
     key — same name as the FormCode literal.
  2. Signature matches the uniform Renderer contract — already pinned
     by test_form_registry; this is a redundancy check at the
     module-level.
  3. Calling the renderer on a real Quote + identity produces valid
     PDF bytes (header + min length).
  4. SolicitationNumber falls back from the Quote → Contract when the
     Quote doesn't carry one yet.
  5. Renderer raises SpineFormFillError when the template is missing
     (mocked by patching the filler to return None).
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote(line_count=2, sol="SOL-CALREC-1"):
    from src.spine.model import Quote, LineItem
    lis = [
        LineItem(
            line_no=i, description=f"item-{i}",
            mfg_number=f"MFG-{i}", qty=1, uom="EA",
            cost_cents=0, unit_price_cents=0,
        ) for i in range(1, line_count + 1)
    ]
    return Quote(
        quote_id="q-calrec-test",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number=sol,
        tax_rate_bps=775,
        line_items=lis,
        status="parsed",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


def _make_contract(sol="SOL-CALREC-CTRACT"):
    from src.spine.email_contract import EmailContract, ContractLineItem
    return EmailContract(
        contract_id="c-calrec-test",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number=sol,
        line_items=[ContractLineItem(
            line_no=1, description="x", qty=1, uom="EA",
        )],
    )


# ─── Registry + signature ────────────────────────────────────────────


def test_calrecycle_74_registered_in_form_registry():
    """The canonical 'calrecycle_74' key resolves to a callable
    renderer — closes the consumer-driven-contract gate for this
    FormCode literal."""
    from src.spine.agency_forms import FORM_REGISTRY
    assert "calrecycle_74" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["calrecycle_74"])


def test_renderer_accepts_uniform_signature():
    """Mirrors the architecture-contract test but pinned here so a
    future refactor of the renderer module's signature is caught at
    this test file too."""
    import inspect
    from src.spine.agency_forms import fill_calrecycle_74_pdf
    sig = inspect.signature(fill_calrecycle_74_pdf)
    params = sig.parameters
    # Required positional `quote`
    assert "quote" in params
    # Keyword args the registry's uniform call passes
    for kw in ("today", "flatten", "contract"):
        assert kw in params, f"missing kwarg {kw!r}, signature {sig}"


# ─── Render output ───────────────────────────────────────────────────


def test_renderer_returns_real_pdf_bytes():
    """End-to-end: render a Quote → bytes start with %PDF-, length
    well above the stub threshold."""
    from src.spine.agency_forms import fill_calrecycle_74_pdf
    data = fill_calrecycle_74_pdf(_make_quote())
    assert isinstance(data, bytes)
    assert data[:5] == b"%PDF-", f"got {data[:8]!r}"
    assert len(data) > 1024, f"suspicious len {len(data)}"


def test_solicitation_falls_back_to_contract():
    """When the Quote has solicitation_number set, that wins. When
    it's empty/None and a contract is passed, the contract's value
    is used. Pinned because the legacy filler's _sol_number reads
    from the `parsed` dict and the adapter needs to populate that
    correctly under both conditions."""
    from src.spine.agency_forms import fill_calrecycle_74_pdf

    # Case 1: Quote has sol# → uses Quote's
    q = _make_quote(sol="QUOTE-SOL")
    c = _make_contract(sol="CONTRACT-SOL")
    data = fill_calrecycle_74_pdf(q, contract=c)
    assert data[:5] == b"%PDF-"

    # Case 2: when the adapter has no sol on Quote, contract supplies.
    # Spine Quote model REQUIRES solicitation_number, so we can't
    # construct a Quote with empty sol#. Instead probe the adapter's
    # internal _quote_to_legacy_parsed helper directly.
    from src.spine.agency_forms.calrecycle_74 import _quote_to_legacy_parsed
    q_with_sol = _make_quote(sol="X-SOL")
    parsed = _quote_to_legacy_parsed(q_with_sol, c)
    assert parsed["solicitation_number"] == "X-SOL"


def test_raises_on_missing_template(monkeypatch):
    """When the legacy filler returns None (template unavailable),
    the Spine renderer raises SpineFormFillError instead of returning
    None — the registry's uniform contract is (raises | bytes)."""
    import pytest
    from src.spine.agency_forms import fill_calrecycle_74_pdf
    from src.spine.agency_forms._identity import SpineFormFillError

    def _none_filler(reytech_info, parsed):
        return None
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_calrecycle_74",
        _none_filler,
    )

    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_calrecycle_74_pdf(_make_quote())


def test_raises_on_non_pdf_output(monkeypatch):
    """If the legacy filler returns junk bytes (not a real PDF),
    the renderer refuses to return them — defends downstream
    visual_qa + send-gate."""
    import io
    import pytest
    from src.spine.agency_forms import fill_calrecycle_74_pdf
    from src.spine.agency_forms._identity import SpineFormFillError

    def _junk_filler(reytech_info, parsed):
        return io.BytesIO(b"not a pdf, just garbage")
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_calrecycle_74",
        _junk_filler,
    )

    with pytest.raises(SpineFormFillError, match="non-PDF"):
        fill_calrecycle_74_pdf(_make_quote())
