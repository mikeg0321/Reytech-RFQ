"""Pin: dvbe_843 + darfur Spine renderers — both wrap their legacy
fillers + are registered in FORM_REGISTRY.

Pillar 4 / G10: 3rd + 4th deferred renderers registered. Both are
CalVet/DGS identity certs; CCHCS bidpkg already fires them
internally via fill_bid_package.

After this PR: 4 of 8 deferred renderers shipped (calrecycle_74,
std_204, dvbe_843, darfur). Remaining 4 (std_1000, cuf, 703c, 704c)
need filler implementations extracted from the packet — separate work.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote(sol="SOL-IDCERT-1"):
    from src.spine.model import Quote, LineItem
    return Quote(
        quote_id="q-idcert-test",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number=sol,
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


# ─── DVBE 843 ────────────────────────────────────────────────────────


def test_dvbe_843_registered_in_form_registry():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "dvbe_843" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["dvbe_843"])


def test_dvbe_843_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_dvbe_843_pdf
    sig = inspect.signature(fill_dvbe_843_pdf)
    for kw in ("today", "flatten", "contract"):
        assert kw in sig.parameters, f"missing {kw!r}, sig {sig}"


def test_dvbe_843_returns_real_pdf_bytes():
    from src.spine.agency_forms import fill_dvbe_843_pdf
    data = fill_dvbe_843_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024


def test_dvbe_843_raises_on_missing_template(monkeypatch):
    import pytest
    from src.spine.agency_forms import fill_dvbe_843_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_dvbe_843",
        lambda r, p: None,
    )
    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_dvbe_843_pdf(_make_quote())


def test_dvbe_843_reads_solicitation_from_quote(monkeypatch):
    """DVBE 843's legacy filler reads `parsed['solicitation_number']`.
    The adapter must populate it from the Quote (or contract fallback)."""
    captured = {}

    def _spy(reytech_info, parsed):
        captured["parsed"] = parsed
        import io
        # Return a minimal valid PDF stub for the size check.
        return io.BytesIO(b"%PDF-1.4\n" + b"x" * 1100)

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_dvbe_843", _spy,
    )
    from src.spine.agency_forms import fill_dvbe_843_pdf
    fill_dvbe_843_pdf(_make_quote(sol="DVBE-SOL-99"))
    assert captured["parsed"]["solicitation_number"] == "DVBE-SOL-99"


# ─── Darfur Act ──────────────────────────────────────────────────────


def test_darfur_registered_in_form_registry():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "darfur" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["darfur"])


def test_darfur_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_darfur_pdf
    sig = inspect.signature(fill_darfur_pdf)
    for kw in ("today", "flatten", "contract"):
        assert kw in sig.parameters, f"missing {kw!r}, sig {sig}"


def test_darfur_returns_real_pdf_bytes():
    from src.spine.agency_forms import fill_darfur_pdf
    data = fill_darfur_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024


def test_darfur_raises_on_missing_template(monkeypatch):
    import pytest
    from src.spine.agency_forms import fill_darfur_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_darfur_act",
        lambda r, p: None,
    )
    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_darfur_pdf(_make_quote())


def test_darfur_identity_block_populated(monkeypatch):
    """Darfur is purely vendor identity — the adapter must pass FEIN
    and contact_person through."""
    captured = {}

    def _spy(reytech_info, parsed):
        captured["info"] = reytech_info
        import io
        return io.BytesIO(b"%PDF-1.4\n" + b"x" * 1100)

    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_darfur_act", _spy,
    )
    from src.spine.agency_forms import fill_darfur_pdf
    from src.spine.agency_forms.cchcs_703b import ReytechIdentity
    fill_darfur_pdf(
        _make_quote(),
        identity=ReytechIdentity(
            business_name="Test Co", fein="12-3456789",
            contact_person="Test Owner", title="CEO",
        ),
    )
    assert captured["info"]["company_name"] == "Test Co"
    assert captured["info"]["fein"] == "12-3456789"
    assert captured["info"]["representative"] == "Test Owner"
    assert captured["info"]["title"] == "CEO"
