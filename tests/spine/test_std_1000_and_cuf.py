"""Pin: std_1000 + cuf Spine renderers — both wrap their new native
fillers + are registered in FORM_REGISTRY.

Pillar 4 / G10: 5th + 6th deferred renderers registered. Both have
templates in `data/templates/` but no standalone legacy fillers
existed before this PR. Built native fillers using the existing
`_fill_and_serialize` pattern.

After this PR: 6 of 8 deferred renderers shipped (calrecycle_74,
std_204, dvbe_843, darfur, std_1000, cuf). Remaining 2 (703c, 704c)
need template-from-attachment-refs architecture — separate work.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote(sol="SOL-X"):
    from src.spine.model import Quote, LineItem
    return Quote(
        quote_id="q-test",
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


# ─── STD 1000 ────────────────────────────────────────────────────────


def test_std_1000_registered():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "std_1000" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["std_1000"])


def test_std_1000_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_std_1000_pdf
    sig = inspect.signature(fill_std_1000_pdf)
    for kw in ("today", "flatten", "contract"):
        assert kw in sig.parameters, f"missing {kw!r}"


def test_std_1000_returns_real_pdf():
    from src.spine.agency_forms import fill_std_1000_pdf
    data = fill_std_1000_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024


def test_std_1000_passes_sol_through(monkeypatch):
    captured = {}
    def _spy(reytech_info, parsed):
        captured["parsed"] = parsed
        captured["info"] = reytech_info
        import io
        return io.BytesIO(b"%PDF-1.4\n" + b"x" * 1100)
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_std_1000", _spy,
    )
    from src.spine.agency_forms import fill_std_1000_pdf
    fill_std_1000_pdf(_make_quote(sol="STD1000-SOL-7"))
    assert captured["parsed"]["header"]["solicitation_number"] == "STD1000-SOL-7"
    # Address parser should populate parts.
    assert captured["info"]["state"] == "CA"


def test_std_1000_raises_on_missing_template(monkeypatch):
    import pytest
    from src.spine.agency_forms import fill_std_1000_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_std_1000",
        lambda r, p: None,
    )
    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_std_1000_pdf(_make_quote())


# ─── CV 012 CUF ──────────────────────────────────────────────────────


def test_cuf_registered():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "cuf" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["cuf"])


def test_cuf_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_cuf_pdf
    sig = inspect.signature(fill_cuf_pdf)
    for kw in ("today", "flatten", "contract"):
        assert kw in sig.parameters


def test_cuf_returns_real_pdf():
    from src.spine.agency_forms import fill_cuf_pdf
    data = fill_cuf_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024


def test_cuf_passes_identity_through(monkeypatch):
    captured = {}
    def _spy(reytech_info, parsed):
        captured["info"] = reytech_info
        captured["parsed"] = parsed
        import io
        return io.BytesIO(b"%PDF-1.4\n" + b"x" * 1100)
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_cuf", _spy,
    )
    from src.spine.agency_forms import fill_cuf_pdf
    from src.spine.agency_forms.cchcs_703b import ReytechIdentity
    fill_cuf_pdf(
        _make_quote(sol="CUF-SOL-3"),
        identity=ReytechIdentity(
            business_name="Test Co",
            contact_person="Tester",
            title="Test Title",
            cert_number="999999",
        ),
    )
    assert captured["info"]["company_name"] == "Test Co"
    assert captured["info"]["representative"] == "Tester"
    assert captured["info"]["title"] == "Test Title"
    assert captured["info"]["cert_number"] == "999999"
    assert captured["parsed"]["header"]["solicitation_number"] == "CUF-SOL-3"


def test_cuf_raises_on_missing_template(monkeypatch):
    import pytest
    from src.spine.agency_forms import fill_cuf_pdf
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    monkeypatch.setattr(
        "src.forms.cchcs_attachment_fillers.fill_cuf",
        lambda r, p: None,
    )
    with pytest.raises(SpineFormFillError, match="template missing"):
        fill_cuf_pdf(_make_quote())
