"""Pin: 703c + 704c Spine renderers + buyer-supplied template resolver.

Chrome MCP audit 2026-05-27 / 703c+704c (Architect approval): CCHCS
sometimes ships alternate 703C / 704C templates with each bid email
rather than expecting Reytech to use bundled blanks. Spine renderer
resolves template at RENDER time via:

  1. Env override: SPINE_703C_TEMPLATE_PATH / SPINE_704C_TEMPLATE_PATH
  2. Contract attachment_refs filename match
  3. Raise SpineFormFillError

Tests pin:
  1. Both registered in FORM_REGISTRY.
  2. Both accept uniform Renderer signature.
  3. Resolver: env override wins
  4. Resolver: attachment_refs filename match (case-insensitive, '703c'
     boundary so '703b' doesn't match '703c' and vice versa)
  5. Resolver: missing template raises with a clear message
  6. Resolver: env var set but path doesn't exist raises
  7. Resolver: attachment_refs has the right filename but path missing
     raises
  8. End-to-end smoke: with env pointed at the existing 703b template
     (close enough — fill_703c auto-detects + redirects to fill_703b
     when the template has 703b-prefixed fields), the renderer returns
     real PDF bytes.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote(sol="SOL-ALT-1"):
    from src.spine.model import Quote, LineItem
    return Quote(
        quote_id="q-alt-test",
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


def _make_contract(attachment_refs=()):
    from src.spine.email_contract import EmailContract, ContractLineItem
    return EmailContract(
        contract_id="c-alt-test",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="SOL-ALT-1",
        line_items=[ContractLineItem(
            line_no=1, description="x", qty=1, uom="EA",
        )],
        attachment_refs=list(attachment_refs),
    )


# ─── Registry + signature ────────────────────────────────────────────


def test_703c_704c_registered_in_form_registry():
    from src.spine.agency_forms import FORM_REGISTRY
    assert "703c" in FORM_REGISTRY
    assert "704c" in FORM_REGISTRY
    assert callable(FORM_REGISTRY["703c"])
    assert callable(FORM_REGISTRY["704c"])


def test_703c_704c_uniform_signature():
    import inspect
    from src.spine.agency_forms import fill_703c_pdf, fill_704c_pdf
    for fn in (fill_703c_pdf, fill_704c_pdf):
        sig = inspect.signature(fn)
        for kw in ("today", "flatten", "contract"):
            assert kw in sig.parameters, (
                f"{fn.__name__} missing {kw!r}, sig {sig}"
            )


# ─── Resolver ────────────────────────────────────────────────────────


def test_resolver_env_override_wins(monkeypatch, tmp_path):
    """Env path takes precedence even when contract.attachment_refs
    has a candidate."""
    from src.spine.agency_forms._template_resolver import resolve_template_path

    # Create a real env-pointed file
    env_blank = tmp_path / "from_env.pdf"
    env_blank.write_bytes(b"%PDF-1.4\n" + b"x" * 1100)

    # ALSO put a 703c-named file in attachment_refs
    attached = tmp_path / "buyer_703c_attached.pdf"
    attached.write_bytes(b"%PDF-1.4\n" + b"x" * 1100)
    contract = _make_contract(attachment_refs=[str(attached)])

    monkeypatch.setenv("SPINE_703C_TEMPLATE_PATH", str(env_blank))
    path = resolve_template_path("703c", contract, "SPINE_703C_TEMPLATE_PATH")
    # Env path wins.
    assert path.endswith("from_env.pdf")


def test_resolver_attachment_refs_filename_match(monkeypatch, tmp_path):
    """When env is unset, scan attachment_refs for a filename containing
    the form code (case-insensitive, word boundary)."""
    monkeypatch.delenv("SPINE_703C_TEMPLATE_PATH", raising=False)
    from src.spine.agency_forms._template_resolver import resolve_template_path

    f = tmp_path / "AMS_703C_blank_rev2025.pdf"
    f.write_bytes(b"%PDF-1.4\n" + b"x" * 1100)
    contract = _make_contract(attachment_refs=[
        str(tmp_path / "unrelated.pdf"),  # doesn't match
        str(f),
        str(tmp_path / "also_unrelated.pdf"),
    ])
    path = resolve_template_path("703c", contract, "SPINE_703C_TEMPLATE_PATH")
    assert path.endswith("AMS_703C_blank_rev2025.pdf")


def test_resolver_word_boundary_703b_doesnt_match_703c(monkeypatch, tmp_path):
    """A '703b' filename must NOT satisfy a '703c' resolver lookup —
    confused-token bug class."""
    monkeypatch.delenv("SPINE_703C_TEMPLATE_PATH", raising=False)
    from src.spine.agency_forms._template_resolver import resolve_template_path
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    import pytest

    f_703b = tmp_path / "buyer_703b_blank.pdf"
    f_703b.write_bytes(b"%PDF-1.4\n" + b"x" * 1100)
    contract = _make_contract(attachment_refs=[str(f_703b)])

    with pytest.raises(SpineFormFillError, match="703C template not resolvable"):
        resolve_template_path("703c", contract, "SPINE_703C_TEMPLATE_PATH")


def test_resolver_raises_when_no_template(monkeypatch):
    monkeypatch.delenv("SPINE_703C_TEMPLATE_PATH", raising=False)
    from src.spine.agency_forms._template_resolver import resolve_template_path
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    import pytest

    with pytest.raises(SpineFormFillError, match="703C template not resolvable"):
        resolve_template_path("703c", None, "SPINE_703C_TEMPLATE_PATH")


def test_resolver_raises_when_env_path_missing(monkeypatch):
    """If env is set but the file doesn't exist, the resolver raises
    with a clear message instead of falling through silently."""
    monkeypatch.setenv("SPINE_703C_TEMPLATE_PATH", "/does/not/exist.pdf")
    from src.spine.agency_forms._template_resolver import resolve_template_path
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    import pytest

    with pytest.raises(SpineFormFillError, match="not a.*readable"):
        resolve_template_path("703c", None, "SPINE_703C_TEMPLATE_PATH")


def test_resolver_raises_when_attachment_path_missing(monkeypatch, tmp_path):
    """attachment_refs has the right filename but file is gone — raise
    rather than fall through to the no-template error (the config
    pointed somewhere; that 'somewhere' is broken)."""
    monkeypatch.delenv("SPINE_703C_TEMPLATE_PATH", raising=False)
    from src.spine.agency_forms._template_resolver import resolve_template_path
    from src.spine.agency_forms.cchcs_703b import SpineFormFillError
    import pytest

    ghost = str(tmp_path / "buyer_703c_blank.pdf")  # never created
    contract = _make_contract(attachment_refs=[ghost])

    with pytest.raises(SpineFormFillError, match="not readable"):
        resolve_template_path("703c", contract, "SPINE_703C_TEMPLATE_PATH")


# ─── End-to-end smoke ────────────────────────────────────────────────


def test_703c_end_to_end_with_env_template(monkeypatch):
    """Point env at the bundled 703b template (close enough — fill_703c
    auto-detects field prefix and redirects to fill_703b when the
    template uses 703b-prefixed fields). Verifies the full render path
    works without needing a real 703c blank in the repo."""
    from pathlib import Path
    bundled_703b = Path(__file__).parent.parent.parent.joinpath(
        "src", "spine", "agency_forms", "templates", "703b_blank.pdf"
    )
    if not bundled_703b.is_file():
        import pytest
        pytest.skip("bundled 703b template not present")
    monkeypatch.setenv("SPINE_703C_TEMPLATE_PATH", str(bundled_703b))

    from src.spine.agency_forms import fill_703c_pdf
    data = fill_703c_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024


def test_704c_end_to_end_with_env_template(monkeypatch):
    """Same shape — env-pointed bundled 704b template exercises the
    full render path for 704c (legacy fill_704b auto-detects layout
    via template_registry.get_profile)."""
    from pathlib import Path
    bundled_704b = Path(__file__).parent.parent.parent.joinpath(
        "src", "spine", "agency_forms", "templates", "704b_blank.pdf"
    )
    if not bundled_704b.is_file():
        import pytest
        pytest.skip("bundled 704b template not present")
    monkeypatch.setenv("SPINE_704C_TEMPLATE_PATH", str(bundled_704b))

    from src.spine.agency_forms import fill_704c_pdf
    data = fill_704c_pdf(_make_quote())
    assert data[:5] == b"%PDF-"
    assert len(data) > 1024
