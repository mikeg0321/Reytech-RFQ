"""Legacy → Spine translator tests.

Covers every documented alias-resolution rule plus the Russ test
fixture round-trip. If any of the 19 substrate failure classes can
re-enter the Spine through the translator, a test here should catch it.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.spine import QuoteStatus
from src.spine_bridge import (
    LegacyTranslationResult,
    TranslationIssue,
    translate_legacy_quote,
)


FIXTURES = Path(__file__).parent / "fixtures"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ──────────────────────────────────────────────────────────────────────
# Russ fixture — the real-shape no-bid test asset.
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def russ_legacy() -> dict:
    return json.loads((FIXTURES / "legacy_russ_no_bid_test.json").read_text())


def test_russ_fixture_translates_cleanly(russ_legacy):
    """The Russ no-bid test fixture must produce a valid Spine Quote.

    7 line items, all aliases present and consistent, tax_rate=0.0775.
    Translator should accept it and produce a 'parsed' Spine Quote.
    """
    result = translate_legacy_quote(russ_legacy)
    assert result.ok, (
        f"Russ fixture failed to translate. Errors:\n"
        + "\n".join(f"  {i.field_path}: {i.detail}" for i in result.errors())
    )
    q = result.quote
    assert q is not None
    assert q.status == QuoteStatus.PARSED
    assert q.agency == "CCHCS"
    assert "Test" in q.facility
    assert q.solicitation_number == "10844444"  # PREQ prefix stripped
    assert q.tax_rate_bps == 775  # 0.0775 → 775 bps
    assert len(q.line_items) == 7


def test_russ_fixture_records_dropped_legacy_fields(russ_legacy):
    """Audit-trail behavior: every legacy alias/orphan should be noted."""
    result = translate_legacy_quote(russ_legacy)
    assert result.ok
    issue_paths = {i.field_path for i in result.issues}

    # Shipping fields dropped (Charter rule #7).
    assert "shipping_option" in issue_paths
    assert "shipping_amount" in issue_paths
    assert "delivery_option" in issue_paths

    # markup_pct dropped on every line (derived in Spine).
    markup_drops = [i for i in result.issues if i.field_path.endswith(".markup_pct")]
    assert len(markup_drops) == 7  # one per line item


def test_russ_quote_renders_to_valid_pdf(russ_legacy):
    """End-to-end: translate → render. Closes the no-bid → test loop."""
    from src.spine import render_quote_pdf

    result = translate_legacy_quote(russ_legacy)
    assert result.ok
    pdf_bytes = render_quote_pdf(result.quote)
    assert pdf_bytes.startswith(b"%PDF-")
    # And it must be a real PDF that pdfplumber can parse.
    import io

    import pdfplumber
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = "\n".join(p.extract_text() for p in pdf.pages if p.extract_text())
    assert "Test" in text  # facility shown
    assert "7.75%" in text  # tax rate label rendered


# ──────────────────────────────────────────────────────────────────────
# Alias resolution — closes findings #1, #2, #3.
# ──────────────────────────────────────────────────────────────────────


def _minimal_legacy(**overrides) -> dict:
    """Minimal legacy dict that translates successfully unless overridden."""
    base = {
        "id": "rfq_minimal_test",
        "institution": "CCHCS-TestSite",
        "facility": "Test",
        "solicitation_number": "PREQ-12345",
        "tax_rate": 0.0825,
        "line_items": [{
            "line_no": 1,
            "description": "Test item",
            "qty": 1,
            "uom": "EA",
            "supplier_cost": 5.00,
            "unit_price": 10.00,
        }],
    }
    base.update(overrides)
    return base


def test_unit_price_alias_first_positive_wins():
    """unit_price > 0 wins even if other aliases are also set."""
    legacy = _minimal_legacy(line_items=[{
        "description": "ambiguous",
        "qty": 1, "uom": "EA",
        "supplier_cost": 1.00,
        "unit_price": 5.00,
        "bid_price": 7.00,
        "price_per_unit": 3.00,
        "our_price": 4.00,
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.line_items[0].unit_price_cents == 500  # unit_price wins


def test_unit_price_falls_back_to_bid_price():
    legacy = _minimal_legacy(line_items=[{
        "description": "no unit_price",
        "qty": 1, "uom": "EA",
        "supplier_cost": 1.00,
        "bid_price": 7.00,
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.line_items[0].unit_price_cents == 700


def test_unit_price_falls_back_to_price_per_unit():
    legacy = _minimal_legacy(line_items=[{
        "description": "only price_per_unit",
        "qty": 1, "uom": "EA",
        "supplier_cost": 1.00,
        "price_per_unit": 3.00,
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.line_items[0].unit_price_cents == 300


def test_unit_price_derives_from_cost_times_markup_when_no_price_set():
    legacy = _minimal_legacy(line_items=[{
        "description": "only cost + markup",
        "qty": 1, "uom": "EA",
        "supplier_cost": 4.00,
        "markup_pct": 25.0,
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    # 4.00 × 1.25 = 5.00 → 500 cents
    assert result.quote.line_items[0].unit_price_cents == 500
    # And a warning records this derivation so an operator can audit.
    derivation_notes = [
        i for i in result.issues
        if "derived from cost" in i.detail
    ]
    assert len(derivation_notes) == 1


def test_translation_fails_when_no_usable_unit_price():
    """If a line has no price AND no cost+markup, refuse to invent zero."""
    legacy = _minimal_legacy(line_items=[{
        "description": "naked", "qty": 1, "uom": "EA",
        # no cost, no price, no markup
    }])
    result = translate_legacy_quote(legacy)
    assert not result.ok
    assert any("usable unit price" in i.detail for i in result.errors())


def test_divergent_aliases_produce_warning():
    """If unit_price and bid_price disagree, record the divergence."""
    legacy = _minimal_legacy(line_items=[{
        "description": "split",
        "qty": 1, "uom": "EA",
        "supplier_cost": 1.00,
        "unit_price": 5.00,
        "bid_price": 9.99,
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    div_warnings = [
        i for i in result.warnings()
        if "divergent unit_price alias" in i.detail
    ]
    assert len(div_warnings) == 1


# ──────────────────────────────────────────────────────────────────────
# Shipping / markup drops — closes findings #17 / qty-clobbers-markup.
# ──────────────────────────────────────────────────────────────────────


def test_shipping_fields_dropped_with_info_note():
    legacy = _minimal_legacy(
        shipping_option="included",
        shipping_amount=42,
        delivery_option="fob_dest",
    )
    result = translate_legacy_quote(legacy)
    assert result.ok
    info_paths = {i.field_path for i in result.issues if i.severity == "info"}
    assert "shipping_option" in info_paths
    assert "shipping_amount" in info_paths
    assert "delivery_option" in info_paths


def test_markup_pct_dropped_from_line_items():
    legacy = _minimal_legacy(line_items=[{
        "description": "x",
        "qty": 1, "uom": "EA",
        "supplier_cost": 5.00,
        "unit_price": 10.00,
        "markup_pct": 100.0,  # legacy says 100%
    }])
    result = translate_legacy_quote(legacy)
    assert result.ok
    li = result.quote.line_items[0]
    # The Spine derives markup display from unit_price/cost:
    # (10 - 5)/5 × 100 = 100% — coincidentally the same here.
    assert li.markup_pct_display == 100.0
    # And the legacy markup_pct was recorded as dropped.
    drops = [i for i in result.issues if i.field_path.endswith(".markup_pct")]
    assert len(drops) == 1


# ──────────────────────────────────────────────────────────────────────
# Tax rate resolution — closes finding #15, #16.
# ──────────────────────────────────────────────────────────────────────


def test_tax_rate_decimal_form_converts_correctly():
    legacy = _minimal_legacy(tax_rate=0.0825)
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.tax_rate_bps == 825


def test_tax_rate_percent_form_converts_correctly():
    """8.25 (percent form) and 0.0825 (decimal form) should both work."""
    legacy = _minimal_legacy(tax_rate=8.25)
    legacy.pop("tax_rate", None)
    legacy["tax_rate"] = 8.25
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.tax_rate_bps == 825


def test_tax_rate_bps_integer_preferred():
    legacy = _minimal_legacy(tax_rate_bps=825, tax_rate=0.075)
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.tax_rate_bps == 825


def test_tax_rate_in_extra_catchall_promoted_with_warning():
    legacy = _minimal_legacy()
    legacy.pop("tax_rate")
    legacy["extra"] = {"tax_rate": 0.0825}
    result = translate_legacy_quote(legacy)
    assert result.ok
    assert result.quote.tax_rate_bps == 825
    promo = [i for i in result.warnings() if "lived in extra" in i.detail]
    assert len(promo) == 1


def test_missing_tax_rate_blocks_translation():
    legacy = _minimal_legacy()
    legacy.pop("tax_rate")
    result = translate_legacy_quote(legacy)
    assert not result.ok
    assert any("tax_rate_bps" in i.field_path for i in result.errors())


# ──────────────────────────────────────────────────────────────────────
# Solicitation # normalization — mirrors AV-1 PREQ-strip rule.
# ──────────────────────────────────────────────────────────────────────


def test_preq_prefix_stripped_from_solicitation():
    for prefix in ("PREQ ", "PREQ-", "preq ", "PREQ"):
        legacy = _minimal_legacy(solicitation_number=f"{prefix}10847262")
        result = translate_legacy_quote(legacy)
        assert result.ok, (
            f"prefix {prefix!r}: errors = "
            f"{[i.detail for i in result.errors()]}"
        )
        assert result.quote.solicitation_number == "10847262", (
            f"prefix {prefix!r}: got {result.quote.solicitation_number!r}"
        )


# ──────────────────────────────────────────────────────────────────────
# Misc invariants
# ──────────────────────────────────────────────────────────────────────


def test_uom_legacy_variants_canonicalize():
    for raw, expected in [
        ("Each", "EA"), ("ea.", "EA"), ("EACH", "EA"),
        ("Box", "BX"), ("box", "BX"),
        ("Case", "CS"), ("Pack", "PK"), ("Pair", "PR"),
        ("dozen", "DZ"),
    ]:
        legacy = _minimal_legacy(line_items=[{
            "description": "x", "qty": 1, "uom": raw,
            "supplier_cost": 1.00, "unit_price": 2.00,
        }])
        result = translate_legacy_quote(legacy)
        assert result.ok, f"uom={raw!r}: {[i.detail for i in result.errors()]}"
        assert result.quote.line_items[0].uom == expected, (
            f"uom={raw!r}: got {result.quote.line_items[0].uom!r}"
        )


def test_unknown_agency_blocks_translation():
    result = translate_legacy_quote(_minimal_legacy(), agency="CalVet")
    assert not result.ok
    assert any("agency" in i.field_path for i in result.errors())


def test_translation_does_not_mutate_input():
    """The translator must be pure — caller's legacy dict is unchanged."""
    import copy
    legacy = _minimal_legacy()
    before = copy.deepcopy(legacy)
    _ = translate_legacy_quote(legacy)
    assert legacy == before
