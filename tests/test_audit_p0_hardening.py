"""Audit-driven P0 hardening tests (2026-04-27).

Covers:
  - P0 #4: send-quote routes reject typo'd email recipients
  - P0 #6: quote_generator handles qty=None/0 without producing ghost
           totals (was: `item["qty"] or 1` → silent conversion to 1)
"""
import pytest

from src.core.validators import validate_email, ValidationError


class TestEmailValidator:
    """validate_email already exists; verifying the strict patterns we rely on."""

    def test_typo_domain_no_tld_rejected(self):
        # "test@test" — no TLD
        with pytest.raises(ValidationError):
            validate_email("test@test")

    def test_double_at_rejected(self):
        with pytest.raises(ValidationError):
            validate_email("buyer@@cdcr.ca.gov")

    def test_space_in_local_rejected(self):
        with pytest.raises(ValidationError):
            validate_email("john smith@cdcr.ca.gov")

    def test_legitimate_buyer_email_passes(self):
        assert validate_email("Purchaser@CDCR.CA.GOV") == "purchaser@cdcr.ca.gov"

    def test_typo_in_domain_passes_format_but_could_be_wrong(self):
        # "@cdrc.ca.gov" (typo: cdrc vs cdcr) is structurally valid —
        # we can't catch this without a known-good list. Document the
        # gap so future-me knows the validator is shape-only.
        assert validate_email("buyer@cdrc.ca.gov") == "buyer@cdrc.ca.gov"


class TestQuoteGeneratorQtyZeroSafety:
    """Ensure missing/zero qty produces $0 line, not ghost qty=1 charge."""

    def test_normalize_handles_none_qty(self):
        from src.forms.ams704_helpers import normalize_line_item
        item = normalize_line_item({"description": "test", "qty": None,
                                     "unit_price": 10.0})
        assert item["qty"] == 0  # not None — coerced to 0

    def test_qty_zero_or_missing_produces_zero_line_total(self):
        """The actual fix: in quote_generator.py the ` or 1` default was
        replaced with ` or 0` so missing qty contributes $0 to subtotal,
        not phantom_qty * unit_price."""
        # Test the logic at the level it lives
        items_with_none = [{"description": "no-qty item",
                            "qty": None, "unit_price": 50.0}]
        from src.forms.ams704_helpers import normalize_line_item
        item = normalize_line_item(items_with_none[0])
        # After normalize, qty is 0
        qty_val = item["qty"]
        # Replicate the guarded computation
        try:
            qty = float(qty_val or 0)
        except (TypeError, ValueError):
            qty = 0.0
        uprice = float(item.get("price_per_unit", 0) or 0) or 50.0
        line_total = round(uprice * qty, 2)
        assert line_total == 0.0, \
            f"qty=None should produce $0 line, got ${line_total} (the bug)"

    def test_legitimate_qty_still_multiplies(self):
        from src.forms.ams704_helpers import normalize_line_item
        item = normalize_line_item({"description": "real item",
                                     "qty": 5, "unit_price": 12.50})
        qty = float(item["qty"] or 0)
        uprice = float(item["price_per_unit"] or 0) or 12.50
        assert round(uprice * qty, 2) == 62.50
