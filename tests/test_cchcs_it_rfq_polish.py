"""Regression: PR-A polish fixes for the LPA IT RFQ fill engine.

Closes gap-report items E/B/C against 2026-04-22 RFQ 10840486:
  E. Price-key fallback — unit_price/bid_price/price_per_unit/our_price/...
  B. Supplier state/zip populate Text7/Text8 (short-form fields)
  C. Due date normalized to US m/d/yyyy on `before` field

Plus classifier hardening:
  - Page-1 header text detects LPA even when fields flattened/renamed
  - Filename pattern supplementary signal
"""
from pathlib import Path

import pytest

from src.forms.reytech_filler_v4 import (
    _is_cchcs_it_rfq,
    _lpa_filename_signal,
    _lpa_item_price,
    _lpa_page1_text_signal,
    _us_date,
    fill_cchcs_it_rfq,
)

FIXTURES = Path(__file__).parent / "fixtures"
BLANK_LPA = FIXTURES / "cchcs_it_rfq_blank.pdf"


@pytest.fixture
def reytech_config():
    return {
        "company": {
            "name": "Reytech Inc.",
            "owner": "Michael Guadan",
            "title": "Owner",
            "phone": "949-229-1575",
            "email": "sales@reytechinc.com",
            "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
            "fein": "47-4588061",
            "sellers_permit": "245652416 - 00001",
            "cert_number": "2002605",
            "cert_expiration": "06/30/2027",
        },
    }


class TestPriceKeyFallback:
    """Audit E — gap test found generated $0.00 because price lookup only
    covered unit_price/bid_price/our_price. V2 records use price_per_unit."""

    def test_unit_price_primary(self):
        assert _lpa_item_price({"unit_price": 179.14}) == 179.14

    def test_price_per_unit_fallback(self):
        """This is the key the Quote Model V2 uses; previous code missed it."""
        assert _lpa_item_price({"price_per_unit": 25.50}) == 25.50

    def test_bid_price_fallback(self):
        assert _lpa_item_price({"bid_price": 10.0}) == 10.0

    def test_our_price_fallback(self):
        assert _lpa_item_price({"our_price": 42.0}) == 42.0

    def test_supplier_cost_last_resort(self):
        """Vendor cost isn't a sell price, but better than 0 while flagged."""
        assert _lpa_item_price({"supplier_cost": 100.0}) == 100.0

    def test_priority_order(self):
        """unit_price beats all other keys when present."""
        item = {
            "unit_price": 200.0,
            "price_per_unit": 150.0,
            "bid_price": 100.0,
            "our_price": 50.0,
        }
        assert _lpa_item_price(item) == 200.0

    def test_zero_values_fall_through(self):
        """unit_price=0 should NOT stop the search — try the next key."""
        item = {"unit_price": 0, "price_per_unit": 99.99}
        assert _lpa_item_price(item) == 99.99

    def test_none_returns_0(self):
        assert _lpa_item_price({}) == 0.0
        assert _lpa_item_price({"unit_price": None}) == 0.0

    def test_string_values_parsed(self):
        """Tolerate str-typed prices from JSON loads etc."""
        assert _lpa_item_price({"price_per_unit": "15.75"}) == 15.75


class TestDueDateUSFormat:
    """Audit C — north star `before` = '4/22/2026', RFQ record stores ISO."""

    def test_iso_date(self):
        assert _us_date("2026-04-22") == "4/22/2026"

    def test_us_date_unchanged(self):
        assert _us_date("4/22/2026") == "4/22/2026"

    def test_empty_returns_empty(self):
        assert _us_date("") == ""
        assert _us_date(None) == ""

    def test_non_date_string_returned_unchanged(self):
        """Don't mangle free text that isn't parseable as a date."""
        assert _us_date("N/A") == "N/A"


class TestClassifierEnhancedSignals:
    """Mike 2026-04-23: 'visual page 1 check will tell you 100%'."""

    def test_page1_text_signal_true_on_real_lpa(self):
        """Both repo fixtures contain 'Request For Quotation / IT Goods and
        Services' on page 1."""
        if not BLANK_LPA.exists():
            pytest.skip("fixture missing")
        assert _lpa_page1_text_signal(str(BLANK_LPA)) is True

    def test_page1_text_signal_missing_path(self):
        assert _lpa_page1_text_signal("/nonexistent.pdf") is False

    def test_filename_signal_matches_rfq_prefix(self):
        assert _lpa_filename_signal("/tmp/RFQ 10840486.pdf") is True
        assert _lpa_filename_signal("RFQ_10840486.pdf") is True
        assert _lpa_filename_signal("CCHCS_IT_RFQ.pdf") is True
        assert _lpa_filename_signal("lpa_it_goods.pdf") is True

    def test_filename_signal_no_match(self):
        assert _lpa_filename_signal("/tmp/random.pdf") is False
        assert _lpa_filename_signal("invoice.pdf") is False

    def test_is_cchcs_it_rfq_combines_signals(self):
        """Real fixture satisfies all three signals; any single one is
        sufficient. This guards against template variants that strip
        AcroForm fields (flattened) or rename them."""
        if not BLANK_LPA.exists():
            pytest.skip("fixture missing")
        assert _is_cchcs_it_rfq(str(BLANK_LPA)) is True


class TestStateZipWritten:
    """Audit B — gap test found NS has Text7='CA' Text8='92679', GEN had both
    empty. Polish forces these plain LPA fields regardless of yaml mapping."""

    def test_state_zip_populated_on_fill(self, reytech_config, tmp_path):
        if not BLANK_LPA.exists():
            pytest.skip("fixture missing")
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        rfq = {
            "solicitation_number": "10840486",
            "due_date": "2026-04-22",
            "sign_date": "04/22/2026",
            "tax_rate": 7.75,
            "line_items": [{"description": "x", "qty": 1, "price_per_unit": 10.0}],
        }
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))
        assert_pdf_fields(str(out), {"Text7": "CA", "Text8": "92679"})


class TestPriceFallbackInFill:
    """End-to-end: V2-style record with price_per_unit produces non-zero total."""

    def test_v2_record_price_per_unit_fills_correctly(self, reytech_config, tmp_path):
        if not BLANK_LPA.exists():
            pytest.skip("fixture missing")
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        rfq = {
            "solicitation_number": "10840486",
            "due_date": "2026-04-22",
            "sign_date": "04/22/2026",
            "tax_rate": 7.75,
            "line_items": [{
                "description": "BLS Provider Course Videos USB",
                "item_number": "978-1-68472-317-1",
                "qty": 2,
                "uom": "EA",
                "price_per_unit": 179.14,  # V2 key — was dropped before PR-A
            }],
        }
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))
        # subtotal 358.28, tax 27.77, total 386.05
        assert_pdf_fields(str(out), {
            "Extension TotalSubtotal": "358.28",
            "Extension TotalSales Tax": "27.77",
            "Extension TotalTotal": "386.05",
            "Price Per Unit1": "179.14",
            "Extension Total1": "358.28",
        })


class TestDueDateFillEnd2End:
    def test_iso_due_date_renders_us_format(self, reytech_config, tmp_path):
        if not BLANK_LPA.exists():
            pytest.skip("fixture missing")
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        rfq = {
            "solicitation_number": "10840486",
            "due_date": "2026-04-22",
            "sign_date": "04/22/2026",
            "tax_rate": 7.75,
            "line_items": [{"description": "x", "qty": 1, "price_per_unit": 10.0}],
        }
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))
        # yaml maps header.due_date → "before" field
        assert_pdf_fields(str(out), {"before": "4/22/2026"})
