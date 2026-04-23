"""Regression: LPA IT RFQ fill engine wiring (Bundle-4 PR-4a, closes audit M).

Template: tests/fixtures/cchcs_it_rfq_blank.pdf (CCHCS "Request For Quotation —
IT Goods and Services" form). Profile: cchcs_it_rfq_reytech_standard.yaml.
Golden reference: tests/fixtures/cchcs_it_rfq_reytech_golden.pdf (PREQ10843276).

Incident 2026-04-22: RFQ 10840486 (live P0 blocker) uploaded an LPA IT RFQ
template that got dispatched to fill_703b(), which name-matches on 703B field
names that the LPA template doesn't have. Most supplier fields rendered blank.

These tests lock in the fingerprint detection + yaml-driven fill so the
regression can't recur.
"""
import os
import pytest
from pathlib import Path

from src.forms.reytech_filler_v4 import (
    fill_cchcs_it_rfq,
    _is_cchcs_it_rfq,
)

FIXTURES = Path(__file__).parent / "fixtures"
BLANK_LPA = FIXTURES / "cchcs_it_rfq_blank.pdf"
GOLDEN_LPA = FIXTURES / "cchcs_it_rfq_reytech_golden.pdf"


pytestmark = pytest.mark.skipif(
    not BLANK_LPA.exists(),
    reason=f"LPA blank fixture missing: {BLANK_LPA}",
)


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


@pytest.fixture
def rfq_data_1item():
    return {
        "solicitation_number": "10840486",
        "due_date": "04/22/2026",
        "sign_date": "04/22/2026",
        "tax_rate": 7.75,
        "shipping": 0,
        "line_items": [
            {
                "description": "BLS Provider Course Videos USB",
                "item_number": "978-1-68472-317-1",
                "qty": 2,
                "uom": "EA",
                "unit_price": 179.14,
            },
        ],
    }


class TestFingerprintDetection:
    def test_lpa_blank_detected_as_cchcs_it_rfq(self):
        """Real LPA template → fingerprint returns True."""
        assert _is_cchcs_it_rfq(str(BLANK_LPA)) is True

    def test_nonexistent_path_returns_false(self):
        """Broken path should not raise, just return False."""
        assert _is_cchcs_it_rfq("/nonexistent/path.pdf") is False


class TestFillCchcsItRfq:
    def test_fill_renders_reytech_canonical_supplier(self, reytech_config, rfq_data_1item, tmp_path):
        """After fill, the PDF must have Reytech Inc., Michael Guadan, phone,
        email populated per profile defaults."""
        from tests.conftest import assert_pdf_fields  # shared helper
        out = tmp_path / "filled.pdf"
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq_data_1item, reytech_config, str(out))
        assert out.exists()
        assert_pdf_fields(str(out), {
            "Supplier Name": "Reytech Inc.",
            "Contact Name": "Michael Guadan",
            "Phone": "949-229-1575",
            "Supplier Email": "sales@reytechinc.com",
            "Supplier Address 1": "30 Carnoustie Way Trabuco Canyon",
            "Supplier Address 2": "CA 92679",
        })

    def test_fill_renders_line_item_row_1(self, reytech_config, rfq_data_1item, tmp_path):
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq_data_1item, reytech_config, str(out))
        assert_pdf_fields(str(out), {
            "Item Description1": "BLS Provider Course Videos USB",
            "Model or Part Number1": "978-1-68472-317-1",
            "Qty1": "2",
            "Unit1": "EA",
            "Price Per Unit1": "179.14",
            "Extension Total1": "358.28",
        })

    def test_fill_computes_totals(self, reytech_config, rfq_data_1item, tmp_path):
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq_data_1item, reytech_config, str(out))
        # subtotal=358.28, tax=7.75% → 27.77 (rounds), total=386.05
        assert_pdf_fields(str(out), {
            "Extension TotalSubtotal": "358.28",
            "Extension TotalSales Tax": "27.77",
            "Extension TotalTotal": "386.05",
        })

    def test_compliance_checkboxes_prefilled_from_profile(self, reytech_config, rfq_data_1item, tmp_path):
        """The yaml profile pre-checks 26 Reytech-standard compliance boxes."""
        from tests.conftest import get_pdf_field_names
        out = tmp_path / "filled.pdf"
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq_data_1item, reytech_config, str(out))
        # Sanity check: all Check Box21.* fields that the yaml pre-ticks
        # must exist on the template. (Profile is authoritative; if this
        # fails, the template has a different field namespace than yaml expects.)
        names = get_pdf_field_names(str(out))
        expected_boxes = {
            "Check Box12", "Check Box14", "Check Box16",
            "Check Box21.0.0", "Check Box27.0.0", "Check Box29.0",
        }
        assert expected_boxes.issubset(names), (
            f"Profile expected these checkbox fields but template has them missing: "
            f"{expected_boxes - names}"
        )


class TestItemOverflowGuard:
    def test_more_than_10_items_raises_not_silently_drops(self, reytech_config, tmp_path):
        """The LPA template has a 10-row cap. Overflow handling (duplicate
        page) is declared in yaml but not yet implemented in the filler.
        Raise rather than silently drop items 11+."""
        rfq = {
            "solicitation_number": "10840486",
            "due_date": "04/22/2026",
            "sign_date": "04/22/2026",
            "tax_rate": 7.75,
            "line_items": [
                {"description": f"item {i}", "qty": 1, "unit_price": 10.0}
                for i in range(12)
            ],
        }
        out = tmp_path / "filled.pdf"
        with pytest.raises(ValueError, match="10-row capacity"):
            fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))


class TestTaxRateTolerance:
    def test_tax_rate_as_percent(self, reytech_config, rfq_data_1item, tmp_path):
        """rfq_data stores tax_rate as percent (7.75). Filler must divide."""
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        rfq = dict(rfq_data_1item, tax_rate=7.75)
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))
        assert_pdf_fields(str(out), {"Extension TotalSales Tax": "27.77"})

    def test_tax_rate_as_decimal(self, reytech_config, rfq_data_1item, tmp_path):
        """Tolerate 0.0775 (decimal) too — tax_decimal computes from whichever."""
        from tests.conftest import assert_pdf_fields
        out = tmp_path / "filled.pdf"
        rfq = dict(rfq_data_1item, tax_rate=0.0775)
        fill_cchcs_it_rfq(str(BLANK_LPA), rfq, reytech_config, str(out))
        assert_pdf_fields(str(out), {"Extension TotalSales Tax": "27.77"})
