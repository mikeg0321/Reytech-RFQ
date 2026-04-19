"""
Tests for the 2026-04-19 URL lookup hardening + dual-profit work:

1. _is_garbage_title catches the empirical bot-stub / 404 phrases that
   were slipping through (Grainger "Whoops, we couldn't find that.",
   Waxie "404 - File or directory not found.", Concordance "Follow us
   on Facebook", Fisher Scientific "accessibility menu, dialog, popup",
   bare "McMaster-Carr").

2. _scrape_generic refuses to parse non-200 HTTP responses (so a 404
   page's <title> never becomes a product name).

3. _stamp_ref_identifier appends "REF ASIN:" / "REF UPC:" idempotently
   and prefers ASIN when both are present.

4. lookup_prices() in price_check.py captures list_price + sale_price
   separately, uses list_price (MSRP) as the cost basis, and computes
   both profit_unit AND discount_profit_unit when a discount is found.
"""
from unittest.mock import patch, MagicMock

from src.agents.item_link_lookup import (
    _is_garbage_title,
    _scrape_generic,
    _stamp_ref_identifier,
)


class TestGarbageTitleDetection:
    def test_empty_is_garbage(self):
        assert _is_garbage_title("")
        assert _is_garbage_title(None)
        assert _is_garbage_title("   ")

    def test_grainger_whoops_404(self):
        # Empirical 2026-04-19: 31 chars, used to bypass the len<30 gate
        assert _is_garbage_title("Whoops, we couldn't find that.")

    def test_waxie_404(self):
        assert _is_garbage_title("404 - File or directory not found.")

    def test_concordance_footer(self):
        assert _is_garbage_title("Follow us on Facebook")

    def test_fisher_sci_bot_block(self):
        assert _is_garbage_title("accessibility menu, dialog, popup")

    def test_mcmaster_brand_only(self):
        # Short brand-only stub
        assert _is_garbage_title("McMaster-Carr")

    def test_real_product_title_passes(self):
        assert not _is_garbage_title(
            "TRU RED 8.5 x 11 Multipurpose Paper, 20 lbs., 96 Brightness, 500/Ream"
        )

    def test_amazon_long_product_passes(self):
        assert not _is_garbage_title(
            "Amazon.com: Apple iPhone 11, 64GB, PRODUCT RED - Unlocked (Renewed)"
        )


class TestScrapeGenericHttpStatus:
    def test_404_returns_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.text = "<html><title>Whoops, we couldn't find that.</title></html>"
        with patch("src.agents.item_link_lookup.requests.get", return_value=mock_resp):
            out = _scrape_generic("https://www.grainger.com/product/missing")
        assert "error" in out
        assert "HTTP 404" in out["error"]
        # Crucially — no title field set even though the 404 page had one
        assert not out.get("title")

    def test_500_returns_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "<html><title>Server Error</title></html>"
        with patch("src.agents.item_link_lookup.requests.get", return_value=mock_resp):
            out = _scrape_generic("https://www.fishersci.com/shop/products/p-x")
        assert "error" in out
        assert "HTTP 500" in out["error"]

    def test_200_parses_title(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '<html><title>Real Product Name 100ct Box</title></html>'
        with patch("src.agents.item_link_lookup.requests.get", return_value=mock_resp):
            out = _scrape_generic("https://example.com/x")
        assert out.get("title", "").startswith("Real Product")


class TestStampRefIdentifier:
    def test_asin_stamp(self):
        out = _stamp_ref_identifier("Nitrile Gloves L 100ct", asin="B07ZPKBL9V")
        assert out == "Nitrile Gloves L 100ct (REF ASIN:B07ZPKBL9V)"

    def test_upc_stamp_when_no_asin(self):
        out = _stamp_ref_identifier("Nitrile Gloves L 100ct", upc="012345678905")
        assert out == "Nitrile Gloves L 100ct (REF UPC:012345678905)"

    def test_asin_preferred_over_upc(self):
        out = _stamp_ref_identifier(
            "Nitrile Gloves L 100ct", asin="B07ZPKBL9V", upc="012345678905"
        )
        assert "REF ASIN:B07ZPKBL9V" in out
        assert "REF UPC" not in out

    def test_idempotent_asin(self):
        once = _stamp_ref_identifier("Item", asin="B07ZPKBL9V")
        twice = _stamp_ref_identifier(once, asin="B07ZPKBL9V")
        assert once == twice

    def test_idempotent_upc(self):
        once = _stamp_ref_identifier("Item", upc="012345678905")
        twice = _stamp_ref_identifier(once, upc="012345678905")
        assert once == twice

    def test_no_identifiers_passes_through(self):
        assert _stamp_ref_identifier("Item") == "Item"

    def test_empty_desc_short_circuits(self):
        assert _stamp_ref_identifier("", asin="B07ZPKBL9V") == ""


class TestLookupPricesDualProfit:
    """Verify lookup_prices() in forms/price_check.py captures dual prices."""

    def test_msrp_first_when_both_prices_present(self):
        from src.forms import price_check as pc_mod

        fake_research = {
            "found": True,
            "price": 7.49,                  # generic price (sale)
            "list_price": 9.69,             # MSRP
            "sale_price": 7.49,
            "title": "TRU RED Paper",
            "url": "https://www.staples.com/x",
            "asin": "",
        }
        parsed = {"line_items": [{"description": "Paper 500ct", "qty": 10}]}
        with patch.object(pc_mod, "HAS_RESEARCH", True), \
             patch.object(pc_mod, "research_product", return_value=fake_research), \
             patch.object(pc_mod, "HAS_WON_QUOTES", False):
            out = pc_mod.lookup_prices(parsed)

        item = out["line_items"][0]
        p = item["pricing"]

        # MSRP is the cost basis ─ amazon_price = list_price, NOT sale
        assert p["amazon_list_price"] == 9.69
        assert p["amazon_sale_price"] == 7.49
        assert p["amazon_price"] == 9.69, "MSRP must win as cost basis"
        assert p["amazon_discount_pct"] == 22.7  # (1 - 7.49/9.69) * 100

    def test_dual_profit_when_discounted(self):
        from src.forms import price_check as pc_mod

        fake_research = {
            "found": True, "price": 7.49,
            "list_price": 9.69, "sale_price": 7.49,
            "title": "Paper", "url": "x", "asin": "",
        }
        parsed = {"line_items": [{"description": "Paper", "qty": 10}]}
        # Force the 25% markup fallback — predictable rec_price
        with patch.object(pc_mod, "HAS_RESEARCH", True), \
             patch.object(pc_mod, "research_product", return_value=fake_research), \
             patch.object(pc_mod, "HAS_WON_QUOTES", False), \
             patch("src.core.pricing_oracle_v2.get_pricing", side_effect=Exception("oracle off")):
            out = pc_mod.lookup_prices(parsed)

        p = out["line_items"][0]["pricing"]
        # 25% markup on $9.69 MSRP → $12.11 (rounded)
        assert p["recommended_price"] == round(9.69 * 1.25, 2)
        # Standard profit uses MSRP as cost: 12.11 - 9.69 = 2.42
        assert p["profit_unit"] == round(p["recommended_price"] - 9.69, 2)
        assert p["profit_total"] == round(p["profit_unit"] * 10, 2)
        # Discount profit uses sale as cost: 12.11 - 7.49 = 4.62
        assert p["discount_profit_unit"] == round(p["recommended_price"] - 7.49, 2)
        assert p["discount_profit_total"] == round(p["discount_profit_unit"] * 10, 2)
        assert p["discount_profit_note"] == "if discount holds for profit calculation"

    def test_no_discount_no_dual_profit(self):
        from src.forms import price_check as pc_mod

        fake_research = {
            "found": True, "price": 50.00,
            "list_price": 50.00, "sale_price": None,
            "title": "Tool", "url": "x", "asin": "",
        }
        parsed = {"line_items": [{"description": "Tool", "qty": 1}]}
        with patch.object(pc_mod, "HAS_RESEARCH", True), \
             patch.object(pc_mod, "research_product", return_value=fake_research), \
             patch.object(pc_mod, "HAS_WON_QUOTES", False), \
             patch("src.core.pricing_oracle_v2.get_pricing", side_effect=Exception("oracle off")):
            out = pc_mod.lookup_prices(parsed)

        p = out["line_items"][0]["pricing"]
        assert p["amazon_list_price"] == 50.00
        assert p["amazon_sale_price"] is None
        assert p["amazon_discount_pct"] is None
        assert "discount_profit_unit" not in p
        assert "discount_profit_note" not in p
