"""Amazon URL parser regression guards — 2026-04-14.

Pins the fix for the regression Mike reported on the paint marker
URL (B0CX1BD86P):
  - Scrape-first strategy (Grok was timing out after the Apr 10
    SerpApi swap)
  - MFG fallback chain: mfg_number → part_number → upc → asin
  - Pricing: list_price used as quote `cost`, discount logged
    separately, `cost_if_discount_holds` exposed for two-profit-
    scenario math downstream

Tests mock `_scrape_generic` so they're deterministic and don't
hit Amazon (which blocks datacenter IPs).
"""
import pytest
from unittest.mock import patch


class TestAsinExtraction:
    def test_dp_url(self):
        from src.agents.item_link_lookup import _extract_asin
        assert _extract_asin(
            "https://www.amazon.com/TBC-Best-Crafts-Waterproof-Non-Toxic/dp/B0CX1BD86P/"
        ) == "B0CX1BD86P"

    def test_dp_url_with_ref_garbage(self):
        from src.agents.item_link_lookup import _extract_asin
        url = ("https://www.amazon.com/TBC-Best-Crafts-Waterproof-Non-Toxic/"
               "dp/B0CX1BD86P/ref=sr_1_10?crid=38G4YHJ84PB1M&qid=1740506883")
        assert _extract_asin(url) == "B0CX1BD86P"

    def test_gp_product_url(self):
        from src.agents.item_link_lookup import _extract_asin
        assert _extract_asin("https://www.amazon.com/gp/product/B09V3KXJPB") == "B09V3KXJPB"

    def test_no_asin(self):
        from src.agents.item_link_lookup import _extract_asin
        assert _extract_asin("https://www.amazon.com/s?k=paint+markers") == ""


class TestScrapeFirst:
    """After the 2026-04-14 fix, _lookup_amazon hits _scrape_generic
    FIRST and only calls Grok as enrichment when the scrape is empty."""

    def test_scrape_happy_path_skips_grok(self):
        from src.agents import item_link_lookup
        fake_scrape = {
            "title": "TBC Best Crafts Waterproof Non-Toxic Paint Marker",
            "list_price": 19.99,
            "sale_price": 14.99,
            "price": 14.99,
            "mfg_number": "TBC-PM12",
            "part_number": "TBC-PM12",
            "upc": "123456789012",
            "manufacturer": "TBC Best Crafts",
            "photo_url": "https://m.media-amazon.com/images/I/xxx.jpg",
        }
        with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape) as mock_scrape:
            # Grok would hang in tests — fail loudly if it gets called
            with patch("src.agents.product_research.lookup_amazon_product",
                       side_effect=AssertionError("Grok must not be called on scrape-happy-path")):
                result = item_link_lookup._lookup_amazon(
                    "https://www.amazon.com/TBC/dp/B0CX1BD86P/"
                )
        mock_scrape.assert_called_once()
        assert result["asin"] == "B0CX1BD86P"
        assert result["title"].startswith("TBC")
        assert result["list_price"] == 19.99
        assert result["sale_price"] == 14.99
        assert result["mfg_number"] == "TBC-PM12"
        assert result["source"] == "amazon_scrape"

    def test_grok_enrichment_fires_when_scrape_empty(self):
        from src.agents import item_link_lookup
        empty_scrape = {}
        grok_result = {
            "title": "Grok Resolved Title",
            "list_price": 25.00,
            "sale_price": None,
            "price": 25.00,
            "mfg_number": "GROK-ABC",
        }
        with patch.object(item_link_lookup, "_scrape_generic", return_value=empty_scrape):
            with patch("src.agents.product_research.lookup_amazon_product",
                       return_value=grok_result) as mock_grok:
                result = item_link_lookup._lookup_amazon(
                    "https://www.amazon.com/dp/B0CX1BD86P"
                )
        mock_grok.assert_called_once_with("B0CX1BD86P")
        assert result["title"] == "Grok Resolved Title"
        assert result["list_price"] == 25.00


class TestMfgFallbackChain:
    """MFG → Item → UPC → ASIN fallback chain on the returned dict."""

    def _call(self, scrape_data):
        from src.agents import item_link_lookup
        with patch.object(item_link_lookup, "_scrape_generic", return_value=scrape_data):
            with patch("src.agents.product_research.lookup_amazon_product",
                       return_value=None):
                return item_link_lookup._lookup_amazon(
                    "https://www.amazon.com/dp/B0CX1BD86P"
                )

    def test_mfg_number_used_when_present(self):
        result = self._call({
            "title": "Test", "price": 10, "list_price": 10,
            "mfg_number": "REAL-MFG", "part_number": "ITEM-001",
            "upc": "123456789012",
        })
        assert result["mfg_number"] == "REAL-MFG"
        assert result["mfg_source"] == "mfg"

    def test_item_number_fallback(self):
        result = self._call({
            "title": "Test", "price": 10, "list_price": 10,
            "mfg_number": "", "part_number": "ITEM-001",
            "upc": "123456789012",
        })
        assert result["mfg_number"] == "ITEM-001"
        assert result["mfg_source"] == "item"

    def test_upc_fallback(self):
        result = self._call({
            "title": "Test", "price": 10, "list_price": 10,
            "mfg_number": "", "part_number": "",
            "upc": "123456789012",
        })
        assert result["mfg_number"] == "123456789012"
        assert result["mfg_source"] == "upc"

    def test_asin_last_resort(self):
        result = self._call({
            "title": "Test", "price": 10, "list_price": 10,
        })
        assert result["mfg_number"] == "B0CX1BD86P"
        assert result["mfg_source"] == "asin_fallback"


class TestPricingSemantics:
    """User ask §3: unit_cost = LIST/MSRP, discount logged separately,
    cost_if_discount_holds exposed for two-profit-scenario math."""

    def _call(self, scrape_data):
        from src.agents import item_link_lookup
        with patch.object(item_link_lookup, "_scrape_generic", return_value=scrape_data):
            with patch("src.agents.product_research.lookup_amazon_product",
                       return_value=None):
                return item_link_lookup._lookup_amazon(
                    "https://www.amazon.com/dp/B0CX1BD86P"
                )

    def test_list_price_is_cost(self):
        result = self._call({"title": "T", "list_price": 20.00, "sale_price": 15.00})
        assert result["cost"] == 20.00
        assert result["list_price"] == 20.00
        assert result["sale_price"] == 15.00

    def test_discount_logged(self):
        result = self._call({"title": "T", "list_price": 20.00, "sale_price": 15.00})
        assert result["discount_amount"] == 5.00
        assert result["discount_pct"] == 25.0
        assert result["cost_if_discount_holds"] == 15.00

    def test_no_discount_when_prices_equal(self):
        result = self._call({"title": "T", "list_price": 20.00, "sale_price": 20.00})
        assert result["discount_pct"] is None
        assert result["discount_amount"] is None
        assert result["cost_if_discount_holds"] is None

    def test_no_discount_when_only_list_price(self):
        result = self._call({"title": "T", "list_price": 20.00})
        assert result["cost"] == 20.00
        assert result["discount_pct"] is None

    def test_fallback_to_sale_when_no_list(self):
        """When MSRP isn't scraped, fall back to sale price as cost —
        but record no discount (we don't know the real list)."""
        result = self._call({"title": "T", "sale_price": 15.00, "price": 15.00})
        assert result["cost"] == 15.00
        assert result["discount_pct"] is None

    def test_price_note_shows_both_when_discounted(self):
        result = self._call({"title": "T", "list_price": 20.00, "sale_price": 14.00})
        # 30% off
        assert "30%" in result["price_note"]
        assert "20" in result["price_note"]
        assert "14" in result["price_note"]


class TestErrorPath:
    def test_empty_scrape_and_empty_grok_returns_error(self):
        from src.agents import item_link_lookup
        with patch.object(item_link_lookup, "_scrape_generic", return_value={}):
            with patch("src.agents.product_research.lookup_amazon_product",
                       return_value=None):
                result = item_link_lookup._lookup_amazon(
                    "https://www.amazon.com/dp/B0CX1BD86P"
                )
        assert result["error"]
        # Even on error, ASIN becomes the last-resort identifier
        assert result["mfg_number"] == "B0CX1BD86P"
        # No more "Lookup timed out" message
        assert "timed out" not in result["error"].lower()


class TestUpcExtractionInScraper:
    """_scrape_generic gains UPC extraction from JSON-LD gtin fields."""

    def test_gtin13(self):
        from src.agents import item_link_lookup
        html = '<script>{"@type":"Product","gtin13":"0123456789012"}</script>'
        with patch("src.agents.item_link_lookup.requests") as mock_req:
            mock_req.get.return_value.text = html
            # HAS_REQUESTS must be truthy for the scrape to run
            with patch.object(item_link_lookup, "HAS_REQUESTS", True):
                result = item_link_lookup._scrape_generic("https://example.com/p")
        assert result.get("upc") == "0123456789012"

    def test_gtin12(self):
        from src.agents import item_link_lookup
        html = '<script>"gtin12":"123456789012"</script>'
        with patch("src.agents.item_link_lookup.requests") as mock_req:
            mock_req.get.return_value.text = html
            with patch.object(item_link_lookup, "HAS_REQUESTS", True):
                result = item_link_lookup._scrape_generic("https://example.com/p")
        assert result.get("upc") == "123456789012"
