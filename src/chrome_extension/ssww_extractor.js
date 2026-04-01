/**
 * S&S Worldwide Price Extractor — Content Script
 * Runs on ssww.com/item/* pages
 * Extracts MSRP (list price) and sale price, posts back to Reytech app
 */
(function() {
  // Always run on S&S product pages — extract prices and post to any listening Reytech tab
  console.log('[Reytech S&S Extractor] Running on', location.href);

  function extractPrices() {
    var result = {
      type: 'reytech_ssww_prices',
      url: location.href,
      msrp: 0,
      sale: 0,
      title: '',
      item_number: '',
      in_stock: false
    };

    // Title
    var h1 = document.querySelector('h1');
    if (h1) result.title = h1.textContent.trim();

    // Item number
    var body = document.body.innerText || '';
    var itemMatch = body.match(/Item\s*#:\s*(\w+)/);
    if (itemMatch) result.item_number = itemMatch[1];

    // MSRP (List price): "List: $82.24"
    var listMatch = body.match(/List:\s*\$(\d+\.?\d*)/);
    if (listMatch) result.msrp = parseFloat(listMatch[1]);

    // Sale price: the large price display "$69.99" (split across elements)
    // Look for the price container near "SALE" text
    var allText = document.body.innerText;
    var saleMatch = allText.match(/SALE[^]*?\$\s*(\d+)\s*\.?\s*(\d{2})/);
    if (saleMatch) {
      result.sale = parseFloat(saleMatch[1] + '.' + saleMatch[2]);
    } else {
      // Fallback: if no sale, MSRP might be the only price
      var priceMatch = allText.match(/\$\s*(\d+)\s*\.\s*(\d{2})\s*(?:Qty|Buy|Add)/);
      if (priceMatch) result.sale = parseFloat(priceMatch[1] + '.' + priceMatch[2]);
    }

    // In stock check
    result.in_stock = body.indexOf('In stock') >= 0;

    // Bulk pricing
    var bulkMatch = body.match(/Buy\s+(\d+)\+.*?Only\s+\$(\d+\.?\d*)/);
    if (bulkMatch) {
      result.bulk_qty = parseInt(bulkMatch[1]);
      result.bulk_price = parseFloat(bulkMatch[2]);
    }

    return result;
  }

  // Wait for page to fully load, then extract and post
  function tryExtract() {
    var prices = extractPrices();
    console.log('[Reytech S&S Extractor] Extracted:', JSON.stringify(prices));
    if (prices.msrp > 0 || prices.sale > 0) {
      // Post to opener window (the Reytech tab that opened this popup)
      if (window.opener) {
        try {
          window.opener.postMessage(prices, '*');
          console.log('[Reytech S&S Extractor] Posted to opener');
        } catch(e) {
          console.log('[Reytech S&S Extractor] opener.postMessage failed:', e);
        }
      }
      // Also use BroadcastChannel (works across tabs, no cross-origin issues)
      try {
        var bc = new BroadcastChannel('reytech_ssww');
        bc.postMessage(prices);
        bc.close();
        console.log('[Reytech S&S Extractor] Broadcast sent');
      } catch(e) {
        console.log('[Reytech S&S Extractor] BroadcastChannel failed:', e);
      }

      // Auto-close after short delay
      setTimeout(function() { window.close(); }, 2000);
    }
  }

  // Try immediately, then retry (page might still be loading)
  setTimeout(tryExtract, 1500);
  setTimeout(tryExtract, 4000);
  setTimeout(tryExtract, 7000);
})();
