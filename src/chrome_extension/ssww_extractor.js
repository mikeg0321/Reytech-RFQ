/**
 * S&S Worldwide Price Extractor — Content Script
 * Runs on ssww.com/item/* pages
 * Extracts MSRP + sale price, sends to background worker for relay
 */
(function() {
  console.log('[S&S Extractor] Running on', location.href);

  function extractPrices() {
    var body = document.body.innerText || '';
    var result = {
      type: 'reytech_ssww_prices',
      url: location.href,
      msrp: 0,
      sale: 0,
      title: '',
      item_number: '',
      in_stock: false
    };

    var h1 = document.querySelector('h1');
    if (h1) result.title = h1.textContent.trim();

    var itemMatch = body.match(/Item\s*#:\s*(\w+)/);
    if (itemMatch) result.item_number = itemMatch[1];

    // Try "List: $XX.XX" first (product on sale)
    var listMatch = body.match(/List:\s*\$(\d+\.?\d*)/);
    if (listMatch) result.msrp = parseFloat(listMatch[1]);

    // Sale price pattern
    var saleMatch = body.match(/SALE[\s\S]*?\$\s*(\d+)\s*\.?\s*(\d{2})/);
    if (saleMatch) result.sale = parseFloat(saleMatch[1] + '.' + saleMatch[2]);

    // If no List price found, product is NOT on sale — find the regular price
    // Look for the main price: "$XX.XX" near "Qty" or "Add To Cart"
    if (!result.msrp) {
      // Try price elements directly
      var priceEls = document.querySelectorAll('[class*="price"], [id*="price"], b, strong');
      for (var i = 0; i < priceEls.length; i++) {
        var txt = (priceEls[i].textContent || '').trim();
        var pm = txt.match(/^\$(\d+\.?\d{0,2})$/);
        if (pm && parseFloat(pm[1]) > 1) {
          result.msrp = parseFloat(pm[1]);
          break;
        }
      }
      // Fallback: regex on body text near "Qty" or "Add To Cart"
      if (!result.msrp) {
        var qtyMatch = body.match(/\$(\d+\.?\d{2})\s*(?:Qty|Buy|Add)/);
        if (qtyMatch) result.msrp = parseFloat(qtyMatch[1]);
      }
      // Fallback: any prominent dollar amount
      if (!result.msrp) {
        var anyPrice = body.match(/\$(\d{2,5}\.\d{2})/);
        if (anyPrice) result.msrp = parseFloat(anyPrice[1]);
      }
    }

    result.in_stock = body.indexOf('In stock') >= 0;
    return result;
  }

  function trySend() {
    var prices = extractPrices();
    console.log('[S&S Extractor] Extracted:', JSON.stringify(prices));
    if (prices.msrp > 0) {
      chrome.runtime.sendMessage(prices, function(resp) {
        console.log('[S&S Extractor] Sent to background:', resp);
      });
      return true;
    }
    return false;
  }

  // Retry with increasing delays — Cloudflare challenge can take 5-10s
  var _attempts = 0;
  var _maxAttempts = 15;
  var _timer = setInterval(function() {
    _attempts++;
    if (document.title === 'Just a moment...') {
      console.log('[S&S Extractor] Waiting for Cloudflare... attempt ' + _attempts);
      return;
    }
    if (trySend() || _attempts >= _maxAttempts) {
      clearInterval(_timer);
      if (_attempts >= _maxAttempts) console.log('[S&S Extractor] Gave up after ' + _attempts + ' attempts');
    }
  }, 1000);
})();
