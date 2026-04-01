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

    var listMatch = body.match(/List:\s*\$(\d+\.?\d*)/);
    if (listMatch) result.msrp = parseFloat(listMatch[1]);

    var saleMatch = body.match(/SALE[\s\S]*?\$\s*(\d+)\s*\.?\s*(\d{2})/);
    if (saleMatch) result.sale = parseFloat(saleMatch[1] + '.' + saleMatch[2]);

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

  setTimeout(function() { if (!trySend()) setTimeout(function() { if (!trySend()) setTimeout(trySend, 4000); }, 2000); }, 1500);
})();
