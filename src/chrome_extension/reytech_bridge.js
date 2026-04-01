/**
 * Reytech Bridge — Content Script
 * Runs on the Reytech app pages
 * Listens for price messages from the S&S extractor and applies them
 */
(function() {
  console.log('[Reytech Bridge] Extension loaded on', location.hostname);

  // Listen on both postMessage AND BroadcastChannel
  function handlePrices(d) {
    if (!d || d.type !== 'reytech_ssww_prices') return;
    _applyPrices(d);
  }

  window.addEventListener('message', function(e) { handlePrices(e.data); });

  try {
    var bc = new BroadcastChannel('reytech_ssww');
    bc.onmessage = function(e) { handlePrices(e.data); };
    console.log('[Reytech Bridge] BroadcastChannel listening');
  } catch(e) {
    console.log('[Reytech Bridge] BroadcastChannel not available');
  }

  function _applyPrices(d) {
    if (!d || !d.msrp) return;
    console.log('[Reytech Bridge] Applying S&S prices:', d);

    // Find the matching item by URL
    var linkInputs = document.querySelectorAll('input[name^="link_"]');
    linkInputs.forEach(function(inp) {
      var url = (inp.value || '').trim();
      if (!url || url.indexOf('ssww.com') < 0) return;

      // Match by item number in URL
      var urlItem = url.match(/[\-\/]([A-Z]?\d{4,6})\/?(\?|$)/i);
      var msgItem = d.item_number || '';
      if (!urlItem || !msgItem) return;
      if (urlItem[1].toUpperCase() !== msgItem.toUpperCase()) return;

      // Found the matching row
      var idx = inp.name.replace('link_', '');
      console.log('[Reytech Extension] Applying prices to row ' + idx + ': MSRP=$' + d.msrp + ', Sale=$' + d.sale);

      // Fill cost with MSRP
      var costEl = document.querySelector('[name=cost_' + idx + ']');
      if (costEl && d.msrp > 0) {
        costEl.value = d.msrp.toFixed(2);
      }

      // Store discount cost on row
      var row = document.querySelector('tr[data-row="' + idx + '"]');
      if (row && d.sale > 0 && d.sale < d.msrp) {
        row.setAttribute('data-discount-cost', d.sale.toFixed(2));
      }

      // Recalc
      if (typeof recalcRow === 'function') recalcRow(parseInt(idx), true);
      if (typeof recalcPC === 'function') recalcPC();
      if (typeof triggerPcAutosave === 'function') triggerPcAutosave();

      // Update the link meta
      var metaEl = document.getElementById('link_meta_' + idx);
      if (metaEl) {
        var html = '<span style="color:#3fb950">MSRP $' + d.msrp.toFixed(2) + ' filled</span>';
        if (d.sale > 0 && d.sale < d.msrp) {
          var pct = ((1 - d.sale / d.msrp) * 100).toFixed(0);
          html += '<br><span style="color:#34d399;font-size:11px">sale $' + d.sale.toFixed(2) + ' (' + pct + '% off)</span>';
        }
        if (d.in_stock) html += ' <span style="color:#3fb950;font-size:11px">In Stock</span>';
        metaEl.innerHTML = html;
      }
    });
  });

  // Expose function for the app to trigger S&S price extraction
  window._reytechFetchSswwPrice = function(url, idx) {
    var popup = window.open(url + '#reytech', '_ssww_' + idx, 'width=420,height=350,left=50,top=50');
    if (!popup) {
      console.warn('[Reytech Extension] Popup blocked — allow popups for this site');
    }
  };
})();
