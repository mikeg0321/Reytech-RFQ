/**
 * Reytech Bridge — Content Script
 * Runs on the Reytech app. Receives prices from background worker.
 */
(function() {
  console.log('[Reytech Bridge] Loaded on', location.hostname);

  chrome.runtime.onMessage.addListener(function(d) {
    if (!d || d.type !== 'reytech_ssww_prices' || !d.msrp) return;
    console.log('[Reytech Bridge] Got prices:', d);

    var linkInputs = document.querySelectorAll('input[name^="link_"]');
    linkInputs.forEach(function(inp) {
      var url = (inp.value || '').trim();
      if (!url || url.indexOf('ssww.com') < 0) return;

      var urlItem = url.match(/[\-\/]([A-Z]?\d{4,6})\/?(\?|$)/i);
      var msgItem = d.item_number || '';
      if (!urlItem || !msgItem) return;
      if (urlItem[1].toUpperCase() !== msgItem.toUpperCase()) return;

      var idx = inp.name.replace('link_', '');
      console.log('[Reytech Bridge] Filling row ' + idx + ': MSRP=$' + d.msrp + ' Sale=$' + d.sale);

      var costEl = document.querySelector('[name=cost_' + idx + ']');
      if (costEl && d.msrp > 0) costEl.value = d.msrp.toFixed(2);

      var row = document.querySelector('tr[data-row="' + idx + '"]');
      if (row && d.sale > 0 && d.sale < d.msrp) {
        row.setAttribute('data-discount-cost', d.sale.toFixed(2));
      }

      if (typeof recalcRow === 'function') recalcRow(parseInt(idx), true);
      if (typeof recalcPC === 'function') recalcPC();
      if (typeof triggerPcAutosave === 'function') triggerPcAutosave();

      var metaEl = document.getElementById('link_meta_' + idx);
      if (metaEl) {
        var html = '<span style="color:#3fb950">MSRP $' + d.msrp.toFixed(2) + ' auto-filled</span>';
        if (d.sale > 0 && d.sale < d.msrp) {
          var pct = ((1 - d.sale / d.msrp) * 100).toFixed(0);
          html += '<br><span style="color:#34d399;font-size:11px">sale $' + d.sale.toFixed(2) + ' (' + pct + '% off)</span>';
        }
        metaEl.innerHTML = html;
      }
    });
  });

  window._reytechFetchSswwPrice = function(url, idx) {
    window.open(url, '_ssww_' + idx, 'width=420,height=350,left=50,top=50');
  };
})();
