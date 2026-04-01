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

      var urlItem = url.match(/[\-\/]([A-Z]{0,4}\d{3,6})\/?(\?|$)/i);
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

      // Dispatch change event so autosave detects the update
      if (costEl) costEl.dispatchEvent(new Event('change', {bubbles: true}));
      if (typeof recalcRow === 'function') recalcRow(parseInt(idx), true);
      if (typeof recalcPC === 'function') recalcPC();
      // Force autosave — invalidate last snapshot so it detects changes
      if (typeof window._pcInvalidateSave === 'function') window._pcInvalidateSave();
      if (typeof triggerPcAutosave === 'function') triggerPcAutosave();

      // Update Sources column with S&S price
      if (row) {
        var srcCell = row.querySelectorAll('td')[8];
        if (srcCell) {
          var srcHtml = '<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:13px;background:rgba(63,185,80,.12);border:1px solid rgba(63,185,80,.3);color:#3fb950;white-space:nowrap">'
            + '<b>$' + d.msrp.toFixed(2) + '</b> S&S Worldwide</span>';
          if (d.sale > 0 && d.sale < d.msrp) {
            srcHtml += '<br><span style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:12px;background:rgba(52,211,153,.12);border:1px solid rgba(52,211,153,.3);color:#34d399;white-space:nowrap;margin-top:2px">'
              + '<b>$' + d.sale.toFixed(2) + '</b> S&S Sale</span>';
          }
          // Replace any existing S&S badges, preserve others
          var existing = srcCell.innerHTML;
          // Remove old S&S badges to prevent duplicates
          existing = existing.replace(/<span[^>]*>.*?S&S.*?<\/span>(\s*<br>)?/gi, '');
          existing = existing.replace(/No sources/gi, '');
          srcCell.innerHTML = srcHtml + (existing.trim() ? '<br>' + existing.trim() : '');
        }
      }

      // Update link meta
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
