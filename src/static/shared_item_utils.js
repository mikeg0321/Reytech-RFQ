/**
 * shared_item_utils.js — Shared between pc_detail.html and rfq_detail.html
 * DO NOT copy these into individual pages. Import via <script src>.
 * Any fix here automatically applies to both PC and RFQ.
 *
 * Functions defined here may be overridden by page-specific versions
 * if the page defines the same function name AFTER this script loads.
 */

/** Check if a string is a URL */
function _isUrl(v) {
  if (!v || typeof v !== 'string') return false;
  v = v.trim();
  return /^https?:\/\//i.test(v) || /^[a-z0-9-]+\.[a-z]{2,}\//i.test(v) || /\.(com|org|net|gov|edu|io|co|us)\b/i.test(v);
}

/** HTML-escape a string */
function _escH(s) {
  if (!s) return '';
  var d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

/** Detect supplier from URL — comprehensive domain list */
function _detectSupplierJS(url) {
  if (!url) return '';
  try {
    var host = new URL(url).hostname.replace(/^www\./, '');
    var _domains = {
      'amazon.com': 'Amazon', 'amzn.': 'Amazon',
      'grainger.com': 'Grainger', 'mcmaster.com': 'McMaster-Carr',
      'mcmaster-carr': 'McMaster-Carr', 'fishersci.com': 'Fisher Scientific',
      'medline.com': 'Medline', 'boundtree.com': 'Bound Tree Medical',
      'henryschein.com': 'Henry Schein', 'uline.com': 'Uline',
      'zoro.com': 'Zoro', 'staples.com': 'Staples', 'waxie.com': 'Waxie',
      'fastenal.com': 'Fastenal', 'globalindustrial.com': 'Global Industrial',
      'homedepot.com': 'Home Depot', 'officedepot.com': 'Office Depot',
      'odpbusiness.com': 'Office Depot', 'aedstore.com': 'AED Store',
      'aed.com': 'AED Superstore', 'concordancehealthcare.com': 'Concordance Healthcare',
      'vwr.com': 'VWR', 'thermofisher.com': 'Thermo Fisher',
      'lowes.com': "Lowe's", 'sysco.com': 'Sysco', 'shoplet.com': 'Shoplet',
      'quill.com': 'Quill', 'usfoods.com': 'US Foods',
      'walmart.com': 'Walmart', 'target.com': 'Target',
      'costco.com': 'Costco', 'webstaurantstore.com': 'WebstaurantStore',
      'ssww.com': 'S&S Worldwide'
    };
    for (var dom in _domains) {
      if (host.includes(dom)) return _domains[dom];
    }
    var parts = host.split('.');
    return parts.length >= 2
      ? parts[parts.length - 2].charAt(0).toUpperCase() + parts[parts.length - 2].slice(1)
      : host;
  } catch (e) { return ''; }
}

/** Close bulk paste modal — shared between PC and RFQ */
function closeBulkPaste() {
  var m = document.getElementById('bulkPasteModal');
  if (m) m.style.display = 'none';
}

/**
 * Fire a link lookup for a URL.
 * mode: 'pc' or 'rfq' — determines which meta element ID to use.
 * Calls page-specific _applyLinkData(idx, d, mode) with the result.
 */
function _fireLinkLookup(idx, url, mode) {
  if (!url || !_isUrl(url)) return;
  var metaId = (mode === 'rfq') ? 'rfq_link_meta_' + idx : 'link_meta_' + idx;
  var metaEl = document.getElementById(metaId);
  if (metaEl) metaEl.innerHTML = '<span style="color:#d29922">\u23F3 Looking up\u2026</span>';
  fetch('/api/item-link/lookup', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({url: url, idx: idx})
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (!d.ok && d.error) {
      if (metaEl) metaEl.innerHTML = '<span style="color:#f85149">\u26A0\uFE0F ' + d.error + '</span>';
      return;
    }
    _applyLinkData(idx, d, mode);
  })
  .catch(function() {
    if (metaEl) metaEl.innerHTML = '<span style="color:#f85149">Lookup failed</span>';
  });
}
