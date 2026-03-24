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
  // 15s client-side timeout — spinner can never hang forever
  var _done = false;
  var _timer = setTimeout(function() {
    if (_done) return;
    _done = true;
    if (metaEl) metaEl.innerHTML = '<span style="color:#d29922">Lookup timed out \u2014 paste cost manually</span>';
  }, 15000);
  fetch('/api/item-link/lookup', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({url: url, idx: idx})
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (_done) return; // timeout already fired
    _done = true; clearTimeout(_timer);
    if (!d.ok && d.error) {
      if (metaEl) metaEl.innerHTML = '<span style="color:#f85149">\u26A0\uFE0F ' + d.error + '</span>';
      return;
    }
    _applyLinkData(idx, d, mode);
  })
  .catch(function() {
    if (_done) return;
    _done = true; clearTimeout(_timer);
    if (metaEl) metaEl.innerHTML = '<span style="color:#f85149">Lookup failed</span>';
  });
}

/** Open bulk paste modal */
function openBulkPaste() {
  var m = document.getElementById('bulkPasteModal');
  if (m) m.style.display = 'flex';
  var ta = document.getElementById('bulkPasteArea');
  if (ta) { ta.value = ''; ta.focus(); }
  var p = document.getElementById('bulkPreview');
  if (p) p.style.display = 'none';
}

/** Toggle source panel visibility */
function toggleSourcePanel() {
  var p = document.getElementById('source-panel');
  if (!p) return;
  p.style.display = (p.style.display !== 'none') ? 'none' : 'block';
}

/** Sanitize a price value — returns float or 0 */
function sanitizePrice(v) {
  if (!v && v !== 0) return 0;
  if (typeof v === 'number') return v;
  var f = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(f) ? 0 : f;
}

/** Sanitize an integer — returns int or default */
function sanitizeInt(v, d) {
  if (d === undefined) d = 0;
  if (!v && v !== 0) return d;
  var i = parseInt(v, 10);
  return isNaN(i) ? d : i;
}

/** Format currency for display */
function fmtCurrency(v) {
  var n = sanitizePrice(v);
  if (n === 0) return '\u2014';
  return '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
}

/**
 * Apply link lookup data to a row with overwrite protection.
 * mode: 'pc' or 'rfq' — determines which meta element ID to use.
 */
function _applyLinkData(idx, d, mode) {
  if (!d) return;
  var filled = [];
  var metaId = (mode === 'pc') ? 'link_meta_' + idx : 'rfq_link_meta_' + idx;
  var metaEl = document.getElementById(metaId);
  var descEl = document.querySelector('[name="desc_' + idx + '"]');
  if (descEl && d.description) {
    var cur = (descEl.value || '').trim();
    if (!cur || cur.length < 3) { descEl.value = d.description; filled.push('desc'); }
  }
  var mfgEl = document.querySelector('[name="itemnum_' + idx + '"]');
  if (mfgEl && d.part_number && !(mfgEl.value || '').trim()) { mfgEl.value = d.part_number; filled.push('mfg'); }
  var costEl = document.querySelector('[name="cost_' + idx + '"]');
  var ec = costEl ? (parseFloat(costEl.value) || 0) : 0;
  if (costEl && d.price && d.price > 0 && ec === 0) { costEl.value = d.price.toFixed(2); filled.push('cost'); }
  if (d.supplier) { var badge = document.getElementById('supplier_badge_' + idx); if (badge) badge.textContent = d.supplier; }
  if (metaEl && filled.length) { metaEl.textContent = filled.join(', ') + ' filled'; metaEl.style.color = '#3fb950'; }
}

/* ── CRM Buyer Autocomplete ─────────────────────────────────────────────── */
/* Used in New PC modal, New RFQ modal, and RFQ detail requestor field       */
var _crmAcDebounce = {};
var _crmAcCache = {};

function initCrmAutocomplete(inputId, opts) {
  var inp = document.getElementById(inputId);
  if (!inp) return;
  var drop = document.createElement('div');
  drop.id = inputId + '-ac-drop';
  drop.style.cssText = 'position:absolute;z-index:9999;background:#161b22;border:1px solid #30363d;' +
    'border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.5);width:320px;max-height:220px;' +
    'overflow-y:auto;display:none;font-size:13px;';
  inp.parentNode.style.position = 'relative';
  inp.parentNode.appendChild(drop);

  function showDrop(contacts) {
    drop.innerHTML = '';
    if (!contacts.length) { drop.style.display='none'; return; }
    contacts.forEach(function(c) {
      var row = document.createElement('div');
      var name = c.buyer_name || c.name || '';
      var inst = c.agency || c.institution || c.department || '';
      var email = c.buyer_email || c.email || '';
      row.style.cssText = 'padding:8px 12px;cursor:pointer;border-bottom:1px solid #21262d;';
      row.innerHTML = '<div style="font-weight:600;color:#e6edf3">' + _escH(name) + '</div>' +
        (inst ? '<div style="color:#8b949e;font-size:12px">' + _escH(inst) + '</div>' : '') +
        (email ? '<div style="color:#58a6ff;font-size:11px">' + _escH(email) + '</div>' : '');
      row.addEventListener('mouseenter', function(){ this.style.background='rgba(79,140,255,.1)'; });
      row.addEventListener('mouseleave', function(){ this.style.background=''; });
      row.addEventListener('mousedown', function(e) {
        e.preventDefault();
        inp.value = name;
        drop.style.display = 'none';
        if (opts && opts.onSelect) opts.onSelect(c);
      });
      drop.appendChild(row);
    });
    drop.style.display = 'block';
  }

  inp.addEventListener('input', function() {
    var q = inp.value.trim();
    if (q.length < 2) { drop.style.display='none'; return; }
    clearTimeout(_crmAcDebounce[inputId]);
    _crmAcDebounce[inputId] = setTimeout(function() {
      if (_crmAcCache[q]) { showDrop(_crmAcCache[q]); return; }
      fetch('/api/crm/search?q=' + encodeURIComponent(q), {credentials:'same-origin'})
        .then(function(r) { return r.json(); })
        .then(function(d) {
          var results = d.ok ? (d.contacts || []) : [];
          _crmAcCache[q] = results;
          showDrop(results);
        }).catch(function() { drop.style.display='none'; });
    }, 250);
  });

  inp.addEventListener('blur', function() {
    setTimeout(function() { drop.style.display='none'; }, 200);
  });
  inp.addEventListener('focus', function() {
    if (inp.value.trim().length >= 2) inp.dispatchEvent(new Event('input'));
  });
}
