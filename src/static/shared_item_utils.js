/**
 * shared_item_utils.js — Shared between pc_detail.html and rfq_detail.html
 * DO NOT copy these into individual pages. Import via <script src>.
 * Any fix here automatically applies to both PC and RFQ.
 *
 * Functions defined here may be overridden by page-specific versions
 * if the page defines the same function name AFTER this script loads.
 */

/** Compare PC description to lookup product title — returns 0-100 match score.
 *  Uses recall-weighted scoring: how much of the PC description appears in the
 *  found product title. Extra words in Amazon titles (brand, "Bulk", "Assorted")
 *  are not penalized heavily. Formula: (2*recall + precision) / 3 * 100.
 */
function _productMatchScore(pcDesc, lookupTitle) {
  if (!pcDesc || !lookupTitle) return 0;
  var _stopwords = ['the','and','for','with','pack','of','per','ea','each','box',
    'pk','set','in','by','to','is','it','at','on','or','an','as','from','bulk',
    'assorted','count','ct','qty','quantity','item','product','new','brand'];
  function _tokenize(s) {
    s = s.toLowerCase();
    // Normalize dimensions: "8.5" x 11", "8.5 x 11", "8.5x11" all → "8.5x11"
    s = s.replace(/(\d+\.?\d*)\s*[""\u201D]?\s*[xX\u00D7]\s*(\d+\.?\d*)\s*[""\u201D]?/g, '$1x$2');
    // Preserve decimals in numbers before stripping punctuation
    s = s.replace(/(\d)\.(\d)/g, '$1_D_$2');
    s = s.replace(/[^a-z0-9\s_]/g, ' ');
    s = s.replace(/_D_/g, '.');
    return s.split(/\s+/).filter(function(w) {
      return w.length > 1 && _stopwords.indexOf(w) < 0;
    });
  }
  var a = _tokenize(pcDesc), b = _tokenize(lookupTitle);
  if (!a.length || !b.length) return 0;
  var setA = {}, setB = {};
  a.forEach(function(w){ setA[w] = true; });
  b.forEach(function(w){ setB[w] = true; });
  var overlap = 0;
  for (var w in setA) { if (setB[w]) overlap++; }
  var sizeA = Object.keys(setA).length, sizeB = Object.keys(setB).length;
  var recall = overlap / sizeA;       // what % of PC tokens found in lookup
  var precision = overlap / sizeB;    // what % of lookup tokens match PC
  return Math.round((2 * recall + precision) / 3 * 100);
}

/** Check if a string is a URL */
function _isUrl(v) {
  if (!v || typeof v !== 'string') return false;
  v = v.trim();
  // Bare ASIN (B0XXXXXXXXX) or ISBN-10 (10 digits) or ISBN-13 (13 digits)
  if (/^B0[A-Z0-9]{8}$/i.test(v) || /^\d{10}$/.test(v) || /^\d{13}$/.test(v)) return true;
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
  // Bare ASIN/ISBN → convert to Amazon URL and update the link field
  var _bare = url.trim();
  if (/^B0[A-Z0-9]{8}$/i.test(_bare) || /^\d{10}$/.test(_bare) || /^\d{13}$/.test(_bare)) {
    url = 'https://www.amazon.com/dp/' + _bare;
    var _linkEl = document.querySelector('[name="link_' + idx + '"]');
    if (_linkEl) _linkEl.value = url;
  }
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
  // Send PC description for server-side semantic matching (Claude AI)
  var _pcDescForLookup = '';
  var _pcDescInput = document.querySelector('[name="desc_' + idx + '"]');
  if (_pcDescInput) _pcDescForLookup = (_pcDescInput.value || '').trim();

  fetch('/api/item-link/lookup', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({url: url, idx: idx, pc_description: _pcDescForLookup})
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
  var isPC = (mode === 'pc');
  var isAmazon = d.supplier === 'Amazon';

  // S&S Worldwide: always open popup for price extraction via extension
  // (server can't scrape S&S due to Cloudflare, extension reads the real page)
  var _url = d.url || '';
  if (_url.indexOf('ssww.com') >= 0 && (!d.price || d.price <= 0)) {
    window.open(_url, '_ssww_' + idx, 'width=500,height=400,left=50,top=50');
  }

  // On PC: NEVER overwrite description or MFG# — buyer's 704 data is sacred.
  // Only overwrite if substitute checkbox is checked (replacement item mode).
  // On RFQ: Amazon overwrites (structured format), others only if empty.
  var isSubstitute = false;
  if (isPC) {
    var subEl = document.querySelector('[name="substitute_' + idx + '"]');
    isSubstitute = subEl && subEl.checked;
  }

  var descEl = document.querySelector('[name="desc_' + idx + '"]');
  if (descEl && d.description) {
    var cur = (descEl.value || '').trim();
    // PC: only fill if substitute mode OR field is truly empty (buyer's 704 is sacred)
    // RFQ: always overwrite when lookup returns longer/better description
    var shouldUpdateDesc = isPC
      ? (isSubstitute || !cur || cur.length < 3)
      : (!cur || cur.length < 5 || d.description.length > cur.length || isAmazon);
    if (shouldUpdateDesc) {
      var descWasReadOnly = descEl.readOnly;
      descEl.readOnly = false; // temporarily unlock for auto-fill
      descEl.value = d.description;
      descEl.readOnly = descWasReadOnly; // restore lock state
      filled.push('desc');
      if (descEl.tagName === 'TEXTAREA') { descEl.style.height = 'auto'; descEl.style.height = Math.min(descEl.scrollHeight, 120) + 'px'; }
    }
  }

  // MFG#: never use ASIN (B0XXXXXXXX) as part number — procurement requires real MFG#
  // PC: only fill if substitute mode or empty. RFQ: always overwrite with real MFG#.
  var mfgEl = document.querySelector('[name="itemnum_' + idx + '"]') || document.querySelector('[name="part_' + idx + '"]');
  var mfgVal = (d.mfg_number || d.part_number || '').trim();
  if (mfgEl && mfgVal) {
    var isAsin = /^B0[A-Z0-9]{8}$/.test(mfgVal);
    if (!isAsin) {
      var curMfg = (mfgEl.value || '').trim();
      if (isPC ? (isSubstitute || !curMfg) : true) {
        var mfgWasLocked = mfgEl.readOnly;
        mfgEl.readOnly = false; // unlock for auto-fill from lookup
        mfgEl.value = mfgVal;
        mfgEl.readOnly = mfgWasLocked; // restore
        filled.push('MFG# ' + mfgVal);
      }
    }
  }

  // Cost: update when URL pasted, but NOT if match confidence is too low
  var costEl = document.querySelector('[name="cost_' + idx + '"]');
  // Pre-compute match score to gate cost fill
  var _matchScore = 100;
  var _lookupT = d.title || d.description || '';
  var _pcDescE = document.querySelector('[name="desc_' + idx + '"]');
  var _pcDescV = _pcDescE ? (_pcDescE.value || '').trim() : '';
  if (_lookupT && _pcDescV && typeof _productMatchScore === 'function') {
    _matchScore = _productMatchScore(_pcDescV, _lookupT);
  }
  // Server-side AI confidence (Claude semantic match) can override token score
  var _serverConf = parseFloat(d.server_confidence) || 0;
  var _serverMatch = !!d.server_match;
  var _aiVerified = (_serverMatch && _serverConf >= 0.70);

  if (costEl && d.price && d.price > 0) {
    var existingCost = parseFloat(costEl.value) || 0;
    // Block auto-fill if match score < 40% AND server AI didn't verify
    if (_matchScore < 40 && !_aiVerified) {
      filled.push('cost BLOCKED — low match ' + _matchScore + '%, verify product');
    } else {
      var msrp = d.list_price ? parseFloat(d.list_price) : d.price;
      costEl.value = (msrp || d.price).toFixed(2);
      filled.push('cost $' + (msrp || d.price).toFixed(2));
    }
    // Warn (but don't block) if price is very different from catalog
    if (existingCost > 0 && d.price > existingCost * 3) {
      filled.push('was $' + existingCost.toFixed(2) + ' catalog');
    }
    if (typeof recalcFromMarkup === 'function') recalcFromMarkup(idx);
    else if (typeof recalcRow === 'function') recalcRow(idx, true);
    else if (typeof recalc === 'function') recalc();
    // Store discount cost if list_price > sale_price (any supplier, not just S&S)
    if (d.list_price && d.sale_price && parseFloat(d.list_price) > parseFloat(d.sale_price)) {
      var _lp = parseFloat(d.list_price), _sp = parseFloat(d.sale_price);
      var row = document.querySelector('tr[data-row="' + idx + '"]');
      if (row) row.setAttribute('data-discount-cost', _sp.toFixed(2));
      var _disc = ((1 - _sp / _lp) * 100).toFixed(0);
      if (metaEl) metaEl.innerHTML = '<span style="color:#d29922">list $' + _lp.toFixed(2)
        + ' (sale: $' + _sp.toFixed(2) + ', ' + _disc + '% off)</span>';
      // Persist sale_price via hidden input so autosave stores it on the item
      var _spEl = document.querySelector('[name="saleprice_' + idx + '"]');
      if (!_spEl && row) {
        _spEl = document.createElement('input');
        _spEl.type = 'hidden'; _spEl.name = 'saleprice_' + idx;
        row.querySelector('td').appendChild(_spEl);
      }
      if (_spEl) _spEl.value = _sp.toFixed(2);
      var _lpEl = document.querySelector('[name="listprice_' + idx + '"]');
      if (!_lpEl && row) {
        _lpEl = document.createElement('input');
        _lpEl.type = 'hidden'; _lpEl.name = 'listprice_' + idx;
        row.querySelector('td').appendChild(_lpEl);
      }
      if (_lpEl) _lpEl.value = _lp.toFixed(2);
    }
  }

  // Persist photo_url as hidden input so autosave can write it to catalog
  if (d.photo_url) {
    var row = document.querySelector('tr[data-row="' + idx + '"]');
    var _phEl = document.querySelector('[name="photo_url_' + idx + '"]');
    if (!_phEl && row) {
      _phEl = document.createElement('input');
      _phEl.type = 'hidden'; _phEl.name = 'photo_url_' + idx;
      row.querySelector('td').appendChild(_phEl);
    }
    if (_phEl) _phEl.value = d.photo_url;
  }

  // Normalize URL field to canonical form (preserve scroll position)
  var linkEl = document.querySelector('[name="link_' + idx + '"]');
  if (linkEl && d.url && d.url !== linkEl.value) {
    var _scrollY = window.scrollY;
    linkEl.value = d.url;
    window.scrollTo(0, _scrollY);
  }

  // Supplier badge
  if (d.supplier) { var badge = document.getElementById('supplier_badge_' + idx); if (badge) badge.textContent = d.supplier; }

  // Refresh Sources column after new price data arrives
  if (filled.length && typeof _refreshSources === 'function') {
    _refreshSources(idx);
  }

  // Status message
  var statusHtml = '';
  if (metaEl && filled.length) {
    statusHtml = '<span style="color:#3fb950">' + filled.join(', ') + ' filled</span>';
  } else if (metaEl && d.ok === false) {
    statusHtml = '<span style="color:#f85149">' + (d.error || 'Lookup failed') + '</span>';
  } else if (metaEl && d.supplier && (!d.price || d.price <= 0) && costEl && !(parseFloat(costEl.value) > 0)) {
    // Price not found AND cost field is empty — show dual-price quick-entry
    var _qeId = '_qe_cost_' + idx;
    var _qeDiscId = '_qe_disc_' + idx;
    statusHtml = '<div style="display:flex;flex-direction:column;gap:4px">'
      + '<div style="display:flex;align-items:center;gap:4px">'
      + '<span style="color:#d29922;font-size:12px;min-width:38px">MSRP</span>'
      + '<input type="text" id="' + _qeId + '" inputmode="decimal" placeholder="0.00" '
      + 'style="width:70px;background:#21262d;border:1px solid #d29922;border-radius:4px;'
      + 'color:#e6edf3;font-size:14px;font-weight:700;padding:3px 6px;text-align:center;'
      + 'font-family:JetBrains Mono,monospace">'
      + '</div>'
      + '<div style="display:flex;align-items:center;gap:4px">'
      + '<span style="color:#3fb950;font-size:12px;min-width:38px">Sale</span>'
      + '<input type="text" id="' + _qeDiscId + '" inputmode="decimal" placeholder="optional" '
      + 'style="width:70px;background:#21262d;border:1px solid #30363d;border-radius:4px;'
      + 'color:#3fb950;font-size:14px;font-weight:700;padding:3px 6px;text-align:center;'
      + 'font-family:JetBrains Mono,monospace">'
      + ' <span style="color:#8b949e;font-size:11px">'
      + '<a href="' + (d.url || '') + '" target="_blank" style="color:#58a6ff">' + (d.supplier || '') + '</a>'
      + '</span></div></div></div>';
    // S&S: open popup for price extraction via extension.
    // First time: user clicks Cloudflare "Verify" once. After that, cookie persists
    // and all subsequent S&S popups load instantly with auto-extraction.
    var _sswUrl = d.url || '';
    if (_sswUrl.indexOf('ssww.com') >= 0) {
      setTimeout(function() {
        window.open(_sswUrl, '_ssww_' + idx, 'width=500,height=400,left=50,top=50');
      }, 300);
    }
  }
  // ASIN: informational badge only — NEVER in description or part# field
  if (d.asin) {
    statusHtml += ' <span style="font-size:11px;background:rgba(255,153,0,.12);color:#ff9900;padding:2px 6px;border-radius:3px;margin-left:4px">' +
      'ASIN: <a href="https://www.amazon.com/dp/' + d.asin + '" target="_blank" style="color:#ff9900">' + d.asin + ' ↗</a></span>';
  }
  // Product match validation: compare lookup title to PC description
  var _lookupTitle = d.title || d.description || '';
  var _pcDescEl = document.querySelector('[name="desc_' + idx + '"]');
  var _pcDesc = _pcDescEl ? (_pcDescEl.value || '').trim() : '';
  if (_lookupTitle && _pcDesc) {
    var _ms = _productMatchScore(_pcDesc, _lookupTitle);
    // AI-verified overrides token score badge
    if (_aiVerified) {
      statusHtml += ' <span style="font-size:11px;font-weight:600;padding:2px 6px;border-radius:3px;background:#3fb95020;color:#3fb950;border:1px solid #3fb95040;cursor:help" '
        + 'title="Claude AI confirmed match (' + Math.round(_serverConf * 100) + '% confidence)\nPC: ' + _pcDesc.substring(0,60).replace(/"/g,'&quot;') + '\nFound: ' + _lookupTitle.substring(0,60).replace(/"/g,'&quot;') + '">'
        + 'AI verified &#10003; ' + Math.round(_serverConf * 100) + '%</span>';
    } else {
      var _mClr = _ms >= 70 ? '#3fb950' : (_ms >= 40 ? '#d29922' : '#f85149');
      var _mLabel = _ms >= 70 ? '✓ Match' : (_ms >= 40 ? '~ Partial' : '✗ Wrong product?');
      statusHtml += ' <span style="font-size:11px;font-weight:600;padding:2px 6px;border-radius:3px;background:' + _mClr + '20;color:' + _mClr + ';border:1px solid ' + _mClr + '40;cursor:help" '
        + 'title="PC: ' + _pcDesc.substring(0,60).replace(/"/g,'&quot;') + '\nFound: ' + _lookupTitle.substring(0,60).replace(/"/g,'&quot;') + '">'
        + _mLabel + ' ' + _ms + '%</span>';
    }
    // For low matches without AI verification, show found title for manual verify
    if (_ms < 40 && !_aiVerified) {
      statusHtml += '<div style="font-size:11px;color:#f85149;margin-top:3px;line-height:1.3">'
        + '⚠ Found: <b>' + _lookupTitle.substring(0,80).replace(/</g,'&lt;') + '</b>'
        + '<br>Cost NOT auto-filled — verify this is the right product</div>';
    } else if (_ms < 70 && !_aiVerified) {
      statusHtml += '<div style="font-size:11px;color:#d29922;margin-top:2px">Found: '
        + _lookupTitle.substring(0,80).replace(/</g,'&lt;') + '</div>';
    }
  }
  if (metaEl && statusHtml) metaEl.innerHTML = statusHtml;

  // Attach Enter + blur handler to quick-entry cost fields (MSRP + optional Sale)
  var _qeInput = document.getElementById('_qe_cost_' + idx);
  var _qeDiscInput = document.getElementById('_qe_disc_' + idx);
  if (_qeInput) {
    function _applyQeDual() {
      var msrp = parseFloat(_qeInput.value);
      if (!(msrp > 0)) return;
      var sale = _qeDiscInput ? parseFloat(_qeDiscInput.value) : 0;
      // Set MSRP as the cost (safe bid basis)
      var c = document.querySelector('[name=cost_' + idx + ']');
      if (c) c.value = msrp.toFixed(2);
      // Store discount cost on the row for dual-profit calculation
      var row = document.querySelector('tr[data-row="' + idx + '"]');
      if (row && sale > 0 && sale < msrp) {
        row.setAttribute('data-discount-cost', sale.toFixed(2));
      }
      if (typeof recalcRow === 'function') recalcRow(idx, true);
      if (typeof recalcPC === 'function') recalcPC();
      if (typeof triggerPcAutosave === 'function') triggerPcAutosave();
      // Show confirmation with both prices
      var container = _qeInput.closest('div').parentElement || _qeInput.parentElement;
      var msg = '<span style="color:#3fb950">cost $' + msrp.toFixed(2) + ' filled</span>';
      if (sale > 0 && sale < msrp) {
        var savings = ((1 - sale / msrp) * 100).toFixed(0);
        msg += '<br><span style="color:#3fb950;font-size:11px">sale $' + sale.toFixed(2)
          + ' (' + savings + '% off) — extra margin if discount holds</span>';
      }
      if (container) container.innerHTML = msg;
    }
    _qeInput.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (_qeDiscInput && !_qeDiscInput.value) { _qeDiscInput.focus(); return; }
        _applyQeDual();
      }
    });
    if (_qeDiscInput) {
      _qeDiscInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') { e.preventDefault(); _applyQeDual(); }
      });
      _qeDiscInput.addEventListener('blur', function() {
        if (parseFloat(_qeInput.value) > 0) _applyQeDual();
      });
    }
    _qeInput.addEventListener('blur', function() {
      if (_qeDiscInput && document.activeElement === _qeDiscInput) return;
      if (parseFloat(this.value) > 0) setTimeout(function() {
        if (document.activeElement !== _qeDiscInput) _applyQeDual();
      }, 200);
    });
    var _sy = window.scrollY;
    _qeInput.focus({preventScroll: true});
    window.scrollTo(0, _sy);
  }

  // Autosave + recalc (preserve scroll position)
  if (filled.length) {
    var _sy2 = window.scrollY;
    if (typeof triggerAutosave === 'function') triggerAutosave();
    if (typeof recalcPC === 'function') recalcPC();
    requestAnimationFrame(function(){ window.scrollTo(0, _sy2); });
  }
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
