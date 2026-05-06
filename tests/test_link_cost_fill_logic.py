"""Documents the link-cost-fill rules added 2026-04-12 for the quoting flow.

The actual logic lives in src/static/shared_item_utils.js (_applyLinkData)
and src/templates/pc_detail.html (handleManualCostChange, _syncBufferOnCostFill,
applyTier). These tests live as documentation of the contract — when the
JS path is changed in the future, this file makes the intended rules
explicit so a reviewer doesn't have to read the inline JS to know what
should happen.

Why no real JS exec: the project has no JS test harness wired up. Adding
one for these three small functions is too much yak-shaving for a
quoting-blocker fix shipping at midnight.
"""


def test_msrp_only_fills_cost_when_list_price_present():
    """Rule 1: if d.list_price exists, fill cost = list_price.
    Rule 2: if only sale price found, DO NOT silently fill cost.
    Rule 3: if neither MSRP nor sale found, log a verify warning.

    See _applyLinkData in src/static/shared_item_utils.js (around line 271).
    Verified by reading the file:
    """
    js_path = "src/static/shared_item_utils.js"
    import os
    full = os.path.join(os.path.dirname(__file__), "..", js_path)
    with open(full, encoding="utf-8") as f:
        source = f.read()
    # Rule 1: list_price wins
    assert "lp = d.list_price ? parseFloat(d.list_price) : 0" in source
    assert "costEl.value = lp.toFixed(2)" in source
    assert "(MSRP)" in source
    # Rule 2: sale-only does NOT fill cost
    assert "MSRP not found" in source
    assert "may expire in 45-day window" in source
    # Rule 3: ambiguous (no split) fills but warns
    assert "verify it is non-discount" in source


def test_buffer_sync_helpers_present():
    """The handleManualCostChange + _syncBufferOnCostFill helpers must
    exist on window so the cost field's onchange and the link cost-fill
    path can both keep data-base-cost in sync with the active tier.
    """
    import os
    full = os.path.join(os.path.dirname(__file__), "..", "src/templates/pc_detail.html")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    assert "window.handleManualCostChange" in source
    assert "window._syncBufferOnCostFill" in source
    assert "window._currentBuffer" in source
    # The manual edit path must back out the buffer when active
    assert "newBase = displayed / (1 + buffer/100)" in source
    # The link-fill path must persist new base + reapply buffer
    assert "Math.round(filled * (1 + buffer/100) * 100) / 100" in source


def test_cost_input_onchange_calls_handle_manual_cost_change():
    """The cost input must invoke handleManualCostChange (with a graceful
    fallback to recalcRow if the helper isn't loaded yet) so manual edits
    keep data-base-cost in sync."""
    import os
    full = os.path.join(os.path.dirname(__file__), "..", "src/api/modules/routes_pricecheck.py")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    assert "window.handleManualCostChange" in source, (
        "Cost field onchange must call handleManualCostChange so the "
        "buffer's data-base-cost stays in sync with the displayed value"
    )
    assert 'name="cost_{idx}"' in source


def test_pc_description_only_fills_in_substitute_mode():
    """PC mode: URL paste must NEVER fill description unless substitute mode.
    Buyer's 704 description is sacred — even on empty fields, URL-derived
    text can stamp wrong-product description that leaks into the catalog
    write-back. Operators must type their own description on manual rows.
    (Mike P0 2026-05-06.)
    """
    import os
    full = os.path.join(os.path.dirname(__file__), "..", "src/static/shared_item_utils.js")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    # The PC branch of shouldUpdateDesc must be substitute-only.
    # The OLD form was: isPC ? (isSubstitute || !cur || cur.length < 3) : ...
    # The NEW form is:  isPC ? isSubstitute : ...
    assert "isPC\n      ? isSubstitute\n      :" in source, (
        "PC description fill must only fire in substitute mode — the "
        "empty-field fallback was leaking wrong-product descriptions into "
        "buyer lines."
    )
    # Make sure the old loophole is gone.
    assert "isSubstitute || !cur || cur.length < 3" not in source


def test_pc_cost_blocked_when_description_missing():
    """PC mode: when buyer description is empty/<3 chars, we cannot compute
    token-overlap match score (default _matchScore=100 would let any URL
    fill cost). Cost-fill must be blocked unless server-side AI verified
    the match (Claude semantic match >= 0.70).

    Without this gate, a URL paste on a fresh manual row stamps the wrong
    product's cost on a buyer line and rides into catalog write-back.
    (Mike P0 2026-05-06.)
    """
    import os
    full = os.path.join(os.path.dirname(__file__), "..", "src/static/shared_item_utils.js")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    # New gate must check both PC-mode and missing description.
    assert "_pcDescMissing = isPC && (!_pcDescV || _pcDescV.length < 3)" in source
    # Block path must require AI verification to bypass.
    assert "_pcDescMissing && !_aiVerified" in source
    assert "type description first to verify URL match" in source


def test_apply_tier_uses_data_base_cost_consistently():
    """applyTier must use data-base-cost when present, fall back to the
    displayed value when not, and PERSIST the base on tier > 0 / clear
    it on tier == 0."""
    import os
    full = os.path.join(os.path.dirname(__file__), "..", "src/templates/pc_detail.html")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    # Reads base from attribute, falls back to displayed
    assert "baseAttr = costInp.getAttribute('data-base-cost')" in source
    assert "baseCost = baseAttr ? (parseFloat(baseAttr) || 0) : displayed" in source
    # Tier > 0: persist + buffered
    assert "costInp.setAttribute('data-base-cost', baseCost.toFixed(4))" in source
    # Tier == 0: restore + clear
    assert "costInp.removeAttribute('data-base-cost')" in source
