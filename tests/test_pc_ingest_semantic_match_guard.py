"""Mike P0 2026-05-05 (Phase 2): semantic match gate on Step 4b Grok first-pass.

Step 5b (web_price_research) ALREADY gates its found product against the
buyer's line description via `claude_semantic_match` — see
`pc_enrichment_pipeline.py` ~lines 860-884: if the semantic confidence is
< 0.60, the result is demoted to a suggestion and not stamped onto the line.

Step 4b (Grok first-pass, ~lines 712-733) had no such gate. Grok's own
`confidence` field tells you how confident Grok is in its OWN answer, not
whether that answer describes the same product as the buyer's line item.
On prod 2026-05-05 this surfaced as: line N says "Love Velvet Coloring
Poster", Grok returns an Anker product with confidence 0.85, Step 4b stamps
the Anker URL + cost + manufacturer onto line N.

Phase 2 fix: backport Step 5b's `claude_semantic_match` gate into Step 4b.
On rejection (sem_conf < 0.60) the Grok answer is demoted to
`p["llm_suggestion"]` + `p["_needs_web_search"] = True` and the stamping
block (unit_cost / item_link / vendor_cost / amazon_*) is skipped via
`continue` — exactly the same shape Step 5b uses.
"""
from pathlib import Path


def _read_step4b_block() -> str:
    """Return the source body of the Step 4b Grok first-pass loop."""
    src = Path(__file__).resolve().parent.parent / "src/agents/pc_enrichment_pipeline.py"
    body = src.read_text(encoding="utf-8")
    # The Step 4b block lives between "Grok first pass" comment and Step 5
    # (web price lookup). Bound the slice so guards don't accidentally pick
    # up Step 5b's already-existing claude_semantic_match call.
    start = body.find("# ── Step 4b: Grok")
    if start < 0:
        # Fallback: locate by the validate_product call site
        start = body.find("validate_product(")
    end = body.find("# ── Step 5", start + 10)
    assert start > 0 and end > start, (
        "Could not locate Step 4b block bounds — the comment markers "
        "moved; update test_pc_ingest_semantic_match_guard.py"
    )
    return body[start:end]


# ── Gate is wired in ───────────────────────────────────────────────


def test_step4b_calls_claude_semantic_match():
    """Step 4b must call claude_semantic_match on Grok's answer before
    stamping URL/cost/manufacturer onto the line."""
    block = _read_step4b_block()
    assert "claude_semantic_match" in block, (
        "Step 4b (Grok first-pass) must call claude_semantic_match to "
        "validate that Grok's answer describes the same product as the "
        "buyer's line description. Without this, Grok returns Anker on a "
        "Love Velvet Coloring Poster line at confidence 0.85 and Step 4b "
        "stamps the wrong URL/cost. Mirror Step 5b's pattern."
    )


def test_step4b_uses_060_threshold():
    """Threshold must match Step 5b (< 0.60 → demote to suggestion).
    Drift between Step 4b and Step 5b thresholds = silent contamination
    leak through the cheaper code path."""
    block = _read_step4b_block()
    assert "0.60" in block, (
        "Step 4b semantic match threshold must be 0.60 (parity with Step 5b)"
    )


def test_step4b_rejection_demotes_to_suggestion():
    """On semantic-match rejection, Step 4b must demote Grok's answer to
    p['llm_suggestion*'] fields (so a future surface can show 'Grok
    suggested X but it didn't match — please review') and set
    _needs_web_search=True so Step 5b retries with web search."""
    block = _read_step4b_block()
    # llm_suggestion field must be set in the rejection branch
    assert 'llm_suggestion' in block, (
        "Rejection branch must demote to p['llm_suggestion'] (not just drop "
        "the answer silently)"
    )
    assert '_needs_web_search' in block, (
        "Rejection branch must set p['_needs_web_search'] = True so Step 5b "
        "retries this line with web search"
    )


def test_step4b_rejection_skips_stamping():
    """The rejection branch must NOT stamp unit_cost / item_link /
    vendor_cost / amazon_* onto the line. Easiest way: `continue` after
    the rejection so the stamping block below is skipped."""
    block = _read_step4b_block()
    # The semantic match gate must skip the stamping block on rejection.
    # We verify by source pattern: there's a `continue` between the
    # claude_semantic_match call and the `p["unit_cost"] = _price` stamp.
    sm_idx = block.find("claude_semantic_match")
    stamp_idx = block.find('p["unit_cost"] = _price')
    assert sm_idx > 0 and stamp_idx > 0
    between = block[sm_idx:stamp_idx]
    assert "continue" in between, (
        "Step 4b must `continue` past the stamping block when "
        "claude_semantic_match rejects the answer. Without this, the "
        "rejection branch falls through and stamps the wrong product "
        "anyway."
    )


def test_step4b_threshold_matches_step5b():
    """Step 4b and Step 5b thresholds must be the same value. Drift
    between them re-opens the cross-contamination loophole through the
    cheaper code path."""
    src = Path(__file__).resolve().parent.parent / "src/agents/pc_enrichment_pipeline.py"
    body = src.read_text(encoding="utf-8")
    # Both Step 4b and Step 5b should use confidence < 0.60
    occurrences = body.count('confidence", 1) < 0.60')
    assert occurrences >= 2, (
        f"Expected at least 2 occurrences of `confidence\", 1) < 0.60` "
        f"(one for Step 4b, one for Step 5b). Found {occurrences}. "
        "If the thresholds drift apart, contamination leaks through "
        "the cheaper Grok path."
    )


# ── Gate must be in Step 4b specifically (not just somewhere else) ─


def test_gate_is_inside_grok_first_pass_block():
    """Pin the gate to Step 4b. If someone factors it out into a helper
    we want the helper called from Step 4b — verified by ensuring the
    semantic-match call lives inside the lexical block bounded by the
    Step 4b / Step 5 comment markers."""
    block = _read_step4b_block()
    # Both validate_product (Grok call) and claude_semantic_match must
    # appear in this block — the gate must live IN Step 4b, not somewhere
    # before or after it.
    assert "validate_product(" in block, (
        "Step 4b block boundaries are wrong — validate_product call "
        "should live inside it"
    )
    assert "claude_semantic_match" in block, (
        "Step 4b block must contain claude_semantic_match call inline"
    )


# ── Compile-check (pin import works after the edit) ────────────────


def test_pc_enrichment_pipeline_compiles():
    import py_compile
    src = Path(__file__).resolve().parent.parent / "src/agents/pc_enrichment_pipeline.py"
    py_compile.compile(str(src), doraise=True)
