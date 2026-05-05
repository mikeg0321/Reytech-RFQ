"""Both LLM-driven price searches must forbid parenthesized per-unit
annotations (Surface #7).

Mike's 2026-05-04 chain (project_session_2026_05_04 surface #7): Heel Donut
listing showed "$7.99 ($4.00 / count)" — pair price + per-cushion math.
The scraper grabbed the parenthesized "/count" figure ($4) instead of the
$7.99 headline → 50% under-quote.

Two LLM-driven paths handle Amazon-style price extraction:
1. `src/agents/product_research.py:_grok_search` (Grok / xAI)
2. `src/agents/web_price_research.py:search_product_price` (Claude Haiku)

Both must explicitly forbid using parenthesized per-unit annotations as the
returned price. These tests assert the prompt text contains the guidance
so a future "let me trim the prompt" PR fails fast.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path


# ─── Path A: Grok search in product_research.py ──────────────────────────

def test_grok_prompt_forbids_per_unit_annotations():
    src = Path("src/agents/product_research.py").read_text(encoding="utf-8")
    # The full _grok_search function block.
    m = re.search(r"def _grok_search\(.*?\n(?:.*\n)*?(?=\ndef |\Z)", src)
    assert m, "_grok_search not found"
    body = m.group(0)
    # Must mention "headline" + forbid parenthesized per-unit annotations.
    assert "HEADLINE" in body or "headline" in body.lower(), (
        "Grok prompt must reference HEADLINE price. Surface #7 regression: "
        "a future trim-the-prompt PR removed the guidance and Mike will "
        "under-quote by 50% the next time a listing has a per-count line."
    )
    # The example case Mike actually saw must be in the prompt as a worked
    # example — that's what the LLM will pattern-match against. Quote style
    # may vary by f-string nesting; check the literal substrings.
    assert "$7.99" in body and "$4.00 / count" in body, (
        "The exact Heel Donut example ($7.99 ($4.00 / count)) must remain "
        "in the prompt as a concrete worked example."
    )


def test_grok_prompt_forbids_named_per_unit_keys():
    src = Path("src/agents/product_research.py").read_text(encoding="utf-8")
    m = re.search(r"def _grok_search\(.*?\n(?:.*\n)*?(?=\ndef |\Z)", src)
    body = m.group(0)
    # Must enumerate at least the count / oz / pair examples — those are
    # the three Amazon-style per-unit annotations that empirically trip
    # the model.
    for needle in ("/ count", "/ oz", "/ pair"):
        assert needle in body, (
            f"Grok prompt missing per-unit example {needle!r} — without "
            f"concrete examples the LLM may still pick those up."
        )


# ─── Path B: Claude web_search in web_price_research.py ───────────────────

def test_web_price_research_sys_prompt_forbids_per_unit():
    src = Path("src/agents/web_price_research.py").read_text(encoding="utf-8")
    # Capture _SYS_PROMPT up to the closing paren.
    m = re.search(r"_SYS_PROMPT\s*=\s*\((.*?)\n\s*\)", src, re.DOTALL)
    assert m, "_SYS_PROMPT not found in web_price_research.py"
    sys_prompt = m.group(1)
    assert "HEADLINE" in sys_prompt or "headline" in sys_prompt.lower(), (
        "_SYS_PROMPT must reference HEADLINE price. Surface #7 regression."
    )
    # Quote style varies (single vs double) depending on f-string nesting;
    # check for the price pair literal regardless of quoting.
    assert "$7.99" in sys_prompt and "$4.00 / count" in sys_prompt, (
        "_SYS_PROMPT missing the Heel Donut worked example "
        "($7.99 ($4.00 / count))."
    )
    for needle in ("/ count", "/ oz", "/ pair"):
        assert needle in sys_prompt, (
            f"_SYS_PROMPT missing per-unit example {needle!r}"
        )


def test_web_price_research_user_prompt_always_includes_uom():
    """The previous shape skipped the UOM line when qty <= 1, so Claude had
    no way to disambiguate per-unit vs per-pack pricing for single-item
    requests. Surface #7 fix: always include UOM."""
    src = Path("src/agents/web_price_research.py").read_text(encoding="utf-8")
    # The user prompt is built right before the _SYS_PROMPT block.
    m = re.search(r"prompt\s*=\s*f\"\"\"(.*?)\"\"\"", src, re.DOTALL)
    assert m, "user prompt not found"
    user_prompt = m.group(1)
    assert "Quantity needed: {qty} {uom}" in user_prompt, (
        "User prompt must always include Quantity needed: {qty} {uom} — "
        "Surface #7 regression: the previous if-qty>1 gate hid UOM from "
        "Claude on every single-item query."
    )
    assert "per 1 {uom}" in user_prompt, (
        "User prompt must explicitly say 'per 1 {uom}' so Claude scales "
        "pack-of-N listings down to the buyer's unit."
    )


def test_search_product_price_signature_unchanged():
    """The fix must NOT change the public signature of search_product_price.
    Call sites in item_link_lookup.py / pc_enrichment_pipeline.py /
    product_catalog.py rely on (description, part_number, qty, uom, context)
    — same as before. Source-level guard so a future refactor that drops
    `uom` or `qty` from the signature fails fast."""
    from src.agents import web_price_research
    sig = inspect.signature(web_price_research.search_product_price)
    params = list(sig.parameters)
    # Order matters for positional callers; presence matters for kwargs.
    for required in ("description", "part_number", "qty", "uom", "context"):
        assert required in params, (
            f"search_product_price missing parameter {required!r} — would "
            f"break existing call sites."
        )
