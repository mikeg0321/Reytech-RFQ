"""CP-2 regression guard: SCPRS price extraction must go through the
canonical `scprs_per_unit` helper at every consumer site.

Audited 2026-04-22. History:
  * `scprs_po_lines.unit_price` stores LINE TOTALS (a 100-qty PO at $5/each
    is 500).
  * `won_quotes_db._sync_scprs_to_won_quotes` divides by qty at ingestion,
    so `won_quotes.unit_price` IS per-unit.
  * `find_similar_items()` returns quote dicts from the `won_quotes` table.
  * `pc_enrichment_pipeline.py` had a defensive "divide again if qty > 1"
    block that was correct BEFORE the ingestion fix and silently wrong
    AFTER — it was over-dividing every multi-unit PO by its original qty
    (often 100+), yielding penny-valued scprs_price fields shipped into
    dashboards.
  * 4 other consumer sites read `quote.get("unit_price")` inline, correct
    today but trivially broken if won_quotes ever starts storing line
    totals again.

Fix: a shared `scprs_per_unit(quote)` helper in won_quotes_db.py owns
the contract, and every consumer calls it.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_scprs_per_unit_helper_exists():
    """The canonical helper must live in won_quotes_db.py."""
    from src.knowledge.won_quotes_db import scprs_per_unit
    assert callable(scprs_per_unit)


def test_scprs_per_unit_returns_unit_price_verbatim():
    """won_quotes.unit_price is already per-unit. Helper must NOT divide
    by the stored quantity (that was the over-divide bug)."""
    from src.knowledge.won_quotes_db import scprs_per_unit
    # A realistic row: $5/each, original PO qty was 100.
    quote = {"unit_price": 5.00, "quantity": 100}
    assert scprs_per_unit(quote) == 5.00, (
        "CP-2 regression: helper is dividing unit_price by quantity again. "
        "won_quotes.unit_price is already per-unit — dividing produces "
        "penny prices."
    )


def test_scprs_per_unit_handles_missing_and_invalid():
    """Helper must tolerate absent keys, None, strings, and zero."""
    from src.knowledge.won_quotes_db import scprs_per_unit
    assert scprs_per_unit({}) == 0.0
    assert scprs_per_unit({"unit_price": None}) == 0.0
    assert scprs_per_unit({"unit_price": "not a number"}) == 0.0
    assert scprs_per_unit({"unit_price": 0}) == 0.0
    assert scprs_per_unit({"unit_price": -5}) == 0.0


def test_pc_enrichment_pipeline_uses_helper():
    """pc_enrichment_pipeline.py must call the helper and must NOT have
    the old `scprs_price / scprs_qty` double-divide."""
    src = (REPO_ROOT / "src" / "agents" / "pc_enrichment_pipeline.py").read_text(encoding="utf-8")
    # Strip full-line comments so audit-intent docstrings don't false-trip.
    code = "\n".join(
        ln for ln in src.splitlines() if not ln.lstrip().startswith("#")
    )
    assert "scprs_per_unit" in code, (
        "CP-2 regression: pc_enrichment_pipeline.py no longer imports "
        "scprs_per_unit. The previous manual divide is the 2026-04-22 bug."
    )
    assert "scprs_price / scprs_qty" not in code, (
        "CP-2 regression: pc_enrichment_pipeline.py re-introduced the "
        "`scprs_price / scprs_qty` over-divide. won_quotes.unit_price is "
        "already per-unit; dividing again produces penny prices."
    )


def test_four_consumer_sites_use_helper():
    """The 4 inline `quote.get('unit_price')` sites must now go through
    scprs_per_unit. If a future refactor adds back a raw read in the same
    shape, this guard fires."""
    sites = [
        REPO_ROOT / "src" / "forms" / "price_check.py",
        REPO_ROOT / "src" / "api" / "modules" / "routes_pricecheck.py",
        REPO_ROOT / "src" / "auto" / "auto_processor.py",
        REPO_ROOT / "src" / "api" / "modules" / "routes_analytics.py",
    ]
    for path in sites:
        assert path.exists(), f"{path} missing"
        src = path.read_text(encoding="utf-8")
        assert "scprs_per_unit" in src, (
            f"CP-2 regression: {path.name} no longer calls scprs_per_unit. "
            "Raw reads of quote['unit_price'] diverge from the canonical "
            "contract — ingestion-layer changes will silently break here."
        )


def test_scprs_assignments_do_not_use_raw_quote_unit_price():
    """Guard against regressions: no source site should assign
    `scprs_price = quote.get("unit_price"...)` directly. All reads must
    go through scprs_per_unit."""
    sites = [
        REPO_ROOT / "src" / "forms" / "price_check.py",
        REPO_ROOT / "src" / "api" / "modules" / "routes_pricecheck.py",
        REPO_ROOT / "src" / "auto" / "auto_processor.py",
        REPO_ROOT / "src" / "agents" / "pc_enrichment_pipeline.py",
    ]
    # pattern: `scprs_price` on LHS (possibly qualified) = `quote.get("unit_price"...)`
    bad = re.compile(r'scprs_price[^\n=]*=\s*[a-zA-Z_]\w*\.get\(\s*["\']unit_price["\']')
    for path in sites:
        src = path.read_text(encoding="utf-8")
        code = "\n".join(
            ln for ln in src.splitlines() if not ln.lstrip().startswith("#")
        )
        m = bad.search(code)
        assert not m, (
            f"CP-2 regression in {path.name}: raw `quote.get('unit_price')` "
            f"assigned to scprs_price. Use scprs_per_unit(quote) instead."
        )
