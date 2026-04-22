"""CP-5 regression guards: src/core/catalog.py must be a thin shim over
the real product_catalog, not the 27-synthetic-SKU toy that shipped
poisoned pricing into bid scoring, RFQ pricing ingest, and hit-rate
analytics.
"""
import re
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "src" / "core" / "catalog.py"


def test_module_compiles():
    import py_compile
    py_compile.compile(str(CATALOG), doraise=True)


def test_no_synthetic_p0_skus_seed():
    """CP-5: the 27-row P0_SKUS literal must be gone."""
    src = CATALOG.read_text(encoding="utf-8")
    assert "P0_SKUS = [" not in src, \
        "CP-5: the P0_SKUS synthetic seed list must be removed"
    assert "NIT-EXAM-MD" not in src, \
        "CP-5: synthetic SKU literals must be gone"
    assert "CHUX-23X36" not in src, \
        "CP-5: synthetic SKU literals must be gone"


def test_search_delegates_to_product_catalog():
    """CP-5: search_catalog must delegate to product_catalog.search_products."""
    src = CATALOG.read_text(encoding="utf-8")
    assert "from src.agents.product_catalog import search_products" in src, \
        "CP-5: search_catalog must import search_products"
    # The function must call it, not query a local `products` table.
    m = re.search(r"def search_catalog\(.*?\n(.*?)\ndef ", src, re.DOTALL)
    assert m, "search_catalog def not found"
    body = m.group(1)
    assert "search_products(" in body, \
        "CP-5: search_catalog body must call search_products"


def test_no_local_products_table_select():
    """CP-5: the shim must not SELECT from its own `products` table."""
    src = CATALOG.read_text(encoding="utf-8")
    # Reads of the toy `products` table are the bug — allow references
    # to `product_catalog` (the real one).
    offending = []
    for line in src.splitlines():
        low = line.lower()
        if "select" in low and re.search(r"\bfrom\s+products\b", low):
            offending.append(line.strip())
    assert not offending, (
        f"CP-5: must not SELECT FROM local `products` table: {offending}"
    )


def test_init_catalog_is_noop_seedwise():
    """CP-5: init_catalog must not insert synthetic P0 SKUs."""
    src = CATALOG.read_text(encoding="utf-8")
    # init_catalog body should not contain INSERT INTO products.
    m = re.search(r"def init_catalog\(\).*?(?=\ndef )", src, re.DOTALL)
    assert m, "init_catalog not found"
    body = m.group(0)
    assert "INSERT INTO products" not in body and "INSERT OR IGNORE INTO products" not in body, \
        "CP-5: init_catalog must not seed the toy products table"


def test_search_catalog_returns_legacy_shape():
    """CP-5: the shim must preserve the legacy return shape so callers
    (bid_decision_agent, routes_rfq, dashboard hit-rate) keep working."""
    from src.core.catalog import search_catalog
    # Run a search — results may be empty in test DB, but the call must
    # not raise and must return a list.
    results = search_catalog("gloves", limit=3)
    assert isinstance(results, list)
    for r in results:
        # Legacy keys callers depend on
        for k in ("sku", "name", "typical_cost", "list_price", "category",
                 "manufacturer", "part_number", "tags"):
            assert k in r, f"missing legacy key {k} in {r}"


def test_get_catalog_stats_reports_zero_p0_skus():
    """CP-5: p0_skus_loaded must be 0 (the synthetic seed is retired)."""
    from src.core.catalog import get_catalog_stats
    stats = get_catalog_stats()
    assert stats.get("p0_skus_loaded") == 0, \
        "CP-5: p0_skus_loaded must be 0 (synthetic SKUs retired)"
