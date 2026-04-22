"""CP-3 regression guards: catalog margin/top-quoted query real schema.

The old code queried a phantom `products` table with `cost_price` /
`last_quoted` columns that have never existed in this schema (the real
table is `product_catalog` with `cost` / `last_sold_date`). The queries
silently returned empty because `if not os.path.exists(catalog.db)`
short-circuited to {} before the SQL even ran — Catalog Intelligence
tabs shipped blind.
"""
import re
from pathlib import Path

ROUTES = Path(__file__).resolve().parents[1] / "src" / "api" / "modules" / "routes_catalog_finance.py"


def _body_of(fn_name: str) -> str:
    """Return the text between def <fn_name> and the next def/@bp.route."""
    src = ROUTES.read_text(encoding="utf-8")
    # Find start
    start = src.find(f"def {fn_name}(")
    assert start >= 0, f"{fn_name} not defined"
    # Find next def or @bp.route at col 0
    rest = src[start:]
    next_boundary = re.search(r"\n(?:def |@bp\.route)", rest[len(f"def {fn_name}("):])
    end = len(rest) if not next_boundary else len(f"def {fn_name}(") + next_boundary.start()
    return rest[:end]


def test_module_compiles():
    import py_compile
    py_compile.compile(str(ROUTES), doraise=True)


def test_margin_analysis_queries_real_table():
    body = _body_of("api_catalog_margin_analysis")
    assert "FROM product_catalog" in body, "margin analysis must query product_catalog"
    # Assert the phantom SELECT ... FROM products is gone
    assert not re.search(r"FROM\s+products\b", body, re.IGNORECASE), \
        "margin analysis must not query phantom products table"


def test_top_quoted_queries_real_table():
    body = _body_of("api_catalog_top_quoted")
    assert "FROM product_catalog" in body
    assert not re.search(r"FROM\s+products\b", body, re.IGNORECASE)


def test_no_cost_price_column_references():
    """The real column is `cost`. `cost_price` was a phantom that never existed.
    Check the SELECT list only — the explanatory docstring legitimately
    mentions `cost_price` as the bug that was fixed."""
    for fn in ("api_catalog_margin_analysis", "api_catalog_top_quoted"):
        body = _body_of(fn)
        sel = re.search(r"SELECT(.*?)FROM", body, re.DOTALL | re.IGNORECASE)
        assert sel, f"{fn}: no SELECT found"
        assert "cost_price" not in sel.group(1), \
            f"{fn} SELECT must use `cost`, not phantom `cost_price`"


def test_top_quoted_selects_last_sold_date():
    """Real schema has `last_sold_date`, not `last_quoted`. JSON key can
    still be `last_quoted` for UI compat."""
    body = _body_of("api_catalog_top_quoted")
    # SELECT list must ask for last_sold_date
    sel_match = re.search(r"SELECT(.*?)FROM", body, re.DOTALL | re.IGNORECASE)
    assert sel_match, "no SELECT found"
    select_list = sel_match.group(1)
    assert "last_sold_date" in select_list, "must SELECT last_sold_date"
    assert "last_quoted" not in select_list, "must not SELECT phantom last_quoted column"


def test_no_stale_catalog_db_existence_check():
    """The two endpoints previously guarded on `catalog.db` existing and
    returned empty before even opening the real DB."""
    body_a = _body_of("api_catalog_margin_analysis")
    body_b = _body_of("api_catalog_top_quoted")
    assert 'catalog.db' not in body_a, "margin analysis must not check catalog.db"
    assert 'catalog.db' not in body_b, "top_quoted must not check catalog.db"
