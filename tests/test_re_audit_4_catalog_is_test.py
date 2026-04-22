"""RE-AUDIT-4 regression guard.

`src/agents/product_catalog.py` — the real catalog engine behind the
legacy `src/core/catalog.py` shim — had no `is_test` column. Rows
inserted by pytest fixtures or staging seeds mixed freely with prod
rows in search/recommendation rankings. A test row called
"TEST nitrile gloves $9.99" could out-rank the real $24.50 glove
entry on popularity sort and on the auto-match engine's token
strategy.

Fix:
- `CATALOG_SCHEMA` declares `is_test INTEGER DEFAULT 0`.
- `init_catalog_db()` migration block ADDs the column to existing DBs.
- Search/ranking/auto-match paths filter `COALESCE(is_test,0)=0`:
  `smart_search` (exact SKU/MFG/UPC short-circuits + popularity path +
  token short-circuit), `_smart_rank` (token fan-out), and
  `match_item` strategies 0/1/2/3/4.

Direct ID/name lookups (`get_product`, `get_product_by_name`) are
intentionally left untouched — those are precise queries, not
rankings, and callers that ask for a specific test row need to
receive it.
"""
from __future__ import annotations

import re
from pathlib import Path


CATALOG = (
    Path(__file__).resolve().parents[1]
    / "src" / "agents" / "product_catalog.py"
)


def _source() -> str:
    return CATALOG.read_text(encoding="utf-8")


def test_schema_declares_is_test_column():
    """CATALOG_SCHEMA must include is_test INTEGER DEFAULT 0."""
    src = _source()
    # Look inside CATALOG_SCHEMA string. Match even if formatted across lines.
    m = re.search(
        r'CATALOG_SCHEMA\s*=\s*"""([\s\S]*?)"""',
        src,
    )
    assert m, "CATALOG_SCHEMA block not found"
    schema = m.group(1)
    assert re.search(r"is_test\s+INTEGER\s+DEFAULT\s+0", schema), (
        "RE-AUDIT-4 regression: product_catalog schema is missing "
        "`is_test INTEGER DEFAULT 0`. Test rows will pollute search "
        "and recommendation rankings for real quotes."
    )


def test_migration_adds_is_test_column():
    """init_catalog_db must ALTER TABLE ADD the is_test column on old DBs."""
    src = _source()
    # Locate the migration list (tuples passed into a for loop).
    assert re.search(
        r'\(\s*"is_test"\s*,\s*"INTEGER\s+DEFAULT\s+0"\s*\)',
        src,
    ), (
        "RE-AUDIT-4 regression: init_catalog_db migration list is "
        "missing ('is_test', 'INTEGER DEFAULT 0'). Fresh DBs get the "
        "column via CATALOG_SCHEMA, but existing prod DB needs an "
        "ALTER TABLE from the migration block."
    )


def test_smart_search_popularity_filters_is_test():
    """The popularity-sort branch in smart_search must exclude is_test rows."""
    src = _source()
    # Walk the smart_search body. The popularity branch is the `if not tokens:` block.
    m = re.search(
        r"def smart_search\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "smart_search function body not located"
    body = m.group(0)
    # The popularity branch seeds `where` with a non-empty sentinel.
    pop = re.search(r"if not tokens:[\s\S]*?fetchall\(\)", body)
    assert pop, "popularity fallback branch not found inside smart_search"
    assert "is_test" in pop.group(0), (
        "RE-AUDIT-4 regression: smart_search popularity fallback does "
        "not filter is_test. An empty/garbage query surfaces test rows "
        "in the 'top of catalog' view."
    )


def test_smart_rank_filters_is_test():
    """The candidate fan-out in _smart_rank must exclude is_test rows."""
    src = _source()
    m = re.search(
        r"def _smart_rank\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "_smart_rank function body not located"
    body = m.group(0)
    # The query uses `where.append(...)` to accumulate filters. is_test must be
    # in there somewhere.
    assert "is_test" in body, (
        "RE-AUDIT-4 regression: _smart_rank does not filter is_test. "
        "Test rows will show up in the token-scored ranked list."
    )


def test_match_item_filters_is_test():
    """The auto-match engine must filter is_test across its strategies."""
    src = _source()
    m = re.search(
        r"def match_item\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "match_item function body not located"
    body = m.group(0)
    # Count is_test occurrences — at least 4 (strategies 0, 1, 2, 3, 4 — we've
    # covered 5 sites but let's gate at >=4 to allow for minor refactors).
    hits = body.count("is_test")
    assert hits >= 4, (
        "RE-AUDIT-4 regression: match_item has only "
        f"{hits} is_test filter(s). Expected ≥4 across the UPC / "
        "exact-part# / extracted-part# / token / description "
        "strategies. Test rows will bleed into auto-match results."
    )


def test_is_test_filter_works_on_real_db():
    """End-to-end: write two rows (one test, one prod), ensure search returns
    only the prod row."""
    import os
    import tempfile
    import sqlite3

    # Point the module's DB_PATH at a temp file for this test only.
    import src.agents.product_catalog as pc

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    original = pc.DB_PATH
    try:
        pc.DB_PATH = tmp.name
        pc.init_catalog_db()

        now = "2026-04-22T00:00:00"
        conn = sqlite3.connect(tmp.name)
        conn.execute(
            "INSERT INTO product_catalog "
            "(name, sku, description, category, is_test, times_quoted, "
            " sell_price, search_tokens, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 0, 99, 24.50, ?, ?, ?)",
            ("Nitrile Gloves Large", "GLV-NIT-L", "nitrile exam gloves large",
             "gloves", "gloves nitrile exam large", now, now),
        )
        conn.execute(
            "INSERT INTO product_catalog "
            "(name, sku, description, category, is_test, times_quoted, "
            " sell_price, search_tokens, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 1, 999, 9.99, ?, ?, ?)",
            ("TEST Nitrile Gloves", "TEST-GLV-NIT", "test nitrile exam gloves",
             "gloves", "test nitrile exam gloves large", now, now),
        )
        conn.commit()
        conn.close()

        results = pc.smart_search("nitrile gloves", limit=10)
        names = [r["name"] for r in results]
        assert "Nitrile Gloves Large" in names, (
            "RE-AUDIT-4 regression: smart_search dropped the real "
            "'Nitrile Gloves Large' prod row entirely. Filter is too "
            f"aggressive. Got: {names!r}"
        )
        assert "TEST Nitrile Gloves" not in names, (
            "RE-AUDIT-4 regression: smart_search returned the "
            "is_test=1 'TEST Nitrile Gloves' row. The filter did not "
            f"take effect. Got: {names!r}"
        )
    finally:
        pc.DB_PATH = original
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
