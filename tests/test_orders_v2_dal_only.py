"""PR F / O-10 regression: `routes_orders_full.py` must not call the
legacy `_load_orders()` helper — every read must go through the V2 DAL
(`load_orders_dict` for iteration patterns, `get_order` for single-row
lookups).

Audit source: project_orders_module_audit_2026_04_21 O-10. The legacy
`_load_orders` wrapper in dashboard.py still exists (other modules call
it), but the orders module itself — the module that defines the
authoritative Order routes — must be fully on V2 so we stop serving
queue + KPI + detail views from different code paths.
"""
from __future__ import annotations

import re


ROUTES_FILE = "src/api/modules/routes_orders_full.py"


def test_no_legacy_load_orders_in_routes_orders_full():
    """Grep-invariant: routes_orders_full.py must have zero `_load_orders()`
    call sites. If this test fails after a refactor, route the new call
    through `load_orders_dict()` or `get_order()` instead."""
    src = open(ROUTES_FILE, encoding="utf-8").read()
    matches = re.findall(r"\b_load_orders\s*\(", src)
    assert not matches, (
        f"O-10 regression: {len(matches)} `_load_orders(` call(s) in "
        f"{ROUTES_FILE}. Replace with load_orders_dict() (iteration) "
        f"or get_order(oid) (single lookup) from src.core.order_dal.")


def test_no_legacy_save_orders_in_routes_orders_full():
    """Grep-invariant: routes_orders_full.py must have zero `_save_orders()`
    call sites. Bulk file-write is a concurrency hazard (see O-11); every
    writer must go through save_order / save_line_item / save_line_items_batch.

    Docstrings/comments that mention the old helper are exempt (we scan
    non-comment, non-docstring lines only — the simplest proxy is to drop
    lines whose first non-whitespace char is `#` or that sit inside a
    triple-quoted block). For this file we approximate by excluding
    matches preceded by a backtick (markdown code span in a docstring)."""
    src = open(ROUTES_FILE, encoding="utf-8").read()
    # Only flag real call sites — skip ` `_save_orders(` ` style mentions
    # inside docstrings (markdown code spans).
    matches = [m for m in re.finditer(r"\b_save_orders\s*\(", src)
               if m.start() == 0 or src[m.start() - 1] != "`"]
    assert not matches, (
        f"O-10/O-11 regression: {len(matches)} `_save_orders(` call(s) in "
        f"{ROUTES_FILE}. Use the DAL write primitives instead.")


def test_v2_dal_primitives_are_referenced():
    """Sanity: ensure the V2 DAL imports we installed are actually in the
    file — catches a merge that reverts only the import line but leaves
    the call site bare."""
    src = open(ROUTES_FILE, encoding="utf-8").read()
    assert "load_orders_dict" in src, (
        "routes_orders_full.py no longer imports load_orders_dict — O-10 "
        "refactor reverted?")
    assert "get_order" in src, (
        "routes_orders_full.py no longer imports get_order — O-10 "
        "refactor reverted?")
