"""CP-2 regression guard: product_catalog.scprs_last_price must be stored
per-unit, not as a SCPRS line total.

Why this matters:
  * `scprs_po_lines.unit_price` is a LINE TOTAL for multi-qty POs — a PO
    of 5 boxes at $20 each shows `unit_price=$100`, `quantity=5`.
  * `product_catalog.scprs_last_price` has NO `quantity` column, so once
    stored it can't be re-normalized at read time. 5+ downstream catalog
    readers (recommendation strategies, pricing opportunities, auto-
    recalculate) treat it as per-unit.
  * If the writer stores the raw line total, `scprs_guided` strategy
    recommends prices 5x the real SCPRS ceiling — we lose winnable bids
    by pricing absurdly high.

The fix: `_record_competitor_prices` in award_tracker.py must normalize
via `_scprs_per_unit(unit_price, quantity)` before writing to
`scprs_last_price`, `competitor_low_price`, `price_history.unit_price`,
and `catalog_price_history.price`. This gives readers a single
contract — product_catalog SCPRS fields are always per-unit — without
forcing every reader to look up qty context that's been discarded.

Prior fix references: IN-2 / IN-11 fixed the same per-unit drift in
pricing_oracle_v2 search paths (PRs in 2026-04-21).
"""
from __future__ import annotations

import re
from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_AWARD = _REPO / "src" / "agents" / "award_tracker.py"
_ORACLE = _REPO / "src" / "core" / "pricing_oracle_v2.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    out = []
    for line in src.splitlines():
        s = line.lstrip()
        if s.startswith("#"):
            continue
        if " # " in line:
            line = line.split(" # ", 1)[0]
        out.append(line)
    return "\n".join(out)


def _record_competitor_prices_body() -> str:
    body = _read(_AWARD)
    start = body.find("def _record_competitor_prices(")
    assert start >= 0, "_record_competitor_prices not found in award_tracker.py"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


# ── Writer normalizes before storing ───────────────────────────────────────

def test_record_competitor_prices_imports_scprs_per_unit():
    fn = _record_competitor_prices_body()
    assert "_scprs_per_unit" in fn, (
        "CP-2 regression: _record_competitor_prices no longer imports "
        "`_scprs_per_unit`. SCPRS po_lines.unit_price stores line totals — "
        "the writer must normalize before storing to product_catalog."
    )


def test_record_competitor_prices_defines_per_unit_local():
    fn = _record_competitor_prices_body()
    assert re.search(r"per_unit\s*=\s*_scprs_per_unit\(\s*unit_price\s*,\s*quantity\s*\)", fn), (
        "CP-2 regression: missing `per_unit = _scprs_per_unit(unit_price, quantity)`. "
        "Both catalog UPDATE branches and price_history INSERT must use this "
        "normalized value, not the raw line total."
    )


def test_catalog_updates_use_per_unit_not_raw_unit_price():
    """Both UPDATE branches must bind `per_unit`, not `unit_price`.

    There are exactly two UPDATE product_catalog statements — the
    cheaper-than-existing branch (sets competitor_low_price + scprs fields)
    and the not-cheaper branch (scprs + times_lost only). Both are
    vulnerable to the line-total bug.
    """
    fn = _strip_comments(_record_competitor_prices_body())
    # Find every UPDATE product_catalog bind tuple and check it doesn't bind
    # `unit_price` positionally for the competitor_low_price / scprs_last_price
    # columns. Since we now use `per_unit`, the raw name must not reappear
    # in those bind sites.
    # Cheaper branch: 8 binds, all reference prices via per_unit
    assert re.search(
        r"""\(\s*per_unit\s*,\s*f"\{supplier\} via SCPRS PO \{po_number\}"\s*,\s*now\s*,\s*\n\s*per_unit\s*,\s*now\s*,\s*agency\s*,\s*now\s*,\s*cat_match\["id"\]\s*\)""",
        fn,
    ), (
        "CP-2 regression: cheaper-than-existing UPDATE branch no longer "
        "binds `per_unit` for competitor_low_price + scprs_last_price. "
        "If it binds raw `unit_price`, multi-qty SCPRS awards will store "
        "line totals in catalog."
    )
    # Not-cheaper branch: per_unit for scprs_last_price
    assert re.search(
        r"""\(\s*per_unit\s*,\s*now\s*,\s*agency\s*,\s*now\s*,\s*cat_match\["id"\]\s*\)""",
        fn,
    ), (
        "CP-2 regression: not-cheaper UPDATE branch no longer binds "
        "`per_unit` for scprs_last_price."
    )


def test_price_history_insert_uses_per_unit():
    fn = _record_competitor_prices_body()
    # price_history INSERT bind tuple, per_unit must be the unit_price slot
    assert re.search(
        r"""\(now,\s*desc,\s*item_id,\s*supplier,\s*quantity,\s*per_unit,""",
        fn,
    ), (
        "CP-2 regression: price_history INSERT binds raw `unit_price` "
        "instead of `per_unit`. Downstream pricing_oracle reads "
        "price_history expecting per-unit values."
    )


def test_catalog_price_history_insert_uses_per_unit():
    fn = _record_competitor_prices_body()
    assert re.search(
        r"""\(cat_match\["id"\],\s*"competitor_scprs",\s*per_unit,\s*quantity,""",
        fn,
    ), (
        "CP-2 regression: catalog_price_history snapshot still binds raw "
        "line-total `unit_price` for the price column."
    )


def test_existing_competitor_comparison_uses_per_unit():
    """The `if unit_price < existing_competitor` branch selector must
    compare PER-UNIT to PER-UNIT. Otherwise a 5-qty SCPRS line total
    ($100) always looks 'more expensive' than the stored per-unit
    competitor ($20), skewing which branch runs."""
    fn = _strip_comments(_record_competitor_prices_body())
    assert re.search(r"if\s+per_unit\s*<\s*existing_competitor", fn), (
        "CP-2 regression: branch comparison still uses raw `unit_price`. "
        "Apples-to-oranges: stored values are per-unit, `unit_price` is "
        "a line total for multi-qty POs."
    )


# ── Sanity: the shared helper contract we depend on still lives where we expect

def test_scprs_per_unit_helper_signature_stable():
    body = _read(_ORACLE)
    assert re.search(r"def\s+_scprs_per_unit\s*\(\s*price\s*,\s*qty\s*\)", body), (
        "CP-2 regression: pricing_oracle_v2._scprs_per_unit signature "
        "changed. award_tracker imports this as the canonical SCPRS "
        "line-total → per-unit normalizer. Keep the (price, qty) shape."
    )


# ── Functional: helper correctness on the actual shapes seen in po_lines

def test_scprs_per_unit_line_total_is_divided():
    """A 5-qty PO at $20/ea ships as unit_price=$100. Helper must return
    $20, not $100, otherwise the catalog stores a 5x-inflated ceiling."""
    from src.core.pricing_oracle_v2 import _scprs_per_unit
    assert _scprs_per_unit(100.0, 5) == 20.0


def test_scprs_per_unit_already_per_unit_is_preserved():
    """A qty=3 PO where unit_price IS already per-unit ($5) must not be
    divided again. The `p > qty * 2` guard preserves already-normalized
    small prices."""
    from src.core.pricing_oracle_v2 import _scprs_per_unit
    assert _scprs_per_unit(5.0, 3) == 5.0


def test_scprs_per_unit_qty_one_is_passthrough():
    """qty=1 is the common case for single-unit POs — must never divide."""
    from src.core.pricing_oracle_v2 import _scprs_per_unit
    assert _scprs_per_unit(250.0, 1) == 250.0
