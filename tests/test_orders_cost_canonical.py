"""O-15 regression: unit_cost is canonical on order line items.

Background — `unit_cost` is the authoritative column in
`order_line_items` (the V2 normalized table) and also what
`save_line_items_batch` reads first on the DELETE/INSERT round-trip.
A legacy dict key `cost` existed in the in-memory shape and in several
older code paths. Prior to this guard:

  - Supplier research wrote only `cost`, not `unit_cost` → next
    persist saw the field as blank and the newly-researched price
    evaporated on the next save.
  - Quote-enrichment backfill wrote only `cost` → same shadow.
  - Several read paths (margin endpoint, supplier grouping, totals
    aggregator, supplier-panel helper) read only `cost` → stale or
    zero cost even when `unit_cost` held the real value.

Rule going forward: every write to line-item cost populates BOTH
`unit_cost` (canonical) and `cost` (legacy alias). Every read prefers
`unit_cost` and falls back to `cost`. The invariant below scans the
module source to lock this pattern in place so we can't silently
regress it with a one-off edit.
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
ROUTES = REPO / "src" / "api" / "modules" / "routes_orders_full.py"


def _src() -> str:
    return ROUTES.read_text(encoding="utf-8")


def test_read_paths_prefer_unit_cost():
    """Every line-item cost read must consult `unit_cost` first.

    Scope: reads on variables that hold a line-item dict. In this module
    those are `it`, `oi`, `target`, `line_item`. The inbound request body
    `data.get("cost", ...)` on the cost-update endpoint is a wire-contract
    field (not a line-item read) and is deliberately excluded.
    """
    src = _src()
    line_item_vars = ("it", "oi", "target", "line_item")
    pattern = re.compile(
        r"\b(" + "|".join(line_item_vars) + r")\.get\(\s*['\"]cost['\"]"
    )
    bare_reads: list[str] = []
    for m in pattern.finditer(src):
        start = max(0, m.start() - 80)
        end = min(len(src), m.end() + 40)
        window = src[start:end]
        if "unit_cost" in window:
            continue
        line_no = src.count("\n", 0, m.start()) + 1
        bare_reads.append(f"line {line_no}: {src[start:end].strip()}")

    assert not bare_reads, (
        "O-15: every line-item `.get(\"cost\")` read must consult "
        "`unit_cost` first. Bare reads found:\n  " + "\n  ".join(bare_reads)
    )


def test_write_paths_populate_unit_cost():
    """Every `<var>["cost"] = …` write must be paired with a canonical
    `unit_cost` write within 3 lines — either an explicit
    `<var>["unit_cost"] = …` or a generic `<var>[field] = …` inside a
    loop whose field list includes `"unit_cost"` (the shared-field-loop
    pattern at the line-save site).
    """
    src = _src()
    lines = src.splitlines()
    orphans: list[str] = []
    for i, line in enumerate(lines):
        m = re.search(r'(\w+)\[["\']cost["\']\]\s*=', line)
        if not m:
            continue
        var = m.group(1)
        window = "\n".join(lines[max(0, i - 3): i + 4])
        if re.search(rf'{re.escape(var)}\[["\']unit_cost["\']\]\s*=', window):
            continue
        # Tolerate the generic field-loop write (`it[field] = data[field]`)
        # when the surrounding loop's field list contains "unit_cost".
        if re.search(rf'{re.escape(var)}\[field\]\s*=', window):
            loop_ctx = "\n".join(lines[max(0, i - 30): i + 4])
            if '"unit_cost"' in loop_ctx or "'unit_cost'" in loop_ctx:
                continue
        orphans.append(f"line {i + 1}: {line.strip()}")

    assert not orphans, (
        "O-15: every `<var>[\"cost\"] = …` write must be paired with a "
        "`<var>[\"unit_cost\"] = …` write (canonical field). Orphans:\n  "
        + "\n  ".join(orphans)
    )
