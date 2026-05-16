"""Bulk shadow-mode diff: run every legacy quote through the Spine.

Reviewer's 2026-05-15 request — the Day-14 shadow window needs a
nightly summary report, not one-off invocations. This script
iterates a corpus of legacy quote JSON files (or a SQLite dump of
priced_carts rows) and emits a one-page summary:

    SPINE BULK SHADOW — 2026-05-16 06:00
    ====================================================
    Corpus      : 47 quotes (CCHCS only)
    CLEAN       : 41    87.2%   ← totals match within 1¢
    1c rounding :  2     4.3%   ← within tolerance but >0 delta
    DIVERGE     :  3     6.4%   ← totals differ by > 1¢
    FAILED      :  1     2.1%   ← legacy data unrepresentable in Spine

    Worst divergences:
      rfq_aaaaaaaa  legacy $12,345.67  spine $11,000.00  Δ $1,345.67
      rfq_bbbbbbbb  legacy  $5,000.00  spine  $4,500.00  Δ   $500.00
      rfq_cccccccc  legacy  $9,876.54  spine  $9,800.00  Δ    $76.54

    Translation failures:
      rfq_dddddddd: line_items[2]: cost_cents <= 0

    Run this nightly during the 14-day shadow window. When you see 5
    consecutive bulk runs at 100% CLEAN, the Spine is ready for an
    operator-side trial run.

Usage:
    # From a directory of legacy *.json files:
    py scripts/spine_bulk_shadow.py --legacy-dir _diag/legacy_dump/

    # From a SQLite DB extract of priced_carts (TODO — needs --sqlite
    # flag; not built yet):
    # py scripts/spine_bulk_shadow.py --sqlite data/quotes.db --recent-days 30

    # Emit machine-readable summary as well:
    py scripts/spine_bulk_shadow.py --legacy-dir _diag/legacy_dump/ \\
        --json-out _diag/bulk_shadow_2026_05_16.json

Exit codes:
    0 = every quote clean
    1 = at least one translation failure
    2 = at least one totals divergence > 1¢
    (1 and 2 are exclusive; on both, returns 2 — the more actionable.)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

# Force UTF-8 on stdout so Windows cp1252 console can render the
# delta/le-equal glyphs we use in the report.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spine import format_dollars  # noqa: E402
from src.spine_bridge import translate_legacy_quote  # noqa: E402

# Reuse the diff helpers from spine_shadow_diff for legacy total math
# so we don't drift across the two scripts.
sys.path.insert(0, str(ROOT / "scripts"))
from spine_shadow_diff import _legacy_totals  # noqa: E402


_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def _classify(legacy: dict) -> dict:
    """Translate + diff one legacy dict; return a small result row."""
    quote_id = (
        legacy.get("id")
        or legacy.get("rfq_id")
        or legacy.get("pc_id")
        or "<unknown>"
    )
    row: dict = {
        "quote_id": quote_id,
        "agency": legacy.get("institution") or legacy.get("agency") or "?",
        "kind": "UNKNOWN",
        "legacy_total_cents": 0,
        "spine_total_cents": 0,
        "delta_cents": 0,
        "error": None,
    }
    result = translate_legacy_quote(legacy)
    if not result.ok:
        row["kind"] = "FAILED"
        first_error = next(iter(result.errors()), None)
        row["error"] = (
            f"{first_error.field_path}: {first_error.detail}"
            if first_error else "unknown translation error"
        )
        return row

    legacy_totals = _legacy_totals(legacy)
    row["legacy_total_cents"] = legacy_totals["total_cents"]
    row["spine_total_cents"] = result.quote.total_cents
    delta = result.quote.total_cents - legacy_totals["total_cents"]
    row["delta_cents"] = delta
    abs_delta = abs(delta)
    if abs_delta == 0:
        row["kind"] = "CLEAN"
    elif abs_delta <= 1:
        row["kind"] = "1C_ROUND"
    else:
        row["kind"] = "DIVERGE"
    return row


def _iter_legacy_files(legacy_dir: Path):
    """Yield (path, dict) for every *.json under legacy_dir."""
    for p in sorted(legacy_dir.rglob("*.json")):
        try:
            yield p, json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  SKIP {p.name}: {e}", file=sys.stderr)
            continue


def render_bulk_report(rows: list[dict], *, color: bool = True) -> tuple[str, int]:
    """Render the bulk summary. Returns (report_text, exit_code)."""
    if not color:
        global _GREEN, _RED, _YELLOW, _RESET
        _GREEN = _RED = _YELLOW = _RESET = ""

    lines: list[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"SPINE BULK SHADOW — {now}")
    lines.append("=" * 70)

    counts = Counter(r["kind"] for r in rows)
    total = len(rows)
    if total == 0:
        lines.append("(empty corpus — no quotes to compare)")
        return "\n".join(lines), 0

    def pct(n: int) -> str:
        return f"{n*100/total:5.1f}%"

    clean = counts.get("CLEAN", 0)
    rounding = counts.get("1C_ROUND", 0)
    diverge = counts.get("DIVERGE", 0)
    failed = counts.get("FAILED", 0)

    # Agency distribution for context.
    agencies = sorted({r["agency"] for r in rows})
    agency_label = ", ".join(agencies)[:60]

    lines.append(f"  Corpus      : {total} quotes ({agency_label})")
    lines.append(f"  {_GREEN}CLEAN{_RESET}       : {clean:4d}  {pct(clean)}  totals match exactly")
    lines.append(f"  {_YELLOW}1c rounding{_RESET} : {rounding:4d}  {pct(rounding)}  delta ≤ $0.01 (within tolerance)")
    lines.append(f"  {_RED}DIVERGE{_RESET}     : {diverge:4d}  {pct(diverge)}  delta > $0.01")
    lines.append(f"  {_RED}FAILED{_RESET}      : {failed:4d}  {pct(failed)}  translation refused")

    # Trial-run readiness signal: only when 100% clean (or clean + 1c).
    ready = (failed == 0 and diverge == 0)
    lines.append("")
    if ready and clean == total:
        lines.append(
            f"{_GREEN}✓ 100% CLEAN — ready for operator trial run.{_RESET}"
        )
    elif ready:
        lines.append(
            f"{_YELLOW}≈ Within tolerance ({rounding} rounding-only) — "
            f"acceptable for trial.{_RESET}"
        )
    else:
        lines.append(
            f"{_RED}Not ready: {diverge} divergence(s), "
            f"{failed} failure(s). Investigate before promoting.{_RESET}"
        )

    # Top divergences (worst first).
    diverges = sorted(
        (r for r in rows if r["kind"] == "DIVERGE"),
        key=lambda r: abs(r["delta_cents"]),
        reverse=True,
    )
    if diverges:
        lines.append("")
        lines.append("  Worst divergences:")
        for r in diverges[:10]:
            sign = "+" if r["delta_cents"] > 0 else "-"
            lines.append(
                f"    {r['quote_id']:<28}  "
                f"legacy {format_dollars(r['legacy_total_cents']):>14}  "
                f"spine {format_dollars(r['spine_total_cents']):>14}  "
                f"Δ {sign}{format_dollars(abs(r['delta_cents'])):>10}"
            )

    failures = [r for r in rows if r["kind"] == "FAILED"]
    if failures:
        lines.append("")
        lines.append("  Translation failures:")
        for r in failures[:10]:
            lines.append(f"    {r['quote_id']}: {r['error']}")

    # Exit code: prefer the more actionable signal.
    if diverge > 0:
        exit_code = 2
    elif failed > 0:
        exit_code = 1
    else:
        exit_code = 0
    return "\n".join(lines), exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bulk shadow-mode diff over a corpus of legacy quotes.",
    )
    parser.add_argument(
        "--legacy-dir", required=True,
        help="Directory of legacy quote *.json files (walked recursively).",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path: also write the per-quote rows as JSON for "
             "downstream automation (dashboards, alerting).",
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI color in the output.",
    )
    args = parser.parse_args()

    legacy_dir = Path(args.legacy_dir)
    if not legacy_dir.is_dir():
        print(f"ERROR: --legacy-dir {legacy_dir!s} is not a directory.",
              file=sys.stderr)
        return 1

    rows = [_classify(d) for _, d in _iter_legacy_files(legacy_dir)]

    report, exit_code = render_bulk_report(rows, color=not args.no_color)
    print(report)

    if args.json_out:
        out = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "corpus_size": len(rows),
            "summary": dict(Counter(r["kind"] for r in rows)),
            "rows": rows,
        }
        Path(args.json_out).write_text(
            json.dumps(out, indent=2), encoding="utf-8",
        )
        print(f"\nWrote per-quote summary to: {args.json_out}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
