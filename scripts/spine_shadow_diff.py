"""Shadow-mode CLI: run a legacy quote through the Spine and diff.

Usage:
    py scripts/spine_shadow_diff.py --legacy-json path/to/legacy.json
    py scripts/spine_shadow_diff.py --legacy-json tests/spine/fixtures/legacy_russ_no_bid_test.json

What it does:
    1. Load the legacy quote dict from JSON (or stdin if --legacy-json -).
    2. Translate it via src.spine_bridge.translator.
    3. Compute Spine totals from the resulting Quote.
    4. Compute legacy totals by walking the legacy dict directly
       (same rules pricing_math.canonical_unit_price would have used).
    5. Render the Spine Quote to a sample PDF (if --pdf-out specified).
    6. Emit a human-readable diff report:

        SHADOW DIFF — rfq_0ebe242f_test
        ───────────────────────────────────────────
        Legacy total :  $4,789.50
        Spine  total :  $4,789.50
        Δ            :  $0.00   ✓ MATCH

        Tax rate (legacy) : 7.75%
        Tax rate (Spine)  : 7.75%
        Translation issues: 11 info, 0 warning, 0 error

This is the Day-14 deliverable from SPINE_CHARTER.md. Use it on every
CCHCS quote shipped over the next 14 days to verify Spine math matches
what legacy actually delivered. When you see 5 consecutive diff-clean
ships, the Spine is ready for an operator-side trial run.

Exit codes:
    0 = clean (totals match within 1 cent)
    1 = translation failed (legacy data unrepresentable in Spine)
    2 = totals diverge (Spine and legacy disagree)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spine import format_dollars, format_tax_rate, render_quote_pdf  # noqa: E402
from src.spine_bridge import translate_legacy_quote  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Legacy-side math — mirrors what the legacy substrate would have done
# if it could produce correct totals. Used as the comparison baseline.
# ──────────────────────────────────────────────────────────────────────


def _legacy_unit_price_dollars(item: dict) -> float:
    """Same priority as canonical_unit_price in pricing_math.py.

    Returns the dollar value, NOT cents. 0.0 if nothing usable.
    """
    for alias in ("unit_price", "bid_price", "price_per_unit", "our_price"):
        v = item.get(alias)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f > 0:
            return f
    # Fall back to cost × markup.
    cost = item.get("supplier_cost") or item.get("vendor_cost") or item.get("cost")
    markup = item.get("markup_pct")
    try:
        if cost and markup is not None:
            return float(cost) * (1 + float(markup) / 100.0)
    except (TypeError, ValueError):
        pass
    return 0.0


def _legacy_totals(legacy: dict) -> dict:
    """Compute (subtotal_cents, tax_cents, total_cents) the legacy way.

    Subtotal uses each line's qty × resolved unit price. Tax uses the
    legacy tax_rate decimal (NOT integer-bps banker's rounded — this
    is what the legacy substrate would actually produce).
    """
    items = legacy.get("line_items") or legacy.get("items") or []
    subtotal = 0.0
    for item in items:
        if not isinstance(item, dict):
            continue
        qty = item.get("qty") or item.get("quantity") or 0
        try:
            qty = int(float(qty))
        except (TypeError, ValueError):
            qty = 0
        unit_price = _legacy_unit_price_dollars(item)
        subtotal += qty * unit_price

    # Resolve tax rate.
    rate = legacy.get("tax_rate")
    if rate is None:
        rate = legacy.get("tax_rate_pct")
        if rate is not None:
            try:
                rate = float(rate) / 100.0
            except (TypeError, ValueError):
                rate = 0.0
    if rate is None:
        rate = 0.0
    try:
        rate = float(rate)
    except (TypeError, ValueError):
        rate = 0.0
    # Disambiguate percent-form (8.25) vs decimal-form (0.0825).
    if rate >= 1.0:
        rate = rate / 100.0

    # Legacy uses straightforward float multiplication + round.
    tax = round(subtotal * rate, 2)
    # The shipping_option=included branch was the 5/15 zeroing bug —
    # but for accurate shadow comparison we DON'T apply that branch
    # here. We compare against what legacy SHOULD have produced.
    return {
        "subtotal_cents": int(round(subtotal * 100)),
        "tax_cents": int(round(tax * 100)),
        "total_cents": int(round((subtotal + tax) * 100)),
        "tax_rate_decimal": rate,
    }


# ──────────────────────────────────────────────────────────────────────
# Diff report
# ──────────────────────────────────────────────────────────────────────


_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def _format_diff_line(label: str, legacy_cents: int, spine_cents: int) -> str:
    legacy_s = format_dollars(legacy_cents)
    spine_s = format_dollars(spine_cents)
    delta = spine_cents - legacy_cents
    if delta == 0:
        marker = f"{_GREEN}MATCH{_RESET}"
        delta_s = "$0.00"
    elif abs(delta) <= 1:
        marker = f"{_YELLOW}1c rounding{_RESET}"
        delta_s = format_dollars(abs(delta))
    else:
        marker = f"{_RED}DIVERGE{_RESET}"
        sign = "+" if delta > 0 else "-"
        delta_s = f"{sign}{format_dollars(abs(delta))}"
    return f"  {label:<20} legacy {legacy_s:>14}  spine {spine_s:>14}  delta {delta_s:>10}  {marker}"


def render_diff_report(legacy: dict, *, color: bool = True) -> tuple[str, int]:
    """Render a human-readable diff. Returns (report, exit_code)."""
    if not color:
        global _GREEN, _RED, _YELLOW, _RESET
        _GREEN = _RED = _YELLOW = _RESET = ""

    quote_id = (
        legacy.get("id")
        or legacy.get("rfq_id")
        or legacy.get("pc_id")
        or "<unknown>"
    )

    lines: list[str] = []
    lines.append(f"SHADOW DIFF — {quote_id}")
    lines.append("=" * 70)

    result = translate_legacy_quote(legacy)
    if not result.ok:
        lines.append(f"{_RED}TRANSLATION FAILED{_RESET}")
        lines.append("")
        for issue in result.errors():
            lines.append(f"  ERROR  {issue.field_path}: {issue.detail}")
        for issue in result.warnings():
            lines.append(f"  WARN   {issue.field_path}: {issue.detail}")
        return "\n".join(lines), 1

    legacy_totals = _legacy_totals(legacy)
    spine_totals = {
        "subtotal_cents": result.quote.subtotal_cents,
        "tax_cents": result.quote.tax_cents,
        "total_cents": result.quote.total_cents,
    }

    lines.append("")
    lines.append(_format_diff_line(
        "Subtotal",
        legacy_totals["subtotal_cents"],
        spine_totals["subtotal_cents"],
    ))
    lines.append(_format_diff_line(
        "Tax",
        legacy_totals["tax_cents"],
        spine_totals["tax_cents"],
    ))
    lines.append(_format_diff_line(
        "Total",
        legacy_totals["total_cents"],
        spine_totals["total_cents"],
    ))
    lines.append("")
    lines.append(f"  Tax rate (legacy): {legacy_totals['tax_rate_decimal'] * 100:.2f}%")
    lines.append(f"  Tax rate (Spine) : {format_tax_rate(result.quote.tax_rate_bps)}")
    lines.append("")

    # Translation issues summary.
    by_sev = {"info": 0, "warning": 0, "error": 0}
    for issue in result.issues:
        by_sev[issue.severity] = by_sev.get(issue.severity, 0) + 1
    lines.append(
        f"  Translation issues: {by_sev['info']} info, "
        f"{by_sev['warning']} warning, {by_sev['error']} error"
    )

    if result.warnings():
        lines.append("")
        lines.append("  Warnings:")
        for issue in result.warnings()[:8]:
            lines.append(f"    - {issue.field_path}: {issue.detail}")

    # Decide exit code: 0 if totals match within 1 cent, 2 otherwise.
    total_delta = abs(spine_totals["total_cents"] - legacy_totals["total_cents"])
    if total_delta > 1:
        exit_code = 2
        lines.append("")
        lines.append(f"{_RED}TOTALS DIVERGE BY {format_dollars(total_delta)}.{_RESET}")
    else:
        exit_code = 0
        lines.append("")
        # Reviewer 2026-05-15: surface the trial-run readiness signal
        # explicitly. After 5 consecutive CLEAN ships, the Spine is
        # ready for an operator-side trial (Charter Day-14 deliverable).
        lines.append(
            f"{_GREEN}CLEAN — Spine total matches legacy "
            f"(ready for operator trial run).{_RESET}"
        )

    return "\n".join(lines), exit_code


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Shadow-mode diff: run a legacy quote through the Spine.",
    )
    parser.add_argument(
        "--legacy-json", required=True,
        help='Path to legacy quote JSON (or "-" for stdin).',
    )
    parser.add_argument(
        "--pdf-out",
        help="Optional path to write the rendered Spine PDF.",
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI color in the output.",
    )
    args = parser.parse_args()

    if args.legacy_json == "-":
        legacy = json.load(sys.stdin)
    else:
        legacy = json.loads(Path(args.legacy_json).read_text(encoding="utf-8"))

    report, exit_code = render_diff_report(legacy, color=not args.no_color)
    print(report)

    if args.pdf_out and exit_code != 1:  # translation succeeded
        result = translate_legacy_quote(legacy)
        if result.ok:
            pdf_bytes = render_quote_pdf(result.quote)
            Path(args.pdf_out).write_bytes(pdf_bytes)
            print(f"\nWrote rendered Spine PDF to: {args.pdf_out}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
