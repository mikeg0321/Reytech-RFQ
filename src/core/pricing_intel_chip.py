"""PR-L: quote-time SCPRS rollup chip.

Closes the feedback loop on /goal. Substrate work (PR-G..PR-K1) gave the
operator a /oracle/drift/preview surface to review AFTER sending. This
helper surfaces the same intel on every PC item row AT QUOTE TIME — the
operator sees "p75 $60 · drift +25% · n=50" inline with each price
input, BEFORE clicking Send.

Decision criteria:
  - Show chip ONLY when oracle_audit.scprs_rollup has p75 > 0 AND
    count >= 5 (below 5 samples the percentile is too noisy to act on).
  - Color band:
      green     operator_price <= p75              ("at/below p75")
      yellow    p75 < operator_price <= p90        ("above p75")
      red       operator_price > p90               ("likely overpriced")
      neutral   no operator price yet              ("no price yet")
  - data-p75/data-p90/data-rcount land on the <td> so the JS recalc
    fires on every keystroke in the price input without a server roundtrip.
"""
from __future__ import annotations


def build_rollup_chip(
    item: dict,
    idx: int,
    current_price: float,
    *,
    min_count: int = 5,
) -> tuple[str, str]:
    """Return (chip_html, td_attrs) for the PC item row.

    Both strings are empty when the item has no usable rollup data —
    that's how the row renders without the chip on items where the
    oracle never had a match key (e.g. operator-typed line, no MFG#).

    `td_attrs` is meant to be inlined directly into the price-column
    `<td>` opening tag (note: leading space). Example:
        f'<td{td_attrs}><input ...></td>'
    """
    if not isinstance(item, dict):
        return "", ""
    rollup = ((item.get("oracle_audit") or {}).get("scprs_rollup") or {})
    if not isinstance(rollup, dict) or not rollup:
        return "", ""
    try:
        p75 = float(rollup.get("p75") or 0)
        rcount = int(rollup.get("count") or 0)
    except (TypeError, ValueError):
        return "", ""
    if p75 <= 0 or rcount < min_count:
        return "", ""

    try:
        p90 = float(rollup.get("p90") or 0) or (p75 * 1.15)
    except (TypeError, ValueError):
        p90 = p75 * 1.15
    try:
        p50 = float(rollup.get("p50") or 0) or (p75 * 0.85)
    except (TypeError, ValueError):
        p50 = p75 * 0.85

    # Determine color band from current price.
    if current_price is None or current_price <= 0:
        color = "#8b949e"  # neutral grey
        bg = "rgba(139,148,158,.12)"
        band = "no price yet"
        drift = 0.0
    else:
        try:
            cp = float(current_price)
        except (TypeError, ValueError):
            cp = 0.0
        drift = round((cp - p75) / p75 * 100, 1)
        if cp <= p75:
            color = "#3fb950"  # green
            bg = "rgba(63,185,80,.12)"
            band = "at/below p75"
        elif cp <= p90:
            color = "#d29922"  # yellow
            bg = "rgba(210,153,34,.12)"
            band = "above p75"
        else:
            color = "#f85149"  # red
            bg = "rgba(248,81,73,.12)"
            band = "above p90 — likely overpriced"

    sign = "+" if drift >= 0 else ""
    match_key = str(rollup.get("match_key") or "").strip()
    match_key_type = str(rollup.get("match_key_type") or "").strip()
    match_suffix = ""
    if match_key and match_key_type:
        match_suffix = f" Match: {match_key_type}={match_key}."
    title = (
        f"SCPRS p75=${p75:.2f} from {rcount} historic winners. "
        f"Drift = (your price − p75) / p75. {band}.{match_suffix}"
    )

    chip = (
        f'<div class="rollup-chip" id="rollup_chip_{idx}" '
        f'style="margin-top:4px;padding:2px 6px;border-radius:3px;'
        f'background:{bg};color:{color};font-size:11px;font-weight:600;'
        f'line-height:1.2;display:inline-block;cursor:help" '
        f'title="{title}">'
        f'p75 ${p75:.2f} · '
        f'<span class="drift-val">{sign}{drift:.1f}%</span> '
        f'<span style="opacity:.7">· n={rcount}</span>'
        f'</div>'
    )

    # The data- attrs ride the price-column <td>, accessed by the inline
    # JS recalcRollupChip(idx) on every keystroke in the price input.
    attrs = (
        f' data-p50="{p50:.2f}" data-p75="{p75:.2f}" '
        f'data-p90="{p90:.2f}" data-rcount="{rcount}"'
    )
    return chip, attrs
