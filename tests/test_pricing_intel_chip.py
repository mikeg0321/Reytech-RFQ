"""PR-L — quote-time SCPRS rollup chip.

Surfaces oracle_audit.scprs_rollup inline next to every priced item
so the operator sees pricing intelligence BEFORE clicking Send.

Pinned guarantees:
  1. Chip appears for items with rollup.p75 > 0 + count >= 5.
  2. Chip is empty for items missing oracle_audit, missing rollup,
     p75=0, or count below threshold.
  3. Color band: green (<=p75), yellow (p75-p90), red (>p90),
     neutral grey when no operator price yet.
  4. data-* attrs on the chip carry p50/p75/p90/rcount so JS can
     recompute on every keystroke without server roundtrip.
  5. Drift sign rendered correctly (+/-).
  6. Route renders the chip inline (static cover that the helper is
     wired in `_pricecheck_detail_inner`).
  7. JS recalcRollupChip function is present in pc_detail.html.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _audit(p75, p90=None, p50=None, count=50, match_key="X-1",
           match_key_type="mfg"):
    return {
        "rec_price": p75,
        "rec_pre_cap_price": None,
        "caps_applied": [],
        "scprs_rollup": {
            "count": count,
            "p50": p50 if p50 is not None else p75 * 0.85,
            "p75": p75,
            "p90": p90 if p90 is not None else p75 * 1.15,
            "match_key": match_key,
            "match_key_type": match_key_type,
        },
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13",
    }


# ── Build helper ─────────────────────────────────────────────────────


def test_chip_renders_for_eligible_item():
    """Item has rollup with p75=$60, count=50, operator sent $60 →
    green at/below p75 chip."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, p90=70.0, count=50)}
    chip, attrs = build_rollup_chip(item, idx=0, current_price=60.0)
    assert chip != "" and attrs != ""
    assert 'id="rollup_chip_0"' in chip
    assert "p75 $60.00" in chip
    assert "+0.0%" in chip  # exactly at p75
    assert 'data-p75="60.00"' in attrs
    assert 'data-p90="70.00"' in attrs
    assert 'data-rcount="50"' in attrs
    # Green color band
    assert "#3fb950" in chip


def test_chip_yellow_when_between_p75_and_p90():
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, p90=80.0, count=50)}
    chip, _ = build_rollup_chip(item, idx=2, current_price=70.0)
    assert "#d29922" in chip  # yellow
    assert "+16.7%" in chip   # (70-60)/60 = 16.67%


def test_chip_red_when_above_p90():
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, p90=80.0, count=50)}
    chip, _ = build_rollup_chip(item, idx=3, current_price=100.0)
    assert "#f85149" in chip  # red
    assert "+66.7%" in chip


def test_chip_neutral_when_no_operator_price():
    """No price entered yet → neutral grey + drift=0. The chip still
    appears so the operator sees the p75 reference even before pricing."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, count=50)}
    chip, _ = build_rollup_chip(item, idx=4, current_price=0)
    assert chip != ""
    assert "#8b949e" in chip  # neutral grey
    assert "no price yet" in chip


# ── Skip conditions ─────────────────────────────────────────────────


def test_chip_empty_when_no_oracle_audit():
    from src.core.pricing_intel_chip import build_rollup_chip
    chip, attrs = build_rollup_chip({}, idx=0, current_price=50.0)
    assert chip == ""
    assert attrs == ""


def test_chip_empty_when_no_rollup_in_audit():
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": {"rec_price": 50.0, "scprs_rollup": None}}
    chip, attrs = build_rollup_chip(item, idx=0, current_price=50.0)
    assert chip == ""


def test_chip_empty_when_count_below_threshold():
    """count=3 < 5 → too noisy, skip. Below this threshold the operator
    would be misled by single-sample percentiles."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, count=3)}
    chip, attrs = build_rollup_chip(item, idx=0, current_price=60.0)
    assert chip == ""
    assert attrs == ""


def test_chip_empty_when_p75_zero_or_missing():
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": {
        "scprs_rollup": {"count": 50, "p75": 0, "p90": 0}
    }}
    chip, _ = build_rollup_chip(item, idx=0, current_price=60.0)
    assert chip == ""


def test_chip_handles_malformed_rollup_safely():
    """Forward-compat: garbage in rollup dict must not raise."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": {
        "scprs_rollup": {"count": "fifty", "p75": "abc"}
    }}
    chip, attrs = build_rollup_chip(item, idx=0, current_price=60.0)
    assert chip == ""
    assert attrs == ""


def test_chip_falls_back_when_p90_missing():
    """Rollup has p75 + count but no p90 → derive p90 ≈ p75 × 1.15."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": {"scprs_rollup": {
        "count": 50, "p75": 60.0,  # no p90
        "match_key": "X", "match_key_type": "mfg",
    }}}
    chip, attrs = build_rollup_chip(item, idx=0, current_price=70.0)
    assert chip != ""
    # p90 fallback = 60 × 1.15 = 69.0 → 70 > 69 → red
    assert 'data-p90="69.00"' in attrs
    assert "#f85149" in chip


# ── Title/tooltip + match key surface ────────────────────────────────


def test_chip_title_includes_sample_count_and_match_key():
    """The title attribute is what the operator sees on hover. It must
    explain the number AND show the join key so they can sanity-check
    the match."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(
        p75=60.0, count=42, match_key="16-N8MMPA",
        match_key_type="mfg",
    )}
    chip, _ = build_rollup_chip(item, idx=0, current_price=60.0)
    assert "42 historic winners" in chip
    assert "16-N8MMPA" in chip
    assert "mfg" in chip


def test_chip_drift_negative_for_below_p75():
    """Operator at $40 vs p75=$60 → drift = -33.3%."""
    from src.core.pricing_intel_chip import build_rollup_chip
    item = {"oracle_audit": _audit(p75=60.0, count=50)}
    chip, _ = build_rollup_chip(item, idx=0, current_price=40.0)
    assert "-33.3%" in chip
    assert "#3fb950" in chip  # still green (below p75 is good)


# ── Route + JS wire-up ───────────────────────────────────────────────


def test_route_imports_and_wires_build_rollup_chip():
    import inspect
    from src.api.modules import routes_pricecheck
    src = inspect.getsource(routes_pricecheck)
    assert "build_rollup_chip" in src, (
        "routes_pricecheck must import + call build_rollup_chip in "
        "_pricecheck_detail_inner — without this the chip never reaches "
        "the rendered HTML"
    )
    assert "_rollup_chip" in src and "_rollup_td_attrs" in src


def test_pc_detail_template_defines_recalc_function():
    """The JS handler must exist in pc_detail.html and be exposed on
    `window` so the inline price-input handlers can find it."""
    import os
    path = os.path.join(
        os.path.dirname(__file__), "..",
        "src", "templates", "pc_detail.html",
    )
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    assert "function recalcRollupChip(idx)" in src
    assert "window.recalcRollupChip = recalcRollupChip" in src
    # Color bands must be present so the JS color flip survives a refactor
    for color in ("#3fb950", "#d29922", "#f85149", "#8b949e"):
        assert color in src


def test_price_input_oninput_calls_recalc():
    """Static cover: every price_{idx} input must have a
    recalcRollupChip() call wired to its keystroke handler. Without
    this, the chip color goes stale the moment the operator types."""
    import inspect
    from src.api.modules import routes_pricecheck
    src = inspect.getsource(routes_pricecheck)
    # The inline route HTML f-string must reference recalcRollupChip
    # alongside the existing sanitizePrice/recalcPC calls
    assert "recalcRollupChip" in src, (
        "price input handler must call recalcRollupChip on each keystroke"
    )
