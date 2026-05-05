"""Profit-floor doctrine contract test.

Mike's directive 2026-05-04: "id rather have 10000 $20 profits, than 5 100$
profits." See `feedback_volume_over_margin` memory.

The profit floor in all its forms (PC QA blocker, oracle auto-bump, CCHCS
markup-warn-low, MSRP-vs-sale-price floor evaluator) was deleted in this
PR. This test pins the scenario from PC `pc_08583a68` that triggered it
and asserts NO blocker fires, NO floor-warning fires, and the symbols
themselves are gone (so a future "looks-low-margin, let's add a guard"
PR can't quietly reintroduce them under another name).
"""
from __future__ import annotations

import pytest


# ─── The scenario ───────────────────────────────────────────────────────

def _mike_scenario_pc():
    """Welch Allyn IV pole mount, MSRP cost $108, qty 2, markup 20%,
    our price $129.60, profit $43.20 (16.7%). Pre-fix this fired:
    'Total profit $43.20 is below $75.00 floor — not worth the operational
    cost to quote' as a BLOCKER. Post-fix: no blocker, no warning."""
    return {
        "pc_number": "AUTO_08583a68",
        "items": [{
            "description": "Welch Allyn IV pole mount for charging stand",
            "qty": 2,
            "uom": "EA",
            "mfg_number": "8500-0001",
            "pricing": {
                "unit_cost": 108.00,
                "markup_pct": 20,
                "unit_price": 129.60,
            },
            "unit_price": 129.60,
        }],
        "profit_summary": {
            "total_revenue": 259.20,
            "total_cost": 216.00,
            "gross_profit": 43.20,
        },
        "ship_to": "100 Folsom Prison Rd, Represa, CA 95671",
        "agency": "CCHCS",
        "requestor": "Valentina Demidenko",
    }


# ─── Behavioral contract ────────────────────────────────────────────────

def test_mike_scenario_emits_no_profit_blocker():
    """The exact PC `pc_08583a68` scenario that fired the $75-floor blocker
    must now produce zero profit-related blockers."""
    from src.agents.pc_qa_agent import run_qa, BLOCKER

    report = run_qa(_mike_scenario_pc(), use_llm=False)
    profit_blockers = [
        i for i in report["issues"]
        if i.get("severity") == BLOCKER
        and ("profit" in (i.get("field") or "").lower()
             or "profit" in (i.get("message") or "").lower())
    ]
    assert not profit_blockers, (
        f"Profit-floor blocker reintroduced. Mike's doctrine: 'id rather "
        f"have 10000 $20 profits, than 5 100$ profits.' See memory file "
        f"feedback_volume_over_margin. Blockers found: {profit_blockers}"
    )


def test_no_floor_warning_in_cchcs_gate_source():
    """CCHCS packet gate source must not contain the markup-low warning
    branch. The MARKUP_WARN_HIGH branch (>200% scrape errors) must remain.
    Source-level guard because gate_validate's full call signature requires
    a real PDF, which is overkill for a doctrine pin."""
    from pathlib import Path

    src = Path("src/forms/cchcs_packet_gate.py").read_text(encoding="utf-8")
    assert "below" not in src.lower() or "below cost" in src.lower(), (
        "Source still contains a 'below floor/threshold' style warning. "
        "Mike's doctrine: low margin is a strategic choice, not a packet "
        "issue. Only 'below cost' (negative margin) should fire."
    )
    assert "markup_warn_low" not in src.lower(), (
        "MARKUP_WARN_LOW reference still in source — should be deleted."
    )
    assert "MARKUP_WARN_HIGH" in src, (
        "MARKUP_WARN_HIGH must remain — it catches >200% scrape errors."
    )


# ─── Symbol contract — guards against quiet reintroduction ──────────────

def test_pc_qa_agent_has_no_profit_floor_symbols():
    """Make sure nobody adds `PROFIT_FLOOR` or `_check_profit` back. If a
    future PR genuinely needs them (it shouldn't), this test must be
    deleted in the same PR — the test failure is the conversation."""
    from src.agents import pc_qa_agent

    assert not hasattr(pc_qa_agent, "PROFIT_FLOOR"), (
        "PROFIT_FLOOR constant reintroduced. Per Mike 2026-05-04, the "
        "profit floor is dead — including soft warnings."
    )
    assert not hasattr(pc_qa_agent, "_check_profit"), (
        "_check_profit reintroduced. The doctrine: many small wins beat "
        "a few large ones. Don't gate on minimum margin."
    )


def test_pricing_oracle_has_no_dollar_floor():
    """The auto-bump-to-floor logic in pricing_oracle_v2 must stay deleted.
    Reintroducing it silently inflates Mike's quotes above the price-to-win,
    losing bids. Acceptable form: oracle SUGGESTS a price; operator decides."""
    from src.core import pricing_oracle_v2

    assert not hasattr(pricing_oracle_v2, "_apply_dollar_floor"), (
        "_apply_dollar_floor reintroduced. This silently bumped quote_price "
        "above what Mike asked for whenever GP fell under $3. Per "
        "feedback_volume_over_margin, the oracle SUGGESTS — it does not "
        "force-correct."
    )
    assert not hasattr(pricing_oracle_v2, "_DOLLAR_FLOOR_DEFAULT"), (
        "_DOLLAR_FLOOR_DEFAULT constant reintroduced."
    )


def test_cchcs_gate_has_no_markup_warn_low():
    """MARKUP_WARN_LOW deleted. MARKUP_WARN_HIGH stays — it catches >200%
    scrape errors, which is a real bug, not a strategic choice."""
    from src.forms import cchcs_packet_gate

    assert not hasattr(cchcs_packet_gate, "MARKUP_WARN_LOW"), (
        "MARKUP_WARN_LOW reintroduced. Low markup is not a packet error — "
        "high markup (>200%) is, because that's almost always a scrape bug. "
        "Keep MARKUP_WARN_HIGH; do NOT add MARKUP_WARN_LOW back."
    )
    assert hasattr(cchcs_packet_gate, "MARKUP_WARN_HIGH"), (
        "MARKUP_WARN_HIGH is the legitimate guard against scrape errors "
        "and must stay."
    )


def test_reytech_config_has_no_profit_floor_keys():
    """The two scattered profit_floor_* config keys must stay deleted from
    BOTH copies of reytech_config.json (root + src/forms)."""
    import json
    from pathlib import Path

    for path in [
        Path("reytech_config.json"),
        Path("src/forms/reytech_config.json"),
    ]:
        if not path.exists():
            continue
        cfg = json.loads(path.read_text(encoding="utf-8"))
        rules = cfg.get("pricing_rules") or {}
        assert "profit_floor_general" not in rules, (
            f"{path}: profit_floor_general reintroduced."
        )
        assert "profit_floor_amazon" not in rules, (
            f"{path}: profit_floor_amazon reintroduced."
        )
