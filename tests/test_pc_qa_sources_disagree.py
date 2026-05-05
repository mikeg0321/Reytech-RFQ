"""PC QA: sources-disagree warning when prices span >=3x (Surface #8).

Mike's 2026-05-04 chain (project_session_2026_05_04_calvet_quote_p0_chain
surface #8): Heel Donut item showed `$49.99 Oracle ~FUZZY` and `$49.99
Amazon` while the real price was `$7.99` — a 6× over-quote. The existing
3× cost-sanity rule (CLAUDE.md "Cost Sanity Guardrail" + quote_model.py:
265) only fires when operator-typed `unit_cost` exceeds 3× a reference.
It never fires when sources disagree AMONG THEMSELVES, so a noisy SCPRS
fuzzy match silently inflates the highest reference.

Fix: `_check_sources_disagree` returns a WARNING (per
feedback_ten_minute_escape_valve — soft warnings only, no hard gates)
when SCPRS / Catalog / Amazon / Web prices span >=3x.
"""
from __future__ import annotations


from src.agents.pc_qa_agent import _check_sources_disagree


def _has_disagree_warning(issues):
    return any(
        "sources disagree" in (i.get("message", "") or "").lower()
        for i in issues
    )


class TestSourcesDisagreeFires:
    def test_six_x_disagreement_fires(self):
        """The exact Heel Donut shape: SCPRS $49.99, Amazon $7.99 -> 6.3x."""
        item = {}
        p = {"scprs_last_price": 49.99, "amazon_price": 7.99}
        issues = _check_sources_disagree(0, item, p)
        assert _has_disagree_warning(issues)
        # Verify the structured value carries enough for a UI badge.
        v = issues[0]["value"]
        assert v["min"] == 7.99
        assert v["max"] == 49.99
        assert v["ratio"] >= 3.0
        assert v["sources"] == {"SCPRS": 49.99, "Amazon": 7.99}

    def test_three_way_disagreement_uses_global_min_max(self):
        """When 3 sources are present, use min/max not adjacent ratios.
        Per memory: 'when sources disagree by >3× either direction'."""
        p = {"scprs_last_price": 60.0, "catalog_cost": 20.0, "amazon_price": 18.0}
        issues = _check_sources_disagree(0, {}, p)
        assert _has_disagree_warning(issues)
        v = issues[0]["value"]
        assert v["ratio"] == round(60.0 / 18.0, 1)

    def test_severity_is_warning_not_blocker(self):
        """feedback_ten_minute_escape_valve + PR-C2 (#52): no hard gates.
        This must be a soft warning — operator can override."""
        p = {"scprs_last_price": 49.99, "amazon_price": 7.99}
        issues = _check_sources_disagree(0, {}, p)
        assert all(i["severity"] == "warning" for i in issues)

    def test_breakdown_in_message(self):
        """The warning message must include each source so the operator
        knows WHICH source is the suspect outlier without clicking through."""
        p = {"scprs_last_price": 49.99, "amazon_price": 7.99}
        issues = _check_sources_disagree(0, {}, p)
        msg = issues[0]["message"]
        assert "SCPRS $49.99" in msg
        assert "Amazon $7.99" in msg


class TestSourcesDisagreeQuiet:
    def test_silent_when_only_one_source(self):
        """Need >=2 sources to disagree."""
        assert _check_sources_disagree(0, {}, {"scprs_last_price": 50.0}) == []
        assert _check_sources_disagree(0, {}, {"amazon_price": 10.0}) == []

    def test_silent_when_within_3x(self):
        """A 2.5x spread should NOT fire — common pricing variance, not
        a fuzzy-match hint."""
        p = {"scprs_last_price": 50.0, "amazon_price": 25.0}
        assert _check_sources_disagree(0, {}, p) == []

    def test_silent_at_exactly_just_under_3x(self):
        """Boundary: 2.99x quiet."""
        p = {"scprs_last_price": 29.9, "amazon_price": 10.0}
        assert _check_sources_disagree(0, {}, p) == []

    def test_silent_when_no_bid_item(self):
        """Per the existing pattern — no_bid items skip pricing checks."""
        item = {"no_bid": True}
        p = {"scprs_last_price": 49.99, "amazon_price": 7.99}
        assert _check_sources_disagree(0, item, p) == []

    def test_silent_when_zero_prices(self):
        """A zero in a source means 'unknown' not 'free' — skip in min/max."""
        p = {"scprs_last_price": 0, "amazon_price": 7.99, "catalog_cost": 9.99}
        # 9.99 / 7.99 = 1.25 — well under 3x. Should NOT fire.
        assert _check_sources_disagree(0, {}, p) == []

    def test_silent_on_garbage_values(self):
        """Defensive: bad floats shouldn't crash, just skip the source."""
        p = {"scprs_last_price": "junk", "amazon_price": 7.99,
             "catalog_cost": None}
        # Only Amazon survives — single source, no warning.
        assert _check_sources_disagree(0, {}, p) == []


class TestSourcesDisagreeWiredIntoRunQa:
    """Source-level guard: the new check is actually wired into run_qa()."""

    def test_run_qa_invokes_check_sources_disagree(self):
        """Read the source so a future refactor that drops the call from
        the run_qa() loop fails fast. Same shape as the test_pc_status_flip
        + test_attachment_filename_title source guards."""
        from pathlib import Path
        src = Path("src/agents/pc_qa_agent.py").read_text(encoding="utf-8")
        assert "_check_sources_disagree" in src
        assert "issues.extend(_check_sources_disagree(idx, item, p))" in src, (
            "_check_sources_disagree must be invoked inside the per-item "
            "loop in run_qa(). If a refactor drops this line the warning "
            "will silently disappear and Surface #8 reopens."
        )
