"""Tests for the orchestrator's skip-routing layer (PR #182).

Consumers from PR #183 onward will return SkipReason objects that the
orchestrator must route into its existing 3-channel envelope:

    BLOCKER → result.blockers   (qa_pass refuses to advance)
    WARNING → result.warnings   (operator-visible degraded feature)
    INFO    → result.notes      (informational)

This is the routing layer — no consumers wired up yet (those land in
PR #183-#190). The contract here:

    OrchestratorResult.add_skip(skip)
        - Routes the skip into the right channel by severity
        - Also stores it on result.skips for the audit/feature_status sink
        - Formats the channel entry as "<name>: <reason>" so the
          dashboard sees consistent prefixing across consumers
"""
from __future__ import annotations

from src.core.dependency_check import Severity, SkipReason
from src.core.quote_orchestrator import OrchestratorResult


class TestAddSkipRouting:
    def test_blocker_routes_to_blockers(self):
        r = OrchestratorResult()
        r.add_skip(SkipReason(
            name="src.core.agency_config",
            reason="ImportError: not found",
            severity=Severity.BLOCKER,
            where="compliance_validator._check_required_forms",
        ))
        assert any(
            "src.core.agency_config" in b and "ImportError" in b
            for b in r.blockers
        ), r.blockers
        assert not r.warnings
        assert not r.notes

    def test_warning_routes_to_warnings(self):
        r = OrchestratorResult()
        r.add_skip(SkipReason(
            name="ANTHROPIC_API_KEY",
            reason="env var unset",
            severity=Severity.WARNING,
            where="compliance_validator._run_llm_gap_check",
        ))
        assert any(
            "ANTHROPIC_API_KEY" in w and "env var unset" in w
            for w in r.warnings
        ), r.warnings
        assert not r.blockers
        assert not r.notes

    def test_info_routes_to_notes(self):
        r = OrchestratorResult()
        r.add_skip(SkipReason(
            name="oracle_cache",
            reason="cold cache, recomputed from source",
            severity=Severity.INFO,
            where="pricing_oracle_v2.lookup",
        ))
        assert any(
            "oracle_cache" in n for n in r.notes
        ), r.notes
        assert not r.blockers
        assert not r.warnings

    def test_skip_also_stored_on_result_for_audit(self):
        """The audit log + feature_status table need the structured form, not
        just the formatted string. add_skip must keep the SkipReason object
        accessible for downstream sinks."""
        r = OrchestratorResult()
        s = SkipReason(
            name="x",
            reason="y",
            severity=Severity.WARNING,
            where="z",
        )
        r.add_skip(s)
        assert s in r.skips, r.skips

    def test_multiple_skips_route_independently(self):
        r = OrchestratorResult()
        r.add_skip(SkipReason("a", "ra", Severity.BLOCKER, "wa"))
        r.add_skip(SkipReason("b", "rb", Severity.WARNING, "wb"))
        r.add_skip(SkipReason("c", "rc", Severity.INFO, "wc"))
        r.add_skip(SkipReason("d", "rd", Severity.WARNING, "wd"))
        assert len(r.blockers) == 1
        assert len(r.warnings) == 2
        assert len(r.notes) == 1
        assert len(r.skips) == 4

    def test_channel_entries_are_consistently_prefixed(self):
        """Dashboards that filter by `<source>:` prefix (compliance/parse/etc.)
        rely on a consistent format. SkipReasons should land as
        '<name>: <reason>' so the prefix is the name field."""
        r = OrchestratorResult()
        r.add_skip(SkipReason("compliance_validator", "import failed", Severity.BLOCKER, "x"))
        assert r.blockers[0].startswith("compliance_validator: ")

    def test_add_skip_is_idempotent_for_duplicate_reasons(self):
        """If the same skip fires twice in a single run (e.g. once per profile),
        we don't want N copies of identical entries spamming warnings. The
        deduplication is by (name, reason, severity) — same skip = same entry."""
        r = OrchestratorResult()
        s = SkipReason("k", "r", Severity.WARNING, "w1")
        s_dup = SkipReason("k", "r", Severity.WARNING, "w2")  # different where, same skip
        r.add_skip(s)
        r.add_skip(s_dup)
        assert len(r.warnings) == 1, r.warnings
        # But both stored for audit so we know it fired twice.
        assert len(r.skips) == 2, r.skips
