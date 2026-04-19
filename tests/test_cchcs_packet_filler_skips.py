"""Tests for the cchcs_packet_filler skip ledger (PR #189).

cchcs_packet_filler is the highest-revenue-impact form generator —
CCHCS packets are ~60% of Reytech's quote volume. Several silent-skip
sites in this module can produce a packet that LOOKS valid but is
quietly wrong:

  * subtotal / grand_total parse failures → return $0 with no signal
    (operator sees wrong totals on the cover page)
  * civil rights template missing → declaration page silently absent
    from the final packet
  * civil rights append failure → same outcome, different cause
  * splice serialize failure → entire attachment splice abandoned,
    placeholder pages survive in final packet
  * per-attachment filler returned None → that single attachment is
    a placeholder in the final packet
  * per-attachment append failure → same per-attachment outcome

Following the rollout pattern from PRs #184/#185/#186/#187, this module
exposes a drainable `_SKIP_LEDGER` whose contents the orchestrator
sweeps after every quote run and persists into `feature_status` so the
dashboard banner shows "civil rights template missing — degraded" until
the operator fixes it.

Per-row INFO skips (subtotal/grand_total parse) tell the dashboard
"this packet's totals were uncomputable" without re-logging — the
re-log would spam the build log on every malformed price field. Whole-
feature WARNING skips (civil rights, splice) re-log because they affect
deliverable correctness.
"""
from __future__ import annotations

import pytest

from src.core.dependency_check import Severity
from src.forms import cchcs_packet_filler


@pytest.fixture(autouse=True)
def _drain_between_tests():
    """Each test starts with an empty ledger so they don't poison each other."""
    cchcs_packet_filler.drain_skips()
    yield
    cchcs_packet_filler.drain_skips()


class TestDrainContract:
    def test_drain_returns_list(self):
        assert cchcs_packet_filler.drain_skips() == []

    def test_drain_clears_the_ledger(self):
        cchcs_packet_filler._record_skip(
            cchcs_packet_filler.SkipReason(
                name="x", reason="y", severity=Severity.INFO, where="z",
            )
        )
        first = cchcs_packet_filler.drain_skips()
        assert len(first) == 1
        # Second drain must be empty — destructive.
        assert cchcs_packet_filler.drain_skips() == []

    def test_record_appends_in_order(self):
        cchcs_packet_filler._record_skip(
            cchcs_packet_filler.SkipReason(
                name="a", reason="r1", severity=Severity.INFO, where="w",
            )
        )
        cchcs_packet_filler._record_skip(
            cchcs_packet_filler.SkipReason(
                name="b", reason="r2", severity=Severity.WARNING, where="w",
            )
        )
        skips = cchcs_packet_filler.drain_skips()
        assert [s.name for s in skips] == ["a", "b"]


class TestTotalsParseSkips:
    """The subtotal / grand_total return values feed the operator-visible
    summary on the dashboard. Silently returning $0 means the operator
    might send a packet thinking the cover-page totals are correct when
    the packet was filled with malformed money strings."""

    def test_invalid_subtotal_emits_info_skip(self):
        # Use the public helper; it should return 0.0 + emit a skip.
        val = cchcs_packet_filler._parse_money_safely(
            "not-a-number", field="subtotal", where="test_subtotal",
        )
        assert val == 0.0
        skips = cchcs_packet_filler.drain_skips()
        assert any(
            s.name == "totals_parse"
            and s.severity == Severity.INFO
            and "subtotal" in s.reason
            for s in skips
        ), skips

    def test_invalid_grand_total_emits_info_skip(self):
        val = cchcs_packet_filler._parse_money_safely(
            "$$BAD$$", field="grand_total", where="test_grand_total",
        )
        assert val == 0.0
        skips = cchcs_packet_filler.drain_skips()
        assert any(
            s.name == "totals_parse"
            and s.severity == Severity.INFO
            and "grand_total" in s.reason
            for s in skips
        ), skips

    def test_valid_money_strings_emit_no_skip(self):
        assert cchcs_packet_filler._parse_money_safely(
            "1,234.56", field="subtotal", where="t",
        ) == 1234.56
        assert cchcs_packet_filler._parse_money_safely(
            "0", field="subtotal", where="t",
        ) == 0.0
        assert cchcs_packet_filler._parse_money_safely(
            "", field="subtotal", where="t",
        ) == 0.0
        assert cchcs_packet_filler._parse_money_safely(
            None, field="subtotal", where="t",
        ) == 0.0
        # Healthy parses must not emit skips — the ledger is for failures.
        assert cchcs_packet_filler.drain_skips() == []


class TestCivilRightsTemplateSkip:
    def test_missing_template_emits_warning_skip(self, monkeypatch):
        # Force the template lookup to fail.
        monkeypatch.setattr(
            cchcs_packet_filler, "_find_civil_rights_template",
            lambda: None,
        )

        # Use a minimal stub writer — the function returns False before
        # touching it when the template is missing.
        class _StubWriter:
            pass

        ok = cchcs_packet_filler._append_civil_rights_attachment(
            _StubWriter(), {"company_name": "Reytech Inc."},
        )
        assert ok is False

        skips = cchcs_packet_filler.drain_skips()
        assert any(
            s.name == "civil_rights_template"
            and s.severity == Severity.WARNING
            and "_append_civil_rights_attachment" in s.where
            for s in skips
        ), skips


class TestSpliceSerializeSkip:
    def test_serialize_failure_emits_warning_skip(self, monkeypatch):
        """If we cannot serialize the in-progress writer, the splice cannot
        run — the final packet would be missing every attachment."""

        class _BoomWriter:
            def write(self, _buf):
                raise RuntimeError("simulated serialize crash")

        bw = _BoomWriter()
        # Returns the original writer untouched on serialize failure
        out = cchcs_packet_filler._splice_attachments(
            bw, parsed={}, reytech_info={"company_name": "Reytech Inc."},
        )
        assert out is bw

        skips = cchcs_packet_filler.drain_skips()
        assert any(
            s.name == "splice_serialize"
            and s.severity == Severity.WARNING
            and "simulated serialize crash" in s.reason
            for s in skips
        ), skips


class TestModuleRegisteredWithOrchestrator:
    """The orchestrator's end-of-run sweep (PR #188) drains every module
    listed in `_SKIP_LEDGER_MODULES`. cchcs_packet_filler must be on
    that list or its ledger is invisible to the dashboard banner."""

    def test_cchcs_packet_filler_is_in_sweep_list(self):
        from src.core.quote_orchestrator import QuoteOrchestrator
        assert "src.forms.cchcs_packet_filler" in QuoteOrchestrator._SKIP_LEDGER_MODULES
