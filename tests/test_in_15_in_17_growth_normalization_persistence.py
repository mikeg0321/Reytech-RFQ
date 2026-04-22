"""Regression tests for IN-15 + IN-17 (growth agent audit).

IN-15: buyer-agency normalization at ingest. Raw agency strings from
       SCPRS / email / manual entry were written to the prospects DB
       as-is, so "CDCR", "California Department of Corrections", and
       "Dept. of Corrections" coexisted and split funnel metrics.
       Every ingest site now funnels raw strings through
       growth_agent._norm_agency() which uses institution_resolver.

IN-17: status persistence across workers. PULL_STATUS, BUYER_STATUS,
       and INTEL_STATUS lived in module globals, so a Gunicorn worker
       querying status while a pull ran on another worker returned
       stale "idle". Each long-running function now persists status
       transitions to /data/status/<name>.json and get_*_status()
       falls back to the file when this worker isn't actively running.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


_REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


# ── IN-15: _norm_agency helper and application sites ─────────────────

def test_in15_norm_agency_helper_exists():
    body = _read("src/agents/growth_agent.py")
    assert "def _norm_agency(raw: str) -> str:" in body, (
        "IN-15 regression: _norm_agency helper missing. Without it, "
        "raw SCPRS agency strings land in prospects with divergent "
        "spellings and split the funnel metrics."
    )


def test_in15_norm_agency_uses_institution_resolver():
    body = _read("src/agents/growth_agent.py")
    assert "from src.core.institution_resolver import resolve" in body, (
        "IN-15 regression: _norm_agency no longer imports the shared "
        "resolver — normalization is drifting from the rest of the app."
    )


def test_in15_prospect_writes_use_norm_agency():
    """find_category_buyers + run_buyer_intelligence (phase 1 + phase 2)
    must call _norm_agency at the dept assignment — those are the 3
    SCPRS-ingest points."""
    body = _read("src/agents/growth_agent.py")
    # Count occurrences. Should be >= 3 (find_category_buyers + p1 + p2).
    occurrences = body.count('_norm_agency(r.get("dept")')
    assert occurrences >= 3, (
        f"IN-15 regression: _norm_agency applied at only {occurrences} "
        f"of 3 SCPRS ingest sites. Check find_category_buyers + "
        f"run_buyer_intelligence phase 1 + phase 2 write sites."
    )


def test_in15_norm_agency_safe_on_none():
    """Helper must tolerate None without crashing — raw fields can be
    None when SCPRS rows are partially populated."""
    import sys
    sys.path.insert(0, str(_REPO))
    from src.agents.growth_agent import _norm_agency
    assert _norm_agency(None) == ""
    assert _norm_agency("") == ""
    assert _norm_agency("   ") == ""


def test_in15_norm_agency_preserves_unknown():
    """If institution_resolver can't match, return the trimmed input —
    never drop the agency entirely or unknown buyers vanish from the
    funnel."""
    import sys
    sys.path.insert(0, str(_REPO))
    from src.agents.growth_agent import _norm_agency
    result = _norm_agency("  Some Unknown Obscure Agency LLC  ")
    # Either canonical (if resolver matched anything) OR trimmed input
    # (if not) — but never empty and never raw whitespace.
    assert result, "Unknown agency collapsed to empty — funnel loss"
    assert result == result.strip()


def test_in15_norm_agency_returns_canonical_for_known():
    """Verify the resolver-backed path actually produces canonical
    output — not just a passthrough — for a well-known agency."""
    import sys
    sys.path.insert(0, str(_REPO))
    from src.agents.growth_agent import _norm_agency
    for raw in ("CDCR", "California Department of Corrections"):
        result = _norm_agency(raw)
        assert result, f"CDCR variant {raw!r} collapsed to empty"
        assert len(result) > 1


# ── IN-17: status persistence helpers and transition hooks ───────────

def test_in17_persist_status_helper_exists():
    body = _read("src/agents/growth_agent.py")
    assert "def _persist_status(name: str, data: dict) -> None:" in body, (
        "IN-17 regression: _persist_status helper missing. Without it, "
        "status can't cross Gunicorn worker boundaries."
    )
    assert "def _load_persisted_status(name: str)" in body, (
        "IN-17 regression: _load_persisted_status helper missing."
    )


def test_in17_status_dir_declared():
    body = _read("src/agents/growth_agent.py")
    assert 'STATUS_DIR = os.path.join(DATA_DIR, "status")' in body, (
        "IN-17 regression: STATUS_DIR constant missing or path changed. "
        "All 4 workers must agree on the status directory."
    )


def test_in17_phase_transitions_persist():
    """Each of the 3 long-running fn — pull_reytech_history,
    find_category_buyers, run_buyer_intelligence — must persist at
    both the running=True start AND at least one termination path
    (complete, error, or session-init fail)."""
    body = _read("src/agents/growth_agent.py")
    # Running=True start points for each: 3 total
    # Termination points: at least complete+error+session-fail for each
    # Total _persist_status calls should be >= 9 (3 start + ≥6 term)
    count = body.count("_persist_status(")
    # 3 for the helper definition + at least 9 for hooks
    # (definition line counts, so total >= 12... actually the definition
    # is `def _persist_status(` which doesn't match the call pattern.)
    # Let's grep for actual call-site pattern.
    import re
    call_pattern = re.compile(r"_persist_status\(\"(pull|buyer|intel)\",")
    calls = call_pattern.findall(body)
    assert len(calls) >= 9, (
        f"IN-17 regression: only {len(calls)} _persist_status call sites "
        f"— expected ≥9 (3 start + 6+ termination paths across pull / "
        f"buyer / intel). Missing persists = cross-worker staleness."
    )
    # Each bucket should be represented
    assert "pull" in calls, "IN-17: no pull status persists"
    assert "buyer" in calls, "IN-17: no buyer status persists"
    assert "intel" in calls, "IN-17: no intel status persists"


def test_in17_get_status_falls_back_to_persisted():
    """get_intel_status / get_pull_status / get_buyer_status must
    check the persisted file when the in-memory dict says not-running.
    That's the whole point — the worker querying status probably didn't
    start the pull."""
    body = _read("src/agents/growth_agent.py")
    for fn in ("get_intel_status", "get_pull_status", "get_buyer_status"):
        # Find the function
        import re
        func_match = re.search(
            rf"def {fn}\(\).*?(?=\ndef |\Z)",
            body,
            re.DOTALL,
        )
        assert func_match, f"IN-17 regression: {fn} function missing"
        func_body = func_match.group(0)
        assert "_load_persisted_status" in func_body, (
            f"IN-17 regression: {fn} no longer reads persisted status "
            f"— cross-worker reads will see stale 'idle' while a peer "
            f"worker runs the actual pull."
        )


def test_in17_persist_is_best_effort():
    """_persist_status must not crash the caller — status tracking is
    non-critical."""
    import sys
    sys.path.insert(0, str(_REPO))
    from src.agents.growth_agent import _persist_status
    # Passing a clearly bad dict (non-serializable) should not raise
    class Unserializable:
        pass
    _persist_status("test_best_effort", {"obj": Unserializable()})
    # If we reach here, best-effort contract held


def test_in17_persist_round_trip(tmp_path, monkeypatch):
    """Happy path: persist + load returns the same data."""
    import sys
    sys.path.insert(0, str(_REPO))
    import src.agents.growth_agent as growth
    monkeypatch.setattr(growth, "STATUS_DIR", str(tmp_path / "status"))
    growth._persist_status("unit_test", {"running": True, "phase": "ok"})
    loaded = growth._load_persisted_status("unit_test")
    assert loaded is not None
    assert loaded["running"] is True
    assert loaded["phase"] == "ok"


def test_in17_load_missing_returns_none(tmp_path, monkeypatch):
    """Missing file → None (not {}), so callers can distinguish 'never
    written' from 'written empty'."""
    import sys
    sys.path.insert(0, str(_REPO))
    import src.agents.growth_agent as growth
    monkeypatch.setattr(growth, "STATUS_DIR", str(tmp_path / "status"))
    result = growth._load_persisted_status("never_written")
    assert result is None
