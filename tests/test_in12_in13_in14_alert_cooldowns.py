"""Regression tests for IN-12, IN-13, IN-14 — alert-pipeline cleanup.

IN-12: every `send_alert()` call in oracle_weekly_report.py must pass a
       `cooldown_key` so a stuck job doesn't spam email/bell on every tick.
IN-13: SCPRS undercut detection in routes_growth_intel.py must emit a
       `log.warning()` (always) and a `send_alert()` (high-gap only) with
       a per-product cooldown_key, not just append to a dashboard list.
IN-14: the overdue-report alert cooldown_key must be threshold-aware so
       escalation happens as days_since grows, not once-and-silent.

These are grep-level guards — they don't replay alert delivery (the
notify_agent has its own tests). Their job is to ensure the fixes don't
silently regress when someone edits the alert blocks.
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_ORACLE = _REPO / "src" / "agents" / "oracle_weekly_report.py"
_INTEL = _REPO / "src" / "api" / "modules" / "routes_growth_intel.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ── IN-12: send_alert calls in oracle_weekly_report have cooldown_keys ───

def test_in12_oracle_weekly_send_has_cooldown():
    body = _read(_ORACLE)
    assert 'cooldown_key="oracle_weekly"' in body, \
        "IN-12 regression: oracle_weekly send_alert lost its cooldown_key"


def test_in12_oracle_weekly_send_failed_has_cooldown():
    body = _read(_ORACLE)
    assert 'cooldown_key="oracle_weekly_send_failed"' in body, \
        "IN-12 regression: oracle_weekly_send_failed send_alert lost its cooldown_key"


def test_in12_oracle_weekly_crash_has_cooldown():
    body = _read(_ORACLE)
    assert 'cooldown_key="oracle_weekly_crash"' in body, \
        "IN-12 regression: oracle_weekly crash send_alert lost its cooldown_key"


def test_in12_no_send_alert_without_cooldown_in_oracle():
    """Every send_alert(...) block in oracle_weekly_report.py must include
    a cooldown_key kwarg. Count actual call sites with regex (excludes
    the `from ... import send_alert` lines) and require one
    cooldown_key= per call site."""
    import re
    body = _read(_ORACLE)
    # Matches `send_alert(` but NOT `import send_alert` — require a
    # non-identifier char (e.g., newline, space after `=`, start-of-line)
    # immediately before the call, and not the word "import".
    call_hits = len(re.findall(r"(?<!import )\bsend_alert\(", body))
    cooldown_hits = body.count("cooldown_key=")
    assert cooldown_hits >= call_hits, (
        f"IN-12 regression: found {call_hits} send_alert() calls but only "
        f"{cooldown_hits} cooldown_key kwargs — at least one alert is "
        f"missing its cooldown dedupe."
    )


# ── IN-13: SCPRS undercut alerts are actively surfaced ──────────────────

def test_in13_scprs_undercut_logs_warning():
    body = _read(_INTEL)
    assert 'log.warning(\n                            "SCPRS undercut' in body, \
        "IN-13 regression: SCPRS undercut detection no longer logs a warning"


def test_in13_scprs_undercut_fires_send_alert():
    body = _read(_INTEL)
    assert 'event_type="scprs_undercut"' in body, \
        "IN-13 regression: SCPRS undercut no longer calls send_alert"


def test_in13_scprs_undercut_uses_per_product_cooldown():
    body = _read(_INTEL)
    assert 'cooldown_key=f"scprs_undercut_{pid}"' in body, (
        "IN-13 regression: SCPRS undercut cooldown_key must include the "
        "product id so different items each get their own bell."
    )


def test_in13_scprs_undercut_alert_is_high_gap_only():
    """Bell spam prevention: we only send_alert when the gap is large
    (≥30%). If someone lowers this threshold without thinking, the
    operator drowns in bells for every 15%+ drift."""
    body = _read(_INTEL)
    # The guard sits between the append() and the send_alert() — grep
    # for the threshold line.
    assert "if _pct >= 30:" in body, (
        "IN-13 regression: SCPRS undercut alert must gate on _pct >= 30 "
        "to avoid bell spam on small drift."
    )


# ── IN-14: overdue alert cooldown is threshold-aware ────────────────────

def test_in14_overdue_cooldown_is_bucketed():
    body = _read(_INTEL.parent.parent.parent / "agents" / "oracle_weekly_report.py")
    # Hard-coded cooldown_key="oracle_overdue" would silence the alert
    # after the first fire. Must be f-string w/ a bucket var.
    assert 'cooldown_key=f"oracle_overdue_{bucket}w"' in body, (
        "IN-14 regression: overdue alert cooldown_key must be "
        "threshold-aware (bucketed by days_since)"
    )
    assert "bucket = max(1, days_since // 7)" in body, (
        "IN-14 regression: the bucket formula changed — verify escalation "
        "still happens as days_since grows"
    )


def test_in14_no_static_oracle_overdue_cooldown():
    """The old bug was `cooldown_key="oracle_overdue"` (static string).
    A new alert with that exact literal would re-introduce the issue."""
    body = _read(_INTEL.parent.parent.parent / "agents" / "oracle_weekly_report.py")
    assert 'cooldown_key="oracle_overdue"' not in body, (
        "IN-14 regression: static cooldown_key=\"oracle_overdue\" is back — "
        "the overdue alert will silence itself after the first fire."
    )
