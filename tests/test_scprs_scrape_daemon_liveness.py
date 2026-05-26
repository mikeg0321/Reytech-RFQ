"""Pin: SCPRS scrape daemon writes scprs_pull_log on every cycle, and
the liveness sweep reads it as a daemon-attempt signal independent
of write success.

Chrome MCP audit 2026-05-26 anomaly #9 Phase 3b: the existing SCPRS
liveness check reads scprs_po_master.scraped_at — which only advances
on successful write. A daemon that runs but writes 0 rows looks
identical to a dead daemon. This PR adds:
  1. Daemon writes scprs_pull_log row at end of every cycle (success
     or partial-failure). Already used by the manual pullers; the
     daemon was the missing writer.
  2. New CHECKS entry reads MAX(pulled_at) from scprs_pull_log →
     measures daemon attempts independent of data success.

Together with the existing scprs_po_master.scraped_at check, three
states are now distinguishable: both fresh (healthy), only daemon
fresh (Layer-2 broken — scraper returns empty), both stale (Layer-1
broken — daemon dead).
"""
from __future__ import annotations

from datetime import datetime, timedelta


# ─── CHECKS registry ─────────────────────────────────────────────────


def test_daemon_liveness_check_in_registry():
    """The new CHECKS entry exists with the expected shape: label,
    event_type, callable, threshold."""
    from src.core.liveness_checks import CHECKS
    labels = [c[0] for c in CHECKS]
    assert "SCPRS scrape daemon liveness" in labels, (
        "CHECKS registry missing the SCPRS scrape daemon liveness entry"
    )
    entry = next(c for c in CHECKS if c[0] == "SCPRS scrape daemon liveness")
    label, event_type, check_fn, threshold = entry
    assert event_type == "scprs_pull_failed_persistent"
    assert callable(check_fn)
    # 26h = nightly + 2h grace
    assert threshold == 26 * 3600


def test_daemon_liveness_check_reads_scprs_pull_log(auth_client):
    """When a recent pulled_at exists in scprs_pull_log, the check
    reports a small age (not 10**9 / never-seen sentinel)."""
    from src.core.db import get_db
    from src.core.liveness_checks import CHECKS

    with get_db() as conn:
        # Schema needs to exist — it should from init_db, but ensure here.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scprs_pull_log (
                id INTEGER PRIMARY KEY,
                pulled_at TEXT, search_term TEXT, dept_filter TEXT,
                results_found INTEGER, lines_parsed INTEGER,
                new_pos INTEGER, error TEXT, duration_sec REAL
            )
        """)
        conn.execute("DELETE FROM scprs_pull_log WHERE search_term='fiscal-exhaustive'")
        recent_iso = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
        conn.execute(
            "INSERT INTO scprs_pull_log "
            "(pulled_at, search_term, results_found, lines_parsed, "
            " new_pos, error, duration_sec) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (recent_iso, "fiscal-exhaustive", 0, 0, 0, "", 1.0),
        )

    entry = next(c for c in CHECKS if c[0] == "SCPRS scrape daemon liveness")
    check_fn = entry[2]
    ok, age, detail = check_fn()
    assert ok is True, f"check failed: {detail}"
    # ~30 minutes → 1800s. Allow drift.
    assert 1500 < age < 2200, f"unexpected age {age}, detail={detail}"


def test_daemon_liveness_threshold_distinct_from_data_freshness():
    """The advancement check (26h) is tighter than the existing data-
    freshness check (48h). They measure different things; thresholds
    should not be coupled."""
    from src.core.liveness_checks import CHECKS
    daemon = next(c for c in CHECKS if c[0] == "SCPRS scrape daemon liveness")
    data = next(c for c in CHECKS if c[0] == "SCPRS award scrape")
    assert daemon[3] < data[3], (
        f"daemon-liveness threshold ({daemon[3]}s) must be tighter than "
        f"data-freshness threshold ({data[3]}s); the daemon attempts "
        f"daily so 26h cap detects miss within one cycle"
    )


# ─── Daemon emit-site source check ───────────────────────────────────


def test_daemon_writes_scprs_pull_log_at_end_of_scrape():
    """The daemon emits a row into scprs_pull_log when _run_exhaustive_
    scrape completes. Anchored on source so future refactors that
    silently drop the write are caught."""
    from pathlib import Path
    src = Path(__file__).parent.parent.joinpath(
        "src", "agents", "scprs_browser.py"
    ).read_text(encoding="utf-8")

    idx = src.find("def _run_exhaustive_scrape")
    assert idx > -1
    # The function body must contain an INSERT into scprs_pull_log.
    body_end = src.find("\n\ndef ", idx + 1)
    body = src[idx:body_end if body_end > -1 else idx + 8000]
    assert "INSERT INTO scprs_pull_log" in body, (
        "_run_exhaustive_scrape no longer writes to scprs_pull_log — "
        "the daemon-liveness signal will go silent again"
    )
    # The row must include search_term='fiscal-exhaustive' so the
    # check can attribute it to the daemon vs manual pullers.
    assert '"fiscal-exhaustive"' in body or "'fiscal-exhaustive'" in body, (
        "daemon's scprs_pull_log row must tag search_term='fiscal-exhaustive'"
    )
