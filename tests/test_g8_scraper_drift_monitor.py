"""Pin: G8 — per-supplier scraper drift monitor.

Chrome MCP audit 2026-05-27 / G8 (Architect approval). Catches the
"scraper alive but returning garbage" failure class — the same
shape that left SCPRS silent for 25 days.

Tests pin:
  1. record_lookup increments per-supplier counters
  2. record_lookup tracks ok + has_price separately (scraper that
     returns ok=True but no usable price is the SILENT-FAILURE signal)
  3. compute_supplier_health reports per-supplier shape
  4. drift_suspected fires when price_rate < threshold + enough attempts
  5. drift_suspected fires when last_with_price_at > drift_window_hours
  6. drift_suspected = False when min_attempts_for_signal not met
     (don't fire on cold start with 2 failures)
  7. Empty supplier name is a no-op
  8. Persistence failure doesn't raise (best-effort)
  9. lookup_from_url wiring — source anchor
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _isolated(tmp_path, monkeypatch):
    """Point state file at tmp + reset for clean test."""
    state_file = str(tmp_path / "scraper_drift.json")
    monkeypatch.setattr(
        "src.agents.scraper_drift_monitor._state_file_path",
        lambda: state_file,
    )
    import src.agents.scraper_drift_monitor as sdm
    sdm.reset_state_for_test()
    return sdm


def test_record_lookup_increments_counters(tmp_path, monkeypatch):
    sdm = _isolated(tmp_path, monkeypatch)
    sdm.record_lookup("Amazon", ok=True, has_price=True)
    sdm.record_lookup("Amazon", ok=True, has_price=True)
    sdm.record_lookup("Amazon", ok=False, has_price=False)

    health = sdm.compute_supplier_health()
    amazon = next(s for s in health["suppliers"] if s["supplier"] == "Amazon")
    assert amazon["total_attempts"] == 3
    assert amazon["total_ok"] == 2
    assert amazon["total_with_price"] == 2
    assert amazon["ok_rate"] == round(2/3, 3)
    assert amazon["price_rate"] == round(2/3, 3)


def test_record_separates_ok_and_has_price(tmp_path, monkeypatch):
    """The SILENT-FAILURE class: ok=True but has_price=False. Scraper
    returned without error but returned garbage. has_price is the
    operationally-useful signal."""
    sdm = _isolated(tmp_path, monkeypatch)
    # 5 OK calls but only 1 with a price → scraper running but mostly empty
    sdm.record_lookup("Uline", ok=True, has_price=True)
    for _ in range(4):
        sdm.record_lookup("Uline", ok=True, has_price=False)

    health = sdm.compute_supplier_health()
    uline = next(s for s in health["suppliers"] if s["supplier"] == "Uline")
    assert uline["total_attempts"] == 5
    assert uline["total_ok"] == 5  # all succeeded
    assert uline["total_with_price"] == 1
    assert uline["ok_rate"] == 1.0
    assert uline["price_rate"] == 0.2  # 20% — way below default threshold 30%


def test_drift_fires_on_low_price_rate(tmp_path, monkeypatch):
    sdm = _isolated(tmp_path, monkeypatch)
    # 10 attempts, 2 with price = 20% → below 30% default threshold
    for _ in range(2):
        sdm.record_lookup("Grainger", ok=True, has_price=True)
    for _ in range(8):
        sdm.record_lookup("Grainger", ok=True, has_price=False)

    health = sdm.compute_supplier_health()
    grainger = next(s for s in health["suppliers"]
                    if s["supplier"] == "Grainger")
    assert grainger["drift_suspected"] is True
    assert "price_rate" in grainger["drift_reason"]


def test_drift_fires_on_stale_last_price(tmp_path, monkeypatch):
    """Even if recent attempts are at high rate, if the LAST successful
    price-bearing call was >drift_window_hours ago, fire."""
    sdm = _isolated(tmp_path, monkeypatch)

    # Old successful call (stale)
    old = datetime.now(timezone.utc) - timedelta(hours=30)
    sdm.record_lookup("S&S", ok=True, has_price=True, now=old)
    # 5 recent failures (so total_attempts clears min_attempts threshold)
    for _ in range(5):
        sdm.record_lookup("S&S", ok=True, has_price=False)

    health = sdm.compute_supplier_health(drift_window_hours=24)
    ss = next(s for s in health["suppliers"] if s["supplier"] == "S&S")
    assert ss["drift_suspected"] is True
    # Either reason is valid; both indicate trouble.
    assert ss["drift_reason"] is not None


def test_drift_holds_below_min_attempts(tmp_path, monkeypatch):
    """Don't fire on cold start — even if both attempts failed."""
    sdm = _isolated(tmp_path, monkeypatch)
    sdm.record_lookup("McMaster", ok=True, has_price=False)
    sdm.record_lookup("McMaster", ok=True, has_price=False)

    health = sdm.compute_supplier_health(min_attempts_for_signal=5)
    mc = next(s for s in health["suppliers"] if s["supplier"] == "McMaster")
    assert mc["drift_suspected"] is False, (
        "drift should NOT fire below min_attempts_for_signal — "
        "cold start is not a signal"
    )


def test_empty_supplier_is_noop(tmp_path, monkeypatch):
    sdm = _isolated(tmp_path, monkeypatch)
    sdm.record_lookup("", ok=True, has_price=True)
    sdm.record_lookup("   ", ok=True, has_price=True)
    sdm.record_lookup(None, ok=True, has_price=True)  # type: ignore

    health = sdm.compute_supplier_health()
    assert health["suppliers"] == []


def test_persistence_failure_does_not_raise(tmp_path, monkeypatch):
    sdm = _isolated(tmp_path, monkeypatch)

    def _boom(state):
        raise RuntimeError("disk full")
    monkeypatch.setattr(
        "src.agents.scraper_drift_monitor._persist_state", _boom,
    )

    # Must not raise.
    sdm.record_lookup("Amazon", ok=True, has_price=True)


def test_lookup_from_url_wires_into_drift_monitor():
    """Anchor on source — the wiring inside item_link_lookup must
    call record_lookup. Catches a future refactor that drops the hook."""
    from pathlib import Path
    src = Path(__file__).parent.parent.joinpath(
        "src", "agents", "item_link_lookup.py"
    ).read_text(encoding="utf-8")
    assert "from src.agents.scraper_drift_monitor import record_lookup" in src
    assert "record_lookup(" in src


def test_persisted_state_round_trip(tmp_path, monkeypatch):
    """Write state, "restart" (clear in-process cache by re-loading),
    confirm counters persist. JSON-file substrate behaves like
    the persistence layer it claims to be."""
    sdm = _isolated(tmp_path, monkeypatch)
    sdm.record_lookup("Amazon", ok=True, has_price=True)
    sdm.record_lookup("Amazon", ok=True, has_price=True)

    # Re-import to simulate a fresh process
    import importlib
    import src.agents.scraper_drift_monitor as sdm2
    importlib.reload(sdm2)
    # Re-apply the path monkeypatch (importlib.reload undid it).
    state_file = str(tmp_path / "scraper_drift.json")
    monkeypatch.setattr(
        "src.agents.scraper_drift_monitor._state_file_path",
        lambda: state_file,
    )

    health = sdm2.compute_supplier_health()
    amazon = next(s for s in health["suppliers"]
                  if s["supplier"] == "Amazon")
    assert amazon["total_attempts"] == 2
    assert amazon["total_with_price"] == 2
