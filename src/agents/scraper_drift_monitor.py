"""Per-supplier scraper drift monitor.

Chrome MCP audit 2026-05-27 / G8 (Architect approval). Catches the
"scraper alive but returning empty" failure class — the Layer 2
defect that left SCPRS silent for 25 days while the daemon thread
appeared healthy. Same shape applies to every URL scraper:
suppliers change their HTML, our scraper still runs, garbage
results flow through, no alarm fires until missed bids surface.

This module tracks per-supplier lookup outcomes in a JSON state
file (data/scraper_drift_state.json — mirrors gmail_health.json /
notification_cooldowns.json / follow_up_state.json conventions).
Every call to `record_lookup(supplier, ok, has_price)` updates the
rolling success counters. `compute_supplier_health()` returns a
snapshot that liveness checks + future telemetry can consume.

Architectural rules:
  - JSON-only persistence — no new SQLite table.
  - Best-effort writes — a persist failure must NOT crash the
    lookup that triggered the record.
  - Rolling counters — keep total + recent-window separately so
    drift detection compares "recent X" vs "historical baseline."
  - Per-supplier — Amazon's success rate is irrelevant to S&S's;
    one alert per actual problem source.

Step 1 of G8 is just the SUBSTRATE — the storage + record + read
primitives. Step 2 (liveness check that consumes this) is a
follow-up PR.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("reytech.scraper_drift")


_WRITE_LOCK = threading.Lock()


def _state_file_path() -> str:
    """Resolve persistence path. Mirrors DATA_DIR convention used
    by gmail_health.json, follow_up_state.json, etc."""
    try:
        from src.core.paths import DATA_DIR
        return os.path.join(str(DATA_DIR), "scraper_drift_state.json")
    except Exception:
        return os.path.join(os.getcwd(), "data", "scraper_drift_state.json")


def _load_state() -> dict:
    """Load the state JSON. Returns empty dict on missing / corrupt."""
    path = _state_file_path()
    try:
        if not Path(path).exists():
            return {}
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception as e:
        log.debug("scraper_drift: load failed: %s", e)
        return {}


def _persist_state(state: dict) -> None:
    """Atomic write — tmp + replace. Failures logged + swallowed."""
    path = _state_file_path()
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("scraper_drift: persist failed: %s", e)


def record_lookup(
    supplier: str,
    *,
    ok: bool,
    has_price: bool,
    now: Optional[datetime] = None,
) -> None:
    """Record one lookup outcome for `supplier`.

    Increments rolling counters in the per-supplier slot:
      - total_attempts
      - total_ok (ok=True)
      - total_with_price (ok=True AND has_price=True — the
        operationally-useful outcome; an OK scrape with no price
        is a "scraper running, returns garbage" signal)
      - last_attempt_at (ISO)
      - last_ok_at (when ok=True)
      - last_with_price_at (when has_price=True)

    Best-effort: never raises. Caller's lookup is unaffected by
    persistence failures.

    Args:
        supplier:  Canonical supplier label, e.g., "Amazon", "Uline",
                   "Grainger". Free-form from item_link_lookup's
                   detect_supplier().
        ok:        True when the scraper returned without error.
        has_price: True when the result has a positive price field.
                   (ok=True + has_price=False is the SILENT-FAILURE
                   class — scrape succeeded, but with garbage.)
        now:       Optional injected timestamp for tests.
    """
    if not supplier or not supplier.strip():
        return
    s = supplier.strip()
    ts = (now or datetime.now(timezone.utc)).isoformat()

    try:
        with _WRITE_LOCK:
            state = _load_state()
            slot = state.get(s) or {
                "supplier": s,
                "total_attempts": 0,
                "total_ok": 0,
                "total_with_price": 0,
                "last_attempt_at": None,
                "last_ok_at": None,
                "last_with_price_at": None,
            }
            slot["total_attempts"] += 1
            if ok:
                slot["total_ok"] += 1
                slot["last_ok_at"] = ts
            if has_price:
                slot["total_with_price"] += 1
                slot["last_with_price_at"] = ts
            slot["last_attempt_at"] = ts
            state[s] = slot
            _persist_state(state)
    except Exception as e:
        log.debug("scraper_drift: record failed: %s", e)


def compute_supplier_health(
    *,
    drift_window_hours: int = 24,
    min_attempts_for_signal: int = 5,
    drift_threshold: float = 0.30,
    now: Optional[datetime] = None,
) -> dict:
    """Return per-supplier health snapshot for telemetry / liveness.

    Returns:
        {
          "as_of": ISO,
          "suppliers": [
            {
              "supplier": "Amazon",
              "total_attempts": N,
              "total_ok": N,
              "total_with_price": N,
              "ok_rate": float (0..1),
              "price_rate": float (0..1),
              "last_with_price_at": ISO | None,
              "hours_since_last_with_price": int | None,
              "drift_suspected": bool,  # see drift_threshold + window
              "drift_reason": str | None,
            },
            ...
          ],
          "drift_window_hours": ...,
          "drift_threshold": ...,
        }

    Drift detection rule (intentionally conservative — false-positive
    rate matters more than false-negative since this is an alert):
      - drift_suspected=True only when total_attempts >=
        min_attempts_for_signal AND
        (a) price_rate < drift_threshold, OR
        (b) hours_since_last_with_price > drift_window_hours
    """
    state = _load_state()
    ref = now or datetime.now(timezone.utc)
    suppliers = []

    for s, slot in state.items():
        attempts = slot.get("total_attempts", 0) or 0
        ok_count = slot.get("total_ok", 0) or 0
        price_count = slot.get("total_with_price", 0) or 0
        ok_rate = (ok_count / attempts) if attempts > 0 else 0.0
        price_rate = (price_count / attempts) if attempts > 0 else 0.0

        last_price_iso = slot.get("last_with_price_at")
        hours_since: Optional[int] = None
        if last_price_iso:
            try:
                ts = datetime.fromisoformat(
                    last_price_iso.replace("Z", "+00:00")
                    if "Z" in last_price_iso else last_price_iso
                )
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                hours_since = int((ref - ts).total_seconds() // 3600)
            except (ValueError, AttributeError):
                hours_since = None

        drift = False
        drift_reason = None
        if attempts >= min_attempts_for_signal:
            if price_rate < drift_threshold:
                drift = True
                drift_reason = (
                    f"price_rate {price_rate:.2f} < threshold "
                    f"{drift_threshold:.2f} over {attempts} attempts"
                )
            elif hours_since is not None and hours_since > drift_window_hours:
                drift = True
                drift_reason = (
                    f"last_with_price_at was {hours_since}h ago "
                    f"(window {drift_window_hours}h)"
                )

        suppliers.append({
            "supplier": s,
            "total_attempts": attempts,
            "total_ok": ok_count,
            "total_with_price": price_count,
            "ok_rate": round(ok_rate, 3),
            "price_rate": round(price_rate, 3),
            "last_attempt_at": slot.get("last_attempt_at"),
            "last_ok_at": slot.get("last_ok_at"),
            "last_with_price_at": last_price_iso,
            "hours_since_last_with_price": hours_since,
            "drift_suspected": drift,
            "drift_reason": drift_reason,
        })

    return {
        "as_of": ref.isoformat(),
        "suppliers": suppliers,
        "drift_window_hours": drift_window_hours,
        "drift_threshold": drift_threshold,
        "min_attempts_for_signal": min_attempts_for_signal,
    }


def reset_state_for_test() -> None:
    """Test-only — wipes the state file. Mirrors notify_agent's
    _reset_cooldowns_for_test convention."""
    path = _state_file_path()
    try:
        if Path(path).exists():
            os.remove(path)
    except Exception:
        pass


__all__ = ["record_lookup", "compute_supplier_health", "reset_state_for_test"]
