"""Liveness checks for critical connections + external dependencies.

The class of failure this catches: an agent reports success (no exception,
heartbeat fires) but the downstream-visible OUTPUT is empty or stale.

This is the "Google Drive connection was silently killed and not writing
to DB data" scenario from 2026-05-25 — the failure was discovered by a
separate Claude session, not by the alert pipeline. Mike: "silent failures
are killers — not noise, because they cause extra work."

The substrate principle: truth lives in OUTPUT tables (and credential
env vars), not in agent self-reports. Each check reads either:

  - MAX(timestamp_col) from an output table → assert recent
  - Filesystem mtime of a backup file → assert recent
  - Credential is_configured()-style check → assert present

When a check fails for longer than its max_age_seconds, fire an alert
via send_alert(). CHANNEL_MAP routes the event_type to Telegram (the
WORTHY tier per Mike's 2026-05-25 silent-default directive). Per-check
cooldown_key prevents the hourly sweep from spamming.

Adding a new check: append one tuple to CHECKS. The tuple shape is
intentionally flat — no factory classes, no registry helpers. New check
in 1 line.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Callable

log = logging.getLogger("reytech.liveness")


# ── Check primitives ──────────────────────────────────────────────────────


def _seconds_since_iso(iso_ts: str) -> int:
    """Parse an ISO 8601 timestamp (with or without timezone) and return
    seconds elapsed since. Treats naive datetimes as UTC."""
    if not iso_ts:
        return 10**9  # never seen — effectively infinite age
    s = str(iso_ts).strip()
    # Handle trailing 'Z' that fromisoformat doesn't accept on older Pythons
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return 10**9
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - dt).total_seconds())


def _table_freshness(table: str, ts_col: str) -> Callable:
    """Return a check_fn that reads MAX(ts_col) from `table` and reports
    age. Returns (ok=True, detail="last write Xh ago") on success;
    (ok=False, detail="...") on missing table or empty result. The CHECKS
    list entry's max_age_seconds decides if the age is acceptable —
    this function just reports age, not pass/fail."""
    def _check():
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    f"SELECT MAX({ts_col}) FROM {table}"
                ).fetchone()
            last = row[0] if row else None
        except Exception as e:
            return (False, 10**9, f"query failed on {table}.{ts_col}: {e}")
        if not last:
            return (False, 10**9, f"{table}.{ts_col} is empty (no writes yet)")
        age = _seconds_since_iso(str(last))
        return (True, age, f"last write {age // 60} min ago")
    return _check


def _backup_file_freshness(rel_subdir: str, prefix: str) -> Callable:
    """Check filesystem mtime of the newest file under DATA_DIR/<rel_subdir>
    matching prefix. Reports age of newest file."""
    def _check():
        try:
            from src.core.paths import DATA_DIR
        except ImportError:
            DATA_DIR = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))), "data")
        d = os.path.join(DATA_DIR, rel_subdir)
        if not os.path.isdir(d):
            return (False, 10**9, f"{d} does not exist")
        files = [f for f in os.listdir(d) if f.startswith(prefix)]
        if not files:
            return (False, 10**9, f"no {prefix}* files in {d}")
        files.sort(key=lambda f: os.path.getmtime(os.path.join(d, f)),
                   reverse=True)
        mtime = os.path.getmtime(os.path.join(d, files[0]))
        age = int(time.time() - mtime)
        return (True, age, f"newest {files[0]} ({age // 60} min old)")
    return _check


def _credential_present(*env_vars: str) -> Callable:
    """Check that ALL named env vars are non-empty. Returns ok=True with
    a 0-second 'age' (this check has no time dimension — it's a binary
    presence check). max_age_seconds for these entries should be 1 so any
    missing credential trips the threshold immediately."""
    def _check():
        missing = [v for v in env_vars if not os.environ.get(v, "").strip()]
        if missing:
            return (False, 10**9, f"missing env vars: {', '.join(missing)}")
        return (True, 0, f"configured: {', '.join(env_vars)}")
    return _check


def _gmail_configured() -> Callable:
    """Gmail OAuth credentials present + the API self-reports configured.
    Different from raw env-var check because gmail_api.is_configured()
    encodes the actual contract (client_id + secret + refresh_token all
    present together)."""
    def _check():
        try:
            from src.core import gmail_api
            ok = gmail_api.is_configured()
        except Exception as e:
            return (False, 10**9, f"gmail_api import failed: {e}")
        if not ok:
            return (False, 10**9,
                    "gmail_api.is_configured() returned False — OAuth "
                    "refresh token missing or revoked")
        return (True, 0, "Gmail OAuth credentials present")
    return _check


# ── The check registry — one tuple per critical connection ────────────────
#
# Shape: (label, alert_event, check_fn, max_age_seconds)
#
#   label             — human-readable name, surfaces in the alert body
#   alert_event       — CHANNEL_MAP key; routes via the WORTHY tier today
#   check_fn          — callable returning (ok: bool, age_seconds: int, detail: str)
#   max_age_seconds   — fail if age exceeds this. Use 1 for binary
#                       credential-present checks.
#
# Mike's 2026-05-25 directive: include all connections. The list below
# covers everything the prod app interacts with. MCPs (Gmail/Drive/Calendar/
# Railway in Mike's Claude Code) are NOT in this list — they live on
# Mike's machine, not in prod. A separate local-machine watchdog would
# probe those.


CHECKS = [
    # ── Output-table freshness (the truth lives here) ────────────────
    ("Gmail inbound poller",
     "external_service_disconnected",
     _table_freshness("email_log", "logged_at"),
     2 * 3600),

    ("SCPRS award scrape",
     "scprs_pull_failed_persistent",
     _table_freshness("scprs_po_master", "pulled_at"),
     48 * 3600),

    ("Award tracker cycle",
     "external_service_disconnected",
     _table_freshness("award_tracker_log", "checked_at"),
     12 * 3600),

    ("Competitor intel writes (loss-detection pipeline)",
     "external_service_disconnected",
     _table_freshness("competitor_intel", "found_at"),
     14 * 24 * 3600),  # alert if NO loss events recorded in 2 weeks

    ("Oracle calibration ticks",
     "external_service_disconnected",
     _table_freshness("oracle_calibration", "last_updated"),
     14 * 24 * 3600),

    ("Quote ingestion pipeline",
     "external_service_disconnected",
     _table_freshness("quotes", "created_at"),
     7 * 24 * 3600),  # silent ingest break: no new quote rows in 7 days

    # ── Filesystem backup recency ────────────────────────────────────
    ("SQLite hourly backup",
     "external_service_disconnected",
     _backup_file_freshness("backups/hourly", "reytech_"),
     26 * 3600),  # nightly + grace

    # ── Credential presence (silent missing-config) ──────────────────
    ("Gmail OAuth (silent token loss)",
     "gmail_oauth_expired",
     _gmail_configured(),
     1),

    ("Telegram bot credentials",
     "external_service_disconnected",
     _credential_present("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"),
     1),

    ("Anthropic API key",
     "external_service_disconnected",
     _credential_present("ANTHROPIC_API_KEY"),
     1),
]


# ── The sweep — call once per hour ────────────────────────────────────────


def run_liveness_sweep() -> dict:
    """Walk CHECKS, fire alerts on stale/broken connections.

    Returns a summary dict for /api/health/liveness:
      {
        "ran_at": iso,
        "checks": [{name, ok, age_seconds, max_age_seconds, detail}, ...],
        "alerts_fired": ["Gmail OAuth (silent token loss)", ...],
        "summary": {"pass": N, "fail": M, "total": N+M},
      }

    Best-effort — a check function raising never crashes the sweep.
    Cooldown is daily-bucketed per check label so the hourly sweep
    doesn't spam Telegram.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    results = []
    alerts_fired = []

    try:
        from src.agents.notify_agent import send_alert
    except Exception as e:
        log.error("liveness sweep: notify_agent import failed: %s", e)
        return {"ran_at": now_iso, "checks": [], "alerts_fired": [],
                "error": f"notify_agent import: {e}"}

    for label, event, check_fn, max_age in CHECKS:
        try:
            ok, age, detail = check_fn()
        except Exception as e:
            ok, age, detail = False, 10**9, f"check raised: {e}"
            log.warning("liveness check %s raised: %s", label, e)

        is_stale = (not ok) or (age > max_age)
        results.append({
            "name": label,
            "ok": bool(ok and not is_stale),
            "age_seconds": int(age),
            "max_age_seconds": int(max_age),
            "detail": detail,
            "alert_event": event,
        })

        if is_stale:
            try:
                age_str = (
                    f"{age // 3600}h" if age >= 3600
                    else f"{age // 60}min" if age >= 60
                    else f"{age}s"
                )
                send_alert(
                    event_type=event,
                    title=f"⚠️ {label}: silent {age_str}",
                    body=(
                        f"{label}\n\n"
                        f"Detail: {detail}\n"
                        f"Age: {age_str} (threshold: {max_age // 3600}h)\n"
                        f"Event: {event}\n\n"
                        f"This is a silent-failure alert — the connection "
                        f"or pipeline stopped producing output without "
                        f"raising an exception. Investigate before more "
                        f"downstream work depends on it."
                    ),
                    urgency="warning",
                    cooldown_key=f"liveness:{label}",
                    cooldown_seconds=86400,  # daily-bucketed per IN-14
                    run_async=False,
                )
                alerts_fired.append(label)
            except Exception as e:
                log.warning("liveness alert for %s failed: %s", label, e)

    summary = {
        "ran_at": now_iso,
        "checks": results,
        "alerts_fired": alerts_fired,
        "summary": {
            "pass": sum(1 for r in results if r["ok"]),
            "fail": sum(1 for r in results if not r["ok"]),
            "total": len(results),
        },
    }
    log.info(
        "liveness sweep: %d/%d pass | %d alerts fired",
        summary["summary"]["pass"], summary["summary"]["total"],
        len(alerts_fired),
    )
    return summary
