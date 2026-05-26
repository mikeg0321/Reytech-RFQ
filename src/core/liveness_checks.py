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
from typing import Callable, Optional

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


def _quote_ingestion_freshness() -> Callable:
    """Quote ingestion check: read newest write across the legacy `quotes`
    table AND the Spine `spine_quotes` table. The Spine is canonical per
    §0 LAW 1 — new quotes flow through `src/spine/db.py` → `spine_quotes`,
    NOT the legacy `quotes` table. Reading `quotes.created_at` alone
    triggered the 2026-05-25 false alarm (~11d silent) because the
    canonical Spine table has been carrying every recent ingest.

    Same shape as the SCPRS sourcing fix and the empty-oracle bug
    (PR #1076) — KPI sourcing the non-canonical substrate column.

    ⚠ DUAL-READ INTERIM SHIM — DELETION TICKET BELOW ⚠

    This helper is a temporary multi-substrate reader pending legacy
    `quotes`-table deletion. It MUST collapse to a single-table read
    on `spine_quotes` only when both conditions hold:

      1. **Job #1 (CCHCS Spine migration, due 2026-06-18)** lands the
         deletion commit for the CCHCS legacy quote-write paths AND
         3 consecutive clean CCHCS Spine ships have been logged.
         At that point CCHCS no longer writes to `quotes`.

      2. **Subsequent agency migrations** (CalVet → DSH → DGS) each
         repeat the same delete-the-legacy-writer step. The last
         agency to migrate ends `quotes`-table writes entirely.

    Acceptance for collapse: `grep -rn 'INTO quotes' src/` finds zero
    callsites. At that point: delete the `"quotes"` branch from the
    `for table in (...)` loop below, delete this docstring section,
    rename the helper to `_spine_quote_ingestion_freshness`. Do NOT
    collapse early — keeping the dual-read while legacy still writes
    masks real silences.

    Audit Item 8 (2026-05-26 back-window pass): named the deletion
    gate explicitly so this shim cannot ossify into a permanent
    multi-substrate reader.
    """
    def _check():
        import os
        import sqlite3
        from src.core.db import get_db
        best_age = 10**9
        details = []

        # ── Legacy `quotes` lives in the dashboard DB (data/reytech.db) ──
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT MAX(created_at) FROM quotes"
                ).fetchone()
                last = row[0] if row else None
            if not last:
                details.append("quotes: empty")
            else:
                age = _seconds_since_iso(str(last))
                details.append(f"quotes: {age // 60}min")
                if age < best_age:
                    best_age = age
        except Exception as e:
            details.append(f"quotes: query failed ({e})")

        # ── Spine `spine_quotes` lives in a SEPARATE DB (data/spine.db) ──
        # Reading via the legacy get_db() returns `no such table:
        # spine_quotes` because Spine has its own substrate per §0 LAW 1.
        # Mirror the path resolution in routes_spine.py:1665-1677 — env
        # override, then DATA_DIR/spine.db, last-ditch cwd fallback.
        # Pre-fix (2026-05-25 PR #1088 → 2026-05-26): both reads hit the
        # legacy connection; spine_quotes always returned "no such table"
        # and the check fell back to the legacy `quotes` age alone,
        # masking the canonical Spine substrate entirely. Same substrate-
        # singleness class as PRs #1076 / #1086 / #1088.
        spine_db_path = os.environ.get("SPINE_DB_PATH")
        if not spine_db_path:
            try:
                from src.core.paths import DATA_DIR
                spine_db_path = os.path.join(str(DATA_DIR), "spine.db")
            except Exception:
                spine_db_path = os.path.join(
                    os.getcwd(), "data", "spine.db"
                )
        try:
            with sqlite3.connect(spine_db_path, timeout=5.0) as spine_conn:
                row = spine_conn.execute(
                    "SELECT MAX(created_at) FROM spine_quotes"
                ).fetchone()
                last = row[0] if row else None
            if not last:
                details.append("spine_quotes: empty")
            else:
                age = _seconds_since_iso(str(last))
                details.append(f"spine_quotes: {age // 60}min")
                if age < best_age:
                    best_age = age
        except Exception as e:
            details.append(f"spine_quotes: query failed ({e})")

        if best_age >= 10**9:
            return (False, 10**9,
                    "no source had data — " + "; ".join(details))
        return (True, best_age,
                f"newest write {best_age // 60} min ago ({'; '.join(details)})")
    return _check


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


def _multi_source_freshness(*sources: tuple) -> Callable:
    """Take freshness across multiple (table, ts_col) sources — youngest wins.

    Use this when a single conceptual event is recorded across more than one
    column or table, e.g. SCPRS PO ingest writes `pulled_at` from one writer
    path and `scraped_at` from another. Reading only one column makes the
    check fire false alarms while the other writer is current.

    The 2026-05-25 silent-failure sweep surfaced this as the SCPRS "24-day
    silent" alert: the liveness check read `scprs_po_master.pulled_at` only
    (4 manual/API writers), but the lone *scheduled* writer
    (`scprs_browser._store_results`) wrote `scraped_at`. Same shape as the
    empty oracle bug (PR #1076) — KPI sourcing wrong substrate column.
    See [[feedback_kpi_substrate_singleness]] and
    [[feedback_audit_all_seams_before_fix]].

    Args:
        *sources: (table, ts_col) pairs to combine, e.g.
            ("scprs_po_master", "pulled_at"),
            ("scprs_po_master", "scraped_at")

    Returns:
        (ok, age_seconds, detail) — age is the SMALLEST across sources that
        returned a value. Sources that error or are empty are noted in
        detail but don't fail the check unless ALL sources are empty/erroring.

    ⚠ MULTI-COLUMN INTERIM SHIM — DELETION TICKET BELOW ⚠

    The primary live call site reads SCPRS as
    `_multi_source_freshness(("scprs_po_master","pulled_at"),
    ("scprs_po_master","scraped_at"))` because the 4 manual/API writer
    paths (`scprs_*_engine` modules) still write `pulled_at` while the
    scheduled scraper writes `scraped_at`. Collapse to a single-column
    read when ONE of these holds:

      1. **SCPRS writer convergence** — all 4 manual/API writers migrate
         to write `scraped_at` (the schedule-canonical column). Owner:
         next session that touches `src/agents/scprs_*_engine.py`. At
         that point: change the call site to single-arg
         `_table_freshness("scprs_po_master", "scraped_at")` and delete
         this helper if it has no other callers.

      2. **The `pulled_at` column is dropped from `scprs_po_master`** in
         a migration (definitive deletion gate).

    Until then: each NEW callsite of `_multi_source_freshness` is itself
    a substrate-singleness smell that should file its own collapse
    ticket per the same pattern.

    Audit Item 8 (2026-05-26 back-window pass): named the convergence
    gates so this shim cannot ossify into permanent N-column reads.
    """
    def _check():
        from src.core.db import get_db
        per_source = []
        best_age = 10**9
        best_label = None
        with get_db() as conn:
            for table, col in sources:
                try:
                    row = conn.execute(
                        f"SELECT MAX({col}) FROM {table}"
                    ).fetchone()
                    last = row[0] if row else None
                except Exception as e:
                    per_source.append(f"{table}.{col}: query failed ({e})")
                    continue
                if not last:
                    per_source.append(f"{table}.{col}: empty")
                    continue
                age = _seconds_since_iso(str(last))
                per_source.append(f"{table}.{col}: {age // 60}min")
                if age < best_age:
                    best_age = age
                    best_label = f"{table}.{col}"
        if best_age >= 10**9:
            return (False, 10**9,
                    "no source had data — " + "; ".join(per_source))
        return (True, best_age,
                f"newest write {best_age // 60} min ago from {best_label} "
                f"({'; '.join(per_source)})")
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
     # Two writer paths feed scprs_po_master: the manual/API pullers
     # (scprs_universal_pull, pull_orchestrator, cchcs_intel_puller,
     # scprs_intelligence_engine) write `pulled_at`; the scheduled browser
     # scrape (scprs_browser._store_results, via schedule_full_fiscal_scrape
     # at 2 AM PST) writes `scraped_at`. Read both so this check reflects
     # whichever path is current. See feedback_kpi_substrate_singleness.
     _multi_source_freshness(
         ("scprs_po_master", "pulled_at"),
         ("scprs_po_master", "scraped_at"),
     ),
     48 * 3600),

    # Chrome MCP audit 2026-05-26 anomaly #9 Phase 3b: daemon-attempt
    # signal, independent of write success. scprs_pull_log gets one row
    # per fiscal-exhaustive cycle (and per manual puller) — read its
    # MAX(pulled_at) to detect "daemon dead" distinct from "daemon ran
    # but wrote 0 rows". 26h threshold = nightly + 2h grace.
    # Together with the SCPRS-award-scrape check above, three states are
    # distinguishable:
    #   • Both fresh        → daemon running + writes succeeding
    #   • Daemon fresh,
    #     award-scrape stale → daemon attempts succeed but the scraper
    #                          itself returns 0 rows (FI$Cal portal
    #                          broken, login lapsed, selectors stale).
    #                          Layer-2 investigation needed.
    #   • Both stale        → daemon dead (thread crashed or never
    #                         scheduled). PR #1115 watchdog should
    #                         catch this within minutes; this is the
    #                         persistent-signal fallback.
    ("SCPRS scrape daemon liveness",
     "scprs_pull_failed_persistent",
     _table_freshness("scprs_pull_log", "pulled_at"),
     26 * 3600),

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
     # Read newest write across legacy `quotes` AND Spine `spine_quotes`.
     # Per §0 LAW 1 the Spine is canonical; reading only `quotes` fires
     # false alarms because new ingest flows through spine_quotes.
     _quote_ingestion_freshness(),
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


# ── PR-B 2026-05-26: env-driven thresholds + recovery close-out ──────────


def _label_to_env_slug(label: str) -> str:
    """Convert a CHECKS label to the env-var slug naming convention.

    "Gmail inbound poller" → "GMAIL_INBOUND_POLLER"
    "SCPRS award scrape"   → "SCPRS_AWARD_SCRAPE"
    "Quote ingestion pipeline" → "QUOTE_INGESTION_PIPELINE"

    Mike can override any threshold without a PR via Railway env, e.g.
    `LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S=86400` bumps the Gmail
    silent-threshold from 2h to 24h.
    """
    out_chars = []
    for ch in (label or "").upper():
        if ch.isalnum():
            out_chars.append(ch)
        else:
            out_chars.append("_")
    slug = "".join(out_chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug


def _threshold_for(label: str, default_s: int) -> int:
    """Read `LIVENESS_<LABEL_SLUG>_MAX_AGE_S` env override; fall back to
    the in-code default. Bad env values (non-int, zero, negative) log
    and fall through to the default — operator typo never breaks the
    sweep."""
    env_key = f"LIVENESS_{_label_to_env_slug(label)}_MAX_AGE_S"
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return default_s
    try:
        v = int(raw)
        if v <= 0:
            log.warning(
                "liveness threshold env override %s=%r is not positive — "
                "falling back to default %ds",
                env_key, raw, default_s,
            )
            return default_s
        return v
    except (TypeError, ValueError):
        log.warning(
            "liveness threshold env override %s=%r is not an int — "
            "falling back to default %ds",
            env_key, raw, default_s,
        )
        return default_s


def _load_liveness_state(label: str) -> Optional[dict]:
    """Read the prior sweep's observation for this label. Returns None
    if this is the first sweep ever (no row yet)."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT last_status, last_alert_at, last_recovered_at, "
                "last_check_at, last_age_seconds, alert_event "
                "FROM liveness_state WHERE label = ?",
                (label,),
            ).fetchone()
        if row is None:
            return None
        if hasattr(row, "keys"):
            return {
                "last_status": row["last_status"],
                "last_alert_at": row["last_alert_at"],
                "last_recovered_at": row["last_recovered_at"],
                "last_check_at": row["last_check_at"],
                "last_age_seconds": row["last_age_seconds"],
                "alert_event": row["alert_event"],
            }
        return {
            "last_status": row[0], "last_alert_at": row[1],
            "last_recovered_at": row[2], "last_check_at": row[3],
            "last_age_seconds": row[4], "alert_event": row[5],
        }
    except Exception as e:
        log.debug("liveness_state load(%s) failed: %s", label, e)
        return None


def _persist_liveness_state(
    label: str, *, status: str, alert_event: str,
    age_seconds: int, fired_alert: bool, fired_recovered: bool,
) -> None:
    """UPSERT the current observation. Best-effort — a write failure
    must not crash the sweep. Preserves prior last_alert_at /
    last_recovered_at when this sweep didn't fire that side."""
    try:
        from src.core.db import get_db
        now = datetime.now(timezone.utc).isoformat()
        prior = _load_liveness_state(label) or {}
        last_alert_at = (
            now if fired_alert
            else (prior.get("last_alert_at") or None)
        )
        last_recovered_at = (
            now if fired_recovered
            else (prior.get("last_recovered_at") or None)
        )
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO liveness_state "
                "(label, last_status, last_alert_at, last_recovered_at, "
                "last_check_at, last_age_seconds, alert_event) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (label, status, last_alert_at, last_recovered_at,
                 now, int(age_seconds), alert_event),
            )
    except Exception as e:
        log.debug("liveness_state persist(%s) failed: %s", label, e)


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

    recovered_fired = []

    for label, event, check_fn, default_max_age in CHECKS:
        # PR-B 2026-05-26: env override per-check (defaults preserved).
        max_age = _threshold_for(label, default_max_age)
        try:
            ok, age, detail = check_fn()
        except Exception as e:
            ok, age, detail = False, 10**9, f"check raised: {e}"
            log.warning("liveness check %s raised: %s", label, e)

        is_stale = (not ok) or (age > max_age)
        current_status = "stale" if is_stale else "ok"

        # PR-B 2026-05-26: transition detection. Prior state lets us
        # fire ONE close-out card when a previously-stale check goes
        # green — Mike sees the alarm cleared without having to check
        # the dashboard. Recovered events are bell-only by override.
        prior = _load_liveness_state(label)
        prior_status = (prior or {}).get("last_status")

        fired_alert = False
        fired_recovered = False

        results.append({
            "name": label,
            "ok": bool(ok and not is_stale),
            "age_seconds": int(age),
            "max_age_seconds": int(max_age),
            "detail": detail,
            "alert_event": event,
            "prior_status": prior_status,
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
                fired_alert = True
            except Exception as e:
                log.warning("liveness alert for %s failed: %s", label, e)
        elif prior_status == "stale":
            # Stale → ok transition: fire ONE bell-only close-out so
            # Mike sees the alarm cleared. channels_override=["bell"]
            # bypasses CHANNEL_MAP — recovery info shouldn't pile on
            # Telegram noise. Uses a separate cooldown key from the
            # stale alert so it can fire independently.
            try:
                send_alert(
                    event_type=f"{event}_recovered",
                    title=f"✓ {label}: recovered",
                    body=(
                        f"{label}\n\n"
                        f"Status: now OK\n"
                        f"Current: {detail}\n"
                        f"Threshold: {max_age // 3600}h\n\n"
                        f"This check was previously stale (see prior "
                        f"alerts for `{event}`); it has now returned "
                        f"fresh. No further action needed."
                    ),
                    urgency="info",
                    channels_override=["bell"],
                    cooldown_key=f"liveness:{label}:recovered",
                    cooldown_seconds=3600,
                    run_async=False,
                )
                recovered_fired.append(label)
                fired_recovered = True
            except Exception as e:
                log.warning(
                    "liveness recovery alert for %s failed: %s", label, e,
                )

        # Persist current observation regardless of fire outcome so the
        # next sweep can detect transitions.
        _persist_liveness_state(
            label,
            status=current_status, alert_event=event, age_seconds=age,
            fired_alert=fired_alert, fired_recovered=fired_recovered,
        )

    summary = {
        "ran_at": now_iso,
        "checks": results,
        "alerts_fired": alerts_fired,
        "recovered_fired": recovered_fired,
        "summary": {
            "pass": sum(1 for r in results if r["ok"]),
            "fail": sum(1 for r in results if not r["ok"]),
            "total": len(results),
        },
    }
    log.info(
        "liveness sweep: %d/%d pass | %d alerts fired | %d recovered",
        summary["summary"]["pass"], summary["summary"]["total"],
        len(alerts_fired), len(recovered_fired),
    )
    return summary
