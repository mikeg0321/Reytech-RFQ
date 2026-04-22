"""SY-3 regression guard: /api/admin/trim-rfq-files must have rate-limit,
single-flight lock, and audit rows for destructive runs.

Audited 2026-04-22 — the route had @auth_required only. On a 525 MB prod DB,
a double-click queues back-to-back VACUUMs that each hold the exclusive
write lock for tens of seconds, stalling every other writer. No audit row
meant "who trimmed what, when" was invisible.

These tests are source-level guards — CI fails if the decorators or the
single-flight/audit pieces are ripped out.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_HEALTH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_health.py"
)


def _extract_trim_fn(src: str) -> str:
    """Return just the body of the trim_rfq_files route function."""
    m = re.search(
        r'@bp\.route\("/api/admin/trim-rfq-files"[\s\S]*?def trim_rfq_files\(\)[\s\S]*?(?=\n@bp\.route|\nclass |\Z)',
        src,
    )
    assert m, "trim_rfq_files route not found in routes_health.py"
    return m.group(0)


def test_trim_has_rate_limit_decorator():
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_trim_fn(src)
    assert "@rate_limit(" in fn, (
        "SY-3 regression: /api/admin/trim-rfq-files is missing @rate_limit. "
        "Without a rate limit, a stuck admin UI can spam back-to-back "
        "destructive calls."
    )


def test_trim_uses_heavy_tier():
    """Heavy tier (12/min, 10 burst) is right for destructive admin ops —
    not so tight it blocks the operator, not so loose it waves through
    every double-click."""
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_trim_fn(src)
    assert '@rate_limit("heavy")' in fn or "@rate_limit('heavy')" in fn, (
        "SY-3: trim-rfq-files rate_limit tier should be 'heavy'."
    )


def test_trim_has_singleflight_lock():
    """A threading.Lock with acquire(blocking=False) prevents two concurrent
    VACUUMs from stacking on the exclusive write lock."""
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    assert "_TRIM_RFQ_FILES_LOCK" in src, (
        "SY-3 regression: module-level threading.Lock for trim-rfq-files is "
        "missing. Double-click would stack two VACUUMs without it."
    )
    fn = _extract_trim_fn(src)
    assert "acquire(blocking=False)" in fn, (
        "SY-3: lock must be acquired with blocking=False so the second "
        "caller gets 409 immediately instead of queueing behind VACUUM."
    )
    assert "409" in fn, (
        "SY-3: concurrent-call response should be HTTP 409 (Conflict)."
    )


def test_trim_releases_lock_in_finally():
    """The finally block must release the lock even on exception so the
    route never gets permanently blocked."""
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_trim_fn(src)
    assert "finally:" in fn, (
        "SY-3: lock release must be in a finally block."
    )
    assert "_TRIM_RFQ_FILES_LOCK.release()" in fn, (
        "SY-3: the lock must be released on every path out of the route."
    )


def test_trim_writes_audit_rows():
    """Every destructive run must leave a trim_rfq_files_start and
    trim_rfq_files_done audit trail entry."""
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_trim_fn(src)
    assert '"trim_rfq_files_start"' in fn, (
        "SY-3: missing trim_rfq_files_start audit action — ops can't tell "
        "who kicked it off."
    )
    assert '"trim_rfq_files_done"' in fn, (
        "SY-3: missing trim_rfq_files_done audit action — ops can't reconcile "
        "start vs finish, reclaimed space, or partial failures."
    )
    assert "_log_audit_internal" in fn, (
        "SY-3: audit rows must go through _log_audit_internal so they land "
        "in the audit_trail table with IP/UA."
    )


def test_trim_dry_run_skips_lock_and_audit():
    """Dry runs are cheap and safe — they shouldn't contend for the lock
    or dirty the audit trail. Guard that the `if not dry_run` gates are
    present for both."""
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_trim_fn(src)
    assert "if not dry_run and not _TRIM_RFQ_FILES_LOCK.acquire" in fn, (
        "SY-3: lock acquisition must be gated on `not dry_run` — dry runs "
        "should not block each other."
    )
    # Count that the audit calls are gated. Simplest check: the words
    # `if not dry_run:` appear at least twice (start audit, done audit).
    gate_count = len(re.findall(r"if not dry_run:", fn))
    assert gate_count >= 2, (
        f"SY-3: expected `if not dry_run:` to gate start + done audit rows, "
        f"found {gate_count} occurrences."
    )
