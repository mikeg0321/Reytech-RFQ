"""Runtime feature flags — Item C of the P0 resilience backlog.

Sub-30-second hotfixes for thresholds, blocklists, and constants without
a deploy. Call `get_flag(key, default)` anywhere in the app; the flag is
read from the `feature_flags` table with a 60s in-memory cache so
repeated calls in a hot loop don't hammer SQLite.

Flag lifecycle:
    1. Code reads `get_flag("pipeline.delivery_threshold", 70)`
    2. Operator POSTs to /api/admin/flags to override
    3. Next read (after 60s cache window) picks up the new value
    4. Optionally DELETE the flag to fall back to the code default

Anti-patterns:
    - Don't use flags for structured config (use reytech_config.json)
    - Don't use flags for per-request state (use session/DB)
    - Don't use flags for long strings or JSON blobs (performance)

Best-for uses (from the backlog):
    - Delivery threshold gates
    - Parser min/max thresholds
    - Blocklists (sender, domain, product)
    - Markup defaults
    - Route enable/disable
    - Gate cutoffs

Every flag read is defensively wrapped — if the DB is unavailable or
the table hasn't migrated yet, `get_flag` returns `default` without
raising. This means the resilience feature can't itself cause an
outage.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

log = logging.getLogger("reytech.flags")


# ── In-memory cache ──────────────────────────────────────────────────────
#
# Per-worker cache (each gunicorn worker has its own process-space cache).
# 60s TTL means operator changes propagate worker-by-worker within a minute.
# That's fast enough for every use case in the backlog spec and avoids
# cross-worker cache invalidation complexity.

_CACHE_TTL_SECONDS = 60
_cache: Dict[str, "_CacheEntry"] = {}
_cache_lock = threading.RLock()


class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: str, expires_at: float):
        self.value = value
        self.expires_at = expires_at


def _cache_get(key: str) -> Optional[str]:
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        if time.time() > entry.expires_at:
            _cache.pop(key, None)
            return None
        return entry.value


def _cache_set(key: str, value: str) -> None:
    with _cache_lock:
        _cache[key] = _CacheEntry(
            value=value,
            expires_at=time.time() + _CACHE_TTL_SECONDS,
        )


def _cache_invalidate(key: str) -> None:
    with _cache_lock:
        _cache.pop(key, None)


def _cache_clear_all() -> None:
    """Test helper — not exposed to the admin API. Workers clear
    their own cache on TTL expiry; if you need cross-worker
    invalidation use a restart."""
    with _cache_lock:
        _cache.clear()


# ── Public API ───────────────────────────────────────────────────────────

def get_flag(key: str, default: Any) -> Any:
    """Return the value of the named flag, or `default` if unset.

    The returned value is coerced to the type of `default`:
        - bool defaults → "1"/"true"/"yes" (case-insensitive) = True
        - int defaults → int parsed, fallback to default on ValueError
        - float defaults → float parsed, fallback to default
        - str defaults → str as-is

    Every read is best-effort: any exception (DB gone, table missing,
    cache corrupted) falls back to `default` without raising. The
    caller can treat this as "flag layer is always available".
    """
    if not key:
        return default

    # Fast path: cache hit
    cached = _cache_get(key)
    if cached is not None:
        return _coerce(cached, default)

    # Slow path: read from DB
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT value FROM feature_flags WHERE key = ?",
                (key,),
            ).fetchone()
    except Exception as e:
        log.debug("get_flag(%s) DB error: %s — using default", key, e)
        return default

    if row is None:
        # Store the default as a cache sentinel so we don't re-hit the
        # DB for 60s on unset flags. But we cache a marker value, not
        # the default itself, so we can distinguish "unset in DB" from
        # "set in DB to empty string".
        _cache_set(key, "\x00__UNSET__\x00")
        return default

    value_str = str(row[0] if not isinstance(row, dict) else row["value"])
    _cache_set(key, value_str)
    return _coerce(value_str, default)


def _coerce(raw: str, default: Any) -> Any:
    """Convert the string value from DB into the type of `default`."""
    if raw == "\x00__UNSET__\x00":
        return default
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on", "y", "t")
    if isinstance(default, int):
        try:
            return int(raw)
        except (ValueError, TypeError):
            return default
    if isinstance(default, float):
        try:
            return float(raw)
        except (ValueError, TypeError):
            return default
    return raw


def set_flag(key: str, value: Any, updated_by: str = "",
             description: str = "") -> bool:
    """Upsert a flag. Invalidates the cache entry so the next read
    returns the new value without waiting for the 60s TTL.

    Returns True on success, False on DB error. Never raises.
    """
    if not key:
        return False
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                """INSERT INTO feature_flags (key, value, updated_by, description,
                                               updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'))
                   ON CONFLICT(key) DO UPDATE SET
                       value = excluded.value,
                       updated_by = excluded.updated_by,
                       description = excluded.description,
                       updated_at = datetime('now')""",
                (key, str(value), updated_by, description),
            )
    except Exception as e:
        log.error("set_flag(%s) DB error: %s", key, e)
        return False
    _cache_invalidate(key)
    log.info("feature flag set: %s = %s (by %s)", key, value, updated_by or "?")
    return True


def delete_flag(key: str) -> bool:
    """Delete a flag so the next read returns its code default.
    Returns True on success (including "flag didn't exist"), False
    on DB error. Never raises."""
    if not key:
        return False
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM feature_flags WHERE key = ?", (key,))
    except Exception as e:
        log.error("delete_flag(%s) DB error: %s", key, e)
        return False
    _cache_invalidate(key)
    log.info("feature flag deleted: %s", key)
    return True


def list_flags() -> list:
    """Return all currently-set flags as a list of dicts. Uncached —
    admin UI operation, not hot path."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT key, value, updated_at, updated_by, description
                   FROM feature_flags ORDER BY key"""
            ).fetchall()
    except Exception as e:
        log.error("list_flags DB error: %s", e)
        return []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
        else:
            out.append({
                "key": row[0],
                "value": row[1],
                "updated_at": row[2],
                "updated_by": row[3],
                "description": row[4] if len(row) > 4 else "",
            })
    return out


__all__ = [
    "get_flag",
    "set_flag",
    "delete_flag",
    "list_flags",
]
