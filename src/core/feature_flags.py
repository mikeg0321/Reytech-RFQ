"""
feature_flags.py — Simple feature flag system backed by app_settings table.

Usage:
    from src.core.feature_flags import get_flag, set_flag, all_flags

    if get_flag("pricing_v2", default=False):
        return new_pricing_logic(item)
    else:
        return legacy_pricing(item)
"""
import json
import time
import logging

log = logging.getLogger("reytech.flags")

# In-memory cache with TTL
_cache = {}
_CACHE_TTL = 30  # seconds


def get_flag(name: str, default=None):
    """Get a feature flag value. Cached in memory for 30s."""
    now = time.time()
    if name in _cache:
        val, expires = _cache[name]
        if now < expires:
            return val

    try:
        from src.core.db import get_setting
        raw = get_setting(f"flag:{name}")
        if raw is None:
            _cache[name] = (default, now + _CACHE_TTL)
            return default
        # Parse JSON values (bool, int, string, dict)
        try:
            val = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            val = raw
        _cache[name] = (val, now + _CACHE_TTL)
        return val
    except Exception as e:
        log.debug("get_flag(%s) error: %s", name, e)
        return default


def set_flag(name: str, value, actor: str = "system") -> bool:
    """Set a feature flag. Value can be bool, int, string, or dict."""
    try:
        from src.core.db import set_setting
        serialized = json.dumps(value)
        result = set_setting(f"flag:{name}", serialized)
        # Invalidate cache
        _cache.pop(name, None)
        log.info("Flag set: %s = %s (by %s)", name, serialized[:100], actor)
        return result
    except Exception as e:
        log.error("set_flag(%s) error: %s", name, e)
        return False


def all_flags() -> dict:
    """Get all feature flags."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT key, value, updated_at FROM app_settings WHERE key LIKE 'flag:%'"
        ).fetchall()
        conn.close()
        flags = {}
        for r in rows:
            name = r["key"].replace("flag:", "", 1)
            try:
                flags[name] = {"value": json.loads(r["value"]), "updated_at": r["updated_at"]}
            except (json.JSONDecodeError, TypeError):
                flags[name] = {"value": r["value"], "updated_at": r["updated_at"]}
        return flags
    except Exception as e:
        log.error("all_flags error: %s", e)
        return {}


def delete_flag(name: str) -> bool:
    """Remove a feature flag."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("DELETE FROM app_settings WHERE key = ?", (f"flag:{name}",))
        conn.commit()
        conn.close()
        _cache.pop(name, None)
        return True
    except Exception as e:
        log.error("delete_flag(%s) error: %s", name, e)
        return False
