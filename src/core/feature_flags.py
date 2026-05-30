"""
feature_flags.py — Thin facade over src.core.flags (the canonical runtime flag
layer introduced in PR #44 / Item C of the P0 resilience backlog).

This module used to store flags in `app_settings` with a `flag:` prefix and its
own 30s cache. That created a split-brain with `src.core.flags` (backed by the
`feature_flags` table and wired to `/api/admin/flags`) — admin-API writes were
invisible to legacy callers, so a flag flip had no effect on them.

Keep importing `from src.core.feature_flags import get_flag, set_flag, ...` —
the calls now resolve through the unified flag table so admin-API writes and
code reads always see the same value. A one-shot boot migration copies any
pre-existing `app_settings WHERE key LIKE 'flag:%'` rows into `feature_flags`
(see `src/core/db.py::_migrate_feature_flags_from_app_settings`). The
temporary dual-read safety net that existed in `flags.get_flag` was removed
on 2026-05-30 — the boot migration is sufficient coverage.

Prefer `from src.core.flags import get_flag` in new code.
"""
from __future__ import annotations

from src.core.flags import (
    delete_flag,
    get_flag,
    list_flags,
    set_flag,
)


def all_flags() -> dict:
    """Legacy shape: {name: {"value": ..., "updated_at": ...}}.

    Kept so existing admin views / scripts that consumed the old dict
    keep working. New code should use `src.core.flags.list_flags()`.
    """
    out: dict = {}
    for row in list_flags():
        out[row.get("key", "")] = {
            "value": row.get("value"),
            "updated_at": row.get("updated_at"),
        }
    return out


__all__ = [
    "get_flag",
    "set_flag",
    "delete_flag",
    "all_flags",
    "list_flags",
]
