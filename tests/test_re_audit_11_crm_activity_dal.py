"""RE-AUDIT-11 regression guard.

`api_expand_targets` in routes_crm.py previously wrote to
`crm_activity.json` via a raw `_json.load(open(...))` /
`_json.dump(f, ...)` pair. That bypassed:
  (a) the helpers (`_load_crm_activity` / `_save_crm_activity`) that
      the rest of the CRM module uses — so concurrent writers could
      race-stomp each other's appends.
  (b) the SQLite `activity_log` dual-write inside `_log_crm_activity`
      — so the unified feed missed every expansion-outreach event.

Fix: replace the raw pair with `_log_crm_activity(...)`.

These tests gate the source file so a future hand-edit can't silently
re-introduce the raw write pattern.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_CRM = (
    Path(__file__).resolve().parents[1]
    / "src" / "api" / "modules" / "routes_crm.py"
)


def _source() -> str:
    return ROUTES_CRM.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    """Drop comment-only lines so the guards match against CODE, not docstrings
    or explanatory comments that mention the old pattern."""
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return "\n".join(out)


def test_expansion_outreach_uses_log_crm_activity():
    """api_expansion_outreach must call _log_crm_activity, not raw _json.dump."""
    src = _source()
    # Locate api_expansion_outreach body — up to the next top-level def/route.
    m = re.search(
        r"def api_expansion_outreach\([\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_])",
        src,
    )
    assert m, "api_expansion_outreach function body not located"
    body = m.group(0)
    assert "_log_crm_activity" in body, (
        "RE-AUDIT-11 regression: api_expansion_outreach does not call "
        "_log_crm_activity. Expansion outreach activity will be lost "
        "from both the JSON log (with its load/save locking) and the "
        "SQLite activity_log unified feed."
    )


def test_expansion_outreach_has_no_raw_crm_activity_json_write():
    """api_expansion_outreach must not contain a raw _json.dump against crm_activity.json."""
    src = _source()
    m = re.search(
        r"def api_expansion_outreach\([\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_])",
        src,
    )
    assert m, "api_expansion_outreach function body not located"
    body = _strip_comments(m.group(0))
    # The old pattern: `_json.dump(..., f, ...)` immediately following a
    # `crm_activity.json` path construction.
    assert "crm_activity.json" not in body, (
        "RE-AUDIT-11 regression: api_expand_targets references "
        "crm_activity.json directly in code (not a comment). The file "
        "path must only be touched through _log_crm_activity / the "
        "_load_crm_activity helpers."
    )
    # Belt-and-suspenders: no raw _json.dump in the body at all.
    assert not re.search(r"_json\.dump\s*\(", body), (
        "RE-AUDIT-11 regression: api_expand_targets still contains a "
        "raw `_json.dump(...)` call. Activity log writes must go "
        "through _log_crm_activity."
    )


def test_no_raw_activity_json_writes_anywhere_in_routes_crm():
    """Module-wide guard: no `_json.dump` against crm_activity.json."""
    src = _strip_comments(_source())
    # Match any assignment of an `act_path` / similar that points at
    # crm_activity.json followed by a _json.dump. Keep the check narrow
    # so future read-paths (e.g. line 1153 file-list loop) don't trip it.
    bad = re.search(
        r'crm_activity\.json["\'][\s\S]{0,400}?_json\.dump\s*\(',
        src,
    )
    assert not bad, (
        "RE-AUDIT-11 regression: a routes_crm.py site writes "
        "crm_activity.json directly via _json.dump. Route it through "
        "_log_crm_activity instead so the SQLite activity_log gets "
        "the dual-write and the JSON load/save helpers serialize "
        "concurrent writers."
    )
