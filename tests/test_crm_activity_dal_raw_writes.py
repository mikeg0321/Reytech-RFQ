"""Regression: admin crm_activity purges route through the DAL.

CR-4 partial (RE-AUDIT-11): two admin "clean stale CRM activity" paths in
routes_pricecheck_admin.py still did raw open/read/filter/open/write against
crm_activity.json instead of routing through _load_crm_activity() +
_save_crm_activity() in dashboard.py.

Why it matters: _load_crm_activity uses _cached_json_load, and _save_crm_activity
calls _invalidate_cache on the path. Raw writes by the admin purge left stale
pre-purge entries in the cache for the rest of the process lifetime — so the
next CRM feed render (follow-up engine, manager agent, unified feed) would
still see the auto_draft rows that the admin had just cleared.

_save_crm_activity also enforces a 5000-entry tail cap. The raw writes skipped
that, so a long-running process that grew past 5000 never trimmed.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_ADMIN = _REPO / "src" / "api" / "modules" / "routes_pricecheck_admin.py"


def _read_admin() -> str:
    return _ADMIN.read_text(encoding="utf-8")


def _strip_comments_and_docstrings(src: str) -> str:
    import re
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out = []
    for line in src.splitlines():
        s = line.lstrip()
        if s.startswith("#"):
            continue
        if " # " in line:
            line = line.split(" # ", 1)[0]
        out.append(line)
    return "\n".join(out)


def test_no_raw_open_against_crm_activity_json():
    """No function body should still do `open(<anything ending crm_activity.json>)`.

    The raw open() pattern was the signal that the DAL was being bypassed.
    """
    body = _strip_comments_and_docstrings(_read_admin())
    # The path construction itself was `os.path.join(DATA_DIR, 'crm_activity.json')`.
    # Flag any line that still binds it to a local we then read/write.
    assert "DATA_DIR, 'crm_activity.json')" not in body and \
           'DATA_DIR, "crm_activity.json")' not in body, (
        "CR-4 regression: routes_pricecheck_admin.py reintroduced a raw path "
        "to crm_activity.json. Use _load_crm_activity() / _save_crm_activity() "
        "from dashboard.py — they invalidate the cache and apply the 5000-row "
        "tail cap."
    )


def test_admin_crm_purges_use_load_and_save_helpers():
    """Both admin 'clean stale CRM activity' paths must call the DAL."""
    body = _strip_comments_and_docstrings(_read_admin())
    load_count = body.count("_load_crm_activity()")
    save_count = body.count("_save_crm_activity(")
    assert load_count >= 3, (
        f"CR-4 regression: expected ≥3 _load_crm_activity() calls in "
        f"routes_pricecheck_admin.py (clean-activity + system-reset + "
        f"reset-and-poll purges), got {load_count}."
    )
    assert save_count >= 3, (
        f"CR-4 regression: expected ≥3 _save_crm_activity(...) calls in "
        f"routes_pricecheck_admin.py, got {save_count}."
    )


def test_dal_helpers_still_exist_in_dashboard():
    """Sanity: the DAL pair the admin paths rely on is still defined."""
    dash = (_REPO / "src" / "api" / "dashboard.py").read_text(encoding="utf-8")
    assert "def _load_crm_activity(" in dash, (
        "DAL regression: _load_crm_activity removed from dashboard.py — "
        "admin purges in routes_pricecheck_admin.py will NameError."
    )
    assert "def _save_crm_activity(" in dash, (
        "DAL regression: _save_crm_activity removed from dashboard.py."
    )


def test_save_helper_preserves_cap_and_cache_invalidation():
    """If someone ever inlines _save_crm_activity, the cap + invalidate must
    come with it. Guard that the shipped helper keeps doing both."""
    dash = (_REPO / "src" / "api" / "dashboard.py").read_text(encoding="utf-8")
    start = dash.find("def _save_crm_activity(")
    assert start >= 0
    next_def = dash.find("\ndef ", start + 1)
    body = dash[start:next_def] if next_def > 0 else dash[start:]
    assert "5000" in body, (
        "DAL regression: _save_crm_activity no longer tail-caps at 5000 rows."
    )
    assert "_invalidate_cache" in body, (
        "DAL regression: _save_crm_activity no longer invalidates the cache "
        "— admin purges will appear successful but stale rows will still "
        "render via _load_crm_activity's cached read."
    )
