"""Regression: startup_checks.check_templates must register the same
Jinja filters the live app registers.

Before this fix, `check_templates` created a bare `Environment(loader=...)`
and iterated every template — but the agency_display filter (added in O-8 /
RFQ-3 / PC-14 lowercase-leak fix) is registered on the *app* jinja_env, not
the bare env. Result: on every boot, the startup check reported
`1/11 FAILED — Templates` with "16 errors: analytics.html: No filter
named 'agency_display'" even though the live app rendered those templates
fine.

A startup check that false-fails trains operators to ignore it — so when a
real template bug ships, the alarm has already been dismissed. Fix by
registering the filter inside the check itself.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


def test_check_templates_registers_agency_display_filter():
    """The check_templates function must register agency_display before
    iterating templates — otherwise every |agency_display use false-fails."""
    body = _read("src/core/startup_checks.py")
    assert 'env.filters["agency_display"]' in body, (
        "Startup check no longer registers agency_display filter — "
        "templates using |agency_display will false-fail the Templates check "
        "even though they render fine in the live app."
    )


def test_check_templates_imports_filter_source():
    body = _read("src/core/startup_checks.py")
    assert "from src.core.agency_display import agency_display" in body, (
        "Startup check must import agency_display from the same module "
        "app.py uses — any drift between the two breaks the check's value."
    )


def test_startup_check_templates_passes_locally():
    """End-to-end: run the exact logic check_templates runs. Must return 0
    errors against the real template tree."""
    import sys
    sys.path.insert(0, str(_REPO))
    from jinja2 import Environment, FileSystemLoader
    from src.core.agency_display import agency_display

    tpl_dir = _REPO / "src" / "templates"
    env = Environment(loader=FileSystemLoader(str(tpl_dir)))
    env.filters["agency_display"] = agency_display

    errors = []
    for name in env.list_templates():
        try:
            env.get_template(name)
        except Exception as e:
            errors.append(f"{name}: {e}")

    assert not errors, (
        f"Template check surfaced {len(errors)} errors: {errors[:3]}"
    )
