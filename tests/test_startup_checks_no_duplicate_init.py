"""Pins the 2026-05-27 fix: startup_checks.check_db must NOT call
init_db() — that caused a second full schema-creation pass at boot
(~0.5s of wasted work, plus it obscured whether the original init_db
in create_app() actually succeeded).

Prod log evidence:
  04:22:08 [BOOT:DB] init_db: creating schema...  ← first run (app.py)
  04:22:08 [BOOT:DB] init_db: complete
  ...
  04:22:28 [BOOT] create_app() complete ✅ (20.8s)
  04:22:28 [BOOT:DB] init_db: creating schema...  ← SECOND run (this check)
  04:22:28 [BOOT:DB] init_db: complete
  04:22:32 ✅ STARTUP: 11/11 passed

A check is supposed to VERIFY state, not MUTATE it. The sqlite_master
SELECT below is the real check; if init_db failed earlier, tables are
missing and the check fails loudly with a clear message.
"""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "core" / "startup_checks.py"


def test_check_db_does_not_call_init_db():
    """The DB-tables check must not call init_db() — pin against a
    regression that re-introduces the duplicate boot init."""
    src = TARGET.read_text(encoding="utf-8")

    # Locate the check_db function body
    start = src.find("def check_db():")
    assert start > 0, "check_db function not found"
    # Walk forward to next `_check(` or `def ` at the same indent level
    end = src.find("    _check(\"DB tables\"", start)
    assert end > start
    body = src[start:end]

    # Strip comment-only and string lines so the assertion only fires
    # on real code (not the explanatory comment that says "pre-fix this
    # called init_db()").
    code_lines = [
        ln for ln in body.splitlines()
        if ln.lstrip() and not ln.lstrip().startswith("#")
    ]
    code_only = "\n".join(code_lines)

    assert "init_db()" not in code_only, (
        "check_db() must NOT call init_db() — pre-2026-05-27 it did, "
        "causing a duplicate full schema-creation pass at boot. "
        "Verify schema via sqlite_master SELECT instead."
    )
    assert "from src.core.db import init_db" not in code_only, (
        "check_db() must NOT import init_db either — the import alone "
        "is the canary for a regression that re-adds the call."
    )


def test_check_db_fails_loudly_on_missing_db_file():
    """If init_db() failed during create_app(), the DB file won't exist.
    The check must surface this clearly — not silently re-create it."""
    src = TARGET.read_text(encoding="utf-8")
    start = src.find("def check_db():")
    end = src.find("    _check(\"DB tables\"", start)
    body = src[start:end]

    assert "DB file missing" in body, (
        "check_db must explicitly fail with 'DB file missing' when the "
        "file isn't there — not paper over it by calling init_db."
    )
    assert "os.path.exists(db_path)" in body, (
        "check_db must explicitly verify the file exists before opening "
        "sqlite3 — otherwise sqlite3 silently creates an empty DB."
    )


def test_check_db_still_validates_critical_tables():
    """The original purpose — verify critical tables exist — must be
    preserved. Pre-fix this was the second half of the function; we
    can't drop it when we drop the init_db call."""
    src = TARGET.read_text(encoding="utf-8")
    start = src.find("def check_db():")
    end = src.find("    _check(\"DB tables\"", start)
    body = src[start:end]

    # The 8 critical tables that gate further app behavior
    for tbl in (
        "price_checks", "quotes", "contacts", "notifications",
        "workflow_runs", "scprs_po_master", "product_catalog",
        "audit_trail",
    ):
        assert f'"{tbl}"' in body, f"check_db must still verify {tbl} exists"
    assert "SELECT name FROM sqlite_master" in body
