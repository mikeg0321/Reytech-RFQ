"""Regression: the parallel `purchase_orders` PO-tracking subsystem is gone.

On 2026-04-29 the orphan PO-tracking feature was deleted as part of the
codebase audit (drift surface #2):

  - routes_order_tracking.py (9 routes, 800+ lines, separate email
    inbox poller, parallel CRUD on `purchase_orders` family)
  - po_email_v2.py (only consumer was routes_order_tracking)
  - po_detail.html template
  - 3 nav entries in base.html

The system was never adopted — all 4 tables on prod were empty
(verified: 0/0/0/0 rows). The canonical `orders` table is the working
order system; this parallel one only added drift potential.

These tests fail loudly if a future change accidentally re-introduces
the orphan paths.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_routes_order_tracking_module_deleted():
    """The 9-route module that owned the orphan tables must not return."""
    assert not (ROOT / "src" / "api" / "modules" / "routes_order_tracking.py").exists(), \
        "routes_order_tracking.py was re-introduced. The orphan PO-tracking " \
        "subsystem was deleted 2026-04-29 — keep it gone or formally re-adopt it."


def test_po_email_v2_module_deleted():
    """Only consumer was routes_order_tracking, so this module dies with it."""
    assert not (ROOT / "src" / "core" / "po_email_v2.py").exists(), \
        "po_email_v2.py was re-introduced. Its only consumer was the " \
        "deleted routes_order_tracking.py."


def test_po_detail_template_deleted():
    """Only routes_order_tracking rendered po_detail.html."""
    assert not (ROOT / "src" / "templates" / "po_detail.html").exists(), \
        "po_detail.html was re-introduced. The page it powered is gone."


def test_dashboard_does_not_exec_routes_order_tracking():
    """The exec list in dashboard.py must not load the deleted module."""
    text = _read(ROOT / "src" / "api" / "dashboard.py")
    assert "routes_order_tracking" not in text, \
        "dashboard.py still references routes_order_tracking — exec at boot " \
        "would fail with FileNotFoundError."


def test_dashboard_does_not_call_start_po_poller():
    """The boot-time email poller is gone with the module."""
    text = _read(ROOT / "src" / "api" / "dashboard.py")
    assert "_start_po_poller" not in text, \
        "dashboard.py still calls _start_po_poller, but it was defined " \
        "inside the deleted routes_order_tracking module."


def test_base_html_has_no_po_tracking_nav():
    """All 3 nav entries (top, all_pages, command palette) are gone."""
    text = _read(ROOT / "src" / "templates" / "base.html")
    assert "/po-tracking" not in text, \
        "base.html still has a /po-tracking nav entry — clicking it 404s."
    assert "PO Track" not in text, \
        "base.html still labels a nav entry 'PO Track'."


def test_migration_35_drops_orphan_tables_idempotently():
    """Migration 35 (drop_orphan_po_tracking_tables) uses IF EXISTS so it
    runs cleanly whether the tables are present (prod migration path) or
    already absent (fresh dev installs since PR #671 deleted the schema)."""
    import sqlite3
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        # Pre-create the orphan tables to simulate a long-running prod DB.
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE purchase_orders (id TEXT PRIMARY KEY);
            CREATE TABLE po_emails (id INTEGER PRIMARY KEY);
            CREATE TABLE po_line_items (id INTEGER PRIMARY KEY);
            CREATE TABLE po_status_history (id INTEGER PRIMARY KEY);
        """)
        conn.commit()

        # Find migration 35 in the registry and run it.
        from src.core.migrations import MIGRATIONS
        m = [x for x in MIGRATIONS if x[0] == 35]
        assert len(m) == 1, "migration 35 (drop_orphan_po_tracking_tables) missing"
        version, name, sql = m[0]
        assert name == "drop_orphan_po_tracking_tables"
        conn.executescript(sql)
        conn.commit()

        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "purchase_orders" not in names
        assert "po_emails" not in names
        assert "po_line_items" not in names
        assert "po_status_history" not in names

        # Idempotent: running again on the now-empty schema must succeed.
        conn.executescript(sql)
        conn.commit()
        conn.close()
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_no_app_writes_to_purchase_orders_table():
    """No app code may INSERT/UPDATE/DELETE the `purchase_orders` family.

    The boot-time legacy migration in db.py is allowed (it READS to merge
    legacy rows into canonical `orders`) — that's intentional, not a writer.
    """
    forbidden_writes = (
        "INSERT INTO purchase_orders",
        "UPDATE purchase_orders",
        "DELETE FROM purchase_orders",
        "INSERT INTO po_emails",
        "INSERT INTO po_line_items",
        "INSERT INTO po_status_history",
        "UPDATE po_line_items",
    )
    src_dir = ROOT / "src"
    offenders = []
    for path in src_dir.rglob("*.py"):
        body = _read(path)
        for needle in forbidden_writes:
            if needle in body:
                offenders.append(f"{path.relative_to(ROOT)}: {needle!r}")
    assert not offenders, (
        "App code is writing to the deleted purchase_orders table family:\n  "
        + "\n  ".join(offenders)
    )
