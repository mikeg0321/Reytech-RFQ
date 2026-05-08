"""Pin S-15 invariant — empty quote_number must NEVER clobber a real one.

Audit 2026-05-07 v2 §S-15 traced the 67-orphan-orders symptom to a
write-side bug: 5+ call sites can pass empty `quote_number` to
`order_dal.save_order`, and the ON CONFLICT clause replaced the
existing non-empty value with the new empty one. Reorders, retry-
match, and partial updates all silently detached orders from their
quotes.

These tests pin:
  1. Initial save with non-empty quote_number stores it correctly.
  2. Re-save with empty quote_number does NOT clobber the existing.
  3. Re-save with whitespace-only quote_number does NOT clobber.
  4. Re-save with a NEW non-empty quote_number DOES update.
  5. routes_orders_full reorder constructor inherits quote_number
     from the source order (no longer hardcoded empty).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestOrderDalQuoteNumberPreserve:
    def test_initial_save_stores_quote_number(self, tmp_path, monkeypatch):
        from src.core import order_dal
        from src.core import db as core_db

        # Point DB at a tmp file
        db_path = tmp_path / "test.db"
        monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
        core_db.init_db()

        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "R26Q-100",
            "agency": "CDCR",
            "total": 100.0,
            "status": "new",
        }, actor="test")

        with core_db.get_db() as conn:
            row = conn.execute(
                "SELECT quote_number FROM orders WHERE id=?", ("oid-1",)
            ).fetchone()
            assert row[0] == "R26Q-100"

    def test_resave_with_empty_does_not_clobber(self, tmp_path, monkeypatch):
        """The exact S-15 bug shape — second save with quote_number=''
        must keep the original quote_number on the row."""
        from src.core import order_dal
        from src.core import db as core_db

        db_path = tmp_path / "test.db"
        monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
        core_db.init_db()

        # First save with a real quote_number.
        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "R26Q-100",
            "total": 100.0,
            "status": "new",
        }, actor="test")

        # Second save with EMPTY quote_number — simulates retry/reorder/
        # partial-update sites. Must NOT clobber the real quote_number.
        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "",
            "total": 100.0,
            "status": "shipped",
        }, actor="test")

        with core_db.get_db() as conn:
            row = conn.execute(
                "SELECT quote_number, status FROM orders WHERE id=?",
                ("oid-1",)
            ).fetchone()
            assert row[0] == "R26Q-100", \
                f"S-15 regression: empty quote_number clobbered to {row[0]!r}"
            assert row[1] == "shipped", "status update should still apply"

    def test_resave_with_whitespace_does_not_clobber(self, tmp_path, monkeypatch):
        from src.core import order_dal
        from src.core import db as core_db

        db_path = tmp_path / "test.db"
        monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
        core_db.init_db()

        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "R26Q-200",
            "total": 100,
            "status": "new",
        }, actor="test")

        # Whitespace-only — also must not clobber per the TRIM in the
        # ON CONFLICT clause.
        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "   ",
            "total": 100,
            "status": "new",
        }, actor="test")

        with core_db.get_db() as conn:
            row = conn.execute(
                "SELECT quote_number FROM orders WHERE id=?", ("oid-1",)
            ).fetchone()
            assert row[0] == "R26Q-200"

    def test_resave_with_new_real_value_does_update(self, tmp_path, monkeypatch):
        """Inverse: if the operator legitimately changes the quote_number
        on an existing order, the new non-empty value MUST update."""
        from src.core import order_dal
        from src.core import db as core_db

        db_path = tmp_path / "test.db"
        monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
        core_db.init_db()

        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "R26Q-100",
            "total": 100,
            "status": "new",
        }, actor="test")

        # Real update — different quote_number, non-empty.
        order_dal.save_order("oid-1", {
            "po_number": "PO-1",
            "quote_number": "R26Q-999",
            "total": 100,
            "status": "new",
        }, actor="test")

        with core_db.get_db() as conn:
            row = conn.execute(
                "SELECT quote_number FROM orders WHERE id=?", ("oid-1",)
            ).fetchone()
            assert row[0] == "R26Q-999"


class TestReorderConstructorInheritsQuoteNumber:
    """Pin the routes_orders_full.py reorder constructor: cloned orders
    must inherit quote_number from the source. Pre-fix this was
    hardcoded "" causing the cloned order to land orphaned."""

    def test_reorder_dict_construction_inherits(self):
        # Read the file and verify the literal "" is gone from the reorder
        # constructor. We're not running the full route; this is a
        # source-level guard so a future edit can't silently revert.
        import pathlib
        src = pathlib.Path("src/api/modules/routes_orders_full.py").read_text(
            encoding="utf-8")
        # The exact bad line that S-15 flagged.
        assert '"quote_number": ""' not in src.split(
            'def api_orders_clone'
        )[0] + src.split('def api_orders_clone')[-1].split(
            '\n        # S-15'
        )[0], (
            "S-15 regression: routes_orders_full has a hardcoded "
            "'\"quote_number\": \"\"' write site"
        )
        # The fix sentinel is present
        assert "S-15 (audit 2026-05-07 v2 §S-15): inherit quote_number" in src
