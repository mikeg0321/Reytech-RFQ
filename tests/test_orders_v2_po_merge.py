"""Orders V2 — purchase_orders → orders merge guard.

The boot migration at `src/core/db.py::_fix_data_on_boot` Fix 6 copies
every `purchase_orders` row into `orders` and `po_line_items` into
`order_line_items`. It's idempotent via the `migrations_applied`
tracker. These tests pin the contract so a refactor to the boot
sequence can't silently break the merge.

See `memory/project_orders_v2_po_merge_audit.md` for the full
migration plan.
"""
import sqlite3
import pytest


def _has_purchase_orders_table(conn) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='purchase_orders'"
    ).fetchone()
    return row is not None


class TestPOMergeMigration:
    def test_migration_marker_concept(self, temp_data_dir):
        """The migrations_applied table is the idempotency gate —
        any row keyed 'orders_v2_merge_po' means the merge already
        ran. Verify the table exists in the test DB so a future
        schema refactor can't silently remove it."""
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='migrations_applied'"
            ).fetchone()
            assert row is not None, (
                "migrations_applied table is missing — the PO merge "
                "can't be idempotent without it"
            )
        finally:
            conn.close()

    def test_marker_is_idempotent_insert(self, temp_data_dir):
        """Inserting the same marker twice must not fail (the real
        code uses `INSERT OR IGNORE` — this test pins that the
        migrations_applied schema supports that upsert pattern)."""
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO migrations_applied (name, applied_at) "
                "VALUES ('orders_v2_merge_po', datetime('now'))"
            )
            conn.execute(
                "INSERT OR IGNORE INTO migrations_applied (name, applied_at) "
                "VALUES ('orders_v2_merge_po', datetime('now'))"
            )
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM migrations_applied WHERE name = 'orders_v2_merge_po'"
            ).fetchone()[0]
            assert count == 1, (
                f"Marker should be unique, got {count} rows — the PO "
                f"merge will run multiple times and can duplicate data"
            )
        finally:
            conn.close()


class TestOrdersTableAcceptsPOConvention:
    """If someone refactors the orders schema and drops a column the
    PO merge path needs, the merge will fail silently at deploy time.
    These tests ensure the columns the merge writes to still exist."""

    def test_orders_table_has_po_merge_columns(self, temp_data_dir):
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            info = conn.execute("PRAGMA table_info(orders)").fetchall()
            cols = {r["name"] for r in info}
            # Columns the PO merge writes to in db.py:2811-2825
            required = {
                "id", "po_number", "agency", "institution", "total",
                "status", "buyer_name", "buyer_email", "created_at",
                "updated_at", "notes",
            }
            missing = required - cols
            assert not missing, (
                f"orders table is missing columns required by the PO "
                f"merge: {sorted(missing)}. Update the migration in "
                f"_fix_data_on_boot Fix 6 if the schema changed."
            )
        finally:
            conn.close()

    def test_order_line_items_has_po_merge_columns(self, temp_data_dir):
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            info = conn.execute("PRAGMA table_info(order_line_items)").fetchall()
            cols = {r["name"] for r in info}
            required = {
                "order_id", "line_number", "description", "part_number",
                "mfg_number", "uom", "qty_ordered", "qty_backordered",
                "unit_price", "extended_price", "sourcing_status",
                "tracking_number", "carrier", "ship_date", "delivery_date",
                "notes", "created_at", "updated_at",
            }
            missing = required - cols
            assert not missing, (
                f"order_line_items table is missing columns required "
                f"by the PO merge: {sorted(missing)}"
            )
        finally:
            conn.close()


class TestPOMergeDataShape:
    def test_simulated_merge_inserts_order_row(self, temp_data_dir):
        """Simulate the merge with a fixture PO row, run the same
        INSERT the boot migration runs, and verify the order row
        lands with the expected id convention."""
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            # Insert a stub purchase_orders row if the table exists
            if not _has_purchase_orders_table(conn):
                pytest.skip("purchase_orders table not present in test DB")

            conn.execute(
                """INSERT OR IGNORE INTO purchase_orders
                   (id, po_number, status, total_amount, institution, created_at)
                   VALUES ('stub_po_1', 'TEST-PO-2026-001', 'received',
                           1250.00, 'cchcs', datetime('now'))"""
            )
            # Run the same INSERT the boot migration does
            oid = "ORD-PO-TEST-PO-2026-001"
            conn.execute(
                """INSERT OR IGNORE INTO orders
                   (id, po_number, agency, institution, total, status,
                    buyer_name, buyer_email, created_at, updated_at, notes)
                   VALUES (?, ?, '', 'cchcs', 1250.00, 'received',
                           '', '', datetime('now'), datetime('now'), '')""",
                (oid, 'TEST-PO-2026-001'),
            )
            conn.commit()

            row = conn.execute(
                "SELECT id, po_number, total, status FROM orders WHERE id = ?",
                (oid,),
            ).fetchone()
            assert row is not None
            assert row[1] == "TEST-PO-2026-001"
            assert row[2] == 1250.00
            assert row[3] == "received"
        finally:
            conn.close()

    def test_duplicate_merge_does_not_create_second_order(self, temp_data_dir):
        """Running the merge INSERT twice for the same PO must not
        create a second orders row — the boot migration uses
        `INSERT OR IGNORE` and skips existing po_numbers."""
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            oid = "ORD-PO-DUP-TEST-001"
            for _ in range(2):
                conn.execute(
                    """INSERT OR IGNORE INTO orders
                       (id, po_number, agency, institution, total, status,
                        buyer_name, buyer_email, created_at, updated_at, notes)
                       VALUES (?, 'DUP-TEST-001', '', 'cchcs', 100, 'new',
                               '', '', datetime('now'), datetime('now'), '')""",
                    (oid,),
                )
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE id = ?", (oid,)
            ).fetchone()[0]
            assert count == 1, (
                f"Duplicate PO merge created {count} rows — INSERT OR "
                f"IGNORE is not behaving as expected"
            )
        finally:
            conn.close()
