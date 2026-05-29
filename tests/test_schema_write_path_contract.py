"""Schema ↔ write-path contract guard.

Closes the bug class behind the 2026-05-28 flaky-CI failure of
`test_reparse_rfq_merges_templates_across_uploads`:

    data_layer.save_single_rfq failed … table rfqs has no column named
    solicitation_number  →  KeyError: '<rfq_id>'  (three layers downstream)

Root cause: `_save_single_rfq` / `_save_single_pc` INSERT columns that the
base `CREATE TABLE` in `src/core/db.py` does NOT define — they are bolted
on at runtime by `_migrate_columns()` (ALTER). A freshly-built DB is only
writable by its own canonical write path if that migration pass completes.
When it doesn't (e.g. CI xdist worker raced `init_db()`), the write fails
and resurfaces as a meaningless KeyError.

This test asserts the invariant directly: every column the canonical RFQ /
PC write path writes MUST exist on the freshly-built table. If a future
change adds a write-path column without also adding it to the schema (base
CREATE TABLE or _migrate_columns), this fails with a NAMED column — not a
cryptic KeyError 30 lines into some other test.

Column lists are mirrored verbatim from the INSERT statements in
`src/api/data_layer.py` (rfqs: the `_save_single_rfq` INSERT; price_checks:
the `_save_single_pc` INSERT). Keep them in sync if those INSERTs change —
that is the entire point of this guard.
"""
import os
import sqlite3

import pytest


# data_layer.py :: _save_single_rfq  — INSERT OR REPLACE INTO rfqs (...)
RFQ_WRITE_PATH_COLUMNS = {
    "id", "received_at", "agency", "institution", "requestor_name",
    "requestor_email", "rfq_number", "items", "status", "source",
    "email_uid", "notes", "solicitation_number", "due_date",
    "email_subject", "body_text", "form_type", "reytech_quote_number",
    "shipping_option", "shipping_amount", "delivery_location",
    "email_thread_id", "email_message_id", "original_sender",
    "gmail_draft_id", "gmail_message_ids", "gmail_thread_duplicate_of",
    "requirements_json", "updated_at", "data_json",
}

# data_layer.py :: _save_single_pc  — INSERT OR REPLACE INTO price_checks (...)
PC_WRITE_PATH_COLUMNS = {
    "id", "created_at", "requestor", "agency", "institution", "items",
    "source_file", "quote_number", "pc_number", "total_items", "status",
    "email_uid", "email_subject", "due_date", "pc_data", "ship_to",
    "email_thread_id", "email_message_id", "original_sender",
    "gmail_draft_id", "gmail_message_ids", "gmail_thread_duplicate_of",
    "requirements_json", "bundle_id", "data_json",
}


def _table_columns(db_path, table):
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    finally:
        conn.close()
    return {r[1] for r in rows}  # r[1] = column name


@pytest.mark.parametrize("table, write_path_columns", [
    ("rfqs", RFQ_WRITE_PATH_COLUMNS),
    ("price_checks", PC_WRITE_PATH_COLUMNS),
])
def test_base_schema_alone_satisfies_write_path(table, write_path_columns):
    """#1199 substrate fix: a DB built from the base SCHEMA ALONE — with NO
    `_migrate_columns()` pass — must already contain every write-path column.

    The parametrized test below builds via the temp_data_dir fixture, which runs
    init_db() + the migration pass, so it passes whether the columns live in the
    base CREATE TABLE or in _migrate_columns. THIS test isolates the base schema:
    it proves a freshly-built DB is writable by its own canonical write path
    without depending on the ALTER-based migration completing (the latent landmine
    behind the 2026-05-28 flaky CI failure)."""
    import sqlite3
    from src.core.db import SCHEMA
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(SCHEMA)
        actual = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    finally:
        conn.close()
    missing = write_path_columns - actual
    assert not missing, (
        f"base SCHEMA for {table!r} is missing write-path columns {sorted(missing)} "
        f"— a fresh DB built from CREATE TABLE alone would not be writable by "
        f"data_layer's INSERT. Add them to the base CREATE TABLE in src/core/db.py."
    )


@pytest.mark.parametrize("table, write_path_columns", [
    ("rfqs", RFQ_WRITE_PATH_COLUMNS),
    ("price_checks", PC_WRITE_PATH_COLUMNS),
])
def test_table_has_all_write_path_columns(temp_data_dir, table, write_path_columns):
    """The freshly-built test DB's table must contain every column its
    canonical write path inserts. A missing column here is the exact
    schema/write-path divergence that made the ingest test flaky."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    assert os.path.exists(db_path), (
        f"test DB not built at {db_path} — temp_data_dir fixture changed?"
    )
    actual = _table_columns(db_path, table)
    assert actual, f"table {table!r} does not exist in the freshly-built test DB"
    missing = write_path_columns - actual
    assert not missing, (
        f"table {table!r} is missing write-path columns {sorted(missing)} — "
        f"the canonical INSERT in data_layer.py writes them but neither the "
        f"base CREATE TABLE nor _migrate_columns() created them. Add them to "
        f"src/core/db.py (schema or _migrate_columns) so a fresh DB is "
        f"writable by its own write path."
    )
