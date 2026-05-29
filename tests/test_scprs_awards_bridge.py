"""PR-O — scprs_awards normalization bridge.

The 2026-05-13 forensics surfaced scprs_awards frozen at 2026-03-14 even
though the SCPRS scheduler kept pulling fresh POs into
scprs_po_master/scprs_po_lines. Root cause: `run_scheduled_pulls` did
`pull_agency(...)` but never invoked `build_scprs_awards(conn)` — only
the manual admin endpoints + the `scripts/run_scprs_harvest.py` CLI
called the bridge. With no operator hitting the admin endpoint, the
awards table sat frozen for 60 days while master/lines kept updating.

Pinned guarantees:
  1. `rebuild_intelligence_tables()` exists and is callable from any
     module without sys.path setup at the caller.
  2. Bridge writes scprs_awards rows when scprs_po_master/lines are
     seeded with fresh data.
  3. Bridge is idempotent — re-running on unchanged input is a no-op
     (no duplicate rows, no errors).
  4. `run_scheduled_pulls` calls the rebuild after the pull cycle.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_po(db_path, po_number, supplier, agency, start_date,
             grand_total, dept_name="CCHCS"):
    """Insert a scprs_po_master row + one matching scprs_po_lines row."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO scprs_po_master "
            "(po_number, supplier, supplier_id, agency_key, dept_name, "
            "start_date, grand_total, buyer_email, pulled_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (po_number, supplier, supplier.lower().replace(" ", "_"),
             agency, dept_name, start_date, grand_total,
             "buyer@test.gov", datetime.now(timezone.utc).isoformat()),
        )
        po_id = cur.lastrowid
        conn.execute(
            "INSERT INTO scprs_po_lines "
            "(po_id, po_number, line_num, description, unit_price, "
            "quantity, line_total, category) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (po_id, po_number, 1, "Test item", grand_total, 1,
             grand_total, "Office Supplies"),
        )
        conn.commit()
    finally:
        conn.close()


def _patch_harvest_script(db_path, monkeypatch):
    """`scripts/run_scprs_harvest.py` captures DB_PATH at module import
    time. Force re-import so it picks up our test DB. Also runs the
    intelligence-table migrations (vendor_intel / buyer_intel /
    competitors / scprs_awards / won_quotes_kb) which live in
    `src/core/migrations.py` — the conftest's `temp_data_dir` only
    invokes db.py SCHEMA, not the migrations runner."""
    import sys
    # Ensure scripts dir is importable
    scripts_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "scripts"))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    for modname in list(sys.modules.keys()):
        if modname == "run_scprs_harvest":
            del sys.modules[modname]
    import run_scprs_harvest as _harvest  # type: ignore[import-not-found]
    monkeypatch.setattr(_harvest, "DB_PATH", db_path)
    # Run migrations so vendor_intel + buyer_intel + competitors +
    # scprs_awards + won_quotes_kb exist
    from src.core.migrations import run_migrations
    run_migrations()


def test_rebuild_intelligence_tables_writes_awards(temp_data_dir, monkeypatch):
    """Seed PO data → call rebuild → scprs_awards has 2 new rows."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_po(db_path, "PO-2026-001", "Gorilla Stationers", "cchcs",
             "05/01/2026", 1500.00)
    _seed_po(db_path, "PO-2026-002", "Wynn Innovations", "cchcs",
             "05/02/2026", 2300.00)

    _patch_harvest_script(db_path, monkeypatch)
    from src.agents.scprs_intelligence_engine import rebuild_intelligence_tables
    result = rebuild_intelligence_tables()

    assert result["scprs_awards"] >= 2
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT po_number, vendor_name, total_value "
            "FROM scprs_awards ORDER BY po_number"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) >= 2
    po_numbers = {r[0] for r in rows}
    assert "PO-2026-001" in po_numbers
    assert "PO-2026-002" in po_numbers


def test_rebuild_is_idempotent(temp_data_dir, monkeypatch):
    """Re-running with no new POs doesn't duplicate rows."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_po(db_path, "PO-IDEM-1", "TestVendor", "cchcs", "05/01/2026", 500.00)

    _patch_harvest_script(db_path, monkeypatch)
    from src.agents.scprs_intelligence_engine import rebuild_intelligence_tables

    rebuild_intelligence_tables()
    conn = sqlite3.connect(db_path)
    n1 = conn.execute("SELECT COUNT(*) FROM scprs_awards").fetchone()[0]
    conn.close()

    rebuild_intelligence_tables()
    conn = sqlite3.connect(db_path)
    n2 = conn.execute("SELECT COUNT(*) FROM scprs_awards").fetchone()[0]
    conn.close()

    assert n1 == n2
    assert n1 >= 1


def test_intel_tables_idempotent_no_unbounded_growth(temp_data_dir, monkeypatch):
    """REGRESSION (2026-05-29): vendor_intel / buyer_intel / won_quotes_kb
    have an AUTOINCREMENT id and NO unique business key, so their
    INSERT OR REPLACE/IGNORE never conflicted — every ~30-min scheduler run
    APPENDED the whole aggregate set, bloating reytech.db to 2.4GB (2.76M
    buyer_intel + 2.8M won_quotes_kb rows from a ~2,788-record source) and
    triggering lock-contention crashes. The old idempotency test only
    checked scprs_awards (which was fine — it sets id=po_number), so the
    bloat sailed through. Pin all three: two rebuilds on unchanged input
    must NOT grow the row counts.
    """
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_po(db_path, "PO-BLOAT-1", "VendorX", "cchcs", "05/01/2026", 500.00)
    _seed_po(db_path, "PO-BLOAT-2", "VendorY", "cdcr", "05/02/2026", 750.00)

    _patch_harvest_script(db_path, monkeypatch)
    from src.agents.scprs_intelligence_engine import rebuild_intelligence_tables

    def _counts():
        conn = sqlite3.connect(db_path)
        try:
            return {
                t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("vendor_intel", "buyer_intel", "won_quotes_kb")
            }
        finally:
            conn.close()

    rebuild_intelligence_tables()
    first = _counts()
    rebuild_intelligence_tables()
    rebuild_intelligence_tables()
    third = _counts()

    assert first == third, f"intel tables grew across rebuilds: {first} -> {third}"
    # Sanity: the rebuild actually populated them (not idempotent-because-empty).
    assert all(v >= 1 for v in first.values()), first


def test_rebuild_picks_up_new_pos_added_between_runs(temp_data_dir, monkeypatch):
    """First run normalizes existing POs. Add more. Second run picks up
    only the deltas (idempotent on the originals + 1 new row)."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_po(db_path, "PO-DELTA-1", "VendorA", "cchcs", "05/01/2026", 100.00)

    _patch_harvest_script(db_path, monkeypatch)
    from src.agents.scprs_intelligence_engine import rebuild_intelligence_tables

    rebuild_intelligence_tables()
    conn = sqlite3.connect(db_path)
    n1 = conn.execute("SELECT COUNT(*) FROM scprs_awards").fetchone()[0]
    conn.close()
    assert n1 == 1

    # Simulate next scheduled pull landing a new PO
    _seed_po(db_path, "PO-DELTA-2", "VendorB", "cchcs", "05/02/2026", 200.00)
    rebuild_intelligence_tables()
    conn = sqlite3.connect(db_path)
    n2 = conn.execute("SELECT COUNT(*) FROM scprs_awards").fetchone()[0]
    conn.close()
    assert n2 == 2


def test_run_scheduled_pulls_calls_rebuild(temp_data_dir, monkeypatch):
    """The scheduler hook must invoke rebuild_intelligence_tables after
    the pull cycle. Patch pull_agency so the test doesn't hit SCPRS,
    then assert rebuild fired."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_po(db_path, "PO-SCHED-1", "VendorS", "cchcs", "05/01/2026", 800.00)

    # Inject a due agency so the pull loop has something to iterate.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO scprs_pull_schedule "
            "(agency_key, enabled, next_pull) VALUES (?,?,?)",
            ("cchcs", 1, "2020-01-01T00:00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_harvest_script(db_path, monkeypatch)

    import src.agents.scprs_intelligence_engine as engine
    pulls = []

    def _fake_pull(agency_key, notify_fn=None):
        pulls.append(agency_key)
        return {"ok": True, "new_pos": 0, "new_lines": 0}

    monkeypatch.setattr(engine, "pull_agency", _fake_pull)

    rebuild_calls = []
    orig_rebuild = engine.rebuild_intelligence_tables

    def _spy_rebuild(notify_fn=None):
        rebuild_calls.append(True)
        return orig_rebuild(notify_fn=notify_fn)

    monkeypatch.setattr(engine, "rebuild_intelligence_tables", _spy_rebuild)

    # Repoint _db() so the scheduler reads our test DB
    def _fake_db():
        c = sqlite3.connect(db_path, timeout=30)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(engine, "_db", _fake_db)

    engine.run_scheduled_pulls()

    assert pulls == ["cchcs"], "scheduler should pull due agencies"
    assert rebuild_calls, "scheduler MUST call rebuild_intelligence_tables"


def test_rebuild_handles_empty_db_gracefully(temp_data_dir, monkeypatch):
    """Empty po_master → rebuild should write 0 rows without raising."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _patch_harvest_script(db_path, monkeypatch)
    from src.agents.scprs_intelligence_engine import rebuild_intelligence_tables
    result = rebuild_intelligence_tables()
    assert result["scprs_awards"] == 0
