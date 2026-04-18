"""Tests for scripts/quote_counter_audit.py and quote_counter_install_unique.py."""
import os
import sqlite3
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Build a minimal DB with the tables the audit/install scripts touch."""
    db_path = str(tmp_path / "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE app_settings (
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
        );
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY, quote_number TEXT
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY, rfq_number TEXT
        );
        CREATE TABLE quote_number_ledger (
            quote_number TEXT PRIMARY KEY, assigned_at TEXT, status TEXT
        );
        INSERT INTO app_settings (key, value) VALUES ('quote_counter_seq', '20');
        INSERT INTO app_settings (key, value) VALUES ('quote_counter', '20');
        INSERT INTO app_settings (key, value) VALUES ('quote_counter_year', '2026');
        INSERT INTO app_settings (key, value) VALUES ('quote_counter_last_good', '20');
    """)
    conn.commit()
    conn.close()

    monkeypatch.setattr("src.core.db.DB_PATH", db_path)
    # Some modules import DB_PATH at module load; patch the attribute on the
    # already-imported audit module if it's been loaded.
    import src.core.paths as paths_mod
    monkeypatch.setattr(paths_mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(paths_mod, "QUOTE_COUNTER_PATH", str(tmp_path / "quote_counter.json"))

    # Re-import scripts so they pick up the patched DB_PATH/DATA_DIR
    for mod_name in ("scripts.quote_counter_audit", "scripts.quote_counter_install_unique"):
        sys.modules.pop(mod_name, None)

    return db_path


def _seed_pc(db, pc_id, qn):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO price_checks (id, quote_number) VALUES (?, ?)", (pc_id, qn))
    conn.commit()
    conn.close()


def _seed_rfq(db, rfq_id, qn):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO rfqs (id, rfq_number) VALUES (?, ?)", (rfq_id, qn))
    conn.commit()
    conn.close()


def _seed_ledger(db, qn):
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO quote_number_ledger (quote_number, assigned_at, status) VALUES (?, datetime('now'), 'active')",
        (qn,),
    )
    conn.commit()
    conn.close()


class TestAudit:
    def test_clean_db_returns_zero(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_pc(isolated_db, "pc2", "R26Q12")
        _seed_rfq(isolated_db, "rfq1", "R26Q13")
        _seed_ledger(isolated_db, "R26Q11")
        _seed_ledger(isolated_db, "R26Q12")
        _seed_ledger(isolated_db, "R26Q13")

        from scripts.quote_counter_audit import audit
        rc = audit()
        assert rc == 0

    def test_pc_dupes_detected(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_pc(isolated_db, "pc2", "R26Q11")  # dupe
        from scripts.quote_counter_audit import audit
        rc = audit()
        assert rc == 2

    def test_rfq_dupes_detected(self, isolated_db):
        _seed_rfq(isolated_db, "rfq1", "R26Q11")
        _seed_rfq(isolated_db, "rfq2", "R26Q11")  # dupe
        from scripts.quote_counter_audit import audit
        rc = audit()
        assert rc == 2

    def test_cross_table_collision_detected(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_rfq(isolated_db, "rfq1", "R26Q11")  # same number used by both
        from scripts.quote_counter_audit import audit
        rc = audit()
        assert rc == 2

    def test_audit_writes_report(self, isolated_db, tmp_path):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        from scripts.quote_counter_audit import audit
        audit()
        reports = list(tmp_path.glob("quote_counter_audit_*.json"))
        assert len(reports) == 1, f"Expected one audit report, got {reports}"

    def test_drift_between_sqlite_and_json(self, isolated_db, tmp_path):
        # Write a JSON counter that disagrees with SQLite (seq=20 vs json=99)
        import json
        json_path = tmp_path / "quote_counter.json"
        json_path.write_text(json.dumps({"year": 2026, "seq": 99}))

        from scripts.quote_counter_audit import audit
        audit()
        reports = list(tmp_path.glob("quote_counter_audit_*.json"))
        report = json.loads(reports[0].read_text())
        assert report["drift"] is not None
        assert str(report["drift"]["sqlite_seq"]) == "20"
        assert str(report["drift"]["json_seq"]) == "99"


class TestInstallUnique:
    def test_install_succeeds_on_clean_db(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_rfq(isolated_db, "rfq1", "R26Q12")
        _seed_ledger(isolated_db, "R26Q11")
        _seed_ledger(isolated_db, "R26Q12")

        from scripts.quote_counter_install_unique import install
        rc = install()
        assert rc == 0

        # Indexes exist
        conn = sqlite3.connect(isolated_db)
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_unique_%'"
        ).fetchall()
        conn.close()
        names = {r[0] for r in idx}
        assert "idx_unique_pc_quote_number" in names
        assert "idx_unique_rfq_number" in names

    def test_install_refuses_on_dupes(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_pc(isolated_db, "pc2", "R26Q11")  # dupe blocks install

        from scripts.quote_counter_install_unique import install
        rc = install()
        assert rc == 2

        conn = sqlite3.connect(isolated_db)
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_unique_pc_quote_number'"
        ).fetchall()
        conn.close()
        assert idx == [], "Index should NOT exist when audit fails"

    def test_install_blocks_future_dupe_inserts(self, isolated_db):
        _seed_pc(isolated_db, "pc1", "R26Q11")
        from scripts.quote_counter_install_unique import install
        assert install() == 0

        # Now try to insert a dupe — must fail
        conn = sqlite3.connect(isolated_db)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO price_checks (id, quote_number) VALUES (?, ?)", ("pc2", "R26Q11"))
            conn.commit()
        conn.close()

    def test_drop_removes_indexes(self, isolated_db):
        from scripts.quote_counter_install_unique import install, drop
        assert install() == 0
        assert drop() == 0

        conn = sqlite3.connect(isolated_db)
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_unique_%'"
        ).fetchall()
        conn.close()
        assert idx == []

    def test_force_skips_audit(self, isolated_db):
        # Even with dupes present, --force installs (and SQLite itself rejects the index)
        _seed_pc(isolated_db, "pc1", "R26Q11")
        _seed_pc(isolated_db, "pc2", "R26Q11")

        from scripts.quote_counter_install_unique import install
        rc = install(force=True)
        # SQLite IntegrityError is caught and returns 2
        assert rc == 2
