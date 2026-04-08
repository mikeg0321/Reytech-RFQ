"""Tests for quote counter in src/forms/quote_generator.py."""
import os
import sys
import sqlite3
import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


@pytest.fixture
def counter_db(tmp_path, monkeypatch):
    """Create an isolated SQLite DB for counter tests."""
    db_path = str(tmp_path / "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('quote_counter_seq', '10')")
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('quote_counter', '10')")
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('quote_counter_year', '2026')")
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('quote_counter_last_good', '10')")
    conn.commit()
    conn.close()

    # Patch DATA_DIR everywhere so _next_quote_number (via get_db) uses test DB
    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    # Force get_db() to create a fresh connection to the test DB
    db_mod.close_thread_db()

    try:
        import src.forms.quote_generator as qg
        monkeypatch.setattr(qg, "DATA_DIR", str(tmp_path))
    except Exception:
        pass

    return db_path


class TestNextQuoteNumber:
    def test_sequential_increment(self, counter_db):
        """10 sequential calls should produce R26Q11 through R26Q20."""
        from src.forms.quote_generator import _next_quote_number
        numbers = []
        for _ in range(10):
            numbers.append(_next_quote_number())

        # All should be unique
        assert len(set(numbers)) == 10

        # All should be sequential
        seqs = [int(n.split("Q")[1]) for n in numbers]
        assert seqs == list(range(11, 21))

    def test_jump_guardrail(self, counter_db):
        """If seq jumps >5 from last_good, cap it."""
        # Set seq to 100 but last_good to 10
        conn = sqlite3.connect(counter_db)
        conn.execute("UPDATE app_settings SET value='100' WHERE key='quote_counter_seq'")
        conn.commit()
        conn.close()

        from src.forms.quote_generator import _next_quote_number
        result = _next_quote_number()

        # Should be capped at last_good + 1 = 11, not 101
        seq = int(result.split("Q")[1])
        assert seq == 11, f"Expected R26Q11 but got {result} — guardrail didn't fire"

    def test_normal_increment_no_guardrail(self, counter_db):
        """Normal +1 increment should not trigger guardrail."""
        from src.forms.quote_generator import _next_quote_number
        result = _next_quote_number()
        assert result == "R26Q11"


class TestSetQuoteCounter:
    def test_set_updates_last_good(self, counter_db, monkeypatch):
        """set_quote_counter should update last_good."""
        # Patch get_setting/set_setting to use our test DB
        import src.core.db as db_mod

        def _mock_set_setting(key, value):
            conn = sqlite3.connect(counter_db)
            conn.execute("""
                INSERT INTO app_settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            conn.commit()
            conn.close()

        def _mock_get_setting(key, default=None):
            conn = sqlite3.connect(counter_db)
            row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            conn.close()
            return row[0] if row else default

        monkeypatch.setattr(db_mod, "set_setting", _mock_set_setting)
        monkeypatch.setattr(db_mod, "get_setting", _mock_get_setting)

        from src.forms.quote_generator import set_quote_counter
        set_quote_counter(50)

        # Verify last_good was updated
        conn = sqlite3.connect(counter_db)
        row = conn.execute("SELECT value FROM app_settings WHERE key='quote_counter_last_good'").fetchone()
        conn.close()
        assert row[0] == "50"


class TestLoadCounter:
    def test_loads_from_sqlite(self, counter_db, monkeypatch):
        """_load_counter should return seq and year from SQLite."""
        import src.core.db as db_mod

        def _mock_get_setting(key, default=None):
            conn = sqlite3.connect(counter_db)
            row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            conn.close()
            return row[0] if row else default

        monkeypatch.setattr(db_mod, "get_setting", _mock_get_setting)

        from src.forms.quote_generator import _load_counter
        result = _load_counter()
        assert result["seq"] == 10
        assert result["year"] == 2026

    def test_returns_empty_on_failure(self, monkeypatch):
        """If SQLite fails, should return empty dict (no JSON fallback)."""
        import src.core.db as db_mod

        def _mock_get_setting(key, default=None):
            raise Exception("DB unavailable")

        monkeypatch.setattr(db_mod, "get_setting", _mock_get_setting)

        from src.forms.quote_generator import _load_counter
        result = _load_counter()
        assert result == {}


class TestRollbackQuoteNumber:
    def test_rollback_decrements(self, counter_db, monkeypatch):
        """_rollback_quote_number should decrement if counter hasn't advanced."""
        import src.core.db as db_mod

        def _mock_get_setting(key, default=None):
            conn = sqlite3.connect(counter_db)
            row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            conn.close()
            return row[0] if row else default

        def _mock_set_setting(key, value):
            conn = sqlite3.connect(counter_db)
            conn.execute("""
                INSERT INTO app_settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            conn.commit()
            conn.close()

        monkeypatch.setattr(db_mod, "get_setting", _mock_get_setting)
        monkeypatch.setattr(db_mod, "set_setting", _mock_set_setting)

        from src.forms.quote_generator import _next_quote_number, _rollback_quote_number

        qn = _next_quote_number()  # R26Q11
        _rollback_quote_number(qn)

        # Next call should re-issue 11 (rolled back)
        qn2 = _next_quote_number()
        assert qn == qn2, f"Rollback failed: got {qn2} after rolling back {qn}"
