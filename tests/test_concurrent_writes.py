"""V2 Infrastructure — Concurrent Write Stress Test.

Simulates 5 simultaneous PC creates + quote counter increments
to verify SQLite WAL mode handles concurrent writes without:
- Database lock errors
- Data corruption
- Quote counter collisions
- Missing records

This is the test that validates the production deployment model:
1 Gunicorn worker, 4 threads, SQLite WAL, busy_timeout=30s.
"""
import json
import os
import sqlite3
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestConcurrentDbWrites:
    """Simulate concurrent writes to SQLite in WAL mode."""

    def test_concurrent_inserts_no_lock_error(self, temp_data_dir):
        """5 threads inserting simultaneously should not raise 'database is locked'."""
        db_path = os.path.join(temp_data_dir, "reytech.db")

        # Ensure table exists
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS test_concurrent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id INTEGER, value TEXT, created_at TEXT)""")
        conn.commit()
        conn.close()

        errors = []
        results = []
        barrier = threading.Barrier(5)  # All threads start at the same time

        def writer(thread_id):
            try:
                barrier.wait(timeout=5)
                c = sqlite3.connect(db_path, timeout=30)
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA busy_timeout=30000")
                for i in range(10):
                    c.execute(
                        "INSERT INTO test_concurrent (thread_id, value, created_at) VALUES (?, ?, ?)",
                        (thread_id, f"value_{thread_id}_{i}", time.time()))
                    c.commit()
                c.close()
                results.append(thread_id)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Concurrent write errors:\n" + "\n".join(errors)
        assert len(results) == 5, f"Only {len(results)}/5 threads completed"

        # Verify all 50 rows written
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM test_concurrent").fetchone()[0]
        conn.close()
        assert count == 50, f"Expected 50 rows, got {count}"

    def test_concurrent_quote_counter_no_collision(self, temp_data_dir):
        """5 threads incrementing quote counter should produce unique sequential numbers."""
        db_path = os.path.join(temp_data_dir, "reytech.db")

        # Initialize counter
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""INSERT OR REPLACE INTO app_settings (key, value, updated_at)
                       VALUES ('quote_counter_seq', '100', datetime('now'))""")
        conn.commit()
        conn.close()

        assigned_numbers = []
        errors = []
        lock = threading.Lock()
        barrier = threading.Barrier(5)

        def increment(thread_id):
            try:
                barrier.wait(timeout=5)
                for _ in range(3):
                    c = sqlite3.connect(db_path, timeout=30)
                    c.execute("PRAGMA busy_timeout=30000")
                    # Simulate _next_quote_number() pattern:
                    # BEGIN IMMEDIATE → read → increment → commit
                    c.execute("BEGIN IMMEDIATE")
                    row = c.execute(
                        "SELECT value FROM app_settings WHERE key='quote_counter_seq'"
                    ).fetchone()
                    seq = int(row[0]) + 1
                    c.execute(
                        "UPDATE app_settings SET value=?, updated_at=datetime('now') "
                        "WHERE key='quote_counter_seq'", (str(seq),))
                    c.commit()
                    c.close()
                    with lock:
                        assigned_numbers.append(seq)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        threads = [threading.Thread(target=increment, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Counter errors:\n" + "\n".join(errors)

        # All numbers should be unique (no collision)
        assert len(assigned_numbers) == 15, f"Expected 15, got {len(assigned_numbers)}"
        assert len(set(assigned_numbers)) == 15, (
            f"Quote counter collision! {len(set(assigned_numbers))} unique "
            f"out of {len(assigned_numbers)}: {sorted(assigned_numbers)}"
        )

    def test_concurrent_reads_during_write(self, temp_data_dir):
        """Reads should not block on concurrent writes (WAL mode)."""
        db_path = os.path.join(temp_data_dir, "reytech.db")

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        # Seed with data
        conn.execute("""CREATE TABLE IF NOT EXISTS test_read_write (
            id INTEGER PRIMARY KEY, value TEXT)""")
        for i in range(100):
            conn.execute("INSERT INTO test_read_write VALUES (?, ?)", (i, f"val_{i}"))
        conn.commit()
        conn.close()

        read_times = []
        write_errors = []
        read_errors = []

        def slow_writer():
            try:
                c = sqlite3.connect(db_path, timeout=30)
                c.execute("PRAGMA busy_timeout=30000")
                for i in range(100, 200):
                    c.execute("INSERT INTO test_read_write VALUES (?, ?)",
                              (i, f"val_{i}"))
                    c.commit()
                    time.sleep(0.01)  # Slow writes
                c.close()
            except Exception as e:
                write_errors.append(str(e))

        def fast_reader():
            try:
                c = sqlite3.connect(db_path, timeout=30)
                c.execute("PRAGMA busy_timeout=30000")
                start = time.time()
                for _ in range(20):
                    rows = c.execute("SELECT COUNT(*) FROM test_read_write").fetchone()
                    time.sleep(0.01)
                elapsed = time.time() - start
                read_times.append(elapsed)
                c.close()
            except Exception as e:
                read_errors.append(str(e))

        writer = threading.Thread(target=slow_writer)
        readers = [threading.Thread(target=fast_reader) for _ in range(3)]

        writer.start()
        for r in readers:
            r.start()
        writer.join(timeout=30)
        for r in readers:
            r.join(timeout=30)

        assert not write_errors, f"Write errors: {write_errors}"
        assert not read_errors, f"Read errors: {read_errors}"
        assert len(read_times) == 3, "Not all readers completed"
        # Reads should complete reasonably fast (< 5s each)
        for rt in read_times:
            assert rt < 5.0, f"Read took {rt:.1f}s — WAL mode should not block reads"

    def test_price_check_concurrent_creates(self, temp_data_dir):
        """Simulate 5 PCs arriving simultaneously from email poller."""
        db_path = os.path.join(temp_data_dir, "reytech.db")

        errors = []
        created_ids = []
        lock = threading.Lock()
        barrier = threading.Barrier(5)

        def create_pc(thread_id):
            try:
                barrier.wait(timeout=5)
                pc_id = f"pc_thread{thread_id}_{int(time.time()*1000)}"
                c = sqlite3.connect(db_path, timeout=30)
                c.execute("PRAGMA busy_timeout=30000")
                c.execute("""INSERT INTO price_checks
                    (id, created_at, requestor, agency, institution, items,
                     pc_number, status, total_items)
                    VALUES (?, datetime('now'), ?, 'CDCR', 'CSP-Test', '[]',
                            ?, 'parsed', 0)""",
                          (pc_id, f"buyer{thread_id}@test.gov",
                           f"OS-T{thread_id}-Apr"))
                c.commit()
                c.close()
                with lock:
                    created_ids.append(pc_id)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        threads = [threading.Thread(target=create_pc, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"PC create errors:\n" + "\n".join(errors)
        assert len(created_ids) == 5, f"Only {len(created_ids)}/5 PCs created"

        # Verify all 5 are in DB
        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM price_checks WHERE id LIKE 'pc_thread%'"
        ).fetchone()[0]
        conn.close()
        assert count == 5, f"Expected 5 PCs in DB, found {count}"
