"""Tests for scripts/backfill_scprs_category.py

Seeds a reytech.db with the corrupted shape produced by pre-#225 sync
(category = ISO start_date), runs the backfill, and verifies the real
l.category lands — with classify_category() fallback when the source
row is missing.
"""
import importlib
import sqlite3
from pathlib import Path


def _seed_corrupted_db(tmp_path):
    db = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY,
            category TEXT
        );
        CREATE TABLE won_quotes (
            id TEXT PRIMARY KEY,
            description TEXT,
            category TEXT,
            source TEXT
        );
        INSERT INTO scprs_po_lines (id, category) VALUES
            (101, 'Medical/PPE'),
            (102, 'Cleaning');
        -- corrupted rows: category holds the start_date instead of l.category
        INSERT INTO won_quotes (id, description, category, source) VALUES
            ('wq_scprs_101', 'Nitrile gloves', '2026-01-15', 'scprs_sync'),
            ('wq_scprs_102', 'Bleach 1gal',    '2026-01-15', 'scprs_sync'),
            -- clean row: should be left alone
            ('wq_scprs_103', 'Paper towels',   'Janitorial', 'scprs_sync'),
            -- orphan row: source line_id missing → falls back to classify
            ('wq_scprs_999', 'Surgical gauze', '2025-11-01', 'scprs_sync'),
            -- non-scprs row: must not be touched
            ('wq_manual_1',  'Unrelated',      '2026-02-02', 'manual');
        """
    )
    conn.commit()
    conn.close()
    return db


def _reload_backfill(monkeypatch, tmp_path):
    """Point DATA_DIR at tmp so _get_db_conn() opens the seeded db."""
    import src.knowledge.won_quotes_db as wqdb

    monkeypatch.setattr(wqdb, "DATA_DIR", str(tmp_path))
    # Re-import the backfill module under the patched env
    import scripts.backfill_scprs_category as mod
    importlib.reload(mod)
    return mod


def test_dry_run_reports_but_does_not_write(tmp_path, monkeypatch):
    db = _seed_corrupted_db(tmp_path)
    mod = _reload_backfill(monkeypatch, tmp_path)

    stats = mod.backfill(dry_run=True)
    assert stats["scanned"] == 4
    assert stats["affected"] == 3  # 2 ISO + 1 orphan ISO
    assert stats["fixed"] == 3

    # Nothing actually written
    conn = sqlite3.connect(str(db))
    cats = {r[0]: r[1] for r in conn.execute(
        "SELECT id, category FROM won_quotes ORDER BY id"
    )}
    conn.close()
    assert cats["wq_scprs_101"] == "2026-01-15"


def test_apply_rewrites_category_from_source(tmp_path, monkeypatch):
    db = _seed_corrupted_db(tmp_path)
    mod = _reload_backfill(monkeypatch, tmp_path)

    stats = mod.backfill(dry_run=False)
    assert stats["affected"] == 3
    assert stats["fixed"] == 3
    assert stats["source_missing"] == 1  # the orphan row

    conn = sqlite3.connect(str(db))
    cats = {r[0]: r[1] for r in conn.execute(
        "SELECT id, category FROM won_quotes"
    )}
    conn.close()

    # ISO dates replaced with real l.category from scprs_po_lines
    assert cats["wq_scprs_101"] == "Medical/PPE"
    assert cats["wq_scprs_102"] == "Cleaning"
    # Orphan: no source row → classify_category("Surgical gauze") lands a real bucket
    assert cats["wq_scprs_999"] and cats["wq_scprs_999"] != "2025-11-01"
    # Already-clean row untouched
    assert cats["wq_scprs_103"] == "Janitorial"
    # Non-scprs row untouched even if it has an ISO date in category
    assert cats["wq_manual_1"] == "2026-02-02"


def test_idempotent_second_run_is_noop(tmp_path, monkeypatch):
    _seed_corrupted_db(tmp_path)
    mod = _reload_backfill(monkeypatch, tmp_path)

    mod.backfill(dry_run=False)
    stats = mod.backfill(dry_run=False)
    # After a successful first pass, no ISO-date categories remain
    assert stats["affected"] == 0
    assert stats["fixed"] == 0
