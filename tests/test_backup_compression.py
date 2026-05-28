"""Pin gzip-compressed backup substrate (Mr. Wolf, 2026-05-28).

Before this PR, both backup writers (`scheduler.run_backup` daily
and `ops_monitor.run_hourly_backup` hourly) wrote uncompressed
`reytech_<ts>.db` files. The 2026-05-28 bloat audit found
`/data/backups` = 20.3 GB / 25 files on a 30 GB / 48.8 GB volume
— each backup snapshot was ~2 GB raw. gzip on a SQLite file is
typically 5-8× → ~250-400 MB per snapshot.

These tests pin:
  1. Both writers produce `.db.gz` files (not `.db`).
  2. The gzip payload round-trips: gunzip → integrity_check passes.
  3. `verify_backup_integrity` decompresses .gz transparently.
  4. The compressed file is meaningfully smaller than raw (sanity
     check; we don't pin a specific ratio to stay portable).
  5. `_rotate_files` accepts a tuple of suffixes so the transition
     period (legacy `.db` + new `.db.gz`) cleans up correctly.

We mock the heartbeat side-effects so tests don't depend on the
scheduler registry. The src+dst DBs are in-memory pre-seeded then
written to temp paths.
"""
from __future__ import annotations

import gzip
import os
import sqlite3
import tempfile

import pytest


def _seed_test_db(path: str) -> None:
    """Create a SQLite DB at `path` with the tables verify_backup_integrity
    insists on (quotes, contacts, price_history, app_settings + the
    quote_counter_seq row). Adds enough rows that gzip can demonstrate
    real compression."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE quotes (id INTEGER PRIMARY KEY, body TEXT);
        CREATE TABLE contacts (id INTEGER PRIMARY KEY, body TEXT);
        CREATE TABLE price_history (id INTEGER PRIMARY KEY, body TEXT);
        CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO app_settings VALUES ('quote_counter_seq', '42');
    """)
    # Repeated-text rows so gzip can do its job (real-prod sqlite
    # files compress 5-8x; this is enough to show a meaningful ratio).
    for i in range(500):
        conn.execute(
            "INSERT INTO quotes (body) VALUES (?)",
            ("R26Q" + str(i) + " " + "lorem ipsum dolor sit amet " * 20,))
    conn.commit()
    conn.close()


def test_hourly_backup_writes_gz_and_verifies(tmp_path, monkeypatch):
    """run_hourly_backup writes a .db.gz and integrity_check passes."""
    # Stage a fake /data dir with a seeded reytech.db
    data_dir = tmp_path
    _seed_test_db(str(data_dir / "reytech.db"))

    # Stub out the heartbeat side-effect so the test doesn't depend on
    # the scheduler registry being initialized.
    import src.core.ops_monitor as om
    monkeypatch.setattr(
        "src.core.scheduler.heartbeat",
        lambda *a, **kw: None,
        raising=False)

    result = om.run_hourly_backup(data_dir=str(data_dir))

    assert result["ok"], f"hourly backup failed: {result}"
    assert result["filename"].endswith(".db.gz"), (
        f"expected .db.gz, got {result['filename']}")
    assert result["integrity"] is True, (
        "compressed backup failed integrity_check")

    backup_path = data_dir / "backups" / "hourly" / result["filename"]
    assert backup_path.exists()

    # And NO leftover .tmp.db
    leftovers = list((data_dir / "backups" / "hourly").glob("*.tmp.db"))
    assert leftovers == [], f"temp DBs not cleaned up: {leftovers}"


def test_hourly_backup_gz_is_smaller_than_raw(tmp_path, monkeypatch):
    """Sanity: the compressed backup is smaller than the source DB."""
    data_dir = tmp_path
    db_path = data_dir / "reytech.db"
    _seed_test_db(str(db_path))
    raw_size = db_path.stat().st_size

    monkeypatch.setattr(
        "src.core.scheduler.heartbeat",
        lambda *a, **kw: None,
        raising=False)
    import src.core.ops_monitor as om
    result = om.run_hourly_backup(data_dir=str(data_dir))
    assert result["ok"]

    backup_path = data_dir / "backups" / "hourly" / result["filename"]
    gz_size = backup_path.stat().st_size
    # Don't pin a ratio (would be flaky across SQLite builds); just
    # require strictly smaller. Real prod 2GB DBs see 5-8x.
    assert gz_size < raw_size, (
        f"gz backup ({gz_size}) is NOT smaller than raw ({raw_size}) — "
        f"compression substrate is broken")


def test_verify_backup_integrity_handles_gz(tmp_path):
    """verify_backup_integrity decompresses .gz transparently."""
    data_dir = tmp_path
    raw_path = data_dir / "src.db"
    _seed_test_db(str(raw_path))

    gz_path = data_dir / "snapshot.db.gz"
    with open(raw_path, "rb") as f_in, \
            gzip.open(gz_path, "wb", compresslevel=6) as f_out:
        f_out.write(f_in.read())

    from src.core.ops_monitor import verify_backup_integrity
    assert verify_backup_integrity(str(gz_path)) is True


def test_verify_backup_integrity_still_handles_raw_db(tmp_path):
    """verify_backup_integrity must NOT regress on legacy uncompressed
    .db files (older backups still on the volume during transition)."""
    data_dir = tmp_path
    raw_path = data_dir / "snapshot.db"
    _seed_test_db(str(raw_path))

    from src.core.ops_monitor import verify_backup_integrity
    assert verify_backup_integrity(str(raw_path)) is True


def test_verify_backup_integrity_detects_corrupt_gz(tmp_path):
    """A truncated .gz file → integrity check returns False, no crash."""
    bad = tmp_path / "broken.db.gz"
    bad.write_bytes(b"\x1f\x8btruncated-not-real-gzip-data")
    from src.core.ops_monitor import verify_backup_integrity
    assert verify_backup_integrity(str(bad)) is False


def test_rotate_files_accepts_suffix_tuple(tmp_path):
    """During the transition both .db and .db.gz coexist. _rotate_files
    must scan both so legacy files age out alongside new ones."""
    # Use a fresh subdir — tmp_path may contain fixture-created dirs
    d = tmp_path / "backups"
    d.mkdir()
    # 4 files spanning both suffixes, mixed timestamps
    for ts, suf in [
        ("20260101_000000", ".db"),
        ("20260102_000000", ".db.gz"),
        ("20260103_000000", ".db"),
        ("20260104_000000", ".db.gz"),
    ]:
        (d / f"reytech_{ts}{suf}").write_bytes(b"x")

    from src.core.ops_monitor import _rotate_files
    # Keep newest 2 (sorted DESC by name → 20260104 + 20260103)
    _rotate_files(str(d), prefix="reytech_",
                  suffix=(".db", ".db.gz"), keep=2)

    remaining = sorted(p.name for p in d.iterdir() if p.is_file())
    assert remaining == [
        "reytech_20260103_000000.db",
        "reytech_20260104_000000.db.gz",
    ]


def test_rotate_files_still_accepts_string_suffix(tmp_path):
    """Back-compat: callers passing a plain string suffix still work."""
    d = tmp_path / "backups"
    d.mkdir()
    for ts in ("20260101_000000", "20260102_000000", "20260103_000000"):
        (d / f"reytech_{ts}.db").write_bytes(b"x")
    (d / "reytech_20260104_000000.db.gz").write_bytes(b"x")  # unrelated

    from src.core.ops_monitor import _rotate_files
    _rotate_files(str(d), prefix="reytech_", suffix=".db", keep=2)

    remaining = sorted(p.name for p in d.iterdir() if p.is_file())
    # The .db.gz is untouched (suffix didn't include it), 2 newest .db kept
    assert "reytech_20260102_000000.db" in remaining
    assert "reytech_20260103_000000.db" in remaining
    assert "reytech_20260104_000000.db.gz" in remaining
    assert "reytech_20260101_000000.db" not in remaining


def test_scheduler_run_backup_writes_gz(tmp_path, monkeypatch):
    """scheduler.run_backup (daily) also writes .db.gz."""
    data_dir = tmp_path
    _seed_test_db(str(data_dir / "reytech.db"))

    monkeypatch.setattr(
        "src.core.scheduler._get_data_dir",
        lambda: str(data_dir),
        raising=False)
    monkeypatch.setattr(
        "src.core.scheduler.heartbeat",
        lambda *a, **kw: None,
        raising=False)

    from src.core.scheduler import run_backup
    result = run_backup(data_dir=str(data_dir))

    assert result["ok"], f"daily backup failed: {result}"
    assert result["filename"].endswith(".db.gz")
    backup_path = data_dir / "backups" / result["filename"]
    assert backup_path.exists()
    leftovers = list((data_dir / "backups").glob("*.tmp.db"))
    assert leftovers == [], f"temp DBs not cleaned up: {leftovers}"
