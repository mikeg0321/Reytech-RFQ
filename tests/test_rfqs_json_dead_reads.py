"""Tier 2b — `rfqs.json` dead-read cleanup (audit 2026-05-07).

The audit named `rfqs.json` reads scattered across `data_layer.py` that
no longer reflected truth — SQLite is authoritative; the JSON file was
a one-time migration source. This PR:

  1. Hoisted the migration block out of `load_rfqs()` into a discrete
     `migrate_legacy_rfqs_json_if_present()` boot helper so the hot
     read path is strictly SQLite.
  2. Removed the dead `_invalidate_cache(rfq_db_path())` calls in
     `_save_single_rfq` and `save_rfqs` — neither writes to JSON, so
     invalidating that cache key was a no-op.
  3. Wired the migration into `init_db_deferred()` (background thread)
     so it still runs once at boot for any environment that hasn't yet.

These tests pin all three changes:
  - `load_rfqs()` does NOT touch `rfqs.json` when SQLite has rows
  - `migrate_legacy_rfqs_json_if_present()` correctly imports + renames
    the legacy file ONLY when SQLite is empty
  - Save sites don't reach into the JSON cache anymore
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest


# ── load_rfqs() hot path is now strictly SQLite ────────────────────

def test_load_rfqs_does_not_read_json_when_sqlite_has_rows(seed_db_quote):
    """Pin the hot path: SQLite-populated → JSON file is irrelevant."""
    from src.api import data_layer
    from src.api.data_layer import _save_single_rfq, load_rfqs, rfq_db_path

    _save_single_rfq("rfq_test_a", {"id": "rfq_test_a",
                                     "solicitation_number": "S-A",
                                     "status": "new"})

    json_path = rfq_db_path()
    # Even if a stale rfqs.json exists with different content, load_rfqs
    # must NOT read it. Drop a JSON file with bogus data and watch it
    # be ignored.
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w") as f:
        json.dump({"BOGUS_FROM_JSON": {
            "id": "BOGUS_FROM_JSON",
            "solicitation_number": "BOGUS",
        }}, f)

    rfqs = load_rfqs()
    assert "rfq_test_a" in rfqs
    assert "BOGUS_FROM_JSON" not in rfqs

    # Cleanup
    try:
        os.remove(json_path)
    except OSError:
        pass


def test_load_rfqs_returns_empty_dict_when_sqlite_empty_and_no_json(
        tmp_path, monkeypatch):
    """Empty SQLite + no JSON file → empty dict, no migration attempt."""
    from src.api import data_layer
    from src.api.data_layer import load_rfqs

    # Point rfq_db_path at a directory that does NOT contain rfqs.json
    monkeypatch.setattr(data_layer, "rfq_db_path",
                        lambda: str(tmp_path / "rfqs.json"))
    rfqs = load_rfqs()
    assert rfqs == {}


def test_load_rfqs_imports_legacy_json_when_sqlite_empty(
        tmp_path, monkeypatch):
    """Fresh deploy: SQLite empty + rfqs.json present → migration fires
    once and the result is returned. This is the seed-via-JSON pattern
    used by golden-path test fixtures (`_seed_pc_and_rfq`)."""
    from src.api import data_layer
    from src.api.data_layer import load_rfqs

    legacy = tmp_path / "rfqs.json"
    with open(legacy, "w") as f:
        json.dump({
            "rfq_fresh_001": {
                "id": "rfq_fresh_001",
                "solicitation_number": "S-FRESH",
                "status": "new",
            },
        }, f)
    monkeypatch.setattr(data_layer, "rfq_db_path", lambda: str(legacy))

    rfqs = load_rfqs()
    assert "rfq_fresh_001" in rfqs
    # Migration helper renamed the source on success
    assert (tmp_path / "rfqs.json.migrated").exists()


# ── migrate_legacy_rfqs_json_if_present() boot helper ──────────────

def test_migrate_legacy_no_ops_when_sqlite_already_populated(
        seed_db_quote, tmp_path, monkeypatch):
    """SQLite has rows → migration MUST NOT touch the JSON file."""
    from src.api import data_layer
    from src.api.data_layer import (_save_single_rfq,
                                     migrate_legacy_rfqs_json_if_present)

    _save_single_rfq("rfq_existing", {"id": "rfq_existing",
                                       "solicitation_number": "S-EX"})

    legacy = tmp_path / "rfqs.json"
    with open(legacy, "w") as f:
        json.dump({"RFQ_FROM_JSON": {"id": "RFQ_FROM_JSON"}}, f)
    monkeypatch.setattr(data_layer, "rfq_db_path", lambda: str(legacy))

    result = migrate_legacy_rfqs_json_if_present()
    assert result == {}
    assert legacy.exists()  # NOT renamed since migration didn't run
    assert not (tmp_path / "rfqs.json.migrated").exists()


def test_migrate_legacy_imports_when_sqlite_empty(tmp_path, monkeypatch):
    """SQLite empty + rfqs.json present → import + rename to .migrated."""
    from src.api import data_layer
    from src.api.data_layer import migrate_legacy_rfqs_json_if_present
    from src.core.db import get_db

    legacy = tmp_path / "rfqs.json"
    with open(legacy, "w") as f:
        json.dump({
            "rfq_legacy_001": {
                "id": "rfq_legacy_001",
                "solicitation_number": "S-LEG-001",
                "status": "new",
            },
            "rfq_legacy_002": {
                "id": "rfq_legacy_002",
                "solicitation_number": "S-LEG-002",
                "status": "new",
            },
        }, f)
    monkeypatch.setattr(data_layer, "rfq_db_path", lambda: str(legacy))

    result = migrate_legacy_rfqs_json_if_present()
    assert "rfq_legacy_001" in result
    assert "rfq_legacy_002" in result

    # Side-effect 1: rows landed in SQLite
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id FROM rfqs WHERE id LIKE 'rfq_legacy%'"
        ).fetchall()
    assert len(rows) == 2

    # Side-effect 2: source file was renamed so this never re-runs
    assert not legacy.exists()
    assert (tmp_path / "rfqs.json.migrated").exists()


def test_migrate_legacy_no_ops_when_no_json_file(tmp_path, monkeypatch):
    """Empty SQLite + missing JSON → return {}, do nothing."""
    from src.api import data_layer
    from src.api.data_layer import migrate_legacy_rfqs_json_if_present

    monkeypatch.setattr(data_layer, "rfq_db_path",
                        lambda: str(tmp_path / "definitely_does_not_exist.json"))
    result = migrate_legacy_rfqs_json_if_present()
    assert result == {}


def test_migrate_legacy_handles_corrupt_json_without_raising(
        tmp_path, monkeypatch):
    """A corrupt rfqs.json must not crash boot — fail-open with {}."""
    from src.api import data_layer
    from src.api.data_layer import migrate_legacy_rfqs_json_if_present

    legacy = tmp_path / "rfqs.json"
    legacy.write_text("{ this is not valid json")
    monkeypatch.setattr(data_layer, "rfq_db_path", lambda: str(legacy))

    result = migrate_legacy_rfqs_json_if_present()
    assert result == {}
    # Source NOT renamed — failure mode preserves operator visibility
    assert legacy.exists()


# ── Save sites don't touch the JSON cache anymore ──────────────────

def test_save_single_rfq_does_not_invalidate_json_cache(seed_db_quote):
    """Pin the dead-read deletion: _save_single_rfq must NOT call
    `_invalidate_cache(rfq_db_path())`. The cache key was for a JSON
    file no one writes; the call was a true no-op but a confusing
    code-read hint that JSON was still part of the path.
    """
    from src.api import data_layer
    from src.api.data_layer import _save_single_rfq

    with patch.object(data_layer, "_invalidate_cache") as mock_inv:
        _save_single_rfq("rfq_save_test", {
            "id": "rfq_save_test",
            "solicitation_number": "S-SAVE",
        })
    # No `_invalidate_cache` call inside this code path
    assert mock_inv.call_count == 0


def test_save_rfqs_does_not_invalidate_json_cache(seed_db_quote):
    """Same pin for `save_rfqs` (the bulk variant)."""
    from src.api import data_layer
    from src.api.data_layer import save_rfqs

    with patch.object(data_layer, "_invalidate_cache") as mock_inv:
        save_rfqs({"rfq_bulk_a": {
            "id": "rfq_bulk_a",
            "solicitation_number": "S-BULK",
        }})
    assert mock_inv.call_count == 0


# ── Migration helper is wired into deferred init ───────────────────

def test_init_db_deferred_calls_rfqs_json_migration():
    """Pin: boot's deferred init runs the rfqs.json migration helper."""
    from src.core import db as db_mod

    with patch("src.api.data_layer.migrate_legacy_rfqs_json_if_present") as mock_mig:
        with patch("src.core.dal.migrate_json_to_db"):
            db_mod.init_db_deferred()
    assert mock_mig.call_count == 1
