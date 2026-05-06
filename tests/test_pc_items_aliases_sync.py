"""PC items/line_items alias sync — pinned by 2026-05-05 incident pc_177b18e6.

A save updated `pc["items"]` but the embedded `pc["line_items"]` (and
`pc["parsed"]["line_items"]`) stayed stale in the blob. The
quote_model_v2 adapter then preferred the stale alias and blanked a
row in the UI. Mike had to flip `quote_model_v2_enabled` off as a
mitigation.

These tests pin both the save-side and load-side sync paths so the
underlying divergence cannot resurface.
"""

import json
import os

import pytest


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("FLASK_ENV", "testing")
    # Force a fresh import so the new DATA_DIR is picked up.
    import importlib
    import src.core.paths as paths
    importlib.reload(paths)
    import src.core.db as core_db
    importlib.reload(core_db)
    import src.api.data_layer as data_layer
    importlib.reload(data_layer)
    core_db.init_db()
    return data_layer


def _make_pc(items=None, line_items=None, parsed_items=None):
    pc = {
        "id": "pc_alias_test",
        "created_at": "2026-05-06T00:00:00",
        "requestor": "Test",
        "institution": "CDCR",
        "agency": "CDCR",
        "status": "parsed",
    }
    if items is not None:
        pc["items"] = items
    if line_items is not None:
        pc["line_items"] = line_items
    if parsed_items is not None:
        pc["parsed"] = {"line_items": parsed_items}
    return pc


class TestSaveTimeAliasSync:
    def test_save_overwrites_stale_line_items_with_canonical_items(self, isolated_db):
        """Canonical `items` wins on save — stale `line_items` gets replaced."""
        canonical = [{"description": "Heel Donut", "qty": 1, "unit_price": 16}]
        stale = [{"description": "OLD", "qty": 9, "unit_price": 99}]
        pc = _make_pc(items=canonical, line_items=stale, parsed_items=stale)

        isolated_db._save_single_pc("pc_alias_test", pc)

        assert pc["items"] == canonical
        assert pc["line_items"] == canonical
        assert pc["parsed"]["line_items"] == canonical
        # `line_items` must be a separate list object so later mutations
        # don't bleed back into `items`.
        assert pc["line_items"] is not pc["items"]

    def test_save_back_fills_items_from_line_items_when_items_missing(self, isolated_db):
        """If only `line_items` is present, fill `items` from it."""
        only_line_items = [{"description": "Foo", "qty": 2, "unit_price": 5}]
        pc = _make_pc(line_items=only_line_items)

        isolated_db._save_single_pc("pc_alias_test", pc)

        assert pc["items"] == only_line_items
        assert pc["line_items"] == only_line_items

    def test_save_drops_parsed_line_items_when_parsed_absent(self, isolated_db):
        """No `parsed` block → no parsed.line_items added."""
        canonical = [{"description": "Bar", "qty": 1, "unit_price": 10}]
        pc = _make_pc(items=canonical)

        isolated_db._save_single_pc("pc_alias_test", pc)

        assert "parsed" not in pc or "line_items" not in pc.get("parsed", {})


class TestLoadTimeAliasSync:
    def test_load_realigns_divergent_aliases(self, isolated_db):
        """Aliases must converge to `items` on read, even if blob held drift."""
        # Manually plant a divergent record by going around _save_single_pc
        # so we can simulate the pre-fix on-disk state.
        canonical = [{"description": "Heel Donut", "qty": 1, "unit_price": 16}]
        stale = [{"description": "OLD", "qty": 9, "unit_price": 99}]
        from src.core.db import get_db

        with get_db() as conn:
            blob = {
                "id": "pc_drift",
                "items": canonical,
                "line_items": stale,
                "parsed": {"line_items": stale},
                "status": "parsed",
                "agency": "CDCR",
                "institution": "CDCR",
            }
            conn.execute(
                "INSERT INTO price_checks (id, created_at, items, status, data_json, pc_data) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("pc_drift", "2026-05-06T00:00:00", json.dumps(canonical),
                 "parsed", json.dumps(blob), json.dumps(blob)),
            )

        loaded = isolated_db._load_price_checks()
        rec = loaded["pc_drift"]
        assert rec["items"] == canonical
        assert rec["line_items"] == canonical
        assert rec["parsed"]["line_items"] == canonical

    def test_load_back_fills_when_only_line_items_present(self, isolated_db):
        only_line_items = [{"description": "Foo", "qty": 2, "unit_price": 5}]
        from src.core.db import get_db

        with get_db() as conn:
            blob = {"id": "pc_legacy", "line_items": only_line_items, "status": "parsed"}
            conn.execute(
                "INSERT INTO price_checks (id, created_at, items, status, data_json, pc_data) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("pc_legacy", "2026-05-06T00:00:00", json.dumps([]),
                 "parsed", json.dumps(blob), json.dumps(blob)),
            )

        loaded = isolated_db._load_price_checks()
        rec = loaded["pc_legacy"]
        # data_json blob takes precedence, so `items` gets back-filled
        # from line_items by the alias normalization.
        assert rec.get("line_items") == only_line_items
        assert rec.get("items") == only_line_items
