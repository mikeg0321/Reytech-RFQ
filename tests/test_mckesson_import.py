"""Tests for the McKesson XLSX catalog importer.

Seeds a tiny 3-row XLSX in a temp dir, ingests, and verifies the rows
landed in product_catalog with the expected shape.
"""
import os
import sqlite3
import pytest


@pytest.fixture
def mckesson_xlsx(tmp_path):
    """Generate a minimal McKesson-shaped XLSX on disk."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Type", "Item", "Description", "Preferred Vendor", "MPN"])
    ws.append(["Inventory Part", "1001682", "Wheelchair Footrest", "McKesson", "1001682"])
    ws.append(["Inventory Part", "1002774", "Tympanic Thermometer Probe Cover", "McKesson", "06000-005"])
    # Blank rows that should be skipped, not crash
    ws.append([None, None, None, None, None])
    # Minimal-info row — only description — still accepted
    ws.append(["Inventory Part", "", "Gauze Pad 4x4", "McKesson", ""])
    path = os.path.join(str(tmp_path), "mckesson.xlsx")
    wb.save(path)
    return path


@pytest.fixture
def catalog_db(temp_data_dir, monkeypatch):
    import src.agents.product_catalog as _pc
    db_path = os.path.join(temp_data_dir, "reytech.db")
    monkeypatch.setattr(_pc, "DB_PATH", db_path)
    _pc.init_catalog_db()
    return db_path


class TestMckessonImport:

    def test_reads_xlsx_and_seeds_catalog(self, mckesson_xlsx, catalog_db):
        from src.agents.mckesson_import import import_mckesson_xlsx
        result = import_mckesson_xlsx(mckesson_xlsx)

        assert result["ok"] is True
        # 3 real rows (blank row skipped via `if not any(row)`)
        assert result["total_rows"] == 3
        assert result["imported"] >= 2  # 2 full rows; 3rd is desc-only
        assert result["errors"] == []
        assert result["supplier_counts"]["McKesson"] >= 2

    def test_rows_have_mpn_and_supplier_in_db(self, mckesson_xlsx, catalog_db):
        from src.agents.mckesson_import import import_mckesson_xlsx
        import_mckesson_xlsx(mckesson_xlsx)

        conn = sqlite3.connect(catalog_db)
        rows = conn.execute(
            "SELECT name, description, mfg_number, manufacturer FROM product_catalog "
            "WHERE manufacturer='McKesson' OR best_supplier='McKesson'"
        ).fetchall()
        conn.close()

        assert len(rows) >= 2
        # Verify MPN was stored (either native MPN or fallback to Item)
        mpns = {r[2] for r in rows}
        assert "1001682" in mpns or "06000-005" in mpns

    def test_rerun_is_idempotent(self, mckesson_xlsx, catalog_db):
        # Catalog row count stays stable after a second import of the same
        # XLSX — proving dedup works, even if the importer's internal
        # imported/updated split differs across runs (add_to_catalog's
        # exact return contract is its own concern).
        from src.agents.mckesson_import import import_mckesson_xlsx
        import_mckesson_xlsx(mckesson_xlsx)

        conn = sqlite3.connect(catalog_db)
        count_after_first = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
        conn.close()

        assert count_after_first >= 2

        import_mckesson_xlsx(mckesson_xlsx)

        conn = sqlite3.connect(catalog_db)
        count_after_second = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
        conn.close()

        assert count_after_second == count_after_first, \
            f"catalog grew from {count_after_first} to {count_after_second} on re-import (dedup broken)"

    def test_supplier_override(self, mckesson_xlsx, catalog_db):
        from src.agents.mckesson_import import import_mckesson_xlsx
        result = import_mckesson_xlsx(mckesson_xlsx, supplier_override="Medline")

        assert "Medline" in result["supplier_counts"]
        assert "McKesson" not in result["supplier_counts"]

    def test_missing_file_returns_error(self, catalog_db):
        from src.agents.mckesson_import import import_mckesson_xlsx
        result = import_mckesson_xlsx("/does/not/exist.xlsx")
        assert result["ok"] is False
        assert "open" in result["error"].lower() or "not found" in result["error"].lower() or "no such file" in result["error"].lower()

    def test_route_accepts_server_side_path(self, client, mckesson_xlsx, catalog_db):
        r = client.post(
            "/api/catalog/import-mckesson",
            json={"path": mckesson_xlsx},
            content_type="application/json",
        )
        assert r.status_code == 200, r.data
        body = r.get_json()
        assert body["ok"] is True
        assert body["total_rows"] >= 3

    def test_route_requires_file_or_path(self, client):
        r = client.post("/api/catalog/import-mckesson",
                        json={}, content_type="application/json")
        assert r.status_code == 400
        body = r.get_json()
        assert body["ok"] is False
        assert "file" in body["error"].lower() or "path" in body["error"].lower()
