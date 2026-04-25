"""Phase 1.7 of PLAN_ONCE_AND_FOR_ALL.md: McKesson catalog importer +
supplier_skus reverse-lookup endpoint.
"""

import io
import os
import tempfile

import pytest

from scripts.import_mckesson_catalog import (
    _clean_description, import_csv, SUPPLIER_NAME,
)


class TestDescriptionCleanup:
    def test_strips_mckesson_marker(self):
        raw = "Wheelchair Footrest..McKesson #\t1001682..Manufacturer #\t90763430"
        assert _clean_description(raw) == "Wheelchair Footrest"

    def test_strips_old_mckesson_marker(self):
        raw = "Bandage Roll..Old McKesson # 34322004..McKesson #\t10057"
        assert _clean_description(raw) == "Bandage Roll"

    def test_strips_low_stock_note(self):
        raw = "Filtered Pouch..**Very low stock, ETA could be around 2 weeks**"
        assert _clean_description(raw) == "Filtered Pouch"

    def test_keeps_clean_description(self):
        raw = "Just a clean description, no markers"
        assert _clean_description(raw) == "Just a clean description, no markers"

    def test_handles_empty(self):
        assert _clean_description("") == ""
        assert _clean_description(None) == ""


def _write_csv(rows):
    fh = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
    )
    fh.write("Type,Item,Description,Preferred Vendor,MPN\n")
    for r in rows:
        fh.write(",".join(f'"{c}"' for c in r) + "\n")
    fh.close()
    return fh.name


class TestImportCsv:
    def test_dry_run_reads_but_doesnt_write(self):
        path = _write_csv([
            ("Inventory Part", "1001682", "Footrest..McKesson #\t1001682..Manufacturer #\t90763430", "McKesson", "90763430"),
        ])
        try:
            r = import_csv(path, dry_run=True)
            assert r["rows_read"] == 1
            assert r["rows_inserted"] == 0
            assert r["rows_updated"] == 0
        finally:
            os.unlink(path)

    def test_inserts_new_rows(self):
        path = _write_csv([
            ("Inventory Part", "1001682", "Footrest..McKesson #\t1001682..Manufacturer #\t90763430", "McKesson", "90763430"),
            ("Inventory Part", "1041721", "Back Brace..McKesson #\t1041721..Manufacturer #\t64179", "McKesson", "64179"),
        ])
        try:
            r = import_csv(path)
            assert r["rows_read"] == 2
            assert r["rows_inserted"] == 2
            assert r["rows_updated"] == 0
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT * FROM supplier_skus WHERE supplier=? AND supplier_sku=?",
                    (SUPPLIER_NAME, "1041721")
                ).fetchone()
            assert row is not None
            assert row["mfg_number"] == "64179"
            assert row["description"] == "Back Brace"
        finally:
            os.unlink(path)

    def test_idempotent_reimport(self):
        path = _write_csv([
            ("Inventory Part", "1001682", "Footrest..McKesson #\t1001682", "McKesson", "90763430"),
        ])
        try:
            r1 = import_csv(path)
            r2 = import_csv(path)
            assert r1["rows_inserted"] == 1
            assert r2["rows_inserted"] == 0
            assert r2["rows_updated"] == 1
        finally:
            os.unlink(path)

    def test_skips_blank_sku(self):
        path = _write_csv([
            ("Inventory Part", "", "Bad row", "McKesson", "X"),
            ("Inventory Part", "1001682", "Good row", "McKesson", "90763430"),
        ])
        try:
            r = import_csv(path)
            assert r["rows_read"] == 1  # blank-sku row skipped before counting
            assert r["rows_inserted"] == 1
        finally:
            os.unlink(path)


class TestSupplierSkuLookupEndpoint:
    def test_lookup_returns_mfg_number(self, client):
        # Seed via the importer so end-to-end is exercised
        path = _write_csv([
            ("Inventory Part", "1041721", "Back Brace..McKesson #\t1041721..Manufacturer #\t64179", "McKesson", "64179"),
        ])
        try:
            import_csv(path)
        finally:
            os.unlink(path)

        r = client.get("/api/catalog/supplier-sku-lookup?supplier=mckesson&sku=1041721")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["mfg_number"] == "64179"
        assert "Back Brace" in body["description"]

    def test_lookup_404_when_missing(self, client):
        r = client.get("/api/catalog/supplier-sku-lookup?supplier=mckesson&sku=NOT-REAL")
        assert r.status_code == 404
        assert r.get_json()["ok"] is False

    def test_lookup_400_when_args_missing(self, client):
        r = client.get("/api/catalog/supplier-sku-lookup")
        assert r.status_code == 400

    def test_stats_endpoint_returns_counts(self, client):
        path = _write_csv([
            ("Inventory Part", "STATS-1", "X", "McKesson", "M1"),
            ("Inventory Part", "STATS-2", "Y", "McKesson", "M2"),
        ])
        try:
            import_csv(path)
        finally:
            os.unlink(path)
        r = client.get("/api/catalog/supplier-skus-stats")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["total"] >= 2
