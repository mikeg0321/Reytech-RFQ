"""Phase 1.6 follow-up — server-side McKesson SKU resolution.

The PC detail UI's `handleMfgInput` (pc_detail.html) auto-resolves a
buyer-pasted McKesson item-number into the canonical MFG# via
/api/catalog/supplier-sku-lookup. The auto-enrichment pipeline never
did the same — buyer's bare numeric SKU survived into Step 2 (catalog
match) where it failed to hit anything.

Step 1.25 in `_run_pipeline` closes that gap: when an item arrives
with a 6-8 digit numeric mfg_number OR an explicit
supplier_skus['mckesson'] entry, the pipeline queries the
supplier_skus table and rewrites mfg_number to the resolved value
before catalog match runs.
"""
import pytest

import src.api.dashboard as _dash
from scripts.import_mckesson_catalog import import_csv
import tempfile
import os


def _clear_pc_cache():
    _dash._pc_cache = None
    _dash._pc_cache_time = 0


def _seed_supplier_sku(supplier_sku, mfg_number, description):
    """Seed one supplier_skus row directly via the importer."""
    fh = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
    )
    fh.write("Type,Item,Description,Preferred Vendor,MPN\n")
    fh.write(f'"Inventory Part","{supplier_sku}","{description}","McKesson","{mfg_number}"\n')
    fh.close()
    try:
        import_csv(fh.name)
    finally:
        os.unlink(fh.name)


def _seed_pc(pc_id, items):
    pc = {
        "id": pc_id,
        "enrichment_status": "raw",
        "items": items,
        "status": "parsed",
    }
    _dash._save_single_pc(pc_id, pc)
    _clear_pc_cache()


def _get_pc(pc_id):
    _clear_pc_cache()
    return _dash._load_price_checks().get(pc_id)


class TestSupplierSkuResolutionStep:
    def test_numeric_mfg_resolves_to_canonical(self, temp_data_dir):
        _seed_supplier_sku("1041721", "64179", "Back Brace")
        _seed_pc("pc-mck-1", [{
            "description": "Back Brace, lumbar support",
            "mfg_number": "1041721",  # buyer pasted McKesson item-#
            "qty": 5,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-1", force=True)

        pc = _get_pc("pc-mck-1")
        assert pc["items"][0]["mfg_number"] == "64179", \
            "buyer's McKesson SKU should be rewritten to canonical MFG#"

    def test_explicit_mckesson_sku_resolves(self, temp_data_dir):
        _seed_supplier_sku("1001682", "90763430", "Wheelchair Footrest")
        _seed_pc("pc-mck-2", [{
            "description": "Wheelchair Footrest with MCK-1001682 marker",
            "mfg_number": "",
            "supplier_skus": {"mckesson": "1001682"},
            "qty": 1,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-2", force=True)

        pc = _get_pc("pc-mck-2")
        assert pc["items"][0]["mfg_number"] == "90763430"

    def test_no_resolution_when_sku_unknown(self, temp_data_dir):
        # Don't seed supplier_skus — table will be empty for this SKU
        _seed_pc("pc-mck-3", [{
            "description": "Random item",
            "mfg_number": "9999999",
            "qty": 1,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-3", force=True)

        pc = _get_pc("pc-mck-3")
        # Pipeline ran but mfg_number untouched because no supplier_skus row
        assert pc["items"][0]["mfg_number"] == "9999999"

    def test_does_not_overwrite_non_numeric_mfg(self, temp_data_dir):
        # Seed a McKesson row whose key happens to match a real MPN
        _seed_supplier_sku("16-3404", "RESOLVED-MFG", "Cohesive Bandage")
        _seed_pc("pc-mck-4", [{
            "description": "Bandage",
            "mfg_number": "16-3404",  # not 6-8 digits, has hyphen — skip lookup
            "qty": 2,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-4", force=True)

        pc = _get_pc("pc-mck-4")
        # Trigger condition is "isdigit() and 6 <= len <= 8" — `16-3404`
        # contains a hyphen so the lookup should not fire.
        assert pc["items"][0]["mfg_number"] == "16-3404"

    def test_backfills_empty_description(self, temp_data_dir):
        _seed_supplier_sku("1004075", "151088", "Toothpaste Colgate Cavity Protection")
        _seed_pc("pc-mck-5", [{
            "description": "",  # empty
            "mfg_number": "1004075",
            "qty": 3,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-5", force=True)

        pc = _get_pc("pc-mck-5")
        assert pc["items"][0]["mfg_number"] == "151088"
        assert "Toothpaste" in pc["items"][0]["description"]

    def test_does_not_overwrite_buyer_description(self, temp_data_dir):
        _seed_supplier_sku("1004075", "151088", "Toothpaste Colgate Cavity Protection")
        _seed_pc("pc-mck-6", [{
            "description": "Buyer's exact description text",
            "mfg_number": "1004075",
            "qty": 3,
        }])

        from src.agents.pc_enrichment_pipeline import _run_pipeline
        _run_pipeline("pc-mck-6", force=True)

        pc = _get_pc("pc-mck-6")
        assert pc["items"][0]["mfg_number"] == "151088"
        # Buyer's description preserved (PC pricing rule — never rewrite buyer text)
        assert pc["items"][0]["description"] == "Buyer's exact description text"
