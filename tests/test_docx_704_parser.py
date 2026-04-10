"""
Tests for AMS 704 DOCX parsing — validates extraction + item parsing against
real buyer DOCX files. Covers both AI-parsed and regex-fallback paths.

Run: python -m pytest tests/test_docx_704_parser.py -v
"""
import os
import sys
import pytest

# Ensure project root is on path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.forms.doc_converter import (
    extract_text, is_office_doc, parse_items_from_text,
    _extract_docx_704, _parse_704_structured_text,
    _parse_704_header_table, _merge_704_table_rows,
)

# ── Test file paths (actual buyer DOCX files) ──────────────────────────────
DESKTOP = os.path.expanduser("~/OneDrive/Desktop")
NON_FOOD_DOCX = os.path.join(DESKTOP, "AMS 704 Price Check Worksheet April 2026 Incentive non Food.docx")
FOOD_DOCX = os.path.join(DESKTOP, "AMS 704 Price Check Worksheet April 2026 Incentive Food a.docx")

has_non_food = os.path.exists(NON_FOOD_DOCX)
has_food = os.path.exists(FOOD_DOCX)


# ═══════════════════════════════════════════════════════════════════════════
# Unit tests — 704 detection and structure
# ═══════════════════════════════════════════════════════════════════════════

class TestOfficeDocDetection:
    def test_docx_detected(self):
        assert is_office_doc("file.docx")
        assert is_office_doc("AMS 704 Price Check.DOCX")

    def test_pdf_not_detected(self):
        assert not is_office_doc("file.pdf")
        assert not is_office_doc("file.png")


class Test704StructuredTextParser:
    """Test _parse_704_structured_text with synthetic input."""

    def test_parses_simple_items(self):
        text = """=== AMS 704 PRICE CHECK WORKSHEET ===

=== HEADER (metadata — NOT line items) ===
Requestor: Test User
Institution: CSP-SAC

=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t5\tEach\t1\tWidget A\tWidget Alpha v2\t\t
2\t10\tBox\t12\tGadget B | Item 123456\tGadget Beta\t\t
"""
        items = _parse_704_structured_text(text)
        assert len(items) == 2

        assert items[0]["qty"] == 5
        assert items[0]["uom"] == "each"
        assert items[0]["qty_per_uom"] == 1
        assert "Widget" in items[0]["description"]

        assert items[1]["qty"] == 10
        assert items[1]["uom"] == "box"
        assert items[1]["qty_per_uom"] == 12
        assert items[1]["part_number"] == "123456"

    def test_ignores_header_and_footer(self):
        text = """=== AMS 704 PRICE CHECK WORKSHEET ===

=== HEADER (metadata — NOT line items) ===
Requestor: Carolyn Montgomery
Institution: CIW RHU
Delivery Zip Code: 92880
Phone Number: 909 597-1771

=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t2\tPack\t12\tCarmex\tCarmex Lip Balm\t\t
"""
        items = _parse_704_structured_text(text)
        assert len(items) == 1
        # Must NOT contain header data
        descs = " ".join(i["description"] for i in items)
        assert "Carolyn" not in descs
        assert "Montgomery" not in descs
        assert "CIW RHU" not in descs
        assert "92880" not in descs
        assert "597-1771" not in descs

    def test_skips_non_numeric_item_rows(self):
        text = """=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t3\tEach\t1\tReal Item\tReal Desc\t\t
\t\t\t\tContinuation text\t\t\t
"""
        items = _parse_704_structured_text(text)
        assert len(items) == 1

    def test_qty_per_uom_with_units(self):
        """QTY PER UOM may be '38oz.' or '90oz' — extract number only."""
        text = """=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t2\tBottle\t38oz.\tShampoo\tPantene Shampoo 38.2 fl oz\t\t
2\t8\tPk\t90oz\tChocolate\tKirkland Chocolate Bag 90oz\t\t
"""
        items = _parse_704_structured_text(text)
        assert items[0]["qty_per_uom"] == 38
        assert items[1]["qty_per_uom"] == 90

    def test_multi_page_items(self):
        """Two separate LINE ITEMS sections (page 1 + page 2)."""
        text = """=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t5\tPk\t12\tItem A\tFull A\t\t
2\t3\tBox\t6\tItem B\tFull B\t\t

ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
3\t2\tEach\t1\tItem C\tFull C\t\t
"""
        items = _parse_704_structured_text(text)
        assert len(items) == 3
        assert items[0]["item_number"] == "1"
        assert items[2]["item_number"] == "3"

    def test_costco_item_number_extraction(self):
        text = """=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t3\tPk\t54oz\tStarbursts | Item 1324466\tStarburst Chewy Candy, 54 oz | Costco\t\t
"""
        items = _parse_704_structured_text(text)
        assert items[0]["part_number"] == "1324466"


class TestParseItemsFromTextDispatch:
    """Verify parse_items_from_text routes 704 text to structured parser."""

    def test_routes_to_704_parser(self):
        text = """=== LINE ITEMS (extract items ONLY from this section) ===
ITEM #\tQTY\tUOM\tQTY PER UOM\tDESCRIPTION\tSUBSTITUTED\tPRICE\tEXT
1\t2\tEach\t1\tTest Product\tTest Substituted\t\t
"""
        items = parse_items_from_text(text)
        assert len(items) == 1
        assert items[0]["description"]  # not garbage

    def test_generic_text_still_works(self):
        text = """Engraved Name Tag, Black/White
Qty: 22
ASIN: B07TEST123

Copy Paper, 8.5x11, 20lb
Qty: 5
"""
        items = parse_items_from_text(text)
        assert len(items) == 2
        assert items[0]["qty"] == 22


# ═══════════════════════════════════════════════════════════════════════════
# Integration tests — actual DOCX files
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not has_non_food, reason="Non-food DOCX not available")
class TestNonFoodDocx:
    """End-to-end tests against the actual non-food DOCX file."""

    def test_extract_detects_704(self):
        text = extract_text(NON_FOOD_DOCX)
        assert "=== AMS 704 PRICE CHECK WORKSHEET ===" in text
        assert "=== LINE ITEMS" in text

    def test_extract_has_header(self):
        text = extract_text(NON_FOOD_DOCX)
        assert "=== HEADER" in text
        assert "Carolyn Montgomery" in text
        assert "CIW RHU" in text
        assert "92880" in text

    def test_parses_exactly_3_items(self):
        text = extract_text(NON_FOOD_DOCX)
        items = parse_items_from_text(text)
        assert len(items) == 3, f"Expected 3 items, got {len(items)}: {[i['description'][:40] for i in items]}"

    def test_item_details_correct(self):
        text = extract_text(NON_FOOD_DOCX)
        items = parse_items_from_text(text)

        # Item 1: Carmex
        assert items[0]["qty"] == 2
        assert items[0]["uom"] == "pack"
        assert items[0]["qty_per_uom"] == 12
        assert "carmex" in items[0]["description"].lower()
        assert items[0]["part_number"] == "130038"

        # Item 2: Pantene Shampoo
        assert items[1]["qty"] == 2
        assert items[1]["uom"] == "bottle"
        assert items[1]["qty_per_uom"] == 38
        assert "pantene" in items[1]["description"].lower()
        assert "shampoo" in items[1]["description"].lower()
        assert items[1]["part_number"] == "1903627"

        # Item 3: Pantene Conditioner
        assert items[2]["qty"] == 2
        assert items[2]["uom"] == "bottle"
        assert items[2]["qty_per_uom"] == 38
        assert "conditioner" in items[2]["description"].lower()
        assert items[2]["part_number"] == "1903628"

    def test_no_header_data_in_items(self):
        """CRITICAL: header metadata must NOT appear as items."""
        text = extract_text(NON_FOOD_DOCX)
        items = parse_items_from_text(text)
        all_descs = " ".join(i["description"] for i in items).lower()
        # These are header fields that were incorrectly parsed as items before
        assert "carolyn" not in all_descs
        assert "montgomery" not in all_descs
        assert "institution or hq program" not in all_descs
        assert "delivery zip code" not in all_descs
        assert "date of request" not in all_descs
        assert "supplier information" not in all_descs.replace(" ", "")
        assert "company name" not in all_descs
        assert "signature" not in all_descs


@pytest.mark.skipif(not has_food, reason="Food DOCX not available")
class TestFoodDocx:
    """End-to-end tests against the actual food DOCX file (multi-page)."""

    def test_extract_detects_704(self):
        text = extract_text(FOOD_DOCX)
        assert "=== AMS 704 PRICE CHECK WORKSHEET ===" in text

    def test_parses_exactly_5_items(self):
        text = extract_text(FOOD_DOCX)
        items = parse_items_from_text(text)
        assert len(items) == 5, f"Expected 5 items, got {len(items)}: {[i['description'][:40] for i in items]}"

    def test_item_details_correct(self):
        text = extract_text(FOOD_DOCX)
        items = parse_items_from_text(text)

        # Item 1: Kirkland Chocolate
        assert items[0]["qty"] == 8
        assert items[0]["uom"] == "pk"
        assert items[0]["qty_per_uom"] == 90
        assert "kirkland" in items[0]["description"].lower() or "chocolate" in items[0]["description"].lower()

        # Item 2: Starburst
        assert items[1]["qty"] == 3
        assert items[1]["part_number"] == "1324466"
        assert "starburst" in items[1]["description"].lower()

        # Item 3: Hi-Chew
        assert items[2]["qty"] == 3
        assert items[2]["part_number"] == "1353251"

        # Item 4: Hershey's Kisses
        assert items[3]["qty"] == 4
        assert items[3]["part_number"] == "401993"
        assert "hershey" in items[3]["description"].lower()

        # Item 5: Crystal Light
        assert items[4]["qty"] == 2
        assert items[4]["part_number"] == "1593505"

    def test_page2_items_included(self):
        """Items 4-5 are on page 2 of the DOCX — must be captured."""
        text = extract_text(FOOD_DOCX)
        items = parse_items_from_text(text)
        item_nums = [i["item_number"] for i in items]
        assert "4" in item_nums
        assert "5" in item_nums

    def test_no_header_data_in_items(self):
        text = extract_text(FOOD_DOCX)
        items = parse_items_from_text(text)
        all_descs = " ".join(i["description"] for i in items).lower()
        assert "carolyn" not in all_descs
        assert "ciw rhu" not in all_descs
        assert "92880" not in all_descs


# ═══════════════════════════════════════════════════════════════════════════
# Upload route integration test (Flask test client)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not has_non_food, reason="Non-food DOCX not available")
class TestUploadRoute:
    """Test the upload-pdf route with actual DOCX file via Flask test client.

    The upload route uses @safe_page and returns a 302 redirect on success.
    We verify by reading the saved PC data after the upload completes.
    """

    def _seed_pc(self, temp_data_dir, pc_id, pc_number):
        import json
        pcs = {pc_id: {
            "id": pc_id,
            "pc_number": pc_number,
            "institution": "CIW",
            "status": "new",
            "items": [],
            "parsed": {},
            "source_pdf": "",
        }}
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        with open(pc_path, "w") as f:
            json.dump(pcs, f)

    def _load_pc(self, temp_data_dir, pc_id):
        """Load PC from JSON file or SQLite (route may save to either)."""
        import json
        # Try JSON first
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        if os.path.exists(pc_path):
            with open(pc_path) as f:
                pcs = json.load(f)
            pc = pcs.get(pc_id)
            if pc and pc.get("items"):
                return pc
        # Fall back to SQLite
        import sqlite3
        db_path = os.path.join(temp_data_dir, "reytech.db")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM price_checks WHERE id=?", (pc_id,)
            ).fetchone()
            conn.close()
            if row:
                pc = dict(row)
                # data_json has the full PC dict
                if pc.get("data_json"):
                    return json.loads(pc["data_json"])
                # items column is a JSON string
                if pc.get("items") and isinstance(pc["items"], str):
                    pc["items"] = json.loads(pc["items"])
                return pc
        return {}

    def test_upload_nonfood_docx_creates_3_items(self, client, temp_data_dir):
        """Upload non-food DOCX to an existing PC and verify 3 items parsed."""
        pc_id = "test-docx-upload"
        self._seed_pc(temp_data_dir, pc_id, "TEST-DOCX")

        with open(NON_FOOD_DOCX, "rb") as docx_file:
            resp = client.post(
                f"/pricecheck/{pc_id}/upload-pdf",
                data={"file": (docx_file, "non_food.docx")},
                content_type="multipart/form-data",
            )

        # Route returns 302 redirect on success, 4xx/5xx on failure
        assert resp.status_code in (200, 302), f"Upload failed with {resp.status_code}"

        # Verify items written to the PC data file
        pc = self._load_pc(temp_data_dir, pc_id)
        items = pc.get("items", [])
        assert len(items) == 3, (
            f"Expected 3 items, got {len(items)}: "
            f"{[i.get('description', '')[:40] for i in items]}"
        )
        assert pc.get("status") == "parsed"

        # Verify no garbage header data
        all_descs = " ".join(i.get("description", "") for i in items).lower()
        assert "carolyn" not in all_descs
        assert "supplier information" not in all_descs.replace(" ", "")

    @pytest.mark.skipif(not has_food, reason="Food DOCX not available")
    def test_upload_food_docx_creates_5_items(self, client, temp_data_dir):
        """Upload food DOCX (multi-page) and verify 5 items parsed."""
        pc_id = "test-docx-food"
        self._seed_pc(temp_data_dir, pc_id, "TEST-FOOD")

        with open(FOOD_DOCX, "rb") as docx_file:
            resp = client.post(
                f"/pricecheck/{pc_id}/upload-pdf",
                data={"file": (docx_file, "food.docx")},
                content_type="multipart/form-data",
            )

        assert resp.status_code in (200, 302), f"Upload failed with {resp.status_code}"

        pc = self._load_pc(temp_data_dir, pc_id)
        items = pc.get("items", [])
        assert len(items) == 5, (
            f"Expected 5 items, got {len(items)}: "
            f"{[i.get('description', '')[:40] for i in items]}"
        )
        assert pc.get("status") == "parsed"


@pytest.mark.skipif(not has_non_food, reason="Non-food DOCX not available")
class TestReparseRoute:
    """Test the reparse route with DOCX files — verifies regex fallback works."""

    def _seed_pc_with_source(self, temp_data_dir, pc_id, docx_path):
        """Create a PC with source_pdf pointing to the DOCX file."""
        import json
        import shutil
        # Copy DOCX to the temp data dir (simulating uploaded file)
        upload_dir = os.path.join(temp_data_dir, "pc_pdfs")
        os.makedirs(upload_dir, exist_ok=True)
        dest = os.path.join(upload_dir, f"{pc_id}_source.docx")
        shutil.copy2(docx_path, dest)

        pcs = {pc_id: {
            "id": pc_id,
            "pc_number": "TEST-REPARSE",
            "institution": "CIW",
            "status": "new",
            "items": [{"description": "GARBAGE ITEM", "qty": 1}] * 34,
            "parsed": {},
            "source_pdf": dest,
        }}
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        with open(pc_path, "w") as f:
            json.dump(pcs, f)

    def _load_pc(self, temp_data_dir, pc_id):
        import json
        import sqlite3
        db_path = os.path.join(temp_data_dir, "reytech.db")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM price_checks WHERE id=?", (pc_id,)
            ).fetchone()
            conn.close()
            if row:
                pc = dict(row)
                if pc.get("data_json"):
                    return json.loads(pc["data_json"])
                if pc.get("items") and isinstance(pc["items"], str):
                    pc["items"] = json.loads(pc["items"])
                return pc
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        if os.path.exists(pc_path):
            with open(pc_path) as f:
                return json.load(f).get(pc_id, {})
        return {}

    def test_reparse_nonfood_produces_3_items(self, client, temp_data_dir):
        """Reparse a DOCX-based PC — should go from 34 garbage items to 3 real ones."""
        pc_id = "test-reparse-nonfood"
        self._seed_pc_with_source(temp_data_dir, pc_id, NON_FOOD_DOCX)

        resp = client.post(f"/pricecheck/{pc_id}/reparse")

        data = resp.get_json() if hasattr(resp, 'get_json') else {}
        assert resp.status_code == 200, f"Reparse failed: {resp.status_code} {data}"
        assert data.get("ok"), f"Reparse error: {data}"
        assert data.get("items") == 3, f"Expected 3 items, got {data}"

    @pytest.mark.skipif(not has_food, reason="Food DOCX not available")
    def test_reparse_food_produces_5_items(self, client, temp_data_dir):
        """Reparse a multi-page DOCX — should produce 5 items."""
        pc_id = "test-reparse-food"
        self._seed_pc_with_source(temp_data_dir, pc_id, FOOD_DOCX)

        resp = client.post(f"/pricecheck/{pc_id}/reparse")

        data = resp.get_json() if hasattr(resp, 'get_json') else {}
        assert resp.status_code == 200, f"Reparse failed: {resp.status_code} {data}"
        assert data.get("ok"), f"Reparse error: {data}"
        assert data.get("items") == 5, f"Expected 5 items, got {data}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
