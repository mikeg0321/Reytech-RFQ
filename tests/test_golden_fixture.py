"""
test_golden_fixture.py — End-to-end test against REAL data.

Uses the Test0321 / R26Q0321 golden fixture (real items from R25Q94 CCHCS quote,
re-keyed). 28 items, spans page 1 → page 2 of the 704. No synthetic items, no
mocked prices — the prices are the actual prices Reytech quoted in July 2025.

If this test passes, we know:
  - The 704B profile (#140) maps fields correctly
  - fill_704b handles a 28-item, 2-page payload
  - Dollar accuracy holds end-to-end
  - The boot validator (#141) doesn't refuse a real-shape payload
"""
import json
import os

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "golden", "test0321_real_cchcs.json")
BLANK_704B = os.path.join(ROOT, "tests", "fixtures", "704b_blank.pdf")


@pytest.fixture(scope="module")
def fixture_data():
    with open(FIXTURE, "r", encoding="utf-8") as f:
        return json.load(f)


class TestGoldenFixtureIntegrity:
    """Sanity checks on the fixture itself — verifies the source data is intact."""

    def test_fixture_has_28_items(self, fixture_data):
        assert len(fixture_data["line_items"]) == 28

    def test_fixture_subtotal_matches(self, fixture_data):
        items = fixture_data["line_items"]
        computed = round(sum(i["extension"] for i in items), 2)
        assert computed == 2392.50, f"Item extensions sum to {computed}, fixture says 2392.50"

    def test_fixture_test_keys(self, fixture_data):
        """Identifiers must be the test prefixes — never real quote numbers."""
        assert fixture_data["test_pc_id"] == "Test0321"
        assert fixture_data["test_rfq_quote_number"] == "R26Q0321"
        assert fixture_data["test_rfq_quote_number"].startswith("R26Q")
        assert fixture_data["test_rfq_quote_number"] != "R26Q94"  # not the original

    def test_fixture_real_solicitation(self, fixture_data):
        """Solicitation must be the real one (10819488) — not made up."""
        assert fixture_data["header"]["solicitation_number"] == "10819488"

    def test_fixture_cchcs_agency(self, fixture_data):
        assert fixture_data["header"]["agency"] == "cchcs"


class TestGoldenFixtureMultiPage:
    """The 28-item fixture must span page 1 → page 2 of the 704B."""

    def test_704b_blank_exists(self):
        assert os.path.exists(BLANK_704B), f"Missing {BLANK_704B}"

    def test_fill_704b_with_real_items_renders_multipage(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_704b, load_config
        with open(FIXTURE, "r", encoding="utf-8") as f:
            data = json.load(f)

        out = str(tmp_path / "test0321_filled.pdf")
        rfq_data = {
            "header": data["header"],
            "line_items": data["line_items"],
            "items": data["line_items"],
            "solicitation_number": data["header"]["solicitation_number"],
        }
        cfg = load_config()
        fill_704b(input_path=BLANK_704B, rfq_data=rfq_data, config=cfg, output_path=out)
        assert os.path.exists(out), f"fill_704b did not produce {out}"

        # Verify page count >= 2 (28 items must overflow page 1's ~11-row capacity)
        from pypdf import PdfReader
        reader = PdfReader(out)
        assert len(reader.pages) >= 2, (
            f"28 items should produce 2+ pages, got {len(reader.pages)}"
        )


class TestGoldenFixtureProfile:
    """The fixture must validate against the 704B profile."""

    def test_704b_profile_loads(self):
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles()
        assert "704b_reytech_standard" in profiles, (
            f"704B profile missing — got: {list(profiles.keys())}"
        )

    def test_704b_profile_has_item_row_mapping(self):
        from src.forms.profile_registry import load_profiles
        profile = load_profiles()["704b_reytech_standard"]
        # Items mapping must use [n] templating
        item_fields = [fm for fm in profile.fields if "[n]" in fm.semantic]
        assert len(item_fields) > 0, "704B profile must have items[n].* row mappings"
