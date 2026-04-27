"""Phase 4.6: category-intel endpoint tests."""

import json

from src.core.db import get_db
from src.core.intel_categories import intel_category, all_categories


def _seed_quote(qnum, status, items, agency="X"):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, institution,
                                line_items, total, created_at, is_test)
            VALUES (?, ?, ?, ?, ?, 100.0, '2025-06-15T12:00:00', 0)
        """, (qnum, status, agency, agency, json.dumps(items)))
        conn.commit()


class TestClassifier:
    def test_propet_walker_strap_is_footwear_orthopedic(self):
        cat, label = intel_category(
            "Propet M3705 Life Walker Strap (White) Width M Size 8"
        )
        assert cat == "footwear-orthopedic"
        assert "footwear" in label.lower() or "walker" in label.lower()

    def test_diabetic_velcro_shoe_is_footwear_orthopedic(self):
        cat, _ = intel_category("SHOE DIABETIC WHITE VELCRO MEN 9.5 M")
        assert cat == "footwear-orthopedic"

    def test_rx_comfort_insoles_is_footwear_orthopedic(self):
        cat, _ = intel_category("RX COMFORT INSOLES # 4, SIZE 10-11.5 MENS")
        assert cat == "footwear-orthopedic"

    def test_tena_brief_is_incontinence(self):
        cat, _ = intel_category(
            "Unisex Adult Incontinence Brief TENA ProSkin Stretch"
        )
        assert cat == "incontinence"

    def test_tranquility_pull_on_is_incontinence(self):
        cat, _ = intel_category(
            "Tranquility Premium OverNight Pull On with Tear Away Seams XL"
        )
        assert cat == "incontinence"

    def test_softpro_splint_is_splint_brace(self):
        cat, _ = intel_category(
            "Resting Hand Splint SoftPro Functional Fabric Left Hand"
        )
        assert cat == "splint-brace"

    def test_bardex_foley_is_catheter(self):
        cat, _ = intel_category(
            "Foley Catheter Bardex I.C. 2-Way Standard Tip 5cc Balloon"
        )
        assert cat == "catheter-foley"

    def test_oreo_snack_pack_is_snack_food(self):
        cat, _ = intel_category(
            "Oreo Chocolate Sandwich Cookies (30-1.59 oz snack packs)"
        )
        assert cat == "snack-food"

    def test_allevyn_foam_dressing_is_gauze_wound(self):
        cat, _ = intel_category(
            "Thin Silicone Foam Dressing Allevyn Gentle Border Lite 6 X 6"
        )
        assert cat == "gauze-wound"

    def test_elastic_bandage_is_gauze_wound(self):
        cat, _ = intel_category(
            "McKesson Brand #16-1033-4 Elastic Bandage 4 Inch X 5 Yard"
        )
        assert cat == "gauze-wound"

    def test_back_support_is_splint_brace(self):
        cat, _ = intel_category(
            "Scott Specialties Sport-Aid Occupational Back Support Medium"
        )
        assert cat == "splint-brace"

    def test_composition_notebook_is_office(self):
        cat, _ = intel_category(
            "Three Leaf 80 Ct, 9-3/4 X 7-1/2, Composition Notebook, Wide"
        )
        assert cat == "office"

    def test_coloring_pages_is_arts_crafts(self):
        cat, _ = intel_category("COLORING ART PAGES DIAMOND / 12 PK")
        assert cat == "arts-crafts"

    def test_brass_letters_is_signage(self):
        cat, _ = intel_category("Brass Letters Font: Helvetica Bold Size 5\"")
        assert cat == "signage"

    def test_unknown_description_is_uncategorized(self):
        # Categorizer should NOT silently bucket unknown items as
        # 'general' — uncategorized is the honest answer.
        cat, _ = intel_category("Random Industrial Widget XYZ-9000")
        assert cat == "uncategorized"

    def test_empty_description_is_uncategorized(self):
        cat, _ = intel_category("")
        assert cat == "uncategorized"
        cat2, _ = intel_category(None)
        assert cat2 == "uncategorized"

    def test_all_categories_returns_dict(self):
        cats = all_categories()
        assert isinstance(cats, dict)
        assert "footwear-orthopedic" in cats
        assert "incontinence" in cats


class TestCategoryIntelEndpoint:
    def test_missing_description_returns_400(self, client):
        r = client.get("/api/oracle/category-intel")
        assert r.status_code == 400

    def test_empty_db_returns_zero_quotes(self, client):
        r = client.get(
            "/api/oracle/category-intel?description=Propet+Walker"
        )
        body = r.get_json()
        assert body["ok"] is True
        assert body["category"] == "footwear-orthopedic"
        assert body["quotes"] == 0
        assert body["danger"] is False

    def test_loss_bucket_triggers_danger_flag(self, client):
        # Seed 6 footwear losses + 0 wins → danger=true
        for i in range(6):
            _seed_quote(
                f"CI-FW-{i}", "lost",
                [{"description": "Propet M3705 Life Walker Strap White"}]
            )
        r = client.get(
            "/api/oracle/category-intel?description=Propet+walking+shoe"
        )
        body = r.get_json()
        assert body["category"] == "footwear-orthopedic"
        assert body["losses"] == 6
        assert body["wins"] == 0
        assert body["win_rate_pct"] == 0.0
        assert body["danger"] is True
        assert "LOSS BUCKET" in body["warning_text"]

    def test_high_win_bucket_surfaces_green_signal(self, client):
        # Seed 6 incontinence wins, 0 losses
        for i in range(6):
            _seed_quote(
                f"CI-INC-{i}", "won",
                [{"description": "TENA ProSkin Adult Brief XL"}]
            )
        r = client.get(
            "/api/oracle/category-intel?description=Adult+Brief+Incontinence"
        )
        body = r.get_json()
        assert body["category"] == "incontinence"
        assert body["wins"] == 6
        assert body["danger"] is False
        assert "WIN BUCKET" in body["warning_text"]

    def test_below_threshold_does_not_warn(self, client):
        # 4 losses → not enough data to trigger danger
        for i in range(4):
            _seed_quote(
                f"CI-LOW-{i}", "lost",
                [{"description": "Propet Life Walker"}]
            )
        r = client.get(
            "/api/oracle/category-intel?description=Propet+Walker"
        )
        body = r.get_json()
        assert body["quotes"] == 4
        assert body["danger"] is False

    def test_danger_fires_at_actual_footwear_rate(self, client):
        # Real prod data shows footwear at 12.9% — ensure the
        # threshold is loose enough to flag it. Seed 4 wins / 27
        # losses (matches live as of 2026-04-26).
        for i in range(4):
            _seed_quote(
                f"CI-FW-WIN-{i}", "won",
                [{"description": "Propet Walker"}]
            )
        for i in range(27):
            _seed_quote(
                f"CI-FW-LOSS-{i}", "lost",
                [{"description": "Propet Walker"}]
            )
        r = client.get(
            "/api/oracle/category-intel?description=Propet+Walker"
        )
        body = r.get_json()
        assert body["quotes"] == 31
        assert body["win_rate_pct"] == 12.9
        assert body["danger"] is True

    def test_win_bucket_fires_at_56_percent(self, client):
        # Real prod data shows incontinence at 56.4% — ensure the
        # green threshold catches it. Seed 22 wins / 17 losses.
        for i in range(22):
            _seed_quote(
                f"CI-INC-WIN-{i}", "won",
                [{"description": "TENA Adult Brief XL"}]
            )
        for i in range(17):
            _seed_quote(
                f"CI-INC-LOSS-{i}", "lost",
                [{"description": "TENA Adult Brief XL"}]
            )
        r = client.get(
            "/api/oracle/category-intel?description=TENA+Adult+Brief"
        )
        body = r.get_json()
        assert body["win_rate_pct"] == 56.4
        assert body["danger"] is False
        assert "WIN BUCKET" in body["warning_text"]

    def test_quote_with_multiple_items_same_cat_counts_once(self, client):
        # A single quote with 3 footwear items should count as ONE
        # quote in the bucket — not three.
        _seed_quote(
            "CI-MULTI", "lost",
            [
                {"description": "Propet Walker Strap White Size 8"},
                {"description": "Propet Walker Strap White Size 9"},
                {"description": "Propet Walker Strap White Size 10"},
            ],
        )
        r = client.get(
            "/api/oracle/category-intel?description=Propet+Walker"
        )
        body = r.get_json()
        assert body["category"] == "footwear-orthopedic"
        assert body["quotes"] == 1

    def test_agency_filter_narrows_bucket(self, client):
        _seed_quote(
            "CI-AG-1", "lost",
            [{"description": "Propet Walker"}],
            agency="Veterans Home Barstow",
        )
        _seed_quote(
            "CI-AG-2", "lost",
            [{"description": "Propet Walker"}],
            agency="CDCR Sacramento",
        )
        r = client.get(
            "/api/oracle/category-intel"
            "?description=Propet+Walker&agency=Barstow"
        )
        body = r.get_json()
        assert body["quotes"] == 1
        assert body["agency_filter"] == "Barstow"

    def test_other_categories_returns_top_5(self, client):
        # Seed multiple categories so we can verify the
        # "other_categories" comparison list shows up.
        _seed_quote(
            "CI-CMP-1", "won",
            [{"description": "TENA Adult Brief XL"}]
        )
        _seed_quote(
            "CI-CMP-2", "won",
            [{"description": "Foley Catheter Bardex IC"}]
        )
        _seed_quote(
            "CI-CMP-3", "lost",
            [{"description": "Propet Walker Strap"}]
        )
        r = client.get(
            "/api/oracle/category-intel?description=Propet+Walker"
        )
        body = r.get_json()
        # Target was footwear; other_categories should NOT include
        # footwear, and should include incontinence + catheter.
        cats = {x["category"] for x in body["other_categories"]}
        assert "footwear-orthopedic" not in cats
        assert "incontinence" in cats or "catheter-foley" in cats


class TestCategoryListEndpoint:
    def test_list_returns_known_categories(self, client):
        r = client.get("/api/oracle/category-list")
        body = r.get_json()
        assert body["ok"] is True
        ids = {c["id"] for c in body["categories"]}
        assert "footwear-orthopedic" in ids
        assert "incontinence" in ids
        assert "splint-brace" in ids
