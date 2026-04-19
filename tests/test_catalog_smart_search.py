"""
Tests for the 2026-04-19 catalog smart-search v2 work.

Background: original predictive_lookup did `WHERE name|sku|description LIKE
%query%` — a single substring match. A 3-token query like "nitrile gloves
large" would only find products containing that literal substring. Mike
called the catalog "unsearchable" because a slightly different word order
(or tokens scattered across fields) produced zero results.

smart_search() tokenizes the query and scores each candidate by per-field
token hits with weights (SKU/MFG# heaviest, category lightest). Coverage
bonus for hitting every token. Times-quoted is a tie-break boost.
"""
import pytest

from src.agents.product_catalog import (
    smart_search, predictive_lookup, search_products,
    init_catalog_db, _get_conn, _smart_tokenize, _score_product,
)


@pytest.fixture
def seeded_catalog():
    """Seed a small catalog for ranking tests. Uses the test-isolated DB."""
    init_catalog_db()
    conn = _get_conn()
    # Wipe existing rows so tests don't contaminate each other.
    conn.execute("DELETE FROM product_catalog")
    conn.commit()
    rows = [
        # (name, sku, description, category, manufacturer, mfg_number, times_quoted)
        ("Nitrile Exam Gloves Large 100ct Box",   "GLV-NIT-L-100", "Powder-free blue nitrile, 4mil", "Medical/PPE",     "Halyard", "44794",      45),
        ("Nitrile Exam Gloves Medium 100ct Box",  "GLV-NIT-M-100", "Powder-free blue nitrile, 4mil", "Medical/PPE",     "Halyard", "44793",      30),
        ("Latex Exam Gloves Large 100ct Box",     "GLV-LAT-L-100", "Lightly powdered, 5mil",         "Medical/PPE",     "Curad",   "CUR8965R",   12),
        ("Vinyl Exam Gloves Large 100ct Box",     "GLV-VIN-L-100", "Powder-free clear vinyl",        "Medical/PPE",     "Ambitex", "VSPF5201",   8),
        ("Hi-Vis ANSI Class 2 Safety Vest Large", "VST-HV-L-001",  "Reflective stripe, mesh back",   "Safety/PPE",      "ML Kishigo", "1163",    20),
        ("Multipurpose Copy Paper 8.5x11 Ream",   "PPR-MUL-85-11", "20lb 96 brightness 500 sheets",  "Office Supplies", "TRU RED", "TR58176",    100),
        ("Heavy Duty Nitrile Gloves XL 50ct",     "GLV-HD-XL-50",  "Industrial 8mil black nitrile",  "Industrial/PPE",  "MicroFlex", "MF93852",   5),
    ]
    now = "2026-04-19T00:00:00"
    for n, sku, desc, cat, mfr, mfg, tq in rows:
        conn.execute(
            """INSERT INTO product_catalog
               (name, sku, description, category, manufacturer, mfg_number,
                sell_price, cost, margin_pct, times_quoted, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 12.50, 8.00, 36.0, ?, ?, ?)""",
            (n, sku, desc, cat, mfr, mfg, tq, now, now),
        )
    conn.commit()
    conn.close()
    yield


class TestTokenizer:
    def test_basic_lowercase_split(self):
        assert _smart_tokenize("Nitrile Exam Gloves") == ["nitrile", "exam", "gloves"]

    def test_drops_short_and_stopwords(self):
        # "of", "x" are dropped; "8mil" stays
        toks = _smart_tokenize("Box of 100 8mil x large")
        assert "of" not in toks
        assert "x" not in toks
        assert "8mil" in toks
        assert "100" in toks

    def test_handles_punctuation(self):
        # Single-char tokens (the "8") are dropped by min_length=2 — this
        # is intentional, otherwise every digit becomes a search term.
        assert _smart_tokenize("8.5x11, 100-ct") == ["5x11", "100", "ct"]

    def test_empty_query(self):
        assert _smart_tokenize("") == []
        assert _smart_tokenize(None) == []


class TestSmartSearchRanking:
    def test_word_order_doesnt_matter(self, seeded_catalog):
        """The original LIKE search failed when query tokens were reordered."""
        a = smart_search("nitrile gloves large", limit=5)
        b = smart_search("large nitrile gloves", limit=5)
        c = smart_search("gloves large nitrile", limit=5)
        # All three should find the Large Nitrile Exam Glove first
        for results in (a, b, c):
            assert results, "smart_search returned nothing"
            assert "Nitrile" in results[0]["name"]
            assert "Large" in results[0]["name"]

    def test_tokens_split_across_fields(self, seeded_catalog):
        """Token can match in name OR description OR mfg_number — old naive
        LIKE required all tokens in one combined string."""
        # "halyard" is in manufacturer, "44794" is mfg_number,
        # "powder-free" is in description. All three should rank top.
        results = smart_search("halyard 44794 powder", limit=3)
        assert results
        top = results[0]
        assert top["mfg_number"] == "44794"

    def test_exact_sku_short_circuits(self, seeded_catalog):
        results = smart_search("GLV-NIT-L-100", limit=5)
        assert results
        assert results[0]["sku"] == "GLV-NIT-L-100"
        assert results[0].get("_match_score", 0) >= 1000  # exact-match flag

    def test_exact_mfg_number_short_circuits(self, seeded_catalog):
        results = smart_search("CUR8965R", limit=5)
        assert results
        assert results[0]["mfg_number"] == "CUR8965R"

    def test_irrelevant_query_returns_nothing(self, seeded_catalog):
        results = smart_search("kazoos and ukuleles", limit=5)
        assert results == []

    def test_partial_coverage_still_ranks(self, seeded_catalog):
        """A query that hits 2 of 3 tokens should still return results,
        ranked below full-coverage matches."""
        # "nitrile foo bar" — nitrile matches several products, foo/bar don't
        results = smart_search("nitrile foo bar", limit=5)
        assert results
        # All results contain "nitrile" somewhere
        for r in results:
            blob = (r["name"] + " " + (r["description"] or "")).lower()
            assert "nitrile" in blob

    def test_empty_query_returns_popular(self, seeded_catalog):
        """Empty query should still work — sorts by times_quoted desc."""
        results = smart_search("", limit=10)
        assert results
        assert results[0]["times_quoted"] == 100  # paper has 100 quotes
        assert results[1]["times_quoted"] == 45   # then nitrile-L


class TestScoreProduct:
    def test_sku_match_outweighs_description_match(self):
        sku_match = {
            "sku": "GLV-NIT-L-100", "name": "x", "description": "x",
            "category": "x", "manufacturer": "x", "mfg_number": "x", "tags": "",
            "times_quoted": 0,
        }
        desc_match = {
            "sku": "x", "name": "x", "description": "GLV NIT L 100 stuff",
            "category": "x", "manufacturer": "x", "mfg_number": "x", "tags": "",
            "times_quoted": 0,
        }
        s_sku, _ = _score_product(sku_match, ["glv-nit-l-100"])
        s_desc, _ = _score_product(desc_match, ["glv-nit-l-100"])
        assert s_sku > s_desc

    def test_full_coverage_bonus(self):
        full = {
            "sku": "x", "name": "alpha beta gamma", "description": "x",
            "category": "x", "manufacturer": "x", "mfg_number": "x", "tags": "",
            "times_quoted": 0,
        }
        partial = {
            "sku": "x", "name": "alpha", "description": "x",
            "category": "x", "manufacturer": "x", "mfg_number": "x", "tags": "",
            "times_quoted": 0,
        }
        s_full, _ = _score_product(full, ["alpha", "beta", "gamma"])
        s_partial, _ = _score_product(partial, ["alpha", "beta", "gamma"])
        assert s_full > s_partial * 1.5  # significant bonus for full coverage


class TestBackCompat:
    def test_search_products_unchanged_signature(self, seeded_catalog):
        """search_products is called from many places — keep signature."""
        results = search_products("nitrile", limit=5, category="Medical/PPE")
        assert all(r["category"] == "Medical/PPE" for r in results)

    def test_predictive_lookup_unchanged_signature(self, seeded_catalog):
        results = predictive_lookup("paper", limit=5)
        assert results
        assert "paper" in results[0]["name"].lower()
