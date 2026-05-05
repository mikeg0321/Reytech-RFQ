"""Mike P0 2026-05-05: catalog cross-contamination root cause.

Live evidence on prod RFQ a5b09b56: every line item bore the same
catalog_match (DEMENTIA & ALZHEIMERS CARE EASY PACK - 16964, sku=8)
because the auto-enrichment pipeline called search_catalog("1"),
search_catalog("2"), ..., search_catalog("8") — placeholder part
numbers from a buyer-supplied form. _smart_tokenize drops anything
shorter than _SMART_SEARCH_MIN_TOKEN_LEN=2, so single-digit input
tokenized to empty. smart_search then fell through to its "empty
query → popularity sort" branch and returned the single most-quoted
product for every item — same dict, eight rows.

The fix distinguishes:
  * truly empty / whitespace-only query → still popularity-sort (browse mode)
  * provided-but-un-tokenizable query → return [] (Mike's case)

Plus the call sites in routes_rfq._enrich_items_with_intel and
routes_rfq_admin.api_rfq_price_intel switch from search_catalog to
match_item (the proper item-matching function with tiered UPC →
exact part# → Jaccard ≥ 0.65 strategy and post-match verification).
"""
from pathlib import Path

import pytest


@pytest.fixture
def seeded_catalog(tmp_path, monkeypatch):
    """Seed a temp catalog DB with one popular product and a few others
    so we can verify popularity-sort fires when expected and does NOT
    fire when query is provided-but-un-tokenizable."""
    monkeypatch.setenv("PRODUCT_CATALOG_DB", str(tmp_path / "catalog.db"))
    from src.agents import product_catalog
    monkeypatch.setattr(product_catalog, "DB_PATH", str(tmp_path / "catalog.db"))
    product_catalog.init_catalog_db()
    conn = product_catalog._get_conn()
    # Highest-quoted "junk" product — the DEMENTIA shape that polluted
    # every item on prod
    conn.execute(
        "INSERT INTO product_catalog (sku, name, description, search_tokens, "
        " times_quoted, sell_price, category, is_test) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
        ("8", "DEMENTIA & ALZHEIMERS CARE EASY PACK - 16964",
         "Dementia & Alzheimers Care Easy Pack",
         "dementia alzheimers care easy pack",
         100, 348.99, "General"),
    )
    # A real coloring poster — what we'd actually want to match against
    # "Love Velvet Coloring Poster"
    conn.execute(
        "INSERT INTO product_catalog (sku, name, description, search_tokens, "
        " times_quoted, sell_price, category, is_test) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
        ("STUFF2COLOR-LV01", "Love Velvet Letters Fuzzy Coloring Poster",
         "Love Velvet Letters Fuzzy Coloring Poster Stuff2Color",
         "love velvet letters fuzzy coloring poster stuff2color",
         5, 4.25, "Crafts"),
    )
    conn.commit()
    conn.close()
    return product_catalog


def test_un_tokenizable_query_returns_empty_not_popularity(seeded_catalog):
    """Single-digit pn ('1') tokenizes to empty. Must return [] — never
    fall through to popularity sort and return the highest-quoted
    catalog row. Mike P0 2026-05-05."""
    pc = seeded_catalog
    for placeholder in ["1", "2", "3", "4", "5", "6", "7", "8"]:
        results = pc.smart_search(placeholder, limit=1)
        assert results == [], (
            f"smart_search({placeholder!r}) returned {results!r} — "
            "must be [] when query is provided but un-tokenizable. "
            "Returning the popularity-sort top-row stamps the same "
            "wrong catalog_match on every item."
        )


def test_short_alphabetic_query_returns_empty(seeded_catalog):
    """'ab' is 2 chars, but `_SMART_SEARCH_MIN_TOKEN_LEN=2` requires
    `len >= 2`, so it actually tokenizes. Test 'a' which is 1 char and
    gets stripped, plus stopword-only inputs like 'and' or 'the'."""
    pc = seeded_catalog
    for q in ["a", "x", "the", "and", "of"]:
        results = pc.smart_search(q, limit=1)
        assert results == [], (
            f"smart_search({q!r}) → {results!r}; expected [] (post-tokenize empty)"
        )


def test_truly_empty_query_still_returns_popularity_sort(seeded_catalog):
    """Browse-mode behavior must be preserved. Empty / whitespace-only
    query is the catalog UI's "show me top of catalog" call — must
    still fall through to popularity-sort."""
    pc = seeded_catalog
    for q in ["", "   ", "\t\n"]:
        results = pc.smart_search(q, limit=2)
        assert len(results) >= 1, (
            f"smart_search({q!r}) returned {len(results)} rows; "
            "browse mode must still surface popular products."
        )
        # The most-quoted product (DEMENTIA, 100 quotes) should be first
        assert results[0]["sku"] == "8", (
            f"Popularity sort broken: top result is {results[0].get('sku')!r}, "
            "expected the highest-times_quoted product (sku=8)"
        )


def test_descriptive_query_still_finds_real_match(seeded_catalog):
    """The fix must not break legitimate queries. Searching for the
    actual product description should still find the right row — not
    fall through to popularity-sort."""
    pc = seeded_catalog
    results = pc.smart_search("Love Velvet Letters Coloring Poster", limit=2)
    assert len(results) >= 1
    skus = [r.get("sku", "") for r in results]
    # Top result should be the coloring poster, not the DEMENTIA pack
    assert skus[0] == "STUFF2COLOR-LV01", (
        f"Real description should find STUFF2COLOR-LV01, got {skus}"
    )


def test_self_heal_clears_pre_fix_catalog_match():
    """Self-heal contract: any existing catalog_match without the new
    `match_confidence` field is from the old buggy code path (where
    search_catalog stamped the popularity-sort top row on placeholder pns).
    The new render-time enrichment must clear it so a fresh match runs.

    Without this, RFQ a5b09b56's 8 wrong DEMENTIA stamps would persist
    forever, since the existing gate skips re-evaluation when
    catalog_match is already set."""
    src = Path(__file__).resolve().parent.parent / "src/api/modules/routes_rfq.py"
    body = src.read_text(encoding="utf-8")
    fn_start = body.find("def _enrich_items_with_intel")
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    # Pre-fix dicts lack match_confidence; the heal must detect that
    assert '"match_confidence" not in' in fn_body, (
        "Self-heal must check for missing match_confidence to detect "
        "and clear pre-fix catalog_match dicts"
    )


def test_call_site_routes_rfq_uses_match_item():
    """_enrich_items_with_intel must NOT use search_catalog (which has
    the popularity-sort bug class). Source-level guard so a future
    refactor doesn't reintroduce the bug shape."""
    src = Path(__file__).resolve().parent.parent / "src/api/modules/routes_rfq.py"
    body = src.read_text(encoding="utf-8")
    # Around _enrich_items_with_intel, must call match_item not search_catalog
    fn_start = body.find("def _enrich_items_with_intel")
    assert fn_start > 0, "_enrich_items_with_intel function disappeared"
    # End boundary: next top-level def
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    assert "from src.agents.product_catalog import match_item" in fn_body, (
        "_enrich_items_with_intel must use match_item — Mike P0 2026-05-05"
    )
    assert "search_catalog(" not in fn_body, (
        "_enrich_items_with_intel must NOT call search_catalog — that "
        "function falls through to popularity-sort for placeholder pn "
        "(see project_catalog_match_cross_contamination_2026_05_05)"
    )


def test_call_site_routes_rfq_admin_uses_match_item():
    """api_rfq_price_intel — same guard as the read-side enrichment."""
    src = Path(__file__).resolve().parent.parent / "src/api/modules/routes_rfq_admin.py"
    body = src.read_text(encoding="utf-8")
    fn_start = body.find("def api_rfq_price_intel")
    assert fn_start > 0, "api_rfq_price_intel disappeared"
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    assert "from src.agents.product_catalog import match_item" in fn_body, (
        "api_rfq_price_intel must use match_item — Mike P0 2026-05-05"
    )
    assert "search_catalog(" not in fn_body, (
        "api_rfq_price_intel must NOT call search_catalog (popularity-sort fallthrough bug)"
    )
