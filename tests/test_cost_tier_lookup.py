"""Phase 2 regression: 3-tier cost lookup cascade.

Guards:
  * Tier ordering is catalog → past_quote → supplier_scrape (most authoritative
    wins; first hit short-circuits).
  * find_recent_quote_cost only returns operator-confirmed rows from
    quote_line_costs — Amazon ghosts cannot resurface.
  * Per-host throttle limits to 3 calls/sec/host.
  * S&S Cloudflare-fallback (where Amazon supplies the price) is reported
    with confidence='reference_only' so the UI can warn instead of accepting
    silently.

The lost-revenue exemplar this protects against:
  CalVet Barstow PC f81c4e9b (2026-04-24) — Phase 1 stopped Amazon from
  auto-filling unit_cost. Phase 2 adds the catalog-first / past-quote /
  supplier-scrape cascade so empty cells get filled accurately. Without this
  test file, a future PR could re-introduce Amazon-as-cost via Tier 2 reading
  from a polluted source, or via Tier 3 silently accepting a Cloudflare
  fallback price as a clean S&S quote.
"""
import os
import sqlite3
import tempfile
import time
import threading

import pytest


# ── Tier 2: find_recent_quote_cost ─────────────────────────────────────


@pytest.fixture
def temp_quote_line_costs_db(monkeypatch):
    """Build a temp DB with the quote_line_costs table seeded for testing."""
    tmp_dir = tempfile.mkdtemp()
    tmp_db = os.path.join(tmp_dir, "test.db")

    # Patch DB path
    import src.core.paths as paths_mod
    monkeypatch.setattr(paths_mod, "DATA_DIR", tmp_dir, raising=False)
    monkeypatch.setenv("REYTECH_DB_PATH", tmp_db)

    # Create the table directly (avoid full init_db dependency tree)
    conn = sqlite3.connect(tmp_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quote_line_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mfg_number TEXT, upc TEXT, description TEXT,
            cost REAL NOT NULL, cost_source TEXT NOT NULL,
            cost_source_url TEXT DEFAULT '',
            quote_number TEXT DEFAULT '', pc_id TEXT DEFAULT '',
            rfq_id TEXT DEFAULT '', supplier_name TEXT DEFAULT '',
            accepted_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()
    return tmp_db


def _insert_quote_line(db_path, **kw):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO quote_line_costs "
        "(mfg_number, upc, description, cost, cost_source, "
        " cost_source_url, supplier_name, pc_id, accepted_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            kw.get("mfg_number"),
            kw.get("upc"),
            kw.get("description", ""),
            kw["cost"],
            kw["cost_source"],
            kw.get("cost_source_url", ""),
            kw.get("supplier_name", ""),
            kw.get("pc_id", ""),
            kw.get("accepted_at", "2026-04-25T12:00:00"),
        ),
    )
    conn.commit()
    conn.close()


def _patch_get_db(monkeypatch, db_path):
    """Make src.core.db.get_db return a connection to our test DB."""
    from contextlib import contextmanager

    @contextmanager
    def fake_get_db():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    import src.core.db as core_db
    monkeypatch.setattr(core_db, "get_db", fake_get_db)


def test_find_recent_quote_cost_returns_operator_confirmed(temp_quote_line_costs_db, monkeypatch):
    _patch_get_db(monkeypatch, temp_quote_line_costs_db)
    _insert_quote_line(temp_quote_line_costs_db,
                       mfg_number="462C51", cost=400.00,
                       cost_source="operator", supplier_name="Grainger",
                       cost_source_url="https://grainger.com/.../462C51",
                       pc_id="pc_old", accepted_at="2026-04-25T10:00:00")
    from src.agents.product_catalog import find_recent_quote_cost
    hit = find_recent_quote_cost("462C51")
    assert hit is not None
    assert hit["cost"] == 400.00
    assert hit["cost_source"] == "operator"
    assert "Grainger" in hit["supplier_name"]


def test_find_recent_quote_cost_returns_most_recent(temp_quote_line_costs_db, monkeypatch):
    _patch_get_db(monkeypatch, temp_quote_line_costs_db)
    _insert_quote_line(temp_quote_line_costs_db, mfg_number="462C51",
                       cost=400.00, cost_source="operator", pc_id="pc_old",
                       accepted_at="2026-04-25T10:00:00")
    _insert_quote_line(temp_quote_line_costs_db, mfg_number="462C51",
                       cost=425.00, cost_source="operator", pc_id="pc_new",
                       accepted_at="2026-04-26T10:00:00")
    from src.agents.product_catalog import find_recent_quote_cost
    hit = find_recent_quote_cost("462C51")
    assert hit["cost"] == 425.00
    assert hit["pc_id"] == "pc_new"


def test_find_recent_quote_cost_refuses_non_operator(temp_quote_line_costs_db, monkeypatch):
    """Even if a non-operator row landed in quote_line_costs (shouldn't happen,
    but defensive), the lookup must REFUSE it. The Phase 1 lesson: never
    trust Amazon/SCPRS values as cost basis."""
    _patch_get_db(monkeypatch, temp_quote_line_costs_db)
    # Pretend something pollutes the table with a scraped value
    _insert_quote_line(temp_quote_line_costs_db, mfg_number="462C51",
                       cost=24.99, cost_source="amazon_scrape", pc_id="bad")
    from src.agents.product_catalog import find_recent_quote_cost
    hit = find_recent_quote_cost("462C51")
    assert hit is None, "Tier 2 must never surface non-operator costs"


def test_find_recent_quote_cost_no_input_returns_none():
    from src.agents.product_catalog import find_recent_quote_cost
    assert find_recent_quote_cost(None) is None
    assert find_recent_quote_cost("") is None
    assert find_recent_quote_cost(None, upc="") is None


# ── Per-host throttle ─────────────────────────────────────────────────


def test_host_throttle_limits_to_3_per_second():
    """3 quick calls to the same host pass without sleep; the 4th must wait."""
    from src.agents.cost_tier_lookup import _host_throttle, _HOST_LAST_CALL

    # Reset state for a clean test
    _HOST_LAST_CALL.clear()
    url = "https://example.com/test"

    t0 = time.monotonic()
    for _ in range(3):
        _host_throttle(url)
    fast = time.monotonic() - t0
    assert fast < 0.5, f"first 3 calls should be fast, took {fast:.2f}s"

    # 4th call must wait at least until the oldest rolls off (~1s window)
    t1 = time.monotonic()
    _host_throttle(url)
    waited = time.monotonic() - t1
    assert 0.4 < waited < 1.5, f"4th call should wait ~1s, waited {waited:.2f}s"


def test_host_throttle_independent_per_host():
    """Throttle must be per-host — calls to grainger don't slow ssww."""
    from src.agents.cost_tier_lookup import _host_throttle, _HOST_LAST_CALL
    _HOST_LAST_CALL.clear()

    # Saturate host A
    for _ in range(5):
        _host_throttle("https://a.com/x")

    # Host B should still be fast
    t0 = time.monotonic()
    _host_throttle("https://b.com/y")
    fast = time.monotonic() - t0
    assert fast < 0.1, f"host B should be unthrottled, took {fast:.2f}s"


# ── Tier cascade ordering ────────────────────────────────────────────


def test_lookup_tiers_returns_first_hit(monkeypatch):
    """When both Tier 1 and Tier 3 would return a result, Tier 1 wins
    (cascade short-circuits)."""
    from src.agents import cost_tier_lookup

    # Force Tier 1 to return a hit
    monkeypatch.setattr(cost_tier_lookup, "_tier1_catalog",
                        lambda mfg, upc: {"tier": "catalog", "cost": 100.0,
                                          "supplier": "Catalog", "url": "",
                                          "source": "Catalog",
                                          "confidence": "high", "raw": {}})
    # Tier 2 / 3 should never be called — assert by raising
    monkeypatch.setattr(cost_tier_lookup, "_tier2_past_quote",
                        lambda *a, **k: pytest.fail("Tier 2 called when Tier 1 hit"))
    monkeypatch.setattr(cost_tier_lookup, "_tier3_supplier_scrape",
                        lambda *a, **k: pytest.fail("Tier 3 called when Tier 1 hit"))

    rec = cost_tier_lookup.lookup_tiers({"mfg_number": "462C51"})
    assert rec["tier"] == "catalog"
    assert rec["cost"] == 100.0


def test_lookup_tiers_falls_through_to_tier3(monkeypatch):
    """When Tier 1 + Tier 2 miss, Tier 3 is called and returned."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_tier1_catalog", lambda *a, **k: None)
    monkeypatch.setattr(cost_tier_lookup, "_tier2_past_quote", lambda *a, **k: None)
    monkeypatch.setattr(cost_tier_lookup, "_tier3_supplier_scrape",
                        lambda item: {"tier": "supplier_scrape", "cost": 400.0,
                                     "supplier": "Grainger",
                                     "url": "https://grainger.com/x",
                                     "source": "Grainger (live)",
                                     "confidence": "high", "raw": {}})
    rec = cost_tier_lookup.lookup_tiers({"mfg_number": "462C51"})
    assert rec["tier"] == "supplier_scrape"
    assert rec["supplier"] == "Grainger"


def test_lookup_tiers_returns_none_when_all_miss(monkeypatch):
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_tier1_catalog", lambda *a, **k: None)
    monkeypatch.setattr(cost_tier_lookup, "_tier2_past_quote", lambda *a, **k: None)
    monkeypatch.setattr(cost_tier_lookup, "_tier3_supplier_scrape", lambda *a, **k: None)
    assert cost_tier_lookup.lookup_tiers({"mfg_number": "UNKNOWN"}) is None


def test_lookup_tiers_no_mfg_or_upc_returns_none():
    from src.agents.cost_tier_lookup import lookup_tiers
    assert lookup_tiers({}) is None
    assert lookup_tiers({"mfg_number": "", "upc": ""}) is None


# ── Tier 3 S&S Cloudflare-fallback signal ─────────────────────────────


def test_tier3_ssww_amazon_fallback_marked_reference_only(monkeypatch):
    """When S&S is Cloudflare-blocked, lookup_from_url returns a price with
    reference_source set (it came from Amazon, not the S&S page itself).
    Tier 3 must mark this as confidence='reference_only' so the UI displays
    a warning instead of presenting it as a clean S&S quote."""
    from src.agents import cost_tier_lookup

    monkeypatch.setattr(cost_tier_lookup,
                        "_host_throttle", lambda url: None)

    # Stub resolve_sku_url to return an S&S URL
    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "S&S Worldwide",
                                     "url": "https://www.ssww.com/item/W1234/"})

    # Stub lookup_from_url to return an Amazon-fallback (S&S blocked) result
    import src.agents.item_link_lookup as ill_mod
    monkeypatch.setattr(ill_mod, "lookup_from_url",
                        lambda url: {"supplier": "S&S Worldwide",
                                     "price": 24.99, "cost": 24.99,
                                     "url": "https://www.ssww.com/item/W1234/",
                                     "reference_source": "Amazon",
                                     "title": "Test item"})

    rec = cost_tier_lookup._tier3_supplier_scrape({"mfg_number": "W1234"})
    assert rec is not None
    assert rec["confidence"] == "reference_only", \
        f"S&S Amazon-fallback must be flagged as reference_only, got {rec['confidence']}"
    assert "blocked" in rec["source"].lower() or "reference" in rec["source"].lower()


def test_tier3_clean_grainger_marked_high_confidence(monkeypatch):
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "Grainger",
                                     "url": "https://www.grainger.com/product/462C51"})
    import src.agents.item_link_lookup as ill_mod
    monkeypatch.setattr(ill_mod, "lookup_from_url",
                        lambda url: {"supplier": "Grainger",
                                     "price": 400.00,
                                     "url": "https://www.grainger.com/product/462C51",
                                     "title": "Stanley RoamAlert Wrist Strap"})

    rec = cost_tier_lookup._tier3_supplier_scrape({"mfg_number": "462C51"})
    assert rec is not None
    assert rec["confidence"] == "high"
    assert rec["cost"] == 400.00
    assert rec["supplier"] == "Grainger"


def test_tier3_zero_price_returns_none(monkeypatch):
    """If lookup_from_url returns 0 (page loaded but no price found), Tier 3
    must return None — never present a $0 recommendation as a hit."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "Grainger",
                                     "url": "https://www.grainger.com/product/462C51"})
    import src.agents.item_link_lookup as ill_mod
    monkeypatch.setattr(ill_mod, "lookup_from_url",
                        lambda url: {"supplier": "Grainger",
                                     "price": 0, "cost": 0,
                                     "url": "https://www.grainger.com/product/462C51"})

    assert cost_tier_lookup._tier3_supplier_scrape({"mfg_number": "462C51"}) is None


# ── Phase 4-A: URL-host allowlist + item.item_link fallback ─────────────


def test_tier3_refuses_amazon_url_via_mfg_routing(monkeypatch):
    """If sku_url_resolver routes to amazon.com (e.g. Amazon Search for unknown
    SKU patterns), Tier 3 must REFUSE the URL — Amazon is retail, not a supplier
    cost basis. Phase 1 architectural rule."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "Amazon Search",
                                     "url": "https://www.amazon.com/s?k=foo"})
    import src.agents.item_link_lookup as ill_mod
    # If lookup_from_url were called, it'd return a price — assert it isn't
    def boom(url):
        raise AssertionError(f"Tier 3 must NOT call lookup_from_url for amazon.com host: {url}")
    monkeypatch.setattr(ill_mod, "lookup_from_url", boom)

    rec = cost_tier_lookup._tier3_supplier_scrape({"mfg_number": "FOO"})
    assert rec is None, "Amazon URL must be refused at allowlist gate"


def test_tier3_url_fallback_uses_item_link_when_mfg_misses(monkeypatch):
    """When MFG# routing returns no allowlisted URL but item.item_link is set
    to a known supplier domain (e.g. grainger.com), Tier 3 must use the
    item_link directly. This recovers Barstow-style PCs where the operator
    pasted a Grainger URL but the parser never set the MFG#."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    # No MFG# → resolve_sku_url returns nothing
    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "", "url": ""})

    # item_link is a Grainger URL → URL fallback fires
    import src.agents.item_link_lookup as ill_mod
    monkeypatch.setattr(ill_mod, "lookup_from_url",
                        lambda url: {"supplier": "Grainger",
                                     "price": 400.00,
                                     "url": url,
                                     "title": "Stanley RoamAlert"})

    rec = cost_tier_lookup._tier3_supplier_scrape({
        "mfg_number": "",
        "item_link": "https://www.grainger.com/product/STANLEY-462C51",
    })
    assert rec is not None
    assert rec["cost"] == 400.00
    assert rec["supplier"] == "Grainger"


def test_tier3_url_fallback_refuses_amazon_item_link(monkeypatch):
    """item_link pointing at amazon.com must NOT trigger Tier 3.
    Amazon is reference-only per Phase 1 architectural rule."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "", "url": ""})
    import src.agents.item_link_lookup as ill_mod
    def boom(url):
        raise AssertionError(f"Tier 3 must NOT scrape amazon.com item_link: {url}")
    monkeypatch.setattr(ill_mod, "lookup_from_url", boom)

    rec = cost_tier_lookup._tier3_supplier_scrape({
        "mfg_number": "",
        "item_link": "https://www.amazon.com/dp/B077JQYDTN",
    })
    assert rec is None


def test_tier3_url_fallback_refuses_unknown_host(monkeypatch):
    """Garbage URLs (operator pasted a Google result, walmart, etc.) must
    NOT be scraped — only the supplier allowlist is trusted as cost basis."""
    from src.agents import cost_tier_lookup
    monkeypatch.setattr(cost_tier_lookup, "_host_throttle", lambda url: None)

    import src.agents.sku_url_resolver as skures_mod
    monkeypatch.setattr(skures_mod, "resolve_sku_url",
                        lambda mfg: {"supplier": "", "url": ""})
    import src.agents.item_link_lookup as ill_mod
    def boom(url):
        raise AssertionError(f"Tier 3 must NOT scrape unknown host: {url}")
    monkeypatch.setattr(ill_mod, "lookup_from_url", boom)

    for url in [
        "https://www.walmart.com/ip/foo/12345",
        "https://www.google.com/search?q=foo",
        "https://example.com/random",
    ]:
        rec = cost_tier_lookup._tier3_supplier_scrape({
            "mfg_number": "", "item_link": url,
        })
        assert rec is None, f"unknown host {url} must be refused"


def test_host_in_allowlist_function():
    """Direct check on the allowlist gate."""
    from src.agents.cost_tier_lookup import _host_in_allowlist
    assert _host_in_allowlist("https://www.grainger.com/product/x") is True
    assert _host_in_allowlist("https://uline.com/Product/Detail/S-12345") is True
    assert _host_in_allowlist("https://www.ssww.com/item/W1234/") is True
    assert _host_in_allowlist("https://www.amazon.com/dp/B0FOO") is False
    assert _host_in_allowlist("https://www.walmart.com/ip/x") is False
    assert _host_in_allowlist("") is False
    assert _host_in_allowlist("not a url") is False
