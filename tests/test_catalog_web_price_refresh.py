"""Tests for refresh_catalog_web_prices.

Honors the user's business rule: items quoted in last ~2 years with URLs
get their web_lowest_price refreshed so MSRP stays current within the
45-day quote-validity window.

All tests stub search_product_price to avoid real API calls.
"""
import os
import sqlite3
from datetime import datetime, timedelta, timezone
import pytest


def _seed_product(db_path, pid, description, mfg_number, updated_at,
                  web_lowest_date=None, web_lowest_price=None, name=None):
    """Insert a product directly (bypasses add_to_catalog's side effects)."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT OR REPLACE INTO product_catalog
           (id, name, description, mfg_number, web_lowest_price, web_lowest_date,
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (pid, name or f"Product {pid}", description, mfg_number,
         web_lowest_price, web_lowest_date, updated_at, updated_at)
    )
    conn.commit()
    conn.close()


@pytest.fixture
def catalog_db(temp_data_dir, monkeypatch):
    """Point product_catalog module at the temp reytech.db and init tables.

    product_catalog.DB_PATH is set at import time, so monkeypatching
    DATA_DIR alone doesn't redirect catalog queries — DB_PATH must be
    patched directly.
    """
    import src.agents.product_catalog as _pc
    db_path = os.path.join(temp_data_dir, "reytech.db")
    monkeypatch.setattr(_pc, "DB_PATH", db_path)
    _pc.init_catalog_db()
    return db_path


@pytest.fixture
def stub_search(monkeypatch):
    """Stub search_product_price so tests never hit the real API."""
    calls = []

    def _install(response_fn):
        import src.agents.web_price_research as _wpr

        def _impl(description, part_number="", **kw):
            calls.append((description, part_number))
            return response_fn(description, part_number)

        monkeypatch.setattr(_wpr, "search_product_price", _impl)
        return calls

    return _install


class TestRefreshCatalogWebPrices:

    def test_skips_items_older_than_lookback(self, catalog_db, stub_search):
        # Item touched 3 years ago — out of 2-year lookback window
        three_years_ago = (datetime.now(timezone.utc) - timedelta(days=1100)).isoformat()
        _seed_product(catalog_db, 1, "Old item", "OLD-1", three_years_ago)

        calls = stub_search(lambda d, p: {"found": True, "price": 9.99, "source": "Amazon"})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(lookback_days=730, limit=10)
        assert result["ok"] is True
        assert result["scanned"] == 0
        assert result["checked"] == 0
        assert calls == []

    def test_fresh_item_skipped_when_checked_recently(self, catalog_db, stub_search):
        now = datetime.now(timezone.utc)
        _seed_product(catalog_db, 1, "Fresh item", "F-1",
                      (now - timedelta(days=30)).isoformat(),
                      web_lowest_date=(now - timedelta(days=1)).isoformat(),
                      web_lowest_price=10.00)

        calls = stub_search(lambda d, p: {"found": True, "price": 9.99})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(max_age_days=7, limit=10)
        assert result["ok"] is True
        assert result["scanned"] == 0
        assert result["checked"] == 0
        assert calls == []

    def test_stale_item_gets_refreshed(self, catalog_db, stub_search):
        now = datetime.now(timezone.utc)
        # Item quoted 100 days ago (within 2y lookback), last price-checked 30 days ago (stale)
        _seed_product(catalog_db, 1, "Stale item", "S-1",
                      (now - timedelta(days=100)).isoformat(),
                      web_lowest_date=(now - timedelta(days=30)).isoformat(),
                      web_lowest_price=15.00)

        stub_search(lambda d, p: {"found": True, "price": 12.50, "source": "NewVendor",
                                  "url": "https://example.com/s1", "cached": False})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(max_age_days=7, limit=10)
        assert result["ok"] is True
        assert result["checked"] == 1
        assert result["updated"] == 1

        conn = sqlite3.connect(catalog_db)
        row = conn.execute(
            "SELECT web_lowest_price, web_lowest_source FROM product_catalog WHERE id=1"
        ).fetchone()
        conn.close()
        assert row[0] == 12.50
        assert row[1] == "NewVendor"

    def test_not_found_still_stamps_check_date(self, catalog_db, stub_search):
        # If search returns found=False, we still stamp web_lowest_date so
        # the same miss isn't re-hit every refresh cycle.
        now = datetime.now(timezone.utc)
        _seed_product(catalog_db, 1, "Obscure item", "OBS-1",
                      (now - timedelta(days=50)).isoformat())

        stub_search(lambda d, p: {"found": False, "reason": "not on Amazon"})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(max_age_days=7, limit=10)
        assert result["ok"] is True
        assert result["checked"] == 1
        assert result["not_found"] == 1
        assert result["updated"] == 0

        conn = sqlite3.connect(catalog_db)
        row = conn.execute(
            "SELECT web_lowest_date, web_lowest_price FROM product_catalog WHERE id=1"
        ).fetchone()
        conn.close()
        assert row[0]  # date was stamped
        assert not row[1]  # price still null

    def test_price_zero_treated_as_not_found(self, catalog_db, stub_search):
        # search returns found=True but price=0 — treat as not_found
        now = datetime.now(timezone.utc)
        _seed_product(catalog_db, 1, "Test item", "T-1",
                      (now - timedelta(days=50)).isoformat())

        stub_search(lambda d, p: {"found": True, "price": 0, "source": ""})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(max_age_days=7, limit=10)
        assert result["not_found"] == 1
        assert result["updated"] == 0

        conn = sqlite3.connect(catalog_db)
        row = conn.execute(
            "SELECT web_lowest_price FROM product_catalog WHERE id=1"
        ).fetchone()
        conn.close()
        assert not row[0]

    def test_limit_caps_batch_size(self, catalog_db, stub_search):
        now = datetime.now(timezone.utc)
        for i in range(1, 6):
            _seed_product(catalog_db, i, f"Item {i}", f"P-{i}",
                          (now - timedelta(days=10 + i)).isoformat())

        calls = stub_search(lambda d, p: {"found": True, "price": 1.00, "source": "X"})

        from src.agents.product_catalog import refresh_catalog_web_prices
        result = refresh_catalog_web_prices(limit=2)
        assert result["scanned"] == 5  # full pool reported
        assert result["checked"] == 2  # but only limit=2 actually hit
        assert len(calls) == 2

    def test_route_surfaces_result(self, client, catalog_db, stub_search):
        now = datetime.now(timezone.utc)
        _seed_product(catalog_db, 1, "Route test item", "RT-1",
                      (now - timedelta(days=20)).isoformat())

        stub_search(lambda d, p: {"found": True, "price": 5.55, "source": "Route"})

        r = client.post("/api/catalog/refresh-web-prices",
                        json={"limit": 10, "max_age_days": 7},
                        content_type="application/json")
        assert r.status_code == 200, r.data
        body = r.get_json()
        assert body["ok"] is True
        assert body["checked"] >= 1
