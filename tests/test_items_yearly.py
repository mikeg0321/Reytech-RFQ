"""Phase 4.5: per-item year-by-year trajectory tests."""

import json
from datetime import datetime

from src.core.db import get_db


def _seed_quote(qnum, status, agency, year, items):
    created = f"{year}-06-15T12:00:00"
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, institution,
                                line_items, total, created_at, is_test)
            VALUES (?, ?, ?, ?, ?, 100.0, ?, 0)
        """, (qnum, status, agency, agency, json.dumps(items), created))
        conn.commit()


class TestItemsYearly:
    def test_empty_returns_empty_items(self, client):
        r = client.get("/api/oracle/items-yearly")
        body = r.get_json()
        assert body["ok"] is True
        assert body["items"] == []

    def test_buckets_by_item_and_year(self, client):
        # Same item across years
        _seed_quote("IY-1", "won", "X", 2024,
                    [{"description": "Glove Medium Powder Free"}])
        _seed_quote("IY-2", "won", "X", 2024,
                    [{"description": "Glove Medium Powder-Free Box"}])
        _seed_quote("IY-3", "lost", "X", 2025,
                    [{"description": "Glove Medium Powder Free"}])
        _seed_quote("IY-4", "lost", "X", 2025,
                    [{"description": "Glove Medium Powder Free"}])
        r = client.get("/api/oracle/items-yearly?min_quotes=1")
        body = r.get_json()
        items = body["items"]
        # Should bucket the glove descriptions together
        glove_items = [i for i in items
                       if "glove" in i["item_key"]]
        assert len(glove_items) == 1
        gi = glove_items[0]
        assert gi["total_quotes"] == 4
        assert gi["total_wins"] == 2
        assert gi["total_losses"] == 2
        years = {y["year"]: y for y in gi["years"]}
        assert "2024" in years
        assert years["2024"]["w"] == 2
        assert years["2025"]["l"] == 2
        assert gi["yoy_delta_pts"] == -100.0  # 100% → 0%

    def test_only_degrading_filter(self, client):
        # Improving item
        _seed_quote("IY-IM-1", "lost", "X", 2024, [{"description": "Stable Item"}])
        _seed_quote("IY-IM-2", "won", "X", 2025, [{"description": "Stable Item"}])
        # Degrading item
        _seed_quote("IY-DG-1", "won", "X", 2024, [{"description": "Bad Item"}])
        _seed_quote("IY-DG-2", "lost", "X", 2025, [{"description": "Bad Item"}])
        r = client.get("/api/oracle/items-yearly?min_quotes=1&only_degrading=1")
        body = r.get_json()
        keys = {i["item_key"] for i in body["items"]}
        assert any("bad" in k for k in keys)
        assert not any("stable" in k for k in keys)

    def test_min_quotes_filter(self, client):
        _seed_quote("IY-MIN-1", "won", "X", 2024, [{"description": "Rare Item"}])
        for i in range(5):
            _seed_quote(f"IY-MIN-{10+i}", "won", "X", 2024,
                        [{"description": "Common Item"}])
        r = client.get("/api/oracle/items-yearly?min_quotes=3")
        body = r.get_json()
        keys = {i["item_key"] for i in body["items"]}
        assert any("common" in k for k in keys)
        assert not any("rare" in k for k in keys)

    def test_agency_filter(self, client):
        _seed_quote("IY-AG-1", "won", "Veterans Home Barstow", 2024,
                    [{"description": "Specific Item Test"}])
        _seed_quote("IY-AG-2", "lost", "CDCR Sacramento", 2024,
                    [{"description": "Specific Item Test"}])
        r = client.get("/api/oracle/items-yearly?agency=Barstow&min_quotes=1")
        body = r.get_json()
        items = [i for i in body["items"] if "specific" in i["item_key"]]
        assert len(items) == 1
        assert items[0]["total_wins"] == 1
        assert items[0]["total_losses"] == 0
