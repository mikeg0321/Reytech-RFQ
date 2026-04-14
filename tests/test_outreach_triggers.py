"""CRM outreach triggers — computed from quotes table, surfaced on
/buyer-intelligence and /api/oracle/outreach-triggers.

Heuristic: buyers with ≥ min_quotes historical quotes AND
≥ silence_days since their most recent quote, ranked by
(avg_quote_total × win_rate × total_quotes).
"""
import sqlite3
import pytest
from datetime import datetime, timedelta


def _seed_quote(institution: str, status: str, total: float,
                quote_number: str, created_at: str = None,
                agency: str = None):
    from src.core.db import DB_PATH
    if created_at is None:
        created_at = datetime.now().isoformat()
    if agency is None:
        agency = institution
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(
        """INSERT INTO quotes
           (quote_number, created_at, institution, agency, total, status,
            is_test, margin_pct, total_cost, items_count)
           VALUES (?, ?, ?, ?, ?, ?, 0, 30, ?, 1)""",
        (quote_number, created_at, institution, agency, total, status,
         total * 0.7),
    )
    conn.commit()
    conn.close()


class TestComputeOutreachTriggers:
    def test_silent_buyer_with_history_surfaces(self, auth_client, temp_data_dir):
        # CCHCS has 6 quotes from 120 days ago → should be a trigger
        old = (datetime.now() - timedelta(days=120)).isoformat()
        for i in range(4):
            _seed_quote("cchcs", "won", 5000, f"OLD-W-{i}", created_at=old)
        for i in range(2):
            _seed_quote("cchcs", "lost", 5000, f"OLD-L-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        names = [t["institution"] for t in d["triggers"]]
        assert "cchcs" in names

    def test_recent_buyer_does_not_surface(self, auth_client, temp_data_dir):
        """A buyer with recent quotes shouldn't be flagged."""
        recent = (datetime.now() - timedelta(days=5)).isoformat()
        for i in range(6):
            _seed_quote("calvet", "won", 3000, f"REC-{i}", created_at=recent)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        d = r.get_json()
        names = [t["institution"] for t in d["triggers"]]
        assert "calvet" not in names

    def test_thin_history_buyer_does_not_surface(self, auth_client, temp_data_dir):
        """A buyer with only 2 historical quotes is below min_quotes=5."""
        old = (datetime.now() - timedelta(days=100)).isoformat()
        # Unique institution name to avoid collisions with test-suite seed data
        INST = "thintest_only_xyz"
        for i in range(2):
            _seed_quote(INST, "won", 10000, f"DSH-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60&min_quotes=5")
        d = r.get_json()
        names = [t["institution"] for t in d["triggers"]]
        assert INST not in names

    def test_high_win_rate_ranks_above_low(self, auth_client, temp_data_dir):
        """Two buyers, both silent, same volume — the one with higher
        win rate should rank first because lost-revenue potential is
        higher."""
        old = (datetime.now() - timedelta(days=100)).isoformat()
        # Buyer A: 10 quotes, 8 wins
        for i in range(8):
            _seed_quote("bigwinner", "won", 5000, f"BW-W-{i}", created_at=old)
        for i in range(2):
            _seed_quote("bigwinner", "lost", 5000, f"BW-L-{i}", created_at=old)
        # Buyer B: 10 quotes, 2 wins
        for i in range(2):
            _seed_quote("mostlyloser", "won", 5000, f"ML-W-{i}", created_at=old)
        for i in range(8):
            _seed_quote("mostlyloser", "lost", 5000, f"ML-L-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        d = r.get_json()
        names = [t["institution"] for t in d["triggers"]]
        assert "bigwinner" in names
        assert "mostlyloser" in names
        assert names.index("bigwinner") < names.index("mostlyloser")

    def test_days_since_last_quote_calculated(self, auth_client, temp_data_dir):
        old = (datetime.now() - timedelta(days=95)).isoformat()
        for i in range(6):
            _seed_quote("cchcs", "won", 4000, f"CC-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers")
        d = r.get_json()
        cchcs = next(t for t in d["triggers"] if t["institution"] == "cchcs")
        # Seeded at 95 days ago — allow 1-2 day fuzz
        assert cchcs["days_since_last_quote"] is not None
        assert 93 <= cchcs["days_since_last_quote"] <= 97

    def test_silence_days_clamp(self, auth_client):
        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=5")
        assert r.status_code == 200
        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=99999")
        assert r.status_code == 200
        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=abc")
        assert r.status_code == 200

    def test_ignores_test_quotes(self, auth_client, temp_data_dir):
        """is_test=1 must not leak into outreach triggers."""
        from src.core.db import DB_PATH
        old = (datetime.now() - timedelta(days=100)).isoformat()
        conn = sqlite3.connect(DB_PATH, timeout=10)
        for i in range(8):
            conn.execute(
                """INSERT INTO quotes
                   (quote_number, created_at, institution, agency, total, status,
                    is_test, margin_pct, total_cost, items_count)
                   VALUES (?, ?, 'testonly', 'testonly', 1000, 'won', 1, 30, 700, 1)""",
                (f"TEST-{i}", old),
            )
        conn.commit()
        conn.close()

        r = auth_client.get("/api/oracle/outreach-triggers")
        d = r.get_json()
        names = [t["institution"] for t in d["triggers"]]
        assert "testonly" not in names


class TestPageIntegration:
    def test_outreach_section_renders_when_triggers_exist(self, auth_client, temp_data_dir):
        old = (datetime.now() - timedelta(days=100)).isoformat()
        for i in range(6):
            _seed_quote("cchcs", "won", 5000, f"OLD-{i}", created_at=old)
        r = auth_client.get("/buyer-intelligence")
        assert r.status_code == 200
        assert b"Reach out" in r.data
        assert b"CCHCS" in r.data

    def test_outreach_section_hidden_when_empty(self, auth_client, temp_data_dir):
        """No outreach triggers → section should not render. Use a
        very short silence window that all pre-seeded data beats."""
        # silence_days=14 is the floor; seed data from the fixture is
        # all very recent so nothing should qualify
        r = auth_client.get("/buyer-intelligence?silence_days=14")
        assert r.status_code == 200
        # No explicit outreach-section marker should appear
        assert b"gone quiet" not in r.data

    def test_silence_days_param_plumbed_to_compute(self, auth_client, temp_data_dir):
        """A buyer silent 40d should surface in the outreach API at
        silence_days=30 but not at silence_days=60. Uses the JSON
        endpoint rather than the HTML page because the buyer also
        appears in the top-buyers-by-volume table regardless of
        silence window, which would confuse a full-page text search."""
        old = (datetime.now() - timedelta(days=40)).isoformat()
        INST = "silencetest_uniq"
        for i in range(6):
            _seed_quote(INST, "won", 5000, f"MS-{i}", created_at=old)

        r30 = auth_client.get("/api/oracle/outreach-triggers?silence_days=30")
        names30 = [t["institution"] for t in r30.get_json()["triggers"]]
        assert INST in names30

        r60 = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        names60 = [t["institution"] for t in r60.get_json()["triggers"]]
        assert INST not in names60
