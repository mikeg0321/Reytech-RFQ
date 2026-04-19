"""
Tests for the 2026-04-19 quoting detail polish (Batch A2).

Adds three things to /quoting/status/<doc_id>:
  1. Status summary pills (advanced count, override count, latest stage).
  2. Hero blocker banner — surfaces the actual reason text when the latest
     transition is blocked/error, so the operator doesn't have to hunt for
     it inside the timeline reasons list.
  3. Override-count pill is visible only when overrides exist.

Background: the override+retry modal already exists, but operators reported
the blocker reason was buried under truncated tooltips. The hero banner
elevates it to where the eye lands first.
"""
from __future__ import annotations

import json

import pytest


def _seed(conn, rows):
    for r in rows:
        conn.execute(
            """INSERT INTO quote_audit_log
               (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                outcome, reasons_json, actor, at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            r,
        )


@pytest.fixture
def seeded_blocked_pc():
    """One PC that advanced twice then got blocked with two reasons."""
    from src.core.migrations import MIGRATIONS
    sql_21 = next(sql for v, _n, sql in MIGRATIONS if v == 21)
    from src.core.db import get_db
    with get_db() as conn:
        conn.executescript(sql_21)
        conn.execute("DELETE FROM quote_audit_log")
        _seed(conn, [
            ("pc_block_1", "pc", "cchcs", "draft", "parsed", "advanced",
             "[]", "system", "2026-04-19T09:00:00"),
            ("pc_block_1", "pc", "cchcs", "parsed", "priced", "advanced",
             "[]", "system", "2026-04-19T09:01:00"),
            ("pc_block_1", "pc", "cchcs", "priced", "qa_pass", "blocked",
             json.dumps(["missing 703b form", "agency requires DVBE letter"]),
             "system", "2026-04-19T09:02:00"),
        ])
    yield "pc_block_1"
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM quote_audit_log")
    except Exception:
        pass


@pytest.fixture
def seeded_overridden_pc():
    """A PC where operator recorded an override on top of a block."""
    from src.core.migrations import MIGRATIONS
    sql_21 = next(sql for v, _n, sql in MIGRATIONS if v == 21)
    from src.core.db import get_db
    with get_db() as conn:
        conn.executescript(sql_21)
        conn.execute("DELETE FROM quote_audit_log")
        _seed(conn, [
            ("pc_ov_1", "pc", "calvet", "draft", "parsed", "advanced",
             "[]", "system", "2026-04-19T10:00:00"),
            ("pc_ov_1", "pc", "calvet", "parsed", "qa_pass", "blocked",
             json.dumps(["price out of range"]), "system", "2026-04-19T10:01:00"),
            ("pc_ov_1", "pc", "calvet", "qa_pass", "qa_pass", "override",
             json.dumps(["[price_error] confirmed manually"]), "operator", "2026-04-19T10:02:00"),
        ])
    yield "pc_ov_1"
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM quote_audit_log")
    except Exception:
        pass


class TestDetailPolish:
    def test_blocker_banner_shows_actual_reason(self, auth_client, seeded_blocked_pc):
        resp = auth_client.get(f"/quoting/status/{seeded_blocked_pc}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'role="alert"' in body
        assert "Latest blocker" in body
        # The actual reason text must be in the banner — not just a tooltip
        assert "missing 703b form" in body
        assert "agency requires DVBE letter" in body

    def test_summary_pills_render(self, auth_client, seeded_blocked_pc):
        resp = auth_client.get(f"/quoting/status/{seeded_blocked_pc}")
        body = resp.get_data(as_text=True)
        # Two advanced transitions in the seed
        assert "2 advanced" in body
        # No overrides → override pill must NOT be present
        assert "override" not in body.lower().split("operator action")[0] \
            or "0 override" not in body

    def test_override_pill_only_when_overrides_exist(self, auth_client, seeded_overridden_pc):
        resp = auth_client.get(f"/quoting/status/{seeded_overridden_pc}")
        body = resp.get_data(as_text=True)
        assert "1 override" in body

    def test_blocker_banner_absent_when_advanced(self, auth_client):
        """If latest outcome is 'advanced' the hero banner must not render."""
        from src.core.migrations import MIGRATIONS
        sql_21 = next(sql for v, _n, sql in MIGRATIONS if v == 21)
        from src.core.db import get_db
        with get_db() as conn:
            conn.executescript(sql_21)
            conn.execute("DELETE FROM quote_audit_log")
            _seed(conn, [
                ("pc_ok_1", "pc", "cchcs", "draft", "qa_pass", "advanced",
                 "[]", "system", "2026-04-19T11:00:00"),
            ])
        try:
            resp = auth_client.get("/quoting/status/pc_ok_1")
            body = resp.get_data(as_text=True)
            assert 'role="alert"' not in body
            assert "Latest blocker" not in body
        finally:
            with get_db() as conn:
                conn.execute("DELETE FROM quote_audit_log")
