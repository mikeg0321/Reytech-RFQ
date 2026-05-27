"""Tests for the orphan-review queue (2026-05-03 — feat/orphan-review-queue).

Covers:
  • src.core.orders_link_orphans.find_quote_candidates — scoring tiers
  • mark_intentional_orphan + link_orphan_to_quote helpers
  • routes_orders_full.* — /api/orders/orphan-review JSON shape,
    POST link, POST mark-intentional, /orders/orphan-review page render

The compute layer is exercised against a real isolated SQLite DB via
the conftest fixtures so the SQL paths are touched, not just the
Python tier-scoring math.
"""
from __future__ import annotations

import json

import pytest


# ─── tier scoring: find_quote_candidates ──────────────────────────────


class TestFuzzyCandidateScoring:

    def test_exact_po_match_scores_100(self, seed_db_quote):
        """PO match outranks every other tier."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        seed_db_quote("Q-PO-EXACT", agency="cchcs", total=1000.00)
        # set the po on the quote we just seeded
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET po_number = ?, sent_at = ? WHERE quote_number = ?",
                ("PO-12345", "2026-05-01T10:00:00", "Q-PO-EXACT"),
            )
            conn.commit()
            orphan = {
                "id": "ORD-X", "po_number": "PO-12345", "po_canonical": "PO-12345",
                "agency": "cchcs", "total": 1000.00,
                "created_at": "2026-05-01T12:00:00",
            }
            cands = find_quote_candidates(conn, orphan)
        assert cands, "should return at least one candidate"
        assert cands[0]["quote_number"] == "Q-PO-EXACT"
        assert cands[0]["score"] == 100
        assert cands[0]["tier"] == "po_exact"

    def test_total_agency_60d_scores_80(self, seed_db_quote):
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        seed_db_quote("Q-RECENT", agency="cchcs", total=500.00)
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET sent_at = ? WHERE quote_number = ?",
                ("2026-04-15T10:00:00", "Q-RECENT"),
            )
            conn.commit()
            orphan = {
                "id": "ORD-Y", "po_number": "", "po_canonical": "",
                "agency": "CCHCS", "total": 500.00,
                "created_at": "2026-05-01T00:00:00",
            }
            cands = find_quote_candidates(conn, orphan)
        assert cands[0]["score"] == 80
        assert cands[0]["tier"] == "total_agency_60d"

    def test_agency_loose_total_5pct(self, seed_db_quote):
        """5% drift + same agency = TIER_TOTAL_AGENCY_LOOSE (40)."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        seed_db_quote("Q-LOOSE", agency="cchcs", total=1000.00)
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET sent_at = ? WHERE quote_number = ?",
                ("2026-04-30T10:00:00", "Q-LOOSE"),
            )
            conn.commit()
            orphan = {
                "id": "ORD-Z", "po_number": "", "po_canonical": "",
                "agency": "cchcs", "total": 1040.00,  # 4% over → loose tier
                "created_at": "2026-05-01T00:00:00",
            }
            cands = find_quote_candidates(conn, orphan)
        assert cands[0]["score"] == 40
        assert cands[0]["tier"] == "total_agency_loose"

    def test_no_total_no_po_returns_empty(self):
        """Orphan with no PO and no positive total has nothing to score."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        with get_db() as conn:
            orphan = {"id": "ORD-Q", "po_number": "", "po_canonical": "",
                      "agency": "", "total": 0, "created_at": ""}
            cands = find_quote_candidates(conn, orphan)
        assert cands == []

    def test_excludes_test_quotes(self, seed_db_quote):
        """is_test=1 quote is invisible to candidate finder."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        seed_db_quote("Q-TEST", agency="cchcs", total=750.00)
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET is_test = 1, sent_at = ? WHERE quote_number = ?",
                ("2026-04-30T10:00:00", "Q-TEST"),
            )
            conn.commit()
            orphan = {"id": "ORD-T", "po_number": "", "po_canonical": "",
                      "agency": "cchcs", "total": 750.00,
                      "created_at": "2026-05-01T00:00:00"}
            cands = find_quote_candidates(conn, orphan)
        assert cands == []

    def test_institution_named_quote_resolves_to_parent_agency(self, seed_db_quote):
        """Regression for the 2026-05-04 prod triage finding: quotes write
        the institution NAME ("CSP California State Prison - Sacramento")
        into the agency column while orders carry the canonical agency
        code ("CCHCS"). Naive lower+strip leaves these unmatched, so 55 of
        64 prod orphans landed in tier `total_only` (score 20) instead of
        `total_agency_60d` (score 80) despite Δ=0% and d<30. After
        canonicalizing through facility_registry, both sides resolve to
        'cchcs' and the high-tier match fires.
        """
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        seed_db_quote(
            "Q-CCHCS-INSTNAME",
            agency="CSP California State Prison - Sacramento",
            total=99.69,
        )
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET sent_at = ? WHERE quote_number = ?",
                ("2026-04-30T10:00:00", "Q-CCHCS-INSTNAME"),
            )
            conn.commit()
            orphan = {
                "id": "ORD-INSTNAME", "po_number": "", "po_canonical": "",
                "agency": "CCHCS", "total": 99.69,
                "created_at": "2026-05-01T00:00:00",
            }
            cands = find_quote_candidates(conn, orphan)
        assert cands, "expected the candidate to surface — naive matcher would have returned []"
        top = cands[0]
        assert top["quote_number"] == "Q-CCHCS-INSTNAME"
        assert top["score"] == 80, (
            f"expected score 80 (total_agency_60d) after parent_agency "
            f"canonicalization, got {top['score']} ({top['tier']})"
        )
        assert top["tier"] == "total_agency_60d"

    def test_score_sort_prioritizes_higher_tier(self, seed_db_quote):
        """When multiple candidates fire, higher score wins."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import find_quote_candidates
        # Loose match (score 40)
        seed_db_quote("Q-LOOSE", agency="cchcs", total=1050.00)
        # Tight match (score 80)
        seed_db_quote("Q-TIGHT", agency="cchcs", total=1000.00)
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET sent_at = ? WHERE quote_number IN (?, ?)",
                ("2026-04-30T10:00:00", "Q-LOOSE", "Q-TIGHT"),
            )
            conn.commit()
            orphan = {"id": "ORD-S", "po_number": "", "po_canonical": "",
                      "agency": "cchcs", "total": 1000.00,
                      "created_at": "2026-05-01T00:00:00"}
            cands = find_quote_candidates(conn, orphan)
        assert len(cands) >= 2
        assert cands[0]["quote_number"] == "Q-TIGHT"
        assert cands[0]["score"] > cands[1]["score"]


# ─── mark_intentional / link helpers ───────────────────────────────────


class TestOrphanActions:

    def test_mark_intentional_flips_flag(self):
        """Once flipped, find_orphan_orders no longer surfaces the row."""
        from src.core.db import get_db
        from src.core.orders_link_orphans import (
            find_orphan_orders, mark_intentional_orphan,
        )
        with get_db() as conn:
            conn.execute("""
                INSERT INTO orders (id, po_number, agency, total, status,
                                    created_at, is_test, is_intentional_orphan)
                VALUES ('ORD-INT', 'PO-X', 'cchcs', 100.00, 'shipped',
                        '2026-05-01T00:00:00', 0, 0)
            """)
            conn.commit()
            before = [o["id"] for o in find_orphan_orders(conn)]
            assert "ORD-INT" in before
            flipped = mark_intentional_orphan(conn, "ORD-INT", actor="test")
            conn.commit()
            assert flipped is True
            after = [o["id"] for o in find_orphan_orders(conn)]
            assert "ORD-INT" not in after

    def test_mark_intentional_idempotent(self):
        from src.core.db import get_db
        from src.core.orders_link_orphans import mark_intentional_orphan
        with get_db() as conn:
            conn.execute("""
                INSERT INTO orders (id, po_number, agency, total, created_at,
                                    is_test, is_intentional_orphan)
                VALUES ('ORD-IDM', 'PO-Y', 'cchcs', 50, '2026-05-01T00:00:00',
                        0, 0)
            """)
            conn.commit()
            assert mark_intentional_orphan(conn, "ORD-IDM", actor="t") is True
            conn.commit()
            assert mark_intentional_orphan(conn, "ORD-IDM", actor="t") is False


# ─── route plumbing ────────────────────────────────────────────────────


class TestOrphanReviewRoutes:

    def test_review_endpoint_returns_orphans(self, auth_client, seed_db_quote):
        """The list endpoint returns orphan rows + candidates JSON."""
        from src.core.db import get_db
        seed_db_quote("Q-CAND", agency="cchcs", total=600.00)
        with get_db() as conn:
            conn.execute(
                "UPDATE quotes SET sent_at = ? WHERE quote_number = ?",
                ("2026-04-30T10:00:00", "Q-CAND"),
            )
            conn.execute("""
                INSERT INTO orders (id, po_number, agency, total, status,
                                    created_at, is_test, is_intentional_orphan)
                VALUES ('ORD-RVW', '', 'cchcs', 600.00, 'shipped',
                        '2026-05-01T00:00:00', 0, 0)
            """)
            conn.commit()

        resp = auth_client.get("/api/orders/orphan-review")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["ok"] is True
        ids = [o["id"] for o in body["orphans"]]
        assert "ORD-RVW" in ids
        rvw = next(o for o in body["orphans"] if o["id"] == "ORD-RVW")
        cand_qns = [c["quote_number"] for c in rvw["candidates"]]
        assert "Q-CAND" in cand_qns

    def test_link_endpoint_rejects_missing_quote_number(self, auth_client):
        resp = auth_client.post("/api/orders/ORD-X/link-quote",
                                data="{}", content_type="application/json")
        assert resp.status_code == 400
        body = json.loads(resp.data)
        assert "quote_number required" in body["error"]

    def test_link_endpoint_returns_409_on_non_orphan(self, auth_client):
        """If the order doesn't exist or isn't orphan, route returns 409."""
        resp = auth_client.post(
            "/api/orders/does_not_exist/link-quote",
            data=json.dumps({"quote_number": "Q-X"}),
            content_type="application/json",
        )
        assert resp.status_code == 409

    def test_mark_intentional_flips_on_post(self, auth_client):
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO orders (id, po_number, agency, total, created_at,
                                    is_test, is_intentional_orphan)
                VALUES ('ORD-MI', 'PO-Z', 'cchcs', 99, '2026-05-01T00:00:00',
                        0, 0)
            """)
            conn.commit()
        resp = auth_client.post("/api/orders/ORD-MI/mark-intentional")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["ok"] is True
        assert body["flipped"] is True

    def test_page_renders(self, auth_client):
        """Smoke: the page route returns 200 + has the expected testid."""
        resp = auth_client.get("/orders/orphan-review")
        assert resp.status_code == 200
        assert b'data-testid="orph-list"' in resp.data
        assert b"Orphan Order Review" in resp.data


# ─── auth gate ─────────────────────────────────────────────────────────


class TestOrphanReviewAuthGate:

    def test_review_requires_auth(self, anon_client):
        resp = anon_client.get("/api/orders/orphan-review")
        assert resp.status_code in (401, 403)

    def test_link_requires_auth(self, anon_client):
        resp = anon_client.post("/api/orders/X/link-quote",
                                data=json.dumps({"quote_number": "Q"}),
                                content_type="application/json")
        assert resp.status_code in (401, 403)

    def test_mark_requires_auth(self, anon_client):
        resp = anon_client.post("/api/orders/X/mark-intentional")
        assert resp.status_code in (401, 403)

    def test_page_requires_auth(self, anon_client):
        resp = anon_client.get("/orders/orphan-review")
        assert resp.status_code in (401, 403)
