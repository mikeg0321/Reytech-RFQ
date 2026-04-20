"""Quoting Health Dashboard — smoke + contract tests.

Guards:
  - /health/quoting renders with an empty DB (defensive path).
  - /api/health/quoting returns the expected keys.
  - With seeded utilization_events the classifier activity / confidence
    distribution / top errors aggregations are correct.
  - A seeded ingest.classify_crashed event surfaces in recent_crashes
    with its context fields unpacked.
"""
import json
import sqlite3

import pytest


def _seed_event(feature: str, context: dict, ok: bool = True,
                duration_ms: int = 0, created_at: str = None):
    """Low-level helper that bypasses the async flusher so tests see
    their own writes immediately."""
    from src.core.db import DB_PATH
    from datetime import datetime
    if created_at is None:
        created_at = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(
        """INSERT INTO utilization_events
           (feature, context, user, duration_ms, ok, created_at)
           VALUES (?, ?, '', ?, ?, ?)""",
        (feature, json.dumps(context), duration_ms, 1 if ok else 0, created_at),
    )
    conn.commit()
    conn.close()


class TestQuotingHealthPage:
    def test_page_renders_with_empty_db(self, auth_client):
        r = auth_client.get("/health/quoting")
        assert r.status_code == 200
        assert b"Quoting Health" in r.data
        # Empty state messaging
        assert b"classifier_v2" in r.data

    def test_page_accepts_days_param(self, auth_client):
        r = auth_client.get("/health/quoting?days=30")
        assert r.status_code == 200
        assert b"last 30d" in r.data

    def test_days_param_clamped(self, auth_client):
        """Out-of-range days param must clamp, not crash."""
        r = auth_client.get("/health/quoting?days=9999")
        assert r.status_code == 200
        r = auth_client.get("/health/quoting?days=abc")
        assert r.status_code == 200

    def test_json_endpoint_shape(self, auth_client):
        r = auth_client.get("/api/health/quoting?days=7")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["days"] == 7
        for key in (
            "flag_card", "classifier_1d", "classifier_window",
            "confidence", "funnel_1d", "funnel_window", "margin",
            "top_errors", "recent_crashes",
        ):
            assert key in d, f"missing {key}"


class TestHealthAggregations:
    def test_classifier_activity_counts_invocations(self, auth_client):
        _seed_event("ingest.process_buyer_request",
                    {"shape": "cchcs_packet", "agency": "cchcs",
                     "confidence": 0.90, "file_count": 1},
                    ok=True, duration_ms=120)
        _seed_event("ingest.process_buyer_request",
                    {"shape": "email_only", "agency": "other",
                     "confidence": 0.40, "file_count": 0},
                    ok=True, duration_ms=80)

        d = auth_client.get("/api/health/quoting?days=7").get_json()
        assert d["classifier_window"]["invocations"] == 2
        assert d["classifier_window"]["crashes"] == 0

    def test_confidence_distribution_buckets(self, auth_client):
        # high
        _seed_event("ingest.process_buyer_request",
                    {"confidence": 0.92, "shape": "cchcs_packet", "agency": "cchcs"},
                    ok=True)
        _seed_event("ingest.process_buyer_request",
                    {"confidence": 0.88, "shape": "cchcs_packet", "agency": "cchcs"},
                    ok=True)
        # mid
        _seed_event("ingest.process_buyer_request",
                    {"confidence": 0.70, "shape": "pc_704_pdf_fillable", "agency": "cchcs"},
                    ok=True)
        # low
        _seed_event("ingest.process_buyer_request",
                    {"confidence": 0.40, "shape": "email_only", "agency": "other"},
                    ok=True)

        d = auth_client.get("/api/health/quoting?days=7").get_json()
        c = d["confidence"]
        assert c["high"] == 2
        assert c["mid"] == 1
        assert c["low"] == 1
        assert c["total"] == 4

    def test_classify_crashed_surfaces_in_recent(self, auth_client):
        _seed_event("ingest.classify_crashed", {
            "error": "KeyError: 'Annots'",
            "error_type": "KeyError",
            "file_count": 1,
            "attachment_names": ["weird.pdf"],
            "sender": "ashley.russ@cdcr.ca.gov",
        }, ok=False)

        d = auth_client.get("/api/health/quoting?days=7").get_json()
        assert len(d["recent_crashes"]) == 1
        crash = d["recent_crashes"][0]
        assert crash["error_type"] == "KeyError"
        assert "KeyError" in crash["error"]
        assert crash["file_count"] == 1
        assert crash["sender"] == "ashley.russ@cdcr.ca.gov"

        # Classifier activity card also picks it up
        assert d["classifier_window"]["crashes"] == 1

    def test_top_errors_only_lists_errored_features(self, auth_client):
        # A healthy feature
        for _ in range(5):
            _seed_event("ok.feature", {}, ok=True)
        # A noisy feature
        for _ in range(3):
            _seed_event("bad.feature", {}, ok=False)
        _seed_event("bad.feature", {}, ok=True)  # 1 success, 3 errors

        d = auth_client.get("/api/health/quoting?days=7").get_json()
        names = [r["feature"] for r in d["top_errors"]]
        assert "bad.feature" in names
        assert "ok.feature" not in names
        bad = next(r for r in d["top_errors"] if r["feature"] == "bad.feature")
        assert bad["errors"] == 3
        assert bad["uses"] == 4
        assert bad["error_rate"] == 75.0


class TestFlagCard:
    def test_flag_card_reflects_runtime_state(self, auth_client):
        from src.core.flags import set_flag, _cache_clear_all
        set_flag("ingest.classifier_v2_enabled", "true")
        _cache_clear_all()

        d = auth_client.get("/api/health/quoting").get_json()
        assert d["flag_card"]["classifier_v2_on"] is True

        set_flag("ingest.classifier_v2_enabled", "false")
        _cache_clear_all()
        d = auth_client.get("/api/health/quoting").get_json()
        assert d["flag_card"]["classifier_v2_on"] is False


class TestDbBloat:
    """Diagnostic endpoint for ops to see which tables bloat the DB."""

    def test_returns_tables_sorted_by_size(self, auth_client):
        r = auth_client.get("/api/health/db-bloat")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "db_size_mb" in d
        assert "tables" in d
        assert isinstance(d["tables"], list)
        # At least the core tables must be present in a fresh init.
        names = {t["table"] for t in d["tables"]}
        assert "rfqs" in names
        assert "price_checks" in names
        # Every entry must report a row_count.
        for entry in d["tables"]:
            assert "table" in entry
            assert "row_count" in entry

    def test_dbstat_sort_when_available(self, auth_client):
        """If dbstat is compiled in, tables must be sorted by mb desc."""
        d = auth_client.get("/api/health/db-bloat").get_json()
        if not d.get("dbstat_available"):
            pytest.skip("dbstat virtual table not available")
        sizes = [t.get("mb", 0) for t in d["tables"]]
        assert sizes == sorted(sizes, reverse=True), (
            "tables must be ranked largest-first when dbstat available"
        )

    def test_rfq_files_breakdown_shape(self, auth_client):
        """rfq_files is the DB's biggest single offender — must report
        per-category/file_type size so trim policy can be picked without
        blindly deleting."""
        d = auth_client.get("/api/health/db-bloat").get_json()
        assert "rfq_files_breakdown" in d
        assert isinstance(d["rfq_files_breakdown"], list)
        for entry in d["rfq_files_breakdown"]:
            for key in ("category", "file_type", "count", "mb", "oldest", "newest"):
                assert key in entry, f"missing {key} in breakdown entry"

    def test_rfq_files_orphan_and_dead_parent_counts(self, auth_client):
        d = auth_client.get("/api/health/db-bloat").get_json()
        assert "rfq_files_orphans" in d
        assert "count" in d["rfq_files_orphans"]
        assert "mb" in d["rfq_files_orphans"]
        assert "rfq_files_dead_parents" in d
        assert "count" in d["rfq_files_dead_parents"]
        assert "mb" in d["rfq_files_dead_parents"]

    def test_rfq_files_size_histogram(self, auth_client):
        d = auth_client.get("/api/health/db-bloat").get_json()
        hist = d.get("rfq_files_size_histogram", {})
        for bucket in ("lt_100kb", "100kb_1mb", "1mb_5mb", "gt_5mb", "biggest_bytes"):
            assert bucket in hist, f"missing {bucket} in size histogram"


class TestTrimRfqFiles:
    """Destructive trim endpoint — verify dry-run is default and confirm=YES
    is required for actual delete, so a forgotten flag can never wipe data."""

    def _seed_rfq_file(self, rfq_id, filename="a.pdf", size=1024,
                       category="attachment", file_type="attachment"):
        """Insert an rfq_files row directly, bypassing the BLOB data column."""
        from src.core.db import DB_PATH
        import sqlite3, uuid
        file_id = f"f_{uuid.uuid4().hex[:10]}"
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rfq_files (
                    id TEXT PRIMARY KEY,
                    rfq_id TEXT NOT NULL,
                    filename TEXT,
                    file_type TEXT,
                    category TEXT DEFAULT 'template',
                    mime_type TEXT,
                    file_size INTEGER,
                    data BLOB,
                    uploaded_by TEXT,
                    created_at TEXT
                )
            """)
            conn.execute("""
                INSERT INTO rfq_files
                  (id, rfq_id, filename, file_type, category, file_size,
                   data, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (file_id, rfq_id, filename, file_type, category, size, b""))
            conn.commit()
        finally:
            conn.close()
        return file_id

    def test_dry_run_is_default(self, auth_client):
        """Calling POST without dry_run=0 must NOT delete anything."""
        fid = self._seed_rfq_file("dead_rfq_1")
        r = auth_client.post("/api/admin/trim-rfq-files?mode=orphans")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["dry_run"] is True
        assert d["deleted"]["count"] == 0
        # Row still present
        from src.core.db import get_db
        with get_db() as c:
            assert c.execute(
                "SELECT 1 FROM rfq_files WHERE id=?", (fid,)
            ).fetchone() is not None

    def test_dry_run_reports_orphans(self, auth_client):
        fid = self._seed_rfq_file("missing_parent", size=5000)
        r = auth_client.post("/api/admin/trim-rfq-files?mode=orphans&dry_run=1")
        d = r.get_json()
        assert d["matched"]["count"] >= 1
        ids = [s["id"] for s in d["sample"]]
        assert fid in ids

    def test_delete_requires_confirm_yes(self, auth_client):
        """dry_run=0 without confirm=YES must be rejected, not silently delete."""
        self._seed_rfq_file("rfq_no_confirm")
        r = auth_client.post("/api/admin/trim-rfq-files?mode=orphans&dry_run=0")
        assert r.status_code == 400
        d = r.get_json()
        assert d["ok"] is False
        assert "confirm" in d["error"].lower()

    def test_delete_with_confirm_actually_deletes(self, auth_client):
        fid = self._seed_rfq_file("rfq_gone", size=9999)
        r = auth_client.post(
            "/api/admin/trim-rfq-files"
            "?mode=orphans&dry_run=0&confirm=YES&vacuum=0"
        )
        assert r.status_code == 200
        d = r.get_json()
        assert d["dry_run"] is False
        assert d["deleted"]["count"] >= 1
        from src.core.db import get_db
        with get_db() as c:
            assert c.execute(
                "SELECT 1 FROM rfq_files WHERE id=?", (fid,)
            ).fetchone() is None

    def test_invalid_mode_rejected(self, auth_client):
        r = auth_client.post("/api/admin/trim-rfq-files?mode=everything")
        assert r.status_code == 400

    def test_orphan_mode_spares_live_rfq_files(self, auth_client):
        """Files whose rfq_id DOES exist must not be deleted by orphan mode."""
        from src.core.db import get_db
        import uuid
        live_id = f"live_{uuid.uuid4().hex[:8]}"
        from datetime import datetime as _dt
        with get_db() as c:
            # rfqs table is created by core.db init; just insert a row.
            c.execute(
                "INSERT INTO rfqs (id, status, received_at) "
                "VALUES (?, 'active', ?)",
                (live_id, _dt.now().isoformat()),
            )
            c.commit()
        fid = self._seed_rfq_file(live_id)
        r = auth_client.post(
            "/api/admin/trim-rfq-files"
            "?mode=orphans&dry_run=0&confirm=YES&vacuum=0"
        )
        d = r.get_json()
        with get_db() as c:
            assert c.execute(
                "SELECT 1 FROM rfq_files WHERE id=?", (fid,)
            ).fetchone() is not None, "live file was wrongly deleted"
