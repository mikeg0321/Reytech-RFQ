"""Cross-table thread-dedup at ingest.

Closes the substrate-singleness gap surfaced by the 2026-05-26 Chechi
NON-IT orphans: `rfq_cd41cfee` was the third orphan minted on the
PREQ 10846357 Gmail thread despite the thread already hosting one
sent RFQ (`e02b7fa6`), three PCs (`pc_10d84af8` etc.), AND a prior
dismissed orphan (`rfq_b52d94bf`). Per-type ingest dedup queried only
one table at a time; the parent on the OTHER table was invisible.

Substrate fix: `_find_active_record_on_thread_any_type(thread_id)`
queries `rfqs` + `price_checks` in a UNION. Both ingest branches
(PC + RFQ) call it as a safety net after the existing same-type
gate. When ANY active record on the thread is found — same-type or
cross-type — the new record is marked `status='duplicate'`,
`dedup_of=<parent_id>`, AND `gmail_thread_duplicate_of=<parent_id>`
so both the Python predicate (`is_active_queue`) and the SQL view
(`v_active_queue_rfqs`) filter it out of the operator queue.
"""
import os
import sys
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ──────────────────────────────────────────────────────────────────────
# Helper directly under test
# ──────────────────────────────────────────────────────────────────────

class TestCrossTypeThreadHelper:
    """`_find_active_record_on_thread_any_type` must return matches
    from EITHER `rfqs` or `price_checks`, newest first, agency-filtered."""

    def test_helper_exists_and_is_importable(self):
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        assert callable(_find_active_record_on_thread_any_type)

    def test_returns_none_for_empty_thread_id(self):
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        assert _find_active_record_on_thread_any_type("") is None
        assert _find_active_record_on_thread_any_type("   ") is None
        assert _find_active_record_on_thread_any_type(None) is None

    def test_returns_none_when_no_match_in_either_table(self, client):
        """Cold DB → no rows → no match."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        result = _find_active_record_on_thread_any_type(
            "no-such-thread-id-xyz", agency="cchcs",
        )
        assert result is None

    def test_finds_match_in_rfqs_table(self, client):
        """A row in `rfqs` with matching email_thread_id is returned
        with record_type='rfq'."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        from src.core.db import get_db
        thread = f"test-thread-rfqonly-{uuid.uuid4().hex[:8]}"
        rid = f"rfq_{uuid.uuid4().hex[:8]}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO rfqs (id, email_thread_id, status, agency, "
                "received_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (rid, thread, "needs_review", "cchcs",
                 datetime.now().isoformat(), "{}"),
            )
            conn.commit()
        match = _find_active_record_on_thread_any_type(thread, agency="cchcs")
        assert match is not None
        assert match["id"] == rid
        assert match["record_type"] == "rfq"

    def test_finds_match_in_price_checks_table(self, client):
        """A row in `price_checks` is returned with record_type='pc'
        — this is the Chechi-orphan-class case (RFQ ingest needs to
        find its PC parent across tables)."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        from src.core.db import get_db
        thread = f"test-thread-pconly-{uuid.uuid4().hex[:8]}"
        pcid = f"pc_{uuid.uuid4().hex[:8]}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO price_checks (id, email_thread_id, status, agency, "
                "created_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (pcid, thread, "parsed", "cchcs",
                 datetime.now().isoformat(), "{}"),
            )
            conn.commit()
        match = _find_active_record_on_thread_any_type(thread, agency="cchcs")
        assert match is not None
        assert match["id"] == pcid
        assert match["record_type"] == "pc"

    def test_skip_set_excludes_duplicate_and_deleted(self, client):
        """Existing rows already marked duplicate/deleted/archived must
        NOT shadow real parents (prevents infinite duplicate chains)."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        from src.core.db import get_db
        thread = f"test-thread-skipped-{uuid.uuid4().hex[:8]}"
        dup_id = f"rfq_{uuid.uuid4().hex[:8]}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO rfqs (id, email_thread_id, status, agency, "
                "received_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (dup_id, thread, "duplicate", "cchcs",
                 datetime.now().isoformat(), "{}"),
            )
            conn.commit()
        assert _find_active_record_on_thread_any_type(thread, agency="cchcs") is None

    def test_agency_filter_excludes_other_agency(self, client):
        """Cross-agency matches don't count — protects against thread-id
        collisions across tenants (theoretical, but cheap to enforce)."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        from src.core.db import get_db
        thread = f"test-thread-agency-{uuid.uuid4().hex[:8]}"
        rid = f"rfq_{uuid.uuid4().hex[:8]}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO rfqs (id, email_thread_id, status, agency, "
                "received_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (rid, thread, "needs_review", "cdcr",
                 datetime.now().isoformat(), "{}"),
            )
            conn.commit()
        # Looking for cchcs — cdcr match is rejected.
        assert _find_active_record_on_thread_any_type(thread, agency="cchcs") is None
        # Same query with no agency hint accepts the cdcr row.
        match = _find_active_record_on_thread_any_type(thread, agency="")
        assert match is not None
        assert match["id"] == rid

    def test_returns_most_recent_when_multiple_match(self, client):
        """Order by created_at DESC so the freshest active parent wins."""
        from src.core.ingest_pipeline import (
            _find_active_record_on_thread_any_type,
        )
        from src.core.db import get_db
        thread = f"test-thread-multi-{uuid.uuid4().hex[:8]}"
        old_id = f"rfq_{uuid.uuid4().hex[:8]}"
        new_id = f"pc_{uuid.uuid4().hex[:8]}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO rfqs (id, email_thread_id, status, agency, "
                "received_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (old_id, thread, "sent", "cchcs",
                 "2026-05-01T10:00:00", "{}"),
            )
            conn.execute(
                "INSERT INTO price_checks (id, email_thread_id, status, agency, "
                "created_at, data_json) VALUES (?, ?, ?, ?, ?, ?)",
                (new_id, thread, "parsed", "cchcs",
                 "2026-05-20T10:00:00", "{}"),
            )
            conn.commit()
        match = _find_active_record_on_thread_any_type(thread, agency="cchcs")
        assert match is not None
        # newer wins regardless of record_type
        assert match["id"] == new_id
        assert match["record_type"] == "pc"


# ──────────────────────────────────────────────────────────────────────
# Wire-in verification: the source code at the two ingest seams must
# actually call the helper. Source-grep tests pin behavior even when
# end-to-end ingest is too heavyweight to drive from a unit test.
# ──────────────────────────────────────────────────────────────────────

class TestIngestSeamWireIn:
    """The two ingest branches must call the cross-table helper after
    their existing same-type dedup. These tests guard against a future
    revert / restructure silently dropping the safety-net call."""

    SOURCE = None

    @classmethod
    def setup_class(cls):
        from pathlib import Path
        repo_root = Path(__file__).resolve().parent.parent
        path = repo_root / "src" / "core" / "ingest_pipeline.py"
        cls.SOURCE = path.read_text(encoding="utf-8")

    def test_helper_definition_exists_in_source(self):
        assert "def _find_active_record_on_thread_any_type" in self.SOURCE, (
            "cross-table helper definition is missing"
        )

    def test_pc_branch_calls_cross_table_helper(self):
        """The PC ingest branch (immediately before `_save_single_pc`
        is called with the freshly-built record) must invoke the
        cross-table helper."""
        anchor = '_save_single_pc(record["id"], record)'
        pc_save_idx = self.SOURCE.find(anchor)
        assert pc_save_idx > 0, f"PC save call '{anchor}' not found"
        pc_region = self.SOURCE[max(0, pc_save_idx - 3000):pc_save_idx]
        assert "_find_active_record_on_thread_any_type(" in pc_region, (
            "PC branch is missing the cross-table dedup call"
        )

    def test_rfq_branch_calls_cross_table_helper(self):
        """Same check for the RFQ branch."""
        anchor = '_save_single_rfq(record["id"], record)'
        rfq_save_idx = self.SOURCE.find(anchor)
        assert rfq_save_idx > 0, f"RFQ save call '{anchor}' not found"
        rfq_region = self.SOURCE[max(0, rfq_save_idx - 3000):rfq_save_idx]
        assert "_find_active_record_on_thread_any_type(" in rfq_region, (
            "RFQ branch is missing the cross-table dedup call"
        )

    def test_cross_table_dedup_sets_gmail_thread_duplicate_of(self):
        """The fix must set BOTH `dedup_of` and `gmail_thread_duplicate_of`
        so the SQL view (v_active_queue_rfqs) filters the orphan."""
        # The dedup block writes both fields. Verify the symbols appear
        # near each CALL SITE of the helper (skip the definition itself).
        marker = "_find_active_record_on_thread_any_type("
        defn_idx = self.SOURCE.find(
            "def _find_active_record_on_thread_any_type("
        )
        assert defn_idx >= 0
        idx = 0
        seen = 0
        while True:
            idx = self.SOURCE.find(marker, idx)
            if idx < 0:
                break
            # Skip the definition itself; only check call sites.
            if abs(idx - defn_idx) < len(marker) + 4:
                idx += len(marker)
                continue
            block = self.SOURCE[idx:idx + 2500]
            assert 'record["gmail_thread_duplicate_of"]' in block, (
                f"cross-table dedup call at offset {idx} doesn't set "
                f"gmail_thread_duplicate_of — queue view won't filter"
            )
            assert 'record["dedup_of"]' in block, (
                f"cross-table dedup call at offset {idx} doesn't set dedup_of"
            )
            assert 'record["status"] = "duplicate"' in block, (
                f"cross-table dedup call at offset {idx} doesn't flip status"
            )
            seen += 1
            idx += len(marker)
        assert seen >= 2, (
            f"expected >=2 cross-table dedup call sites (PC + RFQ branch), "
            f"found {seen}"
        )
