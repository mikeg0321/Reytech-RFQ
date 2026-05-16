"""Spine substrate: IngestRejection model + db writer + read surface.

Closes the missed-bid silent-drop class (5/15 mode c). Every email the
parser considered and refused must emit exactly one durable row; the
substrate guarantees append-only history.
"""
from __future__ import annotations

import os
import pytest
from datetime import datetime, timedelta, timezone

from src.spine import (
    IngestRejection,
    RejectionReason,
    SpineValidationError,
    init_db,
    latest_rejections,
    write_ingest_rejection,
)


@pytest.fixture
def spine_db(tmp_path):
    db = tmp_path / "spine.db"
    init_db(str(db))
    return str(db)


def _make(**over) -> IngestRejection:
    base = dict(
        rejection_id="rej_msgABC_1747000000",
        source_email_id="msgABC",
        source_thread_id="threadXYZ",
        sender_email="argarin@cchcs.ca.gov",
        subject="PREQ 10847457 SAC bid request",
        reason_code="parse_failed",
        reason_detail="Vision parser returned 0 line items",
        raw_excerpt="From: Argarin, Marc\nSubject: PREQ 10847457...\n",
        parser_version="vision_v3.2.1",
    )
    base.update(over)
    return IngestRejection(**base)


# ── Model invariants ─────────────────────────────────────────────────


def test_model_extra_forbid():
    with pytest.raises(Exception):
        IngestRejection(
            rejection_id="rej_x_1",
            reason_code="other",
            extra_alias_field="boom",
        )


def test_model_rejected_at_defaults_to_now():
    before = datetime.now(timezone.utc) - timedelta(seconds=1)
    r = IngestRejection(rejection_id="rej_x_1", reason_code="other",
                        reason_detail="needs detail when other")
    after = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert before <= r.rejected_at <= after


@pytest.mark.parametrize("reason", [
    "agency_not_supported",
    "no_attachments",
    "parse_failed",
    "tax_lookup_failed",
    "missing_solicitation_number",
    "duplicate_thread",
    "unrecognized_form_code",
    "low_parse_confidence",
    "other",
])
def test_every_reason_code_round_trips(spine_db, reason):
    r = _make(rejection_id=f"rej_{reason}_1", reason_code=reason)
    meta = write_ingest_rejection(spine_db, r)
    assert meta["reason_code"] == reason
    rows = latest_rejections(spine_db, reason_code=reason)
    assert len(rows) == 1
    assert rows[0]["reason_code"] == reason


def test_model_rejects_unknown_reason_code():
    with pytest.raises(Exception):
        IngestRejection(rejection_id="rej_x_1", reason_code="bogus_reason")


def test_model_id_pattern_enforced():
    with pytest.raises(Exception):
        IngestRejection(rejection_id="not/a/valid id", reason_code="other")


def test_model_raw_excerpt_truncates_via_max_length():
    long = "x" * 1500
    with pytest.raises(Exception):
        IngestRejection(rejection_id="rej_x_1", reason_code="other",
                        raw_excerpt=long)


# ── Writer / append-only invariants ──────────────────────────────────


def test_write_persists_and_returns_metadata(spine_db):
    r = _make()
    meta = write_ingest_rejection(spine_db, r)
    assert meta["rejection_id"] == r.rejection_id
    assert meta["reason_code"] == "parse_failed"
    assert "rejected_at" in meta


def test_write_rejects_duplicate_id(spine_db):
    r1 = _make()
    write_ingest_rejection(spine_db, r1)
    r2 = _make()  # same id
    with pytest.raises(SpineValidationError, match="already exists"):
        write_ingest_rejection(spine_db, r2)


def test_write_rejects_non_model_input(spine_db):
    with pytest.raises(SpineValidationError, match="IngestRejection"):
        write_ingest_rejection(spine_db, {"rejection_id": "x"})


# ── Read surface ─────────────────────────────────────────────────────


def test_latest_rejections_empty_returns_empty_list(spine_db):
    assert latest_rejections(spine_db) == []


def test_latest_rejections_newest_first(spine_db):
    write_ingest_rejection(spine_db, _make(
        rejection_id="rej_old_1",
        rejected_at=datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc),
    ))
    write_ingest_rejection(spine_db, _make(
        rejection_id="rej_new_1",
        rejected_at=datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
    ))
    rows = latest_rejections(spine_db)
    assert [r["rejection_id"] for r in rows] == ["rej_new_1", "rej_old_1"]


def test_latest_rejections_reason_code_filter(spine_db):
    write_ingest_rejection(spine_db, _make(
        rejection_id="rej_a_1", reason_code="parse_failed"))
    write_ingest_rejection(spine_db, _make(
        rejection_id="rej_b_1", reason_code="tax_lookup_failed",
        reason_detail="CDTFA 503"))
    rows = latest_rejections(spine_db, reason_code="tax_lookup_failed")
    assert len(rows) == 1
    assert rows[0]["rejection_id"] == "rej_b_1"


def test_latest_rejections_limit_validation(spine_db):
    with pytest.raises(SpineValidationError):
        latest_rejections(spine_db, limit=0)
    with pytest.raises(SpineValidationError):
        latest_rejections(spine_db, limit=1001)


def test_latest_rejections_received_at_round_trips(spine_db):
    received = datetime(2026, 5, 16, 8, 30, tzinfo=timezone.utc)
    write_ingest_rejection(spine_db, _make(
        rejection_id="rej_r_1", received_at=received))
    rows = latest_rejections(spine_db)
    # ISO string round-trips with the timezone preserved.
    assert rows[0]["received_at"].startswith("2026-05-16T08:30:00")
