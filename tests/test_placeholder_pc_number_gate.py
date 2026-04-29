"""Tests for the legacy email-poller subject-fallback fix + zero-items gate.

2026-04-29 incident: prod records `pc_a391db8f` (pc_number="GOOD") and
`rfq_7813c4e1` (sol#="WORKSHEET") from keith.alsing@calvet.ca.gov landed
with status='parsed' and items=[] because the legacy poller chain ended
in `subject[:40]`. The classifier matched on agency keyword + zeroed
into the placeholder.

Locks:
  1. The buyer-content-derived fallback (subject[:40], filename word
     fragments) is replaced with the deterministic AUTO_<short_id>
     pattern from ingest_v2.
  2. ingest_v2 marks zero-item records 'needs_review' not 'parsed'
     so the queue surfaces them as triage instead of fake-done.
  3. _is_placeholder_number flags single-uppercase-word junk so the
     backfill script can find legacy rows.
"""
from __future__ import annotations


# ── Helper functions ───────────────────────────────────────────────────


def test_auto_placeholder_strips_record_prefix():
    from src.api.dashboard import _auto_placeholder_number
    assert _auto_placeholder_number("pc_abcd1234") == "AUTO_abcd1234"
    assert _auto_placeholder_number("rfq_7813c4e1") == "AUTO_7813c4e1"


def test_auto_placeholder_handles_no_prefix():
    from src.api.dashboard import _auto_placeholder_number
    # Defensive: bare ID still yields a valid AUTO_ token
    assert _auto_placeholder_number("abcd1234").startswith("AUTO_")


def test_auto_placeholder_truncates_to_8_chars():
    from src.api.dashboard import _auto_placeholder_number
    # Even if the id has a long suffix, AUTO_ token is 8 hex
    out = _auto_placeholder_number("pc_abcdef0123456789")
    assert out == "AUTO_abcdef01"


# ── Placeholder detection ──────────────────────────────────────────────


def test_is_placeholder_flags_known_junk():
    from src.api.dashboard import _is_placeholder_number
    assert _is_placeholder_number("WORKSHEET") is True
    assert _is_placeholder_number("GOOD") is True
    assert _is_placeholder_number("RFQ") is True
    assert _is_placeholder_number("QUOTE") is True
    assert _is_placeholder_number("unknown") is True


def test_is_placeholder_flags_empty():
    from src.api.dashboard import _is_placeholder_number
    assert _is_placeholder_number("") is True
    assert _is_placeholder_number(None) is True
    assert _is_placeholder_number("   ") is True


def test_is_placeholder_skips_auto_prefix():
    """The new AUTO_<id> format is intentionally deterministic — backfill
    must NOT flip rows that already use the new format."""
    from src.api.dashboard import _is_placeholder_number
    assert _is_placeholder_number("AUTO_abcd1234") is False


def test_is_placeholder_passes_real_solicitation_numbers():
    from src.api.dashboard import _is_placeholder_number
    # CDCR PR numbers
    assert _is_placeholder_number("10840486") is False
    # CalVet 8955-prefixed
    assert _is_placeholder_number("8955-00012345") is False
    # Solicitation with letters
    assert _is_placeholder_number("25-067MC") is False
    # Mixed-case PC name from filename — not pure all-caps
    assert _is_placeholder_number("RT Supplies") is False


# ── Zero-items gate in ingest_v2 ──────────────────────────────────────


def test_ingest_v2_zero_items_lands_as_needs_review(temp_data_dir, monkeypatch):
    """Body-only email with no parseable attachments → ingest creates
    record with items=[] → must NOT stamp status='parsed'."""
    from src.core import ingest_pipeline

    class _FakeClassification:
        def __init__(self):
            self.shape = "unknown"
            self.agency = "calvet"
            self.solicitation_number = ""
            self.institution = ""

        def to_dict(self):
            return {"shape": self.shape, "agency": self.agency}

    # Direct-call _create_record with empty items (covers the gate logic
    # without dragging in the full classifier + parser stack).
    rid = ingest_pipeline._create_record(
        record_type="rfq",
        items=[],
        header={},
        classification=_FakeClassification(),
        primary_path=None,
        email_subject="Need pricing on widgets",
        email_sender="keith.alsing@calvet.ca.gov",
        email_uid="msg-uid-123",
    )

    from src.api.data_layer import load_rfqs
    rfq = load_rfqs().get(rid)
    assert rfq is not None
    assert rfq["status"] == "needs_review", (
        f"zero-items record stamped {rfq['status']!r} — must be 'needs_review'")


def test_ingest_v2_with_items_still_lands_as_parsed(temp_data_dir, monkeypatch):
    """Sanity check: the gate only fires on items==0. Non-empty items
    keep the 'parsed' status."""
    from src.core import ingest_pipeline

    class _FakeClassification:
        def __init__(self):
            self.shape = "generic_rfq_xlsx"
            self.agency = "calvet"
            self.solicitation_number = "10840486"
            self.institution = "vhc-wla"

        def to_dict(self):
            return {"shape": self.shape, "agency": self.agency}

    rid = ingest_pipeline._create_record(
        record_type="pc",
        items=[{"description": "Widget", "qty": 5, "unit_price": 12.50}],
        header={},
        classification=_FakeClassification(),
        primary_path=None,
        email_subject="Quote 10840486",
        email_sender="buyer@calvet.ca.gov",
        email_uid="msg-uid-124",
    )

    from src.api.data_layer import _load_price_checks
    pc = _load_price_checks().get(rid)
    assert pc is not None
    assert pc["status"] == "parsed"
