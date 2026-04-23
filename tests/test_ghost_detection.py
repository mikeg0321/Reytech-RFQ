"""Bundle-2 PR-2c: ingest-time ghost-record detection.

Source: `feedback_ghost_record_heuristics` + audit item E. These
tests pin the heuristics so a later "tighten the filter" pass can't
silently start swallowing real buyer requests.

Two groups:
  1. **Detection** — each ghost heuristic fires for the right input
     and does NOT fire for legitimate records.
  2. **Wiring** — detection hooks into `process_buyer_request` only
     when the `ingest.ghost_quarantine_enabled` flag is on; triage
     queue skips records carrying `hidden_reason`.
"""
from __future__ import annotations

import pytest

from src.core.ghost_detection import (
    detect_ghost_pattern,
    is_quarantined,
    REASON_INTERNAL_SENDER,
    REASON_SELF_BUYER,
    REASON_SYNTHETIC_PREFIX,
    REASON_ZERO_ITEMS_BLANK_INSTITUTION,
    REASON_NON_AGENCY_SENDER_SELF,
)


# ── Detection unit tests ──────────────────────────────────────────

class TestInternalSender:
    """`@reytechinc.com` addresses and Michael Guadan himself are
    NEVER buyers — Mike's #1 ingest-time rule."""

    def test_reytech_domain_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Customer", "institution": "CDCR"},
            email_sender="mike@reytechinc.com",
        )
        assert reason == REASON_INTERNAL_SENDER

    def test_michael_guadan_sender_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "x", "institution": "y"},
            email_sender="Michael Guadan <mtnbkr@personal.com>",
        )
        assert reason == REASON_INTERNAL_SENDER

    def test_legit_cdcr_sender_not_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Steve Phan", "institution": "CSP-Sacramento"},
            email_sender="Steve.Phan@cdcr.ca.gov",
            items_parsed=3,
        )
        assert reason is None


class TestSelfBuyer:
    """Buyer and institution containing the same human name is a
    parse-failure signature — agency directories never list an
    individual as both requestor and agency."""

    def test_same_person_in_both_slots_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Garrett Arase", "institution": "Garrett Arase"},
            email_sender="garrett@somewhere.com",
            items_parsed=0,
        )
        assert reason == REASON_SELF_BUYER

    def test_buyer_different_from_institution_not_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Jane Doe", "institution": "CDCR"},
            email_sender="jane@cdcr.ca.gov",
            items_parsed=2,
        )
        assert reason is None


class TestSyntheticPrefix:
    """CCHCS 45XXXXXX numbers are QA / test seeds. Never real."""

    def test_45_prefix_flagged(self):
        reason = detect_ghost_pattern(
            {
                "buyer_name": "Buyer",
                "institution": "CCHCS",
                "pc_number": "45007355",
            },
            email_sender="buyer@cchcs.ca.gov",
            items_parsed=1,
        )
        assert reason == REASON_SYNTHETIC_PREFIX

    def test_real_10843_prefix_not_flagged(self):
        reason = detect_ghost_pattern(
            {
                "buyer_name": "Buyer",
                "institution": "CCHCS",
                "pc_number": "10843276",
            },
            email_sender="buyer@cchcs.ca.gov",
            items_parsed=1,
        )
        assert reason is None


class TestZeroItemsBlankInstitution:
    """0 items parsed + no institution = pure parse-fail placeholder.
    Zero items WITH an institution is fine — could be a form-only
    certification packet."""

    def test_zero_items_and_blank_institution_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Tarrna Solis", "institution": ""},
            email_sender="t@unknownvendor.com",
            items_parsed=0,
        )
        assert reason == REASON_ZERO_ITEMS_BLANK_INSTITUTION

    def test_zero_items_with_institution_not_flagged_on_this_rule(self):
        """With institution set, zero items could be a legitimate
        forms-only packet. Must NOT trigger the zero-items rule."""
        reason = detect_ghost_pattern(
            {"buyer_name": "Real Buyer", "institution": "CCHCS",
             "pc_number": "10844123"},
            email_sender="buyer@cchcs.ca.gov",
            items_parsed=0,
        )
        assert reason is None

    def test_many_items_with_blank_institution_not_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "Buyer", "institution": "",
             "pc_number": "10841234"},
            email_sender="b@cdcr.ca.gov",
            items_parsed=5,
        )
        assert reason is None


class TestNonAgencySender:
    """Sender on a non-gov domain AND institution looks like the
    sender's own name — likely the operator forwarded something
    from their personal mailbox, not a real RFQ."""

    def test_non_gov_sender_with_self_institution_flagged(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "someone", "institution": "jane doe"},
            email_sender="jane.doe@gmail.com",
            items_parsed=1,
        )
        assert reason == REASON_NON_AGENCY_SENDER_SELF

    def test_gov_sender_still_passes(self):
        reason = detect_ghost_pattern(
            {"buyer_name": "someone", "institution": "jane doe"},
            email_sender="jane.doe@cdcr.ca.gov",
            items_parsed=1,
        )
        # Agency senders always pass this rule, regardless of
        # institution-name-matching.
        assert reason != REASON_NON_AGENCY_SENDER_SELF


class TestIsQuarantined:
    def test_stamped_record_is_quarantined(self):
        assert is_quarantined({
            "hidden_reason": REASON_INTERNAL_SENDER,
        }) is True

    def test_unstamped_record_is_not_quarantined(self):
        assert is_quarantined({
            "buyer_name": "Jane", "institution": "CDCR",
        }) is False

    def test_blank_hidden_reason_is_not_quarantined(self):
        """Empty-string `hidden_reason` shouldn't count as
        quarantined — the field might be default-initialized by
        adapters without actually meaning anything."""
        assert is_quarantined({"hidden_reason": ""}) is False
        assert is_quarantined({"hidden_reason": None}) is False


# ── Wiring: triage filter ─────────────────────────────────────────

class TestTriageSkipsQuarantined:
    """The `/api/triage` queue must exclude records carrying
    `hidden_reason`. Regression guard: a future refactor that loses
    the filter would pollute NEXT UP with the very records PR-2c
    is trying to hide."""

    def test_quarantined_rfq_excluded_from_triage(
        self, client, temp_data_dir, sample_rfq
    ):
        import json
        import os

        # Seed a legitimate RFQ + a quarantined RFQ.
        good = dict(sample_rfq)
        good["id"] = "rfq-good-001"
        good["status"] = "new"
        good["due_date"] = "2026-05-01"
        ghost = dict(sample_rfq)
        ghost["id"] = "rfq-ghost-001"
        ghost["status"] = "new"
        ghost["due_date"] = "2026-05-01"
        ghost["hidden_reason"] = REASON_INTERNAL_SENDER
        ghost["hidden_at"] = "2026-04-23T12:00:00"
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({good["id"]: good, ghost["id"]: ghost}, f)

        resp = client.get("/api/triage")
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True

        # Flatten every record returned across next_up + emergency + queue
        surfaces = []
        if payload.get("next_up"):
            surfaces.append(payload["next_up"])
        surfaces.extend(payload.get("emergency", []))
        surfaces.extend(payload.get("queue", []))
        surfaced_ids = {s.get("rfq_id") or s.get("pc_id") or s.get("id") for s in surfaces}

        assert "rfq-ghost-001" not in surfaced_ids, (
            "quarantined RFQ must NOT appear in triage output"
        )
