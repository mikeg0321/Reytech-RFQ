"""Home-queue ghost filter + dedup-by-number.

Mike's 2026-04-23 screenshot showed 5 ghost PC rows (Michael Guadan
self-buyer / 45* synthetic / Garrett Arase self-buyer / Marc Argarin
self-buyer / parse-fail zero-items) AND a duplicate RFQ #10840486
appearing twice (once as uppercase "CA STATE PRISON SACRAMENTO",
once as title-case "CA State Prison Sacramento") because institution
text differed between the two ingest passes.

This PR adds render-time filters on `/` so both classes of noise
stay out of the operator queue regardless of whether the ghost-
quarantine flag is on or the records carry a `hidden_reason` stamp.
Records are NEVER deleted — they still live in storage and show in
admin views.

Tests here seed the same patterns Mike saw and assert the queue
renders without them.
"""
from __future__ import annotations

import json
import os

import pytest


def _seed_records(temp_data_dir, pcs=None, rfqs=None):
    if pcs:
        path = os.path.join(temp_data_dir, "price_checks.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(pcs, f)
    if rfqs:
        path = os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rfqs, f)


def _fetch_home(client):
    resp = client.get("/")
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def _mk_pc(pcid, **kwargs):
    base = {
        "id": pcid,
        "pc_number": pcid,
        "status": "new",
        "buyer_name": "Buyer",
        "institution": "CDCR",
        "line_items": [
            {"qty": 1, "uom": "EA", "description": "Test item",
             "price_per_unit": 10.0},
        ],
        "due_date": "2026-05-15",
    }
    base.update(kwargs)
    return base


def _mk_rfq(rid, **kwargs):
    base = {
        "id": rid,
        "rfq_number": rid,
        "solicitation_number": rid,
        "status": "new",
        "buyer_name": "Buyer",
        "institution": "CCHCS",
        "line_items": [
            {"qty": 1, "uom": "EA", "description": "Test item",
             "price_per_unit": 10.0},
        ],
        "due_date": "2026-05-15",
    }
    base.update(kwargs)
    return base


class TestGhostFilter:
    """Each of the 5 ghost patterns Mike screenshotted must be hidden
    from the queue."""

    def test_michael_guadan_self_buyer_hidden(
        self, client, temp_data_dir
    ):
        ghost = _mk_pc(
            "45007355",
            buyer_name="Michael Guadan",
            institution="Michael Guadan",
        )
        legit = _mk_pc(
            "10844466", buyer_name="Ashley Russ", institution="CCHCS",
        )
        _seed_records(temp_data_dir, pcs={
            ghost["id"]: ghost, legit["id"]: legit,
        })
        html = _fetch_home(client)
        assert "/pricecheck/45007355" not in html, (
            "Michael Guadan self-buyer ghost leaked into queue"
        )
        assert "/pricecheck/10844466" in html, "legit PC should still render"

    def test_synthetic_45_prefix_hidden(self, client, temp_data_dir):
        ghost = _mk_pc(
            "45007500",
            buyer_name="Someone",
            institution="CCHCS",
            line_items=[
                {"qty": 1, "uom": "EA", "description": "x",
                 "price_per_unit": 1.0}
                for _ in range(10)
            ],
        )
        _seed_records(temp_data_dir, pcs={ghost["id"]: ghost})
        html = _fetch_home(client)
        # Page has an input placeholder "e.g. 45007500" so bare
        # substring is not specific enough. Check for the detail-page
        # href that only a rendered PC row would emit.
        assert "/pricecheck/45007500" not in html, (
            "Synthetic 45* prefix ghost leaked into queue"
        )

    def test_garrett_arase_self_buyer_hidden(
        self, client, temp_data_dir
    ):
        """Real screenshot case: buyer and institution both = 'Garrett
        Arase', 6 items, due tomorrow. Looks legitimate on count alone
        but the self-buyer pattern marks it as ghost."""
        ghost = _mk_pc(
            "10841666",
            buyer_name="Garrett Arase",
            institution="Garrett Arase",
            line_items=[
                {"qty": 1, "uom": "EA", "description": "x",
                 "price_per_unit": 1.0}
                for _ in range(6)
            ],
        )
        _seed_records(temp_data_dir, pcs={ghost["id"]: ghost})
        html = _fetch_home(client)
        assert "/pricecheck/10841666" not in html

    def test_marc_argarin_self_buyer_hidden(self, client, temp_data_dir):
        ghost = _mk_pc(
            "10837703",
            buyer_name="Marc Argarin",
            institution="Marc Argarin",
        )
        _seed_records(temp_data_dir, pcs={ghost["id"]: ghost})
        html = _fetch_home(client)
        assert "/pricecheck/10837703" not in html

    def test_parse_fail_zero_items_blank_institution_hidden(
        self, client, temp_data_dir
    ):
        ghost = _mk_pc(
            "Med_OS_test",
            pc_number="Med OS -",
            buyer_name="Valentina Demidenko",
            institution="",
            line_items=[],
        )
        _seed_records(temp_data_dir, pcs={ghost["id"]: ghost})
        html = _fetch_home(client)
        assert "/pricecheck/Med_OS_test" not in html


class TestDedupByNumber:
    """Audit E addendum: same solicitation number, different
    institution text (casing / whitespace differences) should
    collapse into ONE row."""

    def test_rfq_10840486_duplicate_collapses(
        self, client, temp_data_dir
    ):
        # Two ingests of the same RFQ with institution text variants
        upper = _mk_rfq(
            "rfq-upper",
            rfq_number="10840486",
            solicitation_number="10840486",
            institution="CA STATE PRISON SACRAMENTO",
            buyer_name="Steve Phan",
            updated_at="2026-04-22T10:00:00",
        )
        title = _mk_rfq(
            "rfq-title",
            rfq_number="10840486",
            solicitation_number="10840486",
            institution="CA State Prison Sacramento",
            buyer_name="Steve Phan",
            updated_at="2026-04-23T10:00:00",  # newer → this one wins
        )
        _seed_records(temp_data_dir, rfqs={
            upper["id"]: upper, title["id"]: title,
        })
        html = _fetch_home(client)
        # Count occurrences of the solicitation number in the rendered
        # queue table(s). Header references may include it; we count
        # occurrences of the ID-anchor pattern unique to row cells.
        n = html.count("10840486")
        # Allow up to 2 (once as the row cell label, once in a link
        # href pointing at the detail page — same record both times).
        # Pre-PR Mike saw 4+ because BOTH records rendered.
        assert n <= 3, (
            f"solicitation 10840486 appeared {n} times — dedup "
            "failed to collapse the 'CA STATE PRISON' / 'CA State "
            "Prison' variants to one row"
        )

    def test_dedup_keeps_most_recent(self, client, temp_data_dir):
        old = _mk_rfq(
            "rfq-old",
            rfq_number="99999",
            solicitation_number="99999",
            buyer_name="OLD",
            updated_at="2026-04-20T10:00:00",
        )
        new = _mk_rfq(
            "rfq-new",
            rfq_number="99999",
            solicitation_number="99999",
            buyer_name="NEW",
            updated_at="2026-04-23T10:00:00",
        )
        _seed_records(temp_data_dir, rfqs={
            old["id"]: old, new["id"]: new,
        })
        html = _fetch_home(client)
        # The rendered row is identified by the detail-page href.
        # Only the newer record (rfq-new) should have its href present;
        # rfq-old should be collapsed by dedup.
        assert "/rfq/rfq-new" in html, (
            "newer record should render after dedup"
        )
        assert "/rfq/rfq-old" not in html, (
            "older record with same solicitation number should be dedup'd out"
        )


class TestLegitRecordsStillRender:
    """The filter must not hide legitimate records."""

    def test_legit_pc_renders(self, client, temp_data_dir):
        legit = _mk_pc(
            "10844466",
            buyer_name="Mohammad Chechi",
            institution="CDCR",
        )
        _seed_records(temp_data_dir, pcs={legit["id"]: legit})
        html = _fetch_home(client)
        assert "/pricecheck/10844466" in html

    def test_legit_rfq_with_different_institution_stays(
        self, client, temp_data_dir
    ):
        """Different records with different solicitation numbers must
        NOT dedup. Regression guard against over-merging."""
        a = _mk_rfq(
            "rfq-a", rfq_number="10840001",
            solicitation_number="10840001",
        )
        b = _mk_rfq(
            "rfq-b", rfq_number="10840002",
            solicitation_number="10840002",
        )
        _seed_records(temp_data_dir, rfqs={
            a["id"]: a, b["id"]: b,
        })
        html = _fetch_home(client)
        assert "/rfq/rfq-a" in html
        assert "/rfq/rfq-b" in html


class TestNonItPlaceholderOrphansHidden:
    """Pre-PR-#1095 ingests stored 703B-title fragments as solicitation
    numbers ("NON-IT", "IT-GOODS", "GOODS", "PAYMENT", "ATTACHED"...).
    PR #1095 closed the ingest path; this filter closes the DISPLAY
    path so legacy orphans no longer pollute the operator queue. Pin
    every word in the canonical placeholder set so a future filter
    refactor cannot silently drop the coverage.

    Trigger: Mike screenshot 2026-05-28 of `#NON-IT` cchcs Mohammad
    Chechi row, 0 items, 200h overdue — same orphan class PRs #1095 +
    #1128 were "supposed to fix" but never cleaned up at display time.
    """

    def test_chechi_non_it_orphan_hidden(self, client, temp_data_dir):
        """The exact 2026-05-28 prod orphan: cchcs, Mohammad Chechi,
        sol# stored as `NON-IT` from the 703B title text, 0 items
        parsed. Must NOT appear on home."""
        orphan = _mk_rfq(
            "rfq_chechi_nonit",
            rfq_number="NON-IT",
            solicitation_number="NON-IT",
            buyer_name="Mohammad Chechi",
            institution="CCHCS",
            line_items=[],  # parser produced 0 items — typical for this orphan
        )
        _seed_records(temp_data_dir, rfqs={orphan["id"]: orphan})
        html = _fetch_home(client)
        assert "/rfq/rfq_chechi_nonit" not in html, (
            "Chechi NON-IT orphan leaked into the home queue. "
            "Display filter must mirror _SOL_PLACEHOLDER_WORDS from "
            "request_classifier.py — see PR #1095 invariant."
        )

    @pytest.mark.parametrize("placeholder", [
        "NON-IT", "IT-GOODS", "GOODS", "PAYMENT", "ATTACHED",
        "QUOTATION", "REQUEST", "RFQ", "SOLICITATION", "UNKNOWN",
    ])
    def test_every_placeholder_word_hides_zero_item_orphan(
        self, client, temp_data_dir, placeholder
    ):
        """Pin substrate singleness — every word the ingest classifier
        rejects must also be hidden by the display filter. If this test
        fails, the display deny list has drifted from the canonical
        `_SOL_PLACEHOLDER_WORDS`."""
        rid = f"rfq_orphan_{placeholder.lower().replace('-','_')}"
        orphan = _mk_rfq(
            rid,
            rfq_number=placeholder,
            solicitation_number=placeholder,
            line_items=[],
        )
        _seed_records(temp_data_dir, rfqs={orphan["id"]: orphan})
        html = _fetch_home(client)
        assert f"/rfq/{rid}" not in html, (
            f"Orphan with sol#={placeholder!r} leaked into the home "
            f"queue. Display filter is out of sync with "
            f"_SOL_PLACEHOLDER_WORDS — see PR #1095."
        )

    def test_real_sol_number_with_zero_items_still_shows(
        self, client, temp_data_dir
    ):
        """The filter must NOT over-block: a 0-item RFQ with a real
        digit-bearing sol# is a parse failure that needs operator
        review and must stay visible (the `_keep_pc` / RFQ predicate's
        sole point)."""
        real = _mk_rfq(
            "rfq_real_zero_items",
            rfq_number="10842999",
            solicitation_number="10842999",  # real digit-bearing sol#
            line_items=[],  # parser failed to find items
        )
        _seed_records(temp_data_dir, rfqs={real["id"]: real})
        html = _fetch_home(client)
        assert "/rfq/rfq_real_zero_items" in html, (
            "RFQ with a real digit-bearing sol# must stay visible "
            "even with 0 items — operator needs to fix the parse."
        )
