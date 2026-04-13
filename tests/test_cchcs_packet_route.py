"""Integration test for the CCHCS packet generation route.

POST /api/pricecheck/<pcid>/cchcs-packet/generate should:
1. Accept a PC whose source_pdf is a CCHCS Non-IT RFQ Packet
2. Parse → match → fill end-to-end
3. Return ok=True with download_url + match_report + totals
4. Persist output_pdf on the PC for the UI download button

Run against the real Apr 2026 sample packet in _overnight_review/.
Uses the Flask test client — NO prod calls.
"""
import json
import os
import shutil

import pytest


SAMPLE_PACKET = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "_overnight_review",
    "source_packet.pdf",
)


@pytest.fixture
def pc_with_packet(temp_data_dir, sample_pc):
    """Seed a PC whose source_pdf points at the real CCHCS packet,
    plus a second priced PC the matcher can hit for DS8178 scanner
    so the gate passes in strict mode."""
    if not os.path.exists(SAMPLE_PACKET):
        pytest.skip("sample CCHCS packet missing")
    # Copy packet into the isolated test data dir so it's under DATA_DIR
    dest = os.path.join(
        temp_data_dir,
        "Non-Cloud RFQ Packet 12.3.25 - PREQ10843276.pdf",
    )
    shutil.copy(SAMPLE_PACKET, dest)
    pc = dict(sample_pc)
    pc["id"] = "pc_cchcs_test"
    pc["pc_number"] = "10843276"
    pc["status"] = "parsed"
    pc["source_pdf"] = dest
    pc["email_subject"] = "PREQ10843276 Quote Request for Scanners"
    pc["items"] = []  # packet parser will populate
    from src.api.dashboard import _save_single_pc
    _save_single_pc(pc["id"], pc)

    # Seed a separate priced PC with a matching DS8178 item so the
    # CCHCS matcher has ground truth to map against. Without this
    # the gate blocks the fill because row 1 has no price.
    priced_pc = dict(sample_pc)
    priced_pc["id"] = "pc_ds8178_priced"
    priced_pc["pc_number"] = "PC_DS8178_HISTORICAL"
    priced_pc["status"] = "complete"
    priced_pc["source_pdf"] = ""
    priced_pc["items"] = [
        {
            "item_number": "1",
            "qty": 15,
            "uom": "EA",
            "description": "Handheld Scanner w/ USB cable and standard cradle",
            "mfg_number": "DS8178",
            "part_number": "DS8178",
            "unit_price": 395.00,
            "extension": 5925.00,
            "pricing": {
                "unit_cost": 295.00,
                "recommended_price": 395.00,
            },
        }
    ]
    _save_single_pc(priced_pc["id"], priced_pc)

    return pc["id"]


class TestCCHCSPacketRoute:

    def test_dry_run_returns_parse_and_match(self, client, pc_with_packet):
        r = client.post(
            f"/api/pricecheck/{pc_with_packet}/cchcs-packet/generate?dry_run=1"
        )
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["dry_run"] is True
        assert d["packet_sol"] == "10843276"
        assert d["packet_items"] == 1
        assert "match_result" in d

    def test_full_run_generates_pdf(self, client, pc_with_packet, temp_data_dir):
        r = client.post(
            f"/api/pricecheck/{pc_with_packet}/cchcs-packet/generate"
        )
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["packet_sol"] == "10843276"
        assert d["output_path"].endswith("_Reytech.pdf")
        assert os.path.exists(d["output_path"])
        # Valid PDF header
        with open(d["output_path"], "rb") as f:
            assert f.read(5) == b"%PDF-"
        # Match report is a list (possibly empty)
        assert isinstance(d["match_report"], list)
        # Download URL shape
        assert d["download_url"].startswith("/api/pricecheck/download/")
        assert "_Reytech" in d["download_url"]

    def test_404_when_pc_missing(self, client):
        r = client.post("/api/pricecheck/pc_nope/cchcs-packet/generate")
        assert r.status_code == 404

    def test_400_when_source_pdf_missing(self, client, temp_data_dir, sample_pc):
        pc = dict(sample_pc)
        pc["id"] = "pc_no_source"
        pc["source_pdf"] = "/tmp/does_not_exist.pdf"
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pc["id"], pc)
        r = client.post(f"/api/pricecheck/{pc['id']}/cchcs-packet/generate")
        assert r.status_code == 400
        d = r.get_json()
        assert "Source PDF not found" in d["error"]

    def test_400_when_source_is_not_a_cchcs_packet(
        self, client, temp_data_dir, sample_pc
    ):
        """A standard 704 PDF should be rejected by the CCHCS route so
        the operator isn't confused into calling the wrong generator."""
        dummy = os.path.join(temp_data_dir, "AMS_704_standard.pdf")
        # Write a minimal valid PDF so existence check passes
        with open(dummy, "wb") as f:
            f.write(b"%PDF-1.3\n%EOF\n")
        pc = dict(sample_pc)
        pc["id"] = "pc_regular_704"
        pc["source_pdf"] = dummy
        pc["email_subject"] = "Price Check Worksheet"
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pc["id"], pc)
        r = client.post(f"/api/pricecheck/{pc['id']}/cchcs-packet/generate")
        assert r.status_code == 400
        d = r.get_json()
        assert "CCHCS Non-IT RFQ Packet" in d["error"]

    def test_output_pdf_persisted_on_pc(self, client, pc_with_packet):
        r = client.post(
            f"/api/pricecheck/{pc_with_packet}/cchcs-packet/generate"
        )
        assert r.status_code == 200
        # Reload PC from DB and confirm output_pdf + metadata persisted
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        pc = pcs[pc_with_packet]
        assert pc.get("output_pdf")
        assert os.path.exists(pc["output_pdf"])
        meta = pc.get("cchcs_packet_last_generated") or {}
        assert "at" in meta
        assert "rows_priced" in meta


class TestCCHCSBackfillRoute:
    """POST /api/admin/cchcs-packets/backfill scans every existing PC
    and tags the ones that look like CCHCS packets. Safe to run
    multiple times."""

    def test_backfill_end_state_correct(self, client, temp_data_dir, sample_pc):
        """End-state assertion: after calling backfill, every CCHCS
        packet PC in the DB has packet_type=cchcs_non_it and no
        standard 704 is tagged. Whether they were tagged at save time
        (via the _save_single_pc auto-hook) or by the backfill endpoint
        doesn't matter — both paths converge on the same end state.
        """
        from src.api.dashboard import _save_single_pc, _load_price_checks

        packet_pc = dict(sample_pc)
        packet_pc["id"] = "pc_backfill_packet"
        packet_pc["email_subject"] = "PREQ10843276 Quote Request"
        packet_pc["source_pdf"] = "/tmp/Non-Cloud RFQ Packet PREQ10843276.pdf"
        _save_single_pc(packet_pc["id"], packet_pc)

        standard_pc = dict(sample_pc)
        standard_pc["id"] = "pc_backfill_std"
        standard_pc["email_subject"] = "Price Check Worksheet"
        standard_pc["source_pdf"] = "/tmp/AMS 704 Office Supplies.pdf"
        _save_single_pc(standard_pc["id"], standard_pc)

        r = client.post("/api/admin/cchcs-packets/backfill")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        # Totals should be consistent — 2 scanned, both handled (either
        # already_tagged because save hook ran, or tagged_now because
        # it didn't — either way, packet_pc must end up tagged).
        assert d["total"] == 2
        assert (d["tagged_now"] + d["already_tagged"]) >= 1
        assert d["not_packet"] >= 1

        # Verify the end-state tags on disk
        pcs = _load_price_checks()
        assert pcs["pc_backfill_packet"].get("packet_type") == "cchcs_non_it"
        assert pcs["pc_backfill_std"].get("packet_type") != "cchcs_non_it"

    def test_save_single_pc_auto_tags_cchcs_packets(self, temp_data_dir, sample_pc):
        """Centralized ingest hook: every _save_single_pc call runs
        tag_pc_if_packet before writing to SQLite. This means any
        path that creates a PC (email poller, manual upload, admin,
        test harness) auto-tags CCHCS packets without having to
        wire each path individually."""
        from src.api.dashboard import _save_single_pc, _load_price_checks

        pc = dict(sample_pc)
        pc["id"] = "pc_ingest_auto_tag"
        pc["email_subject"] = "PREQ99999 Quote Request"
        pc["source_pdf"] = "/tmp/Non-Cloud RFQ Packet PREQ99999.pdf"
        # Note: no packet_type set by caller
        assert "packet_type" not in pc

        _save_single_pc(pc["id"], pc)

        # Reload from DB and verify the tag landed
        pcs = _load_price_checks()
        loaded = pcs.get(pc["id"])
        assert loaded is not None
        assert loaded.get("packet_type") == "cchcs_non_it"

    def test_save_single_pc_does_not_tag_standard_704(self, temp_data_dir, sample_pc):
        """Negative case: a standard AMS 704 PC must NOT get tagged."""
        from src.api.dashboard import _save_single_pc, _load_price_checks

        pc = dict(sample_pc)
        pc["id"] = "pc_standard_704_ingest"
        pc["email_subject"] = "Price Check Worksheet — Office Supplies"
        pc["source_pdf"] = "/tmp/AMS 704 Office Supplies.pdf"

        _save_single_pc(pc["id"], pc)

        pcs = _load_price_checks()
        loaded = pcs.get(pc["id"])
        assert loaded is not None
        # Either absent or explicitly not the CCHCS type
        assert loaded.get("packet_type") != "cchcs_non_it"

    def test_backfill_is_idempotent(self, client, temp_data_dir, sample_pc):
        from src.api.dashboard import _save_single_pc

        pc = dict(sample_pc)
        pc["id"] = "pc_already_tagged"
        pc["email_subject"] = "PREQ12345 Quote Request"
        pc["source_pdf"] = "/tmp/RFQ Packet PREQ12345.pdf"
        pc["packet_type"] = "cchcs_non_it"  # already tagged
        _save_single_pc(pc["id"], pc)

        r = client.post("/api/admin/cchcs-packets/backfill")
        assert r.status_code == 200
        d = r.get_json()
        # Already-tagged PC counted as already_tagged, not tagged_now
        assert d["already_tagged"] >= 1
        assert "pc_already_tagged" not in d["tagged_ids"]
