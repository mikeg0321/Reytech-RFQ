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
    """Seed a PC whose source_pdf points at the real CCHCS packet."""
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
