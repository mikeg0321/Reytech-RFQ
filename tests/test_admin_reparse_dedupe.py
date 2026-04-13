"""Tests for the admin bulk-reparse and PC/RFQ dupe resolver endpoints.

Regression for 2026-04-12 incidents:
- 12 empty Valencia PCs had to be reparsed one-by-one via curl loop
- Drew Sims #26-04-003 appeared as BOTH a PC and an RFQ row simultaneously
"""
import json
import os


def _write_pcs(temp_data_dir, pcs_dict):
    with open(os.path.join(temp_data_dir, "price_checks.json"), "w") as f:
        json.dump(pcs_dict, f)


def _write_rfqs(temp_data_dir, rfqs_dict):
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
        json.dump(rfqs_dict, f)


class TestBulkReparseEmptyPCs:
    """POST /api/admin/reparse-empty-pcs"""

    def test_dry_run_lists_candidates(self, client, temp_data_dir, sample_pc):
        empty_from_email = dict(sample_pc)
        empty_from_email["id"] = "pc_empty_email"
        empty_from_email["items"] = []
        empty_from_email["parsed"] = {"line_items": [], "header": {}}
        empty_from_email["status"] = "parsed"
        empty_from_email["email_subject"] = "Price Quote Request - Group Tx Materials"
        empty_from_email["sender_email"] = "katrina.valencia@cdcr.ca.gov"
        # An unrelated empty PC without email metadata — must NOT be picked up
        noise = dict(sample_pc)
        noise["id"] = "pc_noise"
        noise["items"] = []
        noise["parsed"] = {"line_items": [], "header": {}}
        noise["status"] = "new"
        for k in ("email_subject", "sender_email", "original_sender", "email_uid"):
            noise.pop(k, None)
        _write_pcs(temp_data_dir, {
            empty_from_email["id"]: empty_from_email,
            noise["id"]: noise,
        })
        resp = client.post("/api/admin/reparse-empty-pcs?dry_run=1")
        assert resp.status_code == 200
        d = resp.get_json()
        assert d["ok"] is True
        assert d["dry_run"] is True
        assert d["would_reparse"] == 1
        ids = [c["pc_id"] for c in d["candidates"]]
        assert "pc_empty_email" in ids
        assert "pc_noise" not in ids

    def test_limit_caps_candidate_count(self, client, temp_data_dir, sample_pc):
        pcs = {}
        for i in range(5):
            p = dict(sample_pc)
            p["id"] = f"pc_{i}"
            p["items"] = []
            p["parsed"] = {"line_items": [], "header": {}}
            p["status"] = "parsed"
            p["email_subject"] = f"Test {i}"
            p["sender_email"] = "test@example.com"
            pcs[p["id"]] = p
        _write_pcs(temp_data_dir, pcs)
        resp = client.post("/api/admin/reparse-empty-pcs?dry_run=1&limit=2")
        assert resp.status_code == 200
        d = resp.get_json()
        assert d["would_reparse"] == 2

    def test_skip_dismissed_and_sent(self, client, temp_data_dir, sample_pc):
        dismissed = dict(sample_pc)
        dismissed["id"] = "pc_dismissed"
        dismissed["items"] = []
        dismissed["status"] = "dismissed"
        dismissed["email_subject"] = "x"
        sent = dict(sample_pc)
        sent["id"] = "pc_sent"
        sent["items"] = []
        sent["status"] = "sent"
        sent["email_subject"] = "x"
        _write_pcs(temp_data_dir, {
            dismissed["id"]: dismissed,
            sent["id"]: sent,
        })
        resp = client.post("/api/admin/reparse-empty-pcs?dry_run=1")
        d = resp.get_json()
        assert d["would_reparse"] == 0


class TestResolvePCRFQDupes:
    """POST /api/admin/resolve-pc-rfq-dupes"""

    def test_dry_run_finds_matching_solicitation(
        self, client, temp_data_dir, sample_pc, sample_rfq
    ):
        # PC and RFQ pointing at the same sol number — the Drew case
        pc = dict(sample_pc)
        pc["id"] = "auto_20260410_test"
        pc["pc_number"] = "26-04-003"
        pc["solicitation_number"] = ""
        pc["status"] = "draft"
        pc["email_subject"] = "CalVet Ref Requisition 26-04-003"
        rfq = dict(sample_rfq)
        rfq["id"] = "20260410_test_abc"
        rfq["solicitation_number"] = "26-04-003"
        rfq["status"] = "draft"
        _write_pcs(temp_data_dir, {pc["id"]: pc})
        _write_rfqs(temp_data_dir, {rfq["id"]: rfq})
        resp = client.post("/api/admin/resolve-pc-rfq-dupes?dry_run=1")
        assert resp.status_code == 200
        d = resp.get_json()
        assert d["ok"] is True
        assert d["would_dismiss"] == 1
        assert d["matches"][0]["rfq_id"] == rfq["id"]
        assert d["matches"][0]["pc_id"] == pc["id"]

    def test_real_run_dismisses_pc_side_by_default(
        self, client, temp_data_dir, sample_pc, sample_rfq
    ):
        pc = dict(sample_pc)
        pc["id"] = "auto_pc_dupe"
        pc["pc_number"] = "TEST-SOL-123"
        pc["status"] = "draft"
        pc["email_subject"] = "x"
        rfq = dict(sample_rfq)
        rfq["id"] = "rfq_match"
        rfq["solicitation_number"] = "TEST-SOL-123"
        rfq["status"] = "draft"
        _write_pcs(temp_data_dir, {pc["id"]: pc})
        _write_rfqs(temp_data_dir, {rfq["id"]: rfq})
        resp = client.post("/api/admin/resolve-pc-rfq-dupes")
        assert resp.status_code == 200
        d = resp.get_json()
        assert d["dismissed_count"] == 1
        assert d["dismissed"][0]["type"] == "pc"
        # Verify the PC was actually marked dismissed. Data layer may have
        # migrated price_checks.json into SQLite, so query via the dashboard
        # helper rather than reading the JSON file directly.
        from src.api.dashboard import _load_price_checks
        updated_pcs = _load_price_checks()
        assert updated_pcs["auto_pc_dupe"]["status"] == "dismissed"
        assert "duplicate of RFQ" in updated_pcs["auto_pc_dupe"]["_dismissed_reason"]

    def test_keep_pc_inverts_direction(
        self, client, temp_data_dir, sample_pc, sample_rfq
    ):
        pc = dict(sample_pc)
        pc["id"] = "auto_keep_pc"
        pc["pc_number"] = "SOL-KEEP-PC"
        pc["status"] = "draft"
        rfq = dict(sample_rfq)
        rfq["id"] = "rfq_drop"
        rfq["solicitation_number"] = "SOL-KEEP-PC"
        rfq["status"] = "draft"
        _write_pcs(temp_data_dir, {pc["id"]: pc})
        _write_rfqs(temp_data_dir, {rfq["id"]: rfq})
        resp = client.post("/api/admin/resolve-pc-rfq-dupes?keep=pc")
        d = resp.get_json()
        assert d["dismissed_count"] == 1
        assert d["dismissed"][0]["type"] == "rfq"

    def test_no_matches_returns_empty(self, client, temp_data_dir, sample_pc, sample_rfq):
        pc = dict(sample_pc)
        pc["id"] = "pc_alone"
        pc["pc_number"] = "ONLY-PC-HERE"
        pc["status"] = "draft"
        rfq = dict(sample_rfq)
        rfq["id"] = "rfq_alone"
        rfq["solicitation_number"] = "TOTALLY-DIFFERENT"
        rfq["status"] = "draft"
        _write_pcs(temp_data_dir, {pc["id"]: pc})
        _write_rfqs(temp_data_dir, {rfq["id"]: rfq})
        resp = client.post("/api/admin/resolve-pc-rfq-dupes?dry_run=1")
        d = resp.get_json()
        assert d["would_dismiss"] == 0
