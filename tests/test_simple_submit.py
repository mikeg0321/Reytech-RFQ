"""Tests for the Simple Submit route (Phase 0 — fast-path quoting)."""
import json
import pytest


class TestSimpleSubmitPages:
    """GET routes return 200 with correct template context."""

    def test_pc_simple_submit_page(self, auth_client, seed_pc):
        pcid = seed_pc
        resp = auth_client.get(f"/simple-submit/pc/{pcid}")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "Simple Submit" in html
        assert pcid[:8] in html

    def test_rfq_simple_submit_page(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.get(f"/simple-submit/rfq/{rid}")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "Simple Submit" in html

    def test_pc_not_found(self, auth_client):
        resp = auth_client.get("/simple-submit/pc/nonexistent_id_12345")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "not found" in html.lower()

    def test_rfq_not_found(self, auth_client):
        resp = auth_client.get("/simple-submit/rfq/nonexistent_id_12345")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "not found" in html.lower()

    def test_auth_required_pc(self, anon_client):
        resp = anon_client.get("/simple-submit/pc/any_id")
        assert resp.status_code == 401

    def test_auth_required_rfq(self, anon_client):
        resp = anon_client.get("/simple-submit/rfq/any_id")
        assert resp.status_code == 401


class TestSimpleSubmitGenerate:
    """POST /api/simple-submit/generate produces files."""

    def test_generate_pc(self, auth_client, seed_pc, blank_704_path):
        pcid = seed_pc
        resp = auth_client.post(
            "/api/simple-submit/generate",
            data=json.dumps({
                "doc_type": "pc",
                "doc_id": pcid,
                "items": [
                    {"line_no": 1, "unit_cost": 10.00, "markup_pct": 35},
                ],
                "default_markup": 35,
                "tax_rate": 0,
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True

    def test_generate_not_found(self, auth_client):
        resp = auth_client.post(
            "/api/simple-submit/generate",
            data=json.dumps({
                "doc_type": "pc",
                "doc_id": "nonexistent",
                "items": [],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 404

    def test_generate_auth_required(self, anon_client):
        resp = anon_client.post(
            "/api/simple-submit/generate",
            data=json.dumps({"doc_type": "pc", "doc_id": "x", "items": []}),
            content_type="application/json",
        )
        assert resp.status_code == 401


class TestSimpleSubmitDownload:
    """Download endpoints enforce auth and path safety."""

    def test_download_auth_required(self, anon_client):
        resp = anon_client.get("/api/simple-submit/download/test/file.pdf")
        assert resp.status_code == 401

    def test_download_path_traversal_blocked(self, auth_client):
        resp = auth_client.get("/api/simple-submit/download/../../../etc/passwd")
        data = resp.get_json()
        assert data.get("ok") is False or resp.status_code == 400

    def test_download_nonexistent_file(self, auth_client):
        resp = auth_client.get("/api/simple-submit/download/fake_id/fake.pdf")
        assert resp.status_code == 404

    def test_bundle_no_files(self, auth_client):
        resp = auth_client.post(
            "/api/simple-submit/download-bundle",
            data=json.dumps({"files": []}),
            content_type="application/json",
        )
        assert resp.status_code == 400
