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

    def test_generate_pc_uses_quote_engine_not_legacy(
        self, auth_client, seed_pc, blank_704_path, monkeypatch
    ):
        """Migration guard: happy path must hit quote_engine.draft, never the legacy fallback."""
        from src.core import quote_engine
        from src.forms import price_check as legacy

        draft_calls = {"n": 0}
        legacy_calls = {"n": 0}

        real_draft = quote_engine.draft

        def spy_draft(*args, **kwargs):
            draft_calls["n"] += 1
            return real_draft(*args, **kwargs)

        def spy_legacy(*args, **kwargs):
            legacy_calls["n"] += 1
            return {"ok": False, "error": "legacy should not be called on happy path"}

        monkeypatch.setattr(quote_engine, "draft", spy_draft)
        monkeypatch.setattr(legacy, "fill_ams704", spy_legacy)

        pcid = seed_pc
        resp = auth_client.post(
            "/api/simple-submit/generate",
            data=json.dumps({
                "doc_type": "pc",
                "doc_id": pcid,
                "items": [{"line_no": 1, "unit_cost": 10.00, "markup_pct": 35}],
                "default_markup": 35,
                "tax_rate": 0,
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert draft_calls["n"] == 1, "quote_engine.draft should be called exactly once"
        assert legacy_calls["n"] == 0, "Legacy fill_ams704 must not be called on happy path"

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
