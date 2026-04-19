"""Regression tests for the competitor-scan endpoint + semantic-match always-on guard.

E1 batch:
- /api/pricecheck/<pcid>/competitor-scan/<idx> returns top 3 web search results
- pc_enrichment_pipeline calls claude_semantic_match unconditionally — no flag gate
"""
import json
import os
import pytest


def _write_pc_to_store(temp_data_dir, pc):
    """Seed a PC into the JSON store the route reads from."""
    path = os.path.join(temp_data_dir, "price_checks.json")
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)
    existing[pc["id"]] = pc
    with open(path, "w") as f:
        json.dump(existing, f, default=str)


def _stub_search(monkeypatch, response):
    """Patch search_product_price to return a canned response (no API calls)."""
    calls = []
    import src.agents.web_price_research as _wpr

    def _impl(description, part_number="", **kw):
        calls.append({"description": description, "part_number": part_number, **kw})
        return response

    monkeypatch.setattr(_wpr, "search_product_price", _impl)
    return calls


@pytest.fixture
def pc_with_item(temp_data_dir):
    pc = {
        "id": "pc_competitor_test",
        "status": "draft",
        "buyer": {"agency": "CCHCS"},
        "items": [{
            "description": "Vinyl Examination Gloves, Size Medium, Box of 100",
            "mfg_number": "CDM7530A",
            "qty": 50,
            "uom": "BX",
            "pricing": {},
        }],
    }
    _write_pc_to_store(temp_data_dir, pc)
    return pc["id"]


class TestCompetitorScanEndpoint:

    def test_returns_top_3_competitors(self, auth_client, pc_with_item, monkeypatch):
        _stub_search(monkeypatch, {
            "found": True,
            "price": 12.00,
            "source": "Amazon",
            "url": "https://amazon.com/x",
            "title": "Vinyl Gloves Box",
            "options": [
                {"price": 12.00, "source": "Amazon", "url": "https://amazon.com/x", "title": "Vinyl Gloves"},
                {"price": 13.50, "source": "Uline", "url": "https://uline.com/y", "title": "Exam Gloves"},
                {"price": 14.99, "source": "MedLine", "url": "https://medline.com/z", "title": "Medical Gloves"},
                {"price": 16.00, "source": "Grainger", "url": "https://grainger.com/w", "title": "Industrial Gloves"},
            ],
        })

        resp = auth_client.post(f"/api/pricecheck/{pc_with_item}/competitor-scan/0")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["found"] is True
        # Must trim to top 3 (UI surfaces 3 medals)
        assert len(body["competitors"]) == 3
        assert body["competitors"][0]["source"] == "Amazon"
        assert body["best_price"] == 12.00
        assert body["best_url"] == "https://amazon.com/x"

    def test_passes_description_and_mfg_to_search(self, auth_client, pc_with_item, monkeypatch):
        calls = _stub_search(monkeypatch, {
            "found": True, "price": 5.0, "source": "Web", "url": "", "options": [],
        })
        resp = auth_client.post(f"/api/pricecheck/{pc_with_item}/competitor-scan/0")
        assert resp.status_code == 200
        assert len(calls) == 1
        # description and part_number must propagate so the search engine can match
        assert "Vinyl Examination Gloves" in calls[0]["description"]
        assert calls[0]["part_number"] == "CDM7530A"
        # qty/uom must come through too — pricing depends on bulk units
        assert calls[0]["qty"] == 50
        assert calls[0]["uom"] == "BX"

    def test_handles_no_results_gracefully(self, auth_client, pc_with_item, monkeypatch):
        _stub_search(monkeypatch, {"found": False, "error": "No products found"})
        resp = auth_client.post(f"/api/pricecheck/{pc_with_item}/competitor-scan/0")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["found"] is False
        assert body["competitors"] == []
        assert "No products" in (body.get("reason") or "")

    def test_404_on_missing_pc(self, auth_client, monkeypatch):
        # No stub needed — should reject before reaching search
        resp = auth_client.post("/api/pricecheck/pc_does_not_exist/competitor-scan/0")
        assert resp.status_code == 404
        assert resp.get_json()["ok"] is False

    def test_404_on_out_of_range_idx(self, auth_client, pc_with_item):
        resp = auth_client.post(f"/api/pricecheck/{pc_with_item}/competitor-scan/99")
        assert resp.status_code == 404
        assert "not found" in resp.get_json()["error"].lower()

    def test_400_when_no_description_or_mfg(self, auth_client, temp_data_dir):
        pc = {
            "id": "pc_empty_item",
            "status": "draft",
            "items": [{"description": "", "mfg_number": "", "qty": 1, "uom": "EA"}],
        }
        _write_pc_to_store(temp_data_dir, pc)
        resp = auth_client.post(f"/api/pricecheck/{pc['id']}/competitor-scan/0")
        assert resp.status_code == 400

    def test_surfaces_cached_flag_to_ui(self, auth_client, pc_with_item, monkeypatch):
        """UI shows '(cached)' chip when the result came from cache."""
        _stub_search(monkeypatch, {
            "found": True, "price": 9.0, "source": "Amazon",
            "url": "", "options": [{"price": 9.0, "source": "Amazon"}],
            "cached": True,
        })
        resp = auth_client.post(f"/api/pricecheck/{pc_with_item}/competitor-scan/0")
        assert resp.get_json()["cached"] is True


class TestSemanticMatchAlwaysOn:
    """Guard that pc_enrichment_pipeline.py never gates claude_semantic_match
    behind a feature flag. Reasons:
    - Disabling semantic match was the root cause of cross-category mismatches
      (e.g. shoes matching medical items) in the 2026-04 audit.
    - The fallback (`except: trust the result`) handles the offline/Claude-down
      case — the call itself must not be conditional.
    """

    def test_pipeline_calls_semantic_match_without_flag_gate(self):
        import inspect
        from src.agents import pc_enrichment_pipeline as _pep
        src = inspect.getsource(_pep)
        # The call must be present and unconditional on a flag
        assert "claude_semantic_match" in src, \
            "pc_enrichment_pipeline must call claude_semantic_match"
        # Walk the lines and assert the call is not nested under an `if FLAG:`
        # gate. It IS legitimately inside a `try:` (for offline tolerance) and
        # inside `if _found_title and desc:` (no point semantic-matching empty
        # strings) — those are not feature flags.
        lines = src.splitlines()
        for i, line in enumerate(lines):
            if "claude_semantic_match" in line and "import" not in line:
                # Look at the prior 8 lines for any feature-flag-style guard
                window = "\n".join(lines[max(0, i - 8):i])
                forbidden = ("SEMANTIC_MATCH_ENABLED", "ENABLE_SEMANTIC",
                             "FEATURE_SEMANTIC", "if not SEMANTIC", "flag.get(\"semantic")
                for token in forbidden:
                    assert token not in window, (
                        f"semantic match must not be flag-gated; "
                        f"found '{token}' guarding the call at line {i + 1}"
                    )
                break
        else:
            pytest.fail("could not locate claude_semantic_match call site")
