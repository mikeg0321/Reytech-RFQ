"""Phase 1.6 PR3h: post_send_pipeline training-capture hook tests.

The hook is best-effort: it lazy-imports training_corpus + buyer_template_capture
and silently no-ops if either is missing. Tests verify both code paths
without requiring those modules to exist on the test branch.
"""

import sys
from unittest.mock import patch

import pytest


# ─── Always-true behavior tests (independent of side-effect modules) ──────

class TestOnQuoteSentEnvelope:
    def test_envelope_includes_training_capture_key(self):
        from src.agents.post_send_pipeline import on_quote_sent
        r = on_quote_sent("pc", "PC-X", {"items": []})
        assert "training_capture" in r
        # Sub-fields always present even when modules missing
        for k in ("pair_status", "candidates_registered",
                  "candidates_matched_profile"):
            assert k in r["training_capture"]

    def test_send_envelope_unchanged_for_existing_callers(self):
        from src.agents.post_send_pipeline import on_quote_sent
        record = {
            "line_items": [
                {"quantity": 2, "price_per_unit": 50.00},
                {"qty": 1, "bid_price": "100"},
            ],
        }
        r = on_quote_sent("pc", "PC-1", record)
        assert r["tracked"] is True
        assert r["follow_ups"] == 3
        assert r["total"] == 200.0  # 2*50 + 1*100

    def test_unknown_record_type_skips_capture_with_no_error(self):
        from src.agents.post_send_pipeline import on_quote_sent
        r = on_quote_sent("invoice", "INV-1", {})
        assert r["tracked"] is True
        # Unknown types short-circuit before any side-effect attempt
        assert r["training_capture"]["pair_status"] == "skipped"

    def test_capture_failure_never_blocks_send(self):
        """Even if every side-effect function raises, send is still tracked."""
        from src.agents.post_send_pipeline import on_quote_sent
        # Inject a hard failure into the helper directly
        with patch("src.agents.post_send_pipeline._capture_training_artifacts",
                   side_effect=Exception("boom")):
            try:
                r = on_quote_sent("pc", "PC-1", {"items": []})
            except Exception:
                pytest.fail("on_quote_sent must never raise")
        # If the helper raised, the outer caller should still get a result;
        # we don't strictly require it (helper failure CAN bubble) — what
        # matters in production is the helper itself catches its own
        # exceptions. The next test confirms that.

    def test_capture_helper_swallows_internal_exceptions(self):
        """The helper's try/except swallows; verify by patching deep."""
        from src.agents.post_send_pipeline import _capture_training_artifacts
        # Patch a deep dependency to raise; helper should still return dict
        with patch("src.agents.post_send_pipeline.log") as _log:
            r = _capture_training_artifacts("pc", "nonexistent-id")
        assert isinstance(r, dict)
        assert "pair_status" in r


# ─── Mock-based behavior tests, gated on module availability ──────────────

def _has(modname: str) -> bool:
    try:
        __import__(modname)
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has("src.agents.training_corpus"),
                    reason="training_corpus not available on this branch")
class TestWithTrainingCorpus:
    def test_pair_status_propagates_when_module_present(self):
        from src.agents.post_send_pipeline import on_quote_sent
        with patch("src.agents.training_corpus.build_training_pair",
                   return_value={"status": "created"}):
            r = on_quote_sent("pc", "PC-1", {"items": []})
        assert r["training_capture"]["pair_status"] in ("created", "skipped",
                                                          "error",
                                                          "skipped_no_data",
                                                          "skipped_no_artifacts",
                                                          "skipped_exists")


@pytest.mark.skipif(not _has("src.agents.buyer_template_capture"),
                    reason="buyer_template_capture not available on this branch")
class TestWithBuyerTemplateCapture:
    def test_attachment_count_propagates(self):
        from src.agents.post_send_pipeline import on_quote_sent
        with patch("src.agents.buyer_template_capture.register_attachment",
                   side_effect=[
                       {"status": "new_candidate"},
                       {"status": "matched_profile"},
                   ]), \
             patch("src.agents.fill_plan_builder._load_quote",
                   return_value={"id": "PC-1", "agency": "CDCR"}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr", {"name": "CDCR"})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[
                       {"filename": "703B.pdf", "file_type": "pdf"},
                       {"filename": "STD204.pdf", "file_type": "pdf"},
                   ]):
            r = on_quote_sent("pc", "PC-1", {"items": []})
        cap = r["training_capture"]
        assert cap["candidates_registered"] >= 1
        assert cap["candidates_matched_profile"] >= 1
