"""UX Audit 2026-04-14 §9 P0 regression guards.

Pins three hard-blocks added after the UX audit:

  P0.1 — DGS RFQ 20260413_215152_19d88d is do-not-send until the
         parser fix ships. validate_ready_to_send must reject it
         loudly with blocked_reason = 'do_not_send_list'.

  P0.2 — /api/outbox/send-approved and /api/growth/outreach?dry_run=false
         are 423-blocked until the outbox.send_approved_enabled flag
         flips true.

  P0.3 — /api/force-reprocess rejects GET entirely and requires
         {"confirm": "wipe_all"} in the POST body.
"""
import json
import pytest


# ── P0.1 ──────────────────────────────────────────────────────────────────

class TestDoNotSendGate:
    def test_blocked_rfq_fails_validation(self, temp_data_dir):
        from src.core.quote_validator import validate_ready_to_send
        result = validate_ready_to_send({
            "id": "20260413_215152_19d88d",
            "requestor_email": "buyer@example.com",
            "reytech_quote_number": "R26Q999",
            "status": "generated",
            "line_items": [{"description": "Test", "qty": 1, "unit_price": 10}],
        })
        assert result["ok"] is False
        assert result.get("blocked_reason") == "do_not_send_list"
        assert any("BLOCKED" in e for e in result["errors"])

    def test_unknown_rfq_not_blocked_by_do_not_send(self, temp_data_dir):
        """A different RFQ id with the same shape should NOT hit the
        block — the guard must be id-specific."""
        from src.core.quote_validator import validate_ready_to_send
        result = validate_ready_to_send({
            "id": "20260413_215152_other_id",
            "requestor_email": "buyer@example.com",
            "reytech_quote_number": "R26Q888",
            "status": "generated",
            "line_items": [{"description": "Test", "qty": 1, "unit_price": 10}],
        })
        # May still fail other validation (missing files, etc.) but the
        # blocked_reason must not be do_not_send_list
        assert result.get("blocked_reason") != "do_not_send_list"

    def test_runtime_flag_adds_to_blocklist(self, temp_data_dir):
        from src.core.flags import set_flag, _cache_clear_all
        from src.core.quote_validator import is_rfq_do_not_send

        assert is_rfq_do_not_send("runtime_test_rfq") is False
        set_flag("rfq.do_not_send_list", "runtime_test_rfq,another_one")
        _cache_clear_all()
        assert is_rfq_do_not_send("runtime_test_rfq") is True
        assert is_rfq_do_not_send("another_one") is True

    def test_static_list_survives_flag_clear(self, temp_data_dir):
        """The static DGS RFQ id must remain blocked even if the
        runtime flag is empty."""
        from src.core.flags import set_flag, _cache_clear_all
        from src.core.quote_validator import is_rfq_do_not_send

        set_flag("rfq.do_not_send_list", "")
        _cache_clear_all()
        assert is_rfq_do_not_send("20260413_215152_19d88d") is True


# ── P0.2 ──────────────────────────────────────────────────────────────────

class TestSendApprovedBlocked:
    def test_outbox_send_approved_returns_423(self, auth_client, temp_data_dir):
        from src.core.flags import delete_flag, _cache_clear_all
        delete_flag("outbox.send_approved_enabled")
        _cache_clear_all()

        r = auth_client.post("/api/outbox/send-approved")
        assert r.status_code == 423
        d = r.get_json()
        assert d["ok"] is False
        assert "BLOCKED" in d["error"]
        assert d.get("blocked_reason") == "ux_audit_p0_2"

    def test_growth_outreach_dry_run_still_works(self, auth_client, temp_data_dir):
        """dry_run=true must NEVER be blocked — operator needs to
        preview drafts to validate the rewrite."""
        r = auth_client.get("/api/growth/outreach?dry_run=true")
        # We don't assert on shape because the growth agent may not
        # be configured in test env — but we require that it does
        # NOT return 423 from our guard.
        assert r.status_code != 423

    def test_growth_outreach_live_send_returns_423(self, auth_client, temp_data_dir):
        from src.core.flags import delete_flag, _cache_clear_all
        delete_flag("outbox.send_approved_enabled")
        _cache_clear_all()

        r = auth_client.get("/api/growth/outreach?dry_run=false")
        assert r.status_code == 423
        d = r.get_json()
        assert d.get("blocked_reason") == "ux_audit_p0_2"

    def test_flag_flip_re_enables_send_approved(self, auth_client, temp_data_dir):
        """When the rewrite lands, `outbox.send_approved_enabled=true`
        must lift the block without a deploy."""
        from src.core.flags import set_flag, _cache_clear_all
        set_flag("outbox.send_approved_enabled", "true")
        _cache_clear_all()

        r = auth_client.post("/api/outbox/send-approved")
        # With the flag on, the route proceeds to the OUTREACH_AVAILABLE
        # check. In the test env that usually fails, but the KEY
        # assertion is that we're past the 423 guard.
        assert r.status_code != 423


# ── P0.3 ──────────────────────────────────────────────────────────────────

class TestForceReprocessGate:
    def test_get_rejected(self, auth_client, temp_data_dir):
        """GET on the force-reprocess endpoint must not trigger the
        destructive wipe. Flask returns 405 natively, but the app's
        global error handler wraps MethodNotAllowed into 500 — either
        way the key property holds: the destructive code never runs.
        Any 4xx/5xx is fine; only 200 would be a regression."""
        r = auth_client.get("/api/force-reprocess")
        assert r.status_code >= 400
        # Belt-and-suspenders: body must not show success markers
        try:
            d = r.get_json() or {}
            assert d.get("cleared") is None
            assert d.get("reprocessed") is None
        except Exception:
            pass  # non-JSON response is also fine

    def test_post_without_confirm_is_blocked(self, auth_client, temp_data_dir):
        r = auth_client.post("/api/force-reprocess")
        assert r.status_code == 400
        d = r.get_json()
        assert d["ok"] is False
        assert d.get("blocked_reason") == "ux_audit_p0_3"

    def test_post_with_wrong_confirm_is_blocked(self, auth_client, temp_data_dir):
        r = auth_client.post(
            "/api/force-reprocess",
            data=json.dumps({"confirm": "yes"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = r.get_json()
        assert d.get("blocked_reason") == "ux_audit_p0_3"

    def test_post_with_correct_confirm_proceeds(self, auth_client, temp_data_dir):
        """Correct confirm value passes the guard. The function may
        still fail later (missing tables, file IO, etc.) but the
        KEY assertion is that it's NOT rejected with 400 at the
        guard step."""
        r = auth_client.post(
            "/api/force-reprocess",
            data=json.dumps({"confirm": "wipe_all"}),
            content_type="application/json",
        )
        # Past the guard → anything except our 400 blocked-reason
        if r.status_code == 400:
            d = r.get_json()
            assert d.get("blocked_reason") != "ux_audit_p0_3"
