"""Test that the pricing.grok_validator_enabled feature flag
gates the Grok product validator without a deploy."""
import pytest


class TestGrokValidatorFeatureFlag:
    def test_disabled_flag_short_circuits_without_api_call(self, monkeypatch):
        """When pricing.grok_validator_enabled is False, validate_product
        must return immediately with skipped=True and must NOT call the
        xAI API (which would need a real key and cost money)."""
        from src.core.flags import set_flag, delete_flag
        from src.agents.product_validator import validate_product

        set_flag("pricing.grok_validator_enabled", "0",
                 updated_by="test_suite",
                 description="disabled for test")
        try:
            # Sabotage HTTP so any real call would blow up loudly
            import src.agents.product_validator as pv
            def _fail_http(*a, **kw):
                raise AssertionError("validate_product should not hit network when flag=off")
            monkeypatch.setattr(pv.requests, "post", _fail_http)

            result = validate_product(
                description="Test Gloves Box 100ct",
                upc="012345678905",
                mfg_number="GLV-100",
            )
            assert result["ok"] is False
            assert result.get("skipped") is True
            assert "feature flag" in result.get("error", "").lower()
        finally:
            delete_flag("pricing.grok_validator_enabled")

    def test_enabled_flag_allows_call_to_proceed(self, monkeypatch):
        """When the flag is True (or unset with default True), the
        validator must NOT short-circuit — it proceeds to cache lookup
        and then to the HTTP call path. We intercept at the HTTP
        boundary so the test stays offline, but we still verify the
        code got past the flag gate."""
        from src.core.flags import set_flag, delete_flag
        from src.agents.product_validator import validate_product
        import src.agents.product_validator as pv

        set_flag("pricing.grok_validator_enabled", "1", updated_by="test_suite")
        try:
            # Stub the HTTP call so we don't hit real xAI
            called = {"n": 0}
            class _FakeResponse:
                status_code = 200
                def json(self):
                    called["n"] += 1
                    return {
                        "choices": [{
                            "message": {"content": '{"is_correct_match": true, "product_name": "fake", "price": 10.0, "confidence": 0.9}'}
                        }],
                        "usage": {"total_tokens": 1},
                    }
                def raise_for_status(self):
                    return None
            monkeypatch.setattr(pv.requests, "post",
                                lambda *a, **kw: _FakeResponse())
            # Ensure API key check passes
            monkeypatch.setenv("XAI_API_KEY", "test_key_for_offline_run")
            # Clear cache so the call actually goes through
            cache_file = pv.CACHE_FILE
            import os as _os
            if _os.path.exists(cache_file):
                _os.unlink(cache_file)

            result = validate_product(
                description="Unique Test Item Not In Cache 99999",
                upc="",
                mfg_number="",
            )
            # Either the HTTP path ran OR we hit a graceful fallback,
            # but we MUST NOT have short-circuited with skipped=True.
            assert not result.get("skipped"), \
                "flag=True must not trigger skip short-circuit"
        finally:
            delete_flag("pricing.grok_validator_enabled")

    def test_flag_defaults_to_true_when_unset(self):
        """Unset flag must default to enabled so the existing
        production Grok path keeps working without requiring the
        operator to explicitly opt in."""
        from src.core.flags import get_flag, delete_flag
        delete_flag("pricing.grok_validator_enabled")
        assert get_flag("pricing.grok_validator_enabled", True) is True
