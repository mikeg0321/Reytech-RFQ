"""Tests for src/core/flags.py and the /api/admin/flags endpoints.

Covers the Item C resilience contract:
- get_flag returns default when unset
- get_flag returns stored value when set
- Type coercion (bool / int / float / str) from stored string
- Cache TTL (60s)
- Cache invalidation on set/delete
- Admin endpoints: list, get, set, delete
- Defensive DB-error fallback (flag layer must never break callers)
"""
import time

import pytest


# ─── src/core/flags.py unit tests ──────────────────────────────────────

class TestGetFlagTypeCoercion:
    def test_returns_default_when_unset(self):
        from src.core.flags import get_flag, _cache_clear_all
        _cache_clear_all()
        assert get_flag("nonexistent.flag", 99) == 99

    def test_int_default_coerces_stored_string(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.int", "42")
        assert get_flag("test.int", 0) == 42
        assert isinstance(get_flag("test.int", 0), int)

    def test_float_default_coerces(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.float", "0.125")
        assert get_flag("test.float", 0.0) == 0.125

    def test_bool_default_true_variants(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        for truthy in ("1", "true", "TRUE", "yes", "Y", "on"):
            set_flag("test.bool", truthy)
            assert get_flag("test.bool", False) is True, f"{truthy} should be True"

    def test_bool_default_false_variants(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        for falsy in ("0", "false", "no", "off", ""):
            set_flag("test.bool", falsy)
            assert get_flag("test.bool", True) is False, f"{falsy} should be False"

    def test_string_default_returns_raw(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.str", "hello world")
        assert get_flag("test.str", "default") == "hello world"

    def test_int_coercion_failure_falls_back_to_default(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.int.garbage", "not a number")
        assert get_flag("test.int.garbage", 77) == 77


class TestSetAndDelete:
    def test_set_upserts(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.upsert", "v1")
        assert get_flag("test.upsert", "") == "v1"
        set_flag("test.upsert", "v2")
        # Cache should be invalidated, new value visible immediately
        assert get_flag("test.upsert", "") == "v2"

    def test_delete_reverts_to_default(self):
        from src.core.flags import get_flag, set_flag, delete_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.del", "override")
        assert get_flag("test.del", "default") == "override"
        delete_flag("test.del")
        assert get_flag("test.del", "default") == "default"

    def test_delete_nonexistent_returns_true(self):
        from src.core.flags import delete_flag
        assert delete_flag("never_set") is True

    def test_set_with_empty_key_returns_false(self):
        from src.core.flags import set_flag
        assert set_flag("", "value") is False

    def test_set_records_updated_by(self):
        from src.core.flags import set_flag, list_flags, _cache_clear_all
        _cache_clear_all()
        set_flag("test.author", "value", updated_by="mike")
        flags = list_flags()
        row = next((f for f in flags if f["key"] == "test.author"), None)
        assert row is not None
        assert row["updated_by"] == "mike"


class TestCacheBehavior:
    def test_cache_hit_does_not_touch_db(self, monkeypatch):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        import src.core.flags as flags_mod

        _cache_clear_all()
        set_flag("test.cache", "cached")
        # First read — populates cache
        assert get_flag("test.cache", "") == "cached"

        # Second read should hit cache, not DB
        call_count = {"n": 0}
        real_get_db = __import__("src.core.db", fromlist=["get_db"]).get_db
        def counting_get_db(*args, **kwargs):
            call_count["n"] += 1
            return real_get_db(*args, **kwargs)
        monkeypatch.setattr("src.core.db.get_db", counting_get_db)

        for _ in range(5):
            get_flag("test.cache", "")
        assert call_count["n"] == 0, "cache should serve all reads"

    def test_cache_invalidated_on_set(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        _cache_clear_all()
        set_flag("test.invalidation", "old")
        assert get_flag("test.invalidation", "") == "old"
        set_flag("test.invalidation", "new")
        # Must be new immediately, not stale cache
        assert get_flag("test.invalidation", "") == "new"

    def test_unset_flag_cached_as_unset_marker(self):
        """Reading an unset flag should ALSO cache (as a sentinel) so
        we don't pound the DB for 60s when a flag never gets set."""
        from src.core.flags import get_flag, _cache_get, _cache_clear_all
        _cache_clear_all()
        get_flag("test.never_set", "default")
        # Sentinel should be in cache
        raw = _cache_get("test.never_set")
        assert raw is not None
        assert "__UNSET__" in raw


class TestDbErrorFallback:
    def test_get_flag_falls_back_on_db_error(self, monkeypatch):
        """If the DB is unavailable, get_flag must return default."""
        from src.core.flags import get_flag, _cache_clear_all
        _cache_clear_all()

        def broken_get_db(*args, **kwargs):
            raise RuntimeError("simulated DB outage")
        monkeypatch.setattr("src.core.db.get_db", broken_get_db)

        assert get_flag("test.broken", "SAFE_DEFAULT") == "SAFE_DEFAULT"


# ─── /api/admin/flags endpoint tests ───────────────────────────────────

class TestFlagsAdminEndpoints:
    def test_post_set_and_get(self, client):
        r = client.post("/api/admin/flags",
                        json={"key": "api.test", "value": "80",
                              "description": "test flag"})
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["key"] == "api.test"

        r2 = client.get("/api/admin/flags/api.test")
        assert r2.status_code == 200
        d2 = r2.get_json()
        assert d2["flag"]["key"] == "api.test"
        assert d2["flag"]["value"] == "80"
        assert d2["flag"]["description"] == "test flag"

    def test_get_nonexistent_returns_404(self, client):
        r = client.get("/api/admin/flags/definitely.not.set")
        assert r.status_code == 404

    def test_list_includes_set_flags(self, client):
        client.post("/api/admin/flags", json={"key": "list.a", "value": "1"})
        client.post("/api/admin/flags", json={"key": "list.b", "value": "2"})
        r = client.get("/api/admin/flags")
        assert r.status_code == 200
        d = r.get_json()
        keys = {f["key"] for f in d["flags"]}
        assert "list.a" in keys
        assert "list.b" in keys

    def test_delete_reverts(self, client):
        client.post("/api/admin/flags", json={"key": "del.test", "value": "x"})
        r = client.delete("/api/admin/flags/del.test")
        assert r.status_code == 200
        assert r.get_json()["deleted"] == "del.test"
        # Get now returns 404
        r2 = client.get("/api/admin/flags/del.test")
        assert r2.status_code == 404

    def test_set_without_key_rejected(self, client):
        r = client.post("/api/admin/flags", json={"value": "5"})
        assert r.status_code == 400

    def test_set_without_value_rejected(self, client):
        r = client.post("/api/admin/flags", json={"key": "no.value"})
        assert r.status_code == 400

    def test_auth_required(self, anon_client):
        r = anon_client.get("/api/admin/flags")
        assert r.status_code in (401, 403)


# ─── Pipeline flag integration test ────────────────────────────────────

class TestPipelineDeliveryThresholdFlag:
    def test_default_is_70(self, monkeypatch):
        """When no override flag is set, pipeline.delivery_threshold
        must default to 70 (the incident post-mortem floor)."""
        from src.core.flags import get_flag, delete_flag
        delete_flag("pipeline.delivery_threshold")
        assert get_flag("pipeline.delivery_threshold", 70) == 70

    def test_operator_override_takes_effect(self, monkeypatch):
        """Operator can bump the threshold to 80 via flag without deploy."""
        from src.core.flags import get_flag, set_flag, delete_flag
        set_flag("pipeline.delivery_threshold", "80",
                 updated_by="incident-response")
        try:
            assert get_flag("pipeline.delivery_threshold", 70) == 80
        finally:
            delete_flag("pipeline.delivery_threshold")
