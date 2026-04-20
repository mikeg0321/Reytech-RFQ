"""Regression tests for /api/health/profiles.

Guards:
  - endpoint returns ok=True with profile_count matching load_profiles().
  - drift list is empty when manifest matches runtime (steady-state CI).
  - each profile entry carries id/form_type/fill_mode/fingerprint/field_count.
  - drift is reported when manifest is stale (fingerprint/field_count edited).
  - auth is required.
"""
from __future__ import annotations


class TestProfilesEndpointShape:
    def test_returns_ok_and_matches_load_profiles(self, auth_client):
        from src.forms.profile_registry import load_profiles

        live = load_profiles()
        r = auth_client.get("/api/health/profiles").get_json()

        assert r["ok"] is True
        assert r["profile_count"] == len(live)
        assert {p["id"] for p in r["profiles"]} == set(live.keys())

    def test_drift_is_empty_when_manifest_matches_runtime(self, auth_client):
        r = auth_client.get("/api/health/profiles").get_json()
        assert r["drift"] == [], f"unexpected drift: {r['drift']}"

    def test_entries_carry_expected_keys(self, auth_client):
        r = auth_client.get("/api/health/profiles").get_json()
        required = {
            "id", "form_type", "fill_mode", "blank_pdf",
            "blank_exists", "fingerprint", "field_count",
        }
        for p in r["profiles"]:
            assert required <= set(p.keys()), f"missing keys in {p.get('id')}: {required - set(p.keys())}"

    def test_manifest_count_matches_runtime(self, auth_client):
        r = auth_client.get("/api/health/profiles").get_json()
        assert r["manifest_count"] == r["profile_count"]


class TestProfilesEndpointDriftDetection:
    def test_fingerprint_mismatch_appears_in_drift(self, auth_client, monkeypatch):
        """Simulate a stale manifest — drift should surface it."""
        from src.api.modules import routes_health
        from src.forms.profile_registry import load_profiles

        live = load_profiles()
        target_pid = next(iter(
            pid for pid, p in live.items() if p.fingerprint
        ))

        real_manifest = {
            pid: {
                "id": p.id,
                "form_type": p.form_type,
                "fill_mode": p.fill_mode,
                "blank_pdf": p.blank_pdf,
                "fingerprint": p.fingerprint,
                "field_count": len(p.fields),
            }
            for pid, p in live.items()
        }
        real_manifest[target_pid]["fingerprint"] = "0" * 64  # forced mismatch

        import src.forms.profile_registry as pr_mod
        monkeypatch.setattr(pr_mod, "load_manifest", lambda: real_manifest)

        r = auth_client.get("/api/health/profiles").get_json()
        reasons = {d["profile_id"]: d["reason"] for d in r["drift"]}
        assert reasons.get(target_pid) == "fingerprint_mismatch"

    def test_missing_manifest_entry_surfaces_as_drift(self, auth_client, monkeypatch):
        from src.forms.profile_registry import load_profiles
        live = load_profiles()
        target_pid = next(iter(live))

        shrunk = {
            pid: {
                "id": p.id,
                "form_type": p.form_type,
                "fill_mode": p.fill_mode,
                "blank_pdf": p.blank_pdf,
                "fingerprint": p.fingerprint,
                "field_count": len(p.fields),
            }
            for pid, p in live.items()
            if pid != target_pid
        }
        import src.forms.profile_registry as pr_mod
        monkeypatch.setattr(pr_mod, "load_manifest", lambda: shrunk)

        r = auth_client.get("/api/health/profiles").get_json()
        reasons = {d["profile_id"]: d["reason"] for d in r["drift"]}
        assert reasons.get(target_pid) == "missing_from_manifest"


class TestProfilesEndpointAuth:
    def test_requires_auth(self, anon_client):
        r = anon_client.get("/api/health/profiles")
        assert r.status_code in (401, 403)
