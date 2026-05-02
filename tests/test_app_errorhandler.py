"""Regression tests for the catch-all errorhandler.

Guards the bug from 2026-05-01 where `/review-package` redirects (302) being
re-POSTed by `curl -L` raised `MethodNotAllowed` and the catch-all
`@app.errorhandler(Exception)` masked it as a synthesized 500 + JSON. Browser
users never hit it; scripted/curl clients did.

The fix re-raises HTTPException subclasses so Flask renders them natively.
"""
from __future__ import annotations


class TestHTTPExceptionPassthrough:
    """4xx HTTPException subclasses must surface as themselves, not 500."""

    def test_405_method_not_allowed_passes_through(self, client):
        # /ping is registered GET-only — POST triggers MethodNotAllowed (405)
        r = client.post("/ping")
        assert r.status_code == 405, (
            f"Expected 405 MethodNotAllowed but got {r.status_code}. "
            "The catch-all errorhandler is masking HTTPException as 500."
        )

    def test_405_does_not_return_500_json(self, client):
        # The masked-as-500 bug returned {"ok": False, "error": "Internal server error"}
        # for /api/* and /pricecheck/* paths. Confirm that an API path's 405
        # surfaces as a 405 rather than the synthesized 500 envelope.
        r = client.post("/api/this-route-does-not-exist")
        # Either 404 (no such route) or 405 — but NEVER 500.
        assert r.status_code != 500, (
            f"Got 500 for non-existent API path; got body {r.get_data(as_text=True)[:200]}"
        )

    def test_unknown_path_still_returns_404(self, client):
        # 404 already has a specific handler (errorhandler(404)). Confirm the
        # passthrough fix does not regress that path.
        r = client.get("/this-page-does-not-exist-xyz")
        assert r.status_code == 404


class TestRealExceptionsStill500:
    """Genuine unhandled exceptions still surface as 500 (passthrough is 4xx-only)."""

    def test_runtime_error_still_500(self, app, client):
        # Register a temporary route that raises a non-HTTPException — confirm
        # it still goes through the catch-all and returns 500.
        @app.route("/_test/_boom_runtime", methods=["GET"])
        def _boom():
            raise RuntimeError("simulated server fault")

        r = client.get("/_test/_boom_runtime")
        assert r.status_code == 500

    def test_runtime_error_on_api_path_returns_json_500(self, app, client):
        @app.route("/api/_test/_boom_runtime_api", methods=["GET"])
        def _boom_api():
            raise RuntimeError("simulated server fault")

        r = client.get("/api/_test/_boom_runtime_api")
        assert r.status_code == 500
        body = r.get_json()
        assert body is not None and body.get("ok") is False
