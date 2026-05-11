"""Pin /health.active_pcs count returns a real integer, not the -1 sentinel.

Background: prod /health was returning active_pcs=-1 indefinitely
because the route imported `load_price_checks` from `src.api.dashboard`,
but the actual function is `_load_price_checks` (with leading underscore).
The ImportError was caught by `except Exception:` and the sentinel was
returned silently. The bug shipped invisible because no test asserted
the field's value.

These tests guarantee:
1. The route imports the *real* name (`_load_price_checks`).
2. `/health` returns a non-negative active_pcs count.
3. Loader returns a dict (so `.items()` works in the route).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(path):
    import pathlib
    return pathlib.Path(path).read_text(encoding="utf-8")


class TestHealthRouteImportsRealLoader:
    def test_route_imports_underscore_loader(self):
        body = _read("src/api/modules/routes_rfq.py")
        assert "from src.api.dashboard import _load_price_checks" in body, (
            "/health must import _load_price_checks (with underscore); "
            "the non-underscore name does not exist"
        )

    def test_route_does_not_import_phantom_name(self):
        body = _read("src/api/modules/routes_rfq.py")
        # The non-underscore form has never existed; if it ever sneaks back
        # in we want the test to scream.
        bad = "from src.api.dashboard import load_price_checks"
        assert bad not in body, (
            "/health regressed: imports nonexistent `load_price_checks`"
        )


class TestLoadPriceChecksContract:
    def test_loader_returns_dict(self):
        from src.api.dashboard import _load_price_checks
        result = _load_price_checks()
        assert isinstance(result, dict), (
            f"_load_price_checks must return dict; got {type(result).__name__}"
        )


class TestHealthEndpoint:
    def test_health_active_pcs_is_non_negative(self, client):
        """GET /health → active_pcs must be a real count, not -1 sentinel."""
        resp = client.get("/health")
        assert resp.status_code in (200, 503), resp.status_code
        data = resp.get_json()
        assert isinstance(data, dict)
        assert "active_pcs" in data
        assert data["active_pcs"] >= 0, (
            f"/health regressed to sentinel: active_pcs={data['active_pcs']}. "
            "Check that _load_price_checks import resolves."
        )
