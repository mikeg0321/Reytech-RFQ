"""Regression guards for the orchestrator observer hooks in
`routes_rfq_gen.py`. The route module is exec-loaded into dashboard.py's
namespace (not an importable package), so we assert wiring statically.

Covered routes (all rfq_id-shaped, same PR-2/PR-3 observer pattern):
  - generate_rfq_package   (POST /rfq/<rid>/generate-package)   [PR-2]
  - generate               (POST /rfq/<rid>/generate)           [PR-3]
  - rfq_generate_quote     (GET  /rfq/<rid>/generate-quote)     [PR-3]
  - api_rfq_manual_submit_704b (POST /api/rfq/<rid>/manual-submit) [PR-3]

The wrapper itself is covered end-to-end in test_orchestrator_legacy_wrapper.py.
"""
from __future__ import annotations

import re
import pytest
from pathlib import Path

ROUTES_FILE = Path(__file__).resolve().parent.parent / "src" / "api" / "modules" / "routes_rfq_gen.py"


def _extract_body(source: str, def_signature: str) -> str:
    """Return the function body up to the next @bp.route or EOF."""
    pattern = re.escape(def_signature) + r"(.*?)(?=\n@bp\.route|\Z)"
    m = re.search(pattern, source, flags=re.DOTALL)
    assert m, f"definition not found: {def_signature}"
    return m.group(1)


OBSERVED_ROUTES = [
    "def generate_rfq_package(rid):",
    "def generate(rid):",
    "def rfq_generate_quote(rid):",
    "def api_rfq_manual_submit_704b(rid):",
]


class TestObserverWiring:
    def test_route_file_exists(self):
        assert ROUTES_FILE.exists(), f"route file missing: {ROUTES_FILE}"

    @pytest.mark.parametrize("signature", OBSERVED_ROUTES)
    def test_route_calls_wrapper(self, signature):
        """Every cut-over route must call run_legacy_package on the rfq_id.
        A future refactor that drops this call fails here so we notice
        before soak silently loses observability."""
        text = ROUTES_FILE.read_text(encoding="utf-8")
        body = _extract_body(text, signature)

        assert "run_legacy_package(" in body, (
            f"{signature} no longer calls run_legacy_package — observer lost."
        )
        # Must reference the route's rfq_id param (named `rid` in all 4).
        assert "rid" in body, (
            f"{signature} call site must reference `rid` — shape drift."
        )

    @pytest.mark.parametrize("signature", OBSERVED_ROUTES)
    def test_observer_is_exception_isolated(self, signature):
        """The observer must be wrapped in try/except so it can never
        crash the legacy route. Regression guard."""
        text = ROUTES_FILE.read_text(encoding="utf-8")
        body = _extract_body(text, signature)

        idx = body.find("run_legacy_package(")
        assert idx != -1, f"{signature}: observer call not present"
        preamble = body[:idx]
        last_try = preamble.rfind("try:")
        assert last_try != -1, (
            f"{signature}: run_legacy_package not inside try/except"
        )
        after_call = body[idx:]
        assert "except" in after_call[:800], (
            f"{signature}: no except clause paired with observer try block"
        )

    @pytest.mark.parametrize("signature", OBSERVED_ROUTES)
    def test_observer_uses_persist_audit_false(self, signature):
        """Observer must NOT write audit rows — that's the legacy route's
        job via lifecycle_events. persist_audit=False keeps the
        quote_audit_log clean during soak."""
        text = ROUTES_FILE.read_text(encoding="utf-8")
        body = _extract_body(text, signature)
        assert "persist_audit=False" in body, (
            f"{signature}: observer must use persist_audit=False"
        )
