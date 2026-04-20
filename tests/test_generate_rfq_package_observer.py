"""Regression guard for the orchestrator observer hook in
`generate_rfq_package` (src/api/modules/routes_rfq_gen.py).

The route-level hook is a small, flag-gated call to
`QuoteOrchestrator.run_legacy_package(rid, request.form)` that runs in parallel
with the legacy filler chain. The route module is exec-loaded into
`dashboard.py`'s namespace (not an importable package), so we assert the
wiring statically: the file must reference the wrapper on every generate
route we've cut over.

The wrapper itself is covered end-to-end in test_orchestrator_legacy_wrapper.py.
"""
from __future__ import annotations

import re
from pathlib import Path

ROUTES_FILE = Path(__file__).resolve().parent.parent / "src" / "api" / "modules" / "routes_rfq_gen.py"


class TestObserverWiring:
    def test_route_file_exists(self):
        assert ROUTES_FILE.exists(), f"route file missing: {ROUTES_FILE}"

    def test_generate_rfq_package_calls_wrapper(self):
        """The cutover line must call run_legacy_package(rid, ...) inside
        generate_rfq_package. A future refactor that drops this call should
        fail here so we notice before soak silently loses observability."""
        text = ROUTES_FILE.read_text(encoding="utf-8")

        # Narrow the search to the generate_rfq_package body — otherwise
        # we can't tell which route has the call.
        m = re.search(
            r"def generate_rfq_package\(rid\):(.*?)(?=\n@bp\.route|\Z)",
            text, flags=re.DOTALL,
        )
        assert m, "generate_rfq_package definition not found"
        body = m.group(1)

        assert "run_legacy_package(" in body, (
            "generate_rfq_package no longer calls "
            "QuoteOrchestrator.run_legacy_package — the PR-2 cutover was lost."
        )
        assert "rid" in body and "request.form" in body, (
            "run_legacy_package call in generate_rfq_package must pass "
            "(rid, request.form) — shape drift detected."
        )

    def test_observer_is_exception_isolated(self):
        """The observer must be wrapped in try/except so it can never crash
        the legacy route. Regression guard."""
        text = ROUTES_FILE.read_text(encoding="utf-8")
        m = re.search(
            r"def generate_rfq_package\(rid\):(.*?)(?=\n@bp\.route|\Z)",
            text, flags=re.DOTALL,
        )
        body = m.group(1)

        # Find the run_legacy_package call and confirm it's inside a try.
        # Simple heuristic: the nearest preceding 'try:' within 30 lines.
        idx = body.find("run_legacy_package(")
        preamble = body[:idx]
        last_try = preamble.rfind("try:")
        assert last_try != -1, (
            "run_legacy_package call is not wrapped in try/except — "
            "observer failures could crash the route."
        )
        # The except clause must exist between try: and call site OR after.
        after_call = body[idx:]
        assert "except" in after_call.split("\n\n")[0] or "except" in after_call[:800], (
            "no except clause paired with the observer try block"
        )

    def test_observer_uses_persist_audit_false(self):
        """The observer should NOT write audit rows — that's the legacy
        route's job via lifecycle_events. persist_audit=False keeps the
        quote_audit_log clean during soak."""
        text = ROUTES_FILE.read_text(encoding="utf-8")
        m = re.search(
            r"def generate_rfq_package\(rid\):(.*?)(?=\n@bp\.route|\Z)",
            text, flags=re.DOTALL,
        )
        body = m.group(1)
        assert "persist_audit=False" in body, (
            "orchestrator observer must be constructed with "
            "persist_audit=False to avoid double-writing the audit log"
        )
