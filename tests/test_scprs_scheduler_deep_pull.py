"""Pin that the Mon-7am/Wed-10am SCPRS scheduler calls the real
deep-pull function, not a phantom.

Pre-fix bug: `_run_scheduled_scprs_pull()` imported
`run_deep_pull` from src.agents.sales_intel — never existed. The
ImportError was swallowed; the entire scheduled pull silently
no-op'd. The real name is `deep_pull_all_buyers`.

This was the highest-impact silent-fail discovered while draining
the phantom-import baseline: a recurring scheduled job that has
NOT been refreshing sales-intel buyer/agency data via this path.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


class TestSchedulerImportsRealName:
    def test_no_phantom_run_deep_pull(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "from src.agents.sales_intel import run_deep_pull" not in src, \
            "_run_scheduled_scprs_pull regressed: still imports phantom run_deep_pull"

    def test_uses_deep_pull_all_buyers(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "from src.agents.sales_intel import deep_pull_all_buyers" in src, \
            "_run_scheduled_scprs_pull must call the real deep_pull_all_buyers"

    def test_max_queries_param_maps_correctly(self):
        """Prior code passed max_items=200; real API takes max_queries.
        The fix must pass max_queries=200 (intent preserved)."""
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "deep_pull_all_buyers(max_queries=200)" in src, \
            "scheduler must pass max_queries=200 (prior max_items=200 intent)"


class TestRealNameResolves:
    def test_deep_pull_all_buyers_resolves(self):
        from src.agents.sales_intel import deep_pull_all_buyers
        assert callable(deep_pull_all_buyers)
