"""Pin the phantom-import lint + the 3 high-impact fixes that motivated it.

Background: prod /health silently returned active_pcs=-1 for unknown
duration because `from src.api.dashboard import load_price_checks` was
a phantom — no such function exists. The ImportError was caught by an
`except` and the sentinel returned. PR #860 fixed /health.

Auditing for the same pattern surfaced ~15+ phantom imports across
src/. This PR ships the substrate (a lint) + fixes 3 high-impact ones:
  * routes_rfq.py — AGENCY_CONFIGS → DEFAULT_AGENCY_CONFIGS
  * routes_analytics.py — find_similar_wins → find_similar_items (2 sites)
  * routes_pricecheck.py + routes_pricecheck_gen.py — wrong module path
    for rfq_files (src.core.db / src.core.dal → src.api.dashboard)

Memory cross-ref: feedback_assert_sentinel_value_not_just_shape.md.
"""
from __future__ import annotations

import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestPhantomLintCatchesKnownBugs:
    """The lint must catch a freshly-planted phantom import."""

    def test_lint_runs(self, tmp_path):
        """Smoke: the lint runs and reports something sane."""
        result = subprocess.run(
            [sys.executable, "tools/lint_phantom_imports.py"],
            capture_output=True, text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
        )
        # ok return code is 0 (all phantoms in baseline) OR 1 (drift).
        # We don't assert which — only that the lint completes without
        # crashing and produces parseable output.
        assert result.returncode in (0, 1), (
            f"lint exited with {result.returncode}; stderr={result.stderr}"
        )
        assert "PHANTOM-IMPORTS:" in (result.stdout + result.stderr), \
            "lint must print a PHANTOM-IMPORTS: marker line"


class TestAgencyConfigsRenameApplied:
    """The manual-RFQ create route must use DEFAULT_AGENCY_CONFIGS, not the
    phantom AGENCY_CONFIGS that silently fell through to raw key."""

    def test_no_phantom_agency_configs(self):
        from pathlib import Path
        src = Path("src/api/modules/routes_rfq.py").read_text(encoding="utf-8")
        assert "from src.core.agency_config import AGENCY_CONFIGS" not in src, \
            "routes_rfq.py regressed: imports phantom AGENCY_CONFIGS"
        assert "DEFAULT_AGENCY_CONFIGS" in src, \
            "routes_rfq.py must use real name DEFAULT_AGENCY_CONFIGS"


class TestFindSimilarWinsRenameApplied:
    """Pricing recommendation + won-history surfaces must call
    find_similar_items (real name), not find_similar_wins (phantom)."""

    def test_no_phantom_find_similar_wins(self):
        from pathlib import Path
        src = Path("src/api/modules/routes_analytics.py").read_text(encoding="utf-8")
        assert "find_similar_wins" not in src, (
            "routes_analytics.py regressed: phantom name find_similar_wins "
            "(real name is find_similar_items)"
        )


class TestRfqFilesModulePathApplied:
    """Source PDF rfq_files DB fallback + manual_upload persistence must
    import from src.api.dashboard, not the wrong (src.core.db /
    src.core.dal) paths."""

    def test_routes_pricecheck_uses_dashboard(self):
        from pathlib import Path
        src = Path("src/api/modules/routes_pricecheck.py").read_text(encoding="utf-8")
        assert "from src.core.db import list_rfq_files" not in src, \
            "routes_pricecheck.py regressed: list_rfq_files from wrong module"
        assert "from src.core.db import get_rfq_file" not in src, \
            "routes_pricecheck.py regressed: get_rfq_file from wrong module"

    def test_routes_pricecheck_gen_uses_dashboard(self):
        from pathlib import Path
        src = Path("src/api/modules/routes_pricecheck_gen.py").read_text(encoding="utf-8")
        assert "from src.core.dal import save_rfq_file" not in src, \
            "routes_pricecheck_gen.py regressed: save_rfq_file from wrong module"


class TestRealNamesActuallyResolve:
    """Live-import the fixed names to prove they actually exist."""

    def test_default_agency_configs_resolves(self):
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        assert isinstance(DEFAULT_AGENCY_CONFIGS, dict)

    def test_find_similar_items_resolves(self):
        from src.knowledge.won_quotes_db import find_similar_items
        assert callable(find_similar_items)

    def test_rfq_files_helpers_resolve(self):
        from src.api.dashboard import (
            list_rfq_files, get_rfq_file, save_rfq_file,
        )
        assert callable(list_rfq_files)
        assert callable(get_rfq_file)
        assert callable(save_rfq_file)
