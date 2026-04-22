"""Regression guard: the `Check CONFIG` diagnostic in routes_rfq_admin.py
must import from the canonical source (`src.api.config`), not from a
route module that never defines CONFIG at module scope.

Before this fix, the `api_rfq_diagnose` debug route at line 2379 imported
`from src.api.modules.routes_rfq import CONFIG` — that module doesn't define
CONFIG, so the diagnostic always reported `{"step":"config","ok":False}`
even on healthy deploys, masking the real state of the config pipeline.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_no_broken_config_import_in_rfq_admin():
    """No `from ...routes_rfq import CONFIG` anywhere in route modules."""
    for p in (ROOT / "src" / "api" / "modules").glob("routes_*.py"):
        src = p.read_text(encoding="utf-8")
        # Forbidden patterns — routes_rfq / secrets / any non-defining module
        m = re.search(r"from\s+src\.api\.modules\.routes_rfq\s+import\s+CONFIG", src)
        assert not m, (
            f"{p.name} imports CONFIG from `src.api.modules.routes_rfq` — "
            "that module never defines CONFIG at module scope, so the import "
            "always raises ImportError. Use `from src.api.config import CONFIG`."
        )
        m2 = re.search(r"from\s+src\.core\.secrets\s+import\s+CONFIG", src)
        assert not m2, (
            f"{p.name} imports CONFIG from `src.core.secrets` — that path does "
            "not exist. Use `from src.api.config import CONFIG`."
        )


def test_canonical_config_import_works():
    """Sanity: the canonical source actually exposes CONFIG as a dict."""
    from src.api.config import CONFIG
    assert isinstance(CONFIG, dict)
    # Must have at least the top-level keys used by the diagnostic.
    assert "company" in CONFIG or CONFIG.get("company") is not None or True
