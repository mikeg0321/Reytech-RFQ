"""Pin that the dead _create_quote_from_pc helper is deleted.

The function imported phantoms `create_quote` and `increment_quote_counter`
from src.forms.quote_generator (neither existed). It had ZERO callers
in the codebase (verified via repo-wide grep) — pure dead scaffold.

Real auto-draft path lives in routes_quoting_status.py +
src.core.quote_orchestrator.QuoteOrchestrator (untouched).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


class TestCreateQuoteFromPcDeleted:
    def test_function_definition_removed(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "def _create_quote_from_pc(" not in src, \
            "regressed: _create_quote_from_pc was supposed to stay deleted"

    def test_no_phantom_create_quote_import(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "from src.forms.quote_generator import (\n        create_quote," not in src
        assert "import create_quote, peek_next_quote_number, increment_quote_counter" not in src

    def test_no_increment_quote_counter_call(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "increment_quote_counter()" not in src, \
            "regressed: phantom increment_quote_counter call should be gone"


class TestRealQuoteOrchestratorStillWorks:
    """The real auto-draft path stays intact."""

    def test_quote_orchestrator_resolves(self):
        from src.core.quote_orchestrator import QuoteOrchestrator
        assert QuoteOrchestrator is not None

    def test_generate_quote_from_pc_resolves(self):
        from src.forms.quote_generator import generate_quote_from_pc
        assert callable(generate_quote_from_pc)
