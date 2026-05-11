"""Pin the 8 phantom-import drains from batch 3 — clean-rename group.

Each entry was a phantom (module or function rename without caller fix).
Signatures verified against the real callees before patching.

  oracle_weekly.py:71  — iter_categories → all_categories().items()
  sales_intel.py:610    — qb_agent.qb_configured → quickbooks_agent.is_configured (aliased)
  sales_intel.py:610    — qb_agent.get_financial_context → quickbooks_agent.get_financial_context
  scprs_scanner.py:264  — won_quotes_db.get_all_items → load_won_quotes
  routes_intel_ops.py:2266 — won_quotes_db.get_all_items → load_won_quotes
  routes_catalog_finance.py:3283 — quickbooks_agent.fetch_payments → get_recent_payments (aliased)
  routes_prd28.py:1067/1110 — paths.data_path → pathlib.Path(DATA_DIR) / ...
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


class TestOracleWeeklyIterCategoriesDrained:
    def test_no_phantom_iter_categories(self):
        src = _read("src/agents/oracle_weekly.py")
        assert "iter_categories" not in src, \
            "oracle_weekly.py regressed: still references phantom iter_categories"

    def test_uses_all_categories_items(self):
        src = _read("src/agents/oracle_weekly.py")
        assert "from src.core.intel_categories import all_categories" in src
        # Returns dict — caller must iterate .items() to keep (id, label) tuple shape
        assert "all_categories().items()" in src, \
            "must call .items() — all_categories() returns dict, caller iterates tuples"


class TestSalesIntelQbModuleDrained:
    def test_no_phantom_qb_agent_module(self):
        src = _read("src/agents/sales_intel.py")
        assert "from src.agents.qb_agent" not in src, \
            "sales_intel.py regressed: still imports phantom src.agents.qb_agent"

    def test_uses_quickbooks_agent_with_alias(self):
        src = _read("src/agents/sales_intel.py")
        assert "from src.agents.quickbooks_agent import" in src
        # qb_configured was the old API name; alias preserves the call shape
        assert "is_configured as qb_configured" in src


class TestScprsScannerGetAllItemsDrained:
    def test_uses_load_won_quotes(self):
        src = _read("src/agents/scprs_scanner.py")
        assert "from src.knowledge.won_quotes_db import load_won_quotes" in src
        assert "get_all_items" not in src, \
            "scprs_scanner.py regressed: still references phantom get_all_items"


class TestRoutesIntelOpsGetAllItemsDrained:
    def test_lead_qualify_uses_load_won_quotes(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        # The qualify endpoint section must use load_won_quotes for won history
        assert "from src.knowledge.won_quotes_db import load_won_quotes" in src


class TestRoutesCatalogFinanceFetchPaymentsDrained:
    def test_no_phantom_fetch_payments_import(self):
        src = _read("src/api/modules/routes_catalog_finance.py")
        # The import block must alias get_recent_payments to fetch_payments
        # so the downstream call sites that use fetch_payments() still work
        assert "get_recent_payments as fetch_payments" in src, \
            "routes_catalog_finance must alias get_recent_payments → fetch_payments"


class TestRoutesPrd28DataPathDrained:
    def test_no_phantom_data_path_import(self):
        src = _read("src/api/modules/routes_prd28.py")
        assert "from src.core.paths import data_path" not in src, \
            "routes_prd28.py regressed: still imports phantom data_path"

    def test_uses_data_dir_path_construction(self):
        src = _read("src/api/modules/routes_prd28.py")
        # Both sites must build paths via pathlib.Path(DATA_DIR) / filename
        assert 'pathlib.Path(DATA_DIR) / "quotes_log.json"' in src
        assert 'pathlib.Path(DATA_DIR) / "growth_outreach.json"' in src


class TestRealNamesResolve:
    def test_all_categories_resolves_as_dict(self):
        from src.core.intel_categories import all_categories
        result = all_categories()
        assert isinstance(result, dict)

    def test_is_configured_resolves(self):
        from src.agents.quickbooks_agent import is_configured
        assert callable(is_configured)

    def test_get_financial_context_resolves(self):
        from src.agents.quickbooks_agent import get_financial_context
        assert callable(get_financial_context)

    def test_load_won_quotes_resolves(self):
        from src.knowledge.won_quotes_db import load_won_quotes
        assert callable(load_won_quotes)

    def test_get_recent_payments_resolves(self):
        from src.agents.quickbooks_agent import get_recent_payments
        assert callable(get_recent_payments)

    def test_data_dir_resolves(self):
        from src.core.paths import DATA_DIR
        assert isinstance(DATA_DIR, str)
        assert len(DATA_DIR) > 0
