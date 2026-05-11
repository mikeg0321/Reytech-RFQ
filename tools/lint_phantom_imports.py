"""Lint that catches `from src.X import NAME` where NAME doesn't exist.

The bug shape this prevents: an ImportError caught by `except Exception:`
silently disables a code path forever. Examples that shipped to prod before
this lint existed:

  * /health returned active_pcs=-1 for unknown duration because the route
    imported `load_price_checks` (no underscore) instead of `_load_price_checks`.
    PR #860 fixed.
  * `from src.core.agency_config import AGENCY_CONFIGS` — real name is
    DEFAULT_AGENCY_CONFIGS. Manual RFQ creation silently used raw agency key
    as the display name.
  * `from src.knowledge.won_quotes_db import find_similar_wins` — real name
    is find_similar_items. Pricing "Priority 2: Historical winning price"
    tier silently no-op'd, falling through to catalog pricing instead.

This lint:
  1. Walks every .py under src/
  2. AST-parses each, finds `ImportFrom` nodes whose module starts with src.
  3. For each imported name, verifies that EITHER the module has the
     attribute OR there's an importable submodule at <module>.<name>.
  4. Reports any name that fails both checks.
  5. Exits non-zero if any phantom is found and not in BASELINE_EXEMPTIONS.

Run: `python tools/lint_phantom_imports.py`
Wired into .githooks/pre-push as a fast pre-push check.
"""
from __future__ import annotations

import ast
import importlib
import os
import pathlib
import sys
from typing import Iterator


# Phantom imports that we know are dead and are queued for follow-on cleanup.
# Each entry: "<relative_path>:<line>:<module>:<name>".
# Strict pin: line number must match. If a file's line numbers shift,
# the lint will trip — that's intentional; re-audit the site when it does.
BASELINE_EXEMPTIONS: set[str] = {
    # ───────── Dead names — features removed or renamed without caller fix.
    # Drain these by fixing or deleting the import; do NOT just bump the line.
    # Each entry is one phantom found by ast.walk(); follow-on PRs should
    # delete entries here as they're resolved.
    "src/agents/email_poller.py:2114:src.core.db_dal:update_quote_status",
    "src/agents/email_poller.py:2127:src.core.db_dal:log_revenue",
    "src/agents/email_poller.py:2240:src.core.db_dal:record_price",
    "src/agents/email_poller.py:2301:src.core.db_dal:log_activity",
    "src/agents/oracle_weekly.py:71:src.core.intel_categories:iter_categories",
    "src/agents/orchestrator.py:180:src.agents.product_research:bulk_research",
    "src/agents/orchestrator.py:322:src.agents.lead_gen_agent:scan_for_leads",
    "src/agents/qa_agent.py:2315:src.agents.orchestrator:WorkflowOrchestrator",
    "src/agents/sales_intel.py:610:src.agents.qb_agent:get_financial_context",
    "src/agents/sales_intel.py:610:src.agents.qb_agent:qb_configured",
    "src/agents/scprs_scanner.py:264:src.knowledge.won_quotes_db:get_all_items",
    "src/agents/tax_agent.py:386:src.forms.quote_generator:load_contacts",
    "src/agents/tax_agent.py:390:src.forms.quote_generator:load_contacts",
    "src/agents/vendor_ordering_agent.py:899:src.knowledge.won_quotes_db:search_pricing",
    "src/api/modules/routes_analytics.py:137:src.agents.web_price_research:research_items",
    "src/api/modules/routes_catalog_finance.py:3283:src.agents.quickbooks_agent:fetch_payments",
    "src/api/modules/routes_intel_ops.py:2266:src.knowledge.won_quotes_db:get_all_items",
    "src/api/modules/routes_intel_ops.py:2470:src.forms.quote_generator:create_quote",
    "src/api/modules/routes_intel_ops.py:2470:src.forms.quote_generator:increment_quote_counter",
    "src/api/modules/routes_intel_ops.py:2612:src.agents.sales_intel:run_deep_pull",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:create_campaign",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:execute_campaign_call",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:get_campaign",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:get_campaign_stats",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:get_campaigns",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:list_scripts",
    "src/api/modules/routes_intel_ops.py:325:src.agents.voice_campaigns:update_call_outcome",
    "src/api/modules/routes_prd28.py:1067:src.core.paths:data_path",
    "src/api/modules/routes_prd28.py:1110:src.core.paths:data_path",
    "src/api/modules/routes_pricecheck_pricing.py:373:src.agents.scprs_lookup:queue_background_lookup",
    "src/api/modules/routes_rfq.py:4790:src.api.data_layer:load_pcs",
    "src/api/modules/routes_rfq.py:4842:src.api.data_layer:load_pcs",
    "src/api/modules/routes_rfq_admin.py:1906:src.api.modules.routes_pricecheck:_remove_processed_uid",
    "src/api/modules/routes_rfq_admin.py:3939:src.core.audit_log:log_event",
    "src/api/modules/routes_rfq_gen.py:4314:src.agents.scprs_lookup:queue_background_lookup",
    "src/api/modules/routes_rfq_gen.py:850:src.agents.web_price_research:research_items",
}


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def iter_py_files() -> Iterator[pathlib.Path]:
    src = REPO_ROOT / "src"
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        yield p


def find_phantom_imports() -> list[str]:
    """Returns list of phantom keys (relative_path:line:module:name)."""
    sys.path.insert(0, str(REPO_ROOT))

    findings: list[str] = []

    for path in iter_py_files():
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            tree = ast.parse(text)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if not node.module or not node.module.startswith("src."):
                continue
            for alias in node.names:
                name = alias.name
                if name == "*":
                    continue

                # 1. Try importing the module and checking attribute.
                has_attr = False
                try:
                    mod = importlib.import_module(node.module)
                    has_attr = hasattr(mod, name)
                except Exception:
                    pass

                # 2. If module didn't have attr, try the alternative:
                #    `from PKG import SUBMODULE` is OK if PKG.SUBMODULE
                #    is itself an importable module.
                is_submodule = False
                if not has_attr:
                    try:
                        importlib.import_module(f"{node.module}.{name}")
                        is_submodule = True
                    except Exception:
                        pass

                if not has_attr and not is_submodule:
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    key = f"{rel}:{node.lineno}:{node.module}:{name}"
                    findings.append(key)

    return findings


def main() -> int:
    findings = find_phantom_imports()
    new_phantoms = [f for f in findings if f not in BASELINE_EXEMPTIONS]
    stale_exemptions = sorted(BASELINE_EXEMPTIONS - set(findings))

    if new_phantoms:
        print("PHANTOM-IMPORTS: new findings not in baseline:")
        for f in sorted(new_phantoms):
            print(f"  {f}")
        print()
        print("Each entry is `from <module> import <name>` where <name> does")
        print("not exist in <module>. Most are silently swallowed by an")
        print("`except Exception:` and the code path no-ops forever.")
        print()
        print("Fix the import (correct name or correct module path), or — if")
        print("the dead path is intentional — add the key to BASELINE_EXEMPTIONS")
        print("in tools/lint_phantom_imports.py with a comment explaining why.")
        return 1

    if stale_exemptions:
        print("PHANTOM-IMPORTS: stale exemptions (no longer findings):")
        for f in stale_exemptions:
            print(f"  {f}")
        print()
        print("Remove these from BASELINE_EXEMPTIONS — the underlying issue")
        print("was fixed, the exemption is dead weight.")
        return 1

    print(f"PHANTOM-IMPORTS: ok ({len(findings)} known, all exempt).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
