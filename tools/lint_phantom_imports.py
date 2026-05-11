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
BASELINE_EXEMPTIONS: set[str] = set()
# Phantom-import baseline — empty as of 2026-05-10 (session 5/10→5/11).
# Drain history:
    # Drained in batch 2: email_poller order-win flow (4 sites) — module
    # was db_dal phantom; real homes: src.forms.quote_generator (update_quote_status)
    # and src.core.db (log_revenue/record_price); CRM activity → _log_crm_activity.
    # Drained in batch 3:
    #   oracle_weekly iter_categories → all_categories().items()
    #   sales_intel qb_agent → quickbooks_agent (is_configured as qb_configured)
    #   scprs_scanner + routes_intel_ops:2266 get_all_items → load_won_quotes
    #   routes_catalog_finance fetch_payments → get_recent_payments as fetch_payments
    #   routes_prd28 data_path → pathlib.Path(DATA_DIR) / ...
    # Drained in batch 4 (voice_campaigns FEATURE DELETE):
    #   Per Mike 2026-05-10: "app isn't ready for agentic voice yet; quoting +
    #   data accuracy still issues." All 7 voice_campaigns imports, the 7
    #   campaign routes in routes_voice_contacts.py (/campaigns, /campaign/<cid>,
    #   /api/campaigns/*), CAMPAIGNS_AVAILABLE flag in config.py + dashboard.py,
    #   and the 2 templates (voice_campaigns.html, campaign_detail.html)
    #   removed entirely.
    # Drained in batch 5:
    #   queue_background_lookup (2 sites) — added as a daemon-thread wrapper
    #     in scprs_lookup.py with 60s dedup on (description, source).
    #   _remove_processed_uid (1 site) — phantom import removed; the
    #     existing JSON fallback IS the real implementation, inlined.
    #   tax_agent.load_contacts (2 sites) — rewired to _load_crm_contacts
    #     from routes_intel_ops; iterate .values() (dict → list of contacts).
    # Drained in batch 6 (LangGraph orchestrator FEATURE DELETE):
    #   Per Mike 2026-05-10: same playbook as voice_campaigns. The
    #   Feb 2026 LangGraph orchestrator was half-built scaffolding
    #   that never worked end-to-end — `bulk_research`/`scan_for_leads`
    #   were never built in their stated modules, and `WorkflowOrchestrator`
    #   the class doesn't exist anywhere. The real production
    #   orchestrator is src.core.quote_orchestrator.QuoteOrchestrator
    #   (untouched, still in production). This drain removes:
    #     - src/agents/orchestrator.py (617 lines)
    #     - 3 /api/workflow/* routes + status field
    #     - _check_orchestrator() in qa_agent + registration
    #     - ORCHESTRATOR_AVAILABLE flag
    #     - tests/test_orchestrator.py (219 lines)
    #     - 2 orchestrator-pricing tests in test_no_amazon_as_supplier_cost.py
    # Drained in batch 6: vendor_ordering search_pricing → find_similar_items
    #   with shape adaptation (read from quote["supplier"]/quote["unit_price"]).
    # Drained: _create_quote_from_pc had phantom create_quote +
    # increment_quote_counter imports — function had zero callers in
    # the codebase, deleted entirely. Auto-draft path lives in
    # routes_quoting_status.py + src.core.quote_orchestrator instead.
    # Drained: run_deep_pull → deep_pull_all_buyers (real name) at
    # routes_intel_ops:_run_scheduled_scprs_pull. Mon 7am + Wed 10am
    # SCPRS scheduler was silently no-op'ing for unknown duration.
    # Drained in batch 2: routes_rfq.load_pcs (2 sites) → _load_price_checks alias.
    # Drained in batch 2: routes_rfq_admin audit_log → src.core.security._log_audit_internal.
    # Drained: research_items (2 sites in routes_analytics + routes_rfq_gen) —
    # added adapter in web_price_research.py wrapping bulk_web_search and
    # merging hits by idx onto input items (amazon_price/item_link/item_supplier).


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
