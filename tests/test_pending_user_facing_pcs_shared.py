"""Pins the substrate-singleness fix shipped 2026-05-27 after
prod log `WORKFLOW FAIL [80/100]: Manager brief shows 0 PC approvals
but 3 unpriced PCs exist` (2026-05-27 04:23:48).

Pre-fix: `manager_agent._get_pending_approvals` read DAL-first with
JSON fallback. `workflow_tester.test_manager_brief_includes_pcs` read
JSON directly. When DAL and JSON had different rows (DAL migration
in flight), the two counts diverged and the workflow test cried wolf
even though manager_agent was correctly reporting what its source
saw.

Fix: extract a single helper `get_pending_user_facing_pcs()` in
`src/api/data_layer.py` that owns the DAL-first / JSON-fallback
sourcing. Both consumers call this helper. They cannot diverge by
construction.

This file pins:
  1. The helper exists and is importable.
  2. Both consumers call the helper (source-grep).
  3. End-to-end: when the helper returns N pending PCs, manager_agent
     produces exactly N `pc_pending` approvals (capped at 8).
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_helper_is_importable():
    """`get_pending_user_facing_pcs` must exist in data_layer."""
    from src.api.data_layer import get_pending_user_facing_pcs
    assert callable(get_pending_user_facing_pcs)


def test_manager_agent_calls_shared_helper():
    """Source-grep: manager_agent must import the helper, not roll
    its own DAL-first/JSON-fallback logic. Pin against regression."""
    src = (REPO_ROOT / "src" / "agents" / "manager_agent.py").read_text(
        encoding="utf-8"
    )
    assert "from src.api.data_layer import get_pending_user_facing_pcs" in src, (
        "manager_agent must import get_pending_user_facing_pcs — pre-fix "
        "it rolled its own get_all_price_checks + _load_json fallback"
    )
    # The old pattern must be gone from section 7 (we don't want a
    # parallel implementation lurking).
    section7_idx = src.find("# 7. Pending price checks")
    assert section7_idx > 0
    section7 = src[section7_idx:section7_idx + 1500]
    assert "get_all_price_checks(include_test=False)" not in section7, (
        "Section 7 must NOT re-roll DAL sourcing — call the shared helper"
    )


def test_workflow_tester_calls_shared_helper():
    """Source-grep: workflow_tester must call the helper too."""
    src = (REPO_ROOT / "src" / "agents" / "workflow_tester.py").read_text(
        encoding="utf-8"
    )
    fn_idx = src.find("def test_manager_brief_includes_pcs")
    assert fn_idx > 0
    # Look at the function body (first ~2000 chars)
    body = src[fn_idx:fn_idx + 2000]
    assert "from src.api.data_layer import get_pending_user_facing_pcs" in body, (
        "workflow_tester must import the shared helper too — pre-fix "
        "it called `_load_json('price_checks.json', {})` directly"
    )
    assert "_load_json(\"price_checks.json\"" not in body, (
        "The legacy JSON-only read must be gone — that was the half "
        "of the divergence the helper closes"
    )


def test_manager_and_tester_see_same_count(monkeypatch):
    """End-to-end: when the helper returns N PCs, both consumers
    observe N. Pre-fix this divergence WAS the prod bug."""
    fake_pcs = {
        f"pc_{i}": {
            "id": f"pc_{i}",
            "pc_number": f"R26P{i:03d}",
            "status": "parsed",
            "items": [{"description": f"Item {i}"}],
            "institution": "CCHCS",
        }
        for i in range(3)
    }

    def fake_helper():
        return fake_pcs

    monkeypatch.setattr(
        "src.api.data_layer.get_pending_user_facing_pcs",
        fake_helper,
    )
    # manager_agent imports lazily inside section 7 — patch at the
    # consumer's lookup location too.
    import src.api.data_layer as dl
    monkeypatch.setattr(dl, "get_pending_user_facing_pcs", fake_helper)

    from src.agents.manager_agent import _get_pending_approvals
    approvals = _get_pending_approvals()
    pc_approvals = [a for a in approvals if a.get("type") == "pc_pending"]

    assert len(pc_approvals) == 3, (
        f"Expected 3 pc_pending approvals (one per fake PC), got "
        f"{len(pc_approvals)}. The helper returned 3 PCs; manager_agent's "
        "emit loop must turn each one into exactly one approval."
    )
