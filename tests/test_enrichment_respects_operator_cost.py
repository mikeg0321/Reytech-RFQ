"""Enrichment must never overwrite operator-typed cost.

Mike P0 2026-05-06: After URL paste, the JS `_backgroundEnrichItem`
fires `/api/pricecheck/<pcid>/retry-auto-price` 3 seconds later. That
endpoint runs `enrich_pc(force=True)`, which iterates Step 4b (Grok
LLM first-pass) and historically would overwrite the operator-typed
`unit_cost` whenever Grok's confidence exceeded the catalog/SCPRS
match confidence.

The bug: "confidence" only describes the AUTO source's certainty in
its own answer. It says nothing about whether the operator's cost is
right. Operator-typed cost is the ground truth — it must NEVER be
overwritten by an enrichment path.

Substrate fix at `pc_enrichment_pipeline.py:749`:
  - OLD: `if not _has_cost or _grok_conf > _best_conf:`
  - NEW: `if not _has_cost:`

Same shape: any future cost-mutation path must guard with
`if not _has_cost` (or equivalent), never with a confidence comparison.

Companion: `rfq_retry_auto_price` (routes_analytics.py) wrapped in
`_save_rfqs_lock` so concurrent autosave + retry can't lose either's
edits — same race shape as PR #778.
"""
from __future__ import annotations

import os


def _read_src(path: str) -> str:
    full = os.path.join(os.path.dirname(__file__), "..", path)
    with open(full, encoding="utf-8") as f:
        return f.read()


def test_step_4b_never_overwrites_operator_cost():
    """The Step 4b Grok first-pass must only fill cost when slot is empty.
    The old `_grok_conf > _best_conf` clause is gone — auto sources don't
    get to override operator truth based on their own self-confidence.
    """
    src = _read_src("src/agents/pc_enrichment_pipeline.py")
    # Look for the apply-cost block — it must NOT contain the old
    # confidence-comparison clause.
    assert "_grok_conf > _best_conf:" not in src, (
        "Step 4b cost-fill must not gate on `_grok_conf > _best_conf` — "
        "operator cost is ground truth, never overridden by AUTO confidence"
    )
    # The new gate is simply `if not _has_cost:` — verify the apply block
    # uses that shape near the unit_cost write.
    apply_idx = src.find('p["unit_cost"] = _price')
    assert apply_idx > 0, "Step 4b apply-cost block missing"
    # Look backwards for the gate within ~300 chars
    gate_window = src[max(0, apply_idx - 400):apply_idx]
    assert "if not _has_cost:" in gate_window, (
        "Step 4b cost-fill must be gated by `if not _has_cost:` — "
        "the operator-cost-protection invariant"
    )


def test_rfq_retry_auto_price_under_lock():
    """rfq_retry_auto_price is fired by `_backgroundEnrichItem` 3s after a
    URL paste; it races directly against operator autosave. Must hold
    `_save_rfqs_lock` across the load → mutate → save."""
    src = _read_src("src/api/modules/routes_analytics.py")
    start = src.index("def rfq_retry_auto_price(")
    next_def = src.find("\n@bp.route", start)
    body = src[start:next_def] if next_def > 0 else src[start:start + 6000]

    assert "from src.api.data_layer import _save_rfqs_lock" in body, (
        "rfq_retry_auto_price must import _save_rfqs_lock"
    )
    assert "with _save_rfqs_lock:" in body, (
        "rfq_retry_auto_price must wrap load+save in `with _save_rfqs_lock:`"
    )
    # load + save both inside the with
    with_idx = body.index("with _save_rfqs_lock:")
    load_idx = body.find("load_rfqs()", with_idx)
    save_idx = body.find("_save_single_rfq(", with_idx)
    assert 0 < with_idx < load_idx < save_idx, (
        "rfq_retry_auto_price: load and save must both appear inside the "
        f"`with _save_rfqs_lock:` block (indices: with={with_idx}, "
        f"load={load_idx}, save={save_idx})"
    )
