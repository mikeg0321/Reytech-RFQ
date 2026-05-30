"""ISSUE-19 regression: a base parser that found 0 items must NOT raise a
count_disagreement.

Architecture invariant: Vision is primary; the base/heuristic parser is a
cross-check only. When the base parser extracts nothing (the norm for
generic_rfq PDFs), there is no competing item set to disagree with -- the
RFQ must NOT be flagged for operator review on that basis. The genuinely
useful signal (Vision found nothing while base found items) is preserved.

`_reconcile_vision_and_base(vision_items, base_items, base_parser_label, path)`
returns (items, warnings, needs_review). It calls `_pdf_page_count(path)` for
its zero-items and low-density gates, so we monkeypatch that to 1 page:
that lets the zero-items gate fire on (0,0) while the multi-page-only
low-density gate stays dormant, isolating the count_disagreement behavior
under test.
"""
import pytest

import src.core.ingest_pipeline as ip
from src.core.ingest_pipeline import _reconcile_vision_and_base


def _items(n):
    return [{"description": f"Widget {i}", "quantity": 1, "unit_price": 1.0}
            for i in range(n)]


@pytest.fixture(autouse=True)
def _one_page(monkeypatch):
    # 1 page: zero-items gate can fire (>=1); low-density gate cannot (>=2).
    monkeypatch.setattr(ip, "_pdf_page_count", lambda *_a, **_k: 1)


def _reconcile(v_count, b_count):
    items, warnings, needs_review = _reconcile_vision_and_base(
        _items(v_count), _items(b_count), "generic_rfq", "/tmp/fake.pdf"
    )
    kinds = [w.get("kind") for w in warnings]
    return items, kinds, needs_review


@pytest.mark.parametrize(
    "v,b,exp_review,exp_kind",
    [
        # ISSUE-19 core: base parser inapplicable (0) -> ship Vision, no review.
        (7, 0, False, None),
        (3, 0, False, None),
        # Both empty -> genuine zero_items review (page-aware gate).
        (0, 0, True, "zero_items_on_pdf"),
        # Vision missed what base caught -> still flag (the useful signal).
        (0, 5, True, "count_disagreement"),
        # Agreement -> no review.
        (7, 7, False, None),
        (5, 5, False, None),
        # Real disagreement, both parsers produced items -> flag.
        (7, 2, True, "count_disagreement"),
    ],
)
def test_reconcile_count_vectors(v, b, exp_review, exp_kind):
    _items_out, kinds, needs_review = _reconcile(v, b)
    assert needs_review is exp_review, (
        f"v={v} b={b}: expected needs_review={exp_review}, got {needs_review} "
        f"(warnings={kinds})"
    )
    if exp_kind is None:
        assert "count_disagreement" not in kinds
    else:
        assert exp_kind in kinds


def test_base_zero_does_not_flag_count_disagreement():
    """The exact rfq_c7c073ae signature: vision 7, base 0 -> no review."""
    items, kinds, needs_review = _reconcile(7, 0)
    assert needs_review is False
    assert "count_disagreement" not in kinds
    assert len(items) == 7  # Vision items shipped


def test_count_disagreement_guard_present_in_source():
    """Structural anti-drift: the single count_disagreement emit site must be
    guarded by the base-inapplicable predicate."""
    from pathlib import Path
    src = Path("src/core/ingest_pipeline.py").read_text(encoding="utf-8")
    assert src.count('"kind": "count_disagreement"') == 1
    assert "base_inapplicable = v_count > 0 and b_count == 0" in src
    assert "if delta > threshold and not base_inapplicable:" in src
