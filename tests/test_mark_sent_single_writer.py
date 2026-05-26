"""Pin: src/api has ZERO inline writers of `record["sent_at"]`.

Audit Item 6 / PR #11 / 2026-05-26: PR #1078 added the
`propagate_sent_to_quote_row` helper but left 8 inline call sites
(routes_rfq_admin, routes_rfq_gen, routes_pricecheck_admin,
routes_pricecheck_pricing, routes_pricecheck_gen, routes_pricecheck)
each doing the 4-step
    _transition_status + r["sent_at"] + r["sent_to"] + r["sent_method"]
dance, plus a separate propagate_sent_to_quote_row tail. That's 9
writers of the 'sent' state across the legacy substrate, the exact
substrate-singleness defect class Mike named as dominant.

The fix: a single `mark_sent_in_place` helper in
`src/core/quote_lifecycle_shared.py` that owns the entire transition.
Every callsite now delegates to it.

This test grep-pins the invariant. If a future PR adds a new inline
`r["sent_at"] = ...` or `pc["sent_at"] = ...` ANYWHERE under src/api,
this test fails — author must route through mark_sent_in_place
instead. Same shape as the existing CI guards
(`test_notify_agent_seam_pin.py`, `test_gmail_api_migration_startup_checks.py`).
"""
from __future__ import annotations

import re
from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_API_DIR = _REPO / "src" / "api"
_HELPER = _REPO / "src" / "core" / "quote_lifecycle_shared.py"


# Match `r["sent_at"] = ...` or `pc["sent_at"] = ...` (any whitespace,
# single OR double quotes) — i.e., dict-style direct writes on the
# entity dict. The helper itself uses `record["sent_at"] = sent_at`
# which we WHITELIST (it's the single-writer site).
_INLINE_SENT_AT_RE = re.compile(
    r"""\b(?:r|pc|rec|record)\[\s*["']sent_at["']\s*\]\s*="""
)


def _iter_py_files(root: Path):
    for p in root.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        yield p


def test_no_inline_sent_at_writers_in_src_api():
    """src/api MUST NOT contain any inline `r["sent_at"] = ...` or
    `pc["sent_at"] = ...`. All such writes route through
    `mark_sent_in_place` in quote_lifecycle_shared.py.

    Violations: re-grep your file for the pattern and replace the
    block with a single `mark_sent_in_place(record, sent_at=..., ...)`
    call. See PR #11 / audit Item 6 for the rationale.
    """
    violations = []
    for fpath in _iter_py_files(_API_DIR):
        rel = fpath.relative_to(_REPO).as_posix()
        for lineno, line in enumerate(fpath.read_text(encoding="utf-8").splitlines(), 1):
            if _INLINE_SENT_AT_RE.search(line):
                violations.append(f"{rel}:{lineno}  {line.strip()}")
    assert not violations, (
        "PR #11 substrate-singleness violation — inline sent_at writers "
        "found in src/api. Route through mark_sent_in_place in "
        "src/core/quote_lifecycle_shared.py instead:\n  "
        + "\n  ".join(violations)
    )


def test_mark_sent_in_place_helper_exists_and_is_unique():
    """Pin the helper name + that it lives in quote_lifecycle_shared.
    If you rename it, update the pin AND every callsite together."""
    src = _HELPER.read_text(encoding="utf-8")
    assert "def mark_sent_in_place(" in src, (
        "mark_sent_in_place must live in src/core/quote_lifecycle_shared.py"
    )


def test_mark_sent_in_place_signature_pinned():
    """Lock the keyword-only kwargs the callsites depend on. Adding
    new optional kwargs is fine; removing/renaming these breaks
    the 8 refactored callsites."""
    import inspect
    from src.core.quote_lifecycle_shared import mark_sent_in_place
    sig = inspect.signature(mark_sent_in_place)
    expected_kw = {"sent_at", "sent_to", "sent_method", "notes",
                   "source", "skip_transition"}
    actual = {n for n, p in sig.parameters.items()
              if p.kind == inspect.Parameter.KEYWORD_ONLY}
    missing = expected_kw - actual
    assert not missing, f"mark_sent_in_place missing kwargs: {missing}"


def test_mark_sent_in_place_writes_sent_at_and_propagates(monkeypatch):
    """Smoke: helper sets the right fields + calls propagate."""
    from src.core import quote_lifecycle_shared as qls

    # Stub _transition_status so we don't drag in the full route module.
    def fake_transition(record, target, *, actor=None, notes=None):
        record["status"] = target

    import src.api.modules.routes_rfq as routes_rfq
    monkeypatch.setattr(routes_rfq, "_transition_status", fake_transition)

    propagated = []
    monkeypatch.setattr(
        qls, "propagate_sent_to_quote_row",
        lambda r, source: (propagated.append(source), True)[1],
    )

    record = {"id": "rfq_x", "reytech_quote_number": "R26Q40"}
    result = qls.mark_sent_in_place(
        record, sent_at="2026-05-26T10:00:00",
        sent_to="buyer@cchcs.ca.gov", sent_method="manual",
        notes="test", source="user",
    )

    assert record["status"] == "sent"
    assert record["sent_at"] == "2026-05-26T10:00:00"
    assert record["sent_to"] == "buyer@cchcs.ca.gov"
    assert record["sent_method"] == "manual"
    assert result["transitioned"] is True
    assert result["propagated"] is True
    assert propagated == ["user"]


def test_mark_sent_in_place_skip_transition(monkeypatch):
    """skip_transition=True does NOT call _transition_status (caller
    already did) but still writes fields + propagates."""
    from src.core import quote_lifecycle_shared as qls

    called_transition = []
    import src.api.modules.routes_rfq as routes_rfq
    monkeypatch.setattr(
        routes_rfq, "_transition_status",
        lambda *a, **kw: called_transition.append(True),
    )
    monkeypatch.setattr(
        qls, "propagate_sent_to_quote_row", lambda r, source: True
    )

    record = {"reytech_quote_number": "R26Q41"}
    result = qls.mark_sent_in_place(
        record, sent_at="2026-05-26T11:00:00",
        skip_transition=True,
    )

    assert called_transition == []  # transition NOT called
    assert record["sent_at"] == "2026-05-26T11:00:00"
    assert result["transitioned"] is False
    assert result["propagated"] is True
