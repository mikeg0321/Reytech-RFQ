"""Mike P0 2026-05-06 (Audit P0 #1, #2): atomic JSON write semantics
for the two operator-facing JSON queues.

Both `_save_pending_pos()` (dashboard.py) and `_save_customers()` (routes_crm.py)
hold queue/CRM state that the home banner / CRM rolodex re-reads on every
page load. Pre-fix: a process crash mid-`f.write()` truncated the JSON file
and the page couldn't render until the next email re-scan rebuilt the queue.

These tests are SOURCE-LEVEL — they read the function bodies and assert
that `atomic_json_save` (or equivalent temp+os.replace pattern) is used,
not a bare `open(...,'w')`. Plus an integration test that proves
`atomic_json_save` itself preserves the original file when interrupted.
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── source-level: dashboard._save_pending_pos uses atomic write ────


def test_save_pending_pos_uses_atomic_write():
    body = (REPO / "src/api/dashboard.py").read_text(encoding="utf-8")
    fn_start = body.find("def _save_pending_pos(")
    assert fn_start > 0, "_save_pending_pos not found"
    # Function body — search next ~20 lines
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else fn_start + 800]
    assert "atomic_json_save" in fn_body, (
        "_save_pending_pos must use atomic_json_save (temp file + os.replace), "
        "not bare open()-then-json.dump. A crash mid-write corrupts "
        "pending_po_reviews.json and blocks the home banner."
    )
    # Defensive: make sure the bare open() pattern is gone
    assert 'open(os.path.join(DATA_DIR, "pending_po_reviews.json"), "w")' not in fn_body, (
        "_save_pending_pos still uses bare open()-for-write — atomic write regressed"
    )


# ── source-level: routes_crm._save_customers JSON-fallback uses atomic write ──


def test_save_customers_json_fallback_uses_atomic_write():
    body = (REPO / "src/api/modules/routes_crm.py").read_text(encoding="utf-8")
    fn_start = body.find("def _save_customers(")
    assert fn_start > 0, "_save_customers not found"
    fn_end = body.find("\n@bp.route", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else fn_start + 1200]
    # The JSON fallback path must use atomic_json_save
    assert "atomic_json_save" in fn_body, (
        "_save_customers JSON fallback must use atomic_json_save. "
        "A crash mid-write corrupts customers.json and the CRM rolodex "
        "can't render."
    )


# ── integration: atomic_json_save preserves original on partial-write crash ──


def test_atomic_json_save_preserves_original_on_crash(tmp_path):
    """If the temp-write fails, the original file must remain untouched."""
    from src.core.data_guard import atomic_json_save
    import json

    target = tmp_path / "queue.json"
    # Seed a valid original
    target.write_text(json.dumps([{"po": "ORIG-001", "amount": 100}]),
                      encoding="utf-8")
    original_bytes = target.read_bytes()

    # Trigger a TypeError mid-encode (object circular ref) — the temp file
    # write should fail and the os.replace should never run.
    bad = {}
    bad["self"] = bad  # circular ref → json.dump raises ValueError
    try:
        atomic_json_save(str(target), bad)
    except (ValueError, TypeError, RecursionError):
        pass  # expected
    # Original must be intact
    assert target.read_bytes() == original_bytes, (
        "atomic_json_save corrupted the original on partial-write failure — "
        "temp+os.replace pattern is not isolating the failure"
    )


def test_atomic_json_save_round_trip(tmp_path):
    """Sanity check: atomic_json_save writes valid JSON that round-trips."""
    from src.core.data_guard import atomic_json_save
    import json

    target = tmp_path / "queue.json"
    payload = [{"po": "PO-001", "amount": 200}, {"po": "PO-002", "amount": 350}]
    atomic_json_save(str(target), payload)
    assert json.loads(target.read_text(encoding="utf-8")) == payload
