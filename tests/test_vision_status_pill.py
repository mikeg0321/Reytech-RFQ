"""PR-X — vision-validation status pill at top of PC detail.

The walkthrough audit (P2-8) found the detailed vision_validation panel
(PR-P) sits below the Generate button — an operator could miss a red
FAIL state by clicking Send before scrolling. PR-X hoists a one-line
colored pill into the top status strip alongside the pipeline tracker
so a FAIL is impossible to miss; the pill anchors to the detailed
panel for per-issue review.

Pinned guarantees:
  1. No `vision_validation` set → no pill rendered (no noise on
     parsed-but-not-generated PCs).
  2. overall_passed=True → green pill "All N forms passed".
  3. total_errors > 0 → red pill with error count + "review before
     Send ↓" anchor cue.
  4. skipped_reason set → neutral pill "Vision skipped — run Generate".
  5. warnings only (no errors) → yellow pill with warning count.
  6. Pill href targets #vision-validation-panel anchor.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_pc_with_validation(temp_data_dir, vv, pcid="pc_x_test"):
    from src.api.data_layer import _save_single_pc
    pc = {"id": pcid, "pc_number": "X-TEST", "status": "draft",
          "agency": "cchcs", "items": []}
    if vv is not None:
        pc["vision_validation"] = vv
    _save_single_pc(pcid, pc)
    return pcid


def test_no_pill_when_no_validation(client, temp_data_dir):
    """A PC without vision_validation must not render the pill — no
    noise for parsed-but-not-generated PCs."""
    pcid = _seed_pc_with_validation(temp_data_dir, vv=None,
                                     pcid="pc_no_vv")
    resp = client.get(f"/pricecheck/{pcid}")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="vision-status-pill"' not in body


def test_pass_pill_renders_green_message(client, temp_data_dir):
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_pass", vv={
        "ok": True, "overall_passed": True, "total_errors": 0,
        "total_warnings": 0, "files": [{"name": "f.pdf"}, {"name": "g.pdf"}],
        "summary_line": "2/2 forms passed", "skipped_reason": "",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    assert 'data-testid="vision-status-pill"' in body
    assert "All 2 forms passed" in body


def test_fail_pill_renders_red_with_error_count(client, temp_data_dir):
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_fail", vv={
        "ok": True, "overall_passed": False, "total_errors": 3,
        "total_warnings": 1, "files": [{"name": "f.pdf"}],
        "summary_line": "0/1 forms passed", "skipped_reason": "",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    assert 'data-testid="vision-status-pill"' in body
    assert "3 errors" in body
    assert "1 warning" in body
    assert "review before Send" in body


def test_fail_pill_singular_grammar(client, temp_data_dir):
    """'1 error' not '1 errors' — small thing but the pill is high-
    visibility."""
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_one_err", vv={
        "ok": True, "overall_passed": False, "total_errors": 1,
        "total_warnings": 0, "files": [{"name": "f.pdf"}],
        "summary_line": "0/1 forms passed", "skipped_reason": "",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    # Normalize whitespace so cross-line jinja output doesn't trip
    # the substring check.
    flat = " ".join(body.split())
    assert "1 error" in flat
    assert "1 errors" not in flat


def test_skipped_pill(client, temp_data_dir):
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_skipped", vv={
        "ok": False, "overall_passed": False, "total_errors": 0,
        "total_warnings": 0, "files": [],
        "summary_line": "Validation skipped: no PDFs",
        "skipped_reason": "no generated PDFs",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    assert "Vision skipped" in body


def test_warnings_only_pill(client, temp_data_dir):
    """No errors but warnings present → yellow pill."""
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_warn", vv={
        "ok": True, "overall_passed": False, "total_errors": 0,
        "total_warnings": 2, "files": [{"name": "f.pdf"}],
        "summary_line": "1/1 forms passed, 0 errors, 2 warnings",
        "skipped_reason": "",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    assert "2 warnings" in body


def test_pill_anchors_to_detail_panel(client, temp_data_dir):
    """The pill's href must jump to the detailed vision panel — the
    operator clicks the pill and lands at the per-issue list."""
    pcid = _seed_pc_with_validation(temp_data_dir, pcid="pc_anchor", vv={
        "ok": True, "overall_passed": True, "total_errors": 0,
        "total_warnings": 0, "files": [{"name": "f.pdf"}],
        "summary_line": "1/1 forms passed", "skipped_reason": "",
    })
    body = client.get(f"/pricecheck/{pcid}").get_data(as_text=True)
    assert 'href="#vision-validation-panel"' in body
