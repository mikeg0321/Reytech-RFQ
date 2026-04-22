"""GRILL-Q3 regression: deadline escalation watcher.

Guards:
  1. `start_deadline_watcher` is exported from notify_agent.
  2. It is registered at the intel_ops startup site alongside stale-watcher.
  3. It scans critical/overdue items via the shared helper in routes_deadlines.
  4. It fires send_alert with a per-bid cooldown key + 1h cooldown.
  5. The shared scan helper `_scan_deadlines` exists and accepts an
     urgencies filter set.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _strip_comments_and_docstrings(src: str) -> str:
    src = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
    src = re.sub(r"'''.*?'''", "", src, flags=re.DOTALL)
    return "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )


def test_start_deadline_watcher_exists():
    src = (REPO / "src/agents/notify_agent.py").read_text(encoding="utf-8")
    code = _strip_comments_and_docstrings(src)
    assert "def start_deadline_watcher" in code, \
        "notify_agent must export start_deadline_watcher (GRILL-Q3)"


def test_deadline_watcher_registered_at_startup():
    src = (REPO / "src/api/modules/routes_intel_ops.py").read_text(encoding="utf-8")
    code = _strip_comments_and_docstrings(src)
    assert "start_deadline_watcher" in code, \
        "start_deadline_watcher must be imported + called at startup site"
    assert "start_stale_watcher" in code, \
        "stale watcher registration must still exist (sibling pattern)"


def test_deadline_watcher_uses_per_bid_cooldown():
    src = (REPO / "src/agents/notify_agent.py").read_text(encoding="utf-8")
    code = _strip_comments_and_docstrings(src)
    # Extract just the deadline watcher block so we don't match stale-watcher.
    m = re.search(r"def start_deadline_watcher.*?(?=\ndef |\n# ═)", code, re.DOTALL)
    assert m, "start_deadline_watcher body not found"
    body = m.group(0)
    assert "send_alert" in body, "watcher must call send_alert for each critical item"
    assert "cooldown_key" in body, "watcher must pass cooldown_key (per-bid dedup)"
    assert 'f"deadline_critical:' in body or "'deadline_critical:" in body, \
        "cooldown_key must embed the doc_id (per-bid dedup, not global)"
    assert "cooldown_seconds=3600" in body, \
        "cooldown must be 3600s (once per hour per bid)"


def test_deadline_watcher_scans_via_shared_helper():
    src = (REPO / "src/agents/notify_agent.py").read_text(encoding="utf-8")
    code = _strip_comments_and_docstrings(src)
    m = re.search(r"def start_deadline_watcher.*?(?=\ndef |\n# ═)", code, re.DOTALL)
    body = m.group(0)
    assert "_scan_deadlines" in body, \
        "watcher must reuse _scan_deadlines helper from routes_deadlines"
    assert '"overdue"' in body and '"critical"' in body, \
        "watcher must filter to overdue/critical urgencies only"


def test_scan_deadlines_helper_exists():
    src = (REPO / "src/api/modules/routes_deadlines.py").read_text(encoding="utf-8")
    code = _strip_comments_and_docstrings(src)
    assert "def _scan_deadlines" in code, \
        "routes_deadlines must export _scan_deadlines for watcher reuse"
    # Ensure the helper still feeds the critical endpoint (no drift).
    assert "_scan_deadlines(urgencies={" in code, \
        "api_deadlines_critical should call _scan_deadlines with urgency filter"
