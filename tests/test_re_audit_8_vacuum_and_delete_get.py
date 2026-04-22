"""RE-AUDIT-8 regression guard.

Two P1 hardenings on `src/api/dashboard.py`:

1. **VACUUM single-flight.** `api_disk_cleanup?action=vacuum` runs VACUUM on
   the 525 MB prod DB. VACUUM holds an exclusive write lock for 30-60s. A
   double-click on the admin button (or two ops hitting the same route in
   parallel) queues two concurrent VACUUMs — the second blocks behind the
   first, pinning every writer on the DB for minutes. Fix: a module-level
   `_vacuum_lock = threading.Lock()`, `acquire(blocking=False)` at entry,
   409 if already held, release in `finally`.

2. **GET-accepting delete routes.** Three admin destructive routes still
   accepted `GET`:
   - `/api/admin/delete-pc/<pcid>`
   - `/api/admin/delete-rfq/<rid>`
   - `/api/admin/delete-by-sol`
   A `<img src="/api/admin/delete-rfq/<rid>">` tag posted anywhere the
   admin's browser loads (a phishing email rendered in Gmail web, a
   markdown preview, a stale tab) fires a delete with the admin's
   session cookie. CSRF 101. Fix: `methods=["POST"]` only.

Audit row: both start (`db_vacuum_start`) and completion (`db_vacuum_done`)
records should land in the audit log so ops can reconcile who ran what.
"""
from __future__ import annotations

import re
from pathlib import Path


DASH = Path(__file__).resolve().parents[1] / "src" / "api" / "dashboard.py"


def _source() -> str:
    return DASH.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return "\n".join(out)


def _route_block(src: str, pattern: str) -> str:
    """Return the block from `@bp.route(pattern, ...)` to the next route."""
    lines = src.splitlines()
    start = None
    for i, line in enumerate(lines):
        if "@bp.route(" in line and pattern in line:
            start = i
            break
    assert start is not None, f"route {pattern!r} not found in dashboard.py"
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("@bp.route(") or lines[j].startswith("def "):
            end = j
            break
    return "\n".join(lines[start:end])


def test_vacuum_single_flight_lock_defined():
    """A module-level _vacuum_lock must exist alongside the other save locks."""
    src = _source()
    assert re.search(r"_vacuum_lock\s*=\s*threading\.Lock\(\)", src), (
        "RE-AUDIT-8 regression: dashboard.py is missing the module-level "
        "_vacuum_lock = threading.Lock(). A double-click on the admin "
        "VACUUM button will queue two concurrent VACUUMs and pin all "
        "writers on the DB for minutes."
    )


def test_vacuum_block_uses_single_flight():
    """The vacuum action must acquire-or-409 before running VACUUM."""
    src = _source()
    # Isolate the vacuum branch.
    m = re.search(
        r'if action == "vacuum":([\s\S]*?)(?=\n    if action ==|\n@bp\.route|\Z)',
        src,
    )
    assert m, "vacuum action branch not located"
    block = _strip_comments(m.group(0))
    assert re.search(
        r"_vacuum_lock\.acquire\(\s*blocking\s*=\s*False\s*\)",
        block,
    ), (
        "RE-AUDIT-8 regression: the vacuum branch is not guarded by "
        "_vacuum_lock.acquire(blocking=False). Without it, a double-click "
        "on the admin VACUUM button queues two 30-60s write-locks back-to-back."
    )
    assert "409" in block, (
        "RE-AUDIT-8 regression: the vacuum branch must return 409 when "
        "the lock is held. Silently queueing the second call defeats the "
        "single-flight guard."
    )
    assert re.search(r"_vacuum_lock\.release\(\s*\)", block), (
        "RE-AUDIT-8 regression: the vacuum branch must release "
        "_vacuum_lock in a finally: block — otherwise a crash during "
        "VACUUM pins the lock forever and every subsequent call 409s."
    )


def test_vacuum_block_emits_audit_rows():
    """Start + done audit rows must be emitted around the VACUUM call."""
    src = _source()
    m = re.search(
        r'if action == "vacuum":([\s\S]*?)(?=\n    if action ==|\n@bp\.route|\Z)',
        src,
    )
    assert m, "vacuum action branch not located"
    block = _strip_comments(m.group(0))
    assert "db_vacuum_start" in block, (
        "RE-AUDIT-8 regression: vacuum branch missing the db_vacuum_start "
        "audit row. Ops cannot reconcile who kicked off a destructive "
        "VACUUM without the start record."
    )
    assert "db_vacuum_done" in block, (
        "RE-AUDIT-8 regression: vacuum branch missing the db_vacuum_done "
        "audit row. Without completion records, the start-row lookup "
        "cannot tell crashed runs apart from long-running ones."
    )


def test_delete_pc_route_is_post_only():
    """/api/admin/delete-pc/<pcid> must reject GET."""
    src = _source()
    block = _route_block(src, "/api/admin/delete-pc/<pcid>")
    assert '"GET"' not in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-pc still accepts GET. "
        "An <img src=.../api/admin/delete-pc/PCID> tag in a rendered "
        "email fires this with the admin's session cookie."
    )
    assert '"POST"' in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-pc must still accept "
        "POST — the legit UI posts to it."
    )


def test_delete_rfq_route_is_post_only():
    """/api/admin/delete-rfq/<rid> must reject GET."""
    src = _source()
    block = _route_block(src, "/api/admin/delete-rfq/<rid>")
    assert '"GET"' not in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-rfq still accepts GET. "
        "CSRF via <img> works against any GET-accepting destructive route."
    )
    assert '"POST"' in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-rfq must still accept "
        "POST — the legit UI posts to it."
    )


def test_delete_by_sol_route_is_post_only():
    """/api/admin/delete-by-sol must reject GET."""
    src = _source()
    block = _route_block(src, "/api/admin/delete-by-sol")
    assert '"GET"' not in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-by-sol still accepts "
        "GET. This route deletes PCs + RFQs by solicitation number; "
        "CSRF via <img> wipes entire solicitations."
    )
    assert '"POST"' in block, (
        "RE-AUDIT-8 regression: /api/admin/delete-by-sol must still "
        "accept POST — the legit admin UI posts to it."
    )
