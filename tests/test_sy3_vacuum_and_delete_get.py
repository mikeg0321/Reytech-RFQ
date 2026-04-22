"""SY-3 / RE-AUDIT-8 regression guards: VACUUM + destructive admin GET routes.

Audited 2026-04-22. Three admin delete routes (/api/admin/delete-pc/<id>,
/api/admin/delete-rfq/<id>, /api/admin/delete-by-sol) admitted GET. The CSRF
origin guard only fires for POST/PUT/DELETE/PATCH, so a crafted
`<img src="/api/admin/delete-by-sol?sol=10840485&type=both">` on any page an
authenticated admin opens would silently drop records from their session.

/api/disk-cleanup additionally exposed a `?action=vacuum` GET that takes an
exclusive write lock on the 525 MB prod DB for 30-60s, with no rate limit
and no single-flight guard — double-clicks would stack two VACUUMs, and an
`<img>` tag could trigger one cross-site.

Fix:
  - Delete routes: POST only (403 on GET + 405 on GET).
  - /api/disk-cleanup: keep GET for the read-only status view, but reject any
    destructive `?action=...` on GET with 405.
  - VACUUM: single-flight non-blocking lock + audit rows for started/ok/
    failed/rejected_in_flight.
"""
from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "src" / "api" / "dashboard.py"


def _find_decorator_for(route: str, src: str) -> str:
    pattern = rf'@bp\.route\(\s*"{re.escape(route)}"[^)]*\)'
    m = re.search(pattern, src)
    return m.group(0) if m else ""


# ── Source-level guards: delete routes POST-only ────────────────────────────

def test_delete_pc_post_only():
    src = DASHBOARD.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/admin/delete-pc/<pcid>", src)
    assert dec, "SY-3: /api/admin/delete-pc/<pcid> decorator not found"
    assert '"GET"' not in dec, (
        f"SY-3 regression: /api/admin/delete-pc admits GET — {dec!r}. "
        "GET + CSRF origin-guard bypass = silent PC deletion via `<img>` tag."
    )
    assert '"POST"' in dec


def test_delete_rfq_post_only():
    src = DASHBOARD.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/admin/delete-rfq/<rid>", src)
    assert dec, "SY-3: /api/admin/delete-rfq/<rid> decorator not found"
    assert '"GET"' not in dec, (
        f"SY-3 regression: /api/admin/delete-rfq admits GET — {dec!r}. "
        "GET + CSRF origin-guard bypass = silent RFQ deletion via `<img>` tag."
    )
    assert '"POST"' in dec


def test_delete_by_sol_post_only():
    src = DASHBOARD.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/admin/delete-by-sol", src)
    assert dec, "SY-3: /api/admin/delete-by-sol decorator not found"
    assert '"GET"' not in dec, (
        f"SY-3 regression: /api/admin/delete-by-sol admits GET — {dec!r}. "
        "This route deletes EVERY PC+RFQ matching a solicitation — GET + CSRF "
        "is a bulk-data-destruction vector."
    )
    assert '"POST"' in dec


# ── Source-level guards: disk-cleanup VACUUM defenses ──────────────────────

def test_disk_cleanup_accepts_post():
    src = DASHBOARD.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/disk-cleanup", src)
    assert dec, "SY-3: /api/disk-cleanup decorator not found"
    assert '"POST"' in dec, (
        f"SY-3: /api/disk-cleanup must accept POST so destructive actions "
        "can be invoked without 405 — {dec!r}."
    )


def test_destructive_actions_registered():
    """All four destructive actions must be in the POST-required set."""
    src = DASHBOARD.read_text(encoding="utf-8")
    m = re.search(r"_DESTRUCTIVE_CLEANUP_ACTIONS\s*=\s*\{([^}]+)\}", src)
    assert m, "SY-3: _DESTRUCTIVE_CLEANUP_ACTIONS set not found"
    body = m.group(1)
    for action in ("clean", "nuke-uploads", "vacuum", "trim-data"):
        assert f'"{action}"' in body, (
            f"SY-3 regression: {action!r} missing from "
            f"_DESTRUCTIVE_CLEANUP_ACTIONS — will be allowed via GET again."
        )


def test_destructive_get_guard_present():
    """api_disk_cleanup must early-return 405 on destructive GET."""
    src = DASHBOARD.read_text(encoding="utf-8")
    # Grab the api_disk_cleanup function body (terminate at next top-level
    # def or EOF — api_disk_cleanup is the last @bp.route in the file).
    func_match = re.search(
        r"^def api_disk_cleanup\(.*?(?=\n\ndef [a-zA-Z_]+\(|\Z)",
        src,
        flags=re.DOTALL | re.MULTILINE,
    )
    assert func_match, "SY-3: api_disk_cleanup function body not found"
    body = func_match.group(0)
    assert "_DESTRUCTIVE_CLEANUP_ACTIONS" in body, (
        "SY-3 regression: destructive-action GET guard removed from "
        "api_disk_cleanup."
    )
    assert 'request.method != "POST"' in body, (
        "SY-3 regression: destructive-action GET guard must compare "
        "request.method against POST."
    )
    assert " 405" in body, (
        "SY-3 regression: destructive-action GET guard must return status "
        "405 to block CSRF `<img>` triggers."
    )


def test_vacuum_singleflight_lock_defined():
    """Module-level non-blocking lock around VACUUM."""
    src = DASHBOARD.read_text(encoding="utf-8")
    assert "_vacuum_singleflight_lock" in src, (
        "SY-3 regression: _vacuum_singleflight_lock removed — double-clicks "
        "will stack two 30-60s VACUUM write locks on the 525 MB prod DB."
    )
    # The lock acquire must be non-blocking so the second caller gets 429
    # immediately instead of blocking the gunicorn worker for 60s.
    assert "_vacuum_singleflight_lock.acquire(blocking=False)" in src, (
        "SY-3 regression: VACUUM lock must use acquire(blocking=False) so a "
        "second caller returns 429 immediately instead of blocking a worker."
    )
    assert "_vacuum_singleflight_lock.release()" in src, (
        "SY-3 regression: VACUUM lock release() missing — every acquired "
        "path must release in a `finally:` block or the lock leaks."
    )


def test_vacuum_audits_every_outcome():
    """Every VACUUM attempt must write an audit row."""
    src = DASHBOARD.read_text(encoding="utf-8")
    for phrase in (
        'db_vacuum", f"rejected_in_flight',
        'db_vacuum", f"started',
        'db_vacuum", f"ok freed_mb=',
        'db_vacuum", f"failed:',
    ):
        assert phrase in src, (
            f"SY-3 regression: VACUUM audit row missing {phrase!r}. "
            "Forensics need every attempt recorded — even contention."
        )


# ── Functional: Flask routes reject GET at framework level ─────────────────

class TestDestructiveRoutesRejectGet:
    """Hit the real routes via test client — Flask rejects GET.

    Flask raises MethodNotAllowed → status 405. The app's global error
    handler wraps it as 500 in the test environment; either is fine as
    long as the GET does NOT succeed (i.e. status != 200) and is NOT
    a redirect (3xx). The invariant is: a crafted `<img src=...>` must
    not succeed in deleting records.
    """

    def test_delete_pc_get_rejected(self, client):
        r = client.get("/api/admin/delete-pc/pc_fake")
        assert r.status_code in (405, 500), (
            f"SY-3 regression: GET /api/admin/delete-pc returned "
            f"{r.status_code}; GET must be rejected (405 direct or 500 "
            "from the global error handler wrapping MethodNotAllowed). "
            "CSRF bypass is back."
        )

    def test_delete_rfq_get_rejected(self, client):
        r = client.get("/api/admin/delete-rfq/rfq_fake")
        assert r.status_code in (405, 500), (
            f"SY-3 regression: GET /api/admin/delete-rfq returned "
            f"{r.status_code}; GET must be rejected."
        )

    def test_delete_by_sol_get_rejected(self, client):
        r = client.get("/api/admin/delete-by-sol?sol=10840485&type=both")
        assert r.status_code in (405, 500), (
            f"SY-3 regression: GET /api/admin/delete-by-sol returned "
            f"{r.status_code}; GET must be rejected. Bulk data-destruction "
            "vector."
        )

    def test_disk_cleanup_vacuum_get_returns_405(self, client):
        r = client.get("/api/disk-cleanup?action=vacuum")
        assert r.status_code == 405, (
            f"SY-3 regression: GET /api/disk-cleanup?action=vacuum returned "
            f"{r.status_code}, expected 405. VACUUM takes a 30-60s exclusive "
            "write lock on prod — must require POST."
        )

    def test_disk_cleanup_clean_get_returns_405(self, client):
        r = client.get("/api/disk-cleanup?action=clean")
        assert r.status_code == 405

    def test_disk_cleanup_nuke_uploads_get_returns_405(self, client):
        r = client.get("/api/disk-cleanup?action=nuke-uploads")
        assert r.status_code == 405

    def test_disk_cleanup_trim_data_get_returns_405(self, client):
        r = client.get("/api/disk-cleanup?action=trim-data")
        assert r.status_code == 405

    def test_disk_cleanup_status_get_still_works(self, client):
        """GET with no action must still render the read-only status view."""
        r = client.get("/api/disk-cleanup")
        assert r.status_code == 200, (
            f"SY-3: GET /api/disk-cleanup (no action) should return the "
            f"read-only disk-usage view — got {r.status_code}."
        )
