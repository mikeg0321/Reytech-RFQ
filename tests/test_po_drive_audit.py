"""Tests for the PO ↔ Drive audit endpoint (PR #638).

Validates the categorization logic without hitting the real Drive
API. The Drive helpers are injected into _audit_one_order so the
test can stub them.

The audit's job is to bucket every prod order with a po_number
into one of:
  - has_folder_has_pdf       (verifiable against the original PDF)
  - has_folder_no_pdf         (folder exists, RFQ subfolder empty)
  - has_folder_no_rfq_subfolder (folder created without subfolders)
  - no_folder                (trigger never fired or lost)
  - incomplete_data          (created_at missing — can't compute year)
  - drive_error              (API failure)

The bucket distribution post-deploy tells the operator whether to
build a continuous reconciler or run a one-off cleanup.
"""
from __future__ import annotations

from datetime import datetime

import pytest


def _row(**overrides):
    """Build a sqlite3.Row-like dict for _audit_one_order."""
    base = {
        "id": "ord-1",
        "quote_number": "Q1",
        "po_number": "8955-0000044935",
        "agency": "CalVet",
        "institution": "Veterans Home of California - Barstow",
        "created_at": "2025-09-12T10:00:00",
        "updated_at": "2025-09-13T11:00:00",
    }
    base.update(overrides)
    return base


def _audit(row, find_po_folder=None, list_files=None,
           find_folder=None):
    """Wrap _audit_one_order with stubs. find_folder is patched at
    the import site since _audit_one_order calls it for the RFQ
    subfolder."""
    from src.api.modules import routes_po_drive_audit as _m
    from src.core import gdrive as _g

    fpf = find_po_folder or (lambda y, q, p: None)
    lf = list_files or (lambda fid: [])
    ff = find_folder or (lambda name, parent: None)

    # _audit_one_order imports find_folder lazily inside the function;
    # patching the gdrive module attribute is enough.
    _orig_find = getattr(_g, "find_folder", None)
    _g.find_folder = ff
    try:
        return _m._audit_one_order(row, fpf, lf)
    finally:
        if _orig_find is not None:
            _g.find_folder = _orig_find


# ── Quarter / year helpers ──────────────────────────────────────────────


@pytest.mark.parametrize("iso,expected", [
    ("2025-01-15T00:00:00", "Q1"),
    ("2025-03-31T23:59:59", "Q1"),
    ("2025-04-01T00:00:00", "Q2"),
    ("2025-06-30T00:00:00", "Q2"),
    ("2025-07-15T00:00:00", "Q3"),
    ("2025-09-12T10:00:00", "Q3"),
    ("2025-10-01T00:00:00", "Q4"),
    ("2025-12-31T00:00:00", "Q4"),
    ("",                    "Q?"),
    (None,                  "Q?"),
    ("garbage",             "Q?"),
])
def test_quarter_for(iso, expected):
    from src.api.modules.routes_po_drive_audit import _quarter_for
    assert _quarter_for(iso) == expected


@pytest.mark.parametrize("iso,expected", [
    ("2025-09-12T10:00:00", "2025"),
    ("2024-01-01",          "2024"),
    ("",                    ""),
    (None,                  ""),
    ("xyz",                 ""),     # too short — len < 4 → "" (no year inferred)
    ("2024xyz",             "2024"), # first 4 chars when long enough
])
def test_year_for(iso, expected):
    from src.api.modules.routes_po_drive_audit import _year_for
    assert _year_for(iso) == expected


# ── _audit_one_order categorization ─────────────────────────────────────


def test_no_folder_when_find_returns_none():
    """No PO folder in Drive → category = no_folder."""
    out = _audit(_row(), find_po_folder=lambda y, q, p: None)
    assert out["category"] == "no_folder"
    assert out["folder_exists"] is False
    assert out["folder_id"] is None


def test_has_folder_has_pdf_when_rfq_has_pdf():
    out = _audit(
        _row(),
        find_po_folder=lambda y, q, p: "po-folder-id",
        find_folder=lambda name, parent: "rfq-id" if name == "RFQ" else None,
        list_files=lambda fid: [
            {"name": "PO_4500737702.pdf", "mimeType": "application/pdf",
             "size": 50000},
        ],
    )
    assert out["category"] == "has_folder_has_pdf"
    assert out["folder_exists"] is True
    assert out["folder_id"] == "po-folder-id"
    assert out["rfq_pdf_count"] == 1
    assert len(out["rfq_files"]) == 1


def test_has_folder_no_pdf_when_rfq_only_has_non_pdf():
    out = _audit(
        _row(),
        find_po_folder=lambda y, q, p: "po-folder-id",
        find_folder=lambda name, parent: "rfq-id",
        list_files=lambda fid: [
            {"name": "notes.txt", "mimeType": "text/plain", "size": 100},
        ],
    )
    assert out["category"] == "has_folder_no_pdf"
    assert out["rfq_pdf_count"] == 0
    assert len(out["rfq_files"]) == 1


def test_has_folder_no_pdf_when_rfq_empty():
    out = _audit(
        _row(),
        find_po_folder=lambda y, q, p: "po-folder-id",
        find_folder=lambda name, parent: "rfq-id",
        list_files=lambda fid: [],
    )
    assert out["category"] == "has_folder_no_pdf"
    assert out["rfq_pdf_count"] == 0


def test_has_folder_no_rfq_subfolder_when_subfolder_missing():
    """Older PO folders may exist without the standard RFQ
    subfolder. Don't crash the audit — categorize separately."""
    out = _audit(
        _row(),
        find_po_folder=lambda y, q, p: "po-folder-id",
        find_folder=lambda name, parent: None,    # RFQ subfolder absent
    )
    assert out["category"] == "has_folder_no_rfq_subfolder"
    assert out["folder_exists"] is True


def test_pdf_detected_by_extension_even_without_mimetype():
    """Some Drive files have empty mimeType — fall back to extension."""
    out = _audit(
        _row(),
        find_po_folder=lambda y, q, p: "po-folder-id",
        find_folder=lambda name, parent: "rfq-id",
        list_files=lambda fid: [
            {"name": "PO_4500737702.PDF", "mimeType": "", "size": 50000},
        ],
    )
    assert out["category"] == "has_folder_has_pdf"
    assert out["rfq_pdf_count"] == 1


def test_incomplete_data_when_created_at_missing():
    """Without created_at we can't compute year/quarter, so we
    can't even guess the folder path. Surface explicitly rather
    than misreporting as 'no_folder'."""
    out = _audit(_row(created_at="", updated_at=""))
    assert out["category"] == "incomplete_data"


def test_incomplete_data_when_po_number_blank_after_strip():
    """Whitespace-only po_number is no PO, period."""
    out = _audit(_row(po_number="   "))
    assert out["category"] == "incomplete_data"


def test_drive_error_caught_per_row():
    """A single row that raises on Drive lookup must not crash the
    whole audit — bucket as drive_error and continue."""
    def _boom(*a, **kw):
        raise RuntimeError("simulated Drive 500")
    out = _audit(_row(), find_po_folder=_boom)
    assert out["category"] == "drive_error"
    assert "simulated" in out["error"].lower()


# ── Expected folder path is rendered correctly ─────────────────────────


def test_expected_folder_uses_year_quarter_from_created_at():
    out = _audit(
        _row(created_at="2025-09-12T10:00:00",
             po_number="4500737702"),
        find_po_folder=lambda y, q, p: None,
    )
    assert out["year"] == "2025"
    assert out["quarter"] == "Q3"
    assert out["expected_folder"] == "2025/Q3/PO-4500737702"


# ── /api/admin/po-drive-audit endpoint integration ─────────────────────


def test_endpoint_returns_drive_not_configured_when_creds_missing(
        auth_client, monkeypatch):
    """If GOOGLE_DRIVE_CREDENTIALS isn't set in the environment,
    the audit must fail loud rather than silent. Operator running
    locally without creds should see why."""
    from src.core import gdrive as _g
    monkeypatch.setattr(_g, "is_configured", lambda: False)
    resp = auth_client.get("/api/admin/po-drive-audit")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["drive_configured"] is False
    assert "error" in data


def test_endpoint_caps_limit_at_500(auth_client, monkeypatch):
    """Limit > 500 gets clamped — bound API calls."""
    from src.core import gdrive as _g
    monkeypatch.setattr(_g, "is_configured", lambda: False)
    resp = auth_client.get("/api/admin/po-drive-audit?limit=99999")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["limit"] == 500


def test_endpoint_only_unidentified_flag_round_trips(auth_client, monkeypatch):
    from src.core import gdrive as _g
    monkeypatch.setattr(_g, "is_configured", lambda: False)
    resp = auth_client.get("/api/admin/po-drive-audit?only_unidentified=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["only_unidentified"] is True


# ── Endpoint with stubbed Drive (drive_configured=True path) ──────────


def test_endpoint_iterates_orders_and_categorizes(
        auth_client, monkeypatch):
    """End-to-end: seed a few orders, stub Drive helpers, hit the
    endpoint, verify categorization."""
    from src.core import gdrive as _g
    from src.core.db import get_db

    with get_db() as c:
        try:
            c.execute("DELETE FROM orders")
        except Exception:
            pass
        when = datetime(2025, 9, 12, 10).isoformat()
        for i, po in enumerate([
            "8955-0000044935", "0000067018", "RFQ882023",
        ]):
            c.execute("""
                INSERT INTO orders
                  (id, quote_number, po_number, agency, institution,
                   total, status, items, created_at, updated_at, is_test)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (f"o{i}", f"Q{i}", po, "CDCR", "Veterans Home",
                  100.0, "open", "[]", when, when, 0))
        c.commit()

    monkeypatch.setattr(_g, "is_configured", lambda: True)
    monkeypatch.setattr(
        _g, "find_po_folder",
        lambda y, q, p: "po-id" if p == "8955-0000044935" else None,
    )
    monkeypatch.setattr(_g, "find_folder",
                        lambda name, parent: "rfq-id")
    monkeypatch.setattr(
        _g, "list_files",
        lambda fid: [{"name": "po.pdf",
                       "mimeType": "application/pdf", "size": 1024}],
    )

    resp = auth_client.get("/api/admin/po-drive-audit?limit=10")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["drive_configured"] is True
    cats = data["categories"]
    # 1 order had a folder + RFQ + PDF
    assert cats["has_folder_has_pdf"] >= 1
    # 2 orders had no folder
    assert cats["no_folder"] >= 2
