"""PO ↔ Google Drive Audit (one-shot diagnostic).

Routes:
    GET /api/admin/po-drive-audit — JSON report

For each non-test orders row with a non-empty po_number, locates the
expected Drive folder (`{year}/{quarter}/PO-{po_number}/`) and the
RFQ subfolder where the original buyer email PDF should land. Reports
by category:
  - has_folder + has_rfq_files (verifiable)
  - has_folder + no_rfq_files  (folder created but PDF never landed)
  - no_folder                  (trigger never fired or folder lost)

Read-only. No writes, no folder creation. Auth-required.

Existed because (2026-04-28) heuristic backfills were correcting
prod rows by pattern. Mike's preference: validate against the
actual stored docs in Drive, not by regex. This endpoint is the
first step — figure out HOW MANY rows we can verify against Drive
before deciding whether to build a continuous reconciler or do a
one-off cleanup pass.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


def _quarter_for(iso_dt: str) -> str:
    """Convert an ISO timestamp to `Q1`/`Q2`/`Q3`/`Q4`. Robust to
    None/empty/garbage — returns `Q?` so the caller can still render
    something rather than blowing up the audit."""
    if not iso_dt:
        return "Q?"
    try:
        m = int(iso_dt[5:7])
        return f"Q{((m - 1) // 3) + 1}"
    except (ValueError, IndexError):
        return "Q?"


def _year_for(iso_dt: str) -> str:
    if not iso_dt or len(iso_dt) < 4:
        return ""
    return iso_dt[:4]


def _audit_one_order(row, find_po_folder, list_files) -> dict:
    """Look up one order's Drive presence. Pure function — both Drive
    helpers are injected so tests can stub the API."""
    po = (row["po_number"] or "").strip()
    created = row["created_at"] or row["updated_at"] or ""
    year = _year_for(created)
    quarter = _quarter_for(created)

    result = {
        "order_id": row["id"],
        "quote_number": row["quote_number"] or "",
        "po_number": po,
        "year": year,
        "quarter": quarter,
        "agency": row["agency"] or "",
        "institution": row["institution"] or "",
        "expected_folder": f"{year}/{quarter}/PO-{po}",
        "folder_id": None,
        "folder_exists": False,
        "rfq_files": [],     # [{name, mimeType, size}]
        "rfq_pdf_count": 0,
        "category": "no_folder",
    }

    if not (po and year and quarter and quarter != "Q?"):
        result["category"] = "incomplete_data"
        return result

    try:
        po_folder_id = find_po_folder(year, quarter, po)
    except Exception as e:
        result["error"] = f"find_folder failed: {e}"
        result["category"] = "drive_error"
        return result

    if not po_folder_id:
        return result   # category stays "no_folder"

    result["folder_id"] = po_folder_id
    result["folder_exists"] = True

    # Look for the RFQ subfolder. Don't fail the audit if it's missing
    # — the structure was supposed to create it, but maybe an earlier
    # version of the trigger didn't.
    try:
        from src.core.gdrive import find_folder
        rfq_id = find_folder("RFQ", po_folder_id)
    except Exception as e:
        result["error"] = f"find RFQ failed: {e}"
        result["category"] = "drive_error"
        return result

    if not rfq_id:
        result["category"] = "has_folder_no_rfq_subfolder"
        return result

    try:
        files = list_files(rfq_id)
    except Exception as e:
        result["error"] = f"list_files failed: {e}"
        result["category"] = "drive_error"
        return result

    pdfs = [f for f in files
            if (f.get("mimeType") == "application/pdf"
                or (f.get("name") or "").lower().endswith(".pdf"))]
    result["rfq_files"] = [
        {"name": f.get("name", ""),
         "mimeType": f.get("mimeType", ""),
         "size": int(f.get("size") or 0)}
        for f in files
    ]
    result["rfq_pdf_count"] = len(pdfs)
    result["category"] = (
        "has_folder_has_pdf" if pdfs
        else "has_folder_no_pdf"
    )
    return result


def _build_audit(limit: int = 50, only_unidentified: bool = False) -> dict:
    """Run the audit over up to `limit` orders. If `only_unidentified=1`,
    restrict to po_numbers that don't match any canonical agency
    prefix (per PR #635's classifier) — those are the ones we most
    want to verify against the actual stored PDFs."""
    from src.core.db import get_db
    from src.core.gdrive import is_configured, find_po_folder, list_files
    from src.api.modules.routes_health import _classify_po_by_prefix

    summary = {
        "drive_configured": is_configured(),
        "limit": limit,
        "only_unidentified": only_unidentified,
        "categories": {
            "has_folder_has_pdf": 0,
            "has_folder_no_pdf": 0,
            "has_folder_no_rfq_subfolder": 0,
            "no_folder": 0,
            "incomplete_data": 0,
            "drive_error": 0,
        },
        "rows": [],
    }

    if not summary["drive_configured"]:
        summary["error"] = "Drive not configured (GOOGLE_DRIVE_CREDENTIALS or GOOGLE_DRIVE_ROOT_FOLDER_ID missing)"
        return summary

    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, quote_number, po_number, agency, institution,
                   created_at, updated_at
            FROM orders
            WHERE COALESCE(is_test, 0) = 0
              AND po_number IS NOT NULL AND po_number != ''
            ORDER BY COALESCE(updated_at, created_at) DESC
        """).fetchall()

    # Optional filter to unidentified-prefix POs.
    if only_unidentified:
        rows = [r for r in rows if not _classify_po_by_prefix(r["po_number"])]

    rows = rows[:limit]

    for row in rows:
        try:
            result = _audit_one_order(row, find_po_folder, list_files)
        except Exception as e:
            log.warning("audit_one_order failed for %s: %s", row["id"], e)
            result = {
                "order_id": row["id"],
                "po_number": row["po_number"],
                "category": "drive_error",
                "error": str(e),
            }
        cat = result.get("category", "drive_error")
        summary["categories"][cat] = summary["categories"].get(cat, 0) + 1
        summary["rows"].append(result)

    return summary


@bp.route("/api/admin/po-drive-audit")
@auth_required
def po_drive_audit_json():
    """JSON audit report. Query params:
      - limit: max orders to check (default 50, max 500 to bound API calls)
      - only_unidentified: if "1", restrict to mismatched-prefix orders
    """
    try:
        limit = max(1, min(500, int(request.args.get("limit", "50"))))
    except (TypeError, ValueError):
        limit = 50
    only_unid = request.args.get("only_unidentified", "0") == "1"
    return jsonify(_build_audit(limit=limit, only_unidentified=only_unid))
