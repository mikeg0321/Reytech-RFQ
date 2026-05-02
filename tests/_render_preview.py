"""Render the new review-package page to ~/rfq_preview_*.html for Mike to
visually verify in his browser. Run via:

    python -m pytest tests/_render_preview.py::test_dump_aligned -s
    python -m pytest tests/_render_preview.py::test_dump_blocked -s

Underscore prefix keeps it out of the default test collection.
"""
from __future__ import annotations

import json
import os


def _seed(rid, mid_args=None):
    from src.core.dal import create_package_manifest
    from src.core.db import get_db
    args = dict(
        rfq_id=rid, agency_key="cchcs", agency_name="CCHCS",
        required_forms=["703b", "704b", "bidpkg", "quote"],
        generated_forms=[
            {"form_id": "703b", "filename": "R26Q38_Reytech_703B.pdf"},
            {"form_id": "704b", "filename": "R26Q38_Reytech_704B.pdf"},
            {"form_id": "bidpkg", "filename": "R26Q38_Reytech_RFQPackage.pdf"},
            {"form_id": "quote", "filename": "R26Q38_Reytech Quote.pdf"},
        ],
        quote_number="R26Q38", quote_total=1094.50, item_count=2,
        created_by="preview",
    )
    if mid_args:
        args.update(mid_args)
    mid = create_package_manifest(**args)
    return mid


def _set_audit(mid, field_audit, source_validation):
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("UPDATE package_manifest SET field_audit=?, source_validation=? WHERE id=?",
            (json.dumps(field_audit), json.dumps(source_validation), mid))


def test_dump_aligned(auth_client, temp_data_dir):
    """All-green render — what Mike wants to see when ready to send."""
    rid = "rfq_preview_aligned"
    rfq = {
        "id": rid, "solicitation_number": "R26Q38",
        "requestor_name": "Keith Alsing",
        "requestor_email": "keith.alsing@calvet.ca.gov",
        "due_date": "2026-05-10",
        "department": "California Department of Veterans Affairs",
        "agency": "CalVet",
        "ship_to": "CalVet HQ, Sacramento, CA 95814",
        "status": "draft",
        "line_items": [
            {"line_number": "1", "qty": 50, "uom": "BX",
             "description": "Flushable Wipes — 200ct dispenser-ready",
             "part_number": "FW-200-BX", "price_per_unit": 12.99},
            {"line_number": "2", "qty": 5, "uom": "EA",
             "description": "Wall-Mount Dispenser, ABS plastic",
             "part_number": "DS-WM-01", "price_per_unit": 89.00},
        ],
    }
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
        json.dump({rid: rfq}, f)

    mid = _seed(rid)
    _set_audit(mid,
        field_audit={"_qa_passed": True,
                     "_qa_summary": {"forms_checked": 4, "duration_ms": 312,
                                     "critical_issues": []}},
        source_validation={"errors": [], "warnings": [],
                           "checks": ["buyer match", "sol# match"]})

    r = auth_client.get(f"/rfq/{rid}/review-package")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"

    out = os.path.expanduser("~/rfq_preview_aligned.html")
    with open(out, "wb") as f:
        f.write(r.data)
    print(f"\n>>> WROTE: {out} ({len(r.data)} bytes)")
    print(f">>> Open with: start \"\" \"{out}\"")


def test_dump_blocked(auth_client, temp_data_dir):
    """Red-state render — Mike sees what blocks send + per-issue list."""
    rid = "rfq_preview_blocked"
    rfq = {
        "id": rid, "solicitation_number": "R26Q39",
        "requestor_name": "Maria Lopez",
        "requestor_email": "maria.lopez@cchcs.ca.gov",
        "due_date": "",  # missing — soft warn
        "department": "California Correctional Health Care Services",
        "agency": "CCHCS",
        "status": "draft",
        "line_items": [
            {"line_number": "1", "qty": 10, "description": "Surgical gloves",
             "price_per_unit": 0},  # unpriced!
            {"line_number": "2", "qty": 5, "description": "Bandages 4x4",
             "price_per_unit": 8.50},
        ],
    }
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
        json.dump({rid: rfq}, f)

    mid = _seed(rid, mid_args={
        "generated_forms": [
            {"form_id": "703b", "filename": "R26Q39_Reytech_703B.pdf"},
            {"form_id": "704b", "filename": ""},  # MISSING
            {"form_id": "bidpkg", "filename": "R26Q39_Reytech_RFQPackage.pdf"},
            {"form_id": "quote", "filename": "R26Q39_Reytech Quote.pdf"},
        ],
    })
    _set_audit(mid,
        field_audit={"_qa_passed": False,
                     "_qa_summary": {"forms_checked": 4,
                                     "critical_issues": [
                                         "703B signature missing",
                                         "704B blank rows on page 2",
                                         "quote # not stamped on quote.pdf"]}},
        source_validation={"errors": ["buyer email not found in source thread"],
                           "warnings": [], "checks": []})

    r = auth_client.get(f"/rfq/{rid}/review-package")
    assert r.status_code == 200

    out = os.path.expanduser("~/rfq_preview_blocked.html")
    with open(out, "wb") as f:
        f.write(r.data)
    print(f"\n>>> WROTE: {out} ({len(r.data)} bytes)")
    print(f">>> Open with: start \"\" \"{out}\"")
