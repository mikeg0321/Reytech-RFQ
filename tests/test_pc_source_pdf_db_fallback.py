"""PR #8 — `/pricecheck/<pcid>/source-pdf` falls back to rfq_files DB.

Closes Handoff A PR-5 (Mike's auto_rfq_11fc_1779308659 incident on
2026-05-25): operator hit "Source PDF not found on disk" because the
PC's `source_pdf` disk path 404s after Railway redeploys wipe
non-persistent paths. The system DOES persist the bytes to the
`rfq_files` DB table at ingest (`dashboard.py:2628` +2772 +2855 +2909
all call `save_rfq_file(..., category="source", ...)` for inbound
buyer PDFs), but the fallback in the route silently queried
`category="template"` — which is the category used for SAVED RESPONSE
templates, NOT inbound buyer PDFs. Result: the DB-fallback path
matched 0 rows for every PC and the operator saw the bare 404.

The fix in `routes_pricecheck.py:3688-pricecheck_source_pdf` queries
categories in priority: "source" (canonical for buyer inbound) →
"template" (legacy stragglers) → no filter (last-resort catch-all).

These tests pin the contract that:
  1. A PC with a missing disk file but a `category="source"` blob in
     rfq_files returns 200 + the blob bytes (the substrate fix).
  2. Legacy PCs with `category="template"` blobs still return 200 via
     the second-tier fallback (back-compat).
  3. PCs with NO disk file AND NO DB blob still 404 cleanly (the
     bare-404 case is preserved — it's the "actually broken" signal).
"""
from __future__ import annotations

import os


def _seed_pc_with_missing_disk_path(temp_data_dir, sample_pc, pcid="pc-srcfix-001"):
    """Stamp the PC with a nonexistent source_pdf path. Whatever the
    fallback returns must come from rfq_files, NOT disk."""
    import json
    pc = dict(sample_pc)
    pc["id"] = pcid
    pc["source_pdf"] = "/data/uploads/nonexistent/missing.pdf"
    pcs = {pcid: pc}
    with open(os.path.join(temp_data_dir, "price_checks.json"), "w") as f:
        json.dump(pcs, f)
    return pcid


def _seed_rfq_files_blob(pcid: str, category: str, content: bytes):
    """Persist a fake PDF blob into rfq_files at the given category.

    Re-initializes the rfq_files table because the module-level init
    in dashboard.py runs once at import time against the pre-fixture
    DB; per-test temp DBs lack the table without this re-run.
    """
    from src.api.dashboard import save_rfq_file, _init_rfq_files_table
    _init_rfq_files_table()
    save_rfq_file(pcid, "source.pdf", "application/pdf",
                  content, category=category, uploaded_by="test_seeder")


def test_source_pdf_falls_back_to_db_blob_category_source(
    client, temp_data_dir, sample_pc
):
    """Mike's canonical case: PC's disk path is gone (post-deploy),
    but the buyer PDF bytes are in rfq_files at category='source'.
    Route must return 200 with the PDF bytes."""
    pcid = _seed_pc_with_missing_disk_path(temp_data_dir, sample_pc, "pc-srcfix-source")
    _seed_rfq_files_blob(pcid, "source", b"%PDF-1.4\n%TEST-SOURCE\n")
    r = client.get(f"/pricecheck/{pcid}/source-pdf")
    assert r.status_code == 200, (
        f"Expected 200 from DB fallback, got {r.status_code}. "
        f"Body: {r.data[:200]!r}. The rfq_files row exists at "
        f"category='source' but the route returned 404 — likely a "
        f"regression of the category-mismatch fix (Handoff A PR-5)."
    )
    assert r.mimetype == "application/pdf"
    assert r.data == b"%PDF-1.4\n%TEST-SOURCE\n"


def test_source_pdf_falls_back_to_db_blob_category_template_legacy(
    client, temp_data_dir, sample_pc
):
    """Back-compat: PCs ingested before the category convention
    normalized — if a record has only a category='template' row in
    rfq_files, the multi-tier fallback should still find it."""
    pcid = _seed_pc_with_missing_disk_path(temp_data_dir, sample_pc, "pc-srcfix-tmpl")
    _seed_rfq_files_blob(pcid, "template", b"%PDF-1.4\n%TEST-LEGACY\n")
    r = client.get(f"/pricecheck/{pcid}/source-pdf")
    assert r.status_code == 200, (
        f"Legacy category='template' row not found by fallback — "
        f"second-tier path broken. Got {r.status_code}."
    )
    assert r.data == b"%PDF-1.4\n%TEST-LEGACY\n"


def test_source_pdf_truly_missing_returns_404(client, temp_data_dir, sample_pc):
    """No disk file AND no DB blob → bare 404. This is the 'actually
    broken, please re-ingest' signal; the operator should see it
    clearly rather than have it masked by a misleading 200."""
    pcid = _seed_pc_with_missing_disk_path(temp_data_dir, sample_pc, "pc-srcfix-none")
    # NO rfq_files row seeded.
    r = client.get(f"/pricecheck/{pcid}/source-pdf")
    assert r.status_code == 404
    assert b"Source PDF not found" in r.data
