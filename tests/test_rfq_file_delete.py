"""PR-AE 2026-05-13 — per-file delete endpoint + chip × button (Bug 1).

The 2026-05-13 walkthrough of PC #10846357 surfaced: operator uploaded
the wrong bid-package PDF as a Source Template and had NO way to
delete it from the UI. Two near-identical bid-package files now ride
in the queue and the operator had to ask Claude to clean up DB-side.

Fix: `DELETE /api/rfq/<rid>/file/<file_id>` in routes_rfq_admin.py
with three safety rails:
  1. file must belong to the given rfq_id (defense against ID swap)
  2. category must be 'template' OR 'attachment' (never 'generated')
  3. file_id must match the rf_<hex> shape minted by save_rfq_file

UI: × button on each Source-Template + Other-Attachments chip in
rfq_detail.html (renderSection now takes `canDelete` flag). Generated
chips have no × — they regen via the existing Generate path.

Pinned guarantees:
  1. Endpoint deletes the row when authorized.
  2. Endpoint refuses 'generated' category (403).
  3. Endpoint refuses cross-rfq id swap (404).
  4. Endpoint refuses malformed file_id (400).
  5. renderSection emits × on template + attachment, NOT on generated.
"""
from __future__ import annotations

import os
import sys
import sqlite3
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_file(db_path, rfq_id, filename, category):
    """Insert a row into rfq_files. Returns the file_id. Lazy-creates
    the table if init_db didn't (test isolation case)."""
    file_id = f"rf_{uuid.uuid4().hex[:10]}"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rfq_files (
                id          TEXT PRIMARY KEY,
                rfq_id      TEXT NOT NULL,
                filename    TEXT NOT NULL,
                file_type   TEXT NOT NULL,
                category    TEXT DEFAULT 'template',
                mime_type   TEXT DEFAULT 'application/pdf',
                file_size   INTEGER DEFAULT 0,
                data        BLOB,
                uploaded_by TEXT DEFAULT 'system',
                created_at  TEXT NOT NULL
            )
        """)
        conn.execute(
            "INSERT INTO rfq_files "
            "(id, rfq_id, filename, file_type, file_size, data, category, "
            "uploaded_by, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (file_id, rfq_id, filename, "application/pdf", 100,
             b"%PDF-1.4 stub", category, "test", "2026-05-13T00:00:00"),
        )
        conn.commit()
    finally:
        conn.close()
    return file_id


def _seed_rfq(db_path, rfq_id):
    """No-op: the DELETE route uses _validate_rid (regex only) and
    operates directly on rfq_files. No rfqs row needed."""
    return


# ── DELETE endpoint ────────────────────────────────────────────────


def test_delete_template_succeeds(client, temp_data_dir):
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    rid = "rfq_pr_ae_test1"
    _seed_rfq(db_path, rid)
    fid = _seed_file(db_path, rid, "wrong_bid_pkg.pdf", "template")
    resp = client.delete(f"/api/rfq/{rid}/file/{fid}")
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["ok"] is True
    assert body["deleted"] == 1
    assert body["filename"] == "wrong_bid_pkg.pdf"
    # Confirm row actually gone
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT id FROM rfq_files WHERE id=?",
                           (fid,)).fetchone()
        assert row is None, "row should be deleted"
    finally:
        conn.close()


def test_delete_refuses_generated_category(client, temp_data_dir):
    """Generated outputs must regen via Generate path, not delete API."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    rid = "rfq_pr_ae_test2"
    _seed_rfq(db_path, rid)
    fid = _seed_file(db_path, rid, "generated_704.pdf", "generated")
    resp = client.delete(f"/api/rfq/{rid}/file/{fid}")
    assert resp.status_code == 403
    body = resp.get_json()
    assert body["ok"] is False
    assert "generated" in body["error"].lower()
    # Confirm row STILL present
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT id FROM rfq_files WHERE id=?",
                           (fid,)).fetchone()
        assert row is not None, "generated row must NOT be deleted"
    finally:
        conn.close()


def test_delete_refuses_cross_rfq_id_swap(client, temp_data_dir):
    """File from rfq A can't be deleted via rfq B's URL."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    rid_a = "rfq_pr_ae_a"
    rid_b = "rfq_pr_ae_b"
    _seed_rfq(db_path, rid_a)
    _seed_rfq(db_path, rid_b)
    fid_a = _seed_file(db_path, rid_a, "for_a.pdf", "template")
    resp = client.delete(f"/api/rfq/{rid_b}/file/{fid_a}")
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["ok"] is False
    assert "not found" in body["error"].lower()


def test_delete_refuses_malformed_file_id(client, temp_data_dir):
    from src.core.migrations import run_migrations
    run_migrations()
    rid = "rfq_pr_ae_malformed"
    _seed_rfq(os.path.join(temp_data_dir, "reytech.db"), rid)
    resp = client.delete(f"/api/rfq/{rid}/file/not-a-real-id")
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["ok"] is False
    assert "invalid" in body["error"].lower()


def test_delete_attachment_category_also_allowed(client, temp_data_dir):
    """Operator-uploaded reference attachments (PDFs, etc.) are also
    operator-correctable — should accept delete."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    rid = "rfq_pr_ae_attach"
    _seed_rfq(db_path, rid)
    fid = _seed_file(db_path, rid, "ref_doc.pdf", "attachment")
    resp = client.delete(f"/api/rfq/{rid}/file/{fid}")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["deleted"] == 1


# ── renderSection chip × markup ────────────────────────────────────


def test_render_section_emits_delete_button_for_template():
    """Source Template + Other Attachments chips must render an × link.
    Generated Files chips must NOT render an × — those regen via the
    Generate path, not delete API."""
    import os
    full = os.path.join(os.path.dirname(__file__), "..",
                         "src/templates/rfq_detail.html")
    with open(full, encoding="utf-8") as f:
        source = f.read()
    # renderSection accepts the canDelete flag
    assert "function renderSection(label,icon,files,canDelete)" in source
    # × is gated on the flag
    assert "if(canDelete){" in source
    assert "class=\"rfq-file-delete\"" in source
    # Template + attachment chips canDelete=true; generated=false
    assert "renderSection('Source Templates','📋',cats.template,true)" in source
    assert "renderSection('Generated Files','📦',cats.generated,false)" in source
    assert "renderSection('Other Attachments','📎',cats.attachment,true)" in source
    # Click handler wires to DELETE endpoint with confirm + reload
    assert "'/api/rfq/'+RID+'/file/'+fid" in source
    assert "method:'DELETE'" in source
    assert "confirm(" in source
