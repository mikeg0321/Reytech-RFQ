"""Bug 3 — fold Templates & Files + QB Export into a single Tools details.

Mike's bug report (2026-05-02 image #4): the standalone QB Export card
above the email composer "is just taking up space". Both Templates &
Files and QB Export are utility surfaces an operator only touches once
per RFQ — wrapping them in the same collapsible "Tools" details halves
the always-visible footprint.

This file locks:
  1. The Tools `<details>` section is in the page (id rfq-tools-card,
     summary testid rfq-tools-summary).
  2. The QB Export block lives INSIDE the Tools details (when output
     files are present) — testid rfq-qb-export-block.
  3. The old standalone "<div class='card'>📊 QB Export</div>" wrapper
     is gone.
  4. Templates & Files functionality (upload form etc.) is preserved
     inside Tools.
"""
from __future__ import annotations


def _seed(rid="rfq_tools", **overrides):
    from src.api.data_layer import _save_single_rfq
    base = {
        "id": rid, "status": "generated",
        "rfq_number": "TOOLS-1",
        "solicitation_number": "TOOLS-1",
        "institution": "CCHCS",
        "line_items": [{"description": "X", "qty": 1, "price_per_unit": 10.0}],
    }
    base.update(overrides)
    _save_single_rfq(rid, base)
    return rid


def test_tools_details_renders(auth_client, temp_data_dir):
    rid = _seed()
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'id="rfq-tools-card"' in html
    assert 'data-testid="rfq-tools-summary"' in html
    # Summary text — keeps "Tools" as the primary label.
    assert "🛠 Tools" in html or "🛠</span> Tools" in html


def test_qb_export_block_inside_tools_when_files_present(
        auth_client, temp_data_dir):
    """When the package is generated, QB Export shows up — inside Tools,
    not as a standalone card."""
    rid = _seed(output_files=["Quote.pdf", "BidPackage.pdf"])
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-qb-export-block"' in html
    # The block must be physically inside the details element.
    tools_open = html.find('id="rfq-tools-card"')
    tools_close = html.find('</details>', tools_open)
    qb_pos = html.find('data-testid="rfq-qb-export-block"')
    assert tools_open != -1 and tools_close != -1 and qb_pos != -1
    assert tools_open < qb_pos < tools_close, (
        f"QB block at {qb_pos} not inside Tools details ({tools_open}-{tools_close})"
    )


def test_qb_export_block_hidden_when_no_output_files(
        auth_client, temp_data_dir):
    """No generated package yet → no QB block (export targets don't exist)."""
    rid = _seed(rid="rfq_tools_empty", status="parsed", output_files=[])
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-qb-export-block"' not in html


def test_old_standalone_qb_export_card_removed(auth_client, temp_data_dir):
    """The old `<div class='card'>📊 QB Export</div>` above the email
    composer must be gone — its content moved into Tools."""
    rid = _seed(output_files=["Quote.pdf"])
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    # Sentinel: the old container had `padding:12px 16px` outside any details.
    # We assert the new block is the ONLY 📊 QB Export occurrence.
    occurrences = html.count("📊 QB Export")
    assert occurrences == 1, f"Expected 1 QB Export header, found {occurrences}"


def test_templates_and_files_upload_form_preserved(
        auth_client, temp_data_dir):
    """Functional regression: the Templates & Files upload form must
    survive the rename — operators still need to upload 703B/704B PDFs."""
    rid = _seed(rid="rfq_upload_form", status="parsed",
                templates={})
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-upload-templates"' in html
    assert 'data-testid="rfq-upload-templates-btn"' in html
