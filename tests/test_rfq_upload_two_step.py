"""Bundle-2 PR-2b: two-step ingest UI on /rfq/upload-manual.

Audit item B: first click of Preview runs `/api/rfq/upload-preview`
(PR-2a), shows editable preview card; second click on Confirm fires
the existing `/api/rfq/upload-manual` to create the record. Below-
threshold confidence surfaces a yellow review banner.

These tests pin the structural bits so a future "tidy the upload
page" pass can't silently revert to the single-step flow.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def upload_page_html(client):
    resp = client.get("/rfq/upload-manual")
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


class TestTwoStepStructure:
    def test_preview_button_replaces_single_ingest(self, upload_page_html):
        """Old 'Ingest RFQ' button on `#submitBtn` is gone; new
        '🔍 Preview detection' lives on `[data-testid=upload-preview-btn]`."""
        assert 'data-testid="upload-preview-btn"' in upload_page_html
        assert 'Preview detection' in upload_page_html
        # Old single-step button removed (id="submitBtn" referenced the
        # ingest-in-one-click button — it must not exist anymore or the
        # operator could accidentally bypass preview).
        assert 'id="submitBtn"' not in upload_page_html

    def test_preview_card_present_hidden_by_default(self, upload_page_html):
        """Card exists in DOM but hidden until a preview response
        populates it. Starts with display:none so the page still looks
        like the familiar single-form on first load."""
        assert 'data-testid="rfq-upload-preview-card"' in upload_page_html
        # Hidden by default — the card's inline style must include
        # display:none for the initial render.
        anchor = upload_page_html.index('data-testid="rfq-upload-preview-card"')
        chunk = upload_page_html[anchor:anchor + 400]
        assert 'display:none' in chunk, (
            "preview card must start hidden; shown after preview response"
        )

    def test_preview_card_exposes_all_detected_fields(self, upload_page_html):
        """Every field the preview endpoint returns must have a slot
        in the card — otherwise the operator can't catch a bad
        detection before confirm."""
        for tid in (
            "preview-shape", "preview-agency", "preview-sol",
            "preview-facility", "preview-deadline",
            "preview-required-forms", "preview-items-count",
        ):
            assert f'data-testid="{tid}"' in upload_page_html, (
                f"preview field slot {tid!r} missing"
            )

    def test_low_confidence_banner_present(self, upload_page_html):
        """Below-threshold confidence must surface a yellow banner —
        the single biggest source of ingest defects is a mis-classified
        shape, so 'look here before confirm' has to be loud."""
        assert 'data-testid="preview-low-confidence-banner"' in upload_page_html
        assert 'confidence below threshold' in upload_page_html.lower()
        # Wired up to a threshold constant so the default is greppable
        assert 'PREVIEW_CONFIDENCE_THRESHOLD = 0.70' in upload_page_html, (
            "confidence threshold must be defined as a named constant, "
            "not a magic number, so tuning it later is a one-line change"
        )

    def test_back_and_confirm_buttons_present(self, upload_page_html):
        assert 'data-testid="preview-back-btn"' in upload_page_html
        assert 'data-testid="preview-confirm-btn"' in upload_page_html
        # Back triggers backToEdit() — hides preview, returns to dropzone
        assert 'onclick="backToEdit()"' in upload_page_html
        # Confirm triggers confirmIngest() — fires /api/rfq/upload-manual
        assert 'onclick="confirmIngest()"' in upload_page_html


class TestJsWiring:
    def test_preview_posts_to_preview_endpoint(self, upload_page_html):
        """The preview button must hit `/api/rfq/upload-preview` (PR-2a),
        NOT the legacy `/api/rfq/upload-manual` which writes records."""
        # Find the submitPreview function body
        anchor = upload_page_html.index('function submitPreview(')
        chunk = upload_page_html[anchor:anchor + 1200]
        assert "fetch('/api/rfq/upload-preview'" in chunk, (
            "submitPreview() must call the preview endpoint, not manual ingest"
        )
        # And must NOT accidentally call upload-manual inside preview
        assert "'/api/rfq/upload-manual'" not in chunk

    def test_confirm_posts_to_upload_manual(self, upload_page_html):
        """Confirm is the ONLY step that writes. It targets the
        existing `/api/rfq/upload-manual` with the same FormData we
        built for preview — no divergence between what's previewed
        and what's saved."""
        anchor = upload_page_html.index('function confirmIngest(')
        chunk = upload_page_html[anchor:anchor + 2000]
        assert "fetch('/api/rfq/upload-manual'" in chunk

    def test_shared_formdata_helper(self, upload_page_html):
        """A `_uploadFormData()` helper builds the POST body for
        BOTH steps so the files/subject/sender/body sent to preview
        are bit-for-bit identical to the files/subject/sender/body
        sent to confirm. Regression guard: if someone splits these
        into two separate readers, preview and confirm can drift."""
        assert 'function _uploadFormData(' in upload_page_html
        # Both steps call it
        assert upload_page_html.count('_uploadFormData()') >= 2
