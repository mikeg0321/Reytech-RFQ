"""Bundle-2 PR-2e: per-RFQ dismiss required-form slot.

Source: audit item L (2026-04-22). Classifier picks a `forms_required`
list on ingest; when it mis-shapes an RFQ the operator needs a scoped
escape hatch — "for THIS RFQ only, stop demanding BidPkg." This PR
adds the UI toggle + persistence + rendering, gated to forms the
classifier actually picked.

Two groups:
  1. **Route behavior** — `/api/rfq/<rid>/toggle-required-form`
     validates inputs, persists `dismissed_required_forms`,
     refuses slugs the classifier never picked, handles restore.
  2. **Template rendering** — dismissed pills render with
     strikethrough + restore icon; non-dismissed pills render with
     dismiss icon.
"""
from __future__ import annotations

import json
import os

import pytest


def _seed(temp_data_dir, sample_rfq, **overrides):
    rfq = dict(sample_rfq)
    rfq.update(overrides)
    # `requirements_json` lives on the record as a JSON-serialized
    # string (the route does `json.loads(r['requirements_json'])`
    # before passing the parsed dict to the template). Tests that
    # pass it as a raw dict would silently land at `_requirements
    # = {}` and the dismiss UI would never render. Normalize here.
    if isinstance(rfq.get("requirements_json"), dict):
        rfq["requirements_json"] = json.dumps(rfq["requirements_json"])
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


class TestToggleRoute:
    def test_dismiss_adds_to_list(self, client, temp_data_dir, sample_rfq):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b", "704b", "bidpkg"]},
        )
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "bidpkg", "dismiss": True},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True
        assert "bidpkg" in payload["dismissed_required_forms"]

    def test_restore_removes_from_list(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b", "bidpkg"]},
            dismissed_required_forms=["bidpkg"],
        )
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "bidpkg", "dismiss": False},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True
        assert "bidpkg" not in payload["dismissed_required_forms"]

    def test_dismiss_is_idempotent(self, client, temp_data_dir, sample_rfq):
        """Re-dismissing an already-dismissed form must not duplicate
        it in the list — the stored shape is a set, serialized as a
        list."""
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["bidpkg"]},
            dismissed_required_forms=["bidpkg"],
        )
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "bidpkg", "dismiss": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["dismissed_required_forms"].count("bidpkg") == 1

    def test_refuses_form_not_in_classifier_list(
        self, client, temp_data_dir, sample_rfq
    ):
        """Guard against spurious slugs: can only dismiss what the
        classifier actually picked. Keeps the dismiss list
        meaningful AND prevents callers from seeding arbitrary
        values onto the record."""
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b"]},
        )
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "obs1600", "dismiss": True},
        )
        assert resp.status_code == 400
        assert "not in this RFQ's forms_required" in resp.get_json()["error"]

    def test_empty_form_slug_400(self, client, temp_data_dir, sample_rfq):
        rid = _seed(temp_data_dir, sample_rfq)
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "", "dismiss": True},
        )
        assert resp.status_code == 400

    def test_missing_rfq_404(self, client):
        resp = client.post(
            "/api/rfq/does-not-exist/toggle-required-form",
            json={"form": "703b", "dismiss": True},
        )
        assert resp.status_code == 404

    def test_case_insensitive_form_slug(
        self, client, temp_data_dir, sample_rfq
    ):
        """Operator-friendly: BidPkg / BIDPKG / bidpkg all match."""
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["bidpkg"]},
        )
        resp = client.post(
            f"/api/rfq/{rid}/toggle-required-form",
            json={"form": "BidPkg", "dismiss": True},
        )
        assert resp.status_code == 200
        assert "bidpkg" in resp.get_json()["dismissed_required_forms"]


class TestPillRendering:
    def test_dismissed_pill_has_strikethrough_and_restore(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b", "bidpkg"]},
            dismissed_required_forms=["bidpkg"],
        )
        resp = client.get(f"/rfq/{rid}")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        # The pill for bidpkg carries data-dismissed="1"
        assert 'data-form-slug="bidpkg"' in html
        dismissed_anchor = html.index('data-form-slug="bidpkg"')
        dismissed_chunk = html[dismissed_anchor:dismissed_anchor + 600]
        assert 'data-dismissed="1"' in dismissed_chunk
        assert "line-through" in dismissed_chunk
        # Restore button present on dismissed pills
        assert 'data-testid="rfq-required-form-restore"' in dismissed_chunk

    def test_active_pill_has_dismiss_button(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b", "bidpkg"]},
            dismissed_required_forms=[],
        )
        resp = client.get(f"/rfq/{rid}")
        html = resp.get_data(as_text=True)
        active_anchor = html.index('data-form-slug="703b"')
        chunk = html[active_anchor:active_anchor + 600]
        assert 'data-dismissed="0"' in chunk
        # Dismiss button present on active pills
        assert 'data-testid="rfq-required-form-dismiss"' in chunk
        # No strikethrough
        assert "line-through" not in chunk

    def test_strip_testid_present(self, client, temp_data_dir, sample_rfq):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b"]},
        )
        resp = client.get(f"/rfq/{rid}")
        html = resp.get_data(as_text=True)
        assert 'data-testid="rfq-required-forms-strip"' in html

    def test_toggle_function_defined(self, client, temp_data_dir, sample_rfq):
        rid = _seed(
            temp_data_dir, sample_rfq,
            requirements_json={"forms_required": ["703b"]},
        )
        resp = client.get(f"/rfq/{rid}")
        html = resp.get_data(as_text=True)
        # The helper is defined idempotently on window.*
        assert "window._toggleRequiredForm" in html
        assert "/toggle-required-form" in html
