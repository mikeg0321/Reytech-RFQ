"""PR-P — generate-then-pause package validator tests.

Pinned guarantees:
  1. validate_pc_package returns a stable shape every time — caller's
     template doesn't need to handle two structures.
  2. When PC has no generated PDFs → skipped_reason populated, files=[],
     overall_passed=False, but ok=False (operator must see "run Generate
     first").
  3. When PC has generated PDFs → calls inspect_package, persists report
     on pc.vision_validation via _save_single_pc.
  4. Persist=False skips the save (admin preview path).
  5. inspect_package failure → graceful skip with reason in report.
  6. PC not found → skip result with reason.
  7. summary_line always populated even on skips.
  8. The admin endpoint /api/pricecheck/<id>/validate-package returns
     the same shape.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class _FakeIssue:
    def __init__(self, severity, category, description, field_name="", page=1):
        self.severity = severity
        self.category = category
        self.description = description
        self.field_name = field_name
        self.page = page


class _FakeResult:
    def __init__(self, passed, errors=None, warnings=None, pages_inspected=2):
        self.passed = passed
        self._errors = errors or []
        self._warnings = warnings or []
        self.pages_inspected = pages_inspected

    @property
    def errors(self):
        return self._errors

    @property
    def warnings(self):
        return self._warnings


def _make_pc_with_pdfs(tmp_path, pcid="pc_test"):
    """Create dummy PDF files + a PC dict pointing to them."""
    pdf_704 = tmp_path / "704_test.pdf"
    pdf_704.write_bytes(b"%PDF-1.4\nfake\n")
    pdf_quote = tmp_path / "Reytech_quote.pdf"
    pdf_quote.write_bytes(b"%PDF-1.4\nfake\n")
    return {
        "id": pcid,
        "pc_number": "TEST-001",
        "status": "completed",
        "output_pdf": str(pdf_704),
        "reytech_quote_pdf": str(pdf_quote),
    }


# ── Skip paths ──────────────────────────────────────────────────────


def test_validate_pc_not_found(temp_data_dir, monkeypatch):
    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package("does_not_exist", persist=False)
    assert report["ok"] is False
    assert "pc not found" in report["skipped_reason"]
    assert report["files"] == []
    assert report["overall_passed"] is False


def test_validate_pc_no_generated_pdfs(temp_data_dir, monkeypatch):
    """PC exists but has no output_pdf — skip with operator-readable
    'run Generate first' reason."""
    from src.agents.package_validator import validate_pc_package
    from src.api.data_layer import _save_single_pc
    _save_single_pc("pc_noPDFs", {
        "id": "pc_noPDFs", "pc_number": "NOPDF-1",
        "status": "parsed", "items": [],
    })
    report = validate_pc_package("pc_noPDFs", persist=True)
    assert report["ok"] is False
    assert "no generated pdfs" in report["skipped_reason"].lower()
    assert report["files"] == []


def test_skipped_result_shape_matches_success_shape():
    """A skipped report must have all the fields the template reads on
    a successful report — otherwise the jinja layer crashes."""
    from src.agents.package_validator import _skip_result
    r = _skip_result("pc_test", "some reason")
    for k in ("ok", "validated_at", "pc_id", "files",
              "overall_passed", "total_errors", "total_warnings",
              "summary_line", "skipped_reason"):
        assert k in r, f"missing field on skip result: {k}"


# ── Happy path ──────────────────────────────────────────────────────


def test_validate_pc_with_pdfs_persists_report(tmp_path, temp_data_dir, monkeypatch):
    from src.api.data_layer import _save_single_pc, _load_price_checks
    pc = _make_pc_with_pdfs(tmp_path)
    _save_single_pc(pc["id"], pc)

    # Stub inspect_package to return controlled results
    fake_results = {
        "704_test.pdf": _FakeResult(passed=True, pages_inspected=2),
        "Reytech_quote.pdf": _FakeResult(
            passed=False,
            errors=[_FakeIssue("error", "blank_field",
                               "TOTAL field appears empty", "TOTAL", 1)],
            pages_inspected=1,
        ),
    }
    import src.forms.pdf_visual_qa as vqa
    monkeypatch.setattr(vqa, "inspect_package",
                        lambda paths, **kw: fake_results)

    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package(pc["id"], persist=True)

    assert report["ok"] is True
    assert len(report["files"]) == 2
    assert report["overall_passed"] is False
    assert report["total_errors"] == 1
    assert report["total_warnings"] == 0
    # summary phrasing
    assert "1/2 forms passed" in report["summary_line"]
    assert "1 errors" in report["summary_line"]

    # Reload the PC and confirm the report was persisted under
    # pc.vision_validation
    reloaded = _load_price_checks().get(pc["id"])
    assert "vision_validation" in reloaded
    assert reloaded["vision_validation"]["summary_line"] == report["summary_line"]


def test_validate_pc_persist_false_skips_save(tmp_path, temp_data_dir, monkeypatch):
    from src.api.data_layer import _save_single_pc, _load_price_checks
    pc = _make_pc_with_pdfs(tmp_path, pcid="pc_no_persist")
    _save_single_pc(pc["id"], pc)
    # No prior vision_validation should be set
    reloaded = _load_price_checks().get(pc["id"])
    assert "vision_validation" not in reloaded

    fake_results = {
        "704_test.pdf": _FakeResult(passed=True),
        "Reytech_quote.pdf": _FakeResult(passed=True),
    }
    import src.forms.pdf_visual_qa as vqa
    monkeypatch.setattr(vqa, "inspect_package",
                        lambda paths, **kw: fake_results)

    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package(pc["id"], persist=False)
    assert report["ok"] is True

    # PC should NOT have vision_validation written
    reloaded = _load_price_checks().get(pc["id"])
    assert "vision_validation" not in reloaded


def test_validate_pc_overall_passed_when_all_files_pass(tmp_path, temp_data_dir, monkeypatch):
    from src.api.data_layer import _save_single_pc
    pc = _make_pc_with_pdfs(tmp_path, pcid="pc_all_pass")
    _save_single_pc(pc["id"], pc)

    fake_results = {
        "704_test.pdf": _FakeResult(passed=True),
        "Reytech_quote.pdf": _FakeResult(passed=True),
    }
    import src.forms.pdf_visual_qa as vqa
    monkeypatch.setattr(vqa, "inspect_package",
                        lambda paths, **kw: fake_results)

    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package(pc["id"], persist=False)
    assert report["overall_passed"] is True
    assert report["total_errors"] == 0


def test_validate_pc_collects_only_existing_files(tmp_path, temp_data_dir, monkeypatch):
    """A PC dict referencing a deleted file shouldn't crash — just skip
    that path. The redeploy-on-git-tracked-data-dir edge case."""
    from src.api.data_layer import _save_single_pc
    pdf_real = tmp_path / "real.pdf"
    pdf_real.write_bytes(b"%PDF-1.4\nfake\n")
    pc = {
        "id": "pc_partial",
        "pc_number": "PART-1",
        "status": "completed",
        "output_pdf": str(pdf_real),
        "reytech_quote_pdf": "/tmp/nonexistent_lost_in_redeploy.pdf",
    }
    _save_single_pc(pc["id"], pc)

    fake_results = {"real.pdf": _FakeResult(passed=True)}
    import src.forms.pdf_visual_qa as vqa
    monkeypatch.setattr(vqa, "inspect_package",
                        lambda paths, **kw: fake_results)

    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package(pc["id"], persist=False)
    assert report["ok"] is True
    assert len(report["files"]) == 1


def test_validate_pc_inspect_failure_returns_skip(tmp_path, temp_data_dir, monkeypatch):
    """If inspect_package raises, validator returns a skip report (no
    exception bubbles up)."""
    from src.api.data_layer import _save_single_pc
    pc = _make_pc_with_pdfs(tmp_path, pcid="pc_crash")
    _save_single_pc(pc["id"], pc)

    import src.forms.pdf_visual_qa as vqa

    def _boom(paths, **kw):
        raise RuntimeError("vision API down")

    monkeypatch.setattr(vqa, "inspect_package", _boom)
    from src.agents.package_validator import validate_pc_package
    report = validate_pc_package(pc["id"], persist=False)
    assert report["ok"] is False
    assert "vision call failed" in report["skipped_reason"]


# ── Endpoint ────────────────────────────────────────────────────────


def test_validate_endpoint_returns_skip_for_missing_pc(client):
    resp = client.post("/api/pricecheck/does_not_exist/validate-package")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is False
    assert "pc not found" in data["skipped_reason"]


def test_validate_endpoint_persists_on_post(tmp_path, temp_data_dir, monkeypatch, client):
    """End-to-end: hit the POST endpoint, get back the report, see it
    persisted on next GET / DB read."""
    from src.api.data_layer import _save_single_pc, _load_price_checks
    pc = _make_pc_with_pdfs(tmp_path, pcid="pc_endpoint")
    _save_single_pc(pc["id"], pc)

    fake_results = {
        "704_test.pdf": _FakeResult(passed=True),
        "Reytech_quote.pdf": _FakeResult(passed=True),
    }
    import src.forms.pdf_visual_qa as vqa
    monkeypatch.setattr(vqa, "inspect_package",
                        lambda paths, **kw: fake_results)

    resp = client.post(f"/api/pricecheck/{pc['id']}/validate-package")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["overall_passed"] is True
    assert "2/2 forms passed" in data["summary_line"]

    # Confirm persistence
    reloaded = _load_price_checks().get(pc["id"])
    assert reloaded.get("vision_validation", {}).get("overall_passed") is True
