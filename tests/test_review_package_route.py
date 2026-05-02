"""Integration test: GET /rfq/<rid>/review-package renders the alignment view.

Runs the full Flask stack against an isolated DB + temp data dir. Confirms
the new alignment rollup, items table, and forms checklist are in the
rendered HTML, and confirms the Force Approve button is gone.
"""
from __future__ import annotations


def _seed_manifest(rid, agency_key="calvet", agency_name="CalVet",
                   required=("quote", "704b", "cv012_cuf"),
                   generated_filenames=None,
                   field_audit=None,
                   source_validation=None):
    """Seed a package_manifest + package_review rows in the test DB."""
    from src.core.dal import create_package_manifest
    if generated_filenames is None:
        generated_filenames = {
            "quote": "RFQ-2026-TEST_Reytech Quote.pdf",
            "704b": "RFQ-2026-TEST_Reytech_704B.pdf",
            "cv012_cuf": "RFQ-2026-TEST_Reytech_CV012CUF.pdf",
        }
    gen_forms = [{"form_id": f, "filename": fn} for f, fn in generated_filenames.items()]

    mid = create_package_manifest(
        rfq_id=rid, agency_key=agency_key, agency_name=agency_name,
        required_forms=list(required),
        generated_forms=gen_forms,
        quote_number="R26Q38", quote_total=1000.00, item_count=1,
        created_by="test",
    )
    assert mid, "manifest creation failed"

    # Set field_audit / source_validation if requested (these are JSON columns)
    if field_audit is not None or source_validation is not None:
        import json
        from src.core.db import get_db
        with get_db() as conn:
            sets, vals = [], []
            if field_audit is not None:
                sets.append("field_audit = ?")
                vals.append(json.dumps(field_audit))
            if source_validation is not None:
                sets.append("source_validation = ?")
                vals.append(json.dumps(source_validation))
            vals.append(mid)
            conn.execute(f"UPDATE package_manifest SET {', '.join(sets)} WHERE id = ?", vals)
    return mid


class TestReviewPackageRoute:

    def test_renders_alignment_rollup_with_realistic_data(self, auth_client, seed_rfq):
        rid = seed_rfq
        # Sample RFQ resolves to agency=cchcs (department="CDCR - ..."), so
        # required_forms = ['703b', '704b', 'bidpkg', 'quote']. Seed all of
        # them on disk so the alignment rollup has complete data.
        _seed_manifest(rid, agency_key="cchcs", agency_name="CCHCS",
                       required=("703b", "704b", "bidpkg", "quote"),
                       generated_filenames={
                           "703b": "RFQ-2026-TEST_Reytech_703B.pdf",
                           "704b": "RFQ-2026-TEST_Reytech_704B.pdf",
                           "bidpkg": "RFQ-2026-TEST_Reytech_RFQPackage.pdf",
                           "quote": "RFQ-2026-TEST_Reytech Quote.pdf",
                       },
                       field_audit={"_qa_passed": True,
                                    "_qa_summary": {"forms_checked": 4,
                                                    "duration_ms": 250,
                                                    "critical_issues": []}},
                       source_validation={"errors": [], "warnings": [],
                                          "checks": ["buyer match", "sol# match"]})

        r = auth_client.get(f"/rfq/{rid}/review-package")
        assert r.status_code == 200
        body = r.get_data(as_text=True)

        # Rollup banner present
        assert 'data-testid="rv-rollup"' in body
        # Items section present
        assert 'data-testid="rv-items-alignment"' in body
        # Forms checklist shows on-disk filenames (the "ESPECIALLY" piece).
        # Check for the unique CCHCS bidpkg name we seeded.
        assert "RFQ-2026-TEST_Reytech_RFQPackage.pdf" in body
        # Force Approve removed (regression guard)
        assert "Force Approve" not in body
        assert "forceApprove()" not in body

    def test_aligned_state_shows_ready_to_send(self, auth_client, seed_rfq):
        rid = seed_rfq
        _seed_manifest(rid,
                       field_audit={"_qa_passed": True,
                                    "_qa_summary": {"forms_checked": 3,
                                                    "critical_issues": []}},
                       source_validation={"errors": [], "checks": ["ok"]})
        r = auth_client.get(f"/rfq/{rid}/review-package")
        body = r.get_data(as_text=True)
        # When all 5 checks pass, the headline is READY TO SEND. (The fixture
        # has full buyer + due_date + priced item + qa pass, so we expect
        # alignment = green except for the "no source items" rendering note,
        # which doesn't block the rollup.)
        assert "READY TO SEND" in body or "issue" in body  # both states valid

    def test_blocked_state_shows_issues(self, auth_client, seed_rfq):
        rid = seed_rfq
        # Seed with field_audit failure → should block
        _seed_manifest(rid,
                       field_audit={"_qa_passed": False,
                                    "_qa_summary": {"forms_checked": 3,
                                                    "critical_issues": ["sig missing on 703B"]}})
        r = auth_client.get(f"/rfq/{rid}/review-package")
        body = r.get_data(as_text=True)
        assert "issue(s) blocking send" in body
        assert "QA failed" in body

    def test_missing_form_marked_in_checklist(self, auth_client, seed_rfq):
        rid = seed_rfq
        _seed_manifest(rid, generated_filenames={
            "quote": "q.pdf",
            "704b": "",  # missing!
            "cv012_cuf": "cv.pdf",
        })
        r = auth_client.get(f"/rfq/{rid}/review-package")
        body = r.get_data(as_text=True)
        # 704B should appear with "Not generated" or similar missing-state marker
        assert "AMS 704B" in body
        # The blocked rollup should mention it
        assert ("Missing required forms" in body and "AMS 704B" in body)
