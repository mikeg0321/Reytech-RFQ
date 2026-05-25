"""Tests for the /api/admin/drive/replay endpoint.

Closes the substrate gap caught 2026-05-25: Drive forms-archive last wrote
2026-05-15 because Mike's manual-send workflow bypasses the operator-button
triggers (drive_triggers.on_quote_sent / on_package_generated). The replay
endpoint backfills the gap by reading Gmail SENT and uploading attachments
to Drive's Pending/{year}/{sol#}/ tree.

These tests pin the helper contracts. The full route is exercised
end-to-end by the `--include slow-net` Drive integration suite (not part
of the pre-push baseline since it requires live Google API auth).
"""
import sys
import os


def _import_helpers():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from src.api.modules.routes_admin_drive_replay import (
        _extract_sol_number,
        _iter_attachments,
        _SOL_PATTERNS,
    )
    return _extract_sol_number, _iter_attachments, _SOL_PATTERNS


def _import_find_subfolder():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from src.api.modules.routes_admin_drive_replay import _find_subfolder_readonly
    return _find_subfolder_readonly


# ── Sol# extraction ───────────────────────────────────────────────────────


class TestExtractSolNumber:
    """The sol# extractor decides which solicitation a Gmail thread belongs
    to. Wrong sol# → wrong Drive folder → wrong archive. Must be precise."""

    def test_cchcs_preq_8digit(self):
        extract, *_ = _import_helpers()
        assert extract("Urgent Request for Quote: 10847776 for VSP") == (
            "10847776", "cchcs_preq",
        )

    def test_cchcs_preq_prefix(self):
        extract, *_ = _import_helpers()
        # The actual Gmail subject from Chechi 2026-05-11
        assert extract("PREQ 10846357") == ("10846357", "cchcs_preq")

    def test_cchcs_in_re_thread(self):
        extract, *_ = _import_helpers()
        assert extract("Re: Urgent Request for Quote: 10843811 for CHCF due 5/19/26 at 5:00 pm") == (
            "10843811", "cchcs_preq",
        )

    def test_dsh_cb_format(self):
        extract, *_ = _import_helpers()
        # The actual Gmail subject from Butuza 2026-05-14
        assert extract("Please find attached quote request 25CB021") == (
            "25CB021", "dsh_cb",
        )

    def test_dsh_cb_lowercase(self):
        extract, *_ = _import_helpers()
        assert extract("Re: 25cb021 status") == ("25cb021", "dsh_cb")

    def test_cchcs_preq_with_pr_prefix(self):
        """8-digit CCHCS number with a 'PR' prefix still resolves cchcs_preq."""
        extract, *_ = _import_helpers()
        assert extract("PR 10842771 - QUOTE REQUEST") == ("10842771", "cchcs_preq")

    def test_no_sol_returns_none(self):
        extract, *_ = _import_helpers()
        assert extract("Hi Mike just checking in") is None
        assert extract("") is None
        assert extract(None) is None


# ── Attachment iterator ───────────────────────────────────────────────────


class TestIterAttachments:
    """The attachment iterator walks Gmail's part-tree. Must yield every
    PDF attachment regardless of nesting depth, and skip inline images +
    text/html parts that don't have an attachmentId."""

    def test_empty_payload(self):
        _, it, _ = _import_helpers()
        assert list(it(None)) == []
        assert list(it({})) == []

    def test_single_attachment(self):
        _, it, _ = _import_helpers()
        payload = {
            "parts": [{
                "filename": "10846357_704B.pdf",
                "mimeType": "application/pdf",
                "body": {"attachmentId": "ATT_a1", "size": 12345},
            }],
        }
        atts = list(it(payload))
        assert len(atts) == 1
        f, m, aid, s = atts[0]
        assert f == "10846357_704B.pdf"
        assert m == "application/pdf"
        assert aid == "ATT_a1"
        assert s == 12345

    def test_nested_parts_traversed(self):
        """Real Gmail payloads nest parts inside multipart/mixed,
        multipart/alternative. The walker must descend."""
        _, it, _ = _import_helpers()
        payload = {
            "parts": [
                {"mimeType": "multipart/alternative", "parts": [
                    {"mimeType": "text/plain", "body": {"size": 100}},
                    {"mimeType": "text/html", "body": {"size": 200}},
                ]},
                {"mimeType": "multipart/mixed", "parts": [
                    {"filename": "703B.pdf", "mimeType": "application/pdf",
                     "body": {"attachmentId": "ATT_2", "size": 50000}},
                    {"filename": "bid.pdf", "mimeType": "application/pdf",
                     "body": {"attachmentId": "ATT_3", "size": 80000}},
                ]},
            ],
        }
        atts = list(it(payload))
        files = sorted(a[0] for a in atts)
        assert files == ["703B.pdf", "bid.pdf"], (
            f"Expected 703B.pdf + bid.pdf, got {files}"
        )

    def test_inline_parts_without_attachment_id_skipped(self):
        """text/plain and text/html body parts have a body.size but no
        attachmentId — they must not be yielded."""
        _, it, _ = _import_helpers()
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"size": 100}},
                {"filename": "quote.pdf", "mimeType": "application/pdf",
                 "body": {"attachmentId": "ATT_X", "size": 1000}},
            ],
        }
        atts = list(it(payload))
        assert len(atts) == 1
        assert atts[0][0] == "quote.pdf"

    def test_filename_without_attachment_id_skipped(self):
        """A part with a filename but no attachmentId (rare; unsupported by
        Gmail) is malformed — skipped to be safe."""
        _, it, _ = _import_helpers()
        payload = {"parts": [
            {"filename": "loose.pdf", "mimeType": "application/pdf",
             "body": {"size": 100}},  # no attachmentId
        ]}
        assert list(it(payload)) == []


class TestSolPatternsList:
    """Sol# patterns are scoped to the two formats Mike's CCHCS/DSH flow
    actually uses. New patterns require explicit Mike approval — adding
    speculative ones risks misrouting an archive to the wrong sol#."""

    def test_only_cchcs_and_dsh_patterns(self):
        _, _, patterns = _import_helpers()
        ids = [p[1] for p in patterns]
        assert ids == ["cchcs_preq", "dsh_cb"], (
            f"Sol# patterns must stay scoped to Mike's actual agencies; got {ids}"
        )


# ── Read-only lookup (dry-run contract) ──────────────────────────────────


class _FakeListCall:
    def __init__(self, files):
        self._files = files

    def execute(self):
        return {"files": self._files}


class _FakeFilesService:
    """Records the kwargs every files().list() call gets so the test can
    assert we never asked Drive to create anything."""
    def __init__(self, present: list[dict]):
        self.present = present
        self.list_calls: list[dict] = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        # Treat any list() that mentions the same parent as returning
        # the configured `present` files; otherwise empty.
        return _FakeListCall(list(self.present))

    # Sentinel: the test asserts these are never invoked.
    def create(self, **kwargs):  # pragma: no cover — must not be reached
        raise AssertionError(
            "files().create called from a read-only path — dry-run leaked a write"
        )


class _FakeDriveService:
    def __init__(self, files_service):
        self._files = files_service

    def files(self):
        return self._files


class TestFindSubfolderReadonly:
    """The dry-run path MUST NOT create folders. This helper is the gate —
    if it ever calls files().create, dry_run is leaking writes."""

    def test_existing_folder_returns_id(self):
        find = _import_find_subfolder()
        fs = _FakeFilesService(present=[{"id": "FOLDER_X", "name": "10843811"}])
        drive = _FakeDriveService(fs)
        assert find(drive, "10843811", "PARENT_P") == "FOLDER_X"
        assert len(fs.list_calls) == 1, "expected exactly one read"

    def test_missing_folder_returns_none(self):
        find = _import_find_subfolder()
        fs = _FakeFilesService(present=[])
        drive = _FakeDriveService(fs)
        assert find(drive, "99999999", "PARENT_P") is None

    def test_query_constraints(self):
        """Search must be scoped to the parent + folder mimeType + non-trashed."""
        find = _import_find_subfolder()
        fs = _FakeFilesService(present=[])
        drive = _FakeDriveService(fs)
        find(drive, "10847776", "PARENT_P")
        assert len(fs.list_calls) == 1
        q = fs.list_calls[0]["q"]
        assert "name='10847776'" in q
        assert "'PARENT_P' in parents" in q
        assert "mimeType='application/vnd.google-apps.folder'" in q
        assert "trashed=false" in q
