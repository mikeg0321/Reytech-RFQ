"""Pin transient-error retry on Google Drive `service.files()...` calls.

Tier 1d follow-on (audit 2026-05-07). Drive had NO retry before this PR
— a transient `IncompleteRead` during a nightly backup or an observed-
send Drive backup silently lost the file (the surrounding `try/except`
in `drive_backup.run_nightly_backup` recorded `files_failed += 1` and
moved on; observed_send_backup recorded a `Backup error` marker and
left the operator to retry by hand).

This test pins:
  1. A 500/502/503/504/429 transient HttpError from Drive triggers a
     retry, and the second attempt's success is returned to the caller.
  2. A socket-layer transient (IncompleteRead) is retried.
  3. A 4xx non-transient (403 Forbidden, 404 Not Found) raises
     immediately — no retry, no extra latency.
  4. The download chunk loop retries per-chunk so a flaky transit on
     the second chunk doesn't lose the whole file.

Predicate parity with gmail (PR #833) is enforced — same socket
needles, plus the HttpError 5xx/429 set is Drive-specific because the
googleapiclient surfaces transient backend errors as HttpError
instead of as raw socket exceptions.
"""
from __future__ import annotations

from unittest.mock import MagicMock


# ─────────────────────────────────────────────────────────────────────
# Predicate
# ─────────────────────────────────────────────────────────────────────

def _make_http_error(status: int):
    """Build a real `googleapiclient.errors.HttpError` carrying
    `.resp.status = <status>`. The predicate matches by isinstance,
    not duck-typing, so we have to construct the real class.
    googleapiclient is a hard dep (`_get_service` imports it), so
    importing here is safe in any env that can run gdrive at all."""
    from googleapiclient.errors import HttpError
    resp = MagicMock()
    resp.status = status
    return HttpError(resp=resp, content=b"")


def test_predicate_recognizes_transient_socket_strings():
    from src.core.gdrive import _is_transient_drive_error
    assert _is_transient_drive_error(OSError(
        "IncompleteRead while reading from server"))
    assert _is_transient_drive_error(OSError(
        "ssl.SSLError: [SSL] record layer failure"))
    assert _is_transient_drive_error(OSError("Connection reset by peer"))
    assert _is_transient_drive_error(OSError("Connection aborted"))
    assert _is_transient_drive_error(OSError("EOF occurred in violation of protocol"))
    assert _is_transient_drive_error(TimeoutError("TimeoutError: timed out"))


def test_predicate_recognizes_transient_http_statuses():
    """5xx + 429 from Drive must be treated as transient."""
    from src.core.gdrive import _is_transient_drive_error
    for status in (429, 500, 502, 503, 504):
        err = _make_http_error(status)
        assert _is_transient_drive_error(err), (
            f"status={status} should be transient")


def test_predicate_rejects_4xx_non_transient():
    """403 / 404 / 400 etc. should NOT trigger retry — these are
    operator/auth/quota errors, not transit blips."""
    from src.core.gdrive import _is_transient_drive_error
    for status in (400, 401, 403, 404, 409):
        err = _make_http_error(status)
        # 4xx-other-than-429 → not in transient set.
        assert not _is_transient_drive_error(err), (
            f"status={status} must NOT be transient")


def test_predicate_rejects_unrelated_exceptions():
    """ValueError / KeyError / generic Exception with no transient
    needle in the message must not retry."""
    from src.core.gdrive import _is_transient_drive_error
    assert not _is_transient_drive_error(ValueError("bad parent_id"))
    assert not _is_transient_drive_error(KeyError("name"))
    assert not _is_transient_drive_error(Exception("Permission denied"))


# ─────────────────────────────────────────────────────────────────────
# Helper wraps `with_retry`
# ─────────────────────────────────────────────────────────────────────

def test_with_drive_retry_succeeds_after_one_transient(monkeypatch):
    """First attempt raises IncompleteRead, second attempt returns
    a folder list — caller should see the list, no exception."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        if n["calls"] == 1:
            raise OSError("IncompleteRead while reading from server")
        return {"files": [{"id": "abc", "name": "PO-001"}]}

    out = gdrive._with_drive_retry(fn, op="folders.list")
    assert n["calls"] == 2
    assert out["files"][0]["id"] == "abc"


def test_with_drive_retry_propagates_non_transient_immediately(monkeypatch):
    """A 403 must NOT be retried — operator wants the real error
    surfaced fast, not delayed by 3 attempts × 0.5/1.0/2.0 sleep."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        raise _make_http_error(403)

    try:
        gdrive._with_drive_retry(fn, op="folders.list")
    except Exception as e:
        # Predicate said "non-transient" → with_retry re-raises original
        # exception. Status 403 carried in the message ensures the
        # operator's debugger sees the real cause.
        assert "403" in str(e)
    assert n["calls"] == 1


def test_with_drive_retry_exhausts_attempts_then_raises(monkeypatch):
    """If a transient persists across all 3 attempts, the last
    exception is raised and the caller hears about it (not silent)."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        raise _make_http_error(503)  # transient — exhausted

    try:
        gdrive._with_drive_retry(fn, op="folders.list")
        raise AssertionError("expected exception")
    except Exception as e:
        assert "503" in str(e)
    assert n["calls"] == 3  # first + 2 retries


# ─────────────────────────────────────────────────────────────────────
# Integration: real call sites use the helper
# ─────────────────────────────────────────────────────────────────────

def test_get_or_create_folder_retries_transient(monkeypatch):
    """End-to-end through `_get_or_create_folder`: a transient on the
    first list() call retries and the second attempt succeeds."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    # Bypass the in-process folder cache so this test exercises
    # the API path even after other tests warmed it.
    gdrive._folder_cache.clear()

    n = {"calls": 0}

    def execute():
        n["calls"] += 1
        if n["calls"] == 1:
            raise OSError("[SSL] record layer failure")
        return {"files": [{"id": "fid_existing", "name": "Reports"}]}

    list_req = MagicMock()
    list_req.execute = execute
    files_obj = MagicMock()
    files_obj.list.return_value = list_req
    service = MagicMock()
    service.files.return_value = files_obj

    monkeypatch.setattr(gdrive, "_get_service", lambda: service)

    fid = gdrive._get_or_create_folder("Reports", "parent_X")
    assert fid == "fid_existing"
    assert n["calls"] == 2  # one transient retry + success


def test_list_files_retries_transient_500(monkeypatch):
    """`list_files` survives a 500 transient on first call."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def execute():
        n["calls"] += 1
        if n["calls"] == 1:
            raise _make_http_error(500)
        return {"files": [
            {"id": "f1", "name": "doc.pdf", "mimeType": "application/pdf"},
        ]}

    list_req = MagicMock()
    list_req.execute = execute
    files_obj = MagicMock()
    files_obj.list.return_value = list_req
    service = MagicMock()
    service.files.return_value = files_obj
    monkeypatch.setattr(gdrive, "_get_service", lambda: service)

    files = gdrive.list_files("folder_X")
    assert len(files) == 1
    assert files[0]["id"] == "f1"
    assert n["calls"] == 2


def test_download_chunk_retries_per_chunk(monkeypatch, tmp_path):
    """If `MediaIoBaseDownload.next_chunk()` raises a transient,
    the loop retries that chunk via `_with_drive_retry` and continues
    — the file is NOT silently truncated."""
    from src.core import gdrive
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    state = {"calls": 0, "transient_thrown": False}

    class FakeDownloader:
        def __init__(self, *a, **kw):
            pass

        def next_chunk(self):
            state["calls"] += 1
            # Throw a single transient mid-download.
            if not state["transient_thrown"]:
                state["transient_thrown"] = True
                raise OSError("Connection reset by peer")
            # After the transient, immediately complete.
            return None, True

    # Stub MediaIoBaseDownload import inside download_file.
    import sys
    import types
    fake_http = types.ModuleType("googleapiclient.http")
    fake_http.MediaIoBaseDownload = FakeDownloader
    monkeypatch.setitem(sys.modules, "googleapiclient.http", fake_http)

    service = MagicMock()
    monkeypatch.setattr(gdrive, "_get_service", lambda: service)
    monkeypatch.setattr(gdrive, "_audit", lambda *a, **k: None)

    out_path = tmp_path / "downloaded.bin"
    ok = gdrive.download_file("file_id_X", str(out_path))
    assert ok is True
    # Calls: 1 (transient raised), 2 (retry → done). Loop exits after
    # the retried chunk reports done=True.
    assert state["calls"] == 2
    assert out_path.exists()
