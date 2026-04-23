"""Pre-deletion canary for the `process_rfq_email` legacy fallback.

Per `project_old_route_audit_2026_04_14.md`, the legacy fallback
path (~1270 lines at `src/api/dashboard.py:2022-3293`) is
dead-after-flip code now that `ingest.classifier_v2_enabled` has
been ON in prod since 2026-04-14. Before deleting it outright,
the audit memo calls for an intermediate step:

> Add a failing-fast guard: replace the legacy block with
> `raise RuntimeError("classifier_v2 path failed — fallback
> removed")` ONE WINDOW before the real delete so any miss causes
> a loud error rather than a silent email drop.

This PR implements that canary behind the
`ingest.legacy_fallback_loud` feature flag (default False —
current behavior preserved). Operator flips the flag via
`/api/admin/flags` for 48h; zero crashes means safe to delete the
legacy block in the next PR.

### Test contract
- Flag OFF (default) → current behavior: v2 rejects or crashes,
  legacy path runs silently.
- Flag ON + v2 rejects → RuntimeError raised.
- Flag ON + v2 crashes → RuntimeError raised.
- Flag ON + v2 succeeds → no RuntimeError (legacy path never
  touched; v2 returned).
"""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock


def _mk_email():
    """Minimal email shape for process_rfq_email."""
    return {
        "subject": "Test RFQ",
        "from": "buyer@cdcr.ca.gov",
        "body": "Please quote attached.",
        "attachments": [],
        "uid": "test-email-uid-123",
    }


class TestFlagOffPreservesLegacyFallback:
    """Default (flag OFF) behavior must be IDENTICAL to pre-canary.
    Regression guard: the canary must not accidentally fire when
    operators haven't opted in."""

    def test_flag_off_v2_rejected_no_runtime_error(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        # Patch classifier_v2 to return an ok=False result
        mock_result = MagicMock(ok=False, errors=["v2 rejected"])
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            return_value=mock_result,
        ), patch(
            "src.core.flags.get_flag",
            return_value=False,  # flag OFF
        ):
            # The legacy path runs but may raise OTHER exceptions
            # downstream (KeyError on missing email fields, etc.).
            # The ONLY assertion: the canary's specific RuntimeError
            # must NOT fire when the flag is off. Any other
            # exception is pre-existing legacy-path behavior.
            try:
                process_rfq_email(_mk_email())
            except Exception as e:
                if isinstance(e, RuntimeError) and "legacy_fallback_loud" in str(e):
                    pytest.fail(
                        f"canary fired with flag OFF: {e}"
                    )
                # Otherwise: pre-existing legacy behavior — fine.


class TestFlagOnSurfacesRejection:
    """When the flag is ON, every path that previously fell
    through to the legacy block must now raise a RuntimeError
    carrying the canary marker."""

    def test_flag_on_v2_rejected_raises(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        mock_result = MagicMock(ok=False, errors=["v2 rejected"])
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            return_value=mock_result,
        ), patch(
            "src.core.flags.get_flag",
            return_value=True,  # flag ON
        ):
            with pytest.raises(RuntimeError) as exc:
                process_rfq_email(_mk_email())
            msg = str(exc.value)
            assert "legacy_fallback_loud=True" in msg
            assert "classifier_v2 rejected" in msg
            # Escape hatch message so operator knows how to revert
            assert "/api/admin/flags" in msg

    def test_flag_on_v2_raises_raises_canary(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            side_effect=ValueError("unexpected crash in v2"),
        ), patch(
            "src.core.flags.get_flag",
            return_value=True,  # flag ON
        ):
            with pytest.raises(RuntimeError) as exc:
                process_rfq_email(_mk_email())
            msg = str(exc.value)
            assert "legacy_fallback_loud=True" in msg
            # The underlying exception type is surfaced so the
            # operator can tell WHAT went wrong in v2
            assert (
                "ValueError" in msg
                or "unexpected crash in v2" in msg
            )


class TestFlagOnV2SuccessNoCanary:
    """When v2 succeeds, the legacy path never runs, so the canary
    must NOT fire regardless of flag state. This is the common
    case — ~100% of real traffic — and must stay silent even with
    the flag on."""

    def test_flag_on_v2_success_returns_record(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        mock_result = MagicMock(
            ok=True, record_type="rfq", record_id="test-rfq-001",
            classification=None, errors=[],
        )
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            return_value=mock_result,
        ), patch(
            "src.core.flags.get_flag",
            return_value=True,  # flag ON but v2 succeeds
        ), patch(
            "src.api.dashboard.load_rfqs",
            return_value={"test-rfq-001": {"id": "test-rfq-001"}},
        ):
            # v2 path returns the record; no canary fire.
            result = process_rfq_email(_mk_email())
            # v2 success path returns the record dict for RFQs
            assert result is not None
            assert result.get("id") == "test-rfq-001"


class TestCanaryDocumentation:
    """The canary's error message must tell the operator how to
    turn it off. Operators who hit this unexpectedly need the
    flip-off instructions in the exception body — no guessing."""

    def test_error_message_names_the_flag(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        mock_result = MagicMock(ok=False, errors=["x"])
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            return_value=mock_result,
        ), patch(
            "src.core.flags.get_flag",
            return_value=True,
        ):
            with pytest.raises(RuntimeError) as exc:
                process_rfq_email(_mk_email())
            msg = str(exc.value)
            assert "ingest.legacy_fallback_loud" in msg

    def test_error_message_names_the_admin_flags_route(self, temp_data_dir):
        from src.api.dashboard import process_rfq_email
        mock_result = MagicMock(ok=False, errors=["x"])
        with patch(
            "src.core.ingest_pipeline.process_buyer_request",
            return_value=mock_result,
        ), patch(
            "src.core.flags.get_flag",
            return_value=True,
        ):
            with pytest.raises(RuntimeError) as exc:
                process_rfq_email(_mk_email())
            assert "/api/admin/flags" in str(exc.value)
