"""Mark-Sent → quotes table propagation — Patch 4 of the 2026-05-25
oracle substrate fix.

Pins the new contract:
  - propagate_sent_to_quote_row(record) reads record.reytech_quote_number,
    calls update_quote_status(qn, "sent") so the quotes table row flips.
  - 4 Mark-Sent code paths (PC mark-sent, PC mark-sent-manually, RFQ
    mark-sent-manually, RFQ send_email_enhanced) all call the helper.
  - The helper degrades cleanly when no quote_number is set on the
    record (e.g. PC marked sent before a quote PDF was generated).

The bug this fixes: pre-Patch-4, the 4 Mark-Sent paths flipped the
PC/RFQ entity status + sent_at but did NOT update the `quotes` table.
award_tracker's eligibility query (`WHERE status='sent' AND total > 0`)
saw zero rows despite real operator activity → empty Oracle weekly.

Tests are grep-level for the wiring (the 4 callsites carry the import)
and unit-level for the helper itself.
"""
from pathlib import Path
from unittest.mock import patch

import pytest


_REPO = Path(__file__).resolve().parent.parent


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


# ── Helper unit tests ──────────────────────────────────────────────────────


def test_propagate_sent_calls_update_quote_status_on_linked_quote():
    """Happy path: record has reytech_quote_number, helper calls
    update_quote_status with status='sent'."""
    from src.core import quote_lifecycle_shared as qls

    captured = {}
    def _fake(qn, status, actor="user", notes=""):
        captured["qn"] = qn
        captured["status"] = status
        captured["actor"] = actor
        captured["notes"] = notes
        return True

    with patch("src.forms.quote_generator.update_quote_status",
               side_effect=_fake):
        ok = qls.propagate_sent_to_quote_row(
            {"reytech_quote_number": "R26Q42",
             "pc_number": "PC-1", "status": "sent"},
            source="user",
        )

    assert ok is True
    assert captured["qn"] == "R26Q42"
    assert captured["status"] == "sent"
    assert captured["actor"] == "user"


def test_propagate_sent_returns_false_when_no_quote_number():
    """Some records (PCs mark-sent before quote PDF generation) have no
    reytech_quote_number — helper must silently return False, not raise."""
    from src.core import quote_lifecycle_shared as qls

    with patch("src.forms.quote_generator.update_quote_status") as mock_up:
        ok = qls.propagate_sent_to_quote_row(
            {"status": "sent", "pc_number": "PC-1"},  # no reytech_quote_number
            source="user",
        )
    assert ok is False
    mock_up.assert_not_called()


def test_propagate_sent_returns_false_when_quote_number_blank():
    """Empty string quote_number must be treated as missing, not as a
    valid lookup key (would cause a 'WHERE quote_number=' match miss)."""
    from src.core import quote_lifecycle_shared as qls

    with patch("src.forms.quote_generator.update_quote_status") as mock_up:
        ok = qls.propagate_sent_to_quote_row(
            {"reytech_quote_number": "   ", "status": "sent"},
            source="user",
        )
    assert ok is False
    mock_up.assert_not_called()


def test_propagate_sent_falls_back_to_quote_number_field():
    """Some legacy records carry `quote_number` directly instead of
    `reytech_quote_number`. Helper must fall back."""
    from src.core import quote_lifecycle_shared as qls

    captured = {}
    def _fake(qn, status, actor="user", notes=""):
        captured["qn"] = qn
        return True

    with patch("src.forms.quote_generator.update_quote_status",
               side_effect=_fake):
        ok = qls.propagate_sent_to_quote_row(
            {"quote_number": "R26Q42-LEGACY", "status": "sent"},
            source="user",
        )
    assert ok is True
    assert captured["qn"] == "R26Q42-LEGACY"


def test_propagate_sent_logs_warning_when_quote_row_not_found(caplog):
    """If update_quote_status returns False (linked quote row doesn't
    exist in the quotes table), log a WARNING so the audit trail tells
    us how many entities are flipped without a backing row."""
    import logging
    from src.core import quote_lifecycle_shared as qls

    with patch("src.forms.quote_generator.update_quote_status",
               return_value=False), caplog.at_level(logging.WARNING):
        ok = qls.propagate_sent_to_quote_row(
            {"reytech_quote_number": "R26Q-MISSING"},
            source="user",
        )
    assert ok is False
    assert any("PROPAGATE_MISS" in r.message for r in caplog.records), (
        "missing-quote case must surface as a warning, not silent debug"
    )


def test_propagate_sent_never_raises_on_exception():
    """If update_quote_status throws, propagate_sent_to_quote_row must
    return False without re-raising — the PC/RFQ flip is authoritative
    and a propagation failure cannot block it."""
    from src.core import quote_lifecycle_shared as qls

    with patch("src.forms.quote_generator.update_quote_status",
               side_effect=RuntimeError("DB locked")):
        # Must not raise
        ok = qls.propagate_sent_to_quote_row(
            {"reytech_quote_number": "R26Q42"},
            source="user",
        )
    assert ok is False


# ── Grep-level wiring tests ────────────────────────────────────────────────


def test_pc_mark_sent_route_calls_propagate():
    body = _read(_REPO / "src" / "api" / "modules" / "routes_pricecheck_pricing.py")
    # Both PC mark-sent flavors must call the helper
    assert body.count("propagate_sent_to_quote_row(pc, source=") >= 2, (
        "regression: one of api_pricecheck_mark_sent / "
        "api_pricecheck_mark_sent_manually stopped propagating to the "
        "quotes table — award_tracker eligibility breaks for PC flow"
    )


def test_rfq_mark_sent_paths_call_propagate():
    body = _read(_REPO / "src" / "api" / "modules" / "routes_rfq_admin.py")
    assert body.count("propagate_sent_to_quote_row(r, source=") >= 2, (
        "regression: one of api_rfq_mark_sent_manually / "
        "send_email_enhanced stopped propagating to the quotes table — "
        "award_tracker eligibility breaks for RFQ flow"
    )


def test_propagate_helper_is_defined():
    """The helper must exist in quote_lifecycle_shared.py at the
    canonical location (next to mark_won / mark_lost)."""
    body = _read(_REPO / "src" / "core" / "quote_lifecycle_shared.py")
    assert "def propagate_sent_to_quote_row(" in body, (
        "regression: propagate_sent_to_quote_row helper was removed or "
        "renamed — the 4 mark-sent callers will fail to import"
    )
