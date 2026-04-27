"""Phase 1.6 PR3i: forms_drift_monitor tests."""

import json
import os
from datetime import datetime
from unittest.mock import patch

import pytest


def _seed_rfq(conn, rid="RFQ-D1", subject="Need STD 999 form filled",
              body="Please complete STD 999 and STD 204"):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(rfqs)")}
    fields = ["id", "agency", "received_at", "email_subject"]
    values = [rid, "CDCR", datetime.utcnow().isoformat(), subject]
    if "body_text" in cols:
        fields.append("body_text"); values.append(body)
    if "items" in cols:
        fields.append("items"); values.append("[]")
    placeholders = ",".join("?" * len(fields))
    conn.execute(
        f"INSERT INTO rfqs ({', '.join(fields)}) VALUES ({placeholders})",
        values,
    )


class TestExtractFormTokens:
    def test_std_pattern(self):
        from src.agents.forms_drift_monitor import _extract_form_tokens
        toks = _extract_form_tokens("Please complete STD 999 and STD-456")
        assert any("999" in t for t in toks)
        assert any("456" in t for t in toks)

    def test_gspd_pattern(self):
        from src.agents.forms_drift_monitor import _extract_form_tokens
        toks = _extract_form_tokens("Sign GSPD-05-105 and submit")
        assert "GSPD-05-105" in toks

    def test_obs_pattern(self):
        from src.agents.forms_drift_monitor import _extract_form_tokens
        toks = _extract_form_tokens("Food items require OBS 1600 cert")
        assert any("1600" in t for t in toks)

    def test_calrecycle_pattern(self):
        from src.agents.forms_drift_monitor import _extract_form_tokens
        toks = _extract_form_tokens("CalRecycle 74 must be signed")
        assert any("74" in t for t in toks)

    def test_empty_text(self):
        from src.agents.forms_drift_monitor import _extract_form_tokens
        assert _extract_form_tokens("") == []
        assert _extract_form_tokens(None) == []


class TestKnownFormTokens:
    def test_includes_std204(self):
        from src.agents.forms_drift_monitor import _known_form_tokens
        toks = _known_form_tokens()
        assert "STD 204" in toks
        assert "DARFUR" in toks


class TestScanFormsDrift:
    def test_returns_report_envelope(self, app):
        from src.agents.forms_drift_monitor import scan_forms_drift
        r = scan_forms_drift(days=30)
        for key in ("scanned_emails", "scanned_attachments",
                    "lookback_days", "scanned_at",
                    "new_form_mentions", "revised_templates",
                    "agency_anomalies"):
            assert key in r
        assert r["lookback_days"] == 30

    def test_detects_unknown_form_in_recent_email(self, app):
        from src.agents.forms_drift_monitor import scan_forms_drift
        from src.core.db import get_db
        with get_db() as conn:
            # Two RFQs mentioning a fake form so the >=2 mention filter passes
            _seed_rfq(conn, rid="RFQ-DA",
                      subject="Need STD 9999 completed",
                      body="STD 9999 is required this week")
            _seed_rfq(conn, rid="RFQ-DB",
                      subject="STD 9999 reminder",
                      body="Don't forget STD 9999")
            conn.commit()

        r = scan_forms_drift(days=30)
        tokens = [m["token"] for m in r["new_form_mentions"]]
        # STD 9999 doesn't exist in FORM_TEXT_PATTERNS — should surface
        assert any("9999" in t for t in tokens), \
            f"STD 9999 not flagged. Got: {tokens}"


class TestSaveAndLatestReport:
    def test_save_then_latest_round_trip(self, app, tmp_path):
        from src.agents.forms_drift_monitor import save_report, latest_report
        with patch("src.agents.forms_drift_monitor._data_dir",
                   return_value=str(tmp_path)):
            saved = {"scanned_emails": 7, "lookback_days": 30,
                     "scanned_at": "2026-04-26T12:00:00Z"}
            path = save_report(saved)
            assert os.path.isfile(path)
            r = latest_report()
        assert r is not None
        assert r["scanned_emails"] == 7

    def test_latest_returns_none_when_no_report(self, app, tmp_path):
        from src.agents.forms_drift_monitor import latest_report
        with patch("src.agents.forms_drift_monitor._data_dir",
                   return_value=str(tmp_path)):
            r = latest_report()
        assert r is None


class TestEndpoints:
    def test_latest_returns_message_when_empty(self, client, app, tmp_path):
        with patch("src.agents.forms_drift_monitor._data_dir",
                   return_value=str(tmp_path)):
            r = client.get("/api/forms-drift/latest")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        # Either no report yet, or a real report (if previous test ran first)
        assert "report" in d

    def test_scan_endpoint_runs_and_persists(self, client, app, tmp_path):
        with patch("src.agents.forms_drift_monitor._data_dir",
                   return_value=str(tmp_path)):
            r = client.post("/api/forms-drift/scan?days=7")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "report" in d
        assert d["report"]["lookback_days"] == 7
