"""Pin S-8 — auto-pricing tp/fp scan dedup + tp_rate writeback.

Audit 2026-05-07 v2 §S-8: pre-fix the JSONL summariser counted every
row in the append-only log, so re-running the scan endpoint
double-counted every record. Plus PR #822 promised tp_rate writeback
per RFQ but never implemented it.

These tests pin:
  1. summarise_jsonl dedups by (rid, _kind) keeping the LATEST scan.
  2. Re-running scan (writes 2 rows per record) shows 1× counts in
     the aggregate.
  3. Anonymous (pre-S-8 schema) rows still count, don't collide.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agents.auto_pricing_tp_fp import summarise_jsonl  # noqa: E402


def _write_log(tmp_path, rows):
    path = tmp_path / "tp_fp.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return str(path)


class TestSummariseDedupsByRecordAndKind:
    def test_two_scans_same_record_count_once(self, tmp_path):
        path = _write_log(tmp_path, [
            # First scan
            {"rid": "rfq-1", "_kind": "rfq",
             "_scanned_at": "2026-05-08T01:00:00",
             "tp": 3, "fp": 1, "by_source": {"catalog": {"tp": 3, "fp": 1}}},
            # Second scan, same record — should override the first
            {"rid": "rfq-1", "_kind": "rfq",
             "_scanned_at": "2026-05-08T02:00:00",
             "tp": 4, "fp": 0, "by_source": {"catalog": {"tp": 4, "fp": 0}}},
        ])
        out = summarise_jsonl(path)
        # Pre-fix: tp=7, fp=1, records=2.
        # Post-fix: tp=4, fp=0, records=1 (latest only).
        assert out["records"] == 1, (
            f"S-8 regression: dedup not applied. records={out['records']}"
        )
        assert out["tp"] == 4
        assert out["fp"] == 0
        assert out["by_source"] == {"catalog": {"tp": 4, "fp": 0}}

    def test_different_records_both_count(self, tmp_path):
        path = _write_log(tmp_path, [
            {"rid": "rfq-1", "_kind": "rfq",
             "_scanned_at": "2026-05-08T01:00:00",
             "tp": 2, "fp": 1, "by_source": {}},
            {"rid": "rfq-2", "_kind": "rfq",
             "_scanned_at": "2026-05-08T01:00:00",
             "tp": 3, "fp": 0, "by_source": {}},
        ])
        out = summarise_jsonl(path)
        assert out["records"] == 2
        assert out["tp"] == 5
        assert out["fp"] == 1

    def test_same_rid_different_kind_both_count(self, tmp_path):
        """An RFQ and PC could share an `rid` (unlikely but possible).
        Dedup key includes _kind so they don't collide."""
        path = _write_log(tmp_path, [
            {"rid": "X", "_kind": "rfq", "_scanned_at": "2026-05-08T01:00:00",
             "tp": 1, "fp": 0, "by_source": {}},
            {"rid": "X", "_kind": "pc", "_scanned_at": "2026-05-08T01:00:00",
             "tp": 2, "fp": 1, "by_source": {}},
        ])
        out = summarise_jsonl(path)
        assert out["records"] == 2
        assert out["tp"] == 3

    def test_pre_s8_rows_without_rid_dont_collide(self, tmp_path):
        """Pre-S-8 rows might lack rid; they should each count
        independently (degraded but safe)."""
        path = _write_log(tmp_path, [
            {"_scanned_at": "2026-05-08T01:00:00", "tp": 1, "fp": 0,
             "by_source": {}},
            {"_scanned_at": "2026-05-08T01:00:00", "tp": 1, "fp": 0,
             "by_source": {}},
        ])
        out = summarise_jsonl(path)
        assert out["records"] == 2
        assert out["tp"] == 2

    def test_empty_log_returns_zero(self, tmp_path):
        path = _write_log(tmp_path, [])
        out = summarise_jsonl(path)
        assert out == {"records": 0, "tp": 0, "fp": 0,
                       "tp_rate": None, "by_source": {}}

    def test_missing_log_returns_zero(self, tmp_path):
        out = summarise_jsonl(str(tmp_path / "nonexistent.jsonl"))
        assert out == {"records": 0, "tp": 0, "fp": 0,
                       "tp_rate": None, "by_source": {}}


class TestTpRateWritebackSentinelInRoute:
    """Source-level guard: the scan endpoint must call _save_single_rfq
    for each scanned record's tp_rate, not just write JSONL."""

    def test_route_has_tp_rate_writeback_logic(self):
        import pathlib
        src = pathlib.Path(
            "src/api/modules/routes_rfq.py"
        ).read_text(encoding="utf-8")

        # The fix block must include the writeback fields.
        assert 'rec["tp_rate"]' in src, \
            "S-8 regression: tp_rate writeback to record not present"
        assert 'rec["auto_priced_tp"]' in src
        assert 'rec["auto_priced_fp"]' in src
        assert 'rec["auto_priced_scanned_at"]' in src
        assert "S-8" in src, "S-8 sentinel comment missing"
