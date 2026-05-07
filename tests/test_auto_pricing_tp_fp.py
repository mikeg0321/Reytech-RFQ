"""Auto-pricing TP/FP rate telemetry (#18, 2026-05-07).

Pins:
  * `classify_item` returns tp/fp/skip per the documented rule.
  * `compute_record_tp_fp` aggregates per record + per source.
  * `scan_records` only counts records in the status allowlist AND with
    at least one auto-priced item.
  * `summarise_jsonl` rolls up across many scans, ignores blanks/garbage.
  * Endpoints require auth.
  * /scan appends one JSONL row per qualifying record and returns a roll-up.
  * /summary returns aggregated counts from the JSONL log.
"""
from __future__ import annotations

import json

import pytest

from src.agents.auto_pricing_tp_fp import (
    classify_item,
    compute_record_tp_fp,
    scan_records,
    summarise_jsonl,
)


# ─────────────────── Pure-helper tests ───────────────────

def test_classify_skip_when_not_auto_priced():
    assert classify_item({"price_per_unit": 5.0}) == "skip"
    assert classify_item({"auto_priced_value": 0,
                          "price_per_unit": 5.0}) == "skip"
    assert classify_item({}) == "skip"
    assert classify_item("nope") == "skip"


def test_classify_tp_when_within_tolerance():
    item = {"auto_priced_value": 10.00, "price_per_unit": 10.005}
    assert classify_item(item) == "tp"


def test_classify_fp_when_overridden():
    item = {"auto_priced_value": 10.00, "price_per_unit": 12.50}
    assert classify_item(item) == "fp"


def test_classify_fp_when_cleared_to_zero():
    item = {"auto_priced_value": 10.00, "price_per_unit": 0}
    assert classify_item(item) == "fp"
    item2 = {"auto_priced_value": 10.00, "price_per_unit": None}
    assert classify_item(item2) == "fp"


def test_compute_record_tp_fp_aggregates_per_source():
    record = {
        "id": "rfq_x",
        "status": "sent",
        "line_items": [
            {"auto_priced_value": 5.00, "price_per_unit": 5.00,
             "auto_priced_source": "catalog"},          # tp
            {"auto_priced_value": 8.00, "price_per_unit": 9.00,
             "auto_priced_source": "catalog"},          # fp
            {"auto_priced_value": 12.00, "price_per_unit": 12.00,
             "auto_priced_source": "amazon"},           # tp
            {"price_per_unit": 100.00},                  # skip
        ],
    }
    out = compute_record_tp_fp(record)
    assert out["rid"] == "rfq_x"
    assert out["status"] == "sent"
    assert out["auto_priced_count"] == 3
    assert out["tp"] == 2
    assert out["fp"] == 1
    assert out["tp_rate"] == pytest.approx(2 / 3, abs=1e-6)
    assert out["by_source"]["catalog"] == {"tp": 1, "fp": 1}
    assert out["by_source"]["amazon"] == {"tp": 1, "fp": 0}


def test_compute_record_tp_fp_empty_record():
    out = compute_record_tp_fp({"id": "x", "status": "draft", "line_items": []})
    assert out["auto_priced_count"] == 0
    assert out["tp_rate"] is None


def test_scan_records_filters_by_status_and_auto_priced():
    records = [
        {"id": "draft", "status": "draft",
         "line_items": [{"auto_priced_value": 5, "price_per_unit": 5}]},
        {"id": "sent_no_auto", "status": "sent",
         "line_items": [{"price_per_unit": 5}]},
        {"id": "sent_yes", "status": "sent",
         "line_items": [{"auto_priced_value": 5, "price_per_unit": 5}]},
        {"id": "won_yes", "status": "won",
         "line_items": [{"auto_priced_value": 5, "price_per_unit": 6}]},
    ]
    out = scan_records(records)
    rids = sorted(r["rid"] for r in out)
    assert rids == ["sent_yes", "won_yes"]


def test_summarise_jsonl_rolls_up_and_ignores_garbage(tmp_path):
    p = tmp_path / "log.jsonl"
    p.write_text(
        json.dumps({"tp": 3, "fp": 1,
                    "by_source": {"catalog": {"tp": 3, "fp": 1}}}) + "\n"
        + "\n"  # blank
        + "not json\n"
        + json.dumps({"tp": 1, "fp": 4,
                      "by_source": {"amazon": {"tp": 1, "fp": 4}}}) + "\n",
        encoding="utf-8",
    )
    out = summarise_jsonl(str(p))
    assert out["records"] == 2
    assert out["tp"] == 4
    assert out["fp"] == 5
    assert out["tp_rate"] == pytest.approx(4 / 9, abs=1e-6)
    assert out["by_source"]["catalog"] == {"tp": 3, "fp": 1}
    assert out["by_source"]["amazon"] == {"tp": 1, "fp": 4}


def test_summarise_jsonl_missing_file(tmp_path):
    out = summarise_jsonl(str(tmp_path / "nope.jsonl"))
    assert out == {"records": 0, "tp": 0, "fp": 0, "tp_rate": None,
                   "by_source": {}}


# ─────────────────── Endpoint tests ───────────────────

def test_scan_endpoint_requires_auth(anon_client):
    r = anon_client.post("/api/admin/auto-pricing-tp-fp/scan")
    assert r.status_code in (401, 302, 403)


def test_summary_endpoint_requires_auth(anon_client):
    r = anon_client.get("/api/admin/auto-pricing-tp-fp/summary")
    assert r.status_code in (401, 302, 403)


def test_scan_endpoint_appends_and_returns_rollup(client, monkeypatch, tmp_path):
    # Two RFQs: one sent w/ auto-priced items, one draft
    rfqs = {
        "rfq_kept": {
            "id": "rfq_kept", "status": "sent",
            "line_items": [
                {"auto_priced_value": 5.0, "price_per_unit": 5.0,
                 "auto_priced_source": "catalog"},
                {"auto_priced_value": 8.0, "price_per_unit": 10.0,
                 "auto_priced_source": "catalog"},
            ],
        },
        "rfq_draft": {
            "id": "rfq_draft", "status": "draft",
            "line_items": [
                {"auto_priced_value": 5.0, "price_per_unit": 5.0},
            ],
        },
    }
    monkeypatch.setattr("src.api.data_layer.load_rfqs", lambda: rfqs)

    log_file = tmp_path / "auto_tp_fp.jsonl"
    monkeypatch.setenv("AUTO_PRICING_TP_FP_LOG_PATH", str(log_file))

    r = client.post("/api/admin/auto-pricing-tp-fp/scan",
                    json={"include_pcs": False})
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["appended"] == 1                           # only sent record
    assert body["rfq"]["records"] == 1
    assert body["rfq"]["tp"] == 1
    assert body["rfq"]["fp"] == 1
    assert body["rfq"]["tp_rate"] == pytest.approx(0.5, abs=1e-6)
    assert body["pc"] is None

    # Verify JSONL got one row tagged
    lines = [ln for ln in log_file.read_text(encoding="utf-8").splitlines()
             if ln.strip()]
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["rid"] == "rfq_kept"
    assert row["_kind"] == "rfq"
    assert row["_scanned_at"]


def test_summary_endpoint_returns_jsonl_rollup(
        client, monkeypatch, tmp_path):
    log_file = tmp_path / "auto_tp_fp.jsonl"
    log_file.write_text(
        json.dumps({"tp": 7, "fp": 3,
                    "by_source": {"catalog": {"tp": 7, "fp": 3}}}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AUTO_PRICING_TP_FP_LOG_PATH", str(log_file))
    r = client.get("/api/admin/auto-pricing-tp-fp/summary")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["records"] == 1
    assert body["tp"] == 7
    assert body["fp"] == 3
    assert body["tp_rate"] == pytest.approx(0.7, abs=1e-6)
    assert body["by_source"]["catalog"] == {"tp": 7, "fp": 3}
