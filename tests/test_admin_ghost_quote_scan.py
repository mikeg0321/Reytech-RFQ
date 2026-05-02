"""Regression: admin scan + clear endpoints for ghost-bound quote_numbers.

Pin the contract so the scan never falsely flags clean RFQs and the
clear path never touches a clean binding.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _seed(temp_data_dir, rfqs):
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rfqs, f)


def test_scan_returns_zero_when_no_quote_numbers(client, temp_data_dir):
    _seed(temp_data_dir, {
        "rfq_a": {
            "id": "rfq_a",
            "solicitation_number": "WORKSHEET",
            "line_items": [{"qty": 1}],
            # No reytech_quote_number — should NOT appear in either bucket
        }
    })
    resp = client.get("/api/admin/scan-ghost-quote-bindings")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["ghost_count"] == 0
    assert data["clean_count"] == 0


def test_scan_flags_placeholder_sol(client, temp_data_dir):
    """The exact incident shape — placeholder sol# + bound quote_number."""
    _seed(temp_data_dir, {
        "rfq_ghost": {
            "id": "rfq_ghost",
            "solicitation_number": "WORKSHEET",
            "reytech_quote_number": "R26Q45",
            "line_items": [{"qty": 1, "description": "x"}],
            "requestor_email": "buyer@calvet.ca.gov",
        },
    })
    resp = client.get("/api/admin/scan-ghost-quote-bindings")
    data = resp.get_json()
    assert data["ghost_count"] == 1
    assert data["ghost_bound"][0]["quote_number"] == "R26Q45"
    assert any("placeholder" in r.lower() for r in data["ghost_bound"][0]["reasons"])


def test_scan_passes_clean_binding(client, temp_data_dir):
    """A real CalVet RFQ with a real sol# + real items + real buyer must
    NOT appear in ghost_bound — only in clean_bound."""
    _seed(temp_data_dir, {
        "rfq_clean": {
            "id": "rfq_clean",
            "solicitation_number": "8955-00001234",
            "reytech_quote_number": "R26Q12",
            "line_items": [{"qty": 5, "description": "Real item"}],
            "requestor_email": "keith.alsing@calvet.ca.gov",
        },
    })
    resp = client.get("/api/admin/scan-ghost-quote-bindings")
    data = resp.get_json()
    assert data["clean_count"] == 1
    assert data["ghost_count"] == 0
    assert data["clean_bound"][0]["quote_number"] == "R26Q12"


def test_clear_dry_run_does_not_mutate(client, temp_data_dir):
    _seed(temp_data_dir, {
        "rfq_ghost": {
            "id": "rfq_ghost",
            "solicitation_number": "WORKSHEET",
            "reytech_quote_number": "R26Q45",
            "line_items": [{"qty": 1, "description": "x"}],
        },
    })
    resp = client.post("/api/admin/clear-ghost-quote-bindings",
                       json={"dry_run": True})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["dry_run"] is True
    assert data["count"] == 1
    # Re-read file: quote_number must STILL be set
    with open(os.path.join(temp_data_dir, "rfqs.json")) as f:
        after = json.load(f)
    assert after["rfq_ghost"]["reytech_quote_number"] == "R26Q45"


def test_clear_releases_only_ghost_bindings(client, temp_data_dir):
    _seed(temp_data_dir, {
        "rfq_ghost": {
            "id": "rfq_ghost",
            "solicitation_number": "WORKSHEET",
            "reytech_quote_number": "R26Q45",
            "line_items": [{"qty": 1, "description": "x"}],
        },
        "rfq_clean": {
            "id": "rfq_clean",
            "solicitation_number": "8955-00001234",
            "reytech_quote_number": "R26Q12",
            "line_items": [{"qty": 5, "description": "Real item"}],
            "requestor_email": "buyer@calvet.ca.gov",
        },
    })
    resp = client.post("/api/admin/clear-ghost-quote-bindings", json={})
    data = resp.get_json()
    assert data["count"] == 1, f"expected 1 ghost cleared, got {data!r}"

    with open(os.path.join(temp_data_dir, "rfqs.json")) as f:
        after = json.load(f)
    # Ghost cleared, clean preserved
    assert after["rfq_ghost"]["reytech_quote_number"] == ""
    assert after["rfq_clean"]["reytech_quote_number"] == "R26Q12"
