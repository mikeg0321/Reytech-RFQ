"""PR-G — `oracle_audit` snapshot must survive the reprice adapter.

The product-engineer audit (2026-05-13) flagged this as the substrate
bug behind the WR measurement loop:

  > `pc_rfq_reprice_adapter.oracle_pricer_for_line` returns a 4-field
  > allowlist `{supplier_cost, unit_price, bid_price, markup_pct}`.
  > Your `caps_applied` and `quote_price_pre_cap` die at that boundary
  > on every PC→RFQ qty-changed reprice. PR-G as written will look
  > like it works on first-quote pricing but show NULL on every
  > repriced line. This is the substrate bug, not the column.

This file pins:
  1. The pricer emits an `oracle_audit` envelope (cap audit + rollup)
  2. The envelope survives the `_VERBATIM_PRICE_FIELDS` allowlist when
     `reprice_qty_changed_lines` runs end-to-end
  3. The pricer threads `mfg_number=` + `unspsc=` to `get_pricing`
     so the rollup lookup actually fires on this path
  4. Defensive: pricer failures don't crash repricing
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Pricer emits the audit envelope ──────────────────────────────


def test_pricer_returns_oracle_audit_envelope():
    """`oracle_pricer_for_line` must include `oracle_audit` in its
    output dict with the cap+rollup snapshot shape."""
    from src.core import pc_rfq_reprice_adapter as adapter
    with patch.object(adapter, "get_pricing", create=True) as _mock_gp:
        _mock_gp.return_value = {
            "scprs_rollup": {
                "match_key": "X-1", "match_key_type": "mfg",
                "count": 50, "p75": 60.0, "p50": 45.0,
            },
            "cost": {"locked_cost": 40.0},
            "recommendation": {
                "quote_price": 60.0,
                "quote_price_pre_cap": 100.0,
                "markup_pct": 50.0,
                "caps_applied": [{
                    "source": "scprs_rollup",
                    "percentile": "p75",
                    "cap_price": 60.0,
                    "pre_cap_price": 100.0,
                    "match_key": "X-1",
                    "match_key_type": "mfg",
                    "sample_count": 50,
                }],
            },
        }
        # Mock the get_pricing imported INSIDE the function too
        with patch("src.core.pricing_oracle_v2.get_pricing",
                   _mock_gp.return_value.__class__) if False else \
             patch("src.core.pricing_oracle_v2.get_pricing",
                   return_value=_mock_gp.return_value):
            out = adapter.oracle_pricer_for_line(
                {"description": "Bandage Sterile", "qty": 20,
                 "mfg_number": "X-1", "supplier_cost": 40.0},
                agency="cchcs",
            )
    assert out is not None
    assert "oracle_audit" in out, (
        "pricer MUST emit oracle_audit envelope; got keys: "
        f"{list(out.keys())!r}"
    )
    audit = out["oracle_audit"]
    assert audit["rec_price"] == 60.0
    assert audit["rec_pre_cap_price"] == 100.0
    assert len(audit["caps_applied"]) == 1
    assert audit["caps_applied"][0]["source"] == "scprs_rollup"
    assert audit["scprs_rollup"]["count"] == 50
    assert audit["oracle_version"] == "v2.1"
    assert "snapshot_at" in audit


def test_pricer_emits_audit_even_when_no_cap_fired():
    """Audit is the proof-of-consideration. Even when no cap fires
    (no rollup data, or rec at/below cap), the audit envelope must
    still be present so we can distinguish "oracle ran, didn't cap"
    from "oracle never ran." `caps_applied` is `[]` in that case."""
    from src.core import pc_rfq_reprice_adapter as adapter
    rec_no_cap = {
        "scprs_rollup": None,
        "cost": {"locked_cost": 40.0},
        "recommendation": {
            "quote_price": 60.0,
            "markup_pct": 50.0,
            # no caps_applied
        },
    }
    with patch("src.core.pricing_oracle_v2.get_pricing",
               return_value=rec_no_cap):
        out = adapter.oracle_pricer_for_line(
            {"description": "Bandage", "qty": 5, "supplier_cost": 40.0},
            agency="cchcs",
        )
    assert out is not None
    assert "oracle_audit" in out
    assert out["oracle_audit"]["caps_applied"] == []
    assert out["oracle_audit"]["scprs_rollup"] is None
    assert out["oracle_audit"]["rec_price"] == 60.0


# ── Audit survives the allowlist ─────────────────────────────────


def test_oracle_audit_in_verbatim_allowlist():
    """The allowlist in pc_rfq_linker.py must include `oracle_audit`
    so the field survives `reprice_qty_changed_lines`. Without this
    entry, the pricer's audit envelope is silently dropped."""
    from src.core.pc_rfq_linker import _VERBATIM_PRICE_FIELDS
    assert "oracle_audit" in _VERBATIM_PRICE_FIELDS, (
        f"`oracle_audit` missing from _VERBATIM_PRICE_FIELDS: "
        f"{_VERBATIM_PRICE_FIELDS!r}. Without it, the reprice adapter "
        f"silently discards the cap audit on every PC→RFQ qty-changed "
        f"reprice — measurement substrate goes dark on repriced lines."
    )


def test_audit_survives_reprice_qty_changed_lines():
    """The CRITICAL end-to-end test: stage an RFQ with a qty_changed
    line, run the actual reprice helper with a pricer that returns
    `oracle_audit`, confirm the field is on the line after.

    This is the test that would have caught the leak before PR-G."""
    from src.core.pc_rfq_linker import reprice_qty_changed_lines

    rfq_data = {
        "line_items": [
            {
                "description": "Bandage Sterile",
                "qty": 20,
                "qty_changed": True,
                "supplier_cost": 40.0,
                "mfg_number": "X-1",
            },
            {
                "description": "Untouched line",
                "qty": 5,
                "qty_changed": False,
                "unit_price": 10.0,
                # No oracle_audit on this line — proves we don't fabricate one
            },
        ],
    }

    # Stub pricer returns the audit envelope
    audit_envelope = {
        "rec_price": 60.0,
        "rec_pre_cap_price": 100.0,
        "caps_applied": [{
            "source": "scprs_rollup", "percentile": "p75",
            "cap_price": 60.0, "pre_cap_price": 100.0,
            "match_key": "X-1", "match_key_type": "mfg",
            "sample_count": 50,
        }],
        "scprs_rollup": {"count": 50, "p75": 60.0, "match_key": "X-1"},
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13T08:00:00",
    }

    def stub_pricer(line):
        return {
            "supplier_cost": 40.0,
            "unit_price": 60.0,
            "bid_price": 60.0,
            "markup_pct": 50.0,
            "oracle_audit": audit_envelope,
        }

    result = reprice_qty_changed_lines(rfq_data, stub_pricer)
    assert result["repriced"] == 1
    assert result["skipped_no_change"] == 1

    # CRITICAL: audit must be on the repriced line
    repriced_line = rfq_data["line_items"][0]
    assert "oracle_audit" in repriced_line, (
        "oracle_audit was DROPPED by the reprice adapter allowlist — "
        "this is the substrate bug PR-G fixes. Check that "
        "_VERBATIM_PRICE_FIELDS in pc_rfq_linker.py includes 'oracle_audit'."
    )
    assert repriced_line["oracle_audit"]["rec_pre_cap_price"] == 100.0
    assert len(repriced_line["oracle_audit"]["caps_applied"]) == 1

    # Untouched line must NOT gain a fabricated audit
    untouched_line = rfq_data["line_items"][1]
    assert "oracle_audit" not in untouched_line


def test_audit_envelope_can_carry_multiple_cap_records():
    """The envelope is forward-compatible: future cap types (floor,
    volume-ceiling, etc.) append to `caps_applied`. Pin that the
    allowlist preserves multi-cap envelopes too."""
    from src.core.pc_rfq_linker import reprice_qty_changed_lines

    rfq_data = {"line_items": [{
        "description": "x", "qty": 1, "qty_changed": True,
    }]}
    multi_cap_audit = {
        "rec_price": 50.0, "rec_pre_cap_price": 100.0,
        "caps_applied": [
            {"source": "scprs_rollup", "percentile": "p75",
             "cap_price": 60.0, "pre_cap_price": 100.0,
             "match_key": "X-1", "match_key_type": "mfg", "sample_count": 50},
            {"source": "volume_aware_ceiling", "cap_price": 50.0,
             "pre_cap_price": 60.0},
        ],
        "scprs_rollup": None,
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13",
    }
    reprice_qty_changed_lines(rfq_data, lambda L: {
        "unit_price": 50.0, "oracle_audit": multi_cap_audit,
    })
    assert len(rfq_data["line_items"][0]["oracle_audit"]["caps_applied"]) == 2


# ── Pricer threads MFG#/UNSPSC kwargs to rollup-enabled get_pricing ──


def test_pricer_threads_mfg_number_kwarg_to_get_pricing():
    """Pre-PR-G the pricer only passed `item_number=` (positional);
    PR-E added `mfg_number=` + `unspsc=` to get_pricing's signature
    to trigger the rollup lookup. The pricer MUST pass them through
    or the rollup lookup never fires on the PC→RFQ reprice path."""
    from src.core import pc_rfq_reprice_adapter as adapter
    captured_kwargs = {}

    def fake_get_pricing(**kwargs):
        captured_kwargs.update(kwargs)
        return {
            "scprs_rollup": None,
            "cost": {},
            "recommendation": {"quote_price": 10.0, "markup_pct": 25.0},
        }

    with patch("src.core.pricing_oracle_v2.get_pricing", fake_get_pricing):
        adapter.oracle_pricer_for_line(
            {"description": "X", "qty": 5, "mfg_number": "X-1",
             "unspsc": "42143000", "supplier_cost": 8.0},
            agency="cchcs",
        )
    assert captured_kwargs.get("mfg_number") == "X-1", (
        "pricer must thread mfg_number= to get_pricing — without it the "
        "SCPRS rollup lookup never fires on the reprice path"
    )
    assert captured_kwargs.get("unspsc") == "42143000"


# ── Defensive — pricer failures don't crash ───────────────────────


def test_pricer_returns_none_on_get_pricing_exception():
    """If get_pricing raises, the pricer must return None (so the
    helper counts it as `skipped_no_price`) rather than propagating —
    a drifted line WRONG is worse than un-repriced."""
    from src.core import pc_rfq_reprice_adapter as adapter
    with patch("src.core.pricing_oracle_v2.get_pricing",
               side_effect=Exception("simulated outage")):
        out = adapter.oracle_pricer_for_line(
            {"description": "X", "qty": 5, "supplier_cost": 10.0},
            agency="cchcs",
        )
    assert out is None


def test_pricer_returns_none_when_no_quote_price():
    """get_pricing succeeded but couldn't recommend a price → skip,
    don't fabricate. No audit emitted on a None return."""
    from src.core import pc_rfq_reprice_adapter as adapter
    with patch("src.core.pricing_oracle_v2.get_pricing", return_value={
        "scprs_rollup": None,
        "cost": {},
        "recommendation": {"quote_price": None},
    }):
        out = adapter.oracle_pricer_for_line(
            {"description": "X", "qty": 5, "supplier_cost": 10.0},
            agency="cchcs",
        )
    assert out is None
