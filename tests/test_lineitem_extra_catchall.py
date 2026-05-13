"""PR-J prereq — LineItem.extra catch-all closes the persistence-P0
class at line level.

The 2026-05-12 tax_rate incident
(project_persistence_p0_class_2026_05_12) showed that any field the
V2 quote model didn't explicitly capture would silently die on every
adapter round-trip. That fix landed `Quote.extra` for top-level keys.

But `LineItem` had no equivalent — so any item-level field outside
the typed schema (oracle_audit from PR-G/H/I, scprs_match, match
feedback, future cap envelopes) would still get eaten the moment
something ran `adapt_pc()` on a PC that had them.

With `quote_model_v2_enabled=True` in prod and `_pricecheck_detail_inner`
calling `adapt_pc` on every PC detail GET, this was a latent
landmine — the moment any path saved an adapted PC back to disk, the
oracle audit substrate would null out across the whole DB.

Pinned guarantees:
  1. `oracle_audit` envelope on a PC item survives a full
     `Quote.from_legacy_dict → to_legacy_dict` round-trip.
  2. Same for `adapt_pc()` with the V2 adapter flag forced ON.
  3. Same for RFQ items via `adapt_rfq()`.
  4. Multiple line-level unknowns ride through together (forward-compat).
  5. Typed LineItem fields are NOT shadowed by stale extras (no
     `unit_price` from extras overwriting the freshly-computed one).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


_AUDIT = {
    "rec_price": 60.0,
    "rec_pre_cap_price": 100.0,
    "caps_applied": [{
        "source": "scprs_rollup", "percentile": "p75",
        "cap_price": 60.0, "pre_cap_price": 100.0,
        "match_key": "16-N8MMPA", "match_key_type": "mfg",
        "sample_count": 50,
    }],
    "scprs_rollup": {"count": 50, "p75": 60.0},
    "oracle_version": "v2.1",
    "snapshot_at": "2026-05-13T08:00:00",
}


def test_quote_model_roundtrip_preserves_item_oracle_audit():
    """Direct model round-trip: legacy dict → Quote → legacy dict.
    This is the FAILING test pre-fix — LineItem had no extra dict so
    oracle_audit died on `from_legacy_dict`."""
    from src.core.quote_model import Quote
    pc = {
        "id": "pc1", "pc_number": "PC-1",
        "agency": "cchcs", "status": "draft",
        "items": [{
            "item_number": "1", "description": "Bandage",
            "qty": 20, "uom": "EA",
            "mfg_number": "16-N8MMPA",
            "unit_cost": 50.0, "unit_price": 60.0,
            "pricing": {"unit_cost": 50.0, "markup_pct": 20.0},
            "oracle_audit": _AUDIT,
        }],
    }
    quote = Quote.from_legacy_dict(pc, doc_type="pc")
    back = quote.to_legacy_dict()
    items = back.get("items") or back.get("line_items") or []
    assert items, f"items lost on round-trip: {back!r}"
    audit_back = items[0].get("oracle_audit")
    assert audit_back is not None, (
        "oracle_audit DROPPED on Quote round-trip — LineItem.extra "
        "not capturing line-level unknowns. Item keys: "
        + repr(list(items[0].keys()))
    )
    assert audit_back["rec_price"] == 60.0
    assert audit_back["caps_applied"][0]["match_key"] == "16-N8MMPA"


def test_adapt_pc_preserves_item_oracle_audit(monkeypatch):
    """End-to-end adapter test with V2 flag forced ON. Mirrors the
    prod read path that runs on every PC detail GET."""
    # Force the V2 adapter flag on regardless of DB state.
    import src.core.quote_adapter as qa
    monkeypatch.setattr(qa, "_is_enabled", lambda: True)
    pc = {
        "id": "pc1", "pc_number": "PC-1",
        "agency": "cchcs", "status": "draft",
        "items": [{
            "item_number": "1", "description": "Bandage",
            "qty": 20, "uom": "EA", "mfg_number": "16-N8MMPA",
            "unit_cost": 50.0,
            "pricing": {"unit_cost": 50.0, "markup_pct": 20.0},
            "oracle_audit": _AUDIT,
        }],
    }
    adapted = qa.adapt_pc(pc, "pc1")
    items = adapted.get("items") or adapted.get("line_items") or []
    assert items
    audit = items[0].get("oracle_audit")
    assert audit is not None, (
        "adapt_pc stripped oracle_audit — substrate landmine open."
    )
    assert audit["rec_price"] == 60.0


def test_adapt_rfq_preserves_item_oracle_audit(monkeypatch):
    import src.core.quote_adapter as qa
    monkeypatch.setattr(qa, "_is_enabled", lambda: True)
    rfq = {
        "id": "rfq1", "rfq_number": "RFQ-1",
        "agency": "cdcr", "status": "draft",
        "items": [{
            "item_number": "1", "description": "Gloves",
            "qty": 10, "uom": "EA", "mfg_number": "X-1",
            "unit_cost": 5.0, "oracle_audit": _AUDIT,
        }],
    }
    adapted = qa.adapt_rfq(rfq, "rfq1")
    items = adapted.get("items") or adapted.get("line_items") or []
    assert items
    assert items[0].get("oracle_audit") is not None


def test_multiple_line_extras_survive_together():
    """Forward-compat: any item-level dict the adapter doesn't know
    about must ride through verbatim. Pins that future fields
    (volume_cap_audit, etc.) work without re-touching the adapter."""
    from src.core.quote_model import Quote
    pc = {
        "id": "pc1", "pc_number": "PC-1", "status": "draft",
        "items": [{
            "item_number": "1", "description": "X",
            "qty": 1, "uom": "EA",
            "unit_cost": 5.0,
            "oracle_audit": _AUDIT,
            "match_feedback_block": ["some_token"],
            "volume_cap_audit": {"trigger_qty": 100, "applied": False},
        }],
    }
    quote = Quote.from_legacy_dict(pc, doc_type="pc")
    back = quote.to_legacy_dict()
    item = (back.get("items") or [])[0]
    assert item.get("oracle_audit") is not None
    assert item.get("match_feedback_block") == ["some_token"]
    assert item.get("volume_cap_audit", {}).get("trigger_qty") == 100


def test_typed_fields_win_over_stale_extras_on_roundtrip():
    """Defensive: a future bug where typed-field aliases (unit_price,
    extension) accidentally land in extras must not overwrite the
    freshly-computed model output. Without this guard, a stale
    cached unit_price=99.99 from extras would shadow the model's
    cost*markup math (e.g. after a markup edit)."""
    from src.core.quote_model import Quote
    pc = {
        "id": "pc1", "pc_number": "PC-1", "status": "draft",
        "items": [{
            "item_number": "1", "description": "X",
            "qty": 1, "uom": "EA",
            "unit_cost": 50.0,
            "pricing": {"unit_cost": 50.0, "markup_pct": 20.0},
            # If `unit_price` accidentally landed in extras, this
            # bogus value would shadow the real computed price.
            "unit_price": 9999.99,
            "oracle_audit": _AUDIT,
        }],
    }
    quote = Quote.from_legacy_dict(pc, doc_type="pc")
    back = quote.to_legacy_dict()
    item = (back.get("items") or [])[0]
    # Model computes unit_price = cost(50) * (1 + 20/100) = 60.0
    assert float(item["unit_price"]) == 60.0, (
        f"typed unit_price shadowed by extras: got {item['unit_price']}"
    )
    # But oracle_audit (the real unknown) still survives
    assert item.get("oracle_audit") is not None
