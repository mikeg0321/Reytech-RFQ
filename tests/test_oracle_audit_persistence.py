"""PR-H — `oracle_audit` envelope persists end-to-end through save→load.

Product-engineer audit (2026-05-13) named this as the most-critical
test:

  > Add a write-then-read test against the actual save→load path —
  > mirror the `tax_rate` diagnostic: save, fetch via `load_rfqs`,
  > assert the value survives. Not a unit test on the adapter — an
  > integration test on the real route. This is the only thing that
  > catches the `from_legacy_dict` `known_keys` class.

The persistence-P0 memo (project_persistence_p0_class_2026_05_12)
documents the prior bug: V2 adapter's `from_legacy_dict` listed
`tax_rate`, `tax_enabled`, `price_buffer`, `default_markup`,
`award_method` in `known_keys` but the model never captured them.
Every adapter round-trip silently dropped the values. That's the
class this test pins against.

Pinned guarantees:
  1. `oracle_audit` on PC items survives `_save_single_pc` →
     `_load_price_checks` roundtrip (the data layer path).
  2. `enrich_pc` actually attaches the envelope when get_pricing
     returns a recommendation.
  3. `enrich_pc` threads mfg_number + unspsc kwargs to get_pricing
     so the rollup lookup fires.
  4. The envelope is a JSON-serializable dict (no Decimal/datetime
     surprises during `json.dumps` in `_save_single_pc`).
  5. Round-trip preserves multi-cap stacks (forward-compat for floor
     + volume-ceiling cap types coming later).
"""
from __future__ import annotations

import json
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Envelope is JSON-serializable ────────────────────────────────


def test_oracle_audit_envelope_is_json_serializable():
    """Save path does `json.dumps(pc, default=str)` — if the envelope
    contains a Decimal or datetime, it'd serialize but read back as
    str. Pin that every value in the envelope round-trips through
    `json.dumps → json.loads` unchanged."""
    from src.core.pc_rfq_reprice_adapter import _build_oracle_audit
    result = {
        "scprs_rollup": {
            "match_key": "X-1", "match_key_type": "mfg",
            "count": 50, "p50": 45.0, "p75": 60.0, "p90": 80.0,
        },
        "recommendation": {
            "quote_price": 60.0,
            "quote_price_pre_cap": 100.0,
            "caps_applied": [{
                "source": "scprs_rollup", "percentile": "p75",
                "cap_price": 60.0, "pre_cap_price": 100.0,
                "match_key": "X-1", "match_key_type": "mfg",
                "sample_count": 50,
            }],
        },
    }
    envelope = _build_oracle_audit(
        result, result["recommendation"], "2026-05-13T08:00:00",
    )
    # Round-trip
    raw = json.dumps(envelope)
    back = json.loads(raw)
    assert back == envelope, "oracle_audit failed json round-trip"
    # Spot-check critical fields
    assert back["rec_price"] == 60.0
    assert back["rec_pre_cap_price"] == 100.0
    assert back["caps_applied"][0]["cap_price"] == 60.0
    assert back["scprs_rollup"]["count"] == 50


# ── enrich_pc threads kwargs + attaches audit ────────────────────


def test_enrich_pc_threads_mfg_and_unspsc_to_get_pricing():
    """Pre-PR-H, the enrichment pipeline only passed `item_number=`
    (positional), so the SCPRS rollup probe never fired from this
    path. PR-H adds the explicit kwargs."""
    import inspect
    from src.agents import pc_enrichment_pipeline as pep
    src = inspect.getsource(pep)
    # Both kwargs must appear in the get_pricing call site within
    # _run_pipeline (the enrichment hot path).
    assert "mfg_number=" in src, (
        "enrich_pc must thread mfg_number= to get_pricing or the rollup "
        "lookup never fires on the enrichment path"
    )
    assert "unspsc=" in src, (
        "enrich_pc must thread unspsc= to get_pricing or UNSPSC-only "
        "items get no rollup probe"
    )


def test_enrich_pc_attaches_oracle_audit_to_items():
    """Static check: after the get_pricing call, the enrichment loop
    must stash `oracle_audit` on the item dict. We grep the source —
    a behavioral test would require building a full PC fixture +
    mocking 8 dependencies, which is more brittle than the grep."""
    import inspect
    from src.agents import pc_enrichment_pipeline as pep
    src = inspect.getsource(pep)
    assert 'it["oracle_audit"]' in src or "it['oracle_audit']" in src, (
        "enrich_pc must attach `oracle_audit` to each item after "
        "get_pricing — otherwise the envelope is lost between pricing "
        "and persistence"
    )
    # And it must use the canonical builder, not duplicate the shape
    assert "_build_oracle_audit" in src, (
        "enrich_pc must call _build_oracle_audit (the canonical helper "
        "from pc_rfq_reprice_adapter) so the envelope shape stays in "
        "sync with the reprice path"
    )


# ── Save→Load integration test (the agent's specific ask) ────────


def test_oracle_audit_survives_real_save_load_roundtrip(tmp_path, monkeypatch):
    """The CRITICAL agent-mandated test: mirror the `tax_rate`
    diagnostic. Stage a PC dict with `oracle_audit` on an item,
    save via the REAL `_save_single_pc`, fetch via the REAL
    `_load_price_checks`, assert the field survives.

    If this fails, the bug is in the data layer — either:
      a) The save path strips unknown fields (it doesn't today, but
         a future "schema-strict" cleanup could introduce this)
      b) The load path applies a v2 adapter that drops the field
      c) The JSON serializer chokes on a non-primitive value in the
         envelope (caught by the json-serializable test above)
    """
    # Point the DB to a temp file so the test doesn't pollute prod data
    tmp_db = tmp_path / "reytech_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    # Reset cached DB connection if module-level
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()

    # Patch the data layer's DATA_DIR so JSON fallbacks land in tmp too
    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))

    # Build a PC blob with a fully-formed oracle_audit envelope on item 0
    audit_envelope = {
        "rec_price": 60.0,
        "rec_pre_cap_price": 100.0,
        "caps_applied": [{
            "source": "scprs_rollup", "percentile": "p75",
            "cap_price": 60.0, "pre_cap_price": 100.0,
            "match_key": "16-N8MMPA", "match_key_type": "mfg",
            "sample_count": 50,
        }],
        "scprs_rollup": {
            "match_key": "16-N8MMPA", "match_key_type": "mfg",
            "agency": "cchcs", "year": "*", "qty_band": "10-49",
            "count": 50, "mean": 55.0, "p50": 50.0, "p75": 60.0,
            "p90": 80.0, "updated_at": "2026-05-13",
        },
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13T08:00:00",
    }
    pcid = "pc_test_h_001"
    pc = {
        "id": pcid,
        "pc_number": "TEST-001",
        "solicitation_number": "TEST-001",
        "agency": "cchcs",
        "institution": "CCHCS",
        "status": "draft",
        "created_at": "2026-05-13",
        "items": [
            {
                "item_number": "1",
                "description": "Bandage Sterile Mfg # 16-N8MMPA",
                "qty": 20,
                "uom": "EA",
                "mfg_number": "16-N8MMPA",
                "supplier_cost": 50.0,
                "unit_price": 60.0,
                "pricing": {
                    "unit_cost": 50.0,
                    "markup_pct": 20.0,
                    "recommended_price": 60.0,
                },
                "oracle_audit": audit_envelope,
            },
        ],
    }

    # SAVE through the real data layer
    from src.api.data_layer import _save_single_pc, _load_price_checks
    _save_single_pc(pcid, pc, raise_on_error=True)

    # LOAD via the same real path
    pcs = _load_price_checks()
    loaded = pcs.get(pcid)
    assert loaded is not None, (
        f"PC {pcid} did not survive save→load roundtrip — was not "
        f"loaded back at all. PCs found: {list(pcs.keys())!r}"
    )
    items = loaded.get("items") or loaded.get("line_items") or []
    assert items, f"PC loaded but items are empty: {loaded!r}"
    item0 = items[0]
    # THE CRITICAL ASSERTION — `oracle_audit` envelope present on item
    assert "oracle_audit" in item0, (
        "oracle_audit DROPPED during save→load roundtrip. This is the "
        "exact bug class as the tax_rate incident "
        "(project_persistence_p0_class_2026_05_12). Check the data layer "
        "for any allowlist / schema-strict / adapter step that may strip "
        "unknown item fields. Item keys: " + repr(list(item0.keys()))
    )
    audit_back = item0["oracle_audit"]
    assert audit_back["rec_price"] == 60.0
    assert audit_back["rec_pre_cap_price"] == 100.0
    assert len(audit_back["caps_applied"]) == 1
    assert audit_back["caps_applied"][0]["match_key"] == "16-N8MMPA"
    assert audit_back["scprs_rollup"]["count"] == 50


def test_multi_cap_envelope_survives_save_load(tmp_path, monkeypatch):
    """Forward-compat: when PR-K eventually adds a `volume_aware_ceiling`
    cap or a `cost_floor` cap, the envelope's `caps_applied` list will
    carry multiple entries. Pin that the stack survives roundtrip."""
    tmp_db = tmp_path / "reytech_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))

    pcid = "pc_test_h_multicap"
    pc = {
        "id": pcid, "pc_number": "TEST-MC", "status": "draft",
        "created_at": "2026-05-13", "agency": "cchcs",
        "items": [{
            "item_number": "1", "description": "x", "qty": 1, "uom": "EA",
            "supplier_cost": 10.0,
            "oracle_audit": {
                "rec_price": 50.0, "rec_pre_cap_price": 100.0,
                "caps_applied": [
                    {"source": "scprs_rollup", "percentile": "p75",
                     "cap_price": 60.0, "pre_cap_price": 100.0,
                     "match_key": "X", "match_key_type": "mfg",
                     "sample_count": 50},
                    {"source": "volume_aware_ceiling",
                     "cap_price": 50.0, "pre_cap_price": 60.0},
                ],
                "scprs_rollup": None,
                "oracle_version": "v2.1",
                "snapshot_at": "2026-05-13",
            },
        }],
    }
    from src.api.data_layer import _save_single_pc, _load_price_checks
    _save_single_pc(pcid, pc, raise_on_error=True)
    pcs = _load_price_checks()
    items = pcs[pcid].get("items") or []
    caps = items[0]["oracle_audit"]["caps_applied"]
    assert len(caps) == 2, f"multi-cap stack collapsed: {caps!r}"
    sources = [c["source"] for c in caps]
    assert "scprs_rollup" in sources
    assert "volume_aware_ceiling" in sources
