"""Pin: G6 step 1 — canonical Spine-first catalog read primitive.

Chrome MCP audit 2026-05-27 / G6 (Architect approval). Substrate-
singleness defect class fix at the READ layer. New
`unified_catalog.lookup_by_mfg` is Spine-first, legacy-fallback.

Tests pin:
  1. Spine has the MFG# → returns Spine result with source='spine'
  2. Spine doesn't have it but legacy does → returns legacy result
     with source='legacy'
  3. Both empty → returns None
  4. Spine has entry but with no cost → falls through to legacy
     (the URL→catalog write-through can observe identity without
     cost; that's not useful for cost-lookup callers)
  5. prefer='spine_only' skips legacy fallback
  6. prefer='legacy_only' skips spine
  7. prefer='legacy' tries legacy first
  8. Empty/None MFG# without UPC → None
"""
from __future__ import annotations

from datetime import datetime, timezone


def _seed_spine(tmp_path, monkeypatch, mfg_number, cost_cents=1500,
                description="Seeded item"):
    db = str(tmp_path / "spine.db")
    monkeypatch.setenv("SPINE_DB_PATH", db)
    from src.spine.db import init_db
    init_db(db)
    from src.spine.catalog import observe
    observe(
        db,
        mfg_number=mfg_number,
        description=description,
        cost_cents=cost_cents,
        actor="test_seed",
    )
    return db


def _mock_legacy_find(monkeypatch, result):
    """Replace legacy find_by_mfg_exact with a constant return."""
    monkeypatch.setattr(
        "src.agents.product_catalog.find_by_mfg_exact",
        lambda mfg, upc=None: result,
    )


# ─── Happy paths ────────────────────────────────────────────────────


def test_spine_hit_returns_spine_source(tmp_path, monkeypatch):
    _seed_spine(tmp_path, monkeypatch, "MFG-SPINE-1", cost_cents=2000)
    _mock_legacy_find(monkeypatch, None)
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-SPINE-1")
    assert res is not None
    assert res["source"] == "spine"
    assert res["cost_cents"] == 2000
    assert res["cost_dollars"] == 20.0


def test_falls_through_to_legacy_when_spine_empty(tmp_path, monkeypatch):
    """Spine DB exists but doesn't have this MFG# → legacy fallback."""
    _seed_spine(tmp_path, monkeypatch, "MFG-DIFFERENT", cost_cents=100)
    _mock_legacy_find(monkeypatch, {
        "id": 42,
        "mfg_number": "MFG-LEGACY-1",
        "cost": 35.50,
        "name": "Legacy item",
        "uom": "EA",
        "cost_source": "operator",
        "times_quoted": 3,
    })
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-LEGACY-1")
    assert res is not None
    assert res["source"] == "legacy"
    assert res["cost_cents"] == 3550
    assert res["cost_dollars"] == 35.50
    assert res["legacy_cost_source"] == "operator"


def test_both_empty_returns_none(tmp_path, monkeypatch):
    _seed_spine(tmp_path, monkeypatch, "MFG-X", cost_cents=100)
    _mock_legacy_find(monkeypatch, None)
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-NOWHERE")
    assert res is None


def test_spine_entry_without_cost_falls_to_legacy(tmp_path, monkeypatch):
    """A Spine row created by URL→catalog write-through that observed
    identity but no price would have last_priced_cents=None. Skip it
    and try legacy."""
    db = str(tmp_path / "spine.db")
    monkeypatch.setenv("SPINE_DB_PATH", db)
    from src.spine.db import init_db
    init_db(db)
    from src.spine.catalog import observe
    # Observe with NO cost_cents → row exists but last_priced_cents is None
    observe(
        db, mfg_number="MFG-IDENTITY-ONLY",
        description="Seeded with identity but no price",
        actor="test_seed",
    )
    _mock_legacy_find(monkeypatch, {
        "id": 1, "mfg_number": "MFG-IDENTITY-ONLY", "cost": 12.34,
        "name": "From legacy", "cost_source": "operator",
    })
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-IDENTITY-ONLY")
    assert res is not None
    assert res["source"] == "legacy", (
        "Spine row without cost should not satisfy a cost-lookup caller"
    )
    assert res["cost_cents"] == 1234


# ─── prefer= parameter ──────────────────────────────────────────────


def test_spine_only_skips_legacy(tmp_path, monkeypatch):
    _seed_spine(tmp_path, monkeypatch, "MFG-X", cost_cents=100)
    _mock_legacy_find(monkeypatch, {
        "mfg_number": "MFG-IN-LEGACY", "cost": 5.0,
        "cost_source": "operator", "name": "legacy only",
    })
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-IN-LEGACY", prefer="spine_only")
    assert res is None


def test_legacy_only_skips_spine(tmp_path, monkeypatch):
    _seed_spine(tmp_path, monkeypatch, "MFG-IN-SPINE", cost_cents=999)
    _mock_legacy_find(monkeypatch, None)
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-IN-SPINE", prefer="legacy_only")
    assert res is None


def test_prefer_legacy_tries_legacy_first(tmp_path, monkeypatch):
    """When both have data, prefer='legacy' returns the legacy row."""
    _seed_spine(tmp_path, monkeypatch, "MFG-IN-BOTH", cost_cents=2000)
    _mock_legacy_find(monkeypatch, {
        "mfg_number": "MFG-IN-BOTH", "cost": 99.99,
        "cost_source": "operator", "name": "from legacy",
    })
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    res = lookup_by_mfg("MFG-IN-BOTH", prefer="legacy")
    assert res["source"] == "legacy"
    assert res["cost_cents"] == 9999


# ─── Validation ─────────────────────────────────────────────────────


def test_empty_mfg_without_upc_returns_none():
    from src.spine_bridge.unified_catalog import lookup_by_mfg
    assert lookup_by_mfg("") is None
    assert lookup_by_mfg(None) is None
    assert lookup_by_mfg("   ") is None
