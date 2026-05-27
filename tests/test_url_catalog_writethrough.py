"""Pin: URL lookup → Spine catalog write-through (Pillar 3 / G9).

Chrome MCP audit 2026-05-26 follow-on: every successful
`item_link_lookup.lookup_from_url` was a one-shot — the catalog
never got richer over time. This module bridges them.

Tests pin:
  1. Valid result writes one observation; the row is then readable
     from spine_catalog via `find_by_mfg`-style query.
  2. Skip rules — return None without writing — for:
       a. No mfg_number / part_number
       b. price <= 0 across all price fields
       c. Garbage title (Amazon stub, 404, captcha)
       d. login_required with no price
       e. error with no price
  3. Idempotency: calling twice with the same data produces ONE row
     with seen_count=2 (delegation to spine.catalog.observe).
  4. observe failure is non-fatal — returns None, doesn't raise.
  5. The wiring inside lookup_from_url calls observe_url_lookup —
     anchored on source so a future refactor that drops the hook
     is caught.
"""
from __future__ import annotations

from pathlib import Path


def _spine_db(tmp_path, monkeypatch):
    """Build an isolated Spine DB at tmp_path and point env at it."""
    db = str(tmp_path / "spine.db")
    monkeypatch.setenv("SPINE_DB_PATH", db)
    from src.spine.db import init_db
    init_db(db)
    return db


# ─── Happy path ──────────────────────────────────────────────────────


def test_writes_observation_on_valid_result(tmp_path, monkeypatch):
    db = _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup

    result = {
        "ok": True,
        "supplier": "Amazon",
        "title": "Nitrile Examination Gloves, Medium, 100 ct",
        "description": "Nitrile Examination Gloves, Medium, 100 ct",
        "mfg_number": "M-GLV-100",
        "price": 24.99,
        "url": "https://amazon.com/dp/B0EXAMPLE1",
    }
    meta = observe_url_lookup(result)
    assert meta is not None
    assert meta["mfg_number"] == "M-GLV-100"
    assert meta["seen_count"] == 1
    assert meta["created"] is True

    # Cross-check: catalog row exists with our cost.
    from src.spine.catalog import get_entry
    entry = get_entry(db, "M-GLV-100")
    assert entry is not None
    assert entry["last_priced_cents"] == 2499


def test_idempotent_observation_increments_seen(tmp_path, monkeypatch):
    """Same URL lookup observed twice → ONE row, seen_count=2.
    Delegation to spine.catalog.observe's deterministic id behavior."""
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup

    result = {
        "ok": True, "title": "widget", "description": "widget alpha",
        "mfg_number": "W-9", "price": 5.00,
    }
    m1 = observe_url_lookup(result)
    m2 = observe_url_lookup(result)
    assert m1["seen_count"] == 1
    assert m2["seen_count"] == 2
    assert m2["created"] is False


# ─── Skip rules ──────────────────────────────────────────────────────


def test_skip_when_no_mfg_or_part_number(tmp_path, monkeypatch):
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    assert observe_url_lookup({
        "ok": True, "title": "thing", "price": 10.0,
    }) is None


def test_skip_when_no_positive_price(tmp_path, monkeypatch):
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    # No price field
    assert observe_url_lookup({
        "ok": True, "title": "thing", "mfg_number": "M-1",
    }) is None
    # Zero price
    assert observe_url_lookup({
        "ok": True, "title": "thing", "mfg_number": "M-1",
        "price": 0,
    }) is None
    # Negative price
    assert observe_url_lookup({
        "ok": True, "title": "thing", "mfg_number": "M-1",
        "price": -5.0,
    }) is None


def test_skip_garbage_titles(tmp_path, monkeypatch):
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    for bad_title in ("Amazon.com",
                      "404 Not Found",
                      "Captcha challenge",
                      "Robot check",
                      "Are you a robot?",
                      ""):
        assert observe_url_lookup({
            "ok": True, "title": bad_title, "mfg_number": "M-1",
            "price": 10.0,
        }) is None, f"garbage title {bad_title!r} should be skipped"


def test_skip_login_required_without_price(tmp_path, monkeypatch):
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    assert observe_url_lookup({
        "ok": False, "login_required": True,
        "error": "Medline requires login",
        "supplier": "Medline",
        "mfg_number": "M-1",
        "title": "thing",
    }) is None


def test_skip_error_without_price(tmp_path, monkeypatch):
    _spine_db(tmp_path, monkeypatch)
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    assert observe_url_lookup({
        "ok": False, "error": "scrape failed",
        "title": "Real Title", "mfg_number": "M-1",
    }) is None


def test_skip_when_spine_db_missing(tmp_path, monkeypatch):
    """If the Spine DB hasn't been initialized, write-through is a
    no-op (returns None) — doesn't auto-create."""
    monkeypatch.setenv("SPINE_DB_PATH", str(tmp_path / "nonexistent.db"))
    from src.spine_bridge.url_catalog_writethrough import observe_url_lookup
    assert observe_url_lookup({
        "ok": True, "title": "real product", "mfg_number": "M-1",
        "price": 10.0,
    }) is None


# ─── Wiring source check ─────────────────────────────────────────────


def test_lookup_from_url_calls_writethrough():
    """Anchor on the source: the lookup_from_url success path must
    call observe_url_lookup. A future refactor that drops the hook
    silently regresses the catalog substrate — catch it here."""
    src = Path(__file__).parent.parent.joinpath(
        "src", "agents", "item_link_lookup.py"
    ).read_text(encoding="utf-8")
    assert "from src.spine_bridge.url_catalog_writethrough import observe_url_lookup" in src, (
        "lookup_from_url no longer imports observe_url_lookup — "
        "URL→Catalog write-through is unwired"
    )
    assert "observe_url_lookup(result)" in src, (
        "lookup_from_url no longer calls observe_url_lookup(result)"
    )
