"""PR-AL — backfill admin route /api/admin/heal-ingest-enrichment.

Heals existing non-terminal PCs and RFQs by retroactively applying
the same auto-tax (PR-AI #990) + auto-price (PR-AJ #991) enrichment
logic from `_create_record`. Designed for the operator's existing
queue where records were ingested BEFORE the auto-enrichment hooks
landed and still carry ⚠ DEFAULT tax / empty cost-basis.

Tests pin:
  1. Dry-run mode reports what WOULD change without mutating disk.
  2. Real-run stamps tax_rate / tax_source / tax_jurisdiction on a
     PC whose tax_rate was zero.
  3. Real-run skips records whose tax_rate is already set (no
     overwriting operator-confirmed tax decisions).
  4. Per-item Oracle enrichment fills only blank reference fields
     and never overwrites operator-typed unit_cost.
  5. Terminal-status records (sent/won/lost/dismissed/etc) are
     skipped entirely — those records are done.
  6. Idempotency: running twice converges; the second run is a no-op.

Hermetic — monkeypatched tax_for_address + recommend_for_item, no
real CDTFA / Oracle calls.
"""
from __future__ import annotations

import pytest


def _seed_pc(pc_id, **overrides):
    """Write a minimal PC record. Defaults to a 'needs-healing' shape
    (empty tax + empty item cost) so the heal route has work to do."""
    from src.api.dashboard import _save_single_pc
    pc = {
        "id": pc_id,
        "pc_number": "TEST-AL-PC",
        "status": "parsed",
        "agency": "cchcs",
        "institution": "CSP-SAC",
        "ship_to": "100 Prison Rd, Coalinga, CA 93210",
        "tax_rate": 0,
        "tax_source": "",
        "tax_jurisdiction": "",
        "tax_validated": False,
        "items": [{
            "description": "test widget",
            "quantity": 5,
            "uom": "EA",
        }],
    }
    pc.update(overrides)
    _save_single_pc(pc_id, pc)
    return pc_id


def _load_pc(pc_id):
    from src.api.dashboard import _load_price_checks
    return _load_price_checks().get(pc_id)


def _patch_enrichment(monkeypatch, *, tax_rate=0.08975, oracle_hit=True):
    """Stub the two enrichment facades for predictable test behavior."""
    monkeypatch.setattr(
        "src.core.quote_contract.tax_for_address",
        lambda addr: {
            "rate": tax_rate, "rate_bps": int(tax_rate * 10000),
            "jurisdiction": "COALINGA", "source": "cdtfa_api",
            "validated": True, "facility_code": "",
        } if addr else {"rate": 0, "rate_bps": 0, "jurisdiction": "",
                          "source": "", "validated": False, "facility_code": ""},
    )
    if oracle_hit:
        monkeypatch.setattr(
            "src.core.pricing_oracle_v2.recommend_for_item",
            lambda description, part_number="", qty=1, upc="": {
                "unit_cost": None, "catalog_cost": 9.99,
                "supplier_cost": 9.99, "supplier": "TestSupplier",
                "source": "oracle", "asin": "B0HEAL",
                "source_url": "https://example.com/p/heal",
                "amazon_price": None, "scprs_price": None,
                "confidence": 0.7,
            },
        )
    else:
        monkeypatch.setattr(
            "src.core.pricing_oracle_v2.recommend_for_item",
            lambda **kw: None,
        )


# ── Dry-run ─────────────────────────────────────────────────────────


def test_heal_dry_run_reports_without_mutating(auth_client, temp_data_dir, monkeypatch):
    _patch_enrichment(monkeypatch)
    _seed_pc("pc_heal_dry")
    resp = auth_client.post("/api/admin/heal-ingest-enrichment",
                              json={"dry_run": True})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["dry_run"] is True
    assert body["records_processed"] >= 1
    assert body["tax_resolved"] >= 1
    assert body["items_enriched"] >= 1
    # Disk state UNCHANGED
    pc = _load_pc("pc_heal_dry")
    assert pc["tax_rate"] == 0
    assert pc["items"][0].get("catalog_cost") in (None, 0, "")
    assert pc["items"][0].get("auto_priced_at_ingest") in (None, False)


# ── Real-run ────────────────────────────────────────────────────────


def test_heal_real_run_stamps_tax_on_pc(auth_client, temp_data_dir, monkeypatch):
    _patch_enrichment(monkeypatch)
    _seed_pc("pc_heal_tax")
    resp = auth_client.post("/api/admin/heal-ingest-enrichment",
                              json={"dry_run": False})
    assert resp.status_code == 200
    pc = _load_pc("pc_heal_tax")
    # PERCENT format: 0.08975 → 8.975
    assert pc["tax_rate"] == 8.975
    assert pc["tax_source"] == "cdtfa_api"
    assert pc["tax_jurisdiction"] == "COALINGA"
    assert pc["tax_validated"] is True


def test_heal_real_run_skips_pc_with_resolved_tax(auth_client, temp_data_dir, monkeypatch):
    """Records that already have tax_rate set are SKIPPED — operator
    decisions are preserved."""
    _patch_enrichment(monkeypatch, tax_rate=0.08975)
    _seed_pc("pc_heal_skip", tax_rate=7.25, tax_source="manual_operator")
    resp = auth_client.post("/api/admin/heal-ingest-enrichment",
                              json={"dry_run": False})
    assert resp.status_code == 200
    pc = _load_pc("pc_heal_skip")
    # Untouched — operator's prior decision sails through
    assert pc["tax_rate"] == 7.25
    assert pc["tax_source"] == "manual_operator"


def test_heal_real_run_enriches_oracle_reference_fields(auth_client, temp_data_dir, monkeypatch):
    _patch_enrichment(monkeypatch)
    _seed_pc("pc_heal_price")
    resp = auth_client.post("/api/admin/heal-ingest-enrichment",
                              json={"dry_run": False})
    pc = _load_pc("pc_heal_price")
    it = pc["items"][0]
    assert it["catalog_cost"] == 9.99
    assert it["supplier"] == "TestSupplier"
    assert it["source_url"] == "https://example.com/p/heal"
    assert it["asin"] == "B0HEAL"
    assert it["confidence"] == 0.7
    assert it.get("auto_priced_at_ingest") is True
    assert it.get("auto_price_at")  # timestamp present
    # CRITICAL — unit_cost still UNSET (operator's decision, never auto-applied)
    assert not it.get("unit_cost")


def test_heal_skips_items_with_operator_cost(auth_client, temp_data_dir, monkeypatch):
    """Items carrying operator-confirmed unit_cost or supplier_cost
    are SKIPPED entirely. URL-paste protection class — Oracle never
    clobbers operator pricing."""
    _patch_enrichment(monkeypatch)
    pc = {
        "id": "pc_heal_op_cost",
        "pc_number": "TEST-OP-COST",
        "status": "parsed",
        "agency": "cchcs",
        "institution": "CSP-SAC",
        "ship_to": "100 Prison Rd, Coalinga, CA 93210",
        "tax_rate": 0,
        "items": [{
            "description": "operator-priced item",
            "quantity": 1,
            "unit_cost": 50.00,  # operator already typed this
        }],
    }
    from src.api.dashboard import _save_single_pc
    _save_single_pc("pc_heal_op_cost", pc)
    auth_client.post("/api/admin/heal-ingest-enrichment",
                       json={"dry_run": False})
    pc_after = _load_pc("pc_heal_op_cost")
    it = pc_after["items"][0]
    assert it["unit_cost"] == 50.00  # untouched
    # Oracle reference fields NOT stamped (Oracle wasn't called for this item)
    assert it.get("catalog_cost") in (None, 0, "")
    assert it.get("supplier") in (None, "")
    assert it.get("auto_priced_at_ingest") in (None, False)


def test_heal_skips_terminal_records(auth_client, temp_data_dir, monkeypatch):
    """Records in terminal statuses (sent, won, lost, archived, etc)
    are skipped — those records are done."""
    _patch_enrichment(monkeypatch)
    _seed_pc("pc_heal_sent", status="sent")
    auth_client.post("/api/admin/heal-ingest-enrichment",
                       json={"dry_run": False})
    pc = _load_pc("pc_heal_sent")
    # Untouched
    assert pc["tax_rate"] == 0


def test_heal_rfq_mutations_persist_across_alias_sync(auth_client, temp_data_dir, monkeypatch):
    """PR-AL hotfix regression. Pre-fix, RFQ heal mutations were lost
    because `_save_single_rfq`'s alias-sync (data_layer.py:315-319)
    overwrote the mutated `items` list with a clean copy of the
    unmutated `line_items` list. After fix: both keys are pinned to
    the same mutated list before save, so the alias-sync becomes
    a no-op shallow copy.

    Simulates the exact failure mode Mike hit on rfq_4a723a40:
    3 consecutive real-runs all reported 22 items enriched, but a
    detail-page walk showed 0 🔮 Oracle badges. The 22 items_enriched
    count was real (`_stamped=True` inside the heal loop) but the
    save's alias-sync clobbered the mutations.
    """
    _patch_enrichment(monkeypatch)
    rid = "rfq_alias_clobber_test"
    # Seed with the EXACT shape json deserialization produces:
    # items + line_items as TWO SEPARATE lists (same content) — the
    # shape that triggered the pre-fix clobber.
    items_a = [{"description": "widget A", "qty": 1, "uom": "EA"}]
    items_b = [{"description": "widget A", "qty": 1, "uom": "EA"}]
    rfq = {
        "id": rid, "rfq_number": "TEST-ALIAS", "status": "parsed",
        "agency": "cchcs", "institution": "CSP-SAC",
        "ship_to": "100 Prison Rd, Coalinga, CA 93210",
        "tax_rate": 8.975,  # already resolved so heal only does items
        "items": items_a,
        "line_items": items_b,  # SEPARATE list with same content
    }
    from src.api.dashboard import _save_single_rfq, load_rfqs
    _save_single_rfq(rid, rfq)
    # First heal run — should enrich the 1 item
    r1 = auth_client.post("/api/admin/heal-ingest-enrichment",
                            json={"dry_run": False}).get_json()
    assert r1["items_enriched"] >= 1
    # Re-load from disk (NOT the in-memory rfq dict we seeded)
    loaded = load_rfqs().get(rid, {})
    line_items = loaded.get("line_items") or loaded.get("items") or []
    assert len(line_items) == 1
    # CRITICAL: the mutation must persist across the load → mutate →
    # save → reload cycle. Pre-fix this was None/empty because the
    # alias-sync clobbered it.
    assert line_items[0].get("auto_priced_at_ingest") is True
    assert line_items[0].get("catalog_cost") == 9.99
    assert line_items[0].get("supplier") == "TestSupplier"
    # Second run should report 0 items enriched (true idempotency
    # now that mutations persist)
    r2 = auth_client.post("/api/admin/heal-ingest-enrichment",
                            json={"dry_run": False}).get_json()
    # rid no longer appears in summary because nothing to enrich
    assert rid not in r2.get("summary", {}) or r2["summary"][rid].get("items_enriched", 0) == 0


def test_heal_v2_fills_ship_to_from_facility_registry(auth_client, temp_data_dir, monkeypatch):
    """PR-AM (2026-05-14): when a record's ship_to is empty/short
    (the operator-typed-only-'CA' pattern Mike hit on 4 prod queue
    records), heal must fall back to facility_registry.resolve(
    institution) → canonical ship_to → tax_for_address. Same logic
    PR-AI uses at fresh-ingest in _create_record."""
    _patch_enrichment(monkeypatch)

    class _FakeFac:
        code = "CSP-SAC"
        address_line1 = "100 Prison Road"
        address_line2 = "Represa, CA 95671"

    monkeypatch.setattr(
        "src.core.facility_registry.resolve",
        lambda inst: _FakeFac() if (inst or "").strip().upper().startswith("CSP") else None,
    )
    _seed_pc("pc_heal_facreg", ship_to="CA",  # 2 chars — too short
             institution="CSP-SAC")
    resp = auth_client.post("/api/admin/heal-ingest-enrichment",
                              json={"dry_run": False})
    body = resp.get_json()
    assert body["ok"] is True
    assert body["ship_to_filled"] >= 1
    assert body["tax_resolved"] >= 1
    pc = _load_pc("pc_heal_facreg")
    assert pc["ship_to"] == "100 Prison Road, Represa, CA 95671"
    assert pc["institution"] == "CSP-SAC"
    # Now tax resolved on the canonical address
    assert pc["tax_rate"] == 8.975
    assert pc["tax_source"] == "cdtfa_api"


def test_heal_v2_skips_ship_to_when_facility_unresolvable(auth_client, temp_data_dir, monkeypatch):
    """Records with agency-only institution codes (cchcs / calvet) —
    facility_registry returns None. Heal leaves ship_to empty so the
    operator can type it manually; the existing ⚠ DEFAULT warning
    surfaces correctly."""
    _patch_enrichment(monkeypatch)
    monkeypatch.setattr("src.core.facility_registry.resolve", lambda inst: None)
    _seed_pc("pc_heal_facreg_miss", ship_to="CA", institution="cchcs")
    auth_client.post("/api/admin/heal-ingest-enrichment",
                       json={"dry_run": False})
    pc = _load_pc("pc_heal_facreg_miss")
    assert pc["ship_to"] == "CA"  # untouched
    assert pc.get("tax_rate") in (0, None)


def test_heal_is_idempotent(auth_client, temp_data_dir, monkeypatch):
    """Running heal twice → second run reports zero changes; disk
    state matches first run output."""
    _patch_enrichment(monkeypatch)
    _seed_pc("pc_heal_idem")
    # First run
    r1 = auth_client.post("/api/admin/heal-ingest-enrichment",
                            json={"dry_run": False}).get_json()
    assert r1["tax_resolved"] >= 1
    pc1 = _load_pc("pc_heal_idem")
    tax1 = pc1["tax_rate"]
    # Second run
    r2 = auth_client.post("/api/admin/heal-ingest-enrichment",
                            json={"dry_run": False}).get_json()
    pc2 = _load_pc("pc_heal_idem")
    # State converged: first run did the work, second is no-op for tax.
    # (items can still match if recommend_for_item is still hit on
    # already-enriched items where catalog_cost is filled — the inner
    # loop's "if not _it.get('catalog_cost')" gates prevent re-stamping
    # but the Oracle call still fires. Either way, no field changes.)
    assert pc2["tax_rate"] == tax1
    # Tax was already set after run 1, so run 2 reports zero tax resolves
    assert r2["tax_resolved"] == 0
