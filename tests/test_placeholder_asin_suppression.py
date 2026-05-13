"""PR-3 / Phase 2 substrate — placeholder-ASIN write-gate + backfill.

Mike P0 live-drive 2026-05-12 (pc_5728f934): five items had templated
Amazon URLs (B07XXXXXXX, B08XXXXXXX, B07H3989EX, B07H123456,
B0ABCDEF12) stamped on them by failed lookups that landed in
item_link anyway. Once a placeholder ASIN lands in product_suppliers
or product_catalog, it poisons the MFG#→ASIN join key forever.

Fix has three parts and this file pins all three:
  1. `is_placeholder_asin` + `sanitize_supplier_url` helpers in
     `src.agents.item_link_lookup` — single chokepoint.
  2. Write-time gates at:
       - product_catalog.add_supplier_price (DB write)
       - routes_pricecheck._do_save_prices (PC autosave)
       - routes_rfq save (RFQ autosave)
  3. Backfill on boot:
       - product_catalog.cleanup_placeholder_asin_urls (catalog DB)
       - product_catalog.cleanup_placeholder_asin_in_json (JSON items)
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Helper-level: is_placeholder_asin ─────────────────────────────


def test_is_placeholder_asin_blocks_repeating_chars():
    """4+ identical consecutive chars is a placeholder."""
    from src.agents.item_link_lookup import is_placeholder_asin
    placeholders = [
        "B07XXXXXXX",   # 7 Xs
        "B08XXXXXXX",
        "B0000XXXXX",   # 4 zeros then 4 Xs
        "B0AAAAAAAA",   # all As
        "B0CCCCCCCC",
    ]
    for asin in placeholders:
        assert is_placeholder_asin(asin), f"{asin} should be flagged as placeholder"


def test_is_placeholder_asin_blocks_sequential_digits():
    """4+ ascending sequential digits is a placeholder."""
    from src.agents.item_link_lookup import is_placeholder_asin
    assert is_placeholder_asin("B07H123456")
    assert is_placeholder_asin("B0X1234567")
    assert is_placeholder_asin("B0X5678ABC")


def test_is_placeholder_asin_blocks_sequential_letters():
    """4+ ascending sequential letters is a placeholder."""
    from src.agents.item_link_lookup import is_placeholder_asin
    assert is_placeholder_asin("B0ABCDEF12")
    assert is_placeholder_asin("B0YEFGHI12")


def test_is_placeholder_asin_mfg_embedded_context():
    """When MFG# is provided and its normalized form appears in the
    ASIN, it's a templated test URL."""
    from src.agents.item_link_lookup import is_placeholder_asin
    # Mike's actual incident: B07H3989EX with item MFG# H-3989
    assert is_placeholder_asin("B07H3989EX", mfg_number="H-3989")
    # Whitespace + case-insensitive normalization
    assert is_placeholder_asin("B07ABCD123", mfg_number="abcd")
    # Without context, B07H3989EX is just a random-looking 10-char ASIN
    assert not is_placeholder_asin("B07H3989EX")


def test_is_placeholder_asin_empty_or_short():
    """Empty or < 10 chars cannot be a valid ASIN — treat as placeholder."""
    from src.agents.item_link_lookup import is_placeholder_asin
    assert is_placeholder_asin("")
    assert is_placeholder_asin("B0")
    assert is_placeholder_asin("B07")
    assert is_placeholder_asin(None)  # type: ignore[arg-type]


def test_is_placeholder_asin_passes_real_asins():
    """No false positives on real Amazon ASINs from the corpus."""
    from src.agents.item_link_lookup import is_placeholder_asin
    reals = [
        "B077JQYDTN",  # McKesson elastic bandage
        "B084TJ9W8V",  # H-8315GR utility cart
        "B00OZACDEC",  # Enluxtra wound dressing
        "B097QCNKJP",  # Manuka honey
        "B084Q9R2W5",  # Clipboard
        "B08113PJ25",  # Bath wipes
        "B0CHH87PT2",  # Generic random
        "B07K1FQVSN",  # McKesson bandage
        "B0746C6NQY",  # McKesson sterile elastic
    ]
    for asin in reals:
        assert not is_placeholder_asin(asin), (
            f"FALSE POSITIVE: {asin} is a real ASIN but was flagged as placeholder"
        )


# ── Helper-level: sanitize_supplier_url ──────────────────────────


def test_sanitize_supplier_url_clears_placeholders():
    """sanitize returns '' for placeholder Amazon URLs."""
    from src.agents.item_link_lookup import sanitize_supplier_url
    placeholders = [
        "https://www.amazon.com/dp/B07XXXXXXX",
        "https://amazon.com/dp/B08XXXXXXX",
        "https://www.amazon.com/dp/B0ABCDEF12",
        "https://www.amazon.com/dp/B07H123456",
    ]
    for url in placeholders:
        assert sanitize_supplier_url(url) == "", f"{url} should sanitize to empty"


def test_sanitize_supplier_url_preserves_real_amazon():
    from src.agents.item_link_lookup import sanitize_supplier_url
    real_url = "https://www.amazon.com/dp/B077JQYDTN"
    assert sanitize_supplier_url(real_url) == real_url


def test_sanitize_supplier_url_preserves_non_amazon():
    """Uline, McMaster, Grainger, etc. URLs pass through unchanged."""
    from src.agents.item_link_lookup import sanitize_supplier_url
    cases = [
        "https://www.uline.com/Product/Detail/H-2749FIH/",
        "https://www.grainger.com/product/foo--12345",
        "https://mms.mckesson.com/product/454621",
        "https://www.bettymills.com/something",
    ]
    for url in cases:
        assert sanitize_supplier_url(url) == url


def test_sanitize_supplier_url_mfg_context():
    """When MFG# is provided, catches templated-URL placeholders too."""
    from src.agents.item_link_lookup import sanitize_supplier_url
    # B07H3989EX is a real-shape ASIN but contains the line's MFG# H-3989
    assert sanitize_supplier_url(
        "https://www.amazon.com/dp/B07H3989EX",
        mfg_number="H-3989"
    ) == ""
    # Same URL without MFG# context passes (not enough signal)
    assert sanitize_supplier_url(
        "https://www.amazon.com/dp/B07H3989EX"
    ) == "https://www.amazon.com/dp/B07H3989EX"


# ── Write-time gate: catalog add_supplier_price ─────────────────


def test_add_supplier_price_refuses_placeholder_url(monkeypatch, tmp_path):
    """add_supplier_price must filter out placeholder ASIN URLs at
    write time, regardless of caller hygiene. Defense in depth — the
    autosave gates are primary, this is the safety net for any code
    path that reaches the catalog DB."""
    from src.agents import product_catalog
    # Probe via the public function signature — we don't need to fully
    # exercise the DB write; we just need to confirm the URL gets
    # sanitized before storage. The function has a try/except that
    # logs and proceeds; we can capture via monkeypatch of the helper.
    captured = {"url": None}

    def fake_sanitize(url, mfg_number=""):
        captured["url"] = url
        # Mimic the real behavior for B07XXXXXXX
        from src.agents.item_link_lookup import sanitize_supplier_url as _real
        return _real(url, mfg_number=mfg_number)

    monkeypatch.setattr(
        "src.agents.item_link_lookup.sanitize_supplier_url",
        fake_sanitize,
    )
    # Mock the DB connection so we don't actually write
    class _FakeConn:
        def execute(self, *a, **kw):
            return self
        def fetchone(self):
            return None
        def fetchall(self):
            return []
        def commit(self):
            pass
        def close(self):
            pass
    monkeypatch.setattr(product_catalog, "_get_conn", lambda: _FakeConn())
    try:
        product_catalog.add_supplier_price(
            product_id=999, supplier_name="Amazon", price=10.0,
            url="https://www.amazon.com/dp/B07XXXXXXX",
        )
    except Exception:
        pass  # we only care that sanitize_supplier_url was invoked
    assert captured["url"] == "https://www.amazon.com/dp/B07XXXXXXX", (
        "add_supplier_price must call sanitize_supplier_url on incoming URL"
    )


# ── Write-time gate: PC autosave (text assertion) ────────────────


def _read_py(path):
    with open(os.path.join(os.path.dirname(__file__), "..", path), encoding="utf-8") as f:
        return f.read()


def test_pc_autosave_calls_sanitize_supplier_url():
    """PC autosave's `link` field-type branch must run the URL
    through sanitize_supplier_url before persisting. The FIRST
    `elif field_type == "link":` in the file is the validation
    helper at module top; the autosave write is the one inside
    _do_save_prices_locked."""
    src = _read_py("src/api/modules/routes_pricecheck.py")
    # Anchor on the write site: items[idx]["item_link"] assignment lives
    # inside the autosave block, never inside the validation helper.
    idx = src.index('_raw_link = str(val).strip() if val else ""')
    block = src[idx:idx + 1500]
    assert "sanitize_supplier_url" in block, (
        "PC autosave link-branch must call sanitize_supplier_url"
    )
    assert 'items[idx]["item_link"]' in block, (
        "PC autosave must still write item_link"
    )


def test_rfq_autosave_calls_sanitize_supplier_url():
    """RFQ autosave's link save block must run URL through
    sanitize_supplier_url before persisting."""
    src = _read_py("src/api/modules/routes_rfq.py")
    idx = src.index('link_val, _ = validate_url(link_raw)')
    block = src[idx:idx + 1500]
    assert "sanitize_supplier_url" in block, (
        "RFQ autosave must call sanitize_supplier_url after validate_url"
    )
    assert 'item["item_link"] = link_val' in block, (
        "RFQ autosave must still write item_link after sanitizing"
    )


# ── Backfill: cleanup_placeholder_asin_in_json ───────────────────


def test_cleanup_placeholder_asin_in_json_scrubs_rfq_items(tmp_path):
    """JSON-side backfill walks rfqs.json + price_checks.json and
    clears item_link on any line whose URL is a placeholder ASIN."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs = {
        "rfq_aaa": {
            "id": "rfq_aaa",
            "line_items": [
                {"item_link": "https://www.amazon.com/dp/B07XXXXXXX",
                 "description": "junk1"},  # placeholder
                {"item_link": "https://www.amazon.com/dp/B077JQYDTN",
                 "description": "real Amazon"},  # real, kept
                {"item_link": "https://www.uline.com/p/H-1234",
                 "description": "uline, kept"},  # non-Amazon, kept
                {"item_link": "",
                 "description": "empty, kept"},
                {"item_link": "https://www.amazon.com/dp/B0ABCDEF12",
                 "description": "alphabet placeholder"},  # placeholder
            ],
        },
        "rfq_clean": {
            "id": "rfq_clean",
            "line_items": [
                {"item_link": "https://example.com/item",
                 "description": "clean rec"},
            ],
        },
    }
    pcs = {
        "pc_one": {
            "id": "pc_one",
            "items": [
                {"item_link": "https://www.amazon.com/dp/B08XXXXXXX",
                 "description": "junk"},  # placeholder
                {"item_link": "https://www.amazon.com/dp/B00OZACDEC",
                 "description": "Enluxtra real"},  # real, kept
            ],
        },
    }
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path.write_text(json.dumps(rfqs), encoding="utf-8")
    pcs_path.write_text(json.dumps(pcs), encoding="utf-8")
    res = cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path),
        pcs_path=str(pcs_path),
    )
    # 2 RFQ items (idx 0 + 4 on rfq_aaa) + 1 PC item (idx 0 on pc_one)
    assert res["rfq_items_cleared"] == 2, res
    assert res["pc_items_cleared"] == 1, res
    assert res["rfqs_touched"] == 1, res
    assert res["pcs_touched"] == 1, res
    # Verify the file was actually updated
    rfqs_after = json.loads(rfqs_path.read_text(encoding="utf-8"))
    items = rfqs_after["rfq_aaa"]["line_items"]
    assert items[0]["item_link"] == ""        # placeholder cleared
    assert items[1]["item_link"] == "https://www.amazon.com/dp/B077JQYDTN"  # real kept
    assert items[2]["item_link"] == "https://www.uline.com/p/H-1234"  # non-Amazon kept
    assert items[3]["item_link"] == ""        # already empty
    assert items[4]["item_link"] == ""        # placeholder cleared
    pcs_after = json.loads(pcs_path.read_text(encoding="utf-8"))
    pc_items = pcs_after["pc_one"]["items"]
    assert pc_items[0]["item_link"] == ""     # placeholder cleared
    assert pc_items[1]["item_link"] == "https://www.amazon.com/dp/B00OZACDEC"  # real kept


def test_cleanup_placeholder_asin_in_json_idempotent(tmp_path):
    """Running twice must not touch anything on the second pass."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path.write_text(json.dumps({
        "rfq_x": {"line_items": [
            {"item_link": "https://www.amazon.com/dp/B07XXXXXXX"},
        ]},
    }), encoding="utf-8")
    pcs_path.write_text("{}", encoding="utf-8")
    res1 = cleanup_placeholder_asin_in_json(rfqs_path=str(rfqs_path), pcs_path=str(pcs_path))
    assert res1["rfq_items_cleared"] == 1
    res2 = cleanup_placeholder_asin_in_json(rfqs_path=str(rfqs_path), pcs_path=str(pcs_path))
    assert res2["rfq_items_cleared"] == 0, (
        "Second pass must be a no-op — file was already scrubbed"
    )


# ── Boot wiring ──────────────────────────────────────────────────


def test_app_boot_calls_cleanup_placeholder_asin_urls():
    """app.py _deferred_init must call both the catalog and JSON
    backfill functions alongside the existing junk-pn cleanup."""
    src = _read_py("app.py")
    idx = src.index("def _deferred_init")
    body = src[idx:idx + 8000]
    assert "cleanup_placeholder_asin_urls" in body, (
        "Boot must invoke catalog-side placeholder-ASIN backfill"
    )
    assert "cleanup_placeholder_asin_in_json" in body, (
        "Boot must invoke JSON-side placeholder-ASIN backfill"
    )


# ── PR-B1 extension: scrub the nested pricing.* chip-data fields ─


def test_cleanup_scrubs_pricing_amazon_url(tmp_path):
    """Live drive 2026-05-13 on pc_5728f934 rows 4/5/9/10: PR #936's
    backfill scrubbed `item.item_link` but not `item.pricing.amazon_url`.
    Server-side chip render at routes_pricecheck.py:692 reads the
    nested field, so chips kept showing `Amazon · B07XXX…` on rows
    where the visible link was already canonical Uline.

    The extended scrub must clear the nested URL too."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path.write_text("{}", encoding="utf-8")
    pcs_path.write_text(json.dumps({
        "pc_chip_leak": {
            "items": [
                {
                    # item_link already canonical (operator pasted Uline)
                    "item_link": "https://www.uline.com/Product/Detail/H-3647GR",
                    "pricing": {
                        # but the nested chip-data still has the placeholder
                        "amazon_url": "https://www.amazon.com/dp/B07XXXXXXX",
                        "amazon_asin": "B07XXXXXXX",
                        "amazon_price": 59.99,
                        "unit_cost": 59.99,
                    },
                },
            ],
        },
    }), encoding="utf-8")
    res = cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path), pcs_path=str(pcs_path),
    )
    # The placeholder URL got scrubbed
    assert res["pc_items_cleared"] >= 1, res
    after = json.loads(pcs_path.read_text(encoding="utf-8"))
    pricing = after["pc_chip_leak"]["items"][0]["pricing"]
    assert pricing["amazon_url"] == "", (
        f"pricing.amazon_url must be cleared; got {pricing['amazon_url']!r}"
    )
    # The stale ASIN cache must clear too so the chip render falls back
    # to fresh lookup rather than re-rendering the same placeholder
    assert pricing["amazon_asin"] == "", (
        f"pricing.amazon_asin must be cleared; got {pricing['amazon_asin']!r}"
    )
    # And the placeholder-derived amazon_price must clear so the chip
    # doesn't re-render with stale dollars
    assert not pricing.get("amazon_price"), (
        f"pricing.amazon_price must clear when ASIN was placeholder; "
        f"got {pricing.get('amazon_price')!r}"
    )
    # Operator-typed canonical item_link untouched
    assert after["pc_chip_leak"]["items"][0]["item_link"] == \
        "https://www.uline.com/Product/Detail/H-3647GR"


def test_cleanup_scrubs_supplier_and_web_url(tmp_path):
    """Same pattern affects pricing.supplier_url and pricing.web_url
    when those URLs carry placeholder ASINs."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path.write_text(json.dumps({
        "rfq_b": {
            "line_items": [
                {
                    "item_link": "https://www.uline.com/p/H-1234",
                    "pricing": {
                        "supplier_url": "https://www.amazon.com/dp/B08XXXXXXX",
                        "web_url": "https://www.amazon.com/dp/B07H123456",
                    },
                },
            ],
        },
    }), encoding="utf-8")
    pcs_path.write_text("{}", encoding="utf-8")
    cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path), pcs_path=str(pcs_path),
    )
    after = json.loads(rfqs_path.read_text(encoding="utf-8"))
    pricing = after["rfq_b"]["line_items"][0]["pricing"]
    assert pricing["supplier_url"] == "", pricing
    assert pricing["web_url"] == "", pricing


def test_cleanup_leaves_real_pricing_chip_data_alone(tmp_path):
    """No false positives — a real ASIN under `pricing.amazon_url`
    must survive the scrub, including its price/asin sibling fields."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    real_url = "https://www.amazon.com/dp/B077JQYDTN"
    pcs_path.write_text(json.dumps({
        "pc_real": {
            "items": [
                {
                    "item_link": real_url,
                    "pricing": {
                        "amazon_url": real_url,
                        "amazon_asin": "B077JQYDTN",
                        "amazon_price": 24.99,
                        "unit_cost": 24.99,
                    },
                },
            ],
        },
    }), encoding="utf-8")
    rfqs_path.write_text("{}", encoding="utf-8")
    res = cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path), pcs_path=str(pcs_path),
    )
    assert res["pc_items_cleared"] == 0, res
    after = json.loads(pcs_path.read_text(encoding="utf-8"))
    pricing = after["pc_real"]["items"][0]["pricing"]
    assert pricing["amazon_url"] == real_url
    assert pricing["amazon_asin"] == "B077JQYDTN"
    assert pricing["amazon_price"] == 24.99


def test_cleanup_extension_idempotent_with_nested_fields(tmp_path):
    """Re-running the extended backfill on already-clean data must not
    re-touch any record (matches the existing idempotency contract)."""
    from src.agents.product_catalog import cleanup_placeholder_asin_in_json
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path.write_text("{}", encoding="utf-8")
    pcs_path.write_text(json.dumps({
        "pc_x": {"items": [{
            "item_link": "",
            "pricing": {
                "amazon_url": "https://www.amazon.com/dp/B07XXXXXXX",
                "amazon_asin": "B07XXXXXXX",
            },
        }]},
    }), encoding="utf-8")
    res1 = cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path), pcs_path=str(pcs_path),
    )
    assert res1["pc_items_cleared"] >= 1
    res2 = cleanup_placeholder_asin_in_json(
        rfqs_path=str(rfqs_path), pcs_path=str(pcs_path),
    )
    assert res2["pc_items_cleared"] == 0, (
        "Second pass must be a no-op — file was already scrubbed"
    )
