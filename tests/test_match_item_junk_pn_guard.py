"""Mike P0 2026-05-05 (cont.): junk-pn guard for match_item + add_to_catalog.

Live evidence: PR #740 fixed the smart_search popularity-sort fallthrough
that was stamping a single product on every line. Re-verify on prod
showed each line now matches a *different* product but they're STILL
wrong: line N's placeholder pn "N" hits a catalog row whose sku/mfg_number
literally equals "N" via match_item Strategy 1 (98% confidence
"Exact part# match"). Two known polluted rows (id=840 sku=1, id=841 sku=2)
were created by the auto-enrichment pipeline writing buyer placeholder
pns straight into the catalog.

Two changes:
  1. `_is_real_part_number(pn)` helper. A real pn must be ≥ 4 chars
     (consistent with smart_search's literal-SKU short-circuit). Single
     digits / 2-3 char strings can't be real part numbers.
  2. match_item Strategies -1 and 1 (the exact-match strategies that
     produce 98% confidence) gate on this helper. Junk pns fall through
     to the token-match strategy where Jaccard ≥ 0.65 keeps cross-
     category matches out.
  3. add_to_catalog strips junk pn / mfg_number before write so the
     catalog stops accumulating new placeholder-keyed rows.
"""
from pathlib import Path

import pytest


@pytest.fixture
def tmp_catalog(tmp_path, monkeypatch):
    monkeypatch.setenv("PRODUCT_CATALOG_DB", str(tmp_path / "catalog.db"))
    from src.agents import product_catalog
    monkeypatch.setattr(product_catalog, "DB_PATH", str(tmp_path / "catalog.db"))
    product_catalog.init_catalog_db()
    return product_catalog


# ── _is_real_part_number contract ──────────────────────────────────


def test_is_real_part_number_rejects_junk():
    from src.agents.product_catalog import _is_real_part_number
    for junk in ["1", "2", "3", "4", "5", "6", "7", "8", "9",
                 "10", "12", "99", "AB", "X", "ab"]:
        assert _is_real_part_number(junk) is False, (
            f"_is_real_part_number({junk!r}) should be False (junk placeholder)"
        )
    for falsy in [None, "", "  ", "\t"]:
        assert _is_real_part_number(falsy) is False


def test_is_real_part_number_accepts_real_part_numbers():
    from src.agents.product_catalog import _is_real_part_number
    for real in ["1234", "12345", "ABCD", "B0CHH87PT2", "AB-12",
                 "008R13041", "00300504-6", "GLV-NIT-L-100"]:
        assert _is_real_part_number(real) is True, (
            f"_is_real_part_number({real!r}) should be True"
        )


# ── match_item: junk pn must NOT trigger Strategy 1 exact-match ────


def test_match_item_junk_pn_does_not_trigger_exact_match(tmp_catalog):
    """Pollution scenario: catalog has a row with sku='1' and mfg='1'
    (real product, but with placeholder pn). match_item("Some other
    product", pn="1") must NOT stamp 98% on that polluted row."""
    pc = tmp_catalog
    conn = pc._get_conn()
    # Polluted row mirroring prod id=840: sku=1, mfg=1, real product name
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, description, "
        " search_tokens, sell_price, cost, times_quoted, is_test) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
        ("Engraved two line name tag, black/white",
         "1", "1",
         "Engraved two line name tag, black/white",
         "engraved two line name tag black white",
         5.00, 3.00, 100),
    )
    conn.commit()
    conn.close()
    # Operator quoting a coloring poster with placeholder pn=1
    matches = pc.match_item(
        "Love Velvet Coloring Poster - Stuff2Color",
        part_number="1", top_n=3,
    )
    # The exact-match strategy must not have fired. Either no match OR
    # a token-based match (which won't be > 95% confidence).
    high_conf = [m for m in matches if m.get("match_confidence", 0) >= 0.95]
    assert high_conf == [], (
        f"match_item with junk pn '1' produced {len(high_conf)} high-"
        f"confidence matches: {[(m.get('name'), m.get('match_confidence'), m.get('match_reason')) for m in high_conf]}. "
        "Junk pn must not trigger Strategy 0 / 1 exact-match."
    )
    # And no match should claim 'Exact part# match' as its reason
    assert all("Exact part# match" not in (m.get("match_reason") or "")
               for m in matches), (
        f"Some match claims 'Exact part# match' for junk pn: "
        f"{[m.get('match_reason') for m in matches]}"
    )


def test_match_item_real_pn_still_finds_exact_match(tmp_catalog):
    """The guard must not break legitimate exact-pn lookups."""
    pc = tmp_catalog
    conn = pc._get_conn()
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, description, "
        " search_tokens, sell_price, cost, times_quoted, is_test) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
        ("Engraved nameplate", "AB-12345", "AB-12345",
         "Engraved nameplate per spec", "engraved nameplate spec",
         15.00, 10.00, 5),
    )
    conn.commit()
    conn.close()
    matches = pc.match_item("Engraved nameplate", part_number="AB-12345", top_n=1)
    assert len(matches) >= 1
    assert matches[0].get("match_confidence", 0) >= 0.95, (
        f"Real pn AB-12345 should hit exact-match, got "
        f"{matches[0].get('match_confidence')} ({matches[0].get('match_reason')})"
    )
    assert "Exact part#" in (matches[0].get("match_reason") or ""), (
        f"Match reason should be Exact part#, got {matches[0].get('match_reason')!r}"
    )


# ── add_to_catalog: junk pn must NOT be written as sku / mfg ───────


def test_add_to_catalog_drops_junk_part_number(tmp_catalog):
    """Operator submits a PC where a buyer's 704 form has placeholder
    pns. The auto-enrichment used to write part_number='1' and
    mfg_number='1' straight into the catalog, polluting it. New writes
    must drop the junk pn so the row gets indexed by description only."""
    pc = tmp_catalog
    pid = pc.add_to_catalog(
        description="Love Velvet Coloring Poster - Stuff2Color",
        part_number="1", mfg_number="1",
        cost=3.15, sell_price=4.25,
        supplier_name="Linn Thriftway",
    )
    assert pid is not None
    conn = pc._get_conn()
    row = conn.execute(
        "SELECT name, sku, mfg_number FROM product_catalog WHERE id=?",
        (pid,),
    ).fetchone()
    conn.close()
    assert row["sku"] in (None, ""), (
        f"Junk pn '1' was written as sku={row['sku']!r}; should be empty"
    )
    assert row["mfg_number"] in (None, ""), (
        f"Junk mfg '1' was written as mfg_number={row['mfg_number']!r}; should be empty"
    )
    # Name should be from description, not "1"
    assert row["name"] != "1", "Name should be description-derived, not the junk pn"
    assert "Love Velvet" in (row["name"] or ""), (
        f"Name should reflect the description, got {row['name']!r}"
    )


def test_add_to_catalog_preserves_real_pn(tmp_catalog):
    """The junk filter must not strip real pns."""
    pc = tmp_catalog
    pid = pc.add_to_catalog(
        description="Echo Dot 5th Gen Smart Speaker",
        part_number="B0CHH87PT2", mfg_number="B0CHH87PT2",
        cost=49.99, sell_price=49.99,
    )
    assert pid is not None
    conn = pc._get_conn()
    row = conn.execute(
        "SELECT name, sku, mfg_number FROM product_catalog WHERE id=?",
        (pid,),
    ).fetchone()
    conn.close()
    assert row["sku"] == "B0CHH87PT2"
    assert row["mfg_number"] == "B0CHH87PT2"


# ── Source-level guard: the helper must be used in match_item ──────


def test_helper_is_wired_into_match_item():
    src = Path(__file__).resolve().parent.parent / "src/agents/product_catalog.py"
    body = src.read_text(encoding="utf-8")
    # Helper must exist
    assert "def _is_real_part_number(" in body
    # Strategy 1 must call it before doing the exact-match SELECT
    fn_start = body.find("def match_item(")
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    assert "_is_real_part_number(part_number)" in fn_body, (
        "match_item must gate Strategies -1 / 1 on _is_real_part_number"
    )
