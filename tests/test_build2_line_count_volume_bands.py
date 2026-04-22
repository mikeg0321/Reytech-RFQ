"""BUILD-2 P0 regression guards — line-count aware volume bands.

Context: `get_volume_band` had a `line_count=None` parameter that was
silently ignored at body level — the DB lookup only keyed on
(agency, qty_bucket). Mike's thesis (validated in the pilot corpus) is
that unit margin shifts with total quote line-count independent of
per-line qty. A qty=1 line in a 20-line quote runs thinner margins than
the same qty=1 line in a 2-line quote.

BUILD-2 added:
  - `_LINE_COUNT_BUCKETS` (lc_1_3 / lc_4_15 / lc_16_plus)
  - `_line_count_bucket(n)` bucketing helper
  - `line_count_bucket` column + 3-dim PK on `volume_margin_bands`
  - schema migration (drop old 2-dim table)
  - `refresh_curve` CTE computing per-PO line counts
  - fallback chain in `get_volume_band` (agency+qty+lc → agency+qty →
    all+qty+lc → all+qty)
  - `line_count` kwarg threaded through `get_pricing` and
    `_calculate_recommendation` to the two volume_aware call sites

These guards lock the invariants so a future refactor that strips the
line_count dimension (or silently drops the migration) is caught in CI
instead of in production pricing drift.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
VAP_PATH = ROOT / "src" / "core" / "volume_aware_pricing.py"
ORACLE_PATH = ROOT / "src" / "core" / "pricing_oracle_v2.py"
PC_PRICING_PATH = ROOT / "src" / "api" / "modules" / "routes_pricecheck_pricing.py"


# ── Pure-helper tests ────────────────────────────────────────────────────────

def test_line_count_buckets_constant_defined():
    from src.core.volume_aware_pricing import _LINE_COUNT_BUCKETS
    names = [b[0] for b in _LINE_COUNT_BUCKETS]
    assert names == ["lc_1_3", "lc_4_15", "lc_16_plus"], (
        "BUILD-2: _LINE_COUNT_BUCKETS must expose exactly these three "
        "buckets in order — the lc_4_15 default and fallback behavior "
        "depend on this ordering"
    )


def test_line_count_bucket_boundaries():
    from src.core.volume_aware_pricing import _line_count_bucket
    # Boundary cases at the edges of each bucket
    assert _line_count_bucket(1) == "lc_1_3"
    assert _line_count_bucket(3) == "lc_1_3"
    assert _line_count_bucket(4) == "lc_4_15"
    assert _line_count_bucket(15) == "lc_4_15"
    assert _line_count_bucket(16) == "lc_16_plus"
    assert _line_count_bucket(10_000) == "lc_16_plus"


def test_line_count_bucket_defaults_for_unknown():
    """None / 0 / negative / non-numeric must fall to lc_4_15 — the
    schema DEFAULT. This keeps callers that don't know the line count
    on the mid-density bucket instead of silently excluded."""
    from src.core.volume_aware_pricing import _line_count_bucket
    assert _line_count_bucket(None) == "lc_4_15"
    assert _line_count_bucket(0) == "lc_4_15"
    assert _line_count_bucket(-5) == "lc_4_15"
    assert _line_count_bucket("abc") == "lc_4_15"
    assert _line_count_bucket("") == "lc_4_15"


def test_line_count_bucket_handles_string_numeric():
    from src.core.volume_aware_pricing import _line_count_bucket
    assert _line_count_bucket("2") == "lc_1_3"
    assert _line_count_bucket("20") == "lc_16_plus"


# ── Schema / source-level guards ─────────────────────────────────────────────

def test_schema_has_line_count_bucket_column():
    """ensure_schema must create the 3-dim table. A regression that
    reverts to the 2-dim PK silently drops the BUILD-2 dimension —
    catch it here."""
    src = VAP_PATH.read_text(encoding="utf-8")
    assert "line_count_bucket TEXT NOT NULL DEFAULT 'lc_4_15'" in src, (
        "BUILD-2: line_count_bucket column missing from ensure_schema"
    )
    assert "PRIMARY KEY (agency, qty_bucket, line_count_bucket)" in src, (
        "BUILD-2: PK must be 3-dimensional (agency, qty_bucket, "
        "line_count_bucket), not the legacy 2-dim"
    )


def test_ensure_schema_drops_pre_build2_table():
    """The migration path: detect legacy schema, DROP, recreate. A
    refactor that removes the drop leaves prod stuck on the old PK
    and every INSERT fails silently."""
    src = VAP_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"def ensure_schema.*?(?=\ndef |\Z)",
        src, re.DOTALL,
    )
    assert m, "BUILD-2: ensure_schema body not found"
    body = m.group(0)
    assert "PRAGMA table_info(volume_margin_bands)" in body, (
        "BUILD-2: migration must probe existing columns"
    )
    assert 'line_count_bucket" not in cols' in body, (
        "BUILD-2: migration must detect missing line_count_bucket"
    )
    assert "DROP TABLE volume_margin_bands" in body, (
        "BUILD-2: migration must drop legacy 2-dim table so refresh_curve "
        "can repopulate under the new PK"
    )


def test_refresh_curve_has_line_count_cte():
    """refresh_curve must compute per-PO line counts via a CTE. A
    regression that drops the CTE re-buckets every row into lc_4_15
    default, collapsing the new dimension to the old behavior."""
    src = VAP_PATH.read_text(encoding="utf-8")
    assert "WITH po_lc AS" in src, (
        "BUILD-2: refresh_curve must use the po_lc CTE to compute "
        "line_count per PO"
    )
    assert "COUNT(*) AS line_count" in src, (
        "BUILD-2: CTE must aggregate line_count per drive_file_id"
    )
    assert "plc.line_count AS line_count" in src, (
        "BUILD-2: outer SELECT must surface plc.line_count so the Python "
        "loop can bucket it"
    )


def test_refresh_curve_inserts_line_count_bucket():
    src = VAP_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"INSERT INTO volume_margin_bands.*?VALUES.*?\)",
        src, re.DOTALL,
    )
    assert m, "BUILD-2: refresh_curve INSERT not found"
    insert = m.group(0)
    assert "line_count_bucket" in insert, (
        "BUILD-2: INSERT column list must include line_count_bucket, "
        "otherwise the column stays empty and every row lands on the "
        "DEFAULT bucket"
    )
    # 9 positional bind slots + 1 datetime('now') for updated_at = 10 total
    assert insert.count("?") == 9, (
        "BUILD-2: INSERT should have 9 ? placeholders (agency, qb, lcb, "
        "sample_size, p25, p50, p75, avg_cost, avg_price); "
        "datetime('now') is inlined for updated_at"
    )


# ── Oracle plumbing guards ───────────────────────────────────────────────────

def test_get_pricing_accepts_line_count_kwarg():
    """get_pricing must expose `line_count` as a kwarg so the batch
    pricing route can pass len(items)."""
    src = ORACLE_PATH.read_text(encoding="utf-8")
    m = re.search(r"def get_pricing\([^)]*\)", src, re.DOTALL)
    assert m, "get_pricing signature not found"
    sig = m.group(0)
    assert "line_count" in sig, (
        "BUILD-2: get_pricing signature must accept line_count=None so "
        "callers can thread the volume-aware line-count dimension"
    )


def test_get_volume_band_called_with_line_count():
    """Both get_volume_band call sites in pricing_oracle_v2 must pass
    line_count. A regression that drops the arg collapses every quote
    onto the lc_4_15 default."""
    src = ORACLE_PATH.read_text(encoding="utf-8")
    # Both VA call sites (step 6b and _calculate_recommendation) must
    # include `line_count` in the call.
    calls = re.findall(r"get_volume_band\([^)]*\)", src)
    assert len(calls) >= 2, (
        "BUILD-2: expected at least two get_volume_band call sites in "
        "pricing_oracle_v2 (step 6b + _calculate_recommendation)"
    )
    for call in calls:
        assert "line_count" in call, (
            f"BUILD-2: get_volume_band call `{call}` missing line_count — "
            "every call must thread the dimension through"
        )


def test_volume_aware_ceiling_called_with_line_count():
    src = ORACLE_PATH.read_text(encoding="utf-8")
    m = re.search(r"volume_aware_ceiling\([^)]*\)", src, re.DOTALL)
    assert m, "volume_aware_ceiling call not found in pricing_oracle_v2"
    call = m.group(0)
    assert "line_count" in call, (
        "BUILD-2: volume_aware_ceiling call must pass line_count so the "
        "ceiling uses the same bucket as the band lookup"
    )


def test_pc_pricing_route_passes_line_count():
    """The batch pricing route (routes_pricecheck_pricing) knows the
    total line count — it MUST pass it to get_pricing."""
    src = PC_PRICING_PATH.read_text(encoding="utf-8")
    assert "line_count = len(items_data)" in src, (
        "BUILD-2: batch pricing route must compute line_count before "
        "the loop"
    )
    # The get_pricing call in the loop must include line_count=line_count.
    # The call has nested parens from item.get("description", ""), so we
    # lock on the full multi-line kwarg block by anchoring at the
    # `department=agency,` line that precedes line_count in the fixed
    # code.
    assert "line_count=line_count" in src, (
        "BUILD-2: get_pricing call in batch route must pass "
        "line_count=line_count so every priced item in the batch shares "
        "the same bucket"
    )


# ── End-to-end behavior test with an isolated DB ─────────────────────────────

def test_get_volume_band_prefers_exact_line_count_bucket(tmp_path, monkeypatch):
    """When agency + qty + lc all match with n≥10, the exact cell wins
    over fallback cells. Guards against a refactor that collapses the
    three-level fallback back to one level."""
    db_path = tmp_path / "reytech.db"

    # Monkeypatch the module's get_db to point at our isolated DB
    from src.core import volume_aware_pricing as vap
    import src.core.db as core_db

    def _get_db():
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(vap, "get_db", _get_db, raising=False)
    # Also patch the in-function import (from src.core.db import get_db)
    monkeypatch.setattr(core_db, "get_db", _get_db)

    # Seed: one cell in lc_1_3 for cchcs+qty_3_10 with n=20, one in
    # lc_4_15 with n=50. Exact match on lc_1_3 should win when caller
    # passes line_count=2 (even though lc_4_15 has more samples).
    conn = _get_db()
    vap.ensure_schema(conn)
    conn.executemany("""
        INSERT INTO volume_margin_bands
          (agency, qty_bucket, line_count_bucket, sample_size,
           p25_margin, p50_margin, p75_margin, avg_unit_cost, avg_unit_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        ("cchcs", "qty_3_10", "lc_1_3",    20, 0.10, 0.15, 0.20, 40.0, 46.0),
        ("cchcs", "qty_3_10", "lc_4_15",   50, 0.05, 0.08, 0.11, 40.0, 43.2),
        ("cchcs", "qty_3_10", "lc_16_plus", 30, 0.03, 0.05, 0.07, 40.0, 42.0),
    ])
    conn.commit()
    conn.close()

    # line_count=2 → lc_1_3 cell, p50=0.15
    band = vap.get_volume_band("cchcs", quantity=5, line_count=2)
    assert band is not None
    assert band["line_count_bucket"] == "lc_1_3"
    assert band["p50_margin"] == pytest.approx(0.15)
    assert band["used_fallback_lc"] is False
    assert band["used_fallback_agency"] is False

    # line_count=20 → lc_16_plus cell, p50=0.05
    band = vap.get_volume_band("cchcs", quantity=5, line_count=20)
    assert band is not None
    assert band["line_count_bucket"] == "lc_16_plus"
    assert band["p50_margin"] == pytest.approx(0.05)

    # line_count=None → lc_4_15 default
    band = vap.get_volume_band("cchcs", quantity=5, line_count=None)
    assert band is not None
    assert band["line_count_bucket"] == "lc_4_15"
    assert band["p50_margin"] == pytest.approx(0.08)
