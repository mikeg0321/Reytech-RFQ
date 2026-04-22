"""Volume-Aware Pricing (Phase B).

Returns the (p25, p50, p75) line-level margin band that historically won
deals, conditioned on (agency, line-item quantity, total line count).
Based on 99 POs / 799 lines ingested in Phase A from Drive PO corpus
joined to QuoterWerks cost data (_phase_a/pilot.sqlite).

Mike's thesis — validated at the data level — is that unit margin drops
sharply with quantity AND with total quote line count. A 2-item request
at qty=1 sustains ~15% margin; a 20-item request at qty=100 wins at ~6%.
Blind 30% markup on a large order loses the bid. Blind 30% markup on a
singleton leaves money on the table.

BUILD-2 (2026-04-22): added `line_count_bucket` dimension — volume bands
now bucket by (agency, qty_bucket, line_count_bucket). This captures the
separate effect of total-quote-size on margin that qty-per-line misses.
A qty=1 line in a 20-line quote competes differently than a qty=1 line
in a 2-line quote.

Public API:
    get_volume_band(agency, quantity, line_count=None) -> dict | None
    volume_aware_ceiling(cost, agency, quantity, line_count=None) -> dict | None
    refresh_curve()                                    -> int  (rows written)

The curve lives in the main `reytech.db` table `volume_margin_bands` so
the oracle can read it without ever opening the phase_a pilot sqlite at
request time. `refresh_curve()` re-derives it from
`_phase_a/pilot.sqlite` — call it after a wide ingest.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Dict, Optional

log = logging.getLogger("reytech.volume_aware")


_QTY_BUCKETS = [
    ("qty_1_2", 1, 2),
    ("qty_3_10", 3, 10),
    ("qty_11_50", 11, 50),
    ("qty_51_200", 51, 200),
    ("qty_201_plus", 201, 10_000_000),
]


# BUILD-2: total line-count buckets. Most quotes land in the 4-15 line
# range, so that bucket is the schema DEFAULT and the fallback when a
# caller passes None.
_LINE_COUNT_BUCKETS = [
    ("lc_1_3", 1, 3),
    ("lc_4_15", 4, 15),
    ("lc_16_plus", 16, 10_000),
]


def _qty_bucket(qty: float) -> str:
    try:
        q = int(qty or 0)
    except (TypeError, ValueError):
        return "qty_1_2"
    for name, lo, hi in _QTY_BUCKETS:
        if lo <= q <= hi:
            return name
    return "qty_1_2"


def _line_count_bucket(line_count) -> str:
    """BUILD-2: bucket a total line-count into 1-3 / 4-15 / 16+.

    None / 0 / negative / non-numeric all fall to 'lc_4_15' — the schema
    default and the mid-density bucket, so unknown-line-count callers
    still get a usable band.
    """
    try:
        lc = int(line_count or 0)
    except (TypeError, ValueError):
        return "lc_4_15"
    if lc <= 0:
        return "lc_4_15"
    for name, lo, hi in _LINE_COUNT_BUCKETS:
        if lo <= lc <= hi:
            return name
    return "lc_4_15"


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create volume_margin_bands with the 3-dim PK.

    BUILD-2 migration: a pre-existing table without `line_count_bucket`
    has a 2-dim PK (agency, qty_bucket) which cannot accept the new
    INSERTs. Drop it — `refresh_curve()` repopulates from
    `_phase_a/pilot.sqlite` on next run. Until then `get_volume_band`
    returns None (existing contract) and callers skip the VA signal.
    """
    try:
        cols = [r[1] for r in conn.execute(
            "PRAGMA table_info(volume_margin_bands)"
        ).fetchall()]
    except Exception:
        cols = []
    if cols and "line_count_bucket" not in cols:
        log.info("volume_margin_bands: pre-BUILD-2 schema, dropping for rebuild")
        conn.execute("DROP TABLE volume_margin_bands")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS volume_margin_bands (
        agency TEXT NOT NULL,
        qty_bucket TEXT NOT NULL,
        line_count_bucket TEXT NOT NULL DEFAULT 'lc_4_15',
        sample_size INTEGER NOT NULL,
        p25_margin REAL,
        p50_margin REAL,
        p75_margin REAL,
        avg_unit_cost REAL,
        avg_unit_price REAL,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (agency, qty_bucket, line_count_bucket)
    );
    """)


def refresh_curve(phase_a_db: Optional[str] = None) -> int:
    """Rebuild volume_margin_bands from _phase_a/pilot.sqlite.
    Returns rows written. Safe to call on cold start; short-circuits
    cleanly when pilot db is missing."""
    from src.core.db import DB_PATH  # noqa: F401 (kept for compatibility)

    if phase_a_db is None:
        # pilot.sqlite lives at repo_root/_phase_a/pilot.sqlite
        here = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        phase_a_db = os.path.join(here, "_phase_a", "pilot.sqlite")

    if not os.path.exists(phase_a_db):
        log.info("refresh_curve: no phase_a db at %s — skipping", phase_a_db)
        return 0

    try:
        src = sqlite3.connect(phase_a_db, timeout=10)
        src.row_factory = sqlite3.Row
        # BUILD-2: CTE joins each line to its PO's total line count so the
        # line_count_bucket can be derived per row. The bucket is what
        # matters — mis-sized cohorts were invisible in the 2-dim schema.
        rows = list(src.execute("""
            WITH po_lc AS (
                SELECT drive_file_id, COUNT(*) AS line_count
                  FROM po_line GROUP BY drive_file_id
            )
            SELECT h.agency AS agency,
                   l.quantity AS quantity,
                   l.qw_unit_cost AS cost,
                   l.unit_price AS price,
                   l.margin_pct AS margin,
                   plc.line_count AS line_count
              FROM po_line l
              JOIN po_header h ON l.drive_file_id = h.drive_file_id
              JOIN po_lc plc ON l.drive_file_id = plc.drive_file_id
             WHERE l.margin_pct IS NOT NULL
               AND l.margin_pct BETWEEN -5.0 AND 5.0
               AND l.quantity IS NOT NULL AND l.quantity > 0
        """))
        src.close()
    except Exception as e:
        log.warning("refresh_curve: pilot read failed: %s", e)
        return 0

    buckets: Dict[tuple, list] = {}
    for r in rows:
        agency = (r["agency"] or "other").lower()
        qb = _qty_bucket(r["quantity"])
        lcb = _line_count_bucket(r["line_count"])
        item = {
            "margin": r["margin"],
            "cost": r["cost"] or 0,
            "price": r["price"] or 0,
        }
        buckets.setdefault((agency, qb, lcb), []).append(item)
        buckets.setdefault(("all", qb, lcb), []).append(item)

    def _pct(vals, p):
        if not vals:
            return None
        s = sorted(vals)
        k = (len(s) - 1) * p
        lo = int(k)
        hi = min(lo + 1, len(s) - 1)
        frac = k - lo
        return s[lo] + (s[hi] - s[lo]) * frac

    written = 0
    from src.core.db import get_db
    with get_db() as dst:
        ensure_schema(dst)
        dst.execute("DELETE FROM volume_margin_bands")
        for (agency, qb, lcb), lines in buckets.items():
            if len(lines) < 3:
                continue
            margins = [x["margin"] for x in lines]
            costs = [x["cost"] for x in lines if x["cost"] > 0]
            prices = [x["price"] for x in lines if x["price"] > 0]
            dst.execute("""
                INSERT INTO volume_margin_bands
                  (agency, qty_bucket, line_count_bucket, sample_size,
                   p25_margin, p50_margin, p75_margin,
                   avg_unit_cost, avg_unit_price, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                agency, qb, lcb, len(lines),
                _pct(margins, 0.25), _pct(margins, 0.50), _pct(margins, 0.75),
                (sum(costs) / len(costs)) if costs else None,
                (sum(prices) / len(prices)) if prices else None,
            ))
            written += 1
    log.info("volume_margin_bands refreshed: %d (agency, qty, line_count) cells",
             written)
    return written


_BAND_COLS = (
    "agency, qty_bucket, line_count_bucket, sample_size, "
    "p25_margin, p50_margin, p75_margin, avg_unit_cost, avg_unit_price"
)


def _row_to_band(row, used_fallback_agency: bool,
                  used_fallback_lc: bool) -> dict:
    return {
        "agency": row[0],
        "qty_bucket": row[1],
        "line_count_bucket": row[2],
        "sample_size": row[3],
        "p25_margin": row[4],
        "p50_margin": row[5],
        "p75_margin": row[6],
        "avg_unit_cost": row[7],
        "avg_unit_price": row[8],
        "used_fallback": used_fallback_agency or used_fallback_lc,
        "used_fallback_agency": used_fallback_agency,
        "used_fallback_lc": used_fallback_lc,
    }


def get_volume_band(agency: str, quantity: float,
                    line_count: Optional[int] = None) -> Optional[dict]:
    """Return the historical margin band for this
    (agency, qty bucket, line_count bucket).

    Fallback chain (BUILD-2):
      1. exact (agency, qty, lc) with n ≥ 10
      2. (agency, qty, *)   best-sample line_count cell, n ≥ 10
      3. ('all', qty, lc)   with n ≥ 5
      4. ('all', qty, *)    best-sample line_count cell, n ≥ 5

    Falls back to whatever non-empty row was found (with flags) before
    returning None, so sparse-data callers still get some signal.

    Shape:
      {
        "agency": "cchcs",
        "qty_bucket": "qty_3_10",
        "line_count_bucket": "lc_4_15",
        "sample_size": 27,
        "p25_margin": 0.09,      # decimal margins (0.09 = 9%)
        "p50_margin": 0.13,
        "p75_margin": 0.18,
        "avg_unit_cost": 48.21,
        "avg_unit_price": 54.40,
        "used_fallback": False,
        "used_fallback_agency": False,
        "used_fallback_lc": False,
      }
    """
    if not agency:
        agency = "other"
    qb = _qty_bucket(quantity)
    lcb = _line_count_bucket(line_count)
    agency_l = agency.lower()

    try:
        from src.core.db import get_db
        with get_db() as conn:
            ensure_schema(conn)

            # Level 1: exact agency + qty + line_count
            row = conn.execute(
                f"SELECT {_BAND_COLS} FROM volume_margin_bands "
                "WHERE agency=? AND qty_bucket=? AND line_count_bucket=?",
                (agency_l, qb, lcb),
            ).fetchone()
            if row is not None and (row[3] or 0) >= 10:
                return _row_to_band(row, False, False)

            # Level 2: agency + qty, best-populated line_count cell
            fb2 = conn.execute(
                f"SELECT {_BAND_COLS} FROM volume_margin_bands "
                "WHERE agency=? AND qty_bucket=? "
                "ORDER BY sample_size DESC LIMIT 1",
                (agency_l, qb),
            ).fetchone()
            if fb2 is not None and (fb2[3] or 0) >= 10:
                return _row_to_band(fb2, False, True)

            # Level 3: 'all' + qty + line_count
            fb3 = conn.execute(
                f"SELECT {_BAND_COLS} FROM volume_margin_bands "
                "WHERE agency='all' AND qty_bucket=? AND line_count_bucket=?",
                (qb, lcb),
            ).fetchone()
            if fb3 is not None and (fb3[3] or 0) >= 5:
                return _row_to_band(fb3, True, False)

            # Level 4: 'all' + qty, best-populated line_count cell
            fb4 = conn.execute(
                f"SELECT {_BAND_COLS} FROM volume_margin_bands "
                "WHERE agency='all' AND qty_bucket=? "
                "ORDER BY sample_size DESC LIMIT 1",
                (qb,),
            ).fetchone()
            if fb4 is not None and (fb4[3] or 0) >= 5:
                return _row_to_band(fb4, True, True)

            # Sparse-data fallback: any non-empty hit, flagged so the
            # caller can weight confidence appropriately.
            if row is not None:
                return _row_to_band(row, False, False)
            if fb2 is not None:
                return _row_to_band(fb2, False, True)
            if fb3 is not None:
                return _row_to_band(fb3, True, False)
            if fb4 is not None:
                return _row_to_band(fb4, True, True)
            return None
    except Exception as e:
        log.debug("get_volume_band(%s, %s, %s) DB error: %s",
                  agency, quantity, line_count, e)
        return None


def volume_aware_ceiling(cost: float, agency: str, quantity: float,
                          line_count: Optional[int] = None) -> Optional[dict]:
    """Given cost and context, return {price, markup_pct, band} recommending
    the median-margin ceiling. Caller decides whether to blend or override."""
    if not cost or cost <= 0:
        return None
    band = get_volume_band(agency, quantity, line_count)
    if not band or band.get("p50_margin") is None:
        return None
    p50 = float(band["p50_margin"])
    # margin = (price - cost) / cost, so price = cost * (1 + margin)
    price = round(cost * (1.0 + p50), 2)
    markup_pct = round(p50 * 100.0, 1)
    return {"price": price, "markup_pct": markup_pct, "band": band}


__all__ = [
    "get_volume_band",
    "volume_aware_ceiling",
    "refresh_curve",
    "ensure_schema",
]
