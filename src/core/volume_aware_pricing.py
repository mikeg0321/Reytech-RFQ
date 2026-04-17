"""Volume-Aware Pricing (Phase B).

Returns the (p25, p50, p75) line-level margin band that historically won
deals, conditioned on (agency, line-item quantity). Based on 99 POs /
799 lines ingested in Phase A from Drive PO corpus joined to
QuoterWerks cost data (_phase_a/pilot.sqlite).

Mike's thesis — validated at the data level — is that unit margin drops
sharply with quantity. A 2-item request sustains ~15% margin; a
100-item request wins at ~6%. Blind 30% markup on a large order loses
the bid. Blind 30% markup on a singleton leaves money on the table.

Public API:
    get_volume_band(agency, quantity, line_count=None) -> dict | None
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


def _qty_bucket(qty: float) -> str:
    try:
        q = int(qty or 0)
    except (TypeError, ValueError):
        return "qty_1_2"
    for name, lo, hi in _QTY_BUCKETS:
        if lo <= q <= hi:
            return name
    return "qty_1_2"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS volume_margin_bands (
        agency TEXT NOT NULL,
        qty_bucket TEXT NOT NULL,
        sample_size INTEGER NOT NULL,
        p25_margin REAL,
        p50_margin REAL,
        p75_margin REAL,
        avg_unit_cost REAL,
        avg_unit_price REAL,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (agency, qty_bucket)
    );
    """)


def refresh_curve(phase_a_db: Optional[str] = None) -> int:
    """Rebuild volume_margin_bands from _phase_a/pilot.sqlite.
    Returns rows written. Safe to call on cold start; short-circuits
    cleanly when pilot db is missing."""
    from src.core.db import DB_PATH

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
        rows = list(src.execute("""
            SELECT h.agency AS agency,
                   l.quantity AS quantity,
                   l.qw_unit_cost AS cost,
                   l.unit_price AS price,
                   l.margin_pct AS margin
              FROM po_line l
              JOIN po_header h ON l.drive_file_id = h.drive_file_id
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
        bucket = _qty_bucket(r["quantity"])
        buckets.setdefault((agency, bucket), []).append({
            "margin": r["margin"],
            "cost": r["cost"] or 0,
            "price": r["price"] or 0,
        })
        buckets.setdefault(("all", bucket), []).append({
            "margin": r["margin"],
            "cost": r["cost"] or 0,
            "price": r["price"] or 0,
        })

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
        for (agency, bucket), lines in buckets.items():
            if len(lines) < 3:
                continue
            margins = [x["margin"] for x in lines]
            costs = [x["cost"] for x in lines if x["cost"] > 0]
            prices = [x["price"] for x in lines if x["price"] > 0]
            dst.execute("""
                INSERT INTO volume_margin_bands
                  (agency, qty_bucket, sample_size, p25_margin, p50_margin, p75_margin,
                   avg_unit_cost, avg_unit_price, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                agency, bucket, len(lines),
                _pct(margins, 0.25), _pct(margins, 0.50), _pct(margins, 0.75),
                (sum(costs) / len(costs)) if costs else None,
                (sum(prices) / len(prices)) if prices else None,
            ))
            written += 1
    log.info("volume_margin_bands refreshed: %d (agency, qty_bucket) rows", written)
    return written


def get_volume_band(agency: str, quantity: float,
                    line_count: Optional[int] = None) -> Optional[dict]:
    """Return the historical margin band for this agency + qty bucket.

    Falls back to the 'all' agency pool when agency-specific data is
    thin (n < 10). Returns None if even the pooled band has fewer than
    5 samples — caller should skip volume-aware pricing in that case.

    Shape:
      {
        "agency": "cchcs",     # matched bucket (may be 'all' on fallback)
        "qty_bucket": "qty_3_10",
        "sample_size": 27,
        "p25_margin": 0.09,     # decimal margins (0.09 = 9%)
        "p50_margin": 0.13,
        "p75_margin": 0.18,
        "avg_unit_cost": 48.21,
        "avg_unit_price": 54.40,
        "used_fallback": False,
      }
    """
    if not agency:
        agency = "other"
    bucket = _qty_bucket(quantity)

    try:
        from src.core.db import get_db
        with get_db() as conn:
            ensure_schema(conn)
            row = conn.execute("""
                SELECT agency, qty_bucket, sample_size, p25_margin, p50_margin,
                       p75_margin, avg_unit_cost, avg_unit_price
                  FROM volume_margin_bands
                 WHERE agency = ? AND qty_bucket = ?
            """, (agency.lower(), bucket)).fetchone()
            used_fallback = False
            if row is None or (row[2] or 0) < 10:
                # Fall back to pooled 'all' bucket
                fb = conn.execute("""
                    SELECT agency, qty_bucket, sample_size, p25_margin, p50_margin,
                           p75_margin, avg_unit_cost, avg_unit_price
                      FROM volume_margin_bands
                     WHERE agency = 'all' AND qty_bucket = ?
                """, (bucket,)).fetchone()
                if fb is not None and (fb[2] or 0) >= 5:
                    row = fb
                    used_fallback = True
                elif row is None:
                    return None
    except Exception as e:
        log.debug("get_volume_band(%s, %s) DB error: %s", agency, quantity, e)
        return None

    return {
        "agency": row[0],
        "qty_bucket": row[1],
        "sample_size": row[2],
        "p25_margin": row[3],
        "p50_margin": row[4],
        "p75_margin": row[5],
        "avg_unit_cost": row[6],
        "avg_unit_price": row[7],
        "used_fallback": used_fallback,
    }


def volume_aware_ceiling(cost: float, agency: str, quantity: float) -> Optional[dict]:
    """Given cost and context, return {price, markup_pct, band} recommending
    the median-margin ceiling. Caller decides whether to blend or override."""
    if not cost or cost <= 0:
        return None
    band = get_volume_band(agency, quantity)
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
