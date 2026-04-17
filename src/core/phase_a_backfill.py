"""Phase A → Oracle feedback backfill (Phase D).

The Oracle V5 calibration and institution-pricing tables have remained
empty because mark-won/lost recording only captures *future* quotes.
But we already have 99 POs / 799 lines of real wins with cost data
sitting in `_phase_a/pilot.sqlite`. That's the largest source of
ground-truth win data the oracle has ever seen — this module
backfills it into:

    won_quotes              — our actual winning line items (per-unit prices)
    oracle_calibration      — category × agency win stats
    institution_pricing_profile — per-agency winning-markup distribution

Idempotent: each PO line is keyed by `drive_file_id + line_number`, so
rerunning only adds net-new rows.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

log = logging.getLogger("reytech.phase_a_backfill")


def _phase_a_path() -> str:
    here = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(here, "_phase_a", "pilot.sqlite")


def backfill_won_quotes(phase_a_db: Optional[str] = None) -> dict:
    """Copy Phase A po_line rows (with non-null qw_unit_price) into the
    main `won_quotes` table. Returns counts."""
    from src.core.db import DB_PATH, get_db

    src_path = phase_a_db or _phase_a_path()
    if not os.path.exists(src_path):
        log.info("backfill_won_quotes: no phase_a db at %s", src_path)
        return {"inserted": 0, "skipped": 0, "total": 0, "source_missing": True}

    src = sqlite3.connect(src_path, timeout=10)
    src.row_factory = sqlite3.Row
    rows = list(src.execute("""
        SELECT l.drive_file_id AS fid, l.line_number, l.po_number, l.description,
               l.mfg_id, l.quantity, l.unit_price, l.qw_unit_cost, l.margin_pct,
               h.agency, h.agency_raw, h.buyer_name, h.po_date
          FROM po_line l
          JOIN po_header h ON l.drive_file_id = h.drive_file_id
         WHERE l.unit_price IS NOT NULL AND l.unit_price > 0
    """))
    src.close()

    inserted = 0
    skipped = 0

    with get_db() as conn:
        for r in rows:
            # Idempotency key: won_quotes has no composite unique; we
            # emulate one by hashing drive_file_id + line_number into
            # the id column as "wq_phasea_<fid>_<line>".
            wq_id = f"wq_phasea_{r['fid']}_{r['line_number']}"
            existing = conn.execute("SELECT 1 FROM won_quotes WHERE id = ?",
                                    (wq_id,)).fetchone()
            if existing:
                skipped += 1
                continue

            desc = (r["description"] or "").strip()
            tokens = _tokenize(desc)
            agency_raw = (r["agency_raw"] or r["agency"] or "").strip()

            conn.execute("""
                INSERT INTO won_quotes
                  (id, po_number, item_number, description, normalized_description,
                   tokens, category, supplier, department, unit_price, quantity,
                   total, award_date, source, confidence, ingested_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """, (
                wq_id,
                r["po_number"] or "",
                r["mfg_id"] or "",
                desc,
                desc.lower(),
                ",".join(tokens),
                _classify(desc),
                "Reytech Inc.",
                agency_raw.upper() if agency_raw else "",
                float(r["unit_price"] or 0),
                int(r["quantity"] or 1),
                float(r["unit_price"] or 0) * int(r["quantity"] or 1),
                r["po_date"] or "",
                "phase_a_drive",
                0.95,
            ))
            inserted += 1

    log.info("backfill_won_quotes: %d inserted, %d skipped (%d total)",
             inserted, skipped, len(rows))
    return {"inserted": inserted, "skipped": skipped, "total": len(rows),
            "source_missing": False}


def backfill_calibration(phase_a_db: Optional[str] = None) -> dict:
    """Populate oracle_calibration (category × agency) from Phase A
    margins. Each po_line with a margin_pct contributes one 'won'
    sample to (category, agency). Recalculates avg_winning_margin
    and recommended_max_markup."""
    from src.core.db import get_db
    from src.core.pricing_oracle_v2 import (_init_calibration_table,
                                              _classify_item_category,
                                              _CAL_MARKUP_FLOOR,
                                              _CAL_MARKUP_CEIL)

    src_path = phase_a_db or _phase_a_path()
    if not os.path.exists(src_path):
        return {"updated_rows": 0, "samples": 0, "source_missing": True}

    src = sqlite3.connect(src_path, timeout=10)
    src.row_factory = sqlite3.Row
    lines = list(src.execute("""
        SELECT l.description, l.margin_pct, h.agency
          FROM po_line l
          JOIN po_header h ON l.drive_file_id = h.drive_file_id
         WHERE l.margin_pct IS NOT NULL
           AND l.margin_pct BETWEEN -2 AND 5
    """))
    src.close()

    # Aggregate (category, agency) → list[margin_pct_as_percent]
    buckets: dict = {}
    for ln in lines:
        cat = _classify_item_category(ln["description"] or "")
        agency = (ln["agency"] or "").upper()
        margin_pct = float(ln["margin_pct"] or 0) * 100.0  # stored as decimal
        buckets.setdefault((cat, agency), []).append(margin_pct)

    with get_db() as conn:
        _init_calibration_table(conn)
        updated = 0
        for (cat, agency), margins in buckets.items():
            if len(margins) < 3:
                continue
            margins.sort()
            avg = sum(margins) / len(margins)
            # p75 of margins as recommended ceiling, clamped
            p75 = margins[int(len(margins) * 0.75)]
            ceiling = max(_CAL_MARKUP_FLOOR, min(_CAL_MARKUP_CEIL, p75))

            conn.execute("""
                INSERT INTO oracle_calibration
                  (category, agency, sample_size, win_count, loss_on_price,
                   loss_on_other, avg_winning_margin, avg_losing_delta,
                   recommended_max_markup, competitor_floor, last_updated)
                VALUES (?, ?, ?, ?, 0, 0, ?, 0, ?, 0, datetime('now'))
                ON CONFLICT(category, agency) DO UPDATE SET
                  sample_size = excluded.sample_size,
                  win_count   = excluded.win_count,
                  avg_winning_margin = excluded.avg_winning_margin,
                  recommended_max_markup = excluded.recommended_max_markup,
                  last_updated = excluded.last_updated
            """, (cat, agency, len(margins), len(margins), avg, ceiling))
            updated += 1

    log.info("backfill_calibration: %d (category, agency) rows, %d samples",
             updated, len(lines))
    return {"updated_rows": updated, "samples": len(lines),
            "source_missing": False}


def backfill_institution_profile(phase_a_db: Optional[str] = None) -> dict:
    """Populate institution_pricing_profile (agency × category) with
    per-agency winning markup stats from Phase A."""
    from src.core.db import get_db
    from src.core.pricing_oracle_v2 import _classify_item_category

    src_path = phase_a_db or _phase_a_path()
    if not os.path.exists(src_path):
        return {"updated_rows": 0, "samples": 0, "source_missing": True}

    src = sqlite3.connect(src_path, timeout=10)
    src.row_factory = sqlite3.Row
    lines = list(src.execute("""
        SELECT l.description, l.margin_pct, h.agency
          FROM po_line l
          JOIN po_header h ON l.drive_file_id = h.drive_file_id
         WHERE l.margin_pct IS NOT NULL
           AND l.margin_pct BETWEEN -2 AND 5
    """))
    src.close()

    buckets: dict = {}
    for ln in lines:
        agency = (ln["agency"] or "").upper()
        if not agency:
            continue
        cat = _classify_item_category(ln["description"] or "")
        margin_pct = float(ln["margin_pct"] or 0) * 100.0
        buckets.setdefault((agency, cat), []).append(margin_pct)
        buckets.setdefault((agency, "general"), []).append(margin_pct)

    with get_db() as conn:
        # Ensure table exists — schema copied from pricing_oracle_v2._get_institution_profile
        conn.execute("""
            CREATE TABLE IF NOT EXISTS institution_pricing_profile (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                institution TEXT NOT NULL, category TEXT DEFAULT 'general',
                avg_winning_markup REAL DEFAULT 25, avg_losing_markup REAL DEFAULT 0,
                win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
                price_sensitivity TEXT DEFAULT 'normal',
                preferred_suppliers TEXT DEFAULT '',
                last_updated TEXT, UNIQUE(institution, category)
            )
        """)

        updated = 0
        for (agency, cat), margins in buckets.items():
            if len(margins) < 3:
                continue
            avg = sum(margins) / len(margins)
            # price_sensitivity: low avg winning margin ⇒ high sensitivity
            if avg < 8:
                sensitivity = "high"
            elif avg > 16:
                sensitivity = "low"
            else:
                sensitivity = "normal"
            conn.execute("""
                INSERT INTO institution_pricing_profile
                  (institution, category, avg_winning_markup, avg_losing_markup,
                   win_count, loss_count, price_sensitivity, preferred_suppliers,
                   last_updated)
                VALUES (?, ?, ?, 0, ?, 0, ?, '', datetime('now'))
                ON CONFLICT(institution, category) DO UPDATE SET
                  avg_winning_markup = excluded.avg_winning_markup,
                  win_count = excluded.win_count,
                  price_sensitivity = excluded.price_sensitivity,
                  last_updated = excluded.last_updated
            """, (agency, cat, avg, len(margins), sensitivity))
            updated += 1

    log.info("backfill_institution_profile: %d (agency, category) rows",
             updated)
    return {"updated_rows": updated, "samples": len(lines),
            "source_missing": False}


def run_full_backfill(phase_a_db: Optional[str] = None) -> dict:
    """Run all three backfills. Safe to call repeatedly."""
    r1 = backfill_won_quotes(phase_a_db)
    r2 = backfill_calibration(phase_a_db)
    r3 = backfill_institution_profile(phase_a_db)
    return {
        "won_quotes": r1,
        "calibration": r2,
        "institution_profile": r3,
    }


# ── helpers ──────────────────────────────────────────────────────────────

def _classify(desc: str) -> str:
    try:
        from src.core.pricing_oracle_v2 import _classify_item_category
        return _classify_item_category(desc)
    except Exception:
        return "general"


def _tokenize(text: str) -> list:
    import re
    toks = re.findall(r"[A-Za-z0-9]{3,}", (text or "").lower())
    # dedupe, keep order
    seen = set()
    out = []
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out[:30]


__all__ = [
    "backfill_won_quotes",
    "backfill_calibration",
    "backfill_institution_profile",
    "run_full_backfill",
]
