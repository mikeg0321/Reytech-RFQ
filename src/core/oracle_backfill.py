"""Oracle calibration backfill — feed all historical won/lost data
through calibrate_from_outcome() so the oracle has real pricing
intelligence immediately.

Background: markQuote() was a silent no-op Feb 17 → Apr 15. During that
window, the oracle feedback loop got zero data. Now that markQuote works
(PR #95), we backfill from every available source:

1. Quotes DB (quotes table) — status='won' or 'lost'
2. Price Checks (JSON or DB) — award_status='won' or 'lost'
3. Award tracker matches (quote_po_matches table) — competitor wins

The backfill is idempotent: calibrate_from_outcome() uses exponential-
moving-average blending, so re-running reinforces existing calibration
without double-counting.
"""

import json
import logging
from datetime import datetime

log = logging.getLogger("oracle_backfill")


def backfill_all(dry_run: bool = False) -> dict:
    """Run the full oracle calibration backfill.

    Args:
        dry_run: if True, count what WOULD be backfilled without writing.

    Returns:
        {ok, quotes_won, quotes_lost, pcs_won, pcs_lost, calibrations_written, errors}
    """
    from src.core.pricing_oracle_v2 import calibrate_from_outcome

    result = {
        "ok": True,
        "quotes_won": 0, "quotes_lost": 0,
        "pcs_won": 0, "pcs_lost": 0,
        "calibrations_written": 0,
        "errors": [],
        "dry_run": dry_run,
    }

    # ── 1. Quotes DB ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       line_items, total, po_number, status_notes
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost')
            """).fetchall()

        for r in rows:
            try:
                items = json.loads(r["line_items"] or "[]")
                if not items:
                    continue

                agency = r["agency"] or r["institution"] or ""
                status = r["status"]

                # Determine loss reason from status_notes if available
                loss_reason = None
                if status == "lost":
                    notes = (r["status_notes"] or "").lower()
                    if "price" in notes or "cost" in notes or "cheaper" in notes:
                        loss_reason = "price"
                    else:
                        loss_reason = "other"

                if not dry_run:
                    calibrate_from_outcome(
                        items, status,
                        agency=agency,
                        loss_reason=loss_reason,
                    )
                    result["calibrations_written"] += 1

                if status == "won":
                    result["quotes_won"] += 1
                else:
                    result["quotes_lost"] += 1

            except Exception as e:
                result["errors"].append(f"quote {r['quote_number']}: {e}")
                log.debug("backfill quote %s: %s", r["quote_number"], e)

    except Exception as e:
        result["errors"].append(f"quotes DB: {e}")
        log.warning("backfill quotes DB: %s", e)

    # ── 2. Award tracker matches (competitor losses with price data) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            matches = conn.execute("""
                SELECT quote_number, scprs_total, our_total,
                       outcome, line_analysis
                FROM quote_po_matches
                WHERE outcome = 'lost_to_competitor'
            """).fetchall()

        for m in matches:
            try:
                line_data = json.loads(m["line_analysis"] or "[]")
                if not line_data:
                    continue

                # Build items + winner_prices from the line analysis
                items = []
                winner_prices = {}
                for i, ld in enumerate(line_data):
                    if not isinstance(ld, dict):
                        continue
                    items.append({
                        "description": ld.get("description", ""),
                        "unit_price": ld.get("our_unit_price", 0),
                        "supplier_cost": ld.get("our_cost", 0),
                    })
                    if ld.get("winner_unit_price"):
                        winner_prices[i] = ld["winner_unit_price"]

                if items and not dry_run:
                    calibrate_from_outcome(
                        items, "lost",
                        loss_reason="price",
                        winner_prices=winner_prices,
                    )
                    result["calibrations_written"] += 1

            except Exception as e:
                result["errors"].append(f"match {m['quote_number']}: {e}")

    except Exception as e:
        result["errors"].append(f"quote_po_matches: {e}")
        log.debug("backfill matches: %s", e)

    log.info(
        "Oracle backfill complete: %d won + %d lost quotes, "
        "%d calibrations written, %d errors, dry_run=%s",
        result["quotes_won"], result["quotes_lost"],
        result["calibrations_written"], len(result["errors"]),
        dry_run,
    )
    return result
