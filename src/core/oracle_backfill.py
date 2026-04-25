"""Oracle calibration backfill — feed all historical won/lost data
through calibrate_from_outcome() so the oracle has real pricing
intelligence immediately.

Background: markQuote() was a silent no-op Feb 17 → Apr 15. During that
window, the oracle feedback loop got zero data. Now that markQuote works
(PR #95), we backfill from every available source:

1. Quotes DB (quotes table) — status='won' or 'lost'
2. Award tracker matches (quote_po_matches table) — competitor wins
3. won_quotes_kb (SCPRS-derived knowledge base) — historical agency-level
   bid outcomes where Reytech bid and we know the winning price.
   This is the largest data source: 1,260+ rows of per-product
   per-agency win/loss signal that the original backfill ignored.
   Phase 0.7 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25).

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
        "kb_wins": 0, "kb_losses": 0, "kb_skipped_no_bid": 0,
        "calibrations_written": 0,
        "errors": [],
        # IN-12: per-agency error histogram. Operator asking "52 errors,
        # is one agency broken or is this scatter?" couldn't tell before.
        # Now the ops dashboard shows {"CCHCS": 47, "CDCR": 3, ...}.
        "errors_by_agency": {},
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
                # IN-12: bucket by agency so a broken-agency pattern shows up
                _ag = (r["agency"] or r["institution"] or "unknown").strip() or "unknown"
                result["errors_by_agency"][_ag] = result["errors_by_agency"].get(_ag, 0) + 1
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
                # IN-12: match table has no agency column, log under generic bucket
                result["errors_by_agency"]["po_match"] = result["errors_by_agency"].get("po_match", 0) + 1

    except Exception as e:
        result["errors"].append(f"quote_po_matches: {e}")
        log.debug("backfill matches: %s", e)

    # ── 3. won_quotes_kb (the bulk of historical data) ──
    # Each row is one bid outcome: (item, agency, winner, winner_price,
    # whether Reytech won, Reytech's price). When Reytech bid we feed
    # calibrate_from_outcome with real data. When Reytech didn't bid the
    # row is market intelligence, not a calibration input — skip it here
    # (a separate pricing-history pipeline consumes those rows).
    try:
        from src.core.db import get_db
        with get_db() as conn:
            kb_rows = conn.execute("""
                SELECT item_description, mfg_number, agency,
                       winning_price, winning_vendor,
                       reytech_won, reytech_price
                FROM won_quotes_kb
                WHERE reytech_price IS NOT NULL AND reytech_price > 0
            """).fetchall()

        for r in kb_rows:
            try:
                desc = (r["item_description"] or "").strip()
                if not desc:
                    continue
                agency = (r["agency"] or "").strip()
                reytech_price = float(r["reytech_price"] or 0)
                winner_price = float(r["winning_price"] or 0)
                won = bool(r["reytech_won"])

                items = [{
                    "description": desc,
                    "mfg_number": r["mfg_number"] or "",
                    "unit_price": reytech_price,
                }]

                if won:
                    if not dry_run:
                        calibrate_from_outcome(items, "won", agency=agency)
                        result["calibrations_written"] += 1
                    result["kb_wins"] += 1
                else:
                    winner_prices = {0: winner_price} if winner_price > 0 else None
                    if not dry_run:
                        calibrate_from_outcome(
                            items, "lost",
                            agency=agency,
                            loss_reason="price",
                            winner_prices=winner_prices,
                        )
                        result["calibrations_written"] += 1
                    result["kb_losses"] += 1

            except Exception as e:
                result["errors"].append(f"kb {r['item_description'][:40]}: {e}")
                _ag = (r["agency"] or "unknown").strip() or "unknown"
                result["errors_by_agency"][_ag] = result["errors_by_agency"].get(_ag, 0) + 1

        # Also count rows where Reytech didn't bid — useful context for the operator.
        with get_db() as conn:
            skipped = conn.execute("""
                SELECT COUNT(*) c FROM won_quotes_kb
                WHERE reytech_price IS NULL OR reytech_price <= 0
            """).fetchone()
            result["kb_skipped_no_bid"] = int(skipped[0] if skipped else 0)

    except Exception as e:
        result["errors"].append(f"won_quotes_kb: {e}")
        log.warning("backfill won_quotes_kb: %s", e)

    log.info(
        "Oracle backfill complete: quotes(%d won + %d lost), "
        "kb(%d wins + %d losses, %d skipped no-bid), "
        "%d calibrations written, %d errors, dry_run=%s",
        result["quotes_won"], result["quotes_lost"],
        result["kb_wins"], result["kb_losses"], result["kb_skipped_no_bid"],
        result["calibrations_written"], len(result["errors"]),
        dry_run,
    )
    return result
