"""Re-classify scprs_po_lines using the real product_catalog match.

Mike P0 2026-05-11 (cross-sell hunting arc): going-forward ingest now
uses `src.core.scprs_classifier.classify_line()` which delegates to
`product_catalog.match_item()` (UPC / supplier SKU / mfg# / token at
0.65 confidence). This script re-runs that classifier across every
existing scprs_po_lines row so the `reytech_sells`, `reytech_sku`,
`opportunity_flag`, and `category` columns reflect the real catalog
instead of the legacy 25-key keyword dict.

Run on prod:
    railway ssh "python scripts/reclassify_scprs_lines_2026_05_11.py"          # dry-run summary
    railway ssh "python scripts/reclassify_scprs_lines_2026_05_11.py --apply"  # commit

Performance: scprs_po_lines has ~150-300K rows. The classifier calls
`match_item()` which hits product_catalog tables. We cache by
(description, item_id) to avoid recomputing for duplicate items —
SCPRS has many repeat purchases of the same product. Expected real
unique-item count is ~5-10K based on scprs_catalog (~4,560 dedup rows).

Idempotent: re-running on already-classified rows is a no-op unless the
catalog grew/changed.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


log = logging.getLogger("reclassify_scprs")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Commit changes (default: dry-run summary)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit rows scanned (0 = all). For smoke-testing.")
    args = parser.parse_args()
    dry_run = not args.apply

    try:
        from src.core.db import get_db
        from src.core.scprs_classifier import classify_line
    except Exception as e:
        log.error("imports failed: %s", e)
        return 1

    cache: dict[tuple[str, str], dict] = {}

    counts = {
        "scanned": 0,
        "would_change": 0,
        "applied": 0,
        "win_back_new": 0,
        "win_back_existing": 0,
        "gap_item_new": 0,
        "other_now": 0,
        "errors": 0,
    }
    sample_changes: list[tuple[int, str, dict, dict]] = []

    log.info("=== RECLASSIFY START (%s) ===",
             "APPLY" if not dry_run else "DRY-RUN")

    with get_db() as conn:
        # Scan all lines.
        sql = """
            SELECT id, description, item_id,
                   reytech_sells, reytech_sku, opportunity_flag, category
            FROM scprs_po_lines
            WHERE COALESCE(is_test, 0) = 0
        """
        if args.limit > 0:
            sql += f" LIMIT {int(args.limit)}"
        rows = conn.execute(sql).fetchall()
        log.info("scanned %d scprs_po_lines rows", len(rows))

        update_buffer: list[tuple] = []
        for r in rows:
            counts["scanned"] += 1
            row_id = r["id"]
            desc = (r["description"] or "").strip()
            pn = (r["item_id"] or "").strip()
            cache_key = (desc.lower(), pn.lower())
            if cache_key in cache:
                new = cache[cache_key]
            else:
                try:
                    new = classify_line(desc, item_id=pn)
                except Exception as e:
                    counts["errors"] += 1
                    log.debug("classify line %d failed: %s", row_id, e)
                    continue
                cache[cache_key] = new

            old = {
                "reytech_sells": int(r["reytech_sells"] or 0),
                "reytech_sku": r["reytech_sku"] or None,
                "opportunity_flag": r["opportunity_flag"] or None,
                "category": r["category"] or None,
            }
            new_norm = {
                "reytech_sells": int(new.get("reytech_sells") or 0),
                "reytech_sku": new.get("reytech_sku") or None,
                "opportunity_flag": new.get("opportunity_flag") or None,
                "category": new.get("category") or None,
            }

            # Aggregate counts on the new state (for the summary)
            flag = new_norm["opportunity_flag"]
            if flag == "WIN_BACK":
                if old["opportunity_flag"] == "WIN_BACK":
                    counts["win_back_existing"] += 1
                else:
                    counts["win_back_new"] += 1
            elif flag == "GAP_ITEM" and old["opportunity_flag"] != "GAP_ITEM":
                counts["gap_item_new"] += 1
            elif flag is None and (new_norm["category"] in (None, "other")):
                counts["other_now"] += 1

            if old == new_norm:
                continue
            counts["would_change"] += 1
            if len(sample_changes) < 10:
                sample_changes.append((row_id, desc[:60], old, new_norm))

            if not dry_run:
                update_buffer.append((
                    new_norm["reytech_sells"],
                    new_norm["reytech_sku"],
                    new_norm["opportunity_flag"],
                    new_norm["category"],
                    row_id,
                ))
                if len(update_buffer) >= 500:
                    conn.executemany(
                        "UPDATE scprs_po_lines SET reytech_sells=?, "
                        "reytech_sku=?, opportunity_flag=?, category=? "
                        "WHERE id=?",
                        update_buffer,
                    )
                    counts["applied"] += len(update_buffer)
                    log.info("  ... flushed %d updates (running total %d)",
                             len(update_buffer), counts["applied"])
                    update_buffer = []

        if not dry_run and update_buffer:
            conn.executemany(
                "UPDATE scprs_po_lines SET reytech_sells=?, "
                "reytech_sku=?, opportunity_flag=?, category=? "
                "WHERE id=?",
                update_buffer,
            )
            counts["applied"] += len(update_buffer)
        if not dry_run:
            conn.commit()

    log.info("")
    log.info("=== RECLASSIFY %s ===", "APPLIED" if not dry_run else "DRY-RUN SUMMARY")
    log.info("scanned:             %d", counts["scanned"])
    log.info("unique items cached: %d", len(cache))
    log.info("rows that would change: %d", counts["would_change"])
    log.info("rows ACTUALLY changed:  %d", counts["applied"])
    log.info("")
    log.info("Flag state after reclassify (full scan, not just changed):")
    log.info("  WIN_BACK new:        %d", counts["win_back_new"])
    log.info("  WIN_BACK kept:       %d", counts["win_back_existing"])
    log.info("  GAP_ITEM new:        %d", counts["gap_item_new"])
    log.info("  other / no flag:     %d", counts["other_now"])
    log.info("  errors during scan:  %d", counts["errors"])

    if sample_changes:
        log.info("")
        log.info("Sample changes (up to 10):")
        for rid, desc, old, new in sample_changes:
            log.info("  row %d  '%s...'", rid, desc)
            log.info("    OLD: sells=%s sku=%s flag=%s cat=%s",
                     old["reytech_sells"], old["reytech_sku"],
                     old["opportunity_flag"], old["category"])
            log.info("    NEW: sells=%s sku=%s flag=%s cat=%s",
                     new["reytech_sells"], new["reytech_sku"],
                     new["opportunity_flag"], new["category"])

    if dry_run:
        log.info("")
        log.info("Dry-run. Pass --apply to commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
