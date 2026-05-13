"""WR-collapse forensics — 2026-05-13.

The product-engineer audit surfaced: 2026 YTD WR is 4.8% (1 win / 20 losses
out of 21 decided). That's a 14-point YoY drop from 2025 (18.5%) and 32
points below the 2023 peak (37.1%). Most alarming: CCHCS — the bread-and-
butter agency — is 0 wins out of 4 in 2026.

Three hypotheses to test:

  H1. **Pricing math shifted high.** PR #939 (markup-sanity gate, 2026-05-13)
      and surrounding recipes might have biased quote totals above market.
      → Test: average margin_pct and total/items_count distributions by year.
      → If 2026 totals/margins are systematically higher than 2025, this is it.

  H2. **award_monitor is over-aggressive.** The lifecycle path
      `quote_lifecycle.process_reply_signal` + `award_monitor.run_award_check`
      can flip quotes to `lost` based on SCPRS detection of "another vendor
      won the matched PO." If the matcher got more permissive, false losses
      would tank WR even if pricing is fine.
      → Test: distribution of `status_notes`/`notes`/`closed_by_agent` on
      2026 losses. If most carry an award_monitor signature, hypothesis 2.
      → Compare to 2025 losses with the same `closed_by_agent` tagging —
      ratio shifts mean the matcher changed behavior.

  H3. **Real, price-driven losses.** We're actually pricing too high for
      the post-shift CCHCS market.
      → Test: for the 20 known 2026 losses, look at competitor_price vs
      our_price in `competitor_intel`. If our_price is systematically above
      competitor_price by a wide margin, hypothesis 3.

Run:
    py scripts/wr_collapse_forensics.py
    py scripts/wr_collapse_forensics.py --db /path/to/prod.db

Output: JSON dict with each query's result. Pipe to jq or eyeball.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys


# ── Queries (each tests a specific hypothesis) ───────────────────────────────


# Q1: BASELINE — 2026 vs 2025 status distribution
# What we expect: pre-2026 distribution should look similar to 2026 modulo
# total quote count. If 2026 has wildly different ratios of won/lost/sent/
# expired, something structural changed.
Q1_STATUS_DISTRIBUTION = """
SELECT SUBSTR(COALESCE(created_at, ''), 1, 4) AS year,
       status,
       COUNT(*) AS n,
       ROUND(AVG(COALESCE(total, 0)), 2) AS avg_total,
       ROUND(AVG(COALESCE(margin_pct, 0)), 2) AS avg_margin_pct
  FROM quotes
 WHERE is_test = 0
   AND created_at >= '2024-01-01'
 GROUP BY year, status
 ORDER BY year, status;
"""


# Q2: H1 SUSPECT — YoY margin trajectory on WON quotes only
# H1 hypothesis: if our pricing math biased high, the margin we win at
# should be similar but the margin we LOSE at should be higher than prior
# years. Compare avg_margin on won vs lost quotes per year.
Q2_MARGIN_BY_OUTCOME = """
SELECT SUBSTR(COALESCE(created_at, ''), 1, 4) AS year,
       status,
       COUNT(*) AS n,
       ROUND(AVG(COALESCE(margin_pct, 0)), 2) AS avg_margin_pct,
       ROUND(AVG(COALESCE(total, 0)), 2) AS avg_total,
       ROUND(AVG(COALESCE(total_cost, 0)), 2) AS avg_cost
  FROM quotes
 WHERE is_test = 0
   AND status IN ('won', 'lost')
   AND created_at >= '2024-01-01'
 GROUP BY year, status
 ORDER BY year, status;
"""


# Q3: H2 SUSPECT — closed_by_agent attribution on 2026 losses
# H2 hypothesis: award_monitor (or quote_lifecycle) is over-aggressively
# flipping `sent` → `lost`. If 2026 losses carry `closed_by_agent IN
# ('award_monitor', 'quote_lifecycle')` at a higher rate than 2024/2025
# losses, the matcher's the issue. Note: column may not exist on older
# DBs — keep a graceful fallback.
Q3_LOSS_SOURCE_ATTRIBUTION = """
SELECT SUBSTR(COALESCE(created_at, ''), 1, 4) AS year,
       COALESCE(NULLIF(TRIM(notes), ''),
                NULLIF(TRIM(status_notes), ''),
                '(empty)') AS reason_text,
       COUNT(*) AS n,
       ROUND(AVG(COALESCE(total, 0)), 2) AS avg_total
  FROM quotes
 WHERE is_test = 0
   AND status = 'lost'
   AND created_at >= '2024-01-01'
 GROUP BY year, reason_text
 ORDER BY year, n DESC
 LIMIT 60;
"""


# Q4: 2026 LOSS DEEP-DIVE — agency × close_reason
# Where in 2026 are the losses landing, and what's the stated reason?
# This is the SQL the agent ran (in spirit) to surface CCHCS 0/4.
Q4_2026_AGENCY_BREAKDOWN = """
SELECT COALESCE(NULLIF(TRIM(agency), ''), TRIM(institution), '(unknown)') AS bucket,
       status,
       COUNT(*) AS n,
       ROUND(SUM(COALESCE(total, 0)), 2) AS total_dollars
  FROM quotes
 WHERE is_test = 0
   AND created_at >= '2026-01-01'
 GROUP BY bucket, status
 ORDER BY bucket, status;
"""


# Q5: H3 SUSPECT — competitor_price vs our_price on resolved losses
# H3 hypothesis: we genuinely lost on price. competitor_intel carries
# competitor_price + our_price. If our_price is systematically above
# competitor by a wide margin in 2026, hypothesis 3 is the real driver.
Q5_COMPETITIVE_GAP = """
SELECT SUBSTR(COALESCE(found_at, ''), 1, 4) AS year,
       COUNT(*) AS n_losses,
       ROUND(AVG(our_price), 2) AS avg_our,
       ROUND(AVG(competitor_price), 2) AS avg_them,
       ROUND(AVG(price_delta_pct), 2) AS avg_gap_pct,
       ROUND(MIN(price_delta_pct), 2) AS min_gap_pct,
       ROUND(MAX(price_delta_pct), 2) AS max_gap_pct
  FROM competitor_intel
 WHERE found_at >= '2024-01-01'
 GROUP BY year
 ORDER BY year;
"""


# Q6: TIMING — when in 2026 did the collapse start?
# Bucket by quarter to surface a discontinuity. If the WR cliffs between
# two adjacent windows, the cause is likely a deploy event in that gap.
Q6_2026_BY_MONTH = """
SELECT SUBSTR(COALESCE(created_at, ''), 1, 7) AS yyyymm,
       SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS losses,
       SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
       COUNT(*) AS total_created
  FROM quotes
 WHERE is_test = 0
   AND created_at >= '2025-06-01'
 GROUP BY yyyymm
 ORDER BY yyyymm;
"""


# Q7: CCHCS-specific deep-dive (the agent flagged 0/4 in 2026)
# What changed for CCHCS specifically? Pull the 4 quotes, see their items,
# margins, close reasons.
Q7_CCHCS_2026_DETAIL = """
SELECT quote_number, created_at, status,
       ROUND(total, 2) AS total,
       ROUND(margin_pct, 2) AS margin_pct,
       items_count,
       COALESCE(NULLIF(TRIM(status_notes), ''), TRIM(notes), '') AS close_reason,
       po_number
  FROM quotes
 WHERE is_test = 0
   AND created_at >= '2026-01-01'
   AND (LOWER(agency) LIKE '%cchcs%'
        OR LOWER(institution) LIKE '%cchcs%'
        OR LOWER(institution) LIKE '%correctional health%')
 ORDER BY created_at DESC;
"""


QUERIES = [
    ("Q1_status_distribution",       Q1_STATUS_DISTRIBUTION),
    ("Q2_margin_by_outcome",         Q2_MARGIN_BY_OUTCOME),
    ("Q3_loss_source_attribution",   Q3_LOSS_SOURCE_ATTRIBUTION),
    ("Q4_2026_agency_breakdown",     Q4_2026_AGENCY_BREAKDOWN),
    ("Q5_competitive_gap",           Q5_COMPETITIVE_GAP),
    ("Q6_2026_by_month",             Q6_2026_BY_MONTH),
    ("Q7_cchcs_2026_detail",         Q7_CCHCS_2026_DETAIL),
]


# ── Runner ──────────────────────────────────────────────────────────────────


def _resolve_db_path(explicit: str | None) -> str:
    if explicit:
        return explicit
    # Mirror src/core/paths.py default
    env = os.environ.get("REYTECH_DB_PATH")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(here), "data", "reytech.db")


def run(db_path: str) -> dict:
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found at {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    out: dict = {"db_path": db_path, "queries": {}}
    for name, sql in QUERIES:
        try:
            rows = conn.execute(sql).fetchall()
            out["queries"][name] = [dict(r) for r in rows]
        except sqlite3.OperationalError as e:
            # Column likely doesn't exist on this DB — log and continue.
            out["queries"][name] = {"error": str(e)}
    conn.close()
    return out


# ── Diagnostic interpretation hints ─────────────────────────────────────────


HYPOTHESIS_GUIDE = """
INTERPRETATION GUIDE
====================

H1 (pricing math biased high) is supported when:
  - Q2 shows avg_margin_pct on 2026 'won' quotes ≥ historical 2024/2025
    avg_margin_pct on 'won' (we're winning at higher margin)
  - AND Q2 shows avg_margin_pct on 2026 'lost' quotes ≥ historical 'lost'
    by a wider gap (we're losing where we used to win)
  - Q1's 2026 avg_total / avg_margin_pct on ALL quotes is meaningfully
    higher than 2025

H2 (award_monitor over-aggressive) is supported when:
  - Q3's 2026 'reason_text' is dominated by award_monitor-shaped strings
    (e.g. "SCPRS PO ... awarded to ...", "Marked LOST by award_monitor", etc.)
    in a higher ratio than 2024/2025
  - Q5 shows price_delta_pct close to zero or NEGATIVE for 2026 losses
    (we were CHEAPER than the supposed winner — false-positive match)

H3 (real price losses) is supported when:
  - Q5 shows 2026's avg_gap_pct meaningfully positive (we were more
    expensive than the competitor by a wide margin)
  - AND Q3 reasons mostly look like real competitor PO references, not
    matcher heuristics

WATCHPOINTS:
  - 2026 is mid-year — sample sizes are small (21 decided). Trust
    direction not exact pct.
  - Q6 (by month) is the discontinuity detector. If WR was holding
    through Feb/Mar/Apr then cliffed in May, look for a deploy event
    in the gap. If it was bad from January, the cause is upstream of
    this session.
  - Q7's CCHCS rows are the smoking gun. If all 4 carry the SAME
    close_reason boilerplate, that's the award_monitor signature.
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="Path to reytech.db")
    ap.add_argument("--pretty", action="store_true",
                    help="Pretty-print JSON output")
    ap.add_argument("--guide", action="store_true",
                    help="Print interpretation guide and exit")
    args = ap.parse_args()
    if args.guide:
        print(HYPOTHESIS_GUIDE)
        return
    db = _resolve_db_path(args.db)
    result = run(db)
    indent = 2 if args.pretty else None
    json.dump(result, sys.stdout, indent=indent, default=str)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
