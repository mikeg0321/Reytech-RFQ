"""PR-S — auto-recommendations agent.

Mike: "auto recommend to meet /goal."

Reads `operator_drift_line` + `operator_drift_shadow` (PR-I + PR-J
substrate) plus the QA heartbeat freshness state, then emits concrete
per-agency markup-tuning recommendations. The output feeds:
  1. the weekly digest body (oracle_weekly.build_weekly_report)
  2. the in-app /admin/auto-recommendations dashboard
  3. operator-facing banners on agency detail pages (future PR-S.2)

Why this matters for /goal: at 4.8% YTD WR vs 50% target, the bottleneck
is "which markup do we adjust where?" The drift table tells us "how
much over/under oracle did the operator price each line" — and now
that PR-R's cap is active, we can compare cap-active vs non-cap-active
agencies to recommend specific tightenings.

Conservative defaults: never recommend dropping markup if sample size
< MIN_SAMPLE_LINES. Never recommend more than ±5% delta in one cycle —
small steps so a noisy week doesn't flip strategy.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.auto_recommendations")


MIN_SAMPLE_LINES = 10
"""Minimum operator_drift_line rows per agency before we trust the
recommendation. Below this, recommendation = 'insufficient data'.
Mike said the goal is 33 sends/mo with ~10 lines each = 330 lines/mo.
At 7-day windows that's ~75 lines total → ~10 per active agency.
Setting the floor at 10 means we wait for actual signal."""

MAX_DELTA_PCT = 5.0
"""Cap on recommended markup-pct adjustments per cycle. Small steps so
a single noisy week doesn't whip the markup floor 20pp."""

DRIFT_HIGH_THRESHOLD_PCT = 15.0
"""Median drift > this → operator is pricing well above oracle. If
sample is healthy, recommend tightening. Below 15% drift is within
oracle's noise band."""

DRIFT_LOW_THRESHOLD_PCT = -5.0
"""Median drift < this → operator is pricing below oracle. Suspicious
when cost basis is uncertain — could indicate stale supplier_cost or
URL-paste poisoning. Surface as 'investigate cost basis' rather than
'lower markup' (which would be the opposite direction)."""


def _classify_agency(
    line_count: int,
    median_drift_pct: Optional[float],
    capped_lines: int,
    capped_pct: float,
) -> Dict[str, Any]:
    """Bucket an agency into one of five recommendation states.

    Returns {bucket, headline, suggestion, color}.

    Buckets:
      - insufficient_data: line_count < MIN_SAMPLE_LINES
      - drift_high:        operator above oracle by > DRIFT_HIGH
      - drift_low:         operator below oracle by < DRIFT_LOW
      - cap_working:       lots of cap activations, drift moderating
      - on_track:          drift small + healthy sample
    """
    if line_count < MIN_SAMPLE_LINES:
        return {
            "bucket": "insufficient_data",
            "headline": "Insufficient data",
            "suggestion": (
                f"Only {line_count} lines this window — need "
                f"{MIN_SAMPLE_LINES}+ before we'll recommend a change."
            ),
            "color": "neutral",
        }

    if median_drift_pct is not None and median_drift_pct > DRIFT_HIGH_THRESHOLD_PCT:
        delta = min(MAX_DELTA_PCT, round(median_drift_pct / 4, 1))
        return {
            "bucket": "drift_high",
            "headline": f"Pricing {median_drift_pct:.1f}% above oracle",
            "suggestion": (
                f"Operator median sent_price is {median_drift_pct:.1f}% "
                f"above oracle rec across {line_count} lines. Likely "
                f"causing WR loss. Suggest tightening markup floor by "
                f"-{delta:.1f}% (small step — re-evaluate next week)."
            ),
            "color": "warn",
        }

    if median_drift_pct is not None and median_drift_pct < DRIFT_LOW_THRESHOLD_PCT:
        return {
            "bucket": "drift_low",
            "headline": f"Pricing {abs(median_drift_pct):.1f}% below oracle",
            "suggestion": (
                f"Operator median sent_price is {abs(median_drift_pct):.1f}% "
                f"BELOW oracle rec across {line_count} lines. Check whether "
                f"unit_cost source is stale (URL-paste poisoning?) or "
                f"oracle is over-recommending. Don't lower markup further."
            ),
            "color": "warn",
        }

    # Cap-impact heuristic: if >50% of lines are getting capped, the
    # SCPRS p75 ceiling is doing the heavy lifting — operator's
    # untouched markup is too aggressive but the cap masks it.
    if capped_pct > 50 and line_count >= MIN_SAMPLE_LINES:
        return {
            "bucket": "cap_working",
            "headline": f"Cap binding {capped_pct:.0f}% of lines",
            "suggestion": (
                f"SCPRS rollup cap is doing the work — {capped_lines}/{line_count} "
                f"lines auto-lowered to p75. Manual markup is too high "
                f"but cap is masking it. Suggest lowering base markup "
                f"floor by -2.0% so cap fires less often (saves operator "
                f"the 'why did this price drop' confusion)."
            ),
            "color": "info",
        }

    return {
        "bucket": "on_track",
        "headline": "Drift within tolerance",
        "suggestion": (
            f"Median drift {median_drift_pct or 0:.1f}% across "
            f"{line_count} lines is within oracle's noise band. "
            f"No markup adjustment recommended. Watch WR for confirmation."
        ),
        "color": "good",
    }


def build_auto_recommendations(window_days: int = 7) -> Dict[str, Any]:
    """Top-level builder. Returns a dict the digest + admin view consume.

    Shape:
      {
        ok, window_days, generated_at,
        total_lines, total_quotes, total_agencies,
        recommendations: [
          {agency, line_count, quote_count, median_drift_pct,
           capped_lines, capped_pct, bucket, headline, suggestion, color},
          ...
        ],
        summary_line: "...",
      }
    """
    from src.core.operator_kpi import get_drift_lines_per_agency
    per_agency = get_drift_lines_per_agency(window_days)
    total_lines = sum(d["line_count"] for d in per_agency)
    total_quotes = sum(d["quote_count"] for d in per_agency)

    recommendations: List[Dict[str, Any]] = []
    for d in per_agency:
        classification = _classify_agency(
            d["line_count"],
            d["median_drift_pct"],
            d["capped_lines"],
            d["capped_pct"],
        )
        recommendations.append({**d, **classification})

    # Compose a one-line summary suitable for the digest header
    actionable = [r for r in recommendations
                  if r["bucket"] in ("drift_high", "drift_low", "cap_working")]
    if not recommendations:
        summary = (
            f"No operator_drift_line rows in last {window_days}d — "
            f"no recommendations possible (was Mark-Sent used?)"
        )
    elif not actionable:
        summary = (
            f"All {len(recommendations)} active agencies on track — "
            f"no markup changes recommended this cycle."
        )
    else:
        summary = (
            f"{len(actionable)} of {len(recommendations)} agencies need "
            f"attention: "
            + ", ".join(f"{r['agency']} ({r['bucket']})" for r in actionable[:5])
        )

    return {
        "ok": True,
        "window_days": window_days,
        "generated_at": datetime.now().isoformat(),
        "total_lines": total_lines,
        "total_quotes": total_quotes,
        "total_agencies": len(per_agency),
        "recommendations": recommendations,
        "summary_line": summary,
    }


def format_for_digest(report: Dict[str, Any]) -> List[str]:
    """Render the auto-recommendations section as plain-text lines for the
    weekly digest body. Returns a list of strings (caller joins with \\n).
    """
    out: List[str] = []
    out.append("AUTO-RECOMMENDATIONS (PR-S)")
    out.append(f"  {report['summary_line']}")
    if not report["recommendations"]:
        return out
    out.append("")
    for r in report["recommendations"][:8]:  # top 8 by volume
        out.append(
            f"  • {r['agency']}: {r['headline']}  "
            f"[lines={r['line_count']}, quotes={r['quote_count']}, "
            f"capped={r['capped_pct']:.0f}%]"
        )
        out.append(f"      → {r['suggestion']}")
    return out
