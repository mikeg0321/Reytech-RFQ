"""
Template Learning — Flywheel strategy database.

Records which fill strategy works for which template fingerprint, so the
pipeline can skip known-failing approaches on future generations. Also
captures buyer feedback and form change events for systemic improvement.

Every generation → record_outcome() → enriches the DB
Next same-template → get_best_strategy() → skip to proven approach
"""

import os
import hashlib
import logging
from typing import Optional

log = logging.getLogger("reytech.template_learning")


# ═══════════════════════════════════════════════════════════════════════════
# TEMPLATE FINGERPRINTING
# ═══════════════════════════════════════════════════════════════════════════

def template_fingerprint(pdf_path: str) -> str:
    """Compute a structural fingerprint for a PDF template.

    Fingerprint is based on: page count, field count, sorted field name hash,
    and page dimensions. NOT content-dependent — different buyer data on the
    same template produces the same fingerprint.

    Returns:
        A hex string fingerprint, e.g. "2p_45f_a1b2c3d4"
    """
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        page_count = len(reader.pages)
        fields = reader.get_fields() or {}
        field_count = len(fields)

        # Hash sorted field names for structural identity
        field_names = sorted(fields.keys())
        name_hash = hashlib.md5(
            "|".join(field_names).encode()
        ).hexdigest()[:8]

        # Page dimensions (from first page)
        dims = ""
        if reader.pages:
            box = reader.pages[0].mediabox
            w = int(float(box.width))
            h = int(float(box.height))
            dims = f"_{w}x{h}"

        return f"{page_count}p_{field_count}f_{name_hash}{dims}"
    except Exception as e:
        log.warning("template_fingerprint failed: %s", e)
        # Fallback: hash file size + first 1KB
        try:
            stat = os.stat(pdf_path)
            with open(pdf_path, "rb") as f:
                head = f.read(1024)
            h = hashlib.md5(head).hexdigest()[:8]
            return f"0p_0f_{h}_{stat.st_size}"
        except Exception:
            return "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY OUTCOME RECORDING
# ═══════════════════════════════════════════════════════════════════════════

def _get_conn():
    """Get a database connection."""
    from src.core.db import get_db
    return get_db()


def record_outcome(fingerprint: str, strategy: str, score: int,
                   source_type: str = "", pc_id: str = "",
                   buyer_agency: str = ""):
    """Record that a strategy produced a given score for this template type.

    Called by DocumentPipeline after each generation attempt.
    """
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO template_strategies "
                "(fingerprint, strategy, score, source_type, pc_id, buyer_agency, event_type) "
                "VALUES (?, ?, ?, ?, ?, ?, 'generation')",
                (fingerprint, strategy, score, source_type, pc_id, buyer_agency)
            )
        log.info("template_learning: recorded %s strategy=%s score=%d (fp=%s)",
                 pc_id or "unknown", strategy, score, fingerprint[:20])
    except Exception as e:
        log.warning("template_learning: record_outcome failed: %s", e)


def get_best_strategy(fingerprint: str, min_samples: int = 2) -> Optional[str]:
    """Get the best-performing strategy for this template fingerprint.

    Only returns a recommendation if we have at least `min_samples` data points
    and the best strategy scored 100 at least once.

    Returns:
        Strategy name ("form_fields", "overlay", "blank_template") or None.
    """
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT strategy, AVG(score) as avg_score, COUNT(*) as cnt, MAX(score) as max_score "
                "FROM template_strategies "
                "WHERE fingerprint = ? AND event_type = 'generation' "
                "GROUP BY strategy "
                "HAVING cnt >= ? "
                "ORDER BY avg_score DESC",
                (fingerprint, min_samples)
            ).fetchall()
            if rows:
                best = rows[0]
                if best["max_score"] >= 100:
                    log.info("template_learning: recommending '%s' for fp=%s (avg=%.0f, n=%d)",
                             best["strategy"], fingerprint[:20],
                             best["avg_score"], best["cnt"])
                    return best["strategy"]
        return None
    except Exception as e:
        log.warning("template_learning: get_best_strategy failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# BUYER FEEDBACK RECORDING
# ═══════════════════════════════════════════════════════════════════════════

def record_buyer_feedback(pc_id: str, feedback_type: str, detail: str,
                          fingerprint: str = ""):
    """Record buyer-reported issues for template learning.

    feedback_type: "field_error" | "missing_form" | "wrong_format" | "resubmit_requested"
    Called when: reply_analyzer detects correction request, or user manually flags.
    """
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO template_strategies "
                "(fingerprint, strategy, score, pc_id, event_type, detail) "
                "VALUES (?, 'buyer_feedback', 0, ?, ?, ?)",
                (fingerprint or "unknown", pc_id, feedback_type, detail[:500])
            )
        log.info("template_learning: buyer feedback recorded for %s: %s",
                 pc_id, feedback_type)
    except Exception as e:
        log.warning("template_learning: record_buyer_feedback failed: %s", e)


def record_template_change(form_id: str, added_fields: list, removed_fields: list):
    """Record that a DGS form template has changed (called by form_updater).

    This signals the pipeline that cached strategies for this form type
    may need re-evaluation.
    """
    detail = ""
    if added_fields:
        detail += f"added: {', '.join(added_fields[:5])}"
    if removed_fields:
        detail += f" removed: {', '.join(removed_fields[:5])}"
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT INTO template_strategies "
                "(fingerprint, strategy, score, event_type, detail) "
                "VALUES (?, 'form_update', 0, 'template_change', ?)",
                (f"form:{form_id}", detail[:500])
            )
        log.info("template_learning: form change recorded for %s: %s",
                 form_id, detail[:100])
    except Exception as e:
        log.warning("template_learning: record_template_change failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════════
# ANALYTICS / DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

def get_strategy_stats(days: int = 90) -> dict:
    """Return success rates per strategy across all templates.

    Returns: {
        "form_fields": {"total": 100, "perfect": 85, "avg_score": 92.3},
        "overlay": {"total": 30, "perfect": 22, "avg_score": 88.1},
        "blank_template": {"total": 10, "perfect": 9, "avg_score": 95.0},
        "top_failing": [{"fingerprint": "...", "failures": 5, "last_score": 72}],
    }
    """
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT strategy, COUNT(*) as total, "
                "SUM(CASE WHEN score = 100 THEN 1 ELSE 0 END) as perfect, "
                "AVG(score) as avg_score "
                "FROM template_strategies "
                "WHERE event_type = 'generation' "
                "AND created_at > datetime('now', ?) "
                "GROUP BY strategy",
                (f"-{days} days",)
            ).fetchall()

            stats = {}
            for row in rows:
                stats[row["strategy"]] = {
                    "total": row["total"],
                    "perfect": row["perfect"],
                    "avg_score": round(row["avg_score"], 1),
                }

            # Top failing fingerprints
            failing = conn.execute(
                "SELECT fingerprint, COUNT(*) as failures, MIN(score) as min_score "
                "FROM template_strategies "
                "WHERE event_type = 'generation' AND score < 100 "
                "AND created_at > datetime('now', ?) "
                "GROUP BY fingerprint "
                "ORDER BY failures DESC LIMIT 5",
                (f"-{days} days",)
            ).fetchall()

            stats["top_failing"] = [
                {"fingerprint": r["fingerprint"], "failures": r["failures"],
                 "min_score": r["min_score"]}
                for r in failing
            ]

            return stats
    except Exception as e:
        log.warning("template_learning: get_strategy_stats failed: %s", e)
        return {}


def get_failure_patterns(days: int = 90) -> dict:
    """Aggregate template failures + buyer feedback to identify systemic issues.

    Returns: {
        fingerprint_X: {
            "generation_failures": 5,
            "buyer_complaints": 2,
            "strategies_that_work": ["overlay"],
            "strategies_that_fail": ["form_fields"],
        },
    }
    """
    try:
        with _get_conn() as conn:
            # Get all fingerprints with failures
            rows = conn.execute(
                "SELECT fingerprint, strategy, event_type, score "
                "FROM template_strategies "
                "WHERE created_at > datetime('now', ?) "
                "ORDER BY fingerprint",
                (f"-{days} days",)
            ).fetchall()

        patterns = {}
        for row in rows:
            fp = row["fingerprint"]
            if fp not in patterns:
                patterns[fp] = {
                    "generation_failures": 0,
                    "buyer_complaints": 0,
                    "strategies_that_work": set(),
                    "strategies_that_fail": set(),
                }
            p = patterns[fp]
            if row["event_type"] == "generation":
                if row["score"] == 100:
                    p["strategies_that_work"].add(row["strategy"])
                else:
                    p["generation_failures"] += 1
                    p["strategies_that_fail"].add(row["strategy"])
            elif row["event_type"] in ("field_error", "missing_form",
                                       "wrong_format", "resubmit_requested"):
                p["buyer_complaints"] += 1

        # Convert sets to lists for JSON serialization
        for fp in patterns:
            patterns[fp]["strategies_that_work"] = list(
                patterns[fp]["strategies_that_work"])
            patterns[fp]["strategies_that_fail"] = list(
                patterns[fp]["strategies_that_fail"])

        # Filter to only problematic fingerprints
        return {
            fp: p for fp, p in patterns.items()
            if p["generation_failures"] > 0 or p["buyer_complaints"] > 0
        }
    except Exception as e:
        log.warning("template_learning: get_failure_patterns failed: %s", e)
        return {}
