"""Build Health Dashboard — tracks fixes vs enhancements over time.

Routes:
    GET /admin/build-health     — Dashboard page
    GET /api/admin/build-health — JSON API with commit analysis

Answers: "Are we building more and fixing less?" If the ratio of
feat/chore commits to fix commits is increasing, the rebuild is working.
"""
import logging
import subprocess

from flask import jsonify

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


def _analyze_git_log(since_date="2026-04-15"):
    """Analyze git log for commit types since a given date."""
    try:
        result = subprocess.run(
            ["git", "log", f"--since={since_date}", "--oneline", "--no-merges", "--format=%s"],
            capture_output=True, text=True, timeout=10,
        )
        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
    except Exception as e:
        log.warning("git log failed: %s", e)
        return {"error": str(e)}

    fixes = []
    features = []
    chores = []
    docs = []
    other = []

    for msg in lines:
        msg_lower = msg.lower()
        if msg_lower.startswith("fix") or "fix(" in msg_lower or "hotfix" in msg_lower:
            fixes.append(msg)
        elif msg_lower.startswith("feat") or "feat(" in msg_lower:
            features.append(msg)
        elif msg_lower.startswith("chore") or msg_lower.startswith("refactor"):
            chores.append(msg)
        elif msg_lower.startswith("doc"):
            docs.append(msg)
        else:
            other.append(msg)

    total = len(lines)
    build_count = len(features) + len(chores)  # New work
    fix_count = len(fixes)

    # Compute ratio: higher = building more, fixing less
    if fix_count > 0:
        build_to_fix_ratio = round(build_count / fix_count, 2)
    else:
        build_to_fix_ratio = build_count if build_count > 0 else 0

    # Weekly breakdown
    try:
        weekly_result = subprocess.run(
            ["git", "log", f"--since={since_date}", "--oneline", "--no-merges",
             "--format=%aI %s"],
            capture_output=True, text=True, timeout=10,
        )
        weekly = {}
        for line in weekly_result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.strip().split(" ", 1)
            if len(parts) < 2:
                continue
            date_str = parts[0][:10]  # YYYY-MM-DD
            msg = parts[1].lower()
            # Group by week (ISO week)
            from datetime import datetime
            try:
                dt = datetime.fromisoformat(date_str)
                week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            except Exception:
                continue

            if week_key not in weekly:
                weekly[week_key] = {"fixes": 0, "features": 0, "other": 0}

            if msg.startswith("fix") or "fix(" in msg:
                weekly[week_key]["fixes"] += 1
            elif msg.startswith("feat") or "feat(" in msg:
                weekly[week_key]["features"] += 1
            else:
                weekly[week_key]["other"] += 1
    except Exception:
        weekly = {}

    return {
        "since": since_date,
        "total_commits": total,
        "fixes": fix_count,
        "features": len(features),
        "chores": len(chores),
        "docs": len(docs),
        "other": len(other),
        "build_to_fix_ratio": build_to_fix_ratio,
        "trend": "improving" if build_to_fix_ratio > 1.5 else "neutral" if build_to_fix_ratio > 0.8 else "fix-heavy",
        "weekly": dict(sorted(weekly.items())),
        "recent_fixes": fixes[:10],
        "recent_features": features[:10],
    }


@bp.route("/api/admin/build-health")
@auth_required
def api_build_health():
    """JSON API: commit analysis since rebuild started."""
    analysis = _analyze_git_log()
    return jsonify({"ok": True, **analysis})


@bp.route("/admin/build-health")
@auth_required
def build_health_page():
    """Build health dashboard."""
    from src.api.render import render_page
    analysis = _analyze_git_log()
    return render_page("build_health.html", active_page="Admin", analysis=analysis)
