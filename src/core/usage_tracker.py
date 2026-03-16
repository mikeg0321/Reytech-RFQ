"""
Usage Tracker
Tracks page views, API calls, button clicks, and feature usage.
"""
import logging

log = logging.getLogger("reytech.usage")


def init_usage_tracking(conn):
    """Create usage tracking table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS usage_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            page TEXT DEFAULT '',
            action TEXT DEFAULT '',
            detail TEXT DEFAULT '',
            route TEXT DEFAULT '',
            method TEXT DEFAULT 'GET',
            status_code INTEGER DEFAULT 200,
            duration_ms INTEGER DEFAULT 0,
            user_agent TEXT DEFAULT '',
            timestamp TEXT DEFAULT (datetime('now')),
            session_id TEXT DEFAULT ''
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON usage_events(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_page ON usage_events(page)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_type ON usage_events(event_type)")
    conn.commit()


def track_page_view(page, route="", duration_ms=0, session_id=""):
    _store_event("page_view", page=page, route=route, duration_ms=duration_ms, session_id=session_id)


def track_api_call(route, method="GET", status_code=200, duration_ms=0):
    _store_event("api_call", route=route, method=method, status_code=status_code, duration_ms=duration_ms)


def track_action(page, action, detail=""):
    _store_event("action", page=page, action=action, detail=detail)


def track_feature(feature_name, detail=""):
    _store_event("feature", action=feature_name, detail=detail)


def _store_event(event_type, page="", action="", detail="",
                 route="", method="GET", status_code=200,
                 duration_ms=0, session_id=""):
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=5)
        db.execute("""
            INSERT INTO usage_events
            (event_type, page, action, detail, route, method,
             status_code, duration_ms, session_id, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
        """, (event_type, page, action, (detail or "")[:500], route,
              method, status_code, duration_ms, session_id))
        db.commit()
        db.close()
    except Exception:
        pass  # Never let tracking break the app


def get_usage_stats(days=30):
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    stats = {}
    d = f"-{days} days"

    stats["top_pages"] = [
        {"page": r[0], "views": r[1]}
        for r in db.execute("""
            SELECT page, COUNT(*) c FROM usage_events
            WHERE event_type='page_view' AND timestamp > datetime('now', ?)
            GROUP BY page ORDER BY c DESC LIMIT 20
        """, (d,)).fetchall()
    ]
    stats["top_api"] = [
        {"route": r[0], "calls": r[1], "avg_ms": round(r[2] or 0)}
        for r in db.execute("""
            SELECT route, COUNT(*) c, AVG(duration_ms)
            FROM usage_events WHERE event_type='api_call'
            AND timestamp > datetime('now', ?) GROUP BY route ORDER BY c DESC LIMIT 20
        """, (d,)).fetchall()
    ]
    stats["top_actions"] = [
        {"page": r[0], "action": r[1], "count": r[2]}
        for r in db.execute("""
            SELECT page, action, COUNT(*) c FROM usage_events
            WHERE event_type='action' AND timestamp > datetime('now', ?)
            GROUP BY page, action ORDER BY c DESC LIMIT 20
        """, (d,)).fetchall()
    ]
    stats["feature_usage"] = [
        {"feature": r[0], "count": r[1]}
        for r in db.execute("""
            SELECT action, COUNT(*) c FROM usage_events
            WHERE event_type='feature' AND timestamp > datetime('now', ?)
            GROUP BY action ORDER BY c DESC LIMIT 20
        """, (d,)).fetchall()
    ]
    stats["total_events"] = db.execute("SELECT COUNT(*) FROM usage_events").fetchone()[0]
    stats["daily_usage"] = [
        {"date": r[0], "events": r[1]}
        for r in db.execute("""
            SELECT DATE(timestamp), COUNT(*) FROM usage_events
            WHERE timestamp > datetime('now', ?)
            GROUP BY DATE(timestamp) ORDER BY DATE(timestamp) DESC
        """, (d,)).fetchall()
    ]
    stats["slowest_pages"] = [
        {"route": r[0], "avg_ms": round(r[1]), "max_ms": r[2], "calls": r[3]}
        for r in db.execute("""
            SELECT route, AVG(duration_ms), MAX(duration_ms), COUNT(*)
            FROM usage_events WHERE duration_ms > 0 AND timestamp > datetime('now', ?)
            GROUP BY route HAVING COUNT(*) > 2 ORDER BY AVG(duration_ms) DESC LIMIT 15
        """, (d,)).fetchall()
    ]
    db.close()
    return stats


def get_dead_pages(days=30):
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    try:
        from app import app
        all_routes = set()
        for rule in app.url_map.iter_rules():
            if rule.endpoint != "static" and "GET" in rule.methods:
                all_routes.add(rule.rule)
        rows = db.execute("""
            SELECT DISTINCT route FROM usage_events
            WHERE timestamp > datetime('now', ?)
        """, (f"-{days} days",)).fetchall()
        used = {r[0] for r in rows}
        dead = sorted(all_routes - used)
        db.close()
        return {"total_routes": len(all_routes), "used_routes": len(used),
                "dead_routes": len(dead), "dead_list": dead}
    except Exception as e:
        db.close()
        return {"error": str(e)}
