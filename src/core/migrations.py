"""
migrations.py — Lightweight schema migration framework for SQLite.
Sprint 5.2 (M3): Versioned migrations with rollback tracking.

Usage:
    from src.core.migrations import run_migrations
    run_migrations()  # Called at app startup
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.migrations")


def _get_db():
    from src.core.db import get_db
    return get_db()


def _ensure_migration_table(conn):
    """Create migration tracking table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            checksum TEXT
        )
    """)


def _get_current_version(conn) -> int:
    """Get the highest applied migration version."""
    try:
        row = conn.execute(
            "SELECT MAX(version) as v FROM schema_migrations"
        ).fetchone()
        return row["v"] or 0 if row else 0
    except Exception:
        return 0


# ── Migration Definitions ────────────────────────────────────────────────────
# Each migration is (version, name, up_sql)
# Add new migrations at the end. NEVER modify existing migrations.

MIGRATIONS = [
    (1, "add_order_status_log", """
        CREATE TABLE IF NOT EXISTS order_status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            old_status TEXT,
            new_status TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            changed_by TEXT DEFAULT 'system',
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_osl_order ON order_status_log(order_id);
        CREATE INDEX IF NOT EXISTS idx_osl_time ON order_status_log(changed_at);
    """),

    (2, "add_processed_emails", """
        CREATE TABLE IF NOT EXISTS processed_emails (
            uid TEXT PRIMARY KEY,
            inbox TEXT DEFAULT 'sales',
            processed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pe_inbox ON processed_emails(inbox);
    """),

    (3, "add_email_classifications", """
        CREATE TABLE IF NOT EXISTS email_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_uid TEXT,
            subject TEXT,
            sender TEXT,
            classification TEXT,
            confidence REAL,
            scores TEXT,
            classified_at TEXT,
            needs_review INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ec_review ON email_classifications(needs_review);
    """),

    (4, "add_backup_log", """
        CREATE TABLE IF NOT EXISTS backup_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_at TEXT NOT NULL,
            file_path TEXT,
            size_bytes INTEGER,
            duration_sec REAL,
            status TEXT DEFAULT 'ok'
        );
    """),

    (5, "add_scheduler_heartbeats", """
        CREATE TABLE IF NOT EXISTS scheduler_heartbeats (
            job_name TEXT PRIMARY KEY,
            last_heartbeat TEXT,
            interval_sec INTEGER,
            status TEXT DEFAULT 'ok'
        );
    """),
]


def run_migrations():
    """Apply any pending migrations. Safe to call on every startup."""
    try:
        with _get_db() as conn:
            _ensure_migration_table(conn)
            current = _get_current_version(conn)

            applied = 0
            for version, name, sql in MIGRATIONS:
                if version <= current:
                    continue

                try:
                    conn.executescript(sql)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (version, name, datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration %d applied: %s", version, name)
                except Exception as e:
                    log.error("Migration %d (%s) FAILED: %s", version, name, e)
                    raise

            if applied:
                log.info("Applied %d migration(s). Schema at version %d",
                         applied, version)
            else:
                log.debug("Schema up to date at version %d", current)

            return {"ok": True, "version": max(v for v, _, _ in MIGRATIONS),
                    "applied": applied}

    except Exception as e:
        log.error("Migration runner failed: %s", e)
        return {"ok": False, "error": str(e)}


def get_migration_status() -> dict:
    """Return current schema version and applied migrations."""
    try:
        with _get_db() as conn:
            _ensure_migration_table(conn)
            rows = conn.execute(
                "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
            ).fetchall()
            current = _get_current_version(conn)
            latest = max(v for v, _, _ in MIGRATIONS) if MIGRATIONS else 0
            pending = [{"version": v, "name": n}
                       for v, n, _ in MIGRATIONS if v > current]
            return {
                "current_version": current,
                "latest_available": latest,
                "applied": [dict(r) for r in rows],
                "pending": pending,
                "up_to_date": current >= latest,
            }
    except Exception as e:
        return {"error": str(e)}
