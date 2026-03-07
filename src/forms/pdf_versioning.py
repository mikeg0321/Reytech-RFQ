"""
pdf_versioning.py — PDF Template Versioning (M6).
Tracks template versions so quotes/invoices can be regenerated
with the correct layout if templates change.

Each PDF generator stamps its version into the output metadata.
Version history is tracked in the database for audit trail.
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.pdf_versions")

# ── Template Version Registry ────────────────────────────────────────────────
# Bump version when layout, colors, positioning, or content changes.
# NEVER modify a version retroactively — always create a new one.

TEMPLATE_VERSIONS = {
    "quote": {
        "current": "2.1",
        "history": {
            "1.0": {"date": "2025-07-01", "desc": "Initial QuoteWerks-match layout"},
            "2.0": {"date": "2025-12-15", "desc": "Agency-specific layouts, dynamic row heights"},
            "2.1": {"date": "2026-02-01", "desc": "Multi-page header repeat, alt-row shading"},
        },
    },
    "invoice": {
        "current": "1.1",
        "history": {
            "1.0": {"date": "2025-10-01", "desc": "Initial invoice layout matching quote branding"},
            "1.1": {"date": "2026-01-15", "desc": "Payment terms, PO reference fields"},
        },
    },
    "price_check": {
        "current": "1.0",
        "history": {
            "1.0": {"date": "2025-08-01", "desc": "Standard price check response format"},
        },
    },
}


def get_template_version(template_type: str) -> str:
    """Get current version for a template type."""
    entry = TEMPLATE_VERSIONS.get(template_type)
    if not entry:
        return "1.0"
    return entry["current"]


def stamp_pdf_metadata(template_type: str, document_id: str, extra: dict = None):
    """Record which template version was used to generate a document."""
    version = get_template_version(template_type)
    record = {
        "template_type": template_type,
        "template_version": version,
        "document_id": document_id,
        "generated_at": datetime.now().isoformat(),
    }
    if extra:
        record.update(extra)
    
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pdf_generation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    template_type TEXT NOT NULL,
                    template_version TEXT NOT NULL,
                    document_id TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    generator TEXT,
                    file_path TEXT,
                    metadata TEXT
                )
            """)
            conn.execute("""
                INSERT INTO pdf_generation_log 
                (template_type, template_version, document_id, generated_at, generator, file_path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                template_type, version, document_id,
                record["generated_at"],
                extra.get("generator", "") if extra else "",
                extra.get("file_path", "") if extra else "",
            ))
        log.debug("PDF stamped: %s v%s for %s", template_type, version, document_id)
    except Exception as e:
        log.warning("Failed to stamp PDF metadata: %s", str(e)[:200])
    
    return record


def get_generation_history(document_id: str = None, template_type: str = None,
                           limit: int = 50) -> list:
    """Query PDF generation history."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conditions = []
            params = []
            if document_id:
                conditions.append("document_id = ?")
                params.append(document_id)
            if template_type:
                conditions.append("template_type = ?")
                params.append(template_type)
            
            where = " WHERE " + " AND ".join(conditions) if conditions else ""
            params.append(limit)
            
            rows = conn.execute("""
                SELECT * FROM pdf_generation_log
                " + where + "
                ORDER BY generated_at DESC LIMIT ?
            """, params).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_version_info() -> dict:
    """Get all template versions and generation stats."""
    stats = {}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for ttype in TEMPLATE_VERSIONS:
                try:
                    row = conn.execute("""
                        SELECT COUNT(*) as total,
                               MAX(generated_at) as last_generated
                        FROM pdf_generation_log WHERE template_type = ?
                    """, (ttype,)).fetchone()
                    stats[ttype] = {
                        "current_version": TEMPLATE_VERSIONS[ttype]["current"],
                        "total_generated": row["total"] if row else 0,
                        "last_generated": row["last_generated"] if row else None,
                    }
                except Exception:
                    stats[ttype] = {
                        "current_version": TEMPLATE_VERSIONS[ttype]["current"],
                        "total_generated": 0,
                    }
    except Exception:
        for ttype in TEMPLATE_VERSIONS:
            stats[ttype] = {"current_version": TEMPLATE_VERSIONS[ttype]["current"]}
    
    return stats
