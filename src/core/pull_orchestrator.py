"""
pull_orchestrator.py — Unified pull orchestrator for all procurement connectors.

Replaces ad-hoc harvest scripts with a single orchestration layer.
"""
import time
import json
import logging
import sqlite3
import importlib
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger("reytech.orchestrator")

REYTECH_VENDOR_NAMES = [
    "reytech", "rey tech", "reytech inc",
    "reytech incorporated", "reytech inc.",
]

DEFAULT_FROM_DATES = {
    "ca_scprs": lambda: datetime.now() - timedelta(days=1095),
    "federal_usaspending": lambda: datetime.now() - timedelta(days=730),
    "__default__": lambda: datetime.now() - timedelta(days=730),
}


def _load_connector(connector_class: str):
    """Dynamically load a connector class from its dotted path."""
    parts = connector_class.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"Invalid connector class: {connector_class}")
    mod = importlib.import_module(parts[0])
    return getattr(mod, parts[1])()


def _get_conn():
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class PullOrchestrator:

    def run_connector(self, connector_id: str,
                      from_date: datetime = None,
                      force: bool = False) -> dict:
        """Full pull for one connector."""
        from src.core.connector_registry import get_connector, update_connector_after_pull

        meta = get_connector(connector_id)
        if not meta:
            return {"ok": False, "error": f"Connector not found: {connector_id}"}
        if meta["status"] != "active" and not force:
            return {"ok": False, "error": f"Connector {connector_id} is {meta['status']}"}
        if not meta.get("connector_class"):
            return {"ok": False, "error": f"No connector_class for {connector_id}"}

        t0 = time.time()
        from_dt = from_date or DEFAULT_FROM_DATES.get(
            connector_id, DEFAULT_FROM_DATES["__default__"])()

        try:
            connector = _load_connector(meta["connector_class"])
        except Exception as e:
            return {"ok": False, "error": f"Load failed: {e}"}

        log.info("Running connector %s from %s...", connector_id, from_dt.strftime("%Y-%m-%d"))

        # Step 1: Authenticate
        if not connector.authenticate():
            return {"ok": False, "error": "Authentication failed"}

        # Step 2: Vendor search (highest priority)
        vendor_results = connector.search_by_vendor(REYTECH_VENDOR_NAMES, from_dt)

        # Step 3: Store results
        conn = _get_conn()
        stored = _store_results(conn, vendor_results, connector_id)

        # Step 4: Health check
        try:
            from src.core.harvest_health import validate_pull
            rows_now = conn.execute(
                "SELECT COUNT(*) FROM scprs_po_master WHERE source_system=?",
                (meta.get("source_system", connector_id),)).fetchone()[0]
            health = validate_pull(connector_id, connector_id,
                                   meta.get("state", ""), conn)
            grade = health.get("grade", "?")
        except Exception:
            grade = "?"
            rows_now = stored

        conn.close()
        duration = round(time.time() - t0, 1)

        # Step 5: Update registry
        update_connector_after_pull(connector_id, grade, rows_now)

        # Step 6: Log to harvest_log
        try:
            hconn = _get_conn()
            now = datetime.now(timezone.utc).isoformat()
            hconn.execute("""
                INSERT INTO harvest_log (source_system, state, agency, pos_found,
                    started_at, completed_at, duration_seconds, tenant_id)
                VALUES (?, ?, 'all', ?, ?, ?, ?, 'reytech')
            """, (connector_id, meta.get("state", ""), stored, now, now, duration))
            hconn.commit()
            hconn.close()
        except Exception:
            pass

        return {"ok": True, "connector_id": connector_id,
                "stored": stored, "health_grade": grade,
                "duration_seconds": duration,
                "vendor_results": len(vendor_results)}

    def run_due_connectors(self) -> list:
        """Run all overdue active connectors. Max 2 at a time."""
        from src.core.connector_registry import get_due_connectors
        due = get_due_connectors()
        results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(self.run_connector, c["id"]): c["id"] for c in due}
            for future in as_completed(futures):
                cid = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append({"ok": False, "connector_id": cid, "error": str(e)})
        return results

    def run_vendor_search(self, vendor_names: list = None,
                          connector_ids: list = None) -> dict:
        """Search for vendor across active connectors."""
        from src.core.connector_registry import get_active_connectors
        names = vendor_names or REYTECH_VENDOR_NAMES
        connectors = get_active_connectors()
        if connector_ids:
            connectors = [c for c in connectors if c["id"] in connector_ids]

        total = 0
        results = {}
        for meta in connectors:
            if not meta.get("connector_class"):
                continue
            try:
                connector = _load_connector(meta["connector_class"])
                if not connector.authenticate():
                    results[meta["id"]] = {"error": "Auth failed"}
                    continue
                from_dt = DEFAULT_FROM_DATES.get(
                    meta["id"], DEFAULT_FROM_DATES["__default__"])()
                found = connector.search_by_vendor(names, from_dt)
                conn = _get_conn()
                stored = _store_results(conn, found, meta["id"])
                conn.close()
                results[meta["id"]] = {"found": len(found), "stored": stored}
                total += stored
            except Exception as e:
                results[meta["id"]] = {"error": str(e)}

        return {"total_stored": total, "results": results}

    def get_status(self) -> dict:
        """Current state of all connectors for /api/v1/health."""
        from src.core.connector_registry import get_all_connectors
        status = {}
        now = datetime.now(timezone.utc)
        for c in get_all_connectors():
            is_overdue = False
            next_due = None
            if c["status"] == "active" and c.get("last_pulled_at"):
                try:
                    last = datetime.fromisoformat(c["last_pulled_at"])
                    next_due = (last + timedelta(hours=c.get("pull_frequency_hours", 168))).isoformat()
                    is_overdue = datetime.fromisoformat(next_due) < now
                except Exception:
                    pass
            status[c["id"]] = {
                "name": c["name"],
                "status": c["status"],
                "state": c["state"],
                "last_pulled": c.get("last_pulled_at"),
                "health_grade": c.get("last_health_grade"),
                "record_count": c.get("record_count", 0),
                "next_due": next_due,
                "is_overdue": is_overdue,
            }
        return status


def _store_results(conn, results: list, connector_id: str) -> int:
    """Store normalized results to scprs_po_master. Returns count stored."""
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in results:
        try:
            po_num = r.get("po_number") or r.get("id", "")
            if not po_num:
                continue
            conn.execute("""
                INSERT OR IGNORE INTO scprs_po_master
                (pulled_at, po_number, dept_name, institution, supplier,
                 supplier_id, status, start_date, grand_total,
                 buyer_email, search_term, agency_key,
                 state, source_system)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, po_num,
                  r.get("agency", r.get("dept_name", "")),
                  r.get("agency", r.get("institution", "")),
                  r.get("vendor_name", r.get("supplier", "")),
                  r.get("supplier_id", ""),
                  r.get("status", "Active"),
                  r.get("award_date", r.get("start_date", "")),
                  float(r.get("total_value", r.get("grand_total", 0)) or 0),
                  r.get("buyer_email", ""),
                  r.get("search_term", "")[:100],
                  r.get("agency_key", ""),
                  r.get("state", "CA"),
                  r.get("source_system", connector_id)))
            count += 1
        except Exception as e:
            log.debug("Store: %s", e)
    conn.commit()
    return count
