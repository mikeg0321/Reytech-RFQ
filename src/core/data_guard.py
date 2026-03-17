"""
Data Guard — Snapshot-before-save protection.
Every JSON save creates a timestamped snapshot.
Blocks saves that would wipe all items.
"""
import os
import json
import shutil
import logging
from datetime import datetime, timedelta

log = logging.getLogger("reytech.data_guard")

SNAPSHOT_DIR = os.path.join(os.environ.get("DATA_DIR", "/data"), "snapshots")


MAX_SNAPSHOTS_PER_FILE = 10  # Keep only the most recent N snapshots per file
SNAPSHOT_THROTTLE_SEC = 60   # Don't snapshot if last one was <60s ago


def _prune_snapshots(basename):
    """Keep only the most recent MAX_SNAPSHOTS_PER_FILE snapshots per file."""
    if not os.path.exists(SNAPSHOT_DIR):
        return
    snaps = sorted(
        [f for f in os.listdir(SNAPSHOT_DIR) if f.startswith(basename + ".")],
        key=lambda f: os.path.getmtime(os.path.join(SNAPSHOT_DIR, f)),
        reverse=True,
    )
    for old in snaps[MAX_SNAPSHOTS_PER_FILE:]:
        try:
            os.remove(os.path.join(SNAPSHOT_DIR, old))
        except OSError:
            pass


def safe_save_json(filepath, data, reason=""):
    """Save JSON with pre-save snapshot. Blocks destructive saves."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    basename = os.path.basename(filepath)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Snapshot current file (throttled: skip if recent snapshot exists)
    if os.path.exists(filepath):
        _skip_snapshot = False
        try:
            existing = [f for f in os.listdir(SNAPSHOT_DIR) if f.startswith(basename + ".")]
            if existing:
                newest = max(existing, key=lambda f: os.path.getmtime(os.path.join(SNAPSHOT_DIR, f)))
                age = datetime.now().timestamp() - os.path.getmtime(os.path.join(SNAPSHOT_DIR, newest))
                if age < SNAPSHOT_THROTTLE_SEC:
                    _skip_snapshot = True
        except Exception:
            pass
        if not _skip_snapshot:
            snapshot_path = os.path.join(SNAPSHOT_DIR, f"{basename}.{ts}")
            try:
                shutil.copy2(filepath, snapshot_path)
            except Exception as e:
                log.warning("Snapshot failed for %s: %s", basename, e)
            _prune_snapshots(basename)

    # 2. Block empty saves on files that had data
    if isinstance(data, dict) and "rfqs" in basename.lower():
        old_items = 0
        new_items = 0
        if os.path.exists(filepath):
            try:
                with open(filepath) as f:
                    old = json.load(f)
                old_items = sum(
                    len(r.get("line_items", r.get("items", [])))
                    for r in old.values() if isinstance(r, dict)
                )
            except Exception:
                pass
        new_items = sum(
            len(r.get("line_items", r.get("items", [])))
            for r in data.values() if isinstance(r, dict)
        )
        if old_items > 0 and new_items == 0:
            log.error(
                "BLOCKED: save would wipe ALL %d items from %s "
                "(reason: %s). Snapshot at %s",
                old_items, basename, reason,
                os.path.join(SNAPSHOT_DIR, f"{basename}.{ts}"),
            )
            return False
        if old_items > 5 and new_items < old_items * 0.5:
            log.warning(
                "CAUTION: %s items dropping %d -> %d (reason: %s)",
                basename, old_items, new_items, reason,
            )

    if isinstance(data, dict) and "price_checks" in basename.lower():
        old_items = 0
        new_items = 0
        if os.path.exists(filepath):
            try:
                with open(filepath) as f:
                    old = json.load(f)
                for pc in old.values():
                    if not isinstance(pc, dict):
                        continue
                    pd = pc.get("pc_data", pc)
                    if isinstance(pd, str):
                        try:
                            pd = json.loads(pd)
                        except Exception:
                            pd = {}
                    old_items += len(
                        pd.get("items", pc.get("items", []))
                        if isinstance(pd, dict) else []
                    )
            except Exception:
                pass
        for pc in (data.values() if isinstance(data, dict) else []):
            if not isinstance(pc, dict):
                continue
            pd = pc.get("pc_data", pc)
            if isinstance(pd, str):
                try:
                    pd = json.loads(pd)
                except Exception:
                    pd = {}
            new_items += len(
                pd.get("items", pc.get("items", []))
                if isinstance(pd, dict) else []
            )
        if old_items > 0 and new_items == 0:
            log.error(
                "BLOCKED: save would wipe ALL %d PC items from %s",
                old_items, basename,
            )
            return False

    # 3. Size guard — catch runaway growth before it fills the disk
    serialized = json.dumps(data, indent=2, default=str)
    size_mb = len(serialized) / 1_000_000
    if size_mb > 10:
        log.error(
            "BLOCKED: %s would be %.1fMB — likely recursive nesting "
            "(reason: %s). Snapshot preserved at %s",
            basename, size_mb, reason,
            os.path.join(SNAPSHOT_DIR, f"{basename}.{ts}"),
        )
        return False
    if size_mb > 2:
        log.warning("LARGE SAVE: %s = %.1fMB (reason: %s)", basename, size_mb, reason)

    # 4. Atomic write
    tmp = filepath + ".tmp"
    try:
        with open(tmp, "w") as f:
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, filepath)
        log.debug("SAVE OK: %s (%.1fKB, %d records, reason: %s)",
                  basename, len(serialized)/1000,
                  len(data) if isinstance(data, dict) else 0, reason)
        return True
    except Exception as e:
        log.error("Save FAILED for %s: %s", basename, e)
        if os.path.exists(tmp):
            os.remove(tmp)
        return False


def list_snapshots(filename="", limit=20):
    """List available snapshots, newest first."""
    if not os.path.exists(SNAPSHOT_DIR):
        return []
    snaps = []
    for f in sorted(os.listdir(SNAPSHOT_DIR), reverse=True):
        if filename and not f.startswith(filename):
            continue
        fpath = os.path.join(SNAPSHOT_DIR, f)
        snaps.append({
            "filename": f,
            "size": os.path.getsize(fpath),
            "created": datetime.fromtimestamp(
                os.path.getmtime(fpath)
            ).isoformat(),
        })
        if len(snaps) >= limit:
            break
    return snaps


def restore_snapshot(snapshot_filename, target_filepath):
    """Restore a file from a snapshot."""
    snapshot_path = os.path.join(SNAPSHOT_DIR, snapshot_filename)
    if not os.path.exists(snapshot_path):
        return {"ok": False, "error": "Snapshot not found"}
    try:
        with open(snapshot_path) as f:
            json.load(f)  # validate
    except Exception as e:
        return {"ok": False, "error": f"Corrupt: {e}"}
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre = os.path.join(SNAPSHOT_DIR,
        f"{os.path.basename(target_filepath)}.pre_restore.{ts}")
    if os.path.exists(target_filepath):
        shutil.copy2(target_filepath, pre)
    shutil.copy2(snapshot_path, target_filepath)
    return {"ok": True, "restored_from": snapshot_filename}


def cleanup_old_snapshots(days=7):
    """Remove snapshots older than N days."""
    if not os.path.exists(SNAPSHOT_DIR):
        return 0
    cutoff = datetime.now() - timedelta(days=days)
    removed = 0
    for f in os.listdir(SNAPSHOT_DIR):
        fpath = os.path.join(SNAPSHOT_DIR, f)
        if datetime.fromtimestamp(os.path.getmtime(fpath)) < cutoff:
            os.remove(fpath)
            removed += 1
    return removed


def boot_health_check():
    """Run on every boot. Logs the state of all critical data files and DB tables.
    Returns a dict with status of each check. Logs errors loudly.
    """
    data_dir = os.environ.get("DATA_DIR", "/data")
    checks = {"ok": True, "issues": [], "stats": {}}

    # 1. Check JSON files
    for fname in ["rfqs.json", "price_checks.json"]:
        fpath = os.path.join(data_dir, fname)
        try:
            if not os.path.exists(fpath):
                checks["issues"].append(f"{fname}: MISSING")
                checks["stats"][fname] = {"exists": False, "records": 0, "size_kb": 0}
                continue
            size = os.path.getsize(fpath)
            with open(fpath) as f:
                data = json.load(f)
            records = len(data) if isinstance(data, dict) else 0
            checks["stats"][fname] = {"exists": True, "records": records, "size_kb": round(size/1000, 1)}
            if records == 0:
                checks["issues"].append(f"{fname}: EXISTS but EMPTY (0 records)")
            if size > 5_000_000:
                checks["issues"].append(f"{fname}: BLOATED ({round(size/1_000_000,1)}MB) — possible recursive nesting")
            log.info("BOOT CHECK: %s — %d records, %.1fKB", fname, records, size/1000)
        except json.JSONDecodeError as e:
            checks["issues"].append(f"{fname}: CORRUPTED JSON ({e})")
            checks["stats"][fname] = {"exists": True, "records": 0, "error": str(e)}
            log.error("BOOT CHECK: %s CORRUPTED: %s", fname, e)
        except Exception as e:
            checks["issues"].append(f"{fname}: ERROR ({e})")
            checks["stats"][fname] = {"error": str(e)}

    # 2. Check SQLite tables
    try:
        import sqlite3
        db_path = os.path.join(data_dir, "reytech.db")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path, timeout=5)
            for table in ["price_checks", "rfqs"]:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    checks["stats"][f"sqlite_{table}"] = {"records": count}
                    log.info("BOOT CHECK: SQLite %s — %d records", table, count)

                    # Cross-check: if SQLite has data but JSON is empty, flag it
                    json_fname = f"{table.replace('price_checks','price_checks')}.json"
                    if table == "price_checks":
                        json_fname = "price_checks.json"
                    elif table == "rfqs":
                        json_fname = "rfqs.json"
                    json_records = checks["stats"].get(json_fname, {}).get("records", 0)
                    if count > 0 and json_records == 0:
                        checks["issues"].append(
                            f"MISMATCH: SQLite {table} has {count} records but {json_fname} has 0 — NEEDS RECOVERY"
                        )
                        log.error("BOOT CHECK: MISMATCH — SQLite %s=%d but %s=0", table, count, json_fname)
                except Exception as e:
                    checks["stats"][f"sqlite_{table}"] = {"error": str(e)}
            conn.close()
        else:
            checks["issues"].append("reytech.db: MISSING")
    except Exception as e:
        checks["issues"].append(f"SQLite check failed: {e}")

    # 3. Check snapshot disk usage
    try:
        if os.path.exists(SNAPSHOT_DIR):
            total_snap_size = sum(
                os.path.getsize(os.path.join(SNAPSHOT_DIR, f))
                for f in os.listdir(SNAPSHOT_DIR)
            )
            snap_count = len(os.listdir(SNAPSHOT_DIR))
            checks["stats"]["snapshots"] = {"count": snap_count, "size_mb": round(total_snap_size/1_000_000, 1)}
            if total_snap_size > 100_000_000:  # >100MB
                checks["issues"].append(f"Snapshots using {round(total_snap_size/1_000_000,1)}MB — run cleanup")
                log.warning("BOOT CHECK: Snapshots using %.1fMB", total_snap_size/1_000_000)
    except Exception:
        pass

    if checks["issues"]:
        checks["ok"] = False
        for issue in checks["issues"]:
            log.error("BOOT ISSUE: %s", issue)
    else:
        log.info("BOOT CHECK: ALL OK — %s",
                 ", ".join(f"{k}={v.get('records','?')}" for k, v in checks["stats"].items() if 'records' in v))

    return checks
