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


def safe_save_json(filepath, data, reason=""):
    """Save JSON with pre-save snapshot. Blocks destructive saves."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    basename = os.path.basename(filepath)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Snapshot current file
    if os.path.exists(filepath):
        snapshot_path = os.path.join(SNAPSHOT_DIR, f"{basename}.{ts}")
        try:
            shutil.copy2(filepath, snapshot_path)
        except Exception as e:
            log.warning("Snapshot failed for %s: %s", basename, e)

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

    # 3. Atomic write
    tmp = filepath + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, filepath)
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
