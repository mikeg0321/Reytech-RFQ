"""
src/core/paths.py — Centralized Path Configuration

Single source of truth for all directory paths across the entire application.
Every module imports from here instead of computing its own DATA_DIR.

On Railway with a volume mounted, DATA_DIR points to the persistent volume
so data survives deploys. Git-tracked data/ serves as seed data only.
"""

import os
import shutil
import logging

log = logging.getLogger("reytech.paths")

# ── Project Root ──────────────────────────────────────────────────────────────
_THIS_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_THIS_FILE)))

# ── Seed data directory (always the git-tracked data/ folder) ────────────────
_GIT_DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ── Resolve persistent DATA_DIR ─────────────────────────────────────────────
# Priority: REYTECH_DATA_DIR env → Railway volume mount → git data/
def _resolve_data_dir() -> str:
    """Find the best persistent data directory."""
    # 1. Explicit override
    env_dir = os.environ.get("REYTECH_DATA_DIR", "")
    if env_dir and os.path.isdir(env_dir):
        return env_dir
    
    # 2. Railway volume detection
    vol_mount = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
    if vol_mount and os.path.isdir(vol_mount):
        return os.path.join(vol_mount, "data") if not vol_mount.endswith("/data") else vol_mount
    
    # 3. Check common Railway volume paths
    is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_SERVICE_NAME"))
    if is_railway:
        for candidate in ("/data", "/app/data"):
            if os.path.isdir(candidate):
                # Test writability
                try:
                    test_f = os.path.join(candidate, ".vol_test")
                    with open(test_f, "w") as f:
                        f.write("ok")
                    os.remove(test_f)
                    return candidate
                except OSError:
                    continue
    
    # 4. Fallback: git-tracked data/ (local dev, or Railway without volume)
    return _GIT_DATA_DIR

DATA_DIR = _resolve_data_dir()
_USING_VOLUME = (DATA_DIR != _GIT_DATA_DIR)

# ── Seed persistent volume from git data on first deploy ─────────────────────
def _seed_volume():
    """Copy seed data from git to volume, only for files that DON'T exist yet."""
    if not _USING_VOLUME:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    seeded = []
    for fname in os.listdir(_GIT_DATA_DIR):
        src = os.path.join(_GIT_DATA_DIR, fname)
        dst = os.path.join(DATA_DIR, fname)
        if os.path.isfile(src) and not os.path.exists(dst):
            shutil.copy2(src, dst)
            seeded.append(fname)
    if seeded:
        log.info(f"Seeded {len(seeded)} files to volume: {', '.join(seeded)}")
    else:
        log.info(f"Volume data intact ({len(os.listdir(DATA_DIR))} files)")

_seed_volume()

if _USING_VOLUME:
    log.info(f"DATA_DIR: {DATA_DIR} (PERSISTENT VOLUME ✅)")
else:
    log.warning(f"DATA_DIR: {DATA_DIR} (git-tracked, WILL RESET ON DEPLOY ⚠️)")

# ── Core Directories ─────────────────────────────────────────────────────────
UPLOAD_DIR = os.path.join(PROJECT_ROOT, "uploads")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
FORMS_DIR = os.path.join(PROJECT_ROOT, "src", "forms")

# ── Key File Paths ───────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(PROJECT_ROOT, "reytech_config.json")
CUSTOMERS_PATH = os.path.join(DATA_DIR, "customers.json")
CUSTOMERS_SEED_PATH = os.path.join(PROJECT_ROOT, "customers_seed.json")
QUOTES_LOG_PATH = os.path.join(DATA_DIR, "quotes_log.json")
QUOTE_COUNTER_PATH = os.path.join(DATA_DIR, "quote_counter.json")
SCPRS_DB_PATH = os.path.join(DATA_DIR, "scprs_prices.json")
RFQS_PATH = os.path.join(DATA_DIR, "rfqs.json")
PRICE_CHECKS_PATH = os.path.join(DATA_DIR, "price_checks.json")

# ── Forms-local files (live alongside the filler module) ─────────────────────
FORMS_CONFIG_PATH = os.path.join(FORMS_DIR, "reytech_config.json")
SIGNATURE_PATH = os.path.join(FORMS_DIR, "signature_transparent.png")

# ── Ensure core dirs exist ───────────────────────────────────────────────────
for _d in [DATA_DIR, UPLOAD_DIR, OUTPUT_DIR]:
    os.makedirs(_d, exist_ok=True)


def validate_paths() -> dict:
    """Runtime validation — call at app startup to catch path issues early.
    
    Returns:
        {"ok": bool, "errors": [str], "warnings": [str], "resolved": {name: path}}
    """
    result = {"ok": True, "errors": [], "warnings": [], "resolved": {}}
    
    checks = {
        "PROJECT_ROOT": (PROJECT_ROOT, True),
        "DATA_DIR": (DATA_DIR, True),
        "UPLOAD_DIR": (UPLOAD_DIR, True),
        "OUTPUT_DIR": (OUTPUT_DIR, True),
        "CONFIG_PATH": (CONFIG_PATH, True),
        "FORMS_CONFIG_PATH": (FORMS_CONFIG_PATH, True),
        "SIGNATURE_PATH": (SIGNATURE_PATH, True),
        "CUSTOMERS_SEED_PATH": (CUSTOMERS_SEED_PATH, False),
    }
    
    for name, (path, required) in checks.items():
        result["resolved"][name] = path
        if not os.path.exists(path):
            if required:
                result["errors"].append(f"{name} not found: {path}")
                result["ok"] = False
            else:
                result["warnings"].append(f"{name} not found: {path}")
    
    # Verify DATA_DIR is writable
    test_file = os.path.join(DATA_DIR, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
    except OSError as e:
        result["errors"].append(f"DATA_DIR not writable: {e}")
        result["ok"] = False
    
    # Flag volume status
    result["resolved"]["USING_VOLUME"] = str(_USING_VOLUME)
    if not _USING_VOLUME and os.environ.get("RAILWAY_ENVIRONMENT"):
        result["warnings"].append(
            "Running on Railway WITHOUT persistent volume! "
            "Data will be lost on every deploy. "
            "Add a volume in Railway UI: Service → Storage → Add Volume → Mount: /data"
        )
    
    return result
