"""
src/core/paths.py — Centralized Path Configuration

Single source of truth for all directory paths across the entire application.
Every module imports from here instead of computing its own DATA_DIR.

This prevents the DATA_DIR mismatch bug where 8 modules independently
computed paths and pointed to src/<pkg>/data/ instead of project root /data/.
"""

import os

# ── Project Root ──────────────────────────────────────────────────────────────
# Resolve from this file: src/core/paths.py → src/core/ → src/ → project_root/
_THIS_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_THIS_FILE)))

# ── Core Directories ─────────────────────────────────────────────────────────
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
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
        "CUSTOMERS_SEED_PATH": (CUSTOMERS_SEED_PATH, False),  # warning only
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
    
    return result
