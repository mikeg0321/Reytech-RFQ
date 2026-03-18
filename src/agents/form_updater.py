"""
Form Updater — Downloads latest CA state procurement forms from official DGS sources.
Runs overnight on 1st + 15th of each month. Compares hashes. Alerts on field changes.
"""
import os
import logging
import hashlib
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta

log = logging.getLogger("reytech.form_updater")

from src.core.paths import DATA_DIR
TEMPLATE_DIR = os.path.join(DATA_DIR, "templates")
FORM_INDEX_PATH = os.path.join(DATA_DIR, "form_index.json")

FORM_REGISTRY = {
    "std204": {
        "name": "STD 204 — Payee Data Record",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/pdf/std204.pdf",
        "filename": "std204_blank.pdf",
        "required_by": ["all"],
    },
    "std205": {
        "name": "STD 205 — Payee Data Record Supplement",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/pdf/std205.pdf",
        "filename": "std205_blank.pdf",
        "required_by": ["calvet", "dgs", "cdfa"],
    },
    "gspd05105": {
        "name": "GSPD-05-105 — Bidder Declaration",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/gs/pd/gspd05-105.pdf",
        "filename": "gspd05105_bidder_declaration.pdf",
        "required_by": ["calvet", "dgs", "cdfa", "dca", "chp"],
    },
    "pd843": {
        "name": "DGS PD 843 — DVBE Declarations",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/gs/pd/pd843.pdf",
        "filename": "dvbe_843_official.pdf",
        "required_by": ["cchcs", "calvet", "dgs", "chp"],
    },
    "pd1_darfur": {
        "name": "DGS PD 1 — Darfur Contracting Act",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/gs/pd/pd1.pdf",
        "filename": "darfur_pd1_official.pdf",
        "required_by": ["calvet", "dgs", "cdfa", "dca"],
    },
    "calrecycle74": {
        "name": "CalRecycle 74 — Recycled Content Cert",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/gs/pd/calrecycle74.pdf",
        "filename": "calrecycle_74_official.pdf",
        "required_by": ["cchcs", "calvet"],
    },
    "std21": {
        "name": "STD 21 — Drug-Free Workplace",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/pdf/std021.pdf",
        "filename": "std21_drug_free.pdf",
        "required_by": ["cchcs"],
    },
    "std1000": {
        "name": "STD 1000 — Service/Consultant Cert",
        "url": "https://www.documents.dgs.ca.gov/dgs/fmc/pdf/std1000.pdf",
        "filename": "std1000_official.pdf",
        "required_by": ["calvet"],
    },
}


def _load_form_index():
    try:
        with open(FORM_INDEX_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_form_index(index):
    try:
        with open(FORM_INDEX_PATH, "w") as f:
            json.dump(index, f, indent=2, default=str)
    except Exception as e:
        log.warning("Form index save: %s", e)


def _file_hash(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_form(form_id, form_info, force=False):
    """Download a single form. Returns result dict."""
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
    target_path = os.path.join(TEMPLATE_DIR, form_info["filename"])
    index = _load_form_index()
    result = {"form_id": form_id, "name": form_info["name"], "action": "skipped"}

    try:
        req = urllib.request.Request(form_info["url"],
            headers={"User-Agent": "Reytech-FormUpdater/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read()
        except urllib.error.HTTPError as e:
            result["action"] = "error"
            result["reason"] = f"HTTP {e.code}"
            return result
        except Exception as e:
            result["action"] = "error"
            result["reason"] = str(e)[:100]
            return result

        if not content[:5] == b"%PDF-":
            result["action"] = "error"
            result["reason"] = "Not a PDF"
            return result
        if len(content) < 1000:
            result["action"] = "error"
            result["reason"] = f"Too small ({len(content)}b)"
            return result

        tmp_path = target_path + ".downloading"
        with open(tmp_path, "wb") as f:
            f.write(content)
        new_hash = _file_hash(tmp_path)

        existing_hash = _file_hash(target_path) if os.path.exists(target_path) else ""
        old_hash = index.get(form_id, {}).get("hash", "")

        if not force and new_hash in (existing_hash, old_hash):
            os.remove(tmp_path)
            result["action"] = "unchanged"
            return result

        # Backup old version
        if os.path.exists(target_path):
            bak = f"{form_info['filename']}.{datetime.now().strftime('%Y%m%d')}.bak"
            try:
                os.rename(target_path, os.path.join(TEMPLATE_DIR, bak))
            except Exception:
                pass

        os.rename(tmp_path, target_path)

        index[form_id] = {
            "name": form_info["name"],
            "filename": form_info["filename"],
            "hash": new_hash,
            "size": len(content),
            "downloaded_at": datetime.now().isoformat(),
            "previous_hash": old_hash or existing_hash,
        }

        # Check for field changes
        try:
            from pypdf import PdfReader
            fields = list((PdfReader(target_path).get_fields() or {}).keys())
            prev_fields = index.get(form_id, {}).get("fields", [])
            index[form_id]["fields"] = fields
            index[form_id]["field_count"] = len(fields)
            if prev_fields and set(fields) != set(prev_fields):
                added = set(fields) - set(prev_fields)
                removed = set(prev_fields) - set(fields)
                if added or removed:
                    index[form_id]["field_changes"] = {
                        "added": list(added)[:10],
                        "removed": list(removed)[:10],
                        "detected_at": datetime.now().isoformat(),
                    }
                    log.warning("FORM FIELD CHANGE: %s added=%s removed=%s",
                               form_id, list(added)[:3], list(removed)[:3])
        except Exception:
            pass

        _save_form_index(index)
        result["action"] = "updated" if existing_hash else "new"
        result["size"] = len(content)
        log.info("Form %s: %s (%d bytes)", form_id, result["action"], len(content))
        return result

    except Exception as e:
        result["action"] = "error"
        result["reason"] = str(e)[:100]
        tmp = target_path + ".downloading"
        if os.path.exists(tmp):
            os.remove(tmp)
        return result


def update_all_forms(force=False):
    """Download/update all registered forms."""
    results = []
    updated = 0
    errors = 0
    for form_id, form_info in FORM_REGISTRY.items():
        try:
            r = download_form(form_id, form_info, force=force)
            results.append(r)
            if r["action"] in ("updated", "new"):
                updated += 1
            elif r["action"] == "error":
                errors += 1
            time.sleep(2)
        except Exception as e:
            results.append({"form_id": form_id, "action": "error", "reason": str(e)[:100]})
            errors += 1
    return {
        "ok": errors == 0,
        "total": len(FORM_REGISTRY),
        "updated": updated,
        "errors": errors,
        "results": results,
        "timestamp": datetime.now().isoformat(),
    }


def get_form_status():
    """Get status of all registered forms."""
    index = _load_form_index()
    status = []
    for form_id, info in FORM_REGISTRY.items():
        path = os.path.join(TEMPLATE_DIR, info["filename"])
        entry = index.get(form_id, {})
        status.append({
            "form_id": form_id,
            "name": info["name"],
            "filename": info["filename"],
            "on_disk": os.path.exists(path),
            "size_kb": round(os.path.getsize(path) / 1024, 1) if os.path.exists(path) else 0,
            "last_downloaded": entry.get("downloaded_at", "never"),
            "field_count": entry.get("field_count", 0),
            "field_changes": entry.get("field_changes"),
            "required_by": info["required_by"],
        })
    return status
