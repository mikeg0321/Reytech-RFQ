"""
Google Drive Integration — Core API Wrapper

Handles: folder creation, file upload, file search, folder structure management.
Uses service account credentials stored as base64 in GOOGLE_DRIVE_CREDENTIALS env var.

Strategy: Write Always, Read on Empty
- Always pushes files to Drive on trigger events
- Only reads from Drive when local data is missing (disaster recovery)
- All operations are async (background thread) to never block the user
"""

import os
import io
import json
import base64
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, List

log = logging.getLogger("reytech.gdrive")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# ═══════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════

GOOGLE_DRIVE_CREDENTIALS = os.environ.get("GOOGLE_DRIVE_CREDENTIALS", "")
GOOGLE_DRIVE_ROOT_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID", "")

# Folder structure constants
FOLDER_STRUCTURE = {
    "subfolders_per_po": ["RFQ", "Supplier", "Delivery", "Invoice", "Misc"],
}

# In-memory folder ID cache (avoids repeated API lookups)
_folder_cache: Dict[str, str] = {}
_cache_lock = threading.Lock()

# Audit log
AUDIT_LOG_PATH = os.path.join(DATA_DIR, "drive_audit_log.json")
DRIVE_INDEX_PATH = os.path.join(DATA_DIR, "drive_index.json")

# Background task queue
_task_queue: List[dict] = []
_queue_lock = threading.Lock()
_worker_started = False


def is_configured() -> bool:
    """Check if Google Drive integration is configured."""
    return bool(GOOGLE_DRIVE_CREDENTIALS and GOOGLE_DRIVE_ROOT_FOLDER_ID)


def _get_service():
    """Build Google Drive API service from credentials."""
    if not GOOGLE_DRIVE_CREDENTIALS:
        raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS not set")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    # Decode base64 credentials
    try:
        creds_json = base64.b64decode(GOOGLE_DRIVE_CREDENTIALS)
        creds_dict = json.loads(creds_json)
    except Exception as e:
        raise RuntimeError(f"Failed to decode GOOGLE_DRIVE_CREDENTIALS: {e}")

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


# ═══════════════════════════════════════════════════════════════════════
# Folder Operations
# ═══════════════════════════════════════════════════════════════════════

def _get_or_create_folder(name: str, parent_id: str) -> str:
    """Get existing folder by name under parent, or create it. Returns folder ID."""
    cache_key = f"{parent_id}/{name}"
    with _cache_lock:
        if cache_key in _folder_cache:
            return _folder_cache[cache_key]

    service = _get_service()

    # Search for existing folder
    query = (f"name='{name}' and '{parent_id}' in parents "
             f"and mimeType='application/vnd.google-apps.folder' and trashed=false")
    results = service.files().list(
        q=query, fields="files(id,name)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
    else:
        # Create folder
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = service.files().create(
            body=metadata, fields="id", supportsAllDrives=True,
        ).execute()
        folder_id = folder["id"]
        log.info("Created Drive folder: %s/%s → %s", parent_id[:8], name, folder_id)

    with _cache_lock:
        _folder_cache[cache_key] = folder_id
    return folder_id


def get_folder_path(year: str = "", quarter: str = "", po_number: str = "",
                    subfolder: str = "", category: str = "") -> str:
    """
    Navigate/create the folder hierarchy and return the target folder ID.
    
    Examples:
        get_folder_path("2026", "Q1", "PO-10840483", "RFQ")
        get_folder_path("2026", "Q1", category="Lost")
        get_folder_path("2026", category="Pending")
        get_folder_path("2026", category="Price_Checks")
        get_folder_path(category="Backups")
        get_folder_path(category="Supplier_Quotes")
    """
    root_id = GOOGLE_DRIVE_ROOT_FOLDER_ID
    if not root_id:
        raise RuntimeError("GOOGLE_DRIVE_ROOT_FOLDER_ID not set")

    # Special top-level folders
    if category == "Backups":
        return _get_or_create_folder("Backups", root_id)
    if category == "Supplier_Quotes":
        return _get_or_create_folder("Supplier_Quotes", root_id)

    if not year:
        year = str(datetime.now().year)

    current_id = _get_or_create_folder(year, root_id)

    # Year-level categories
    if category == "Pending":
        return _get_or_create_folder("Pending", current_id)
    if category == "Price_Checks":
        return _get_or_create_folder("Price_Checks", current_id)

    if quarter:
        current_id = _get_or_create_folder(quarter, current_id)

    # Quarter-level categories
    if category == "Lost":
        return _get_or_create_folder("Lost", current_id)

    if po_number:
        current_id = _get_or_create_folder(po_number, current_id)
        # Create standard subfolders
        for sf in FOLDER_STRUCTURE["subfolders_per_po"]:
            _get_or_create_folder(sf, current_id)

    if subfolder:
        current_id = _get_or_create_folder(subfolder, current_id)

    return current_id


# ═══════════════════════════════════════════════════════════════════════
# File Operations
# ═══════════════════════════════════════════════════════════════════════

def upload_file(local_path: str, folder_id: str, drive_filename: str = "",
                mime_type: str = "") -> Optional[str]:
    """Upload a file to a specific Drive folder. Returns file ID or None."""
    if not os.path.exists(local_path):
        log.warning("Upload skipped — file not found: %s", local_path)
        return None

    service = _get_service()
    filename = drive_filename or os.path.basename(local_path)
    if not mime_type:
        mime_type = _guess_mime(filename)

    from googleapiclient.http import MediaFileUpload

    # Check if file already exists (update instead of duplicate)
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(
        q=query, fields="files(id)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()

    if existing.get("files"):
        # Update existing file (Google Drive keeps version history automatically)
        file_id = existing["files"][0]["id"]
        media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
        service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True,
        ).execute()
        log.info("Updated Drive file: %s (id=%s)", filename, file_id)
    else:
        # Create new file
        metadata = {"name": filename, "parents": [folder_id]}
        media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
        result = service.files().create(
            body=metadata, media_body=media, fields="id", supportsAllDrives=True,
        ).execute()
        file_id = result["id"]
        log.info("Uploaded to Drive: %s → folder %s (id=%s)", filename, folder_id[:8], file_id)

    # Record in audit log and index
    _audit("upload", filename, folder_id, file_id, os.path.getsize(local_path))
    _index_file(file_id, filename, folder_id, mime_type, os.path.getsize(local_path))

    return file_id


def upload_bytes(data: bytes, folder_id: str, filename: str,
                 mime_type: str = "") -> Optional[str]:
    """Upload bytes directly to Drive. Returns file ID."""
    service = _get_service()
    if not mime_type:
        mime_type = _guess_mime(filename)

    from googleapiclient.http import MediaInMemoryUpload

    # Check for existing
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(
        q=query, fields="files(id)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()

    if existing.get("files"):
        file_id = existing["files"][0]["id"]
        media = MediaInMemoryUpload(data, mimetype=mime_type, resumable=True)
        service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True,
        ).execute()
        log.info("Updated Drive file (bytes): %s", filename)
    else:
        metadata = {"name": filename, "parents": [folder_id]}
        media = MediaInMemoryUpload(data, mimetype=mime_type, resumable=True)
        result = service.files().create(
            body=metadata, media_body=media, fields="id", supportsAllDrives=True,
        ).execute()
        file_id = result["id"]
        log.info("Uploaded to Drive (bytes): %s → %s", filename, folder_id[:8])

    _audit("upload", filename, folder_id, file_id, len(data))
    _index_file(file_id, filename, folder_id, mime_type, len(data))
    return file_id


def download_file(file_id: str, local_path: str) -> bool:
    """Download a file from Drive by ID. Returns True on success."""
    try:
        service = _get_service()
        request = service.files().get_media(fileId=file_id)
        with open(local_path, "wb") as f:
            from googleapiclient.http import MediaIoBaseDownload
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        _audit("download", os.path.basename(local_path), "", file_id, os.path.getsize(local_path))
        return True
    except Exception as e:
        log.error("Drive download failed for %s: %s", file_id, e)
        return False


def list_files(folder_id: str) -> List[dict]:
    """List all files in a folder."""
    service = _get_service()
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query, fields="files(id,name,mimeType,size,modifiedTime)",
        pageSize=100, supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    return results.get("files", [])


# ═══════════════════════════════════════════════════════════════════════
# Audit Trail
# ═══════════════════════════════════════════════════════════════════════

def _audit(action: str, filename: str, folder_id: str, file_id: str, size: int = 0):
    """Log every Drive operation for FAR compliance."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "action": action,
        "filename": filename,
        "folder_id": folder_id,
        "file_id": file_id,
        "size_bytes": size,
    }
    try:
        log_data = []
        if os.path.exists(AUDIT_LOG_PATH):
            with open(AUDIT_LOG_PATH) as f:
                log_data = json.load(f)
        log_data.append(entry)
        # Keep last 10000 entries
        if len(log_data) > 10000:
            log_data = log_data[-10000:]
        os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
        with open(AUDIT_LOG_PATH, "w") as f:
            json.dump(log_data, f, indent=1)
    except Exception as e:
        log.debug("Audit log write failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Drive Index (searchable metadata)
# ═══════════════════════════════════════════════════════════════════════

def _index_file(file_id: str, filename: str, folder_id: str,
                mime_type: str, size: int):
    """Add/update file in the searchable index."""
    try:
        index = {}
        if os.path.exists(DRIVE_INDEX_PATH):
            with open(DRIVE_INDEX_PATH) as f:
                index = json.load(f)
        index[file_id] = {
            "filename": filename,
            "folder_id": folder_id,
            "mime_type": mime_type,
            "size": size,
            "uploaded_at": datetime.now().isoformat(),
        }
        os.makedirs(os.path.dirname(DRIVE_INDEX_PATH), exist_ok=True)
        with open(DRIVE_INDEX_PATH, "w") as f:
            json.dump(index, f, indent=1)
    except Exception as e:
        log.debug("Drive index write failed: %s", e)


def search_index(query: str) -> List[dict]:
    """Search the local Drive index by filename or metadata."""
    results = []
    try:
        if os.path.exists(DRIVE_INDEX_PATH):
            with open(DRIVE_INDEX_PATH) as f:
                index = json.load(f)
            q = query.lower()
            for fid, meta in index.items():
                if q in meta.get("filename", "").lower():
                    results.append({"file_id": fid, **meta})
    except Exception:
        pass
    return results


# ═══════════════════════════════════════════════════════════════════════
# Background Worker (async uploads)
# ═══════════════════════════════════════════════════════════════════════

def enqueue(task: dict):
    """Add a task to the background upload queue.
    
    Task format: {"action": "upload_file", "local_path": "...", "folder_id": "...", ...}
    """
    with _queue_lock:
        _task_queue.append(task)
    _ensure_worker()


def _ensure_worker():
    """Start background worker thread if not running."""
    global _worker_started
    if _worker_started:
        return
    _worker_started = True

    def _worker():
        while True:
            task = None
            with _queue_lock:
                if _task_queue:
                    task = _task_queue.pop(0)
            if task:
                try:
                    _process_task(task)
                except Exception as e:
                    log.error("Drive task failed: %s — %s", task.get("action"), e)
            else:
                time.sleep(5)

    t = threading.Thread(target=_worker, daemon=True, name="gdrive-worker")
    t.start()
    log.info("Google Drive background worker started")


def _process_task(task: dict):
    """Execute a single queued Drive task."""
    action = task.get("action", "")
    if action == "upload_file":
        upload_file(task["local_path"], task["folder_id"],
                    task.get("filename", ""), task.get("mime_type", ""))
    elif action == "upload_bytes":
        upload_bytes(task["data"], task["folder_id"],
                     task["filename"], task.get("mime_type", ""))
    elif action == "create_po_folder":
        _create_po_folder_with_contents(task)
    else:
        log.warning("Unknown Drive task action: %s", action)


def _create_po_folder_with_contents(task: dict):
    """Create PO folder structure and copy files from Pending."""
    po_number = task.get("po_number", "")
    year = task.get("year", str(datetime.now().year))
    quarter = task.get("quarter", _current_quarter())
    sol_number = task.get("solicitation_number", "")

    if not po_number:
        return

    # Create PO folder with subfolders
    po_folder_id = get_folder_path(year, quarter, po_number)

    # Copy files from Pending if they exist
    if sol_number:
        try:
            pending_id = get_folder_path(year, category="Pending")
            sol_folder_id = _get_or_create_folder(sol_number, pending_id)
            pending_files = list_files(sol_folder_id)
            rfq_folder_id = get_folder_path(year, quarter, po_number, subfolder="RFQ")
            
            service = _get_service()
            for pf in pending_files:
                # Copy file to PO/RFQ/ subfolder
                service.files().copy(
                    fileId=pf["id"],
                    body={"name": pf["name"], "parents": [rfq_folder_id]},
                    supportsAllDrives=True,
                ).execute()
                log.info("Copied %s from Pending to PO/%s/RFQ/", pf["name"], po_number)
        except Exception as e:
            log.warning("Failed to copy Pending files to PO folder: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════

def _guess_mime(filename: str) -> str:
    """Guess MIME type from filename."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return {
        "pdf": "application/pdf",
        "json": "application/json",
        "db": "application/x-sqlite3",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
    }.get(ext, "application/octet-stream")


def _current_quarter() -> str:
    """Return current quarter string (Q1-Q4)."""
    month = datetime.now().month
    return f"Q{(month - 1) // 3 + 1}"


def get_last_backup_date() -> Optional[str]:
    """Return the date of the most recent successful backup, or None."""
    try:
        if os.path.exists(AUDIT_LOG_PATH):
            with open(AUDIT_LOG_PATH) as f:
                entries = json.load(f)
            for entry in reversed(entries):
                if entry.get("action") == "backup_complete":
                    return entry.get("timestamp", "")[:10]
    except Exception:
        pass
    return None
