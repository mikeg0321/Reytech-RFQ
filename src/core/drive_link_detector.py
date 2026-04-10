"""
Google Drive Link Detector — Finds and downloads Drive-linked files from emails.

When buyers attach Google Docs/Sheets to emails instead of real files, IMAP
sees nothing. This module parses email HTML for Drive/Docs links and downloads
the files via the Drive API.

Google Docs → exported as DOCX (preserves table structure for 704 parsing)
Google Sheets → exported as XLSX
Regular Drive files → downloaded as-is
"""

import os
import re
import io
import logging
from typing import List, Optional

log = logging.getLogger("reytech.drive_link_detector")

# ═══════════════════════════════════════════════════════════════════════
# Drive Link Patterns
# ═══════════════════════════════════════════════════════════════════════

# Extract file ID from various Google Drive/Docs URL formats
_DRIVE_PATTERNS = [
    # Google Docs: https://docs.google.com/document/d/{ID}/edit
    re.compile(r'https?://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)'),
    # Google Sheets: https://docs.google.com/spreadsheets/d/{ID}/edit
    re.compile(r'https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)'),
    # Google Slides: https://docs.google.com/presentation/d/([a-zA-Z0-9_-]+)'),
    re.compile(r'https?://docs\.google\.com/presentation/d/([a-zA-Z0-9_-]+)'),
    # Drive file: https://drive.google.com/file/d/{ID}/view
    re.compile(r'https?://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)'),
    # Drive open: https://drive.google.com/open?id={ID}
    re.compile(r'https?://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)'),
    # Drive uc (download): https://drive.google.com/uc?id={ID}
    re.compile(r'https?://drive\.google\.com/uc\?.*?id=([a-zA-Z0-9_-]+)'),
]

# Google Apps MIME types → export formats
_EXPORT_MAP = {
    "application/vnd.google-apps.document": {
        "mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "ext": ".docx",
    },
    "application/vnd.google-apps.spreadsheet": {
        "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ext": ".xlsx",
    },
    "application/vnd.google-apps.presentation": {
        "mime": "application/pdf",
        "ext": ".pdf",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# Link Extraction
# ═══════════════════════════════════════════════════════════════════════

def extract_drive_links(html_or_text: str) -> List[dict]:
    """Extract Google Drive/Docs file IDs from email HTML or plain text.

    Returns list of {"file_id": "...", "url": "...", "type": "document|spreadsheet|file"}
    Deduplicates by file_id.
    """
    if not html_or_text:
        return []

    seen_ids = set()
    links = []

    for pattern in _DRIVE_PATTERNS:
        for match in pattern.finditer(html_or_text):
            file_id = match.group(1)
            if file_id in seen_ids:
                continue
            seen_ids.add(file_id)

            url = match.group(0)
            link_type = "file"
            if "document" in url:
                link_type = "document"
            elif "spreadsheet" in url:
                link_type = "spreadsheet"
            elif "presentation" in url:
                link_type = "presentation"

            links.append({
                "file_id": file_id,
                "url": url,
                "type": link_type,
            })

    return links


# ═══════════════════════════════════════════════════════════════════════
# File Download
# ═══════════════════════════════════════════════════════════════════════

def download_drive_file(file_id: str, save_dir: str, drive_service) -> Optional[str]:
    """Download a single Drive file.

    For Google Docs/Sheets: exports as DOCX/XLSX (preserves structure for parsing).
    For regular files: downloads as-is.

    Returns: local file path, or None on failure.
    """
    try:
        # Get file metadata
        meta = drive_service.files().get(
            fileId=file_id, fields="name,mimeType"
        ).execute()
        name = meta.get("name", f"drive_file_{file_id}")
        mime_type = meta.get("mimeType", "")

        log.info("Drive file %s: name='%s' mimeType='%s'", file_id, name, mime_type)

        # Google Apps file → export
        if mime_type in _EXPORT_MAP:
            export_info = _EXPORT_MAP[mime_type]
            export_mime = export_info["mime"]
            ext = export_info["ext"]

            # Ensure filename has correct extension
            base = os.path.splitext(name)[0]
            safe_name = re.sub(r'[^\w\-_. ()]+', '_', base) + ext

            data = drive_service.files().export(
                fileId=file_id, mimeType=export_mime
            ).execute()

            filepath = os.path.join(save_dir, safe_name)
            with open(filepath, "wb") as f:
                f.write(data)
            log.info("Drive export: %s → %s (%d bytes)", name, safe_name, len(data))
            return filepath

        # Regular file → direct download
        from googleapiclient.http import MediaIoBaseDownload
        safe_name = re.sub(r'[^\w\-_. ()]+', '_', name)
        filepath = os.path.join(save_dir, safe_name)

        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        with open(filepath, "wb") as f:
            f.write(fh.getvalue())
        log.info("Drive download: %s → %s (%d bytes)", name, safe_name, len(fh.getvalue()))
        return filepath

    except Exception as e:
        log.warning("Drive file download failed for %s: %s", file_id, e)
        return None


# ═══════════════════════════════════════════════════════════════════════
# Top-Level: Detect + Download from Email Message
# ═══════════════════════════════════════════════════════════════════════

def _extract_html_body(msg) -> str:
    """Extract HTML body from an email.message.Message object."""
    html_parts = []
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "text/html":
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                try:
                    html_parts.append(payload.decode(charset, errors="replace"))
                except Exception:
                    html_parts.append(payload.decode("utf-8", errors="replace"))
    return "\n".join(html_parts)


def _extract_text_body(msg) -> str:
    """Extract plain text body from an email.message.Message object."""
    text_parts = []
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                try:
                    text_parts.append(payload.decode(charset, errors="replace"))
                except Exception:
                    text_parts.append(payload.decode("utf-8", errors="replace"))
    return "\n".join(text_parts)


def detect_and_download_drive_attachments(msg, save_dir: str,
                                          drive_service) -> List[dict]:
    """Top-level function: find Drive links in email, download files.

    Args:
        msg: email.message.Message object (from email.message_from_bytes)
        save_dir: directory to save downloaded files
        drive_service: Google Drive API service (from gmail_api.get_drive_service)

    Returns: list of {"path": filepath, "filename": name, "type": "drive_doc"}
    """
    if not drive_service:
        return []

    # Search both HTML and plain text for Drive links
    html_body = _extract_html_body(msg)
    text_body = _extract_text_body(msg)
    combined = f"{html_body}\n{text_body}"

    links = extract_drive_links(combined)
    if not links:
        return []

    log.info("Found %d Google Drive link(s) in email", len(links))
    os.makedirs(save_dir, exist_ok=True)

    saved = []
    for link in links:
        filepath = download_drive_file(link["file_id"], save_dir, drive_service)
        if filepath:
            saved.append({
                "path": filepath,
                "filename": os.path.basename(filepath),
                "type": "drive_doc",
                "drive_file_id": link["file_id"],
                "drive_type": link["type"],
            })

    if saved:
        log.info("Downloaded %d Drive file(s): %s", len(saved),
                 [s["filename"] for s in saved])
    return saved
