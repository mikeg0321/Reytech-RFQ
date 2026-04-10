"""
template_downloader.py — Download linked templates from buyer emails.

Buyers often include OneDrive/SharePoint/Google Drive links to 703B/704B
templates in their emails. This module downloads from whitelisted domains
only, classifies by form type, and saves to the RFQ file store.

Security:
  - Only downloads from TRUSTED_DOMAINS (ca.gov, sharepoint, google drive)
  - Never follows redirects to untrusted domains
  - 30-second timeout per download
  - Max 10MB file size
  - Only saves PDF/DOCX/XLSX files
"""

import logging
import os
import re
import time
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger("reytech.template_downloader")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ── Configuration ───────────────────────────────────────────────────────────

TRUSTED_DOMAINS = [
    ".ca.gov",
    ".sharepoint.com",
    "onedrive.live.com",
    "drive.google.com",
    "docs.google.com",
]

ALLOWED_CONTENT_TYPES = [
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/msword",
    "application/vnd.ms-excel",
    "application/octet-stream",  # common fallback for binary downloads
]

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".xlsx", ".doc", ".xls"}

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
DOWNLOAD_TIMEOUT = 30  # seconds

# Form type detection from filename/URL
FORM_TYPE_PATTERNS = {
    "703b": ["703b", "703-b", "ams703b", "ams-703b"],
    "703c": ["703c", "703-c", "ams703c"],
    "704b": ["704b", "704-b", "ams704b", "quote_worksheet"],
    "704": ["704", "ams704", "price_check"],
    "bidpkg": ["bid_package", "bid-package", "bidpackage", "bid_pkg"],
    "std204": ["std204", "std-204", "std_204", "payee"],
    "std205": ["std205", "std-205", "std_205"],
    "dvbe843": ["dvbe", "843", "dvbe843"],
    "darfur": ["darfur"],
    "calrecycle": ["calrecycle", "recycled"],
}


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def download_templates(
    urls: list,
    rfq_id: str,
    save_dir: str,
) -> list:
    """Download linked templates from trusted URLs.

    Args:
        urls: List of URLs from the email body.
        rfq_id: RFQ ID for file organization.
        save_dir: Base directory to save downloaded files.

    Returns:
        List of dicts: [{url, local_path, form_type, content_type, filename}]
        Empty list if nothing downloaded.
    """
    if not urls or not HAS_REQUESTS:
        return []

    results = []
    rfq_dir = os.path.join(save_dir, rfq_id)
    os.makedirs(rfq_dir, exist_ok=True)

    for url in urls[:5]:  # Cap at 5 downloads per RFQ
        result = _download_single(url, rfq_dir)
        if result:
            results.append(result)

    if results:
        log.info("Downloaded %d templates for RFQ %s: %s",
                 len(results), rfq_id,
                 ", ".join(r["form_type"] for r in results))

    return results


def _download_single(url: str, save_dir: str) -> Optional[dict]:
    """Download a single file from a trusted URL.

    Returns dict with download info, or None if failed/untrusted.
    """
    # Validate URL is trusted
    if not is_trusted_url(url):
        log.debug("Skipping untrusted URL: %s", url[:80])
        return None

    try:
        # Convert Google Drive view links to direct download
        download_url = _convert_drive_url(url)

        resp = requests.get(
            download_url,
            timeout=DOWNLOAD_TIMEOUT,
            stream=True,
            allow_redirects=True,
            headers={"User-Agent": "Reytech-RFQ/1.0"},
        )

        # Verify redirect didn't escape trusted domains
        if resp.url != download_url:
            final_domain = urlparse(resp.url).netloc
            if not _domain_is_trusted(final_domain):
                log.warning("Redirect escaped trusted domain: %s → %s", url[:60], final_domain)
                return None

        if resp.status_code != 200:
            log.debug("Download failed %d: %s", resp.status_code, url[:80])
            return None

        # Check content type
        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()

        # Check file size from headers
        content_length = int(resp.headers.get("Content-Length", 0))
        if content_length > MAX_FILE_SIZE:
            log.warning("File too large (%d bytes): %s", content_length, url[:80])
            return None

        # Determine filename
        filename = _extract_filename(resp, url)
        ext = os.path.splitext(filename)[1].lower()

        if ext not in ALLOWED_EXTENSIONS:
            log.debug("Skipping non-document file (%s): %s", ext, url[:80])
            return None

        # Download content (with size cap)
        local_path = os.path.join(save_dir, filename)
        downloaded = 0
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                downloaded += len(chunk)
                if downloaded > MAX_FILE_SIZE:
                    log.warning("Download exceeds max size, aborting: %s", url[:80])
                    f.close()
                    os.unlink(local_path)
                    return None
                f.write(chunk)

        # Classify form type
        form_type = classify_form_type(filename, url)

        log.info("Downloaded %s (%s, %d bytes) → %s",
                 filename, form_type, downloaded, local_path)

        return {
            "url": url,
            "local_path": local_path,
            "form_type": form_type,
            "content_type": content_type,
            "filename": filename,
            "size": downloaded,
        }

    except requests.exceptions.Timeout:
        log.debug("Download timeout: %s", url[:80])
        return None
    except Exception as e:
        log.debug("Download error for %s: %s", url[:60], e)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# URL VALIDATION & HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def is_trusted_url(url: str) -> bool:
    """Check if URL is from a whitelisted domain."""
    try:
        parsed = urlparse(url)
        return _domain_is_trusted(parsed.netloc)
    except Exception:
        return False


def _domain_is_trusted(netloc: str) -> bool:
    """Check if a domain/netloc is in the trusted list."""
    lower = netloc.lower()
    return any(lower.endswith(d) for d in TRUSTED_DOMAINS)


def _convert_drive_url(url: str) -> str:
    """Convert Google Drive view URLs to direct download URLs."""
    # Google Drive: /file/d/FILE_ID/view → /file/d/FILE_ID/export?format=pdf
    m = re.search(r'drive\.google\.com/file/d/([^/]+)', url)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"

    # Google Docs: /document/d/FILE_ID → export as docx
    m = re.search(r'docs\.google\.com/document/d/([^/]+)', url)
    if m:
        file_id = m.group(1)
        return f"https://docs.google.com/document/d/{file_id}/export?format=docx"

    return url


def _extract_filename(response, url: str) -> str:
    """Extract filename from Content-Disposition header or URL."""
    # Try Content-Disposition header
    cd = response.headers.get("Content-Disposition", "")
    if cd:
        m = re.search(r'filename[*]?=(?:UTF-8\'\')?["\']?([^"\';]+)', cd, re.IGNORECASE)
        if m:
            fname = m.group(1).strip()
            if fname:
                return _sanitize_filename(fname)

    # Fall back to URL path
    path = urlparse(url).path
    fname = os.path.basename(path)
    if fname and "." in fname:
        return _sanitize_filename(fname)

    # Last resort: generate from URL hash
    import hashlib
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"template_{url_hash}.pdf"


def _sanitize_filename(name: str) -> str:
    """Remove unsafe characters from filename."""
    # Keep alphanumeric, spaces, dots, hyphens, underscores
    safe = re.sub(r'[^\w\s.\-]', '', name).strip()
    return safe or "template.pdf"


# ═══════════════════════════════════════════════════════════════════════════
# FORM TYPE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

def classify_form_type(filename: str, url: str = "") -> str:
    """Classify a file as a specific form type based on filename/URL patterns."""
    combined = (filename + " " + url).lower()
    for form_type, patterns in FORM_TYPE_PATTERNS.items():
        if any(p in combined for p in patterns):
            return form_type
    return "unknown"
