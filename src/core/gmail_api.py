"""
Gmail API Integration — Core Wrapper

Replaces IMAP polling with Gmail API for better reliability and access to
Google Drive-linked attachments that IMAP cannot see.

Auth: OAuth2 with stored refresh token (one-time browser consent, then permanent).
Falls back to IMAP if GMAIL_OAUTH_REFRESH_TOKEN is not set.
"""

import os
import base64
import logging
import threading
from typing import Optional, List, Dict

log = logging.getLogger("reytech.gmail_api")

# ═══════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════

GMAIL_OAUTH_CLIENT_ID = os.environ.get("GMAIL_OAUTH_CLIENT_ID", "")
GMAIL_OAUTH_CLIENT_SECRET = os.environ.get("GMAIL_OAUTH_CLIENT_SECRET", "")
GMAIL_OAUTH_REFRESH_TOKEN = os.environ.get("GMAIL_OAUTH_REFRESH_TOKEN", "")
GMAIL_OAUTH_REFRESH_TOKEN_2 = os.environ.get("GMAIL_OAUTH_REFRESH_TOKEN_2", "")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Cached service instances per email address
_service_cache: Dict[str, object] = {}
_cache_lock = threading.Lock()


def is_configured() -> bool:
    """Check if Gmail API OAuth2 credentials are available."""
    return bool(GMAIL_OAUTH_CLIENT_ID and GMAIL_OAUTH_CLIENT_SECRET
                and GMAIL_OAUTH_REFRESH_TOKEN)


def get_refresh_token(inbox_name: str = "sales") -> str:
    """Get the refresh token for the specified inbox."""
    if inbox_name == "mike":
        return GMAIL_OAUTH_REFRESH_TOKEN_2 or GMAIL_OAUTH_REFRESH_TOKEN
    return GMAIL_OAUTH_REFRESH_TOKEN


# ═══════════════════════════════════════════════════════════════════════
# Credentials & Service
# ═══════════════════════════════════════════════════════════════════════

def _build_credentials(inbox_name: str = "sales"):
    """Build OAuth2 credentials from stored refresh token.

    No browser needed at runtime — the refresh token was obtained via
    scripts/gmail_oauth_setup.py and stored as an env var.
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    refresh_token = get_refresh_token(inbox_name)
    if not refresh_token:
        raise RuntimeError(f"No refresh token for inbox '{inbox_name}'")

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=GMAIL_OAUTH_CLIENT_ID,
        client_secret=GMAIL_OAUTH_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )

    # Force initial token refresh
    creds.refresh(Request())
    return creds


def get_service(inbox_name: str = "sales"):
    """Get a cached Gmail API service for the specified inbox.

    Returns: googleapiclient.discovery.Resource for Gmail API v1
    """
    with _cache_lock:
        if inbox_name in _service_cache:
            svc = _service_cache[inbox_name]
            # Check if credentials are still valid (auto-refreshes if not)
            if hasattr(svc, '_http') and hasattr(svc._http, 'credentials'):
                try:
                    if svc._http.credentials.expired:
                        from google.auth.transport.requests import Request
                        svc._http.credentials.refresh(Request())
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
            return svc

    from googleapiclient.discovery import build

    creds = _build_credentials(inbox_name)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    with _cache_lock:
        _service_cache[inbox_name] = service

    log.info("Gmail API service built for inbox '%s'", inbox_name)
    return service


def get_drive_service(inbox_name: str = "sales"):
    """Get a Drive API service using the same OAuth2 credentials.

    Used to download Google Drive-linked files from emails.
    """
    cache_key = f"drive_{inbox_name}"
    with _cache_lock:
        if cache_key in _service_cache:
            return _service_cache[cache_key]

    from googleapiclient.discovery import build

    creds = _build_credentials(inbox_name)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    with _cache_lock:
        _service_cache[cache_key] = service

    log.info("Drive API service built for inbox '%s'", inbox_name)
    return service


def clear_cache():
    """Clear cached service instances (for reconnection)."""
    with _cache_lock:
        _service_cache.clear()


# ═══════════════════════════════════════════════════════════════════════
# Gmail API Operations
# ═══════════════════════════════════════════════════════════════════════

def list_message_ids(service, query: str = "", max_results: int = 500) -> List[str]:
    """List message IDs matching a query.

    Args:
        service: Gmail API service
        query: Gmail search query (e.g., "after:2026/04/01 in:inbox")
        max_results: Maximum messages to return

    Returns: List of message ID strings
    """
    ids = []
    try:
        request = service.users().messages().list(
            userId="me", q=query, maxResults=min(max_results, 500)
        )
        while request and len(ids) < max_results:
            response = request.execute()
            messages = response.get("messages", [])
            ids.extend(m["id"] for m in messages)
            request = service.users().messages().list_next(request, response)
    except Exception as e:
        log.error("Gmail API list_message_ids error: %s", e)
        raise
    return ids[:max_results]


def get_raw_message(service, msg_id: str) -> bytes:
    """Fetch a message in raw RFC 2822 format.

    Returns bytes that can be parsed with email.message_from_bytes().
    This is the same format IMAP returns with BODY.PEEK[].
    """
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="raw"
        ).execute()
        raw = result.get("raw", "")
        # Gmail API returns base64url-encoded RFC 2822
        return base64.urlsafe_b64decode(raw)
    except Exception as e:
        log.error("Gmail API get_raw_message error for %s: %s", msg_id, e)
        raise


def get_message_metadata(service, msg_id: str) -> dict:
    """Fetch lightweight message metadata (headers only).

    Returns dict with 'subject', 'from', 'date', 'message_id' keys.
    """
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="metadata",
            metadataHeaders=["Subject", "From", "Date", "Message-ID"]
        ).execute()
        headers = {h["name"].lower(): h["value"]
                   for h in result.get("payload", {}).get("headers", [])}
        return {
            "subject": headers.get("subject", ""),
            "from": headers.get("from", ""),
            "date": headers.get("date", ""),
            "message_id": headers.get("message-id", ""),
            "gmail_id": msg_id,
        }
    except Exception as e:
        log.error("Gmail API get_message_metadata error for %s: %s", msg_id, e)
        return {"gmail_id": msg_id}
