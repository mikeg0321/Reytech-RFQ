"""
Gmail API Integration — Core Wrapper

The sole inbound email backend. Replaced IMAP polling 2026-04-21 (IMAP
support was ripped out entirely — there is no longer a fallback).

Auth: OAuth2 with stored refresh token (one-time browser consent, then permanent).
If GMAIL_OAUTH_REFRESH_TOKEN is unset, inbound email is disabled — the poller
and all Gmail-dependent features fail loudly rather than silently.
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
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive.readonly",
]
# Existing refresh tokens granted only readonly will keep working for reads.
# Sending requires re-running scripts/gmail_oauth_setup.py so the user grants
# the gmail.send scope; send_message() will return a 403 from Google until then.

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

    `scopes` is intentionally NOT passed. If the stored refresh token was
    granted a narrower scope set than the SCOPES constant (e.g. the prod
    token predates gmail.send + drive.readonly being added), forcing the
    broader list on refresh makes Google return `invalid_scope: Bad Request`
    and the entire Gmail API path dies — circuit breaker opens, IMAP
    fallback fires, smoke test flags a poller error. Leaving scopes
    unset lets the refresh carry whatever was actually granted; send and
    Drive calls will fail gracefully if the token lacks them (see the
    403 handling in send_message) and the user can re-run
    scripts/gmail_oauth_setup.py to upgrade scopes when needed.
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


def get_raw_message(service, msg_id: str, return_thread_id: bool = False):
    """Fetch a message in raw RFC 2822 format.

    Returns bytes that can be parsed with email.message_from_bytes().
    This is the same format IMAP returns with BODY.PEEK[].

    When ``return_thread_id=True``, returns a 2-tuple ``(raw_bytes,
    gmail_thread_id)``. The threadId is the Gmail-internal ID used by the
    API to group messages into threads — distinct from the RFC 2822
    Message-ID. Both are needed for reply-on-thread send: Message-ID for
    the In-Reply-To/References headers, threadId for the API's
    `users.messages.send` / `users.drafts.create` body.
    """
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="raw"
        ).execute()
        raw = result.get("raw", "")
        # Gmail API returns base64url-encoded RFC 2822
        raw_bytes = base64.urlsafe_b64decode(raw)
        if return_thread_id:
            return raw_bytes, result.get("threadId", "")
        return raw_bytes
    except Exception as e:
        log.error("Gmail API get_raw_message error for %s: %s", msg_id, e)
        raise


def get_message_metadata(service, msg_id: str) -> dict:
    """Fetch lightweight message metadata (headers only).

    Returns dict with 'subject', 'from', 'date', 'message_id', 'thread_id',
    'gmail_id' keys. ``thread_id`` is the Gmail-internal threadId; needed
    for reply-on-thread when sending or drafting.
    """
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="metadata",
            metadataHeaders=["Subject", "From", "Date", "Message-ID", "To", "Cc"]
        ).execute()
        headers = {h["name"].lower(): h["value"]
                   for h in result.get("payload", {}).get("headers", [])}
        return {
            "subject": headers.get("subject", ""),
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "cc": headers.get("cc", ""),
            "date": headers.get("date", ""),
            "message_id": headers.get("message-id", ""),
            "thread_id": result.get("threadId", ""),
            "gmail_id": msg_id,
        }
    except Exception as e:
        log.error("Gmail API get_message_metadata error for %s: %s", msg_id, e)
        return {"gmail_id": msg_id}


# ═══════════════════════════════════════════════════════════════════════
# Sending (Gmail API — replaces smtplib.SMTP_SSL for outbound)
# ═══════════════════════════════════════════════════════════════════════
#
# Gmail API send is strictly better than SMTP for our use case:
#   - OAuth refresh tokens (same auth story as inbound) — no app password.
#   - Returns a message_id + thread_id on success; no silent-success modes.
#   - Auto-appends the user's configured Gmail signature (enforces the
#     "Gmail Handles Signatures" rule in CLAUDE.md — callers MUST pass a
#     body WITHOUT any app-level signature block).
#   - Automatically writes to the authenticated user's Sent folder; no
#     IMAP-append dance needed.
#   - Threading via `thread_id` parameter (preferred) or In-Reply-To/References
#     MIME headers (fallback).
#
# None of the 9 existing smtplib call sites are migrated by this module.
# Migration is separate work; see project_gmail_api_send_gap_2026_04_21.md.


def _split_list(value) -> List[str]:
    """Accept a list or a comma-separated string; return a clean list."""
    if not value:
        return []
    if isinstance(value, str):
        return [x.strip() for x in value.split(",") if x.strip()]
    return [str(x).strip() for x in value if str(x).strip()]


def _build_mime_message(
    to,
    subject: str,
    body_plain: str = "",
    body_html: str = "",
    cc=None,
    bcc=None,
    attachments: Optional[List[str]] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    from_name: Optional[str] = None,
    from_addr: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
):
    """Build an email.message.Message suitable for Gmail API send.

    Callers MUST NOT include a hardcoded Reytech signature in body_plain or
    body_html — Gmail auto-appends the configured signature. See CLAUDE.md
    "Gmail Handles Signatures."

    Returns the MIMEMultipart (or MIMEText) message object, ready to be
    serialized with .as_bytes() and base64url-encoded.
    """
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    to_list = _split_list(to)
    cc_list = _split_list(cc)
    bcc_list = _split_list(bcc)
    attachment_paths = list(attachments or [])

    if not to_list:
        raise ValueError("send_message requires a non-empty 'to' recipient")
    if not subject:
        raise ValueError("send_message requires a non-empty subject")
    if not (body_plain or body_html):
        raise ValueError("send_message requires either body_plain or body_html")

    has_attachments = any(os.path.exists(p) for p in attachment_paths)
    has_html = bool(body_html)

    if has_attachments:
        msg = MIMEMultipart("mixed")
        if has_html:
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body_plain or "", "plain", "utf-8"))
            alt.attach(MIMEText(body_html, "html", "utf-8"))
            msg.attach(alt)
        else:
            msg.attach(MIMEText(body_plain or "", "plain", "utf-8"))
    elif has_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_plain or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_plain, "plain", "utf-8")

    msg["To"] = ", ".join(to_list)
    msg["Subject"] = subject
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    # Bcc intentionally NOT added as a header — Gmail API handles BCC by
    # delivering to the address without exposing it in the message.
    if from_addr:
        msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to
    for name, value in (extra_headers or {}).items():
        msg[name] = value

    for path in attachment_paths:
        if not os.path.exists(path):
            # Loud-fail: silently skipping a missing attachment causes the
            # worst kind of silent failure — operator hits "Send Quote",
            # email goes out with NO PDF, DB marks status=sent, buyer gets
            # a bare email body. Raise so the caller's outer try/except
            # surfaces a red error toast and status does NOT get set to sent.
            raise FileNotFoundError(
                f"Attachment not found at send time: {path!r}. "
                f"This is a bug — the file should have existed when the "
                f"send was queued. Re-generate the PDF and retry."
            )
        with open(path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(path)}"',
        )
        msg.attach(part)

    return msg


def send_message(
    service,
    to,
    subject: str,
    body_plain: str = "",
    body_html: str = "",
    cc=None,
    bcc=None,
    attachments: Optional[List[str]] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    from_name: Optional[str] = None,
    from_addr: Optional[str] = None,
    thread_id: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> dict:
    """Send an email via the Gmail API.

    `service` is a Gmail API service object from get_service() (or
    get_send_service() once send-scope is granted). Pass the built service
    in explicitly so tests can inject a mock.

    Returns the Gmail API response dict — typically {'id': str, 'threadId': str,
    'labelIds': [...]}.

    Raises ValueError on bad input, RuntimeError on auth issues, and allows
    googleapiclient.errors.HttpError to propagate on 4xx/5xx from Google
    (callers should log and handle; never swallow silently).
    """
    msg = _build_mime_message(
        to=to,
        subject=subject,
        body_plain=body_plain,
        body_html=body_html,
        cc=cc,
        bcc=bcc,
        attachments=attachments,
        in_reply_to=in_reply_to,
        references=references,
        from_name=from_name,
        from_addr=from_addr,
        extra_headers=extra_headers,
    )

    raw_bytes = msg.as_bytes()
    raw_b64url = base64.urlsafe_b64encode(raw_bytes).decode("ascii")

    body: Dict[str, object] = {"raw": raw_b64url}
    if thread_id:
        body["threadId"] = thread_id

    try:
        response = service.users().messages().send(
            userId="me", body=body
        ).execute()
    except Exception as e:
        log.error("Gmail API send_message failed: %s", e, exc_info=True)
        raise

    to_summary = to if isinstance(to, str) else ", ".join(_split_list(to))
    log.info(
        "Gmail API send_message ok: id=%s thread=%s to=%s",
        response.get("id", "?"),
        response.get("threadId", "?"),
        to_summary,
    )
    return response


def save_draft(
    service,
    to,
    subject: str,
    body_plain: str = "",
    body_html: str = "",
    cc=None,
    bcc=None,
    attachments: Optional[List[str]] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    thread_id: Optional[str] = None,
    label_ids: Optional[List[str]] = None,
    from_name: Optional[str] = None,
    from_addr: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> dict:
    """Save an email as a Gmail draft via the Gmail API.

    Mirrors send_message() semantics but calls users.drafts.create instead of
    messages.send. Returns the Gmail API draft response (typically
    {'id': str, 'message': {'id': str, 'threadId': str, 'labelIds': [...]}}).

    Threading parameters (added 2026-05-01 for PR-B1's reply-on-original-
    thread send flow):
      - ``in_reply_to``: RFC 2822 Message-ID of the message being replied to
        (anchors the reply in the *initial* received email per Mike's
        directive — clean threading, not threading-to-latest).
      - ``references``: full reference chain (defaults to in_reply_to).
      - ``thread_id``: Gmail's internal threadId from the inbound message;
        Gmail uses this to group the draft with the buyer's original thread
        (deeplinks then work; reply lands in the right Gmail thread). Set on
        ``body.message.threadId`` per the API spec.

    ``label_ids`` lets callers tag the draft (e.g. ``["CalVet"]`` so the
    Sent view groups by agency). Empty/None = default labels only.

    Requires the same gmail.send (or gmail.compose) scope as send_message —
    refresh token must include it or Google returns 403.
    """
    msg = _build_mime_message(
        to=to,
        subject=subject,
        body_plain=body_plain,
        body_html=body_html,
        cc=cc,
        bcc=bcc,
        attachments=attachments,
        in_reply_to=in_reply_to,
        references=references,
        from_name=from_name,
        from_addr=from_addr,
        extra_headers=extra_headers,
    )

    raw_bytes = msg.as_bytes()
    raw_b64url = base64.urlsafe_b64encode(raw_bytes).decode("ascii")

    inner: Dict[str, object] = {"raw": raw_b64url}
    if thread_id:
        inner["threadId"] = thread_id
    if label_ids:
        inner["labelIds"] = list(label_ids)
    body: Dict[str, object] = {"message": inner}

    try:
        response = service.users().drafts().create(
            userId="me", body=body
        ).execute()
    except Exception as e:
        log.error("Gmail API save_draft failed: %s", e, exc_info=True)
        raise

    to_summary = to if isinstance(to, str) else ", ".join(_split_list(to))
    log.info(
        "Gmail API save_draft ok: draft_id=%s to=%s thread=%s in_reply_to=%s",
        response.get("id", "?"),
        to_summary,
        thread_id or "(new)",
        (in_reply_to or "")[:40],
    )
    return response


def get_send_service(inbox_name: str = "sales"):
    """Get a Gmail API service authorized for sending.

    Separate from get_service() so callers self-document intent and so we can
    fail loudly if the granted scope doesn't include gmail.send. For now this
    is a thin wrapper that requires is_configured(); the actual 403 from Google
    is the only signal that the refresh token lacks send scope (user must re-run
    scripts/gmail_oauth_setup.py after deploying).
    """
    if not is_configured():
        raise RuntimeError(
            "Gmail API not configured (missing GMAIL_OAUTH_CLIENT_ID / "
            "CLIENT_SECRET / REFRESH_TOKEN env vars). Cannot send."
        )
    return get_service(inbox_name)
