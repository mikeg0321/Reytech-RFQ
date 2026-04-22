#!/usr/bin/env python3
"""
Gmail OAuth2 Setup — One-time authorization to get a refresh token.

Run this locally (not on Railway) to authorize Gmail API access:

    python scripts/gmail_oauth_setup.py

Prerequisites:
    1. Create OAuth2 credentials in Google Cloud Console:
       - Go to https://console.cloud.google.com/apis/credentials
       - Create OAuth 2.0 Client ID (type: Desktop application)
       - Download the client ID and secret

    2. Enable Gmail API and Drive API:
       - https://console.cloud.google.com/apis/library/gmail.googleapis.com
       - https://console.cloud.google.com/apis/library/drive.googleapis.com

    3. Set environment variables:
       export GMAIL_OAUTH_CLIENT_ID="your-client-id.apps.googleusercontent.com"
       export GMAIL_OAUTH_CLIENT_SECRET="your-client-secret"

    4. Install the OAuth library (if not already):
       pip install google-auth-oauthlib

After authorization, copy the printed refresh token and set it in Railway:
    GMAIL_OAUTH_REFRESH_TOKEN=<token>        (for sales@ inbox)
    GMAIL_OAUTH_REFRESH_TOKEN_2=<token>       (for mike@ inbox, if different account)
"""

import os
import sys


def main():
    client_id = os.environ.get("GMAIL_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GMAIL_OAUTH_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        print("ERROR: Set GMAIL_OAUTH_CLIENT_ID and GMAIL_OAUTH_CLIENT_SECRET env vars first.")
        print()
        print("Steps:")
        print("  1. Go to https://console.cloud.google.com/apis/credentials")
        print("  2. Create OAuth 2.0 Client ID (Desktop application)")
        print("  3. export GMAIL_OAUTH_CLIENT_ID='your-id.apps.googleusercontent.com'")
        print("  4. export GMAIL_OAUTH_CLIENT_SECRET='your-secret'")
        print("  5. Run this script again")
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("ERROR: google-auth-oauthlib not installed.")
        print("  pip install google-auth-oauthlib")
        sys.exit(1)

    # Must match SCOPES in src/core/gmail_api.py. If these diverge, refresh
    # will fail with `invalid_scope: Bad Request` the moment the runtime
    # asks for a scope the stored token was never granted.
    SCOPES = [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    # Build client config from env vars (no JSON file needed)
    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    print("Opening browser for Google authorization...")
    print(f"  Scopes: {', '.join(SCOPES)}")
    print()

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    if not creds.refresh_token:
        print("WARNING: No refresh token returned. Try revoking access at")
        print("  https://myaccount.google.com/permissions")
        print("Then run this script again.")
        sys.exit(1)

    print()
    print("=" * 60)
    print("SUCCESS! Copy this refresh token to Railway env vars:")
    print("=" * 60)
    print()
    print(f"  GMAIL_OAUTH_REFRESH_TOKEN={creds.refresh_token}")
    print()
    print("Also set these (same values for both inboxes):")
    print(f"  GMAIL_OAUTH_CLIENT_ID={client_id}")
    print(f"  GMAIL_OAUTH_CLIENT_SECRET={client_secret}")
    print()
    print("For a SECOND inbox (mike@), run this script again while")
    print("logged into the other Google account, and set:")
    print(f"  GMAIL_OAUTH_REFRESH_TOKEN_2=<new-token>")
    print()
    print("The email poller will automatically switch from IMAP to")
    print("Gmail API on the next deploy.")


if __name__ == "__main__":
    main()
