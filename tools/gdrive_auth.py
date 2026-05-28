"""One-time helper to capture a Google Drive OAuth refresh token.

Run this LOCALLY on your laptop (not on Railway). It opens a browser
window where you sign in with the Gmail account that owns the target
Drive folder, then prints the Railway env-var commands.

Why this script exists:
  Service accounts on personal Gmail can't own files in Drive (they
  have 0 GB of quota), so any upload via service-account credentials
  fails with `storageQuotaExceeded`. User-delegated OAuth tokens
  impersonate a real user — files are created in that user's Drive and
  count against their quota normally.

Usage:
  pip install google-auth-oauthlib
  python tools/gdrive_auth.py path/to/oauth_client.json

The path is the JSON file downloaded from GCP Console → Credentials →
OAuth client ID (Desktop application type).
"""
from __future__ import annotations

import json
import sys

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python tools/gdrive_auth.py path/to/oauth_client.json")
        sys.exit(1)

    client_secrets_file = sys.argv[1]

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("ERROR: google-auth-oauthlib not installed.")
        print("Run: pip install google-auth-oauthlib")
        sys.exit(1)

    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        access_type="offline",
        open_browser=True,
    )

    if not creds.refresh_token:
        print()
        print("ERROR: No refresh token returned. Did you click 'Continue' on the")
        print("'This app isn't verified' screen? If you see this error twice,")
        print("revoke the app at https://myaccount.google.com/permissions and")
        print("re-run this script.")
        sys.exit(1)

    with open(client_secrets_file) as f:
        client_data = json.load(f)

    cfg = client_data.get("installed") or client_data.get("web") or {}
    client_id = cfg.get("client_id", "")
    client_secret = cfg.get("client_secret", "")

    print()
    print("=" * 72)
    print("SUCCESS — paste these into your terminal:")
    print("=" * 72)
    print()
    print(f'railway variable set --skip-deploys GDRIVE_CLIENT_ID="{client_id}"')
    print(f'railway variable set --skip-deploys GDRIVE_CLIENT_SECRET="{client_secret}"')
    print(f'railway variable set --skip-deploys GDRIVE_REFRESH_TOKEN="{creds.refresh_token}"')
    print()
    print("Then remove the (now-unused) service-account variable:")
    print("  railway variable delete GDRIVE_SERVICE_ACCOUNT_JSON")
    print()
    print("Refresh tokens stay valid until you explicitly revoke them or the")
    print("Google account hasn't been used for 6+ months.")


if __name__ == "__main__":
    main()
