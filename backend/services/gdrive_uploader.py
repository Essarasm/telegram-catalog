"""Google Drive uploader for the /foto catalog workflow.

Uses OAuth user-delegated credentials (refresh-token flow) rather than a
service account. Service accounts on personal Gmail can't own files —
they have 0 GB of storage quota, so any upload fails with
`storageQuotaExceeded`. With user-delegated OAuth the bot impersonates
the configured user; files are created in that user's Drive and counted
against their quota.

Lazy-initialized so the bot keeps booting even if Drive credentials are
missing or malformed — the /foto handler surfaces a clear error to the
group instead of crashing the whole process.

Env vars:
  GDRIVE_CLIENT_ID                  — OAuth client ID (Desktop application type)
  GDRIVE_CLIENT_SECRET              — OAuth client secret
  GDRIVE_REFRESH_TOKEN              — refresh token captured by tools/gdrive_auth.py
  GDRIVE_EMPLOYEE_UPLOADS_FOLDER_ID — destination folder ID in Drive

Run `python tools/gdrive_auth.py path/to/oauth_client.json` locally to
capture the refresh token. The script prints the three Railway commands
to paste.
"""
from __future__ import annotations

import io
import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
_TOKEN_URI = "https://oauth2.googleapis.com/token"

_client_lock = threading.Lock()
_service = None
_init_error: Optional[str] = None


def _build_service():
    """Build the Drive v3 service object. Returns (service, error_msg).
    On any failure returns (None, "<reason>") so callers can surface it
    without raising into the bot loop."""
    client_id = os.getenv("GDRIVE_CLIENT_ID", "").strip()
    client_secret = os.getenv("GDRIVE_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("GDRIVE_REFRESH_TOKEN", "").strip()

    missing = [
        name for name, val in [
            ("GDRIVE_CLIENT_ID", client_id),
            ("GDRIVE_CLIENT_SECRET", client_secret),
            ("GDRIVE_REFRESH_TOKEN", refresh_token),
        ]
        if not val
    ]
    if missing:
        return None, f"Missing env vars: {', '.join(missing)}"

    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as e:
        return None, f"google-api-python-client not installed: {e}"

    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri=_TOKEN_URI,
            client_id=client_id,
            client_secret=client_secret,
            scopes=_SCOPES,
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return service, None
    except Exception as e:
        return None, f"Failed to init Drive service: {e}"


def _get_service():
    """Lazy singleton accessor. Caches both the service and any init error
    so we don't re-attempt creation on every upload."""
    global _service, _init_error
    if _service is not None or _init_error is not None:
        return _service, _init_error
    with _client_lock:
        if _service is not None or _init_error is not None:
            return _service, _init_error
        _service, _init_error = _build_service()
    return _service, _init_error


def is_configured() -> bool:
    """True if all four env vars are set. Use this to gate the /foto
    command so an unconfigured deploy surfaces a clear setup error
    instead of a runtime upload failure on the first photo."""
    return bool(
        os.getenv("GDRIVE_CLIENT_ID", "").strip()
        and os.getenv("GDRIVE_CLIENT_SECRET", "").strip()
        and os.getenv("GDRIVE_REFRESH_TOKEN", "").strip()
        and os.getenv("GDRIVE_EMPLOYEE_UPLOADS_FOLDER_ID", "").strip()
    )


def upload_bytes(
    data: bytes,
    filename: str,
    mime_type: str = "application/octet-stream",
) -> dict:
    """Upload raw bytes to the configured Drive folder.

    Returns {"id": <drive_file_id>, "name": <stored_name>} on success.
    Raises RuntimeError with a human-readable message on failure — the
    caller (bot handler) should catch and reply to the group.
    """
    service, err = _get_service()
    if err:
        raise RuntimeError(f"Drive not configured: {err}")

    folder_id = os.getenv("GDRIVE_EMPLOYEE_UPLOADS_FOLDER_ID", "").strip()
    if not folder_id:
        raise RuntimeError("GDRIVE_EMPLOYEE_UPLOADS_FOLDER_ID env var is empty")

    from googleapiclient.http import MediaIoBaseUpload

    media = MediaIoBaseUpload(
        io.BytesIO(data),
        mimetype=mime_type,
        resumable=False,
    )
    metadata = {"name": filename, "parents": [folder_id]}
    try:
        # num_retries triggers exponential backoff inside googleapiclient
        # for transient errors (SSL EOF, 5xx, network resets between Railway
        # and Google's servers). Without this a single blip during upload
        # surfaces to the user as a hard failure even though a retry would
        # succeed. 3 retries adds at most ~7s of latency on flaky links.
        result = service.files().create(
            body=metadata,
            media_body=media,
            fields="id, name",
            supportsAllDrives=True,
        ).execute(num_retries=3)
    except Exception as e:
        raise RuntimeError(f"Drive upload failed: {e}") from e

    return {"id": result.get("id"), "name": result.get("name")}
