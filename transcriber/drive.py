"""Google Drive interaction helpers."""
import os
import io
from google.oauth2 import service_account
from google.auth import default
from google.auth.transport.requests import Request as AuthRequest
from google.auth.exceptions import GoogleAuthError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

PROCESSED_FOLDER_ID_CACHE = None


def _resolve_service_account_path(service_account_file: str | None) -> str | None:
    candidates = []
    if service_account_file:
        candidates.append(service_account_file)
    candidates.extend([
        os.path.join(os.path.dirname(__file__), "sa.json"),
        os.path.join(os.getcwd(), "sa.json"),
        "sa.json",
    ])
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def drive_service(skip_drive: bool, service_account_file: str | None):
    if skip_drive:
        return None
    scopes = ["https://www.googleapis.com/auth/drive"]
    sa_path = _resolve_service_account_path(service_account_file)
    if sa_path:
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
        print(f"Using service account for Drive: {sa_path}")
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        creds, _ = default(scopes=scopes)
    except Exception as e:
        raise RuntimeError(f"ADC credential load failed: {e}")
    try:
        if not creds.valid:
            creds.refresh(AuthRequest())
    except GoogleAuthError as e:
        raise RuntimeError(f"Credential refresh failed: {e}")
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_m4a_files(service, drive_folder_id: str, skip_drive: bool):
    if skip_drive:
        return []
    q = (
        f"'{drive_folder_id}' in parents "
        "and (name contains '.m4a' or name contains '.M4A') "
        "and mimeType != 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    try:
        res = service.files().list(q=q, fields="files(id,name,createdTime)").execute()
    except HttpError as e:
        raise RuntimeError(f"Drive list error: {e}")
    return res.get("files", [])


def download_file(service, file_id, dst_path, skip_drive: bool):
    if skip_drive:
        return
    fh = io.FileIO(dst_path, mode="wb")
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()


def get_or_create_processed_folder(service, parent_folder_id: str, skip_drive: bool):
    if skip_drive:
        return None
    global PROCESSED_FOLDER_ID_CACHE
    if PROCESSED_FOLDER_ID_CACHE:
        return PROCESSED_FOLDER_ID_CACHE
    folder_name = "processed"
    q = f"'{parent_folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    try:
        res = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
        items = res.get('files', [])
        if items:
            folder_id = items[0]['id']
            PROCESSED_FOLDER_ID_CACHE = folder_id
            return folder_id
    except HttpError as e:
        print(f"Error searching for '{folder_name}' folder: {e}. Will attempt to create it.")
    try:
        folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_folder_id]}
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')
        PROCESSED_FOLDER_ID_CACHE = folder_id
        print(f"Created 'processed' folder with ID: {folder_id}")
        return folder_id
    except HttpError as e:
        print(f"Fatal: Could not create 'processed' folder: {e}")
        return None


def move_file_to_folder(service, file_id, new_parent_id, old_parent_id, skip_drive: bool):
    if skip_drive:
        return
    try:
        service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=old_parent_id,
            fields='id, parents'
        ).execute()
    except HttpError as e:
        print(f"Warning: Failed to move file {file_id} to folder {new_parent_id}: {e}")

