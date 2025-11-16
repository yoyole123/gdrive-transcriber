import os
import io
import subprocess
import tempfile
import asyncio
from datetime import datetime, timezone
from google.auth import default
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import Request as AuthRequest
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import json
import aiohttp
import re
from dotenv import load_dotenv

load_dotenv()

# ----------------- Configuration -----------------
# NOTE: Defaults for secrets/IDs have been removed to avoid committing real keys.
# Configure these via environment variables in Lambda / your local shell.
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")  # Source folder containing .m4a files
EMAIL_TO = os.environ.get("EMAIL_TO")
GMAIL_SENDER_EMAIL = os.environ.get("GMAIL_SENDER_EMAIL")  # The Gmail address you're sending from
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")  # Gmail App Password (must be set in env)
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY")
RUNPOD_ENDPOINT_ID = os.environ.get("RUNPOD_ENDPOINT_ID")
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "..", "config.json"))

# Segment duration (30 minutes)
SEG_SECONDS = 10 * 60
TEMP_DIR = os.path.join(tempfile.gettempdir(), "drive_work")
os.makedirs(TEMP_DIR, exist_ok=True)
MAX_SEGMENT_CONCURRENCY = int(os.environ.get("MAX_SEGMENT_CONCURRENCY", "2"))  # limit parallel segment jobs to avoid RunPod worker exhaustion

# Unicode cleanup similar to app.py
def clean_some_unicode_from_text(text: str) -> str:
    chars_to_remove = "\u061C"  # Arabic letter mark
    chars_to_remove += "\u200B\u200C\u200D"  # Zero-width space, non/ joiner
    chars_to_remove += "\u200E\u200F"  # LTR/RTL marks
    chars_to_remove += "\u202A\u202B\u202C\u202D\u202E"  # embeddings/overrides
    chars_to_remove += "\u2066\u2067\u2068\u2069"  # isolate controls
    chars_to_remove += "\uFEFF"  # zero-width no-break space
    return text.translate({ord(c): None for c in chars_to_remove})

# Optional flag to skip Drive calls (for local dry-run/testing)
SKIP_DRIVE = os.environ.get("SKIP_DRIVE") == "1"

# ----------------- Credential Helpers -----------------

def _resolve_service_account_path():
    candidates = []
    if SERVICE_ACCOUNT_FILE:
        candidates.append(SERVICE_ACCOUNT_FILE)
    candidates.append(os.path.join(os.path.dirname(__file__), "sa.json"))
    candidates.append("sa.json")
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def drive_service():
    if SKIP_DRIVE:
        return None
    scopes = ["https://www.googleapis.com/auth/drive"]
    sa_path = _resolve_service_account_path()
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

# ----------------- Drive Helpers -----------------

def list_m4a_files(service):
    if SKIP_DRIVE:
        return []
    q = (
        f"'{DRIVE_FOLDER_ID}' in parents "
        "and (name contains '.m4a' or name contains '.M4A') "
        "and mimeType != 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    try:
        res = service.files().list(q=q, fields="files(id,name,createdTime)").execute()
    except HttpError as e:
        raise RuntimeError(f"Drive list error: {e}")
    return res.get("files", [])


def download_file(service, file_id, dst_path):
    if SKIP_DRIVE:
        return
    fh = io.FileIO(dst_path, mode="wb")
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

# ----------------- Audio Processing -----------------

def convert_m4a_to_mp3(m4a_path, mp3_path):
    subprocess.check_call(["ffmpeg", "-y", "-i", m4a_path, "-vn", "-acodec", "libmp3lame", "-q:a", "2", mp3_path])


def split_mp3(mp3_path, out_pattern):
    subprocess.check_call([
        "ffmpeg", "-y", "-i", mp3_path,
        "-f", "segment", "-segment_time", str(SEG_SECONDS),
        "-c", "copy", out_pattern
    ])

# ----------------- Drive File Management Helpers -----------------
PROCESSED_FOLDER_ID_CACHE = None

def get_or_create_processed_folder(service, parent_folder_id):
    if SKIP_DRIVE:
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

def move_file_to_folder(service, file_id, new_parent_id, old_parent_id):
    if SKIP_DRIVE:
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

# ----------------- Email (Gmail SMTP) -----------------

def send_transcription_email(subject, body_text, attachment_path):
    if not GMAIL_APP_PASSWORD:
        print("GMAIL_APP_PASSWORD missing; skipping email.")
        return False
    message = MIMEMultipart()
    message["From"] = GMAIL_SENDER_EMAIL
    message["To"] = EMAIL_TO
    message["Subject"] = subject
    message.attach(MIMEText(body_text, "plain", "utf-8"))
    if attachment_path and os.path.exists(attachment_path):
        part = MIMEBase('application', 'octet-stream')
        with open(attachment_path, 'rb') as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
        message.attach(part)
    context = ssl.create_default_context()
    smtp_server = "smtp.gmail.com"
    port = 465
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(GMAIL_SENDER_EMAIL, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER_EMAIL, EMAIL_TO, message.as_string())
        print("Email sent successfully via Gmail SMTP!")
        return True
    except smtplib.SMTPAuthenticationError:
        print("Gmail SMTP authentication failed. Check GMAIL_SENDER_EMAIL and GMAIL_APP_PASSWORD.")
        return False
    except Exception as e:
        print(f"An error occurred while sending email via Gmail SMTP: {e}")
        return False

# ----------------- RunPod / Model Helpers -----------------

def load_model(language="he"):
    if not RUNPOD_API_KEY or not RUNPOD_ENDPOINT_ID:
        raise RuntimeError("RUNPOD_API_KEY or RUNPOD_ENDPOINT_ID not set.")
    # Lazy import to allow smoke tests without ivrit installed
    import ivrit
    # Load config to resolve model per language
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        lang_cfg = cfg.get("languages", {}).get(language)
        if not lang_cfg:
            raise RuntimeError(f"Language '{language}' not found in config.")
        model_name = lang_cfg.get("model")
        if not model_name:
            raise RuntimeError(f"Model not configured for language '{language}'.")
    except Exception as e:
        raise RuntimeError(f"Failed loading config: {e}")
    print(f"Loading model '{model_name}' for language '{language}' via RunPod endpoint {RUNPOD_ENDPOINT_ID}...")
    return ivrit.load_model(engine='runpod', model=model_name, api_key=RUNPOD_API_KEY, endpoint_id=RUNPOD_ENDPOINT_ID, core_engine='stable-whisper')

async def transcribe_segment(model, segment_path, index):
    """Transcribe a single audio segment using async streaming."""
    try:
        print(f"Transcribing segment {index}: {segment_path}")
        segs = model.transcribe_async(path=segment_path, diarize=True)
        collected = []
        async for s in segs:
            collected.append(clean_some_unicode_from_text(s.text))
        text = "\n".join(collected)
        print(f"Finished segment {index}")
        return {"index": index, "text": text}
    except Exception as e:
        print(f"Error transcribing segment {index}: {e}")
        return {"index": index, "text": f"[ERROR segment {index}: {e}]"}

async def transcribe_file(model, mp3_full_path, work_dir):
    """Split the full mp3 into 30-min segments then transcribe each respecting concurrency limit."""
    out_pattern = os.path.join(work_dir, "seg%03d.mp3")
    if os.environ.get("BYPASS_SPLIT") == "1":
        segments = sorted([f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)])
    else:
        split_mp3(mp3_full_path, out_pattern)
        segments = sorted([f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)])
    if not segments:
        return "", []
    sem = asyncio.Semaphore(MAX_SEGMENT_CONCURRENCY)

    async def run_segment(idx, fname):
        seg_path = os.path.join(work_dir, fname)
        async with sem:
            return await transcribe_segment(model, seg_path, idx)

    tasks = [asyncio.create_task(run_segment(idx, fname)) for idx, fname in enumerate(segments)]
    results = await asyncio.gather(*tasks)
    ordered = sorted(results, key=lambda r: r["index"])
    full_text = "\n\n".join(r["text"] for r in ordered)
    return full_text, segments

async def fetch_runpod_balance(api_key: str):
    """Fetch RunPod balance similar to app.py get_balance logic."""
    if not api_key:
        return None
    GRAPHQL_URL = "https://api.runpod.io/graphql"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    query = """
    query {
        myself {
            clientBalance
            currentSpendPerHr
            spendLimit
        }
    }
    """
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(GRAPHQL_URL, headers=headers, json={"query": query}) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if "errors" in data:
                    return None
                myself = data.get("data", {}).get("myself", {})
                return {
                    "clientBalance": myself.get("clientBalance"),
                    "currentSpendPerHr": myself.get("currentSpendPerHr"),
                    "spendLimit": myself.get("spendLimit"),
                }
    except Exception:
        return None

# ----------------- Core Flow -----------------

async def process_drive_files():
    if SKIP_DRIVE:
        print("SKIP_DRIVE=1 set; treating as no files.")
        return {"status": "no audio files found", "total_files": 0}
    if not DRIVE_FOLDER_ID:
        return {"error": "DRIVE_FOLDER_ID env var is required"}
    try:
        drive_svc = drive_service()
    except Exception as e:
        return {"error": "auth_drive_failed", "detail": str(e)}
    try:
        files = list_m4a_files(drive_svc)
    except Exception as e:
        return {"error": "drive_list_failed", "detail": str(e)}
    if not files:
        print("No .m4a files found.")
        return {"status": "no audio files found", "total_files": 0}
    processed_folder_id = get_or_create_processed_folder(drive_svc, DRIVE_FOLDER_ID)
    if not processed_folder_id:
        return {"error": "drive_processed_folder_failure", "detail": "Could not find or create 'processed' folder."}
    try:
        model = load_model(language="he")
    except Exception as e:
        return {"error": "model_load_failed", "detail": str(e)}

    # Fetch initial balance once; may fetch per file again for accuracy after each transcription
    initial_balance = await fetch_runpod_balance(RUNPOD_API_KEY)

    summaries = []
    for f in files:
        fid = f.get("id")
        name = f.get("name")
        created = f.get("createdTime")
        if created:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00")).astimezone(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        ts_dir_name = dt.strftime("%Y-%m-%d_%H-%M")
        work_dir = os.path.join(TEMP_DIR, fid)
        os.makedirs(work_dir, exist_ok=True)
        m4a_path = os.path.join(work_dir, name)
        mp3_full = os.path.join(work_dir, name + ".mp3")
        # Download
        try:
            download_file(drive_svc, fid, m4a_path)
        except Exception as e:
            print(f"Download failed {fid}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"download_failed: {e}"})
            continue
        # Convert
        try:
            convert_m4a_to_mp3(m4a_path, mp3_full)
        except subprocess.CalledProcessError as e:
            print(f"Conversion failed {name}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"conversion_failed: {e}"})
            continue
        # Transcribe (split + concurrency)
        try:
            full_text, segments = await transcribe_file(model, mp3_full, work_dir)
        except Exception as e:
            print(f"Transcription failed {name}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"transcription_failed: {e}"})
            continue
        # After transcription, fetch current balance (may have changed slightly)
        balance_info = await fetch_runpod_balance(RUNPOD_API_KEY) or initial_balance
        balance_val = balance_info.get("clientBalance") if balance_info else "N/A"
        spend_hr_val = balance_info.get("currentSpendPerHr") if balance_info else "N/A"
        limit_val = balance_info.get("spendLimit") if balance_info else "N/A"

        # Ensure all values are strings for concatenation
        balance_str = str(balance_val)
        spend_hr = str(spend_hr_val)
        limit_str = str(limit_val)

        # Write transcription file
        base_name = os.path.splitext(name)[0]
        transcription_filename = f"{base_name}_transcription.txt"
        transcription_path = os.path.join(work_dir, transcription_filename)
        try:
            with open(transcription_path, 'w', encoding='utf-8') as tf:
                tf.write(full_text)
        except Exception as e:
            print(f"Failed to write transcription file {transcription_filename}: {e}")
        # Email (include balance)
        email_subject = f"Transcription: {base_name} (Balance: {balance_str})"
        email_body = (
            f"Transcription for file {name} (segments: {len(segments)})\n"
            f"Timestamp folder: {ts_dir_name}\n"
            f"RunPod Balance: {balance_str} | Spend/hr: {spend_hr} | Limit: {limit_str}\n\n"
            f"{full_text[:5000]}\n\n"
            f"--\nRemaining RunPod balance after this transcription: {balance_str}"
        )
        email_sent = send_transcription_email(email_subject, email_body, transcription_path)
        # Move original file to processed folder
        move_file_to_folder(drive_svc, fid, processed_folder_id, DRIVE_FOLDER_ID)
        # Cleanup working dir (optional - keep for debug?). We'll clean.
        try:
            for p in os.listdir(work_dir):
                try:
                    os.remove(os.path.join(work_dir, p))
                except Exception:
                    pass
            os.rmdir(work_dir)
        except Exception:
            pass
        summaries.append({"id": fid, "name": name, "segments": len(segments), "email_sent": email_sent, "balance": balance_str})
    return {"processed": summaries, "total_files": len(summaries)}

# ----------------- Test Harness -----------------

async def _smoke_test_concurrency():
    print("Running concurrency smoke test...")
    work_dir = tempfile.mkdtemp(prefix="seg_smoke_")
    try:
        # Create dummy segment files
        for i in range(3):
            p = os.path.join(work_dir, f"seg{i:03d}.mp3")
            with open(p, 'wb') as f:
                f.write(b"\x00")

        class FakeSeg:
            def __init__(self, t):
                self.text = t
        class FakeModel:
            def transcribe_async(self, path=None, url=None, diarize=False):
                async def gen():
                    # yield a couple of chunks per segment to simulate streaming
                    base = os.path.basename(path)
                    yield FakeSeg(f"start:{base}")
                    await asyncio.sleep(0.01)
                    yield FakeSeg(f"end:{base}")
                return gen()
        model = FakeModel()
        os.environ["BYPASS_SPLIT"] = "1"
        text, segs = await transcribe_file(model, mp3_full_path=os.path.join(work_dir, "dummy.mp3"), work_dir=work_dir)
        print("Segments:", segs)
        print("Text:\n", text)
        assert "seg000.mp3" in text and "seg001.mp3" in text and "seg002.mp3" in text
        print("Smoke test passed.")
    finally:
        try:
            for f in os.listdir(work_dir):
                os.remove(os.path.join(work_dir, f))
            os.rmdir(work_dir)
        except Exception:
            pass

# ----------------- Lambda Handler & Entry Point -----------------

async def _run_process_drive_files():
    """Internal async wrapper so both Lambda and CLI can run the same logic."""
    return await process_drive_files()


def lambda_handler(event, context):
    """AWS Lambda entry point.

    Invokes the same logic as the CLI main() and returns the result dictionary.
    """
    # Lambda does not have a running event loop by default in Python 3.11 runtime, so use asyncio.run
    result = asyncio.run(_run_process_drive_files())
    return result


def main():
    """Local CLI entry point.

    Usage (from project root):
        python -m auto_splitter.main
    """
    print("Starting scheduled Drive transcription run (local CLI)...")
    result = asyncio.run(process_drive_files())
    print("Run result:")
    print(result)
    return result


if __name__ == "__main__":
    if os.environ.get("RUN_SMOKE_TEST") == "1":
        asyncio.run(_smoke_test_concurrency())
    else:
        main()
