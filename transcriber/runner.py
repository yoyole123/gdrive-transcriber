"""Orchestrator logic for end-to-end processing of Drive audio files."""
from __future__ import annotations
import os
import tempfile
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any

from .config import load_config
from .drive import drive_service, list_m4a_files, download_file, get_or_create_processed_folder, move_file_to_folder
from .audio import convert_m4a_to_mp3, split_mp3
from .model import load_model, transcribe_file
from .emailer import send_transcription_email

import aiohttp

TEMP_DIR = os.path.join(tempfile.gettempdir(), "drive_work")
os.makedirs(TEMP_DIR, exist_ok=True)


async def fetch_runpod_balance(api_key: str | None):
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


async def process_drive_files(cfg) -> Dict[str, Any]:
    if not cfg.within_schedule_window:
        return {"status": "outside schedule window"}
    if cfg.skip_drive:
        print("SKIP_DRIVE=1 set; treating as no files.")
        return {"status": "no audio files found", "total_files": 0}
    if not cfg.drive_folder_id:
        return {"error": "DRIVE_FOLDER_ID env var is required"}
    try:
        drive_svc = drive_service(cfg.skip_drive, cfg.service_account_file)
    except Exception as e:
        return {"error": "auth_drive_failed", "detail": str(e)}
    try:
        files = list_m4a_files(drive_svc, cfg.drive_folder_id, cfg.skip_drive)
    except Exception as e:
        return {"error": "drive_list_failed", "detail": str(e)}
    if not files:
        print("No .m4a files found.")
        return {"status": "no audio files found", "total_files": 0}
    processed_folder_id = get_or_create_processed_folder(drive_svc, cfg.drive_folder_id, cfg.skip_drive)
    if not processed_folder_id:
        return {"error": "drive_processed_folder_failure", "detail": "Could not find or create 'processed' folder."}
    try:
        model = load_model(cfg.runpod_api_key, cfg.runpod_endpoint_id, cfg.languages, language="he")
    except Exception as e:
        return {"error": "model_load_failed", "detail": str(e)}

    initial_balance = await fetch_runpod_balance(cfg.runpod_api_key)
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
        try:
            download_file(drive_svc, fid, m4a_path, cfg.skip_drive)
        except Exception as e:
            print(f"Download failed {fid}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"download_failed: {e}"})
            continue
        try:
            convert_m4a_to_mp3(m4a_path, mp3_full)
        except Exception as e:
            print(f"Conversion failed {name}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"conversion_failed: {e}"})
            continue
        try:
            full_text, segments = await transcribe_file(
                model,
                mp3_full_path=mp3_full,
                work_dir=work_dir,
                seg_seconds=cfg.seg_seconds,
                max_concurrency=cfg.max_segment_concurrency,
                bypass_split=cfg.bypass_split,
                splitter_fn=split_mp3,
            )
        except Exception as e:
            print(f"Transcription failed {name}: {e}")
            summaries.append({"id": fid, "name": name, "error": f"transcription_failed: {e}"})
            continue
        balance_info = await fetch_runpod_balance(cfg.runpod_api_key) or initial_balance
        balance_val = balance_info.get("clientBalance") if balance_info else "N/A"
        spend_hr_val = balance_info.get("currentSpendPerHr") if balance_info else "N/A"
        limit_val = balance_info.get("spendLimit") if balance_info else "N/A"
        balance_str = str(balance_val)
        spend_hr = str(spend_hr_val)
        limit_str = str(limit_val)
        base_name = os.path.splitext(name)[0]
        transcription_filename = f"{base_name}_transcription.txt"
        transcription_path = os.path.join(work_dir, transcription_filename)
        try:
            with open(transcription_path, 'w', encoding='utf-8') as tf:
                tf.write(full_text)
        except Exception as e:
            print(f"Failed to write transcription file {transcription_filename}: {e}")
        email_subject = f"Transcription: {base_name} (Balance: {balance_str})"
        email_body = (
            f"Transcription for file {name} (segments: {len(segments)})\n"
            f"Timestamp folder: {ts_dir_name}\n"
            f"RunPod Balance: {balance_str} | Spend/hr: {spend_hr} | Limit: {limit_str}\n\n"
            f"{full_text[:5000]}\n\n"
            f"--\nRemaining RunPod balance after this transcription: {balance_str}"
        )
        email_sent = send_transcription_email(
            cfg.gmail_app_password,
            cfg.gmail_sender_email,
            cfg.email_to,
            email_subject,
            email_body,
            transcription_path
        )
        move_file_to_folder(drive_svc, fid, processed_folder_id, cfg.drive_folder_id, cfg.skip_drive)
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


async def run() -> Dict[str, Any]:
    cfg = load_config()
    return await process_drive_files(cfg)


def main():
    print("Starting scheduled Drive transcription run (local CLI)...")
    result = asyncio.run(run())
    print("Run result:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result

if __name__ == "__main__":
    main()

