"""Orchestrator logic for end-to-end processing of Drive audio files."""
from __future__ import annotations
import os
import tempfile
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from .config import load_config, Config
from .drive import (
    drive_service,
    list_audio_files,
    download_file,
    get_or_create_processed_folder,
    move_file_to_folder,
)
from .audio import convert_to_mp3, split_mp3_by_size  # size-based splitter
from .model import load_model, transcribe_file
from .emailer import send_transcription_email
from .utils import sanitize_filename, generate_positive_personal_message

import aiohttp
from . import logger

TEMP_DIR = os.path.join(tempfile.gettempdir(), "drive_work")
os.makedirs(TEMP_DIR, exist_ok=True)


async def fetch_runpod_balance(api_key: str | None) -> Optional[Dict[str, Any]]:
    """Fetch RunPod account balance via GraphQL; returns None on error."""
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
                myself: dict = data.get("data", {}).get("myself", {})
                return {
                    "clientBalance": myself.get("clientBalance"),
                    "currentSpendPerHr": myself.get("currentSpendPerHr"),
                    "spendLimit": myself.get("spendLimit"),
                }
    except Exception:
        return None


async def process_drive_files(cfg: Config) -> Dict[str, Any]:
    """Process all new audio files in the configured Drive folder.

    Returns a summary dict with processed items or error/status info.
    """
    if not cfg.within_schedule_window:
        return {"status": "outside schedule window"}
    if cfg.skip_drive:
        logger.info("SKIP_DRIVE=1 set; treating as no files.")
        return {"status": "no audio files found", "total_files": 0}
    if not cfg.drive_folder_id:
        return {"error": "DRIVE_FOLDER_ID env var is required"}
    try:
        drive_svc = drive_service(cfg.skip_drive, cfg.service_account_file)
    except Exception as e:
        return {"error": "auth_drive_failed", "detail": str(e)}
    try:
        files: List[Dict[str, Any]] = list_audio_files(drive_svc, cfg.drive_folder_id, cfg.skip_drive)
    except Exception as e:
        return {"error": "drive_list_failed", "detail": str(e)}
    if not files:
        logger.info("No audio files found.")
        return {"status": "no audio files found", "total_files": 0}
    processed_folder_id = get_or_create_processed_folder(drive_svc, cfg.drive_folder_id, cfg.skip_drive)
    if not processed_folder_id:
        return {"error": "drive_processed_folder_failure", "detail": "Could not find or create 'processed' folder."}
    try:
        transcription_language = getattr(cfg, "transcription_language", "en")
        model = load_model(cfg.runpod_api_key, cfg.runpod_endpoint_id, cfg.languages, language=transcription_language)
    except Exception as e:
        return {"error": "model_load_failed", "detail": str(e)}

    initial_balance = await fetch_runpod_balance(cfg.runpod_api_key)
    summaries: List[Dict[str, Any]] = []
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
        audio_input_path = os.path.join(work_dir, name)  # original downloaded file
        mp3_full = os.path.join(work_dir, os.path.splitext(name)[0] + ".mp3")
        try:
            download_file(drive_svc, fid, audio_input_path, cfg.skip_drive)
        except Exception as e:
            logger.error("Download failed %s: %s", fid, e)
            summaries.append({"id": fid, "name": name, "error": f"download_failed: {e}"})
            continue
        try:
            convert_to_mp3(audio_input_path, mp3_full)
        except Exception as e:
            logger.error("Conversion failed %s: %s", name, e)
            summaries.append({"id": fid, "name": name, "error": f"conversion_failed: {e}"})
            continue

        def splitter_callable(src: str, pattern: str, seg_secs: int, max_segment_size: int = cfg.max_segment_size) -> None:
            """Wrapper preserving expected arg order for transcribe_file splitter."""
            return split_mp3_by_size(src, pattern, max_segment_size, seg_secs)
        try:
            full_text, segments = await transcribe_file(
                model,
                mp3_full_path=mp3_full,
                work_dir=work_dir,
                seg_seconds=cfg.seg_seconds,
                max_concurrency=cfg.max_segment_concurrency,
                bypass_split=cfg.bypass_split,
                splitter_fn=splitter_callable,
                max_segment_retries=cfg.max_segment_retries,
                max_payload_size=cfg.max_payload_size,
                max_split_depth=cfg.max_split_depth,
            )
        except Exception as e:
            logger.error("Transcription failed %s: %s", name, e)
            summaries.append({"id": fid, "name": name, "error": f"transcription_failed: {e}"})
            continue
        balance_info = await fetch_runpod_balance(cfg.runpod_api_key) or initial_balance
        balance_val = balance_info.get("clientBalance") if balance_info else "N/A"
        spend_hr_val = balance_info.get("currentSpendPerHr") if balance_info else "N/A"
        limit_val = balance_info.get("spendLimit") if balance_info else "N/A"
        balance_str = str(balance_val)
        spend_hr = str(spend_hr_val)
        limit_str = str(limit_val)
        base_name_raw = os.path.splitext(name)[0]
        base_name = sanitize_filename(base_name_raw)
        transcription_filename = f"{base_name}_transcription.txt"
        transcription_path = os.path.join(work_dir, transcription_filename)
        try:
            with open(transcription_path, 'w', encoding='utf-8') as tf:
                tf.write(full_text)
        except Exception as e:
            logger.error("Failed to write transcription file %s: %s", transcription_filename, e)
        email_subject_balance_part = str(balance_str)
        low_balance_suffix = ""
        try:
            bal_f = float(balance_str)
            if bal_f < cfg.balance_alert_value:
                low_balance_suffix = " LOW BALANCE!"
        except Exception:
            pass
        email_subject = f"Transcription: {base_name} (Balance: {email_subject_balance_part}{low_balance_suffix})"
        personal_prefix = ""
        if cfg.add_random_personal_message:
            try:
                personal_prefix = generate_positive_personal_message(cfg.email_to) + "\n\n"
            except Exception as e:
                logger.error("Failed to generate personal message: %s", e)
                personal_prefix = ""
        email_body_main = (
            f"Transcription for file {name} (segments: {len(segments)})\n"
            f"Timestamp folder: {ts_dir_name}\n"
            f"RunPod Balance: {balance_str} | Spend/hr: {spend_hr} | Limit: {limit_str}\n\n"
            f"{full_text[:5000]}\n\n"
            f"--\nRemaining RunPod balance after this transcription: {balance_str}"
        )
        email_body = personal_prefix + email_body_main
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
        summaries.append({
            "id": fid,
            "name": name,
            "segments": len(segments),
            "email_sent": email_sent,
            "balance": balance_str,
        })
    return {"processed": summaries, "total_files": len(summaries)}


async def run() -> Dict[str, Any]:
    """Load config and trigger processing; small wrapper for local/Lambda entrypoints."""
    cfg = load_config()
    return await process_drive_files(cfg)


def main() -> Dict[str, Any]:
    logger.info("Starting scheduled Drive transcription run (local CLI)...")
    result = asyncio.run(run())
    logger.info("Run result:\n%s", json.dumps(result, indent=2, ensure_ascii=False))
    return result

if __name__ == "__main__":
    main()