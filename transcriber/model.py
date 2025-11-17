"""Model loading + transcription logic."""
from __future__ import annotations
import os
import re
import asyncio
from typing import List, Dict, Any, Tuple
from .utils import clean_some_unicode_from_text

async def transcribe_segment(model, segment_path, index):
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


async def transcribe_file(model, mp3_full_path: str, work_dir: str, seg_seconds: int, max_concurrency: int, bypass_split: bool, splitter_fn):
    out_pattern = os.path.join(work_dir, "seg%03d.mp3")
    if bypass_split:
        segments = sorted([f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)])
    else:
        splitter_fn(mp3_full_path, out_pattern, seg_seconds)
        segments = sorted([f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)])
    if not segments:
        return "", []
    sem = asyncio.Semaphore(max_concurrency)

    async def run_segment(idx, fname):
        seg_path = os.path.join(work_dir, fname)
        async with sem:
            return await transcribe_segment(model, seg_path, idx)

    tasks = [asyncio.create_task(run_segment(idx, fname)) for idx, fname in enumerate(segments)]
    results = await asyncio.gather(*tasks)
    ordered = sorted(results, key=lambda r: r["index"])
    full_text = "\n\n".join(r["text"] for r in ordered)
    return full_text, segments


def load_model(runpod_api_key: str | None, runpod_endpoint_id: str | None, languages_cfg: Dict[str, Any], language: str = "he"):
    if not runpod_api_key or not runpod_endpoint_id:
        raise RuntimeError("RUNPOD_API_KEY or RUNPOD_ENDPOINT_ID not set.")
    import ivrit  # Lazy import
    lang_cfg = languages_cfg.get(language)
    if not lang_cfg:
        raise RuntimeError(f"Language '{language}' not found in config.")
    model_name = lang_cfg.get("model")
    if not model_name:
        raise RuntimeError(f"Model not configured for language '{language}'.")
    print(f"Loading model '{model_name}' for language '{language}' via RunPod endpoint {runpod_endpoint_id}...")
    return ivrit.load_model(engine='runpod', model=model_name, api_key=runpod_api_key, endpoint_id=runpod_endpoint_id, core_engine='stable-whisper')

