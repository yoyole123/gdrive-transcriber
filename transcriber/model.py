"""Model loading + transcription logic with minimal recursive payload splitting."""
from __future__ import annotations
import os
import re
import asyncio
import subprocess
from typing import List, Dict, Any
from .utils import clean_some_unicode_from_text

PAYLOAD_ERR_RE = re.compile(r"Payload length is (\d+), exceeding max payload length of (\d+)")


def _format_ts(seconds: float) -> str:
    total = int(round(seconds))
    h = total // 3600; m = (total % 3600) // 60; s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _probe_duration(path: str) -> float:
    try:
        out = subprocess.check_output([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", path
        ], stderr=subprocess.DEVNULL).decode().strip()
        return float(out)
    except Exception:
        return 0.0


def _encode_slice(src: str, dst: str, start: float, dur: float):
    subprocess.check_call([
        "ffmpeg", "-y", "-ss", f"{start:.3f}", "-t", f"{dur:.3f}", "-i", src,
        "-vn", "-acodec", "copy", dst
    ], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)


async def transcribe_segment(model, segment_path: str, index: int, start_s: float, end_s: float, max_retries: int, payload_size_cap: int) -> Dict[str, Any]:
    """Transcribe a single segment (async). Signals split on payload error pattern.

    Retries performed with simple linear backoff using asyncio.sleep.
    """
    attempts = max_retries + 1
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            print(f"Transcribing segment {index} attempt {attempt}/{attempts}: {segment_path}")
            segs = model.transcribe_async(path=segment_path, diarize=True)
            collected: List[str] = []
            async for s in segs:
                collected.append(clean_some_unicode_from_text(getattr(s, 'text', str(s))))
            text = "\n".join(collected).strip()
            if text:
                return {"index": index, "text": text, "start_s": start_s, "end_s": end_s}
            last_error = "empty transcription"
        except Exception as e:
            msg = str(e)
            m = PAYLOAD_ERR_RE.search(msg)
            if m:
                print(f"Payload error detected for segment {index}: {msg}. Marking for split.")
                return {"index": index, "split_required": True, "segment_path": segment_path, "start_s": start_s, "end_s": end_s}
            last_error = msg
            print(f"Error segment {index} attempt {attempt}: {msg}")
        if attempt < attempts:
            await asyncio.sleep(attempt)  # linear backoff
    placeholder = f"[Transcription failed - {_format_ts(start_s)} - {_format_ts(end_s)} Reason: {last_error or 'unknown'}]"
    return {"index": index, "text": placeholder, "start_s": start_s, "end_s": end_s}


async def _recursive_split_and_transcribe(model, path: str, start_s: float, end_s: float, depth: int, max_depth: int, payload_size_cap: int, max_retries: int) -> List[Dict[str, Any]]:
    """Async recursive split of oversized segment until size under cap or depth limit reached."""
    results: List[Dict[str, Any]] = []
    raw_size = os.path.getsize(path) if os.path.exists(path) else 0
    if raw_size <= payload_size_cap or depth >= max_depth:
        if raw_size > payload_size_cap:
            placeholder = f"[Transcription failed - {_format_ts(start_s)} - {_format_ts(end_s)} Reason: payload-too-large-after-splits]"
            results.append({"index": start_s, "text": placeholder, "start_s": start_s, "end_s": end_s})
            return results
        term = await transcribe_segment(model, path, int(start_s), start_s, end_s, max_retries=0, payload_size_cap=payload_size_cap)
        if term.get("split_required"):
            placeholder = f"[Transcription failed - {_format_ts(start_s)} - {_format_ts(end_s)} Reason: payload-error-persistent]"
            term = {"index": start_s, "text": placeholder, "start_s": start_s, "end_s": end_s}
        results.append(term)
        return results
    dur = _probe_duration(path) or (end_s - start_s) or 1.0
    half = dur / 2.0
    left_path = f"{path}_partL.mp3"; right_path = f"{path}_partR.mp3"
    _encode_slice(path, left_path, 0.0, half)
    _encode_slice(path, right_path, half, dur - half)
    left_res = await _recursive_split_and_transcribe(model, left_path, start_s, start_s + half, depth + 1, max_depth, payload_size_cap, max_retries)
    right_res = await _recursive_split_and_transcribe(model, right_path, start_s + half, end_s, depth + 1, max_depth, payload_size_cap, max_retries)
    return left_res + right_res


async def transcribe_file(model, mp3_full_path: str, work_dir: str, seg_seconds: int, max_concurrency: int, bypass_split: bool, splitter_fn, max_segment_retries: int, max_payload_size: int, max_split_depth: int):
    """Transcribe file; split segments recursively on payload errors.

    Parameters kept minimal to avoid broad refactors. Size-based splitting only triggered by payload error.
    """
    out_pattern = os.path.join(work_dir, "seg%03d.mp3")
    if bypass_split:
        segments = [f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)]
    else:
        splitter_fn(mp3_full_path, out_pattern, seg_seconds)
        segments = [f for f in os.listdir(work_dir) if re.match(r"seg\d{3}\.mp3", f)]
    segments.sort()
    if not segments:
        return "", []
    starts: List[float] = []
    ends: List[float] = []
    cursor = 0.0
    for fname in segments:
        p = os.path.join(work_dir, fname)
        d = _probe_duration(p) or float(seg_seconds)
        starts.append(cursor); cursor += d; ends.append(cursor)
    sem = asyncio.Semaphore(max_concurrency)
    async def _run(idx: int, fname: str):
        async with sem:
            seg_path = os.path.join(work_dir, fname)
            return await transcribe_segment(model, seg_path, idx, starts[idx], ends[idx], max_segment_retries, max_payload_size)
    base_results = await asyncio.gather(*[asyncio.create_task(_run(i, f)) for i, f in enumerate(segments)])
    expanded: List[Dict[str, Any]] = []
    for res in base_results:
        if res.get("split_required"):
            s = res["start_s"]; e = res["end_s"]
            print(f"Starting recursive split for segment index={res['index']} range {s:.2f}-{e:.2f}s")
            expanded.extend(await _recursive_split_and_transcribe(model, res["segment_path"], s, e, depth=0, max_depth=max_split_depth, payload_size_cap=max_payload_size, max_retries=max_segment_retries))
        else:
            expanded.append(res)
    expanded.sort(key=lambda r: r.get("start_s", 0))
    full_text = "\n\n".join(r.get("text", "") for r in expanded)
    return full_text, segments


def load_model(runpod_api_key: str | None, runpod_endpoint_id: str | None, languages_cfg: Dict[str, Any], language: str = "he"):
    if not runpod_api_key or not runpod_endpoint_id:
        raise RuntimeError("RUNPOD_API_KEY or RUNPOD_ENDPOINT_ID not set.")
    import ivrit
    lang_cfg = languages_cfg.get(language)
    if not lang_cfg:
        raise RuntimeError(f"Language '{language}' not found in config.")
    model_name = lang_cfg.get("model")
    if not model_name:
        raise RuntimeError(f"Model not configured for language '{language}'.")
    print(f"Loading model '{model_name}' for language '{language}' via RunPod endpoint {runpod_endpoint_id}...")
    return ivrit.load_model(engine='runpod', model=model_name, api_key=runpod_api_key, endpoint_id=runpod_endpoint_id, core_engine='stable-whisper')