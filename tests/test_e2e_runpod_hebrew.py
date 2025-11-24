import os
import asyncio
import shutil
import pytest


from transcriber.audio import convert_to_mp3, split_mp3_by_size
from transcriber.model import load_model, transcribe_file
from dotenv import load_dotenv

load_dotenv()

@pytest.mark.e2e
def test_e2e_runpod_hebrew(tmp_path):
    """Full end-to-end transcription against the RunPod endpoint using the static Hebrew sample.

    Starts AFTER the Drive download step (we copy the asset into a fresh work directory),
    performs real conversion + size-based segmentation, loads the real model via RunPod, and
    invokes transcription. This test is OPT-IN: it only runs if RUNPOD_E2E=1 and credentials
    are present; otherwise it is skipped (avoids network/API cost during normal CI runs).

    Environment vars required:
      RUNPOD_E2E=1                 -> enable the test
      RUNPOD_API_KEY               -> RunPod API key
      RUNPOD_ENDPOINT_ID           -> RunPod endpoint id
    Optional:
      RUNPOD_MODEL                 -> override model name (default stable-whisper-large)
      MAX_SEGMENT_SIZE             -> initial size-based splitting cap (default 8MB)
      SEG_SECONDS                  -> fallback seconds for bitrate estimation (default 600)
      RUNPOD_E2E_REQUIRE_NONEMPTY  -> if '1', assert non-empty transcription text
    """
    if os.environ.get("RUNPOD_E2E") != "1":
        pytest.skip("RUNPOD_E2E!=1; skipping real RunPod e2e test")
    api_key = os.environ.get("RUNPOD_API_KEY")
    endpoint = os.environ.get("RUNPOD_ENDPOINT_ID")
    if not api_key or not endpoint:
        pytest.skip("Missing RunPod credentials; skipping e2e")

    repo_root = os.path.dirname(os.path.dirname(__file__))
    asset_path = os.path.join(repo_root, "tests", "test_assets", "hebrew_sample.m4a")
    if not os.path.exists(asset_path):
        pytest.skip("hebrew_sample.m4a asset missing")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    original_local = work_dir / os.path.basename(asset_path)
    shutil.copyfile(asset_path, original_local)

    # Convert to MP3 (real ffmpeg). If ffmpeg missing, skip.
    ffmpeg_bin = os.environ.get("FFMPEG_PATH", "ffmpeg")
    if shutil.which(ffmpeg_bin) is None:
        pytest.skip("ffmpeg not available in PATH; skipping e2e")
    mp3_path = work_dir / (original_local.stem + ".mp3")
    convert_to_mp3(str(original_local), str(mp3_path))

    # Perform size-based initial splitting
    max_segment_size = int(os.environ.get("MAX_SEGMENT_SIZE", str(8*1024*1024)))
    seg_seconds = int(os.environ.get("SEG_SECONDS", "600"))
    out_pattern = os.path.join(str(work_dir), "seg%03d.mp3")
    split_mp3_by_size(str(mp3_path), out_pattern, max_segment_size, seg_seconds)

    # Load model
    model_name = os.environ.get("RUNPOD_MODEL", "stable-whisper-large")
    languages_cfg = {"he": {"model": model_name}}
    model = load_model(api_key, endpoint, languages_cfg, language="he")

    # Transcribe with concurrency=1 (sample is very short) and allow splitting logic if needed.
    full_text, segments = asyncio.run(transcribe_file(
        model,
        mp3_full_path=str(mp3_path),
        work_dir=str(work_dir),
        seg_seconds=seg_seconds,
        max_concurrency=1,
        bypass_split=False,
        splitter_fn=lambda src, pattern, seg_secs: split_mp3_by_size(src, pattern, max_segment_size, seg_secs),
        max_segment_retries=1,
        max_payload_size=int(os.environ.get("MAX_PAYLOAD_SIZE", str(9*1024*1024))),
        max_split_depth=int(os.environ.get("MAX_SPLIT_DEPTH", "3")),
    ))

    # Basic validations
    assert isinstance(segments, list) and len(segments) >= 1, "No segments produced"
    assert isinstance(full_text, str), "Transcription result not a string"

    if os.environ.get("RUNPOD_E2E_REQUIRE_NONEMPTY") == "1":
        assert full_text.strip(), "Empty transcription text (strict mode)"

    # Persist transcription for inspection
    transcription_file = work_dir / "e2e_transcription.txt"
    with open(transcription_file, "w", encoding="utf-8") as f:
        f.write(full_text)

    # Emit a short diagnostic line
    print(f"E2E transcription segments={len(segments)} chars={len(full_text.strip())} saved={transcription_file}")

