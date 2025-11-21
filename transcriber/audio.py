"""Audio conversion and splitting helpers using ffmpeg."""
import os
import subprocess
import shutil

FFMPEG_BIN = os.environ.get("FFMPEG_PATH", "ffmpeg")

# New generic conversion

def convert_to_mp3(input_path: str, output_path: str):
    """Convert any supported audio file to MP3 (libmp3lame). If already MP3, copy or reuse."""
    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".mp3":
        # If output different path, just copy
        if input_path != output_path:
            shutil.copyfile(input_path, output_path)
        return
    subprocess.check_call([
        FFMPEG_BIN, "-y", "-i", input_path, "-vn", "-acodec", "libmp3lame", "-q:a", "2", output_path
    ])

# Backwards compatibility wrapper

def convert_m4a_to_mp3(m4a_path, mp3_path):
    convert_to_mp3(m4a_path, mp3_path)


def split_mp3(mp3_path, out_pattern, seg_seconds: int):
    subprocess.check_call([
        FFMPEG_BIN, "-y", "-i", mp3_path,
        "-f", "segment", "-segment_time", str(seg_seconds),
        "-c", "copy", out_pattern
    ])
