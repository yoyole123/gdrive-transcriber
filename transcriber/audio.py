"""Audio conversion and splitting helpers using ffmpeg."""
import os
import subprocess

FFMPEG_BIN = os.environ.get("FFMPEG_PATH", "ffmpeg")

def convert_m4a_to_mp3(m4a_path, mp3_path):
    subprocess.check_call([FFMPEG_BIN, "-y", "-i", m4a_path, "-vn", "-acodec", "libmp3lame", "-q:a", "2", mp3_path])


def split_mp3(mp3_path, out_pattern, seg_seconds: int):
    subprocess.check_call([
        FFMPEG_BIN, "-y", "-i", mp3_path,
        "-f", "segment", "-segment_time", str(seg_seconds),
        "-c", "copy", out_pattern
    ])
