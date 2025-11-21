import os
import subprocess
import builtins
from transcriber.audio import split_mp3_by_size
import transcriber.audio as audio_mod

class DummyCompleted(Exception):
    pass

def test_size_split_single_segment(tmp_path, monkeypatch):
    # Create small mp3 (dummy bytes, treat as already below cap)
    mp3 = tmp_path / 'file.mp3'
    mp3.write_bytes(b'0' * 1000)  # 1KB
    out_pattern = str(tmp_path / 'seg%03d.mp3')
    copied = {}
    def fake_copy(src, dst):
        copied['src'] = src; copied['dst'] = dst
        builtins.open(dst, 'wb').close()
    monkeypatch.setattr(audio_mod.shutil, 'copyfile', fake_copy)
    split_mp3_by_size(str(mp3), out_pattern, max_segment_size=8000, fallback_seg_seconds=600)
    assert copied['dst'].endswith('seg000.mp3')
    assert os.path.exists(copied['dst'])


def test_size_split_duration_calculation(monkeypatch, tmp_path):
    # Large file triggers splitting
    mp3 = tmp_path / 'big.mp3'
    mp3.write_bytes(b'0' * (9 * 1024 * 1024))  # 9MB
    out_pattern = str(tmp_path / 'seg%03d.mp3')
    # Force bitrate 128kbps (128000 bits/sec)
    monkeypatch.setattr(audio_mod, '_get_bitrate_bits', lambda p: 128000)
    captured = {}
    def fake_call(args, **kwargs):
        # Find segment_time value
        for i,a in enumerate(args):
            if a == '-segment_time':
                captured['segment_time'] = int(args[i+1])
        return 0
    monkeypatch.setattr(audio_mod.subprocess, 'check_call', fake_call)
    split_mp3_by_size(str(mp3), out_pattern, max_segment_size=8*1024*1024, fallback_seg_seconds=600)
    # Expected duration_target = floor((8MB * 0.9)/ (128000/8))
    expected = int((8*1024*1024 * 0.9) / (128000/8))
    assert captured['segment_time'] == expected
    assert 30 <= captured['segment_time'] <= 600

