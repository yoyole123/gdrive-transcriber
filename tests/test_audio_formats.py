import os
import importlib
import types
import tempfile
from transcriber import drive as drive_mod
from transcriber.audio import convert_to_mp3


def _fake_service(file_entries):
    class _Files:
        def list(self, q=None, fields=None):
            class _Exec:
                def execute(self_non):
                    return {"files": file_entries}
            return _Exec()
    class _Svc:
        def files(self):
            return _Files()
    return _Svc()


def test_list_audio_files_multi_extension(monkeypatch):
    monkeypatch.setenv("AUDIO_EXTENSIONS", ".m4a,.wav,.mp3")
    import transcriber.drive as d1
    importlib.reload(d1)  # reload to apply new env var
    files = [
        {"id": "1", "name": "song.m4a"},
        {"id": "2", "name": "talk.WAV"},
        {"id": "3", "name": "music.mp3"},
        {"id": "4", "name": "doc.txt"},
    ]
    svc = _fake_service(files)
    listed = d1.list_audio_files(svc, "folder", False)
    names = {f['name'] for f in listed}
    assert "song.m4a" in names
    assert "talk.WAV" in names  # case-insensitive
    assert "music.mp3" in names
    assert "doc.txt" not in names


def test_convert_to_mp3_skips_for_mp3(tmp_path):
    # create a dummy mp3 file (content irrelevant)
    src = tmp_path / "input.mp3"
    dst = tmp_path / "output.mp3"
    src.write_bytes(b"dummy data")
    convert_to_mp3(str(src), str(dst))
    assert dst.read_bytes() == b"dummy data"

