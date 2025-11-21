from transcriber.model import transcribe_file
import asyncio

class DummyModel:
    def transcribe_async(self, path, diarize=True):
        async def gen():
            raise Exception("boom")
            yield  # unreachable, ensures async generator type
        return gen()


def test_failed_segments_placeholder(tmp_path):
    seg0 = tmp_path / "seg000.mp3"
    seg1 = tmp_path / "seg001.mp3"
    seg0.write_bytes(b"")
    seg1.write_bytes(b"")

    dummy = DummyModel()
    full_text, segments = asyncio.run(transcribe_file(
        dummy,
        mp3_full_path=str(seg0),
        work_dir=str(tmp_path),
        seg_seconds=10,
        max_concurrency=2,
        bypass_split=True,
        splitter_fn=lambda *a, **k: None,
        max_segment_retries=0,
        max_payload_size=10_000_000,
        max_split_depth=3,
    ))
    assert len(segments) == 2
    lines = [l for l in full_text.split("\n") if l.strip()]
    assert any("[Transcription failed - 00:00:00 - 00:00:10" in l for l in lines)
    assert any("[Transcription failed - 00:00:10 - 00:00:20" in l for l in lines)
    assert "Reason: boom" in full_text
