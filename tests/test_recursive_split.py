import os
import asyncio
from transcriber.model import transcribe_file
import transcriber.model as model_mod

class DummyPayloadModel:
    """Simulates payload error for original segment and success for split parts."""
    def transcribe_async(self, path, diarize=True):
        async def gen():
            base = os.path.basename(path)
            if 'seg000' in base and 'part' not in base:
                # Trigger payload error pattern to force split
                raise Exception("Payload length is 50000, exceeding max payload length of 10000")
            # Success for smaller parts
            yield type('Seg', (), {'text': f"{base}-fragmentA"})()
            yield type('Seg', (), {'text': f"{base}-fragmentB"})()
        return gen()


def test_recursive_split_success(tmp_path, monkeypatch):
    # Large initial segment exceeding cap
    seg = tmp_path / 'seg000.mp3'
    seg.write_bytes(b'0' * 50000)  # 50KB

    # Monkeypatch _encode_slice to avoid invoking real ffmpeg; create half-size files
    def fake_encode(src, dst, start, dur):
        # Determine size of source then write half into dst
        sz = os.path.getsize(src)
        new_size = max(1, sz // 2)
        with open(dst, 'wb') as f:
            f.write(b'0' * new_size)
    monkeypatch.setattr(model_mod, '_encode_slice', fake_encode)

    dummy = DummyPayloadModel()
    full_text, segments = asyncio.run(transcribe_file(
        dummy,
        mp3_full_path=str(seg),
        work_dir=str(tmp_path),
        seg_seconds=30,
        max_concurrency=1,
        bypass_split=True,
        splitter_fn=lambda *a, **k: None,
        max_segment_retries=0,
        max_payload_size=10_000,  # 10KB cap
        max_split_depth=4,
    ))
    # Should have original segment detected then split recursively until size below cap
    assert 'payload-error-persistent' not in full_text
    # Expect texts from partL and partR final segments
    assert 'seg000.mp3_partL.mp3' in full_text
    assert 'seg000.mp3_partR.mp3' in full_text
    # Ensure left appears before right (chronological ordering)
    assert full_text.index('seg000.mp3_partL.mp3') < full_text.index('seg000.mp3_partR.mp3')
    # Ensure no placeholder for oversized-after-splits
    assert 'payload-too-large-after-splits' not in full_text


class DummyAlwaysPayloadModel:
    """Always raises payload error to force persistent error placeholder when size below cap."""
    def transcribe_async(self, path, diarize=True):
        async def gen():
            raise Exception("Payload length is 50000, exceeding max payload length of 10000")
            yield  # unreachable to satisfy async generator protocol
        return gen()


def test_recursive_split_depth_limit_placeholder(tmp_path, monkeypatch):
    seg = tmp_path / 'seg000.mp3'
    seg.write_bytes(b'0' * 50000)

    def fake_encode(src, dst, start, dur):
        # Keep size large so that even after split parts remain above cap
        with open(dst, 'wb') as f:
            f.write(b'0' * 50000)
    monkeypatch.setattr(model_mod, '_encode_slice', fake_encode)

    dummy = DummyAlwaysPayloadModel()
    full_text, _ = asyncio.run(transcribe_file(
        dummy,
        mp3_full_path=str(seg),
        work_dir=str(tmp_path),
        seg_seconds=30,
        max_concurrency=1,
        bypass_split=True,
        splitter_fn=lambda *a, **k: None,
        max_segment_retries=0,
        max_payload_size=10_000,
        max_split_depth=1,  # only one level of splitting allowed
    ))
    # After one split depth reached; parts still oversized -> placeholders
    assert 'payload-too-large-after-splits' in full_text
