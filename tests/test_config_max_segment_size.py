from transcriber.config import load_config

def test_max_segment_size_default(monkeypatch):
    monkeypatch.delenv("MAX_SEGMENT_SIZE", raising=False)
    cfg = load_config()
    assert cfg.max_segment_size == 8*1024*1024


def test_max_segment_size_override(monkeypatch):
    monkeypatch.setenv("MAX_SEGMENT_SIZE", str(5*1024*1024))
    cfg = load_config()
    assert cfg.max_segment_size == 5*1024*1024

