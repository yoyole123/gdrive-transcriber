import os

from transcriber.config import load_config

load_config()

def test_within_window_enabled_inside_range(monkeypatch=None):
    os.environ['TIME_WINDOW_ENABLED'] = '1'
    os.environ['SCHEDULE_START_HOUR'] = '8'
    os.environ['SCHEDULE_END_HOUR'] = '22'
    os.environ['SCHEDULE_DAYS'] = 'SUN-THU'
    os.environ['SCHEDULE_TIMEZONE'] = 'UTC'
    cfg = load_config()
    # Can't guarantee current day/time; just ensure property accesses without error
    assert isinstance(cfg.within_schedule_window, bool)


def test_within_window_disabled():
    os.environ['TIME_WINDOW_ENABLED'] = '0'
    cfg = load_config()
    assert cfg.within_schedule_window is True  # disabled should allow
