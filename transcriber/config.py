"""Configuration loading utilities.

Loads environment variables and optional JSON config file. Provides a single
Config dataclass for downstream code. All tunables are controllable via env
variables to keep local and Lambda execution identical.
"""
from __future__ import annotations
import os
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
import datetime as _dt

from .constants import (
    ENV_SERVICE_ACCOUNT_FILE,
    ENV_DRIVE_FOLDER_ID,
    ENV_EMAIL_TO,
    ENV_GMAIL_SENDER_EMAIL,
    ENV_GMAIL_APP_PASSWORD,
    # New generic SMTP settings
    ENV_SMTP_SERVER,
    ENV_SMTP_PORT,
    ENV_SMTP_USE_SSL,
    ENV_RUNPOD_API_KEY,
    ENV_RUNPOD_ENDPOINT_ID,
    ENV_CONFIG_PATH,
    ENV_MAX_SEGMENT_CONCURRENCY,
    ENV_SEG_SECONDS,
    ENV_SKIP_DRIVE,
    ENV_BYPASS_SPLIT,
    ENV_TIME_WINDOW_ENABLED,
    ENV_SCHEDULE_START_HOUR,
    ENV_SCHEDULE_END_HOUR,
    ENV_SCHEDULE_DAYS,
    ENV_SCHEDULE_TIMEZONE,
    ENV_MAX_SEGMENT_RETRIES,
    ENV_BALANCE_ALERT_VALUE,
    ENV_MAX_PAYLOAD_SIZE,
    ENV_MAX_SPLIT_DEPTH,
    ENV_MAX_SEGMENT_SIZE,
    DEFAULT_CONFIG_PATH_REL,
    ENV_ADD_RANDOM_PERSONAL_MESSAGE,
    # Defaults for SMTP when env vars are not set
    DEFAULT_SMTP_SERVER,
    DEFAULT_SMTP_PORT,
    DEFAULT_SMTP_USE_SSL,
)

from dotenv import load_dotenv
load_dotenv()

# Default config path: env override or repo root config.json
DEFAULT_CONFIG_PATH = os.environ.get(
    ENV_CONFIG_PATH,
    os.path.join(os.path.dirname(__file__), "..", DEFAULT_CONFIG_PATH_REL),
)


@dataclass
class Config:
    """Strongly-typed configuration values consumed by the pipeline."""
    service_account_file: Optional[str]
    drive_folder_id: Optional[str]
    email_to: Optional[str]
    gmail_sender_email: Optional[str]
    gmail_app_password: Optional[str]
    # Generic SMTP settings (server/port/SSL) to avoid hard-coding Gmail
    smtp_server: str
    smtp_port: int
    smtp_use_ssl: bool
    runpod_api_key: Optional[str]
    runpod_endpoint_id: Optional[str]
    config_path: str
    max_segment_concurrency: int
    seg_seconds: int
    skip_drive: bool
    bypass_split: bool
    time_window_enabled: bool
    schedule_start_hour: int
    schedule_end_hour: int
    schedule_days: str  # e.g. SUN-SAT
    timezone: str       # IANA timezone name
    add_random_personal_message: bool  # new flag controlling fun header in email
    languages: Dict[str, Dict[str, Any]]
    max_segment_retries: int  # how many retries after first attempt (total attempts = 1 + retries)
    balance_alert_value: float  # threshold below which we mark LOW BALANCE in subject
    max_payload_size: int       # raw segment byte ceiling triggering split (default 9MB)
    max_split_depth: int        # recursion depth when splitting oversized segments
    max_segment_size: int       # initial segmentation size cap in bytes (default 8MB)

    @property
    def within_schedule_window(self) -> bool:
        """Return True if current local time is within configured schedule window.

        - If time-window enforcement is disabled, always returns True.
        - Day range specified via labels like SUN-SAT.
        - Uses zoneinfo if available; falls back to UTC.
        """
        if not self.time_window_enabled:
            return True
        now: _dt.datetime
        try:
            from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # Python 3.9+
            tz_key = self.timezone
            if tz_key.upper() in {"UTC", "ETC/UTC", "GMT", "Z"}:
                tz_key = "UTC"
            try:
                tz = ZoneInfo(tz_key)
                now = _dt.datetime.now(tz)
            except ZoneInfoNotFoundError:
                # Fallback: use timezone-aware UTC now (Python 3.12+)
                now = _dt.datetime.now(_dt.UTC)
        except Exception:
            now = _dt.datetime.now(_dt.UTC)
        label_for = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
        idx_for = {v: k for k, v in label_for.items()}
        parts = self.schedule_days.split('-')
        if len(parts) == 1:
            start_label = end_label = parts[0]
        else:
            start_label, end_label = parts[0], parts[-1]
        if start_label not in idx_for or end_label not in idx_for:
            return True
        start_idx = idx_for[start_label]; end_idx = idx_for[end_label]
        if start_idx <= end_idx:
            allowed_days = set(range(start_idx, end_idx + 1))
        else:
            allowed_days = set(list(range(start_idx, 7)) + list(range(0, end_idx + 1)))
        if now.weekday() not in allowed_days:
            return False
        if not (self.schedule_start_hour <= now.hour <= self.schedule_end_hour):
            return False
        return True


def _parse_bool_env(key: str, default_true: bool = False) -> bool:
    """Parse an environment variable into boolean with common falsy synonyms.

    Accepts: 0, false, no, off -> False, anything else -> True when present.
    When absent, returns default_true.
    """
    val = os.environ.get(key)
    if val is None:
        return default_true
    val_norm = val.strip().lower()
    if val_norm in {"0", "false", "no", "off"}:
        return False
    return True


def load_config(path: Optional[str] = None) -> Config:
    """Load configuration by merging environment variables and config.json.

    The JSON file is used for language mapping and similar structured config,
    while scalar toggles come from env vars for easy overrides.
    """
    cfg_path = path or DEFAULT_CONFIG_PATH
    languages: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                file_cfg = json.load(f)
            languages = file_cfg.get('languages', {})
        except Exception:
            languages = {}
    # Parse SMTP settings with sensible defaults pointing at Gmail but overridable
    smtp_server = os.environ.get(ENV_SMTP_SERVER, DEFAULT_SMTP_SERVER)
    try:
        smtp_port = int(os.environ.get(ENV_SMTP_PORT, str(DEFAULT_SMTP_PORT)))
    except ValueError:
        smtp_port = DEFAULT_SMTP_PORT
    # If SMTP_USE_SSL is not set, default to DEFAULT_SMTP_USE_SSL
    smtp_use_ssl_env = os.environ.get(ENV_SMTP_USE_SSL)
    if smtp_use_ssl_env is None:
        smtp_use_ssl = DEFAULT_SMTP_USE_SSL
    else:
        smtp_use_ssl = smtp_use_ssl_env.strip().lower() not in {"0", "false", "no", "off"}

    return Config(
        service_account_file=os.environ.get(ENV_SERVICE_ACCOUNT_FILE),
        drive_folder_id=os.environ.get(ENV_DRIVE_FOLDER_ID),
        email_to=os.environ.get(ENV_EMAIL_TO),
        gmail_sender_email=os.environ.get(ENV_GMAIL_SENDER_EMAIL),
        gmail_app_password=os.environ.get(ENV_GMAIL_APP_PASSWORD),
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        smtp_use_ssl=smtp_use_ssl,
        runpod_api_key=os.environ.get(ENV_RUNPOD_API_KEY),
        runpod_endpoint_id=os.environ.get(ENV_RUNPOD_ENDPOINT_ID),
        config_path=cfg_path,
        max_segment_concurrency=int(os.environ.get(ENV_MAX_SEGMENT_CONCURRENCY, "2")),
        seg_seconds=int(os.environ.get(ENV_SEG_SECONDS, str(8 * 60))),
        skip_drive=os.environ.get(ENV_SKIP_DRIVE) == "1",
        bypass_split=os.environ.get(ENV_BYPASS_SPLIT) == "1",
        time_window_enabled=os.environ.get(ENV_TIME_WINDOW_ENABLED, "1") == "1",
        schedule_start_hour=int(os.environ.get(ENV_SCHEDULE_START_HOUR, "8")),
        schedule_end_hour=int(os.environ.get(ENV_SCHEDULE_END_HOUR, "22")),
        schedule_days=os.environ.get(ENV_SCHEDULE_DAYS, "SUN-SAT"),
        timezone=os.environ.get(ENV_SCHEDULE_TIMEZONE, "UTC"),
        add_random_personal_message=_parse_bool_env(ENV_ADD_RANDOM_PERSONAL_MESSAGE, default_true=True),
        languages=languages,
        max_segment_retries=int(os.environ.get(ENV_MAX_SEGMENT_RETRIES, "2")),
        balance_alert_value=float(os.environ.get(ENV_BALANCE_ALERT_VALUE, "2")),
        max_payload_size=int(os.environ.get(ENV_MAX_PAYLOAD_SIZE, str(9 * 1024 * 1024))),
        max_split_depth=int(os.environ.get(ENV_MAX_SPLIT_DEPTH, "3")),
        max_segment_size=int(os.environ.get(ENV_MAX_SEGMENT_SIZE, str(8 * 1024 * 1024))),
    )