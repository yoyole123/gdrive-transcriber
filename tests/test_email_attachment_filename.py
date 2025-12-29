import os
from email import message_from_string

from transcriber.emailer import send_transcription_email
from transcriber.config import Config


class _FakeSMTP:
    """Minimal SMTP/SMTP_SSL stub that captures the outbound message."""

    last_message: str | None = None

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def ehlo(self):
        return None

    def starttls(self, context=None):
        return None

    def login(self, user, password):
        return None

    def sendmail(self, from_addr, to_addrs, msg):
        _FakeSMTP.last_message = msg
        return {}


def _minimal_config() -> Config:
    # Build a Config instance; only SMTP fields are actually used here.
    return Config(
        service_account_file=None,
        drive_folder_id=None,
        email_to="to@example.com",
        gmail_sender_email="from@example.com",
        gmail_app_password="pw",
        smtp_server="smtp.example.com",
        smtp_port=465,
        smtp_use_ssl=True,
        runpod_api_key=None,
        runpod_endpoint_id=None,
        config_path="config.json",
        max_segment_concurrency=1,
        seg_seconds=60,
        skip_drive=True,
        bypass_split=True,
        time_window_enabled=False,
        schedule_start_hour=0,
        schedule_end_hour=23,
        schedule_days="SUN-SAT",
        timezone="UTC",
        add_random_personal_message=False,
        languages={},
        max_segment_retries=0,
        balance_alert_value=0.0,
        max_payload_size=1024,
        max_split_depth=0,
        max_segment_size=1024,
        transcription_language="en",
    )


def test_email_attachment_has_filename_and_txt_extension(tmp_path, monkeypatch):
    monkeypatch.setattr("smtplib.SMTP_SSL", _FakeSMTP)

    # Create an attachment file without a .txt extension to ensure we still attach as .txt
    p = tmp_path / "my_transcription"
    p.write_text("hello", encoding="utf-8")

    ok = send_transcription_email(
        gmail_app_password="pw",
        gmail_sender_email="from@example.com",
        email_to="to@example.com",
        subject="subj",
        body_text="body",
        attachment_path=str(p),
        config=_minimal_config(),
    )
    assert ok is True

    assert _FakeSMTP.last_message is not None
    msg = message_from_string(_FakeSMTP.last_message)

    # Find the attachment part
    attachments = [
        part
        for part in msg.walk()
        if part.get_content_disposition() == "attachment"
    ]
    assert len(attachments) == 1

    filename = attachments[0].get_filename()
    assert filename is not None
    assert filename.endswith(".txt")


def test_email_attachment_keeps_existing_extension(tmp_path, monkeypatch):
    monkeypatch.setattr("smtplib.SMTP_SSL", _FakeSMTP)

    p = tmp_path / "lecture_transcription.txt"
    p.write_text("hello", encoding="utf-8")

    ok = send_transcription_email(
        gmail_app_password="pw",
        gmail_sender_email="from@example.com",
        email_to="to@example.com",
        subject="subj",
        body_text="body",
        attachment_path=str(p),
        config=_minimal_config(),
    )
    assert ok is True

    msg = message_from_string(_FakeSMTP.last_message)
    attachments = [
        part
        for part in msg.walk()
        if part.get_content_disposition() == "attachment"
    ]
    assert len(attachments) == 1

    assert attachments[0].get_filename() == "lecture_transcription.txt"

