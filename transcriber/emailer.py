"""Email sending via configurable SMTP (Gmail by default)."""
import os
import ssl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from . import logger
from .config import Config


def send_transcription_email(
    gmail_app_password: str | None,
    gmail_sender_email: str | None,
    email_to: str | None,
    subject: str,
    body_text: str,
    attachment_path: str | None,
    *,
    config: Config | None = None,
):
    """Send the transcription email.

    SMTP server, port and SSL usage are taken from the provided Config when
    available, falling back to Gmail-compatible defaults inside Config. This
    keeps the function backward compatible for existing callers that do not
    pass a Config instance while allowing other SMTP providers via env vars.
    """
    if not gmail_app_password or not gmail_sender_email or not email_to:
        logger.info("Missing email configuration; skipping email.")
        return False

    message = MIMEMultipart()
    message["From"] = gmail_sender_email
    message["To"] = email_to
    message["Subject"] = subject
    message.attach(MIMEText(body_text, "plain", "utf-8"))

    if attachment_path and os.path.exists(attachment_path):
        part = MIMEBase('application', 'octet-stream')
        with open(attachment_path, 'rb') as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
        message.attach(part)

    # Decide SMTP connection parameters
    smtp_server = getattr(config, "smtp_server", "smtp.gmail.com")
    smtp_port = int(getattr(config, "smtp_port", 465))
    smtp_use_ssl = bool(getattr(config, "smtp_use_ssl", True))

    context = ssl.create_default_context()
    try:
        if smtp_use_ssl:
            # Direct SSL (typical for port 465 and Gmail-style endpoints)
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
                server.login(gmail_sender_email, gmail_app_password)
                server.sendmail(gmail_sender_email, email_to, message.as_string())
        else:
            # Plain connection upgraded with STARTTLS (typical for port 587)
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                try:
                    server.starttls(context=context)
                    server.ehlo()
                except smtplib.SMTPException:
                    # Some providers expect plain-text only; continue without STARTTLS.
                    pass
                server.login(gmail_sender_email, gmail_app_password)
                server.sendmail(gmail_sender_email, email_to, message.as_string())

        logger.info("Email sent successfully via SMTP %s:%s (SSL=%s)", smtp_server, smtp_port, smtp_use_ssl)
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP authentication failed. Check credentials and SMTP settings.")
        return False
    except Exception as e:
        logger.error("An error occurred while sending email via SMTP %s:%s (SSL=%s): %s", smtp_server, smtp_port, smtp_use_ssl, e)
        return False
