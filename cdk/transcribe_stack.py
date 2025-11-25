import os
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
)

DEFAULT_FFMPEG_LAYER_ARN = ""  # "arn:aws:lambda:eu-west-1:182544233882:layer:ffmpeg:1"  <- Created manually

# The stack can either (1) attach an existing ffmpeg layer via FFMPEG_LAYER_ARN env var or default ARN
# or (2) build a new layer with a static ffmpeg binary during bundling.

class TranscribeStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        python_runtime = _lambda.Runtime.PYTHON_3_11

        external_ffmpeg_layer_arn = os.getenv("FFMPEG_LAYER_ARN") or DEFAULT_FFMPEG_LAYER_ARN
        ffmpeg_layer: _lambda.ILayerVersion
        if external_ffmpeg_layer_arn:
            ffmpeg_layer = _lambda.LayerVersion.from_layer_version_arn(
                self,
                "ExternalFfmpegLayer",
                layer_version_arn=external_ffmpeg_layer_arn,
            )
        else:
            ffmpeg_download_cmd = (
                "mkdir -p /asset-output/bin "
                "&& curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz "
                "-o /tmp/ffmpeg.tar.xz "
                "&& tar -xJf /tmp/ffmpeg.tar.xz -C /tmp "
                "&& cp /tmp/ffmpeg-*-amd64-static/ffmpeg /asset-output/bin/ffmpeg "
                "&& chmod +x /asset-output/bin/ffmpeg"
            )
            ffmpeg_layer_bundling = {
                "image": python_runtime.bundling_image,
                "user": "root",
                "command": ["bash", "-c", ffmpeg_download_cmd],
            }
            ffmpeg_layer = _lambda.LayerVersion(
                self,
                "FfmpegLayer",
                code=_lambda.Code.from_asset(project_root, bundling=ffmpeg_layer_bundling),
                compatible_runtimes=[python_runtime],
                description="Static ffmpeg binary layer (downloaded during synth)",
            )

        app_bundling_cmd = (
            "pip install -r requirements.txt -t /asset-output "
            "&& cp -r transcriber /asset-output/transcriber "
            "&& if [ -f main.py ]; then cp main.py /asset-output/; fi "
            "&& if [ -f config.json ]; then cp config.json /asset-output/; fi "
            "&& if [ -f sa.json ]; then cp sa.json /asset-output/; fi"
        )
        app_bundling = {
            "image": python_runtime.bundling_image,
            "user": "root",
            "command": ["bash", "-c", app_bundling_cmd],
        }

        env_vars = {
            "SERVICE_ACCOUNT_FILE": os.getenv("SERVICE_ACCOUNT_FILE", "sa.json"),
            "DRIVE_FOLDER_ID": os.getenv("DRIVE_FOLDER_ID", "CHANGE_ME"),
            "EMAIL_TO": os.getenv("EMAIL_TO", "CHANGE_ME"),
            "GMAIL_SENDER_EMAIL": os.getenv("GMAIL_SENDER_EMAIL", "CHANGE_ME"),
            "GMAIL_APP_PASSWORD": os.getenv("GMAIL_APP_PASSWORD", "CHANGE_ME"),
            # New SMTP configuration (Gmail defaults, overridable for other providers)
            "SMTP_SERVER": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            "SMTP_PORT": os.getenv("SMTP_PORT", "465"),
            "SMTP_USE_SSL": os.getenv("SMTP_USE_SSL", "1"),
            "RUNPOD_API_KEY": os.getenv("RUNPOD_API_KEY", "CHANGE_ME"),
            "RUNPOD_ENDPOINT_ID": os.getenv("RUNPOD_ENDPOINT_ID", "CHANGE_ME"),
            "CONFIG_PATH": os.getenv("CONFIG_PATH", "config.json"),
            "MAX_SEGMENT_CONCURRENCY": os.getenv("MAX_SEGMENT_CONCURRENCY", "4"),
            "SEG_SECONDS": os.getenv("SEG_SECONDS", str(7 * 60)),
            "MAX_SEGMENT_SIZE": os.getenv("MAX_SEGMENT_SIZE", str(8 * 1024 * 1024)),
            "TIME_WINDOW_ENABLED": os.getenv("TIME_WINDOW_ENABLED", "1"),
            "SCHEDULE_START_HOUR": os.getenv("SCHEDULE_START_HOUR", "8"),
            "SCHEDULE_END_HOUR": os.getenv("SCHEDULE_END_HOUR", "22"),
            "SCHEDULE_DAYS": os.getenv("SCHEDULE_DAYS", "SUN-SAT"),  # full week
            "SCHEDULE_TIMEZONE": os.getenv("SCHEDULE_TIMEZONE", "UTC"),
            "SKIP_DRIVE": os.getenv("SKIP_DRIVE", "0"),
            "BYPASS_SPLIT": os.getenv("BYPASS_SPLIT", "0"),
            "FFMPEG_PATH": os.getenv("FFMPEG_PATH", "/opt/bin/ffmpeg"),
            "MAX_SEGMENT_RETRIES": os.getenv("MAX_SEGMENT_RETRIES", "2"),
            "BALANCE_ALERT_VALUE": os.getenv("BALANCE_ALERT_VALUE", "2"),
            "ADD_RANDOM_PERSONAL_MESSAGE": os.getenv("ADD_RANDOM_PERSONAL_MESSAGE", "1"),
            # New recursive payload splitting controls
            "MAX_PAYLOAD_SIZE": os.getenv("MAX_PAYLOAD_SIZE", str(9 * 1024 * 1024)),  # 9MB default
            "MAX_SPLIT_DEPTH": os.getenv("MAX_SPLIT_DEPTH", "3"),
        }

        lambda_fn = _lambda.Function(
            self,
            "DriveTranscriberLambda",
            runtime=python_runtime,
            handler="transcriber.lambda_handler.lambda_handler",
            code=_lambda.Code.from_asset(project_root, bundling=app_bundling),
            timeout=Duration.minutes(15),
            memory_size=2048,
            environment=env_vars,
            layers=[ffmpeg_layer],
        )

        schedule_expression = events.Schedule.cron(
            minute="0",
            hour="8,10,12,14,16,18,20,22",
            week_day="*",  # every day
        )

        events.Rule(
            self,
            "TranscriptionScheduleRule",
            schedule=schedule_expression,
            targets=[targets.LambdaFunction(lambda_fn)],
            enabled=True,
            description="Invoke transcription Lambda every 2 hours (08-22) daily",
        )
