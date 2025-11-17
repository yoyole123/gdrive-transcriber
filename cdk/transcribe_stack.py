import os
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
)

class TranscribeStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Use asset bundling to include dependencies from requirements.txt and only required sources
        bundling = {
            "image": _lambda.Runtime.PYTHON_3_11.bundling_image,
            "command": [
                "bash", "-c",
                "pip install -r requirements.txt -t /asset-output "
                "&& cp -r transcriber /asset-output/transcriber "
                "&& if [ -f main.py ]; then cp main.py /asset-output/; fi "
                "&& if [ -f config.json ]; then cp config.json /asset-output/; fi "
                "&& if [ -f sa.json ]; then cp sa.json /asset-output/; fi"
            ],
        }

        env_vars = {
                "SERVICE_ACCOUNT_FILE": os.getenv("SERVICE_ACCOUNT_FILE", "sa.json"),
                "DRIVE_FOLDER_ID": os.getenv("DRIVE_FOLDER_ID", "CHANGE_ME"),
                "EMAIL_TO": os.getenv("EMAIL_TO", "CHANGE_ME"),
                "GMAIL_SENDER_EMAIL": os.getenv("GMAIL_SENDER_EMAIL", "CHANGE_ME"),
                "GMAIL_APP_PASSWORD": os.getenv("GMAIL_APP_PASSWORD", "CHANGE_ME"),
                "RUNPOD_API_KEY": os.getenv("RUNPOD_API_KEY", "CHANGE_ME"),
                "RUNPOD_ENDPOINT_ID": os.getenv("RUNPOD_ENDPOINT_ID", "CHANGE_ME"),
                "CONFIG_PATH": os.getenv("CONFIG_PATH", "config.json"),
                "MAX_SEGMENT_CONCURRENCY": os.getenv("MAX_SEGMENT_CONCURRENCY", "2"),
                "SEG_SECONDS": os.getenv("SEG_SECONDS", str(10*60)),
                "TIME_WINDOW_ENABLED": os.getenv("TIME_WINDOW_ENABLED", "1"),
                "SCHEDULE_START_HOUR": os.getenv("SCHEDULE_START_HOUR", "8"),
                "SCHEDULE_END_HOUR": os.getenv("SCHEDULE_END_HOUR", "22"),
                "SCHEDULE_DAYS": os.getenv("SCHEDULE_DAYS", "SUN-THU"),
                "SCHEDULE_TIMEZONE": os.getenv("SCHEDULE_TIMEZONE", "UTC"),
                # Operational toggles
                "SKIP_DRIVE": os.getenv("SKIP_DRIVE", "0"),
                "BYPASS_SPLIT": os.getenv("BYPASS_SPLIT", "0"),
        }

        lambda_fn = _lambda.Function(
            self,
            "DriveTranscriberLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="transcriber.lambda_handler.lambda_handler",
            code=_lambda.Code.from_asset("..", bundling=bundling),  # bundle root project
            timeout=Duration.minutes(15),
            memory_size=1024,
            environment=env_vars,
        )

        # Optional: attach a prebuilt ffmpeg layer (set FFMPEG_LAYER_ARN) and set runtime path
        ffmpeg_layer_arn = os.getenv("FFMPEG_LAYER_ARN")
        if ffmpeg_layer_arn:
            layer = _lambda.LayerVersion.from_layer_version_arn(self, "FfmpegLayer", layer_version_arn=ffmpeg_layer_arn)
            lambda_fn.add_layers(layer)
            lambda_fn.add_environment("FFMPEG_PATH", os.getenv("FFMPEG_PATH", "/opt/bin/ffmpeg"))

        # EventBridge Rule: every 2 hours 08-22 UTC Sunday-Thursday
        schedule_expression = events.Schedule.cron(
            minute="0",
            hour="8,10,12,14,16,18,20,22",
            week_day="SUN-THU",
        )

        events.Rule(
            self,
            "TranscriptionScheduleRule",
            schedule=schedule_expression,
            targets=[targets.LambdaFunction(lambda_fn)],
            enabled=True,
            description="Invoke transcription Lambda every 2 hours (08-22) Sun-Thu",
        )
