#!/usr/bin/env python3
import os
import aws_cdk as cdk
from transcribe_stack import TranscribeStack

# Load .env from repo root if present
try:
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path))
except Exception:
    pass

app = cdk.App()
TranscribeStack(
    app,
    "TranscribeServiceStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "us-east-1"),
    ),
)
app.synth()
