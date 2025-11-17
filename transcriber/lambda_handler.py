"""AWS Lambda entry point."""
from __future__ import annotations
import asyncio
from .runner import run


def lambda_handler(event, context):
    """Lambda handler wraps async run logic."""
    return asyncio.run(run())
