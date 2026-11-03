from __future__ import annotations

import os

import boto3


def get_ssm_client():
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    kwargs: dict[str, str] = {}
    if region:
        kwargs["region_name"] = region
    return boto3.client("ssm", **kwargs)


def get_secretsmanager_client():
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    kwargs: dict[str, str] = {}
    if region:
        kwargs["region_name"] = region
    return boto3.client("secretsmanager", **kwargs)
