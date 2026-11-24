"""Tiny S3 helpers so the same code writes to a local folder (tests) or to S3 (Glue)."""
from __future__ import annotations

import json
import os
from urllib.parse import urlparse


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def split(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def _client():
    import boto3  # available in Glue and SageMaker; imported lazily for local tests
    return boto3.client("s3")


def upload_dir(local_dir: str, s3_uri: str) -> int:
    """Upload every file under local_dir to s3_uri, keeping relative paths. Returns file count."""
    bucket, prefix = split(s3_uri)
    s3, n = _client(), 0
    for root, _, files in os.walk(local_dir):
        for f in files:
            full = os.path.join(root, f)
            key = f"{prefix.rstrip('/')}/{os.path.relpath(full, local_dir).replace(os.sep, '/')}"
            s3.upload_file(full, bucket, key)
            n += 1
    return n


def read_json(path: str) -> dict:
    if is_s3(path):
        bucket, key = split(path)
        try:
            return json.loads(_client().get_object(Bucket=bucket, Key=key)["Body"].read())
        except Exception:  # NoSuchKey on first run
            return {}
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def write_json(path: str, data: dict) -> None:
    body = json.dumps(data, indent=2, default=str)
    if is_s3(path):
        bucket, key = split(path)
        _client().put_object(Bucket=bucket, Key=key, Body=body.encode(), ContentType="application/json")
    else:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            f.write(body)
