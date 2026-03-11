"""Cloudflare R2 storage client for image overrides."""

import os
import boto3
from botocore.config import Config


def _get_client():
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY", ""),
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _bucket():
    return os.environ.get("R2_BUCKET_NAME", "")


def _public_url(key: str) -> str:
    base = os.environ.get("R2_PUBLIC_URL", "").rstrip("/")
    if not base:
        bucket = _bucket()
        account_id = os.environ.get("R2_ACCOUNT_ID", "")
        base = f"https://{bucket}.{account_id}.r2.dev"
    return f"{base}/{key}"


def upload(file_bytes: bytes, key: str, content_type: str = "image/jpeg") -> str:
    """Upload bytes to R2 and return the public URL."""
    client = _get_client()
    client.put_object(
        Bucket=_bucket(),
        Key=key,
        Body=file_bytes,
        ContentType=content_type,
    )
    return _public_url(key)


def delete(key: str):
    """Delete an object from R2."""
    client = _get_client()
    client.delete_object(Bucket=_bucket(), Key=key)


def set_cors(allowed_origins=None):
    """Set CORS rules on the bucket."""
    if allowed_origins is None:
        allowed_origins = ["*"]
    client = _get_client()
    client.put_bucket_cors(
        Bucket=_bucket(),
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedOrigins": allowed_origins,
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 86400,
                }
            ]
        },
    )
