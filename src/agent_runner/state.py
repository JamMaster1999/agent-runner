"""The S3 side of the state mirror: one bucket/prefix, whole-object puts
and gets, and the credential denylist every transfer passes through.

What travels is ``agent_runner.workspace``'s business — a sandbox's
working tree, pushed on a cadence and pulled back when a fresh sandbox
resumes a child. This module is only the transport and its two rules:

- Credential files NEVER travel, in either direction (``is_denied``): every
  sandbox seeds its own login from the environment (``agent_runner.auth``),
  and a credential on a shared bucket is a credential in a place nobody
  audits. The denylist guards uploads AND downloads.
- The mirror is opt-in: ``AGENT_RUNNER_STATE_S3=s3://bucket/prefix``.
  Unset, ``mirror()`` returns None and no S3 client is ever built. Set but
  unusable (malformed URL, boto3 absent) raises — call it at startup so a
  process refuses to boot on a mirror it cannot write.

Credentials and region come from the standard AWS environment variables;
boto3 is an optional extra (``pip install 'agent-runner[s3]'``) imported
only when the variable is set.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

STATE_S3_ENV = "AGENT_RUNNER_STATE_S3"

# The exclusion, spelled out: the first two are the CLI login files every
# sandbox seeds for itself, then the usual credential-file names.
DENIED_NAMES = frozenset({"auth.json", ".credentials.json", ".netrc", ".env"})

# ...plus anything whose name says credential. Session transcripts are
# hex-uuid and rollout-<timestamp> filenames, which cannot contain these.
DENIED_MARKERS = (
    "credential",
    "auth",
    "token",
    "secret",
    "password",
    "cookie",
    "apikey",
    "api_key",
    ".pem",
    ".key",
)


def is_denied(name: str) -> bool:
    """True for a file name that could hold a credential. Applied to every
    upload and every download: the mirror carries working state, never
    logins."""
    lowered = name.lower()
    return lowered in DENIED_NAMES or any(m in lowered for m in DENIED_MARKERS)


def key_segment(value: str) -> str:
    """One key segment from opaque text (a session ref, a path component).
    Anything outside the safe set collapses to '_', so a ref can never
    steer the key out of its own prefix."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", value) or "_"


def parse_s3_url(url: str) -> tuple[str, str]:
    """``s3://bucket/prefix`` -> ``("bucket", "prefix")``. Raises on
    anything else: a misconfigured destination is a boot-time error, never
    a silently disabled mirror."""
    parsed = urlparse(url)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(
            f"{STATE_S3_ENV}={url!r} is not an S3 URL; expected s3://bucket/prefix."
        )
    return parsed.netloc, parsed.path.strip("/")


def warn(message: str) -> None:
    print(f"WARNING: state mirror: {message}", file=sys.stderr)


def s3_client() -> Any:
    """A fresh S3 client on its own boto3 Session — boto3's default session
    is not thread-safe. Timeouts are short and retries few on purpose: a
    transfer that gives up quickly is doing its job; the next checkpoint
    tries again."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError(
            f"{STATE_S3_ENV} is set but boto3 is not installed. Install the "
            "optional extra: pip install 'agent-runner[s3]'."
        ) from exc
    return boto3.session.Session().client(
        "s3",
        config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}),
    )


class StateMirror:
    """One bucket/prefix. Every transfer is a whole-object PUT or GET of
    one file: no read-modify-write, so two writers can at worst
    last-writer-win on one object."""

    def __init__(self, bucket: str, prefix: str, client: Any) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.client = client

    def key(self, *parts: str) -> str:
        return "/".join(part for part in (self.prefix, *parts) if part)

    def put_file(self, key: str, path: Path) -> None:
        self.client.upload_file(Filename=str(path), Bucket=self.bucket, Key=key)

    def get_file(self, key: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(Bucket=self.bucket, Key=key, Filename=str(path))

    def put_json(self, key: str, value: Any) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(value).encode(),
            ContentType="application/json",
        )

    def get_json(self, key: str) -> Any | None:
        """The object parsed, or None when the key does not exist."""
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None
            raise
        return json.loads(body)


def mirror(client: Any | None = None) -> StateMirror | None:
    """The configured mirror, or None when ``AGENT_RUNNER_STATE_S3`` is
    unset. Raises when the variable is set but unusable."""
    url = os.environ.get(STATE_S3_ENV)
    if not url:
        return None
    bucket, prefix = parse_s3_url(url)
    return StateMirror(bucket, prefix, client if client is not None else s3_client())
