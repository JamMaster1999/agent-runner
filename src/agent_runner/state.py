"""State mirror: worker-local files mirrored through S3 so any worker can
pick up any run.

This module is the transport and its policy — where objects go, what is
allowed to travel, what a failure costs. What travels is its callers':
``agent_runner.sessions`` mirrors CLI session transcripts,
``agent_runner.workdirs`` mirrors checkpoint folders.

Local disk stays the working layer. The CLIs read and write their own
files exactly as before, and nothing here sits on the hot path: the mirror
is written AFTER the fact (a session transcript when the attempt that wrote
it ends, a checkpoint folder when the attempt that stamped it ends) and read
only when this host is missing something a resume needs. Absent in both
places is a fresh run — the behavior a single-volume worker already had.

The mirror is opt-in: ``AGENT_RUNNER_STATE_S3=s3://bucket/prefix``. Unset,
every entry point returns immediately and no S3 client is ever built, so
the mirror-less behavior is byte for byte the one this package shipped
before the module existed. Credentials and region come from the standard
AWS environment variables the workers already carry; boto3 is an optional
extra (``pip install 'agent-runner[s3]'``) imported only when the variable
is set.

Credential files NEVER travel, in either direction (``is_denied``): every
worker seeds its own login from the environment (``agent_runner.auth``),
and a credential on a shared bucket is a credential in a place nobody
audits. The denylist guards uploads AND downloads, so a bucket cannot
write a login file into a CLI home either.

Failure posture: the mirror is best-effort by construction. A failed upload
warns — the local copy is still the truth — and a failed download leaves
the file absent, which every caller already reads as "run fresh".
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse

STATE_S3_ENV = "AGENT_RUNNER_STATE_S3"

# The exclusion, spelled out: the first two are the CLI login files every
# worker seeds for itself, then the usual credential-file names.
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
    upload and every download: the mirror carries session and checkpoint
    state, never logins."""
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


def _warn(message: str) -> None:
    print(f"WARNING: state mirror: {message}", file=sys.stderr)


def s3_client() -> Any:
    """A fresh S3 client on its own boto3 Session — boto3's default session
    is not thread-safe, and attempts finish concurrently.

    Timeouts are short and retries few on purpose: these calls sit in an
    attempt's teardown, where a stalled transfer costs heartbeats. A mirror
    that gives up quickly is doing its job."""
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
        config=Config(
            connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}
        ),
    )


class StateMirror:
    """One bucket/prefix, one file per key.

    Every transfer is a whole-object PUT or GET of a single file: no
    listing-then-merging, no read-modify-write, so two attempts finishing at
    the same moment cannot corrupt each other's state — the worst case is
    last writer wins on one file, which is what a shared volume does too.
    """

    def __init__(self, bucket: str, prefix: str, client: Any) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.client = client

    def key(self, group: str, relative: str = "") -> str:
        return "/".join(part for part in (self.prefix, group, relative) if part)

    def push(self, group: str, root: Path, files: Iterable[Path]) -> None:
        """Upload each file under ``group`` at its path relative to
        ``root``, so a restore lands it exactly where the CLI looks for it.
        A file that will not upload warns and is skipped."""
        for path in files:
            if is_denied(path.name):
                # Never silent: session and checkpoint files cannot hit the
                # denylist, so a name that does is worth an operator's eyes.
                _warn(f"{path.name} reads as a credential; not uploaded")
                continue
            try:
                relative = path.relative_to(root).as_posix()
            except ValueError:
                _warn(f"{path} is outside {root}; not mirrored")
                continue
            try:
                self.client.upload_file(
                    Filename=str(path), Bucket=self.bucket, Key=self.key(group, relative)
                )
            except Exception as exc:
                _warn(f"upload of {path} failed: {exc}")

    def pull(self, group: str, root: Path) -> None:
        """Download everything under ``group`` into ``root``, skipping files
        this host already has at least as fresh. A file that will not
        download warns and stays absent, which every caller reads as
        "run fresh"."""
        root = Path(root)
        base = self.key(group)
        try:
            listing = list(self._list(base + "/"))
        except Exception as exc:
            _warn(f"listing {base} failed: {exc}")
            return
        for key, modified in listing:
            relative = key[len(base) + 1 :]
            if not relative:
                continue  # a folder-marker object names no file
            target = root / relative
            if is_denied(target.name):
                _warn(f"{target.name} reads as a credential; not downloaded")
                continue
            if not self._inside(root, target):
                continue
            if not _is_stale(target, modified):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.client.download_file(
                    Bucket=self.bucket, Key=key, Filename=str(target)
                )
            except Exception as exc:
                _warn(f"download of {key} failed: {exc}")

    def _list(self, prefix: str) -> Iterator[tuple[str, Any]]:
        token: str | None = None
        while True:
            request = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                request["ContinuationToken"] = token
            response = self.client.list_objects_v2(**request)
            for item in response.get("Contents") or ():
                yield item["Key"], item.get("LastModified")
            token = response.get("NextContinuationToken")
            # No token means there is no next page to ask for, whatever the
            # truncation flag says: asking again would replay this one.
            if not response.get("IsTruncated") or not token:
                return

    @staticmethod
    def _inside(root: Path, target: Path) -> bool:
        """A key is remote input: one that walks out of ``root`` is refused
        rather than trusted."""
        try:
            target.resolve().relative_to(root.resolve())
        except ValueError:
            _warn(f"refusing {target}: outside {root}")
            return False
        return True


def _is_stale(path: Path, modified: Any) -> bool:
    """True when the mirror's copy should replace this host's: the file is
    missing, or the object was written after the local one. Local writes
    always win over their own echo, so a resumed session is never
    overwritten by the copy it just uploaded."""
    try:
        local = path.stat().st_mtime
    except OSError:
        return True
    remote = getattr(modified, "timestamp", None)
    return remote is not None and remote() > local


def mirror(client: Any | None = None) -> StateMirror | None:
    """The configured mirror, or None when ``AGENT_RUNNER_STATE_S3`` is
    unset. Raises when the variable is set but unusable (malformed URL,
    boto3 absent) — call it at startup so a worker refuses to boot on a
    mirror it cannot write."""
    url = os.environ.get(STATE_S3_ENV)
    if not url:
        return None
    bucket, prefix = parse_s3_url(url)
    return StateMirror(bucket, prefix, client if client is not None else s3_client())


def active_mirror() -> StateMirror | None:
    """The mirror for a running attempt: same as ``mirror``, except a
    configuration error warns and disables instead of raising. Startup
    already refused to boot on one, and no mirror problem may cost a run."""
    try:
        return mirror()
    except Exception as exc:
        _warn(str(exc))
        return None
