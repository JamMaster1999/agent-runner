"""Workspace: a sandbox's working tree, kept in S3 (agent_runner.md).

The local filesystem is the working store everywhere: the CLI homes
(sessions, transcripts), the checkpoint folders, and the attempt workdirs
all live under one root inside the sandbox, and the CLIs read and write
them exactly as they would on a laptop. S3 is the backup, reached through
an object API only — never a mount (the spikes: a mounted bucket commits
at ``close()`` and cannot host sqlite).

Three verbs, one rule each:

- ``prepare`` — a local copy present means use it; absent, pull the last
  complete push from S3; nothing anywhere is a fresh workspace.
- ``checkpoint`` — push every file that changed since the last push, then
  ``manifest.json`` last. The manifest names the files of a complete push,
  so a pull never restores a half-pushed tree.
- ``release`` — the final checkpoint.

``keeper`` is the sandbox's entrypoint: prepare, announce readiness, then
checkpoint every ``every`` seconds until the release marker appears, and
once more on the way out. What a terminate can destroy is bounded by that
cadence, and stated rather than pretended away.

Attempt workdirs stay local: CLI stream captures and Chrome profiles are
scratch, and the transcript in the CLI home is the record. Credentials
never travel (``state.is_denied``).
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Iterator

from agent_runner import state

MANIFEST = "manifest.json"
RUNNER_DIR = ".runner"                 # markers and pid files: never pushed
ATTEMPTS_DIR = "attempts"              # attempt workdirs: never pushed
READY_MARKER = "ready"
RELEASE_MARKER = "release"
DEFAULT_CHECKPOINT_SECONDS = 60.0

WORKSPACE_ENV = "AGENT_RUNNER_WORKSPACE"
GROUP_ENV = "AGENT_RUNNER_WORKSPACE_GROUP"


def workspace_root() -> Path:
    """The root the executor handed this sandbox (``AGENT_RUNNER_WORKSPACE``)."""
    root = os.environ.get(WORKSPACE_ENV)
    if not root:
        raise RuntimeError(f"{WORKSPACE_ENV} is not set — the executor sets it at create")
    return Path(root)


def attempts_root(root: Path) -> Path:
    return Path(root) / ATTEMPTS_DIR


def marker(root: Path, name: str) -> Path:
    return Path(root) / RUNNER_DIR / name


class Workspace:
    """One root, one key group in the mirror. With no mirror configured
    ``prepare`` reports fresh and the push verbs do nothing."""

    def __init__(self, root: Path, group: str, mirror: state.StateMirror | None) -> None:
        self.root = Path(root)
        self.group = group
        self.mirror = mirror
        self._pushed: dict[str, tuple[int, int]] = {}

    def key(self, relative: str) -> str:
        assert self.mirror is not None
        return self.mirror.key(self.group, relative)

    def files(self) -> dict[str, tuple[int, int]]:
        """Every file that travels: ``relative -> (size, mtime_ns)``. A file
        that vanishes mid-walk (the CLI homes churn) is simply not there."""
        found: dict[str, tuple[int, int]] = {}
        for path in _walk(self.root):
            try:
                stat = path.stat()
            except OSError:
                continue
            found[path.relative_to(self.root).as_posix()] = (stat.st_size, stat.st_mtime_ns)
        return found

    def prepare(self) -> str:
        """``local`` (a copy is already here), ``pulled`` (restored from the
        last complete push), or ``fresh``."""
        self.root.mkdir(parents=True, exist_ok=True)
        if self.files():
            return "local"
        if self.mirror is None:
            return "fresh"
        manifest = self.mirror.get_json(self.key(MANIFEST))
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if not files:
            return "fresh"
        for relative in files:
            target = self.root / relative
            if state.is_denied(target.name) or not _inside(self.root, target):
                continue
            try:
                self.mirror.get_file(self.key(relative), target)
            except Exception as exc:
                state.warn(f"download of {relative} failed: {exc}")
        return "pulled"

    def checkpoint(self) -> int:
        """Push what changed, then the manifest. Returns the files pushed.
        A file that will not upload warns and is retried next time; the
        manifest still names only what is on disk."""
        if self.mirror is None:
            return 0
        current = self.files()
        pushed = 0
        for relative, signature in current.items():
            if self._pushed.get(relative) == signature:
                continue
            try:
                self.mirror.put_file(self.key(relative), self.root / relative)
            except Exception as exc:
                state.warn(f"upload of {relative} failed: {exc}")
                continue
            self._pushed[relative] = signature
            pushed += 1
        self._pushed = {rel: sig for rel, sig in self._pushed.items() if rel in current}
        try:
            self.mirror.put_json(self.key(MANIFEST), {"files": sorted(self._pushed)})
        except Exception as exc:
            state.warn(f"manifest upload failed: {exc}")
        return pushed

    def release(self) -> int:
        return self.checkpoint()


def _inside(root: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(root.resolve())
    except ValueError:
        state.warn(f"refusing {target}: outside {root}")
        return False
    return True


def _walk(root: Path) -> Iterator[Path]:
    """Regular files under ``root``, skipping the runner's own markers, the
    attempt workdirs, symlinks, and anything named like a credential."""
    if not root.is_dir():
        return
    frontier = [root]
    while frontier:
        folder = frontier.pop()
        try:
            entries = sorted(os.scandir(folder), key=lambda e: e.name)
        except OSError:
            continue
        for entry in entries:
            if folder == root and entry.name in (RUNNER_DIR, ATTEMPTS_DIR):
                continue
            if entry.is_symlink():
                continue
            if entry.is_dir(follow_symlinks=False):
                frontier.append(Path(entry.path))
            elif entry.is_file(follow_symlinks=False) and not state.is_denied(entry.name):
                yield Path(entry.path)


def keeper(root: Path, group: str, every: float, stop: threading.Event) -> int:
    """The sandbox entrypoint's loop: prepare, say ``ready``, checkpoint on
    the cadence until the release marker (or ``stop``), then a final push."""
    workspace = Workspace(root, group, state.mirror())
    verdict = workspace.prepare()
    marker(root, READY_MARKER).parent.mkdir(parents=True, exist_ok=True)
    marker(root, READY_MARKER).write_text(verdict)
    print(f"ready {verdict}", flush=True)
    release = marker(root, RELEASE_MARKER)
    while not stop.is_set() and not release.exists():
        deadline = time.monotonic() + every
        while time.monotonic() < deadline and not stop.is_set() and not release.exists():
            time.sleep(1)
        try:
            pushed = workspace.checkpoint()
        except Exception as exc:  # the keeper outlives any one push
            state.warn(f"checkpoint failed: {exc}")
            continue
        if pushed:
            print(f"checkpoint {pushed}", flush=True)
    try:
        pushed = workspace.release()
    except Exception as exc:
        state.warn(f"release failed: {exc}")
        return 1
    print(f"released {pushed}", flush=True)
    return 0


def keeper_main(root: Path | None, every: float) -> int:
    """``agent-runner keeper``: the process behind ``keeper``. The root
    defaults to what the executor handed this sandbox; SIGTERM releases."""
    group = os.environ.get(GROUP_ENV)
    if not group:
        print(f"{GROUP_ENV} is not set", file=sys.stderr)
        return 2
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    return keeper(root or workspace_root(), group, every, stop)
