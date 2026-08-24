"""Executor: where a sandbox runs (agent_runner.md).

One sandbox per child: created at child start, every attempt is an
``exec`` inside it, terminated at child end, with a hard TTL equal to the
child's budget. The adaptor is the whole vocabulary a caller may use —
``create`` / ``find`` / ``attach`` / ``list`` on the executor, ``exec`` /
``poll`` / ``terminate`` on the sandbox — so no platform call ever
appears in a project.

Two backends: ``ModalExecutor`` (a Modal Sandbox on the published worker
image) and ``LocalExecutor`` (the same lifecycle as subprocesses on this
host — the bare-box backend and the test double, the same class). Both
hand the sandbox its workspace root through ``AGENT_RUNNER_WORKSPACE``
and expose it as ``Sandbox.workspace``, so the caller never guesses a
path.

``ExecutorGone`` is the one failure a caller must route: the sandbox no
longer exists (TTL, crash, terminate), so the attempt cannot continue in
it and only a new sandbox can — never a plain retry.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

from agent_runner.runtime import RunnerError
from agent_runner.workspace import WORKSPACE_ENV

SANDBOX_GONE = "sandbox_gone"
# Modal bills the greater of reserved and used, so the request stays tiny
# and only the limit (the money backstop) comes from the caller.
MEMORY_REQUEST_MB = 512


class ExecutorGone(RunnerError):
    """The sandbox is gone: it is not "ask again", it is "make another"."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=SANDBOX_GONE, retryable=False, alert=False)


@dataclass(frozen=True)
class SandboxSpec:
    """Everything a sandbox is created from. ``command`` is the entrypoint
    (the workspace keeper); the sandbox lives while it runs, at most
    ``ttl_seconds``. ``secrets`` reach the sandbox as environment like
    ``env`` does, but are never logged or tagged."""

    name: str
    command: tuple[str, ...]
    ttl_seconds: int
    env: dict[str, str] = field(default_factory=dict)
    secrets: dict[str, str] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    cpu: float | None = None
    memory_limit_mb: int | None = None   # a ceiling, not a reservation


class Proc(ABC):
    """One command running inside a sandbox."""

    @abstractmethod
    def lines(self) -> Iterator[str]:
        """Stdout line by line (newline stripped) until the process ends."""

    @abstractmethod
    def wait(self) -> int:
        """The exit code."""

    @abstractmethod
    def stderr(self) -> str:
        """Everything the process wrote to stderr, read after it ended."""


class Sandbox(ABC):
    id: str
    name: str
    workspace: str
    tags: dict[str, str]

    @abstractmethod
    def exec(
        self, *command: str, stdin: bytes | None = None, env: Mapping[str, str] | None = None
    ) -> Proc:
        """Run ``command`` in the sandbox; ``stdin`` is written whole and
        closed. Raises ``ExecutorGone`` when the sandbox no longer exists."""

    @abstractmethod
    def poll(self) -> int | None:
        """The entrypoint's exit code once the sandbox ended, else None."""

    @abstractmethod
    def terminate(self) -> None:
        """Destroy the sandbox and everything in it. Idempotent."""


class Executor(ABC):
    @abstractmethod
    def create(self, spec: SandboxSpec) -> Sandbox: ...

    @abstractmethod
    def find(self, name: str) -> Sandbox | None:
        """The running sandbox of that name, else None."""

    @abstractmethod
    def attach(self, sandbox_id: str) -> Sandbox:
        """A handle on a running sandbox by id; ``ExecutorGone`` otherwise."""

    @abstractmethod
    def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        """Every running sandbox carrying all of ``tags``."""


# ── Modal ────────────────────────────────────────────────────────────────


class ModalExecutor(Executor):
    """Sandboxes on Modal, on the image built from the project's own
    Dockerfile (one recipe, any backend). ``modal`` is imported here and
    nowhere else."""

    def __init__(
        self,
        app_name: str,
        dockerfile: Path,
        context_dir: Path,
        workspace: str = "/work",
    ) -> None:
        import modal

        self.modal = modal
        self.app_name = app_name
        self.dockerfile = Path(dockerfile)
        self.context_dir = Path(context_dir)
        self.workspace = workspace
        self._app: Any = None
        self._image: Any = None

    def app(self) -> Any:
        if self._app is None:
            self._app = self.modal.App.lookup(self.app_name, create_if_missing=True)
        return self._app

    def image(self) -> Any:
        if self._image is None:
            self._image = self.modal.Image.from_dockerfile(
                str(self.dockerfile), context_dir=str(self.context_dir)
            )
        return self._image

    def create(self, spec: SandboxSpec) -> Sandbox:
        # The name rides in the tags so attach/list can recover it.
        tags = {**spec.tags, "name": spec.name}
        sandbox = self.modal.Sandbox.create(
            *spec.command,
            app=self.app(),
            name=spec.name,
            image=self.image(),
            timeout=spec.ttl_seconds,
            env={**spec.env, WORKSPACE_ENV: self.workspace},
            secrets=[self.modal.Secret.from_dict(dict(spec.secrets))],
            tags=tags,
            cpu=spec.cpu,
            memory=(MEMORY_REQUEST_MB, spec.memory_limit_mb) if spec.memory_limit_mb else None,
        )
        return ModalSandbox(self, sandbox, spec.name, tags)

    def find(self, name: str) -> Sandbox | None:
        try:
            sandbox = self.modal.Sandbox.from_name(self.app_name, name)
        except self.modal.exception.NotFoundError:
            return None
        return ModalSandbox(self, sandbox, name, sandbox.get_tags())

    def attach(self, sandbox_id: str) -> Sandbox:
        try:
            sandbox = self.modal.Sandbox.from_id(sandbox_id)
        except self.modal.exception.NotFoundError as exc:
            raise ExecutorGone(f"sandbox {sandbox_id} no longer exists") from exc
        if sandbox.poll() is not None:
            raise ExecutorGone(f"sandbox {sandbox_id} has ended (rc={sandbox.poll()})")
        tags = sandbox.get_tags()
        return ModalSandbox(self, sandbox, tags.get("name", sandbox_id), tags)

    def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        found = []
        for sandbox in self.modal.Sandbox.list(app_id=self.app().app_id, tags=dict(tags)):
            all_tags = sandbox.get_tags()
            found.append(ModalSandbox(self, sandbox, all_tags.get("name", sandbox.object_id), all_tags))
        return found


class ModalSandbox(Sandbox):
    def __init__(self, executor: ModalExecutor, sandbox: Any, name: str, tags: dict[str, str]) -> None:
        self._modal = executor.modal
        self._sandbox = sandbox
        self.id = sandbox.object_id
        self.name = name
        self.workspace = executor.workspace
        self.tags = tags

    def exec(
        self, *command: str, stdin: bytes | None = None, env: Mapping[str, str] | None = None
    ) -> Proc:
        try:
            process = self._sandbox.exec(*command, env=dict(env or {}))
            if stdin is not None:
                process.stdin.write(stdin)
            process.stdin.write_eof()
            process.stdin.drain()
        except self._modal.exception.NotFoundError as exc:
            raise ExecutorGone(f"sandbox {self.id} is gone: {exc}") from exc
        return ModalProc(process)

    def poll(self) -> int | None:
        return self._sandbox.poll()

    def terminate(self) -> None:
        try:
            self._sandbox.terminate()
        except self._modal.exception.NotFoundError:
            pass


class ModalProc(Proc):
    def __init__(self, process: Any) -> None:
        self._process = process

    def lines(self) -> Iterator[str]:
        # The stream arrives in chunks, not lines; split here so a JSON
        # line is never handed over torn.
        buffer = ""
        for chunk in self._process.stdout:
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                yield line
        if buffer:
            yield buffer

    def wait(self) -> int:
        return self._process.wait()

    def stderr(self) -> str:
        try:
            return self._process.stderr.read()
        except Exception:
            return ""


# ── local ────────────────────────────────────────────────────────────────


class LocalExecutor(Executor):
    """The same lifecycle as subprocesses on this host: a sandbox is a
    directory under ``root`` plus its entrypoint process; ``exec`` runs
    in the same environment; the TTL is a timer that kills it. Sandboxes
    live in this process's registry, so a restarted worker finds none —
    which is exactly ``ExecutorGone``, and the caller makes another."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._sandboxes: dict[str, LocalSandbox] = {}

    def create(self, spec: SandboxSpec) -> Sandbox:
        home = self.root / spec.name
        home.mkdir(parents=True, exist_ok=True)
        workspace = home / "work"
        env = {**os.environ, **spec.env, **spec.secrets, WORKSPACE_ENV: str(workspace)}
        log = (home / "keeper.log").open("ab")
        process = subprocess.Popen(
            list(spec.command), cwd=str(home), env=env, stdout=log, stderr=subprocess.STDOUT
        )
        sandbox = LocalSandbox(spec.name, process, env, str(workspace), dict(spec.tags))
        threading.Timer(spec.ttl_seconds, sandbox.terminate).start()
        self._sandboxes[sandbox.id] = sandbox
        return sandbox

    def find(self, name: str) -> Sandbox | None:
        for sandbox in self._sandboxes.values():
            if sandbox.name == name and sandbox.poll() is None:
                return sandbox
        return None

    def attach(self, sandbox_id: str) -> Sandbox:
        sandbox = self._sandboxes.get(sandbox_id)
        if sandbox is None or sandbox.poll() is not None:
            raise ExecutorGone(f"sandbox {sandbox_id} is not running on this host")
        return sandbox

    def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        return [
            sandbox
            for sandbox in self._sandboxes.values()
            if sandbox.poll() is None and all(sandbox.tags.get(k) == v for k, v in tags.items())
        ]


class LocalSandbox(Sandbox):
    def __init__(
        self, name: str, process: subprocess.Popen, env: dict[str, str], workspace: str, tags: dict[str, str]
    ) -> None:
        self.id = f"local-{name}-{process.pid}"
        self.name = name
        self.workspace = workspace
        self.tags = tags
        self._process = process
        self._env = env
        self._procs: list[LocalProc] = []

    def exec(
        self, *command: str, stdin: bytes | None = None, env: Mapping[str, str] | None = None
    ) -> Proc:
        if self.poll() is not None:
            raise ExecutorGone(f"sandbox {self.id} has ended (rc={self.poll()})")
        stderr = tempfile.TemporaryFile()
        process = subprocess.Popen(
            list(command),
            env={**self._env, **(env or {})},
            cwd=str(Path(self.workspace).parent),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        # Feed stdin from a thread: a large request must never deadlock
        # against a child that starts writing before it finishes reading.
        threading.Thread(target=_feed, args=(process, stdin), daemon=True).start()
        proc = LocalProc(process, stderr)
        self._procs.append(proc)
        return proc

    def poll(self) -> int | None:
        return self._process.poll()

    def terminate(self) -> None:
        # The keeper first: a supervisor that sees its exec die must find
        # the sandbox already ended, never a live keeper for one more poll.
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait()
        for proc in self._procs:
            proc.kill()


def _feed(process: subprocess.Popen, stdin: bytes | None) -> None:
    try:
        if stdin:
            process.stdin.write(stdin)
    except (BrokenPipeError, OSError):
        pass
    finally:
        try:
            process.stdin.close()
        except OSError:
            pass


class LocalProc(Proc):
    def __init__(self, process: subprocess.Popen, stderr: Any) -> None:
        self._process = process
        self._stderr = stderr

    def lines(self) -> Iterator[str]:
        for raw in self._process.stdout:
            yield raw.decode("utf-8", errors="replace").rstrip("\n")

    def wait(self) -> int:
        return self._process.wait()

    def stderr(self) -> str:
        self._stderr.seek(0)
        return self._stderr.read().decode("utf-8", errors="replace")

    def kill(self) -> None:
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait()
