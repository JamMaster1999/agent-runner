"""Executor: where a sandbox runs (agent_runner.md).

One sandbox per child: created at child start, every attempt is an
``exec`` inside it, terminated at child end, with a hard TTL equal to the
child's budget. The adaptor is the whole vocabulary a caller may use —
``create`` / ``find`` / ``attach`` / ``list`` on the executor, ``exec`` /
``poll`` / ``terminate`` on the sandbox — so no platform call ever
appears in a project.

The contract is async: every call awaits the platform, and a process's
output is an async iterator of lines. A supervisor on an event loop
therefore keeps heartbeating through the slowest create or exec, and
reads a stream without a thread or a queue between it and the process.

Two backends: ``ModalExecutor`` (a Modal Sandbox on the published worker
image, on Modal's own async API) and ``LocalExecutor`` (the same lifecycle
as asyncio subprocesses on this host — the bare-box backend and the test
double, the same class). Both hand the sandbox its workspace root through
``AGENT_RUNNER_WORKSPACE`` and expose it as ``Sandbox.workspace``, so the
caller never guesses a path.

``ExecutorGone`` is the one failure a caller must route: the sandbox no
longer exists (TTL, crash, terminate), so the attempt cannot continue in
it and only a new sandbox can — never a plain retry.
"""

from __future__ import annotations

import asyncio
import os
import signal
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

from agent_runner.runtime import RunnerError
from agent_runner.workspace import READY_MARKER, RELEASE_MARKER, WORKSPACE_ENV, marker

SANDBOX_GONE = "sandbox_gone"
# Modal bills the greater of reserved and used, so the request stays tiny
# and only the limit (the money backstop) comes from the caller.
MEMORY_REQUEST_MB = 512
# The longest line the local backend reads from a process: a report line
# carries the validated payload whole.
LINE_LIMIT = 64 * 1024 * 1024


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
    def lines(self) -> AsyncIterator[str]:
        """Stdout line by line (newline stripped) until the process ends."""

    @abstractmethod
    async def wait(self) -> int:
        """The exit code."""

    @abstractmethod
    async def stderr(self) -> str:
        """Everything the process wrote to stderr, read after it ended."""


class Sandbox(ABC):
    id: str
    name: str
    workspace: str
    tags: dict[str, str]

    @abstractmethod
    async def exec(
        self,
        *command: str,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: int | None = None,
    ) -> Proc:
        """Run ``command`` in the sandbox; ``stdin`` is written whole and
        closed; ``timeout`` (seconds) ends it. Raises ``ExecutorGone`` when
        the sandbox no longer exists — and so does any call on the
        returned ``Proc`` once the sandbox has ended under it."""

    @abstractmethod
    async def poll(self) -> int | None:
        """The entrypoint's exit code once the sandbox ended, else None."""

    @abstractmethod
    async def terminate(self) -> None:
        """Destroy the sandbox and everything in it. Idempotent."""

    async def ensure_alive(self) -> None:
        rc = await self.poll()
        if rc is not None:
            raise ExecutorGone(f"sandbox {self.id} has ended (rc={rc})")


class Executor(ABC):
    @abstractmethod
    async def create(self, spec: SandboxSpec) -> Sandbox: ...

    @abstractmethod
    async def find(self, name: str) -> Sandbox | None:
        """The running sandbox of that name, else None."""

    @abstractmethod
    async def attach(self, sandbox_id: str) -> Sandbox:
        """A handle on a running sandbox by id; ``ExecutorGone`` otherwise."""

    @abstractmethod
    async def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        """Every running sandbox carrying all of ``tags``."""


# ── Modal ────────────────────────────────────────────────────────────────


class ModalExecutor(Executor):
    """Sandboxes on Modal, on the image built from the project's own
    Dockerfile (one recipe, any backend). ``modal`` is imported here and
    nowhere else; every call is the SDK's own ``.aio`` form."""

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

    async def app(self) -> Any:
        if self._app is None:
            self._app = await self.modal.App.lookup.aio(self.app_name, create_if_missing=True)
        return self._app

    def image(self) -> Any:
        if self._image is None:
            self._image = self.modal.Image.from_dockerfile(
                str(self.dockerfile), context_dir=str(self.context_dir)
            )
        return self._image

    async def create(self, spec: SandboxSpec) -> Sandbox:
        # The name rides in the tags so attach/list can recover it.
        tags = {**spec.tags, "name": spec.name}
        sandbox = await self.modal.Sandbox.create.aio(
            *spec.command,
            app=await self.app(),
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

    async def find(self, name: str) -> Sandbox | None:
        try:
            sandbox = await self.modal.Sandbox.from_name.aio(self.app_name, name)
        except self.modal.exception.NotFoundError:
            return None
        if await sandbox.poll.aio() is not None:
            return None
        return ModalSandbox(self, sandbox, name, await sandbox.get_tags.aio())

    async def attach(self, sandbox_id: str) -> Sandbox:
        try:
            sandbox = await self.modal.Sandbox.from_id.aio(sandbox_id)
        except self.modal.exception.NotFoundError as exc:
            raise ExecutorGone(f"sandbox {sandbox_id} no longer exists") from exc
        tags = await sandbox.get_tags.aio()
        handle = ModalSandbox(self, sandbox, tags.get("name", sandbox_id), tags)
        await handle.ensure_alive()
        return handle

    async def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        found = []
        app = await self.app()
        async for sandbox in self.modal.Sandbox.list.aio(app_id=app.app_id, tags=dict(tags)):
            if await sandbox.poll.aio() is not None:
                continue
            all_tags = await sandbox.get_tags.aio()
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

    async def exec(
        self,
        *command: str,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: int | None = None,
    ) -> Proc:
        await self.ensure_alive()
        async with self.gone_guard():
            # Line-buffered text: the SDK hands over whole lines, so a JSON
            # line is never torn.
            process = await self._sandbox.exec.aio(
                *command, env=dict(env or {}), timeout=timeout, text=True, bufsize=1
            )
            if stdin is not None:
                process.stdin.write(stdin)
            process.stdin.write_eof()
            await process.stdin.drain.aio()
        return ModalProc(self, process)

    @asynccontextmanager
    async def gone_guard(self) -> AsyncIterator[None]:
        """Modal answers a dead sandbox with NotFoundError, ConflictError
        ("already finished"), or a dropped connection, depending on which
        call notices first. Any of them on an ended sandbox is one thing."""
        try:
            yield
        except self._modal.exception.Error as exc:
            if await self.poll() is not None:
                raise ExecutorGone(f"sandbox {self.id} is gone: {exc}") from exc
            raise

    async def poll(self) -> int | None:
        return await self._sandbox.poll.aio()

    async def terminate(self) -> None:
        try:
            await self._sandbox.terminate.aio()
        except self._modal.exception.NotFoundError:
            pass


class ModalProc(Proc):
    def __init__(self, sandbox: ModalSandbox, process: Any) -> None:
        self._sandbox = sandbox
        self._process = process

    async def lines(self) -> AsyncIterator[str]:
        async with self._sandbox.gone_guard():
            async for line in self._process.stdout:
                yield line.rstrip("\n")

    async def wait(self) -> int:
        async with self._sandbox.gone_guard():
            return await self._process.wait.aio()

    async def stderr(self) -> str:
        try:
            return await self._process.stderr.read.aio()
        except Exception:
            return ""


# ── local ────────────────────────────────────────────────────────────────


class LocalExecutor(Executor):
    """The same lifecycle as subprocesses on this host: a sandbox is a
    directory under ``root`` plus its entrypoint process, running in this
    host's environment plus the spec's (a bare box IS the host); the TTL
    is a timer that kills it. Sandboxes live in this process's registry,
    so a restarted worker finds none — which is exactly ``ExecutorGone``,
    and the caller makes another."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._sandboxes: dict[str, LocalSandbox] = {}

    async def create(self, spec: SandboxSpec) -> Sandbox:
        home = self.root / spec.name
        home.mkdir(parents=True, exist_ok=True)
        workspace = home / "work"
        # The name's workspace survives between sandboxes (that is the local
        # resume story); the last keeper's markers must not, or the opener
        # reads a stale ready and the new keeper a stale release.
        for name in (READY_MARKER, RELEASE_MARKER):
            marker(workspace, name).unlink(missing_ok=True)
        env = {**os.environ, **spec.env, **spec.secrets, WORKSPACE_ENV: str(workspace)}
        with (home / "keeper.log").open("ab") as log:
            process = await asyncio.create_subprocess_exec(
                *spec.command, cwd=str(home), env=env, stdout=log, stderr=asyncio.subprocess.STDOUT
            )
        sandbox = LocalSandbox(spec.name, home, process, env, str(workspace), dict(spec.tags))
        sandbox.ttl = asyncio.get_running_loop().call_later(spec.ttl_seconds, sandbox.expire)
        self._sandboxes[sandbox.id] = sandbox
        return sandbox

    async def find(self, name: str) -> Sandbox | None:
        for sandbox in self._sandboxes.values():
            if sandbox.name == name and await sandbox.poll() is None:
                return sandbox
        return None

    async def attach(self, sandbox_id: str) -> Sandbox:
        sandbox = self._sandboxes.get(sandbox_id)
        if sandbox is None:
            raise ExecutorGone(f"sandbox {sandbox_id} is not running on this host")
        await sandbox.ensure_alive()
        return sandbox

    async def list(self, tags: Mapping[str, str]) -> list[Sandbox]:
        return [
            sandbox
            for sandbox in self._sandboxes.values()
            if await sandbox.poll() is None and all(sandbox.tags.get(k) == v for k, v in tags.items())
        ]


class LocalSandbox(Sandbox):
    def __init__(
        self,
        name: str,
        home: Path,
        process: asyncio.subprocess.Process,
        env: dict[str, str],
        workspace: str,
        tags: dict[str, str],
    ) -> None:
        self.id = f"local-{name}-{process.pid}"
        self.name = name
        self.workspace = workspace
        self.tags = tags
        self.ttl: asyncio.TimerHandle | None = None
        self._expiry: asyncio.Task[None] | None = None
        self._home = home
        self._process = process
        self._env = env
        self._procs: list[LocalProc] = []

    async def exec(
        self,
        *command: str,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: int | None = None,
    ) -> Proc:
        await self.ensure_alive()
        self._procs = [proc for proc in self._procs if proc.running()]
        process = await asyncio.create_subprocess_exec(
            *command,
            env={**self._env, **(env or {})},
            cwd=str(self._home),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=LINE_LIMIT,
        )
        proc = LocalProc(process, stdin, timeout)
        self._procs.append(proc)
        return proc

    async def poll(self) -> int | None:
        return self._process.returncode

    def expire(self) -> None:
        # Held: a task nothing references may be dropped before it runs.
        self._expiry = asyncio.get_running_loop().create_task(self.terminate())

    async def terminate(self) -> None:
        if self.ttl is not None:
            self.ttl.cancel()
        # The keeper first: a supervisor that sees its exec die must find
        # the sandbox already ended, never a live keeper for one more poll.
        if self._process.returncode is None:
            _kill(self._process)
            await self._process.wait()
        for proc in self._procs:
            proc.kill()


def _kill(process: asyncio.subprocess.Process) -> None:
    # Straight to the kernel: Process.kill() polls first and would reap a
    # child the loop's watcher is about to reap itself.
    with suppress(ProcessLookupError):
        os.kill(process.pid, signal.SIGKILL)


async def _feed(process: asyncio.subprocess.Process, stdin: bytes | None) -> None:
    try:
        if stdin:
            process.stdin.write(stdin)
            await process.stdin.drain()
    except OSError:
        pass
    finally:
        process.stdin.close()


class LocalProc(Proc):
    def __init__(self, process: asyncio.subprocess.Process, stdin: bytes | None, timeout: int | None) -> None:
        self._process = process
        # stdin and stderr each get a task of their own: a large request
        # must never deadlock against a child that writes before it reads,
        # and a chatty stderr must never block the child on a full pipe.
        # Both are held here: a task nothing references may be dropped.
        self._feeder = asyncio.create_task(_feed(process, stdin))
        self._stderr = asyncio.create_task(process.stderr.read())
        self._deadline = (
            asyncio.get_running_loop().call_later(timeout, self.kill) if timeout is not None else None
        )

    async def lines(self) -> AsyncIterator[str]:
        async for raw in self._process.stdout:
            yield raw.decode("utf-8", errors="replace").rstrip("\n")

    async def wait(self) -> int:
        rc = await self._process.wait()
        if self._deadline is not None:
            self._deadline.cancel()
        return rc

    def running(self) -> bool:
        return self._process.returncode is None

    async def stderr(self) -> str:
        return (await self._stderr).decode("utf-8", errors="replace")

    def kill(self) -> None:
        if self._deadline is not None:
            self._deadline.cancel()
        if self.running():
            _kill(self._process)
