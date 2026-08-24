"""The attempt-in-sandbox protocol (agent_runner.md).

The whole ``run_attempt`` runs INSIDE the sandbox — Chrome provisioning,
prompt delivery, validation, repair rounds, reaping, all in situ — and the
supervisor outside sees only a line stream. Two sides, one module:

- ``serve`` is the sandbox side: read one ``AttemptRequest`` from stdin,
  run the attempt, write JSON events to stdout as they happen (session,
  progress, usage, a tick every few seconds so silence is never
  ambiguous) and the ``report`` last. The project owns the entrypoint
  command that calls ``serve`` and hands it the one thing only a project
  can build: the validate closure, from the opaque ``validator`` payload
  it put in the request.
- ``AttemptRequest`` / ``report_to_json`` / ``report_from_json`` are the
  wire shapes both sides share; ``pid_file`` is where the attempt process
  leaves its pid so a supervisor can end it (a cancel, or a retry's
  stale predecessor) with one ``kill``.

A supervisor may beat its heartbeat only on what it just fetched from
this stream. No fetch, no beat.
"""

from __future__ import annotations

import dataclasses
import json
import os
import signal
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Callable

from agent_runner import workdirs
from agent_runner.attempt import AttemptCancelled, run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.harness.stream import StreamEvent
from agent_runner.runtime import AttemptReport, Policy, RunSpec, Usage, Verdict
from agent_runner.sessions import prepare_session_homes
from agent_runner.state import key_segment
from agent_runner.workspace import RUNNER_DIR, attempts_root, workspace_root

TICK_SECONDS = 15.0


def pid_file(root: str | Path, key: str) -> Path:
    return Path(root) / RUNNER_DIR / "attempts" / f"{key_segment(key)}.pid"


def attempt_workdir(root: str | Path, key: str, attempt: int) -> Path:
    """``<root>/attempts/<key>/attempt-NN`` — under the workspace, outside
    what it pushes."""
    return attempts_root(Path(root)) / key_segment(key) / f"attempt-{attempt:02d}"


def checkpoint_dir(root: str | Path, child: str, term: str) -> Path:
    """The term-scoped checkpoint folder inside the workspace — the one
    builder, so the path is term-scoped by construction and travels with
    the workspace."""
    return workdirs.checkpoint_dir(Path(root), child, term)


@dataclasses.dataclass(frozen=True)
class AttemptRequest:
    """Everything one attempt needs, as data. ``validator`` is the
    project's own payload; ``serve`` hands it back to the project's
    ``build_validate`` untouched."""

    spec: RunSpec
    task: str
    workdir: str
    validator: dict[str, Any]
    agent: AgentDef | None = None
    session_ref: str | None = None
    session_usage: dict[str, Any] | None = None
    run_id: str = ""
    attempt: int = 1
    timeout_minutes: float | None = None
    checkpoint: dict[str, str] | None = None     # {"directory": ..., "term": ...}
    resources: tuple[str, ...] = ()
    watch_dirs: tuple[str, ...] = ()
    pid_file: str | None = None

    def to_json(self) -> str:
        data = dataclasses.asdict(self)
        return json.dumps(data)

    @classmethod
    def from_json(cls, text: str) -> "AttemptRequest":
        data = json.loads(text)
        spec = _tuples(data.pop("spec"))
        spec["policy"] = Policy(**_tuples(spec["policy"]))
        agent = data.pop("agent")
        return cls(
            spec=RunSpec(**spec),
            agent=AgentDef(**agent) if agent else None,
            resources=tuple(data.pop("resources")),
            watch_dirs=tuple(data.pop("watch_dirs")),
            **data,
        )


def _tuples(fields: dict[str, Any]) -> dict[str, Any]:
    """JSON has no tuples: every top-level list comes back as one (the
    frozen dataclasses hold tuples, and equality depends on it)."""
    return {k: tuple(v) if isinstance(v, list) else v for k, v in fields.items()}


def report_to_json(report: AttemptReport) -> dict[str, Any]:
    return {
        "outcome": report.outcome,
        "session_ref": report.session_ref,
        "error": report.error,
        "detail": report.detail,
        "resets_at": report.resets_at.isoformat() if report.resets_at else None,
        "data": report.data,
        "usage": report.usage.as_dict(),
        "session_usage": report.session_usage.as_dict(),
        "resumed": report.resumed,
        "repair_rounds_used": report.repair_rounds_used,
    }


def report_from_json(data: dict[str, Any]) -> AttemptReport:
    resets_at = data.get("resets_at")
    return AttemptReport(
        outcome=data["outcome"],
        session_ref=data.get("session_ref"),
        error=data.get("error") or "",
        detail=data.get("detail") or "",
        resets_at=datetime.fromisoformat(resets_at) if resets_at else None,
        data=data.get("data"),
        usage=Usage.from_dict(data.get("usage") or {}),
        session_usage=Usage.from_dict(data.get("session_usage") or {}),
        resumed=bool(data.get("resumed")),
        repair_rounds_used=int(data.get("repair_rounds_used") or 0),
    )


class _Emitter:
    """One JSON line per event, flushed at once, from any thread."""

    def __init__(self, out: IO[str]) -> None:
        self._out = out
        self._lock = threading.Lock()

    def __call__(self, event: str, **fields: Any) -> None:
        line = json.dumps({"e": event, **fields})
        with self._lock:
            self._out.write(line + "\n")
            self._out.flush()


def serve(
    build_validate: Callable[[dict[str, Any]], Callable[[Path], Verdict]],
    stdin: IO[str] = sys.stdin,
    stdout: IO[str] = sys.stdout,
) -> int:
    """The sandbox side of one attempt. Returns the process exit code:
    0 when a report (or a cancellation) was emitted, 1 when the attempt
    could not even start — the supervisor reads that as ``infra``."""
    from agent_runner.resources import providers

    emit = _Emitter(stdout)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    def on_event(event: StreamEvent) -> None:
        fields: dict[str, Any] = {"kind": event.kind, "message": event.message}
        if event.current is not None or event.total is not None:
            fields["current"] = event.current
            fields["total"] = event.total
        emit("event", **fields)

    def on_usage(usage: Usage, session_usage: Usage) -> None:
        emit("usage", usage=usage.as_dict(), session_usage=session_usage.as_dict())

    def tick() -> None:
        while not stop.wait(TICK_SECONDS):
            emit("tick")

    try:
        request = AttemptRequest.from_json(stdin.read())
        prepare_session_homes(workspace_root())
        if request.pid_file:
            pid = Path(request.pid_file)
            pid.parent.mkdir(parents=True, exist_ok=True)
            pid.write_text(str(os.getpid()))
        if request.checkpoint:
            workdirs.verify_or_discard(Path(request.checkpoint["directory"]), request.checkpoint["term"])
        threading.Thread(target=tick, name="attempt-tick", daemon=True).start()
        report = run_attempt(
            request.spec,
            request.task,
            Path(request.workdir),
            agent=request.agent,
            validate=build_validate(request.validator),
            on_event=on_event,
            on_session=lambda ref: emit("session", ref=ref),
            on_usage=on_usage,
            session_ref=request.session_ref,
            session_usage=Usage.from_dict(request.session_usage) if request.session_usage else None,
            run_id=request.run_id,
            attempt=request.attempt,
            resources={kind: providers()[kind]() for kind in request.resources},
            timeout_minutes=request.timeout_minutes,
            should_stop=stop.is_set,
            watch_dirs=tuple(Path(p) for p in request.watch_dirs),
        )
    except AttemptCancelled:
        emit("cancelled")
        return 0
    except Exception as exc:
        emit("error", message=f"{type(exc).__name__}: {exc}")
        return 1
    finally:
        stop.set()
    emit("report", **report_to_json(report))
    return 0
