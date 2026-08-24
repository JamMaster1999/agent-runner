"""The Temporal activity wrapper around one attempt in a sandbox.

The in-process wrapper (``activity.run_agent_attempt``) runs the CLI on
the worker; this one runs it in a sandbox the project opened
(``agent_runner.executor``) and supervises the line stream
``agent_runner.remote.serve`` writes. Same heartbeat state, same attempt
record, same outcome-to-retry mapping on the way out — what changes is
where the process lives and how liveness is judged:

- the heartbeat beats only on what it just fetched from the stream (a
  session, a usage line, a tick). No fetch, no beat: a sandbox that goes
  silent past the heartbeat timeout is rescheduled, and the retry lands
  back in the same sandbox, kills the stale attempt process by pid, and
  resumes the session
- a sandbox that is gone (TTL, crash, terminate) raises ``sandbox_gone``,
  non-retryable at the activity: only the workflow can open another
- a cancelled activity ends the attempt process with one ``kill`` before
  the cancellation propagates; the sandbox itself is the project's to
  close
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Sequence

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agent_runner import outcomes
from agent_runner.executor import SANDBOX_GONE, ExecutorGone, Sandbox
from agent_runner.harness.base import AgentDef
from agent_runner.harness.stream import StreamEvent
from agent_runner.remote import (
    AttemptRequest,
    attempt_workdir,
    pid_file,
    report_from_json,
)
from agent_runner.runtime import AttemptReport, RunSpec, Usage
from agent_runner.temporal.activity import (
    HeartbeatState,
    TemporalRunConfig,
    attempt_record,
    conclude,
    heartbeat_callbacks,
    now_iso,
    prior_heartbeat_details,
    starting_state,
)
from agent_runner.temporal.retry import failure_details

CANCEL_GRACE_SECONDS = 30.0
EXEC_GRACE_SECONDS = 600.0
ENDED_POLLS = 3
STDERR_TAIL = 4000


KILL_SCRIPT = """
[ -f "$1" ] || exit 0
pid=$(cat "$1"); rm -f "$1"
case "$pid" in ''|*[!0-9]*) exit 0;; esac
kill -TERM "$pid" 2>/dev/null || exit 0
for i in $(seq {grace}); do kill -0 "$pid" 2>/dev/null || exit 0; sleep 1; done
kill -KILL "$pid" 2>/dev/null
"""


def kill_stale(sandbox: Sandbox, pidfile: str) -> None:
    """End whatever attempt process a dead supervisor left behind for this
    key, and wait until it is gone: two attempts of one batch must never
    run at once, and the file is the only handle the sandbox keeps. The
    pid is checked to be one — the agent can write to the workspace."""
    sandbox.exec("sh", "-c", KILL_SCRIPT.format(grace=int(CANCEL_GRACE_SECONDS)), "kill", pidfile).wait()


async def run_sandboxed_attempt(
    sandbox: Sandbox,
    command: Sequence[str],
    spec: RunSpec,
    task: str,
    *,
    validator: dict[str, Any],
    agent: AgentDef | None = None,
    checkpoint: dict[str, str] | None = None,
    resources: Sequence[str] = (),
    watch_dirs: Sequence[str] = (),
    config: TemporalRunConfig | None = None,
    timeout_minutes: float | None = None,
) -> AttemptReport:
    """One attempt in ``sandbox`` via ``command`` (the project's entrypoint
    that calls ``remote.serve``). Returns the ``valid`` report, raises an
    ``ApplicationError`` typed with the outcome word otherwise, and
    ``sandbox_gone`` (non-retryable) when the sandbox ended under it."""
    config = config or TemporalRunConfig()
    info = activity.info()
    state = starting_state(spec.key, info, prior_heartbeat_details(), agent, config)
    state.sandbox = sandbox.id
    pidfile = str(pid_file(sandbox.workspace, spec.key))
    request = AttemptRequest(
        spec=spec,
        task=task,
        workdir=str(attempt_workdir(sandbox.workspace, spec.key, info.attempt)),
        validator=validator,
        agent=agent,
        session_ref=state.session_ref,
        session_usage=dict(state.session_usage),
        run_id=info.workflow_run_id or "",
        attempt=info.attempt,
        timeout_minutes=timeout_minutes,
        checkpoint=checkpoint,
        resources=tuple(resources),
        watch_dirs=tuple(watch_dirs),
        pid_file=pidfile,
        tick_seconds=config.heartbeat_seconds,
    )
    # The exec's own deadline: the attempt's budget plus the time its
    # cleanup and validation may take — never open-ended (a dead worker
    # on the platform's side could otherwise hold the call forever).
    exec_timeout = (
        int(timeout_minutes * 60 + EXEC_GRACE_SECONDS) if timeout_minutes is not None else None
    )
    on_event, on_session, on_usage = heartbeat_callbacks(state)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    started_at = now_iso()
    try:
        await asyncio.to_thread(kill_stale, sandbox, pidfile)
        proc = await asyncio.to_thread(
            sandbox.exec, *command, stdin=request.to_json().encode(), timeout=exec_timeout
        )
    except ExecutorGone as exc:
        raise _gone(state, info.attempt, started_at, str(exc)) from exc
    activity.heartbeat(state.payload())  # the attempt is running; the first tick is a while off
    stream_failure: list[ExecutorGone] = []

    def pump() -> None:
        try:
            for line in proc.lines():
                loop.call_soon_threadsafe(queue.put_nowait, line)
        except ExecutorGone as exc:
            stream_failure.append(exc)
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    reader = threading.Thread(target=pump, name="sandbox-stream", daemon=True)
    reader.start()
    report: AttemptReport | None = None
    cancelled = False
    try:
        while True:
            line = await queue.get()
            if line is None:
                break
            try:
                event = json.loads(line)
            except ValueError:
                continue  # not ours: the entrypoint's own chatter
            if not isinstance(event, dict):
                continue
            kind = event.pop("e", None)
            if kind == "session":
                on_session(str(event.get("ref") or ""))
            elif kind == "usage":
                on_usage(Usage.from_dict(event.get("usage") or {}), Usage.from_dict(event.get("session_usage") or {}))
            elif kind == "event":
                on_event(StreamEvent(**event))
            elif kind == "report":
                report = report_from_json(event)
            elif kind == "cancelled":
                cancelled = True
            elif kind == "error":
                report = AttemptReport(
                    outcome=outcomes.INFRA,
                    session_ref=state.session_ref,
                    error=f"{spec.key}: attempt process failed: {event.get('message')}",
                )
            activity.heartbeat(state.payload())  # only what was just fetched
    except asyncio.CancelledError:
        await asyncio.to_thread(_kill, sandbox, pidfile)
        await asyncio.to_thread(reader.join, CANCEL_GRACE_SECONDS)
        if not reader.is_alive():
            await asyncio.to_thread(proc.wait)  # the stream closed: reap, never leave a zombie
        raise

    if cancelled:
        activity.heartbeat(state.payload())
        raise ApplicationError(f"{spec.key}: the attempt process was cancelled under the activity", type=outcomes.INFRA)
    try:
        if stream_failure:
            raise stream_failure[0]
        rc = await asyncio.to_thread(proc.wait)
    except ExecutorGone as exc:
        raise _gone(state, info.attempt, started_at, str(exc)) from exc
    if report is None:
        if await _ended(sandbox):
            raise _gone(state, info.attempt, started_at, f"sandbox {sandbox.id} ended during the attempt (rc={rc})")
        report = AttemptReport(
            outcome=outcomes.INFRA,
            session_ref=state.session_ref,
            error=f"{spec.key}: attempt process exited {rc} without a report",
            detail=(await asyncio.to_thread(proc.stderr))[-STDERR_TAIL:],
        )
    return conclude(state, info, report, started_at, config)


async def _ended(sandbox: Sandbox) -> bool:
    """Whether the sandbox is gone. Asked only after the attempt process
    died without a report; a platform can report the sandbox alive for a
    moment after it ended (Modal: ~1 s), so the answer is asked more than
    once before an exit is blamed on the process alone."""
    for _ in range(ENDED_POLLS):
        if await asyncio.to_thread(sandbox.poll) is not None:
            return True
        await asyncio.sleep(1)
    return False


def _kill(sandbox: Sandbox, pidfile: str) -> None:
    try:
        kill_stale(sandbox, pidfile)
    except ExecutorGone:
        pass


def _gone(state: HeartbeatState, attempt: int, started_at: str, message: str) -> ApplicationError:
    """The sandbox ended under the attempt: recorded like any other
    failed attempt, raised as the one error type only a new sandbox can
    answer."""
    report = AttemptReport(outcome=outcomes.INFRA, session_ref=state.session_ref, error=message)
    report.attempts = tuple(state.record(attempt_record(attempt, report, started_at, now_iso())))
    activity.heartbeat(state.payload())
    return ApplicationError(message, failure_details(report), type=SANDBOX_GONE, non_retryable=True)
