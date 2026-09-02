"""The Temporal activity wrapper around one attempt in a sandbox.

The in-process wrapper (``activity.run_agent_attempt``) runs the CLI on
the worker; this one runs it in a sandbox the project opened
(``agent_runner.executor``) and supervises the line stream
``agent_runner.remote.serve`` writes. Same heartbeat state, same attempt
record, same outcome-to-retry mapping on the way out — what changes is
where the process lives and how liveness is judged:

- the heartbeat pumps through every phase — the wait for an account, the
  stale kill, the exec, the stream — so the server never mistakes a slow
  platform for a dead worker. The attempt process is judged here instead:
  its stream ticks every ``heartbeat_seconds``, so a sandbox silent for
  the activity's heartbeat timeout (no line, no exit, no status) is a
  wedged or dead attempt, killed by pid and ended ``infra``. The retry
  lands back in the same sandbox and resumes the session
- a sandbox that is gone (TTL, crash, terminate) raises ``sandbox_gone``,
  non-retryable at the activity: only the workflow can open another
- a cancelled activity ends the attempt process with one ``kill`` before
  the cancellation propagates; the sandbox itself is the project's to
  close
- on a credential pool the attempt runs on the least-loaded account and
  waits here while every account is full. A ``rate`` or ``server`` limit
  re-runs the attempt in place after a jittered pause (the account's cap
  halved, the session resumed); a ``usage`` limit holds the account until
  its reset and the activity retries on the pool's next free one
"""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agent_runner import outcomes
from agent_runner.executor import SANDBOX_GONE, ExecutorGone, Proc, Sandbox
from agent_runner.harness.base import AgentDef
from agent_runner.harness.stream import StreamEvent
from agent_runner.pool import KIND_SERVER, KIND_USAGE, Pool, jitter
from agent_runner.remote import AttemptRequest, known, report_from_json
from agent_runner.runtime import AttemptReport, RunSpec, Usage
from agent_runner.temporal.activity import (
    HeartbeatState,
    TemporalRunConfig,
    activity_deadline,
    attempt_record,
    conclude,
    heartbeat_callbacks,
    heartbeating,
    now_iso,
    prior_heartbeat_details,
    starting_state,
)
from agent_runner.temporal.retry import failure_details
from agent_runner.workspace import attempt_workdir, pid_file

START_SECONDS = 900.0         # the stale kill plus the exec, on a platform under load
SILENCE_SECONDS = 120.0       # when the activity names no heartbeat timeout
CANCEL_GRACE_SECONDS = 30.0   # how long a cancel waits for the stream to close
KILL_GRACE_SECONDS = 2        # TERM, then KILL
EXEC_GRACE_SECONDS = 600.0
ENDED_POLLS = 3
STDERR_TAIL = 4000


# TERM lets serve end its CLI and say "cancelled"; KILL is unblockable and
# a zombie runs no code, so after it there is nothing left to wait for.
KILL_SCRIPT = """
[ -f "$1" ] || exit 0
pid=$(cat "$1"); rm -f "$1"
case "$pid" in ''|*[!0-9]*) exit 0;; esac
kill -TERM "$pid" 2>/dev/null || exit 0
sleep {grace}
kill -KILL "$pid" 2>/dev/null
"""


async def kill_stale(sandbox: Sandbox, pidfile: str) -> None:
    """End whatever attempt process a dead supervisor left behind for this
    key: two attempts of one batch must never run at once, and the file is
    the only handle the sandbox keeps. The pid is checked to be one — the
    agent can write to the workspace."""
    proc = await sandbox.exec("sh", "-c", KILL_SCRIPT.format(grace=KILL_GRACE_SECONDS), "kill", pidfile)
    await proc.wait()


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
    env: Mapping[str, str] | None = None,
    pool: Pool | None = None,
) -> AttemptReport:
    """One attempt in ``sandbox`` via ``command`` (the project's entrypoint
    that calls ``remote.serve``). ``env`` rides the exec, over the sandbox's
    own — the place for what changes per attempt; ``pool`` adds the
    credential of the account this attempt runs on (agent_runner.pool).
    Returns the ``valid`` report, raises an ``ApplicationError`` typed with
    the outcome word otherwise, and ``sandbox_gone`` (non-retryable) when
    the sandbox ended under it. The activity's heartbeat timeout is how
    long the attempt's stream may go silent, so it must leave the ticks
    (every ``config.heartbeat_seconds``) room: several ticks, not one."""
    config = config or TemporalRunConfig()
    info = activity.info()
    state = starting_state(spec.key, info, prior_heartbeat_details(), agent, config)
    state.sandbox = sandbox.id
    pidfile = str(pid_file(sandbox.workspace, spec.key))
    # The exec's own deadline: the attempt's budget plus the time its
    # cleanup and validation may take — never open-ended (a dead worker
    # on the platform's side could otherwise hold the call forever).
    exec_timeout = (
        int(timeout_minutes * 60 + EXEC_GRACE_SECONDS) if timeout_minutes is not None else None
    )
    silence = (info.heartbeat_timeout or timedelta(seconds=SILENCE_SECONDS)).total_seconds()
    deadline = activity_deadline(info)
    on_event, on_session, on_usage = heartbeat_callbacks(state)

    def infra(error: str, detail: str = "") -> AttemptReport:
        return AttemptReport(outcome=outcomes.INFRA, session_ref=state.session_ref, error=error, detail=detail)

    def request() -> AttemptRequest:
        return AttemptRequest(
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

    async def run_once(env: Mapping[str, str] | None, started_at: str) -> AttemptReport:
        """One attempt process, start to report. Raises ``sandbox_gone`` and
        re-raises cancellation; every other failure is a report."""
        proc: Proc | None = None
        pid: int | None = None
        report: AttemptReport | None = None
        watchdog: asyncio.Timeout | None = None
        try:
            async with asyncio.timeout(START_SECONDS):
                await kill_stale(sandbox, pidfile)
                proc = await sandbox.exec(
                    *command, stdin=request().to_json().encode(), env=env, timeout=exec_timeout
                )
            lines = proc.lines()
            while True:
                async with asyncio.timeout(silence) as watchdog:
                    line = await anext(lines, None)
                    if line is None:
                        rc = await proc.wait()
                        break
                try:
                    event = json.loads(line)
                except ValueError:
                    continue  # not ours: the entrypoint's own chatter
                if not isinstance(event, dict):
                    continue
                kind = event.pop("e", None)
                if kind == "pid":
                    pid = int(event["pid"])
                elif kind == "session":
                    on_session(str(event.get("ref") or ""))
                elif kind == "usage":
                    on_usage(Usage.from_dict(event.get("usage") or {}), Usage.from_dict(event.get("session_usage") or {}))
                elif kind == "event":
                    on_event(StreamEvent(**known(StreamEvent, event)))
                elif kind == "report":
                    report = report_from_json(event)
                elif kind == "cancelled":
                    report = infra(f"{spec.key}: the attempt process was cancelled under the activity")
                elif kind == "error":
                    report = infra(f"{spec.key}: attempt process failed: {event.get('message')}")
            if report is None:
                # The process died without a word, or the sandbox under it.
                async with asyncio.timeout(silence) as watchdog:
                    if await _ended(sandbox):
                        raise _gone(state, info.attempt, started_at, f"sandbox {sandbox.id} ended during the attempt (rc={rc})")
                    detail = (await proc.stderr())[-STDERR_TAIL:]
                report = infra(f"{spec.key}: attempt process exited {rc} without a report", detail)
        except asyncio.CancelledError:
            # The pid THIS attempt announced, not whatever the shared pid file
            # holds by now (a successor scheduled by the heartbeat timeout may
            # already own it).
            if pid is not None:
                await _kill(sandbox, pid)
            if proc is not None:
                with suppress(TimeoutError, ExecutorGone):
                    async with asyncio.timeout(CANCEL_GRACE_SECONDS):
                        await proc.wait()  # the stream closed: reap, never leave a zombie
            raise
        except ApplicationError:
            raise  # _gone: already recorded, already typed
        except ExecutorGone as exc:
            raise _gone(state, info.attempt, started_at, str(exc)) from exc
        except Exception as exc:
            if pid is not None:
                await _kill(sandbox, pid)
            if proc is None:
                what = f"sandbox {sandbox.id} did not start the attempt within {START_SECONDS:g}s"
            elif watchdog is not None and watchdog.expired():
                what = f"sandbox {sandbox.id} went silent for {silence:g}s"
            else:
                what = f"the attempt in sandbox {sandbox.id} failed: {exc!r}"
            report = infra(f"{spec.key}: {what}")
        return report

    async with heartbeating(state, config.heartbeat_seconds):
        while True:
            started_at = now_iso()
            slot = await pool.acquire() if pool else None
            try:
                report = await run_once({**(env or {}), **pool.env(slot)} if pool else env, started_at)
            finally:
                if pool:
                    pool.release(slot)
            if pool and report.outcome == outcomes.VALID:
                pool.succeeded(slot)
            if not pool or report.outcome != outcomes.RATE_LIMITED:
                break
            now = datetime.now(timezone.utc)
            kind = report.limit_kind or KIND_SERVER
            pool.limited(slot, kind, until=report.resets_at or now + config.rate_limit_backoff)
            pause = jitter(config.rate_limit_pause)
            if kind == KIND_USAGE or (deadline and now + pause > deadline - config.rate_limit_reset_margin):
                break
            # The window is not spent: the same activity attempt runs again
            # after a pause, resuming the session, and the record says so.
            state.record(attempt_record(info.attempt, report, started_at, now_iso()))
            on_event(StreamEvent("rate_limited", f"{kind} limit on account {slot}: re-running in {pause.total_seconds():.0f}s"))
            activity.heartbeat(state.payload())
            await asyncio.sleep(pause.total_seconds())
    resets_at = pool.next_free() if pool and report.outcome == outcomes.RATE_LIMITED else None
    return conclude(state, info, report, started_at, config, resets_at)


async def _ended(sandbox: Sandbox) -> bool:
    """Whether the sandbox is gone. Asked only after the attempt process
    died without a report; a platform can report the sandbox alive for a
    moment after it ended (Modal: ~1 s), so the answer is asked more than
    once before an exit is blamed on the process alone."""
    for attempt in range(ENDED_POLLS):
        if attempt:
            await asyncio.sleep(1)
        if await sandbox.poll() is not None:
            return True
    return False


async def _kill(sandbox: Sandbox, pid: int) -> None:
    try:
        proc = await sandbox.exec(
            "sh", "-c", f"kill -TERM {pid} 2>/dev/null; sleep {KILL_GRACE_SECONDS}; kill -KILL {pid} 2>/dev/null"
        )
        await proc.wait()
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
