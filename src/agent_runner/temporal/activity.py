"""The Temporal activity wrapper around ``run_attempt``.

Runs INSIDE a project's activity function. The project owns the workflow,
the activity registration, and its retry policy; this wrapper owns the
Temporal-facing mechanics of one CLI attempt:

- the heartbeat pump while the CLI runs (the heartbeat says the attempt is
  still running; whether the agent is still PRODUCING is the attempt loop's
  stall watchdog, which fails the attempt so the retry lands here)
- session_ref, progress, and the running attempt's usage in heartbeat
  details (a retry resumes the session; a dashboard shows spend live)
- one typed record per attempt, success included (``attempt_record``),
  carried by the heartbeat and landing in the final result and in failure
  details as ``attempts``
- the resume budget with fresh-session fallback, recorded
- checkpoint folders prepared before spawn, term stamps verified before
  resume (mismatch: discard, log loudly, run fresh)
- graceful cancellation: a cancelled activity terminates and reaps the CLI
  before the cancellation propagates
- the ruled outcome-to-retry mapping on the way out (retry.py)
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import threading
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from temporalio import activity

from agent_runner import outcomes, workdirs
from agent_runner.attempt import AttemptCancelled, run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.harness.stream import StreamEvent
from agent_runner.runtime import AttemptReport, RunSpec, Usage, Verdict
from agent_runner.temporal.retry import application_error_for


@dataclass
class TemporalRunConfig:
    """The wrapper's knobs, all backstopped by the caller's activity
    options (the retry policy and the heartbeat/start-to-close timeouts are
    project config — the 8h runaway backstop lives there)."""

    heartbeat_seconds: float = 15.0
    resume_budget: int = 3            # resumes of ONE session before a fresh fallback
    rate_limit_backoff: timedelta = timedelta(minutes=15)
    # The longest a rate_limited retry waits on the CLI's reset time. A
    # waiting retry holds whatever slot the caller gated the activity
    # behind, so the cap is the caller's call. A reset that leaves less
    # than the margin before the activity's schedule-to-close fails at
    # once instead (retry.application_error_for): the retry could not
    # finish anyway.
    rate_limit_reset_cap: timedelta = timedelta(hours=6)
    rate_limit_reset_margin: timedelta = timedelta(minutes=15)
    # The pause (jittered) before a sandboxed attempt re-runs in place after
    # a rate or server limit — the account is over its concurrency or the
    # provider is busy, and the window is not spent.
    rate_limit_pause: timedelta = timedelta(seconds=30)


@dataclass
class CheckpointSpec:
    """A term-scoped checkpoint folder declaration (``scrape``-shaped
    children). The path comes from the ONE builder function
    (``workdirs.checkpoint_dir``) so it is term-scoped by construction; the
    project renders the same path into its prompt as ``${checkpoint_dir}``."""

    root: Path
    child: str
    term: str

    @property
    def directory(self) -> Path:
        return workdirs.checkpoint_dir(self.root, self.child, self.term)


@dataclass
class HeartbeatState:
    """What rides heartbeat details: one dict, keys stable.

    Written from the attempt's worker thread (the on_event/on_session
    callbacks), read from the event loop (the heartbeat pump): the lock keeps
    each payload a consistent snapshot — never a stale session_ref beside a
    reset resume_count."""

    attempt: int = 1
    session_ref: str | None = None
    resume_count: int = 0
    agent_fp: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=lambda: Usage().as_dict())
    session_usage: dict[str, Any] = field(default_factory=lambda: Usage().as_dict())
    attempts: list[dict[str, Any]] = field(default_factory=list)
    sandbox: str | None = None      # the sandbox the attempt runs in (the sandboxed wrapper)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def payload(self) -> dict[str, Any]:
        with self.lock:
            return {
                "attempt": self.attempt,
                "session_ref": self.session_ref,
                "resume_count": self.resume_count,
                "agent_fp": self.agent_fp,
                "progress": dict(self.progress),
                "usage": dict(self.usage),
                "session_usage": dict(self.session_usage),
                "attempts": list(self.attempts),
                "sandbox": self.sandbox,
            }

    def record(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """One record per attempt, carried to the next attempt by the
        heartbeat and into history by the final result or failure. Bounded:
        the retry policy caps attempts, the slice caps a runaway one.
        Returns the list as it now stands."""
        with self.lock:
            self.attempts.append(entry)
            del self.attempts[:-ATTEMPTS_KEPT]
            return list(self.attempts)


ATTEMPTS_KEPT = 10
RECORD_TEXT_LIMIT = 500  # bytes, for each of error and detail
RECORD_REF_LIMIT = 128   # bytes, for the CLI-supplied session_ref


@dataclass
class AttemptRecord:
    """What one attempt leaves behind, success or not: how it ended, the
    session it ran in, when, what it alone spent, and where that left the
    session's total. The one shape for reported and vanished attempts
    alike; the defaults are what a vanished attempt can say."""

    attempt: int
    outcome: str
    error: str
    detail: str = ""
    session_ref: str | None = None
    resumed: bool | None = None
    resets_at: str | None = None
    limit_kind: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    usage: dict[str, Any] = field(default_factory=lambda: Usage().as_dict())
    session_usage: dict[str, Any] = field(default_factory=lambda: Usage().as_dict())


RECORD_KEYS = tuple(f.name for f in dataclasses.fields(AttemptRecord))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _head(text: str, limit: int) -> str:
    return text.encode()[:limit].decode(errors="ignore")


def _tail(text: str, limit: int) -> str:
    return text.encode()[-limit:].decode(errors="ignore")


def attempt_record(
    attempt: int, report: AttemptReport, started_at: str, ended_at: str
) -> dict[str, Any]:
    """``error`` keeps its head (the key and the cause come first), ``detail``
    its tail (the CLI's last words)."""
    return dataclasses.asdict(
        AttemptRecord(
            attempt=attempt,
            outcome=report.outcome,
            error=_head(report.error, RECORD_TEXT_LIMIT),
            detail=_tail(report.detail, RECORD_TEXT_LIMIT),
            session_ref=_head(report.session_ref, RECORD_REF_LIMIT) if report.session_ref else None,
            resumed=report.resumed,
            resets_at=report.resets_at.isoformat(timespec="seconds") if report.resets_at else None,
            limit_kind=report.limit_kind,
            started_at=started_at,
            ended_at=ended_at,
            usage=report.usage.as_dict(),
            session_usage=report.session_usage.as_dict(),
        )
    )


def session_usage_before(session_ref: str | None, attempts: list[dict[str, Any]]) -> Usage:
    """Where the session stood when its latest recorded attempt ended —
    the baseline this attempt's own spend is measured from."""
    if session_ref:
        for entry in reversed(attempts):
            if entry.get("session_ref") == session_ref and entry.get("session_usage"):
                return Usage.from_dict(entry["session_usage"])
    return Usage()


def agent_fingerprint(agent: AgentDef | None) -> str | None:
    """A content hash of the prompt the session was started under. A session
    is only resumable by the SAME prompt: any change — minor bumps included
    (ruled 2026-08-16) — makes the recorded transcript a conversation with a
    prompt that no longer exists, so a differing fingerprint runs fresh."""
    if agent is None:
        return None
    material = agent.body + "\x00" + repr(sorted((agent.config or {}).items()))
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def vanished_attempts(
    attempt: int, recorded: list[dict[str, Any]], prior: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """The attempts nobody reported: a worker that dies mid-attempt never
    writes its failure, Temporal just starts the next attempt elsewhere. The
    attempt number counts them anyway, so the gap between it and the record
    names them. The dead worker's last heartbeat is its only other trace:
    when it belongs to a vanished attempt, that attempt's record takes the
    session it was in and what it had spent, so the session's total still
    counts the dead attempt. Only the newest ``ATTEMPTS_KEPT`` numbers are
    named — older ones would be sliced away anyway."""
    seen = {entry.get("attempt") for entry in recorded}
    prior = prior or {}
    entries = []
    for number in range(max(1, attempt - ATTEMPTS_KEPT), attempt):
        if number in seen:
            continue
        record = AttemptRecord(
            attempt=number,
            outcome=outcomes.INFRA,
            error="attempt ended without a report — the worker died mid-attempt",
        )
        if prior.get("attempt") == number:
            record.session_ref = prior.get("session_ref")
            record.usage = Usage.from_dict(prior.get("usage") or {}).as_dict()
            record.session_usage = Usage.from_dict(prior.get("session_usage") or {}).as_dict()
        entries.append(dataclasses.asdict(record))
    return entries


def prior_heartbeat_details() -> dict[str, Any] | None:
    """The previous attempt's last recorded heartbeat payload, or None on a
    first attempt."""
    details = activity.info().heartbeat_details
    if details and isinstance(details[0], dict):
        return details[0]
    return None


def resume_decision(
    prior: dict[str, Any] | None, budget: int
) -> tuple[str | None, int, bool]:
    """(session_ref to resume, this attempt's resume_count, fresh_fallback).

    ``resume_count`` counts resumes OF ONE SESSION: a prior session under
    budget is resumed and the count increments; past the budget the attempt
    falls back to a fresh session (recorded by the True flag) and the count
    restarts — a brand-new session always has a full budget."""
    if not prior:
        return None, 0, False
    session_ref = prior.get("session_ref")
    resume_count = int(prior.get("resume_count") or 0)
    if not session_ref:
        return None, 0, False
    if resume_count >= budget:
        return None, 0, True
    return str(session_ref), resume_count + 1, False


async def run_agent_attempt(
    spec: RunSpec,
    task: str,
    workdir: Path,
    *,
    agent: AgentDef | None = None,
    validate: Callable[[Path], Verdict] | None = None,
    checkpoint: CheckpointSpec | None = None,
    resources: dict[str, Any] | None = None,
    variables: dict[str, str] | None = None,
    config: TemporalRunConfig | None = None,
    timeout_minutes: float | None = None,
) -> AttemptReport:
    """One CLI attempt under Temporal: returns the ``valid`` report, raises
    ``ApplicationError`` typed with the outcome word for everything else
    (see ``retry.application_error_for``), and re-raises cancellation after
    the CLI child is reaped."""
    config = config or TemporalRunConfig()
    info = activity.info()
    state = starting_state(spec.key, info, prior_heartbeat_details(), agent, config)
    session_ref = state.session_ref

    if checkpoint is not None:
        # Prepared before spawn; verified before ANY resume of work in it.
        # A stamp from another term is discarded loudly and the run is fresh
        # for whatever was lost — time, never correctness.
        workdirs.verify_or_discard(checkpoint.directory, checkpoint.term)
    on_event, on_session, on_usage = heartbeat_callbacks(state)
    stop = threading.Event()
    started_at = now_iso()
    async with heartbeating(state, config.heartbeat_seconds):
        inner = asyncio.create_task(
            asyncio.to_thread(
                run_attempt,
                spec,
                task,
                workdir,
                agent=agent,
                validate=validate,
                on_event=on_event,
                on_session=on_session,
                on_usage=on_usage,
                session_ref=session_ref,
                session_usage=Usage.from_dict(state.session_usage),
                run_id=info.workflow_run_id or "",
                attempt=info.attempt,
                variables=variables,
                resources=resources,
                timeout_minutes=timeout_minutes,
                should_stop=stop.is_set,
                watch_dirs=(checkpoint.directory,) if checkpoint is not None else (),
            )
        )
        try:
            report = await asyncio.shield(inner)
        except asyncio.CancelledError:
            # Graceful cancel: terminate and reap the CLI before propagating.
            stop.set()
            with suppress(AttemptCancelled, asyncio.CancelledError):
                await inner
            raise
    return conclude(state, info, report, started_at, config)


@asynccontextmanager
async def heartbeating(state: HeartbeatState, seconds: float) -> AsyncIterator[None]:
    """The heartbeat pump: ``state``'s payload every ``seconds`` for as
    long as the body runs. The server learns the attempt is alive; whether
    the agent still is, is the body's own judgment."""

    async def pump() -> None:
        while True:
            activity.heartbeat(state.payload())
            await asyncio.sleep(seconds)

    task = asyncio.create_task(pump())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def heartbeat_callbacks(
    state: HeartbeatState,
) -> tuple[Callable[[StreamEvent], None], Callable[[str], None], Callable[[Usage, Usage], None]]:
    """The three attempt callbacks that keep ``state`` current — one set,
    driven by ``run_attempt`` in-process or by a sandbox's event stream,
    so both wrappers heartbeat the same payload."""

    def on_event(event: StreamEvent) -> None:
        with state.lock:
            if event.current is not None or event.total is not None:
                state.progress = {
                    "current": event.current,
                    "total": event.total,
                    "message": event.message,
                }
            else:
                state.progress = {**state.progress, "message": event.message}

    def on_session(ref: str) -> None:
        with state.lock:
            if ref != state.session_ref:
                # A fresh session opened: the budget is the session's, so the
                # count restarts with it, and so does its usage.
                state.resume_count = 0
                state.session_usage = dict(state.usage)
            state.session_ref = ref

    def on_usage(usage: Usage, session_usage: Usage) -> None:
        with state.lock:
            state.usage = usage.as_dict()
            state.session_usage = session_usage.as_dict()

    return on_event, on_session, on_usage


def conclude(
    state: HeartbeatState,
    info: activity.Info,
    report: AttemptReport,
    started_at: str,
    config: TemporalRunConfig,
    resets_at: datetime | None = None,
) -> AttemptReport:
    """The attempt's last word: its record joins the history, the final
    state is heartbeat-recorded (session_ref for the next attempt's resume,
    even on failure), and anything but ``valid`` becomes the typed retry
    error. ``resets_at`` overrides the report's own for the retry delay: a
    pool's next free account, when the attempt ran on one — a five-hour
    limit is one account's, and the next account is usually free."""
    report.attempts = tuple(state.record(attempt_record(info.attempt, report, started_at, now_iso())))
    activity.heartbeat(state.payload())
    if report.outcome == outcomes.VALID:
        return report
    deadline = activity_deadline(info)
    now = datetime.now(timezone.utc)
    raise application_error_for(
        report,
        rate_limit_backoff=config.rate_limit_backoff,
        reset_cap=config.rate_limit_reset_cap,
        retry_by=deadline - config.rate_limit_reset_margin if deadline else None,
        now=now,
        resets_at=resets_at,
    )


def starting_state(
    key: str,
    info: activity.Info,
    prior: dict[str, Any] | None,
    agent: AgentDef | None,
    config: TemporalRunConfig,
) -> HeartbeatState:
    """The prior attempt's last heartbeat folded into this attempt's
    starting state: the record so far with the vanished attempts named,
    the session to resume when the prompt is unchanged and the budget
    allows, and where that session's usage stood. The record outlives the
    session it came from: a prompt change or an exhausted budget starts a
    fresh session, not a fresh history."""
    attempts = list((prior or {}).get("attempts") or [])
    attempts.extend(vanished_attempts(info.attempt, attempts, prior))
    del attempts[:-ATTEMPTS_KEPT]
    current_fp = agent_fingerprint(agent)
    if prior and prior.get("agent_fp") and current_fp and prior["agent_fp"] != current_fp:
        activity.logger.warning(
            "%s: the agent prompt changed since the prior session started; "
            "its transcript belongs to a prompt that no longer exists — running fresh",
            key,
        )
        prior = None
    session_ref, resume_count, fresh_fallback = resume_decision(prior, config.resume_budget)
    if fresh_fallback:
        activity.logger.warning(
            "%s: resume budget (%d) exhausted for the prior session; "
            "falling back to a fresh session",
            key,
            config.resume_budget,
        )
    return HeartbeatState(
        attempt=info.attempt,
        session_ref=session_ref,
        resume_count=resume_count,
        agent_fp=current_fp,
        session_usage=session_usage_before(session_ref, attempts).as_dict(),
        attempts=attempts,
    )


def activity_deadline(info: activity.Info) -> datetime | None:
    """When this activity's schedule-to-close runs out, or None when the
    caller set no such backstop."""
    if not info.schedule_to_close_timeout:
        return None
    return info.scheduled_time + info.schedule_to_close_timeout
