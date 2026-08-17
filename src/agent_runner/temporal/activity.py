"""The Temporal activity wrapper around ``run_attempt``.

Runs INSIDE a project's activity function. The project owns the workflow,
the activity registration, and its retry policy; this wrapper owns the
Temporal-facing mechanics of one CLI attempt:

- the heartbeat pump while the CLI runs (liveness is the heartbeat)
- session_ref + progress in heartbeat details (a retry resumes the session)
- the resume budget with fresh-session fallback, recorded
- checkpoint folders prepared before spawn, term stamps verified before
  resume (mismatch: discard, log loudly, run fresh)
- graceful cancellation: a cancelled activity terminates and reaps the CLI
  before the cancellation propagates
- the ruled outcome-to-retry mapping on the way out (retry.py)
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from temporalio import activity

from agent_runner import outcomes, workdirs
from agent_runner.attempt import AttemptCancelled, run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.harness.stream import StreamEvent
from agent_runner.runtime import AttemptReport, RunSpec, Verdict
from agent_runner.temporal.retry import application_error_for


@dataclass
class TemporalRunConfig:
    """The wrapper's knobs, all backstopped by the caller's activity
    options (the retry policy and the heartbeat/start-to-close timeouts are
    project config — the 8h runaway backstop lives there)."""

    heartbeat_seconds: float = 15.0
    resume_budget: int = 3            # resumes of ONE session before a fresh fallback
    rate_limit_backoff: timedelta = timedelta(minutes=15)


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
class _HeartbeatState:
    """What rides heartbeat details: one dict, keys stable.

    Written from the attempt's worker thread (the on_event/on_session
    callbacks), read from the event loop (the heartbeat pump): the lock keeps
    each payload a consistent snapshot — never a stale session_ref beside a
    reset resume_count."""

    session_ref: str | None = None
    resume_count: int = 0
    agent_fp: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def payload(self) -> dict[str, Any]:
        with self.lock:
            return {
                "session_ref": self.session_ref,
                "resume_count": self.resume_count,
                "agent_fp": self.agent_fp,
                "progress": dict(self.progress),
            }


def agent_fingerprint(agent: AgentDef | None) -> str | None:
    """A content hash of the prompt the session was started under. A session
    is only resumable by the SAME prompt: any change — minor bumps included
    (ruled 2026-08-16) — makes the recorded transcript a conversation with a
    prompt that no longer exists, so a differing fingerprint runs fresh."""
    if agent is None:
        return None
    material = agent.body + "\x00" + repr(sorted((agent.config or {}).items()))
    return hashlib.sha256(material.encode()).hexdigest()[:16]


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

    prior = prior_heartbeat_details()
    current_fp = agent_fingerprint(agent)
    if prior and prior.get("agent_fp") and current_fp and prior["agent_fp"] != current_fp:
        activity.logger.warning(
            "%s: the agent prompt changed since the prior session started; "
            "its transcript belongs to a prompt that no longer exists — running fresh",
            spec.key,
        )
        prior = None
    session_ref, resume_count, fresh_fallback = resume_decision(prior, config.resume_budget)
    if fresh_fallback:
        activity.logger.warning(
            "%s: resume budget (%d) exhausted for the prior session; "
            "falling back to a fresh session",
            spec.key,
            config.resume_budget,
        )

    if checkpoint is not None:
        # Prepared before spawn; verified before ANY resume of work in it.
        # A stamp from another term is discarded loudly and the run is fresh
        # for whatever was lost — time, never correctness.
        workdirs.verify_or_discard(checkpoint.directory, checkpoint.term)

    state = _HeartbeatState(
        session_ref=session_ref, resume_count=resume_count, agent_fp=current_fp
    )

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
            if ref != session_ref:
                # A fresh session opened: the budget is the session's, so the
                # count restarts with it.
                state.resume_count = 0
            state.session_ref = ref

    async def pump() -> None:
        while True:
            activity.heartbeat(state.payload())
            await asyncio.sleep(config.heartbeat_seconds)

    stop = threading.Event()
    pump_task = asyncio.create_task(pump())
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
            session_ref=session_ref,
            run_id=info.workflow_run_id or "",
            attempt=info.attempt,
            variables=variables,
            resources=resources,
            timeout_minutes=timeout_minutes,
            should_stop=stop.is_set,
        )
    )
    try:
        report = await asyncio.shield(inner)
    except asyncio.CancelledError:
        # Graceful cancel: terminate and reap the CLI before propagating.
        stop.set()
        try:
            await inner
        except (AttemptCancelled, asyncio.CancelledError):
            pass
        raise
    finally:
        pump_task.cancel()
        try:
            await pump_task
        except asyncio.CancelledError:
            pass
        # The last word always lands: the final state (session_ref for the
        # next attempt's resume) is heartbeat-recorded even on failure.
        activity.heartbeat(state.payload())

    if report.outcome == outcomes.VALID:
        return report
    raise application_error_for(report, rate_limit_backoff=config.rate_limit_backoff)
