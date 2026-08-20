"""The core attempt loop: spawn, stream, classify, repair (agent_runner.md).

One synchronous function, ``run_attempt``: start an agent CLI with an
agent definition and a task message (both arrive already split — rendering
happened upstream), read the live output as it runs, and end the attempt
with exactly one outcome from ``agent_runner.outcomes``. Zero Temporal
imports, zero database, zero business knowledge: retries, receipts, and
workflows are the caller's.

Hygiene rides here too: the CLI child is ALWAYS reaped — on valid exit, on
timeout, on cancellation, and on any exception crossing the loop — so a
dead attempt can never leave a live agent burning provider budget, and
heavy memory dies with the process.

What crosses the boundary from the project side:

- ``validate`` — the project's contract closure; its ``Verdict`` decides
  ``valid`` vs ``invalid_schema`` and supplies the repair message. Output
  validity beats exit code (preserved core policy).
- ``on_event`` — live ``StreamEvent`` telemetry (progress, tool calls,
  token usage); the Temporal layer forwards it into heartbeat details.
- ``on_session`` — called once with the CLI session ref as soon as the
  stream reveals it, so a caller can persist the resume handle before the
  attempt ends.
- ``resources`` — registered providers for the spec's declared
  ``resource_specs`` (see ``agent_runner.resources``); their values arrive
  in the task as ``{{RESOURCE:*}}`` template substitutions.
"""

from __future__ import annotations

import dataclasses
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

from agent_runner import outcomes, util
from agent_runner.harness import get_adapter
from agent_runner.harness.base import AgentDef, HarnessAdapter
from agent_runner.harness.stream import JsonlTail, StreamEvent, parse_json_dict
from agent_runner.isolation import agent_env
from agent_runner.runtime import AttemptReport, RunnerError, RunSpec, Usage, Verdict
from agent_runner.sessions import RESUME_PREAMBLE, ensure_session_local, push_session
from agent_runner.templates import substitute
from agent_runner.util import write_text

DEFAULT_ATTEMPT_TIMEOUT_MINUTES = 60.0
REPAIR_TIMEOUT_MINUTES = 15.0

# Adapter evidence codes -> the outcome vocabulary: one alias, outcome words
# pass through, anything unproven is infra by definition.
_CODE_ALIASES = {"missing_command": outcomes.SPAWN_FAILURE}


def _outcome_for(code: str) -> str:
    aliased = _CODE_ALIASES.get(code)
    if aliased:
        return aliased
    return code if code in outcomes.OUTCOMES else outcomes.INFRA


class AttemptCancelled(RunnerError):
    """The caller stopped the attempt (``should_stop``); the CLI child was
    terminated and reaped before this raised. Cancellation is not an
    outcome — the attempt did not end, it was ended."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="cancelled", retryable=False, alert=False)


def runner_variables(
    run_id: str,
    key: str,
    attempt: int,
    workdir: Path,
    resource_variables: dict[str, str] | None = None,
) -> dict[str, str]:
    """Substitution values for the closed variable set, bound at attempt
    start. Each token substitutes exactly what its name promises;
    RUNNER_OUTPUT_PATH is the attempt's private directory — task messages
    append their own artifact filenames."""
    variables = {
        "RUNNER_ATTEMPT": str(attempt),
        "RUNNER_RUN_ID": run_id,
        "RUNNER_JOB_KEY": key,
        "RUNNER_OUTPUT_PATH": str(workdir),
    }
    variables.update(resource_variables or {})
    return variables


def _pdeathsig() -> Callable[[], None] | None:
    """Linux: the spawned CLI asks the kernel to TERM it when the worker
    dies, so a hard-killed worker cannot leak a token-burning orphan
    session. (PR_SET_PDEATHSIG also fires on forking-thread death — fine,
    the attempt thread always outlives its CLI.) Elsewhere: None — prod
    runs on Linux containers, which reap on exit anyway."""
    if sys.platform != "linux":
        return None
    import ctypes

    libc = ctypes.CDLL("libc.so.6", use_errno=True)

    def _set() -> None:
        libc.prctl(1, signal.SIGTERM)  # 1 = PR_SET_PDEATHSIG

    return _set


def _cpu_affinity() -> Callable[[], None] | None:
    """Linux: AGENT_RUNNER_AGENT_CPUS=N pins the spawned CLI to cores
    0..N-1, so its runtime sizes thread pools to N instead of every
    visible core. The workload is network-bound; on a big host each CLI
    otherwise idles 40+ threads, and a container's task cap dies at a few
    dozen agents. Unset, invalid, or elsewhere: None — behavior unchanged."""
    if sys.platform != "linux":
        return None
    try:
        cpus = int(os.environ.get("AGENT_RUNNER_AGENT_CPUS", ""))
    except ValueError:
        return None
    if cpus <= 0:
        return None

    def _set() -> None:
        os.sched_setaffinity(0, range(cpus))

    return _set


def _preexec() -> Callable[[], None] | None:
    """Everything the CLI child runs between fork and exec: PDEATHSIG,
    then the optional CPU clamp."""
    hooks = [hook for hook in (_pdeathsig(), _cpu_affinity()) if hook is not None]
    if not hooks:
        return None

    def _run() -> None:
        for hook in hooks:
            hook()

    return _run


def _terminate(process: subprocess.Popen) -> None:
    """Reap one CLI child: TERM, a 15s grace, then KILL. Never returns with
    the child alive."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


class _HookDrain:
    """Tail the harness hook log and convert this attempt's newly captured
    events to StreamEvents. The log is shared O_APPEND across parallel
    attempts, so each line is read once (offset tail) and filtered by the
    attempt's attribution stamp; a torn or corrupt line is skipped, never
    fatal."""

    def __init__(self, adapter: HarnessAdapter, spec: RunSpec, run_id: str, attempt: int):
        self.adapter = adapter
        self.spec = spec
        self.run_id = run_id
        self.attempt = attempt
        self.tail = JsonlTail(adapter.hook_event_log())

    def drain(self) -> list[StreamEvent]:
        events: list[StreamEvent] = []
        for line in self.tail.read_new_lines():
            event = parse_json_dict(line)
            if event is None:
                continue
            if (
                event.get("run_id") != self.run_id
                or event.get("job_stable_id") != self.spec.key
            ):
                continue
            try:
                if int(event.get("attempt") or -1) != self.attempt:
                    continue
            except (TypeError, ValueError):
                continue
            normalized = self.adapter.normalize_hook_event(event, self.spec.agent_ref)
            if normalized is None:
                continue
            kind, message = normalized
            events.append(StreamEvent(kind, message))
        return events


def _spawn_report(spec: RunSpec, message: str, detail: str = "") -> AttemptReport:
    return AttemptReport(
        outcome=outcomes.SPAWN_FAILURE,
        error=f"{spec.key}: {message}",
        detail=detail,
    )


def _classify_exit(
    adapter: HarnessAdapter, spec: RunSpec, spawn, returncode: int
) -> tuple[str, str, str]:
    """(outcome, error, detail) for a nonzero CLI exit, from CLI-owned error
    text only — never agent transcript tails."""
    detail = adapter.error_report(spawn.stdout_path, spawn.stderr_path)
    failure = adapter.classify_failure(
        detail or f"{adapter.display_name} exited {returncode}."
    )
    return _outcome_for(failure.code), f"{spec.key}: {failure}", failure.details


def _repair(
    adapter: HarnessAdapter,
    spec: RunSpec,
    verdict: Verdict,
    workdir: Path,
    stdout_path: Path,
    env: dict[str, str],
    emit: Callable[[StreamEvent], None],
    poll_seconds: float,
    should_stop: Callable[[], bool] | None,
) -> bool:
    """One repair round: message the attempt's own session with the
    project's repair text instead of burning a full re-run. The session
    already holds the research context, so a repair costs seconds. False on
    any failure — the caller falls back to the normal retry machinery."""
    message = verdict.repair_message
    if not message:
        return False
    session_ref = adapter.session_ref_from_log(stdout_path)
    if not session_ref:
        return False
    try:
        followup = adapter.build_followup(spec, workdir, session_ref)
    except RunnerError:
        return False
    if followup is None:
        return False
    emit(
        StreamEvent(
            "repair_started",
            f"Output failed validation ({verdict.message or 'invalid'}); "
            f"messaging the {adapter.display_name} session to repair",
        )
    )
    repair_tail = JsonlTail(followup.stdout_path)
    repair_parser = adapter.stream_parser()
    process: subprocess.Popen | None = None
    try:
        try:
            repair_tail.offset = followup.stdout_path.stat().st_size
        except FileNotFoundError:
            pass
        # Append, not truncate: repair rounds share these paths, and a
        # multi-round failure must keep every round's log for debugging.
        with followup.stdout_path.open("a") as stdout, followup.stderr_path.open("a") as stderr:
            process = subprocess.Popen(
                followup.command,
                cwd=util.project_root(),
                env=env,
                stdin=subprocess.PIPE,
                preexec_fn=_preexec(),
                stdout=stdout,
                stderr=stderr,
                text=True,
            )
            try:
                if process.stdin is None:
                    return False
                try:
                    process.stdin.write(message)
                    process.stdin.close()
                except BrokenPipeError:
                    return False
                deadline = time.monotonic() + REPAIR_TIMEOUT_MINUTES * 60
                while process.poll() is None:
                    if should_stop is not None and should_stop():
                        raise AttemptCancelled(f"{spec.key}: repair cancelled by caller")
                    if time.monotonic() > deadline:
                        return False
                    time.sleep(poll_seconds)
            finally:
                _terminate(process)
    except OSError:
        return False
    finally:
        if process is not None:
            for line in repair_tail.read_new_lines():
                for event in repair_parser.parse_line(line):
                    emit(event)
    return process is not None and process.returncode == 0


def run_attempt(
    spec: RunSpec,
    task: str,
    workdir: Path,
    *,
    agent: AgentDef | None = None,
    validate: Callable[[Path], Verdict] | None = None,
    on_event: Callable[[StreamEvent], None] | None = None,
    on_session: Callable[[str], None] | None = None,
    session_ref: str | None = None,
    run_id: str = "",
    attempt: int = 1,
    variables: dict[str, str] | None = None,
    resources: dict[str, Any] | None = None,
    timeout_minutes: float | None = None,
    poll_seconds: float = 2.0,
    should_stop: Callable[[], bool] | None = None,
) -> AttemptReport:
    """Run one CLI attempt and end it with exactly one outcome.

    ``task`` is the rendered task message; the only substitution performed
    here is the closed run-varying set (``{{RUNNER_*}}`` and
    ``{{RESOURCE:*}}`` tokens — values that cannot exist until attempt
    start). ``session_ref`` resumes that session instead of starting fresh
    (the resume preamble is prepended; ``spec.policy.resume_preamble``
    overrides the default text). ``should_stop`` polled true terminates the
    CLI and raises ``AttemptCancelled``.

    Configuration errors (an unknown harness, an unrenderable agent, a
    malformed template) RAISE — they are caller bugs, not attempt outcomes.
    A missing CLI binary is an attempt outcome (``spawn_failure``): another
    worker may have it.
    """
    adapter = get_adapter(spec.harness)
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    usage = Usage()

    def emit(event: StreamEvent) -> None:
        usage.add_event(event)
        if on_event is not None:
            try:
                on_event(event)
            except Exception:
                pass  # telemetry must never kill the attempt

    if agent is not None:
        effective_config = adapter.prepare_agent(agent)
        spec = dataclasses.replace(
            spec, agent_ref=agent.name, agent_config=effective_config
        )

    provisioned: list[Any] = []
    resources = resources or {}
    process: subprocess.Popen | None = None
    report: AttemptReport | None = None
    try:
        for resource_spec in spec.resource_specs:
            provider = resources.get(resource_spec.get("kind"))
            if provider is not None:
                provisioned.append(provider.provision(spec.key, attempt, workdir))
        resource_variables: dict[str, str] = {}
        for provider in resources.values():
            null_variables = getattr(provider, "null_variables", None)
            if null_variables is not None:
                resource_variables.update(null_variables())
        for resource in provisioned:
            resource_variables.update(resource.variables())
        resource_variables.update(variables or {})

        prompt = substitute(
            task, runner_variables(run_id, spec.key, attempt, workdir, resource_variables)
        )
        if session_ref and not ensure_session_local(adapter, session_ref):
            # The transcript exists on no worker and in no mirror, so the
            # CLI has nothing to reopen: run fresh rather than spend the
            # attempt on a resume that cannot land.
            session_ref = None
        if session_ref:
            preamble = spec.policy.resume_preamble
            prompt = (RESUME_PREAMBLE if preamble is None else preamble) + prompt

        try:
            if session_ref:
                spawn = adapter.build_resume(spec, workdir, session_ref)
            else:
                spawn = adapter.build_spawn(spec, workdir)
        except RunnerError as exc:
            if exc.code == "missing_command":
                return _spawn_report(spec, str(exc), exc.details)
            raise

        write_text(workdir / "prompt.md", prompt)
        env = agent_env(adapter, spec, run_id, attempt, workdir)
        env.update(adapter.bind_credentials())
        env.update(adapter.env_overrides())

        report = AttemptReport(
            outcome=outcomes.INFRA,
            session_ref=session_ref,
            usage=usage,
            resumed=bool(session_ref),
            workdir=workdir,
        )
        stream_tail = JsonlTail(spawn.stdout_path)
        stream_parser = adapter.stream_parser()
        hook_drain = _HookDrain(adapter, spec, run_id, attempt)

        fatal_errors: list[RunnerError] = []

        def drain_streams() -> None:
            for line in stream_tail.read_new_lines():
                payload = parse_json_dict(line)
                if payload is not None:
                    fatal = adapter.stream_fatal(payload)
                    if fatal is not None:
                        fatal_errors.append(fatal)
                    # The session ref rides the lines already being tailed —
                    # no log rescan, ever.
                    if report.session_ref is None or report.session_ref == session_ref:
                        live_ref = adapter.session_ref_from_event(payload)
                        if live_ref and live_ref != report.session_ref:
                            report.session_ref = live_ref
                            if on_session is not None:
                                try:
                                    on_session(live_ref)
                                except Exception:
                                    pass
                for event in stream_parser.parse_line(line):
                    emit(event)
            for event in hook_drain.drain():
                emit(event)

        with spawn.stdout_path.open("w") as stdout, spawn.stderr_path.open("w") as stderr:
            try:
                process = subprocess.Popen(
                    spawn.command,
                    cwd=util.project_root(),
                    env=env,
                    stdin=subprocess.PIPE,
                    preexec_fn=_preexec(),
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                )
            except OSError as exc:
                # The OS refused the fork/exec, or the binary vanished: no
                # CLI ever started, so this says nothing about the job.
                return _spawn_report(
                    spec, f"failed to spawn {adapter.display_name}: {exc}"
                )
            try:
                if process.stdin is None:
                    return _spawn_report(
                        spec, f"{adapter.display_name} stdin was not available"
                    )
                try:
                    process.stdin.write(prompt)
                    process.stdin.close()
                except BrokenPipeError:
                    drain_streams()
                    return _spawn_report(
                        spec,
                        f"{adapter.display_name} exited before receiving the prompt",
                        adapter.error_report(spawn.stdout_path, spawn.stderr_path),
                    )
                timeout = float(
                    timeout_minutes
                    if timeout_minutes is not None
                    else spec.policy.attempt_timeout_minutes
                    if spec.policy.attempt_timeout_minutes is not None
                    else DEFAULT_ATTEMPT_TIMEOUT_MINUTES
                )
                deadline = time.monotonic() + timeout * 60
                while process.poll() is None:
                    drain_streams()
                    if fatal_errors:
                        # The CLI just proved this attempt cannot succeed;
                        # waiting out its own retry ladder buys nothing. The
                        # first typed fatal decides the outcome.
                        _terminate(process)
                        failure = fatal_errors[0]
                        report.outcome = _outcome_for(failure.code)
                        report.error = f"{spec.key}: {failure}"
                        report.detail = failure.details
                        return report
                    if should_stop is not None and should_stop():
                        _terminate(process)
                        raise AttemptCancelled(
                            f"{spec.key}: attempt cancelled by caller"
                        )
                    if time.monotonic() > deadline:
                        _terminate(process)
                        report.outcome = outcomes.TIMEOUT
                        report.error = (
                            f"{spec.key}: timed out after {timeout:g} minutes "
                            f"waiting for {adapter.display_name}"
                        )
                        return report
                    time.sleep(poll_seconds)
                drain_streams()
            finally:
                # Never leak a live agent child: any exit from this block —
                # cancellation, telemetry crash, KeyboardInterrupt — reaps it.
                _terminate(process)

        verdict = validate(workdir) if validate is not None else None

        # A zero exit is not proof of success for every CLI: some exit 0
        # with a failed final turn, so ask the adapter for terminal stream
        # evidence before believing the exit code. Typed fatals collected
        # live take precedence over marker classification of terminal text.
        zero_exit_error: RunnerError | None = None
        if process.returncode == 0:
            terminal_text = adapter.terminal_failure(spawn.stdout_path)
            if terminal_text is not None:
                zero_exit_error = adapter.classify_failure(terminal_text)
            elif fatal_errors:
                zero_exit_error = fatal_errors[0]

        if process.returncode != 0 or zero_exit_error:
            # Output validity beats exit code: a crash during shutdown after
            # the deliverable was written must not burn the completed work.
            if verdict is not None and verdict.valid:
                report.outcome = outcomes.VALID
                report.data = verdict.data
                return report
            if zero_exit_error:
                report.outcome = _outcome_for(zero_exit_error.code)
                report.error = f"{spec.key}: {zero_exit_error}"
                report.detail = zero_exit_error.details
                return report
            report.outcome, report.error, report.detail = _classify_exit(
                adapter, spec, spawn, process.returncode
            )
            return report

        if verdict is None or verdict.valid:
            report.outcome = outcomes.VALID
            report.data = verdict.data if verdict is not None else None
            return report

        # Repair: on invalid_schema, send the project's auto-generated repair
        # message into the still-open session. Fixing one defect can surface
        # the next, so rounds iterate up to the spec's budget.
        if adapter.capabilities.followup and spec.repair_rounds > 0:
            for _ in range(spec.repair_rounds):
                repaired = _repair(
                    adapter, spec, verdict, workdir, spawn.stdout_path,
                    env, emit, poll_seconds, should_stop,
                )
                if not repaired:
                    break  # nothing ran; the workdir is unchanged
                report.repair_rounds_used += 1
                retry_verdict = validate(workdir)
                if retry_verdict.valid:
                    emit(StreamEvent("repair_succeeded", "Repair pass fixed the output"))
                    report.outcome = outcomes.VALID
                    report.data = retry_verdict.data
                    return report
                if retry_verdict.message == verdict.message:
                    break
                verdict = retry_verdict

        report.outcome = outcomes.INVALID_SCHEMA
        report.error = f"{spec.key}: output failed validation: {verdict.message}"
        return report
    finally:
        if process is not None:
            _terminate(process)
        for resource in provisioned:
            try:
                resource.close()
            except Exception:
                pass
        # The CLI is reaped, so its transcript is final: mirror it on every
        # exit path (valid, failed, cancelled) — a lost attempt is exactly
        # the one whose session the next attempt wants.
        if report is not None and report.session_ref:
            push_session(adapter, report.session_ref)
