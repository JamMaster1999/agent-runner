"""The core attempt loop: spawn, stream, classify, repair (agent_runner.md).

One synchronous function, ``run_attempt``: start an agent CLI with an
agent definition and a task message (both arrive already split — rendering
happened upstream), read the live output as it runs, and end the attempt
with exactly one outcome from ``agent_runner.outcomes``. Zero Temporal
imports, zero database, zero business knowledge: retries, receipts, and
workflows are the caller's.

Hygiene rides here too: the CLI child is ALWAYS reaped — on valid exit, on
timeout, on a stall, on cancellation, and on any exception crossing the
loop — so a dead attempt can never leave a live agent burning provider
budget, and heavy memory dies with the process.

Liveness is measured as WORK, not as a running process: a CLI that
neither streams a line nor writes a file under the watched folders for the
stall window is terminated and the attempt ends ``stalled`` (see
``_stall_seconds``). A CLI tree whose resident memory crosses the fuse
(``Policy.rss_limit_mb``) is terminated too — one runaway must never take
the sandbox and every sibling in it.

What crosses the boundary from the project side:

- ``validate`` — the project's contract closure; its ``Verdict`` decides
  ``valid`` vs ``invalid_schema`` and supplies the repair message. Output
  validity beats exit code (preserved core policy).
- ``on_event`` — live ``StreamEvent`` telemetry (progress, tool calls,
  token usage); the Temporal layer forwards it into heartbeat details.
- ``on_session`` — called once with the CLI session ref as soon as the
  stream reveals it, so a caller can persist the resume handle before the
  attempt ends.
- ``on_usage`` — called with the attempt's own running ``Usage`` and the
  session's total each time the stream reports spend, so a caller can show
  them before the attempt ends.
- ``resources`` — registered providers for the spec's declared
  ``resource_specs`` (see ``agent_runner.resources``); their values arrive
  in the task as ``{{RESOURCE:*}}`` template substitutions.
"""

from __future__ import annotations

import dataclasses
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Callable, Sequence

from agent_runner import outcomes, util
from agent_runner.harness import get_adapter
from agent_runner.harness.base import AgentDef, HarnessAdapter
from agent_runner.harness.stream import JsonlTail, StreamEvent, parse_json_dict
from agent_runner.isolation import agent_env
from agent_runner.runtime import AttemptReport, RunnerError, RunSpec, Usage, Verdict
from agent_runner.sessions import RESUME_PREAMBLE
from agent_runner.templates import substitute
from agent_runner.util import write_text
from agent_runner.workdirs import RUNNER_DIR

DEFAULT_ATTEMPT_TIMEOUT_MINUTES = 60.0
REPAIR_TIMEOUT_MINUTES = 15.0
# Fifteen minutes of total silence. Measured across the 60 most recent
# production transcripts, the worst gap between events on a healthy run is
# about 3.6 minutes, so the window is 4x the typical worst: wide enough for
# a slow model call, narrow enough to catch a wedge inside a quarter hour.
DEFAULT_STALL_SECONDS = 900.0
RSS_CHECK_SECONDS = 10.0

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


def _stall_seconds(spec: RunSpec) -> float:
    """The stall window: how long the CLI may produce NOTHING before the
    runner calls it dead. The spec's policy when the caller said, else
    AGENT_RUNNER_STALL_SECONDS, else the default. Only the caller's code
    can disable the watchdog (an explicit ``Policy(stall_seconds=0)``); the
    environment merely tunes the window — zero, negative, or unreadable
    values fall back to the default, so an operator typo can never switch
    the protection off."""
    if spec.policy.stall_seconds is not None:
        return max(float(spec.policy.stall_seconds), 0.0)
    try:
        configured = float(os.environ["AGENT_RUNNER_STALL_SECONDS"])
    except (KeyError, ValueError):
        return DEFAULT_STALL_SECONDS
    return configured if configured > 0 else DEFAULT_STALL_SECONDS


def newest_mtime(roots: Sequence[Path], skip: Sequence[Path] = ()) -> float | None:
    """The newest mtime under ``roots``, or None when the trees are empty
    or unreadable. ``skip`` names folders whose churn is not the agent's
    work (the runner's own files, a browser profile)."""
    skipped = {Path(folder) for folder in skip}
    newest: float | None = None
    frontier = [Path(root) for root in roots if Path(root) not in skipped]
    while frontier:
        folder = frontier.pop()
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    if path in skipped:
                        continue
                    try:
                        mtime = entry.stat(follow_symlinks=False).st_mtime
                    except OSError:
                        continue
                    if newest is None or mtime > newest:
                        newest = mtime
                    if entry.is_dir(follow_symlinks=False):
                        frontier.append(path)
        except OSError:
            continue
    return newest


def tree_rss_mb(pid: int) -> float | None:
    """Resident memory of ``pid`` and every descendant, from ``/proc``;
    None where ``/proc`` says nothing (not Linux, or a kernel that reports
    no VmRSS) — never a zero that reads as "fine"."""
    proc = Path("/proc")
    if not proc.is_dir():
        return None
    parents: dict[int, int] = {}
    rss: dict[int, int] = {}
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            status = (entry / "status").read_text()
        except OSError:
            continue
        fields = dict(
            line.split(":", 1) for line in status.splitlines() if ":" in line
        )
        try:
            parents[int(entry.name)] = int(fields.get("PPid", "0").strip())
            rss[int(entry.name)] = int(fields["VmRSS"].split()[0])
        except (KeyError, ValueError, IndexError):
            continue
    if pid not in rss:
        return None
    total = 0
    frontier = [pid]
    while frontier:
        current = frontier.pop()
        total += rss.get(current, 0)
        frontier.extend(child for child, parent in parents.items() if parent == current)
    return total / 1024


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


class _StampedCapture:
    """The CLI's stdout, copied into the capture file one line at a time
    with the wall clock of its arrival: every JSON line gets a
    ``timestamp`` (the rollout transcripts' key, left alone where the CLI
    already wrote one), everything else passes through verbatim. The
    capture file keeps its shape — the tail, the replay fixtures, and the
    log scanners read it exactly as before — and the dashboard no longer
    has to guess when a line happened.

    The reader always drains the pipe to EOF, so the CLI can never block on
    a full pipe: once the capture file is closed under it (the attempt is
    over, a grandchild still holds stdout) it discards what follows."""

    def __init__(self, pipe: IO[bytes], out: IO[bytes]) -> None:
        self._pipe = pipe
        self._out: IO[bytes] | None = out
        self._thread = threading.Thread(target=self._copy, name="stamped-capture", daemon=True)
        self._thread.start()

    def _copy(self) -> None:
        with self._pipe:
            for raw in self._pipe:
                out = self._out
                if out is None:
                    continue
                line = raw.rstrip(b"\n")
                payload = parse_json_dict(line.decode("utf-8", errors="replace"))
                if payload is not None:
                    payload.setdefault(
                        "timestamp",
                        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    )
                    line = json.dumps(payload).encode()
                try:
                    out.write(line + b"\n")
                    out.flush()
                except ValueError:
                    self._out = None

    def join(self) -> None:
        """Wait for the pipe to drain after the CLI exits — bounded, since a
        grandchild that inherited stdout can hold the pipe open after it,
        and waited for once: a second call after a timeout returns at once."""
        if self._out is None:
            return
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            self._out = None


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
                or event.get("key") != self.spec.key
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


def _classify_exit(adapter: HarnessAdapter, spawn, returncode: int) -> RunnerError:
    """The failure behind a nonzero CLI exit, from CLI-owned error text
    only — never agent transcript tails."""
    detail = adapter.error_report(spawn.stdout_path, spawn.stderr_path)
    return adapter.classify_failure(detail or f"{adapter.display_name} exited {returncode}.")


def _apply(report: AttemptReport, spec: RunSpec, failure: RunnerError) -> AttemptReport:
    """End the attempt with this failure's verdict."""
    report.outcome = _outcome_for(failure.code)
    report.error = f"{spec.key}: {failure}"
    report.detail = failure.details
    report.resets_at = failure.resets_at
    return report


def _repair(
    adapter: HarnessAdapter,
    spec: RunSpec,
    verdict: Verdict,
    workdir: Path,
    stdout_path: Path,
    prompt_path: Path,
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
        write_text(prompt_path, message)
        # Append, not truncate: repair rounds share these paths, and a
        # multi-round failure must keep every round's log for debugging.
        with (
            prompt_path.open("rb") as stdin,
            followup.stdout_path.open("ab") as stdout,
            followup.stderr_path.open("a") as stderr,
        ):
            process = subprocess.Popen(
                followup.command,
                cwd=util.project_root(),
                env=env,
                stdin=stdin,
                preexec_fn=_pdeathsig(),
                stdout=subprocess.PIPE,
                stderr=stderr,
            )
            capture = _StampedCapture(process.stdout, stdout)
            try:
                deadline = time.monotonic() + REPAIR_TIMEOUT_MINUTES * 60
                while process.poll() is None:
                    if should_stop is not None and should_stop():
                        raise AttemptCancelled(f"{spec.key}: repair cancelled by caller")
                    if time.monotonic() > deadline:
                        return False
                    time.sleep(poll_seconds)
            finally:
                _terminate(process)
                capture.join()
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
    on_usage: Callable[[Usage, Usage], None] | None = None,
    session_ref: str | None = None,
    session_usage: Usage | None = None,
    run_id: str = "",
    attempt: int = 1,
    variables: dict[str, str] | None = None,
    resources: dict[str, Any] | None = None,
    timeout_minutes: float | None = None,
    poll_seconds: float = 2.0,
    should_stop: Callable[[], bool] | None = None,
    watch_dirs: Sequence[Path] = (),
) -> AttemptReport:
    """Run one CLI attempt and end it with exactly one outcome.

    ``task`` is the rendered task message; the only substitution performed
    here is the closed run-varying set (``{{RUNNER_*}}`` and
    ``{{RESOURCE:*}}`` tokens — values that cannot exist until attempt
    start). ``session_ref`` resumes that session instead of starting fresh
    (the resume preamble is prepended; ``spec.policy.resume_preamble``
    overrides the default text); ``session_usage`` is where that session
    stood before this attempt (the prior attempt's ``report.session_usage``),
    so ``report.session_usage`` can carry the session's whole total.
    ``should_stop`` polled true terminates the CLI and raises
    ``AttemptCancelled``. A CLI that neither streams nor writes a file
    under the workdir or ``watch_dirs`` for the stall window is terminated
    too, ending the attempt ``stalled``.

    Configuration errors (an unknown harness, an unrenderable agent, a
    malformed template) RAISE — they are caller bugs, not attempt outcomes.
    A missing CLI binary is an attempt outcome (``spawn_failure``): another
    worker may have it.
    """
    adapter = get_adapter(spec.harness)
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    usage = Usage()
    session_total = Usage()
    before = Usage()

    def emit(event: StreamEvent) -> None:
        if any(getattr(event, name) is not None for name in Usage.names()):
            usage.add_event(event)
            session_total.assign(before + usage)
            if on_usage is not None:
                try:
                    on_usage(dataclasses.replace(usage), dataclasses.replace(session_total))
                except Exception:
                    pass
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
        if session_ref and not adapter.session_present(session_ref):
            # Nothing to reopen on this host: run fresh rather than spend
            # the attempt on a resume that cannot land.
            print(
                f"WARNING: {adapter.session_noun} {session_ref} is not in this "
                f"host's {adapter.display_name} home; running fresh.",
                file=sys.stderr,
            )
            session_ref = None
        if session_ref:
            preamble = spec.policy.resume_preamble
            prompt = (RESUME_PREAMBLE if preamble is None else preamble) + prompt
            if session_usage is not None:
                before.assign(session_usage)
                session_total.assign(session_usage)

        try:
            if session_ref:
                spawn = adapter.build_resume(spec, workdir, session_ref)
            else:
                spawn = adapter.build_spawn(spec, workdir)
        except RunnerError as exc:
            if exc.code == "missing_command":
                return _spawn_report(spec, str(exc), exc.details)
            raise

        # Runner-private files live under .runner/, outside the agent's
        # output namespace, so a prompt file can never shadow an artifact.
        prompt_path = workdir / RUNNER_DIR / "prompt.md"
        write_text(prompt_path, prompt)
        env = agent_env(adapter, spec, run_id, attempt, workdir)
        env.update(adapter.bind_credentials())
        env.update(adapter.env_overrides())

        report = AttemptReport(
            outcome=outcomes.INFRA,
            session_ref=session_ref,
            usage=usage,
            session_usage=session_total,
            resumed=bool(session_ref),
            workdir=workdir,
        )
        stream_tail = JsonlTail(spawn.stdout_path)
        stream_parser = adapter.stream_parser()
        hook_drain = _HookDrain(adapter, spec, run_id, attempt)

        fatal_errors: list[RunnerError] = []

        def drain_streams() -> bool:
            """Consume everything the CLI produced since the last pass;
            True when that was anything at all — the watchdog's proof that
            the agent is still producing, not merely running."""
            lines = stream_tail.read_new_lines()
            for line in lines:
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
                            if live_ref != session_ref:
                                # The CLI opened a session other than the one
                                # asked for: its usage starts from nothing.
                                before.assign(Usage())
                                session_total.assign(usage)
                            if on_session is not None:
                                try:
                                    on_session(live_ref)
                                except Exception:
                                    pass
                for event in stream_parser.parse_line(line):
                    emit(event)
            hook_events = hook_drain.drain()
            for event in hook_events:
                emit(event)
            return bool(lines or hook_events)

        # The prompt reaches the CLI as a file on stdin: the kernel feeds it,
        # so a CLI that never drains stdin wedges nothing on this side.
        with (
            prompt_path.open("rb") as stdin,
            spawn.stdout_path.open("wb") as stdout,
            spawn.stderr_path.open("w") as stderr,
        ):
            try:
                process = subprocess.Popen(
                    spawn.command,
                    cwd=util.project_root(),
                    env=env,
                    stdin=stdin,
                    preexec_fn=_pdeathsig(),
                    stdout=subprocess.PIPE,
                    stderr=stderr,
                )
            except OSError as exc:
                # The OS refused the fork/exec, or the binary vanished: no
                # CLI ever started, so this says nothing about the job.
                return _spawn_report(
                    spec, f"failed to spawn {adapter.display_name}: {exc}"
                )
            capture = _StampedCapture(process.stdout, stdout)
            try:
                timeout = float(
                    timeout_minutes
                    if timeout_minutes is not None
                    else spec.policy.attempt_timeout_minutes
                    if spec.policy.attempt_timeout_minutes is not None
                    else DEFAULT_ATTEMPT_TIMEOUT_MINUTES
                )
                deadline = time.monotonic() + timeout * 60
                stall_seconds = _stall_seconds(spec)
                last_output = time.monotonic()
                # The agent's work, not the runner's own files or a
                # browser profile rewriting itself.
                watched = (workdir, *watch_dirs)
                unwatched = (
                    workdir / RUNNER_DIR,
                    spawn.stdout_path,
                    spawn.stderr_path,
                    *(resource.scratch() for resource in provisioned),
                )
                last_write = newest_mtime(watched, unwatched)
                next_rss_check = time.monotonic() + RSS_CHECK_SECONDS
                while process.poll() is None:
                    if drain_streams():
                        last_output = time.monotonic()
                    if fatal_errors:
                        # The CLI just proved this attempt cannot succeed;
                        # waiting out its own retry ladder buys nothing. The
                        # first typed fatal decides the outcome.
                        _terminate(process)
                        return _apply(report, spec, fatal_errors[0])
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
                    if spec.policy.rss_limit_mb and time.monotonic() > next_rss_check:
                        next_rss_check = time.monotonic() + RSS_CHECK_SECONDS
                        resident = tree_rss_mb(process.pid)
                        if resident is not None and resident > spec.policy.rss_limit_mb:
                            _terminate(process)
                            report.outcome = outcomes.INFRA
                            report.error = (
                                f"{spec.key}: memory fuse — the {adapter.display_name} "
                                f"process tree reached {resident:.0f} MB "
                                f"(limit {spec.policy.rss_limit_mb} MB)"
                            )
                            return report
                    if stall_seconds and time.monotonic() - last_output > stall_seconds:
                        # Silent is not the same as dead: a long shell
                        # command streams nothing and still writes files.
                        # Any write under the watched folders since the
                        # last look is proof of work and restarts the clock.
                        written = newest_mtime(watched, unwatched)
                        if written is not None and (last_write is None or written > last_write):
                            last_write = written
                            last_output = time.monotonic()
                            continue
                        # Alive is not producing: a CLI whose helper died
                        # under it holds its process open and streams
                        # nothing, and a heartbeat that watches the process
                        # calls that healthy until the caller's multi-hour
                        # backstop. Silence for the window ends the attempt
                        # now, so the retry gets a fresh CLI in minutes.
                        _terminate(process)
                        report.outcome = outcomes.STALLED
                        report.error = (
                            f"{spec.key}: {adapter.display_name} produced no output "
                            f"and wrote no file for {stall_seconds:g} seconds while still running"
                        )
                        report.detail = adapter.error_report(
                            spawn.stdout_path, spawn.stderr_path
                        )
                        return report
                    time.sleep(poll_seconds)
            finally:
                # Never leak a live agent child: any exit from this block —
                # cancellation, telemetry crash, KeyboardInterrupt — reaps it.
                # Then let its last lines land and read them, on every path.
                _terminate(process)
                capture.join()
                drain_streams()

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
            return _apply(
                report,
                spec,
                zero_exit_error or _classify_exit(adapter, spawn, process.returncode),
            )

        if verdict is None or verdict.valid:
            report.outcome = outcomes.VALID
            report.data = verdict.data if verdict is not None else None
            return report

        # Repair: on invalid_schema, send the project's auto-generated repair
        # message into the still-open session. Fixing one defect can surface
        # the next, so rounds iterate up to the spec's budget.
        if adapter.capabilities.followup and spec.repair_rounds > 0:
            for round_number in range(1, spec.repair_rounds + 1):
                repaired = _repair(
                    adapter, spec, verdict, workdir, spawn.stdout_path,
                    workdir / RUNNER_DIR / f"repair-{round_number}.md",
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
