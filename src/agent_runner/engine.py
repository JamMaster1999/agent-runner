"""Generic attempt engine: one loop for every harness (design doc §3).

Phase-2 step 5 collapsed the twin per-harness attempt loops into
run_agent_job_once(adapter, ...); run_with_retries dispatches through the
harness registry on the job's harness. Every provider difference — markers,
error-report dialects, session extraction, hook conversion, spawn command
shapes, env quirks, health commands — lives in the adapter modules under
agent_runner/harness/. The engine holds only judgment: claim/retry policy,
validation gating, and 'output validity beats exit code'.

Phase-2 step 7 layered the structured-outcome contract on top (design doc
§6, D5): probes report ProbeReport verdicts instead of raising, and the
POLICY table below is the only place retry decisions live.

Step-5 retype (extraction plan §4 item 5): the engine speaks the generic
``RunnerJob``/``RunnerError`` only. Everything pipeline-shaped arrives as
data (the submit spec on the job) or as facade-built closures: ``probes``
(primary/review validation), ``hooks`` (attempt-start seeding), ``notifier``
(operator alerts), ``resources`` (declared resource providers, e.g. the
phase-3 CDP browser). No GTM chain-module import (prompts/probes/
artifacts/cdp) remains.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from agent_runner import outcomes
from agent_runner.attempts import (
    RESUME_PREAMBLE,
    claim_resumable_attempt,
    record_attempt_outcome,
    record_attempt_session,
    record_attempt_start,
    resume_prompt_fingerprint,
    unconsume_attempt,
)
from agent_runner.events import run_job_event
from agent_runner.outcomes import ProbeReport
from agent_runner.harness import get_adapter
from agent_runner.harness.base import HarnessAdapter
from agent_runner.jobstore import (
    HEARTBEAT_SECONDS,
    claim_job,
    job_heartbeat,
    mark_blocked,
    mark_retry,
    poll_heartbeat,
)
from agent_runner.runtime import (
    AttemptResult,
    RunnerConfig,
    RunnerError,
    RunnerJob,
    attempt_dir,
    attempt_output_path,
    project_id,
)
from agent_runner.templates import substitute
from agent_runner import util
from agent_runner.util import db_rows, write_text
from agent_runner.harness.stream import JsonlTail, StreamEvent

# ---------------------------------------------------------------------------
# §6 policy — the ONLY place retry decisions live (design doc §6, D5).
#
# Everything below this block supplies evidence (probe verdicts, adapter
# classifications, carrier RunnerError fields for the facade's chain-control
# wrap); the decision — succeed / retry / block / halt, and what each retry
# costs — always comes from one POLICY lookup.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyDecision:
    """What the engine does with one classified failure signal.

    ``consumes_attempt`` False (D5) leaves the job's remaining max_attempts
    untouched: the failure says nothing about the job. Such retries are
    bounded by ``retry_cap``, an in-process counter — no DDL tonight, the
    pipeline_jobs columns are unchanged. ``consumes_resume_budget`` False
    means the retry must never spend a session resume; for infrastructure
    failures this is enforced structurally — the re-probe happens inside the
    still-running attempt and never re-claims a session."""

    action: str                       # 'succeed' | 'retry' | 'block' | 'halt'
    consumes_attempt: bool = True
    consumes_resume_budget: bool = True
    repair_eligible: bool = False
    retry_cap: int | None = None      # separate bounded cap, outside max_attempts (D5)
    alert: bool = False


# Engine-internal signals for the §6 rows that are conditions rather than
# probe verdicts or adapter error codes.
ATTEMPTS_EXHAUSTED = "attempts_exhausted"
# Client chain-control failures (packet build, unhandled exceptions, DB
# failures flagged alert) still cross the engine tonight; they block exactly
# as before the contract and leave for the pipeline manager at extraction.
CHAIN_TERMINAL = "chain_terminal"


POLICY: dict[str, PolicyDecision] = {
    # Probe verdicts (client vocabulary, §6). A valid output succeeds even on
    # nonzero exit — output validity beats exit code, preserved as core policy.
    outcomes.VALID: PolicyDecision(action="succeed"),
    # Schema/evidence defects: repair round if the adapter has followup and
    # the job has repair budget, else retry — either way an attempt is spent.
    outcomes.INVALID_SCHEMA: PolicyDecision(action="retry", repair_eligible=True),
    outcomes.MISSING_EVIDENCE: PolicyDecision(action="retry", repair_eligible=True),
    # D5: the probe's own infrastructure broke — retry under a separate
    # bounded cap, outside max_attempts, never consuming resume budget (the
    # session is fine).
    outcomes.INFRASTRUCTURE_FAILURE: PolicyDecision(
        action="retry", consumes_attempt=False, consumes_resume_budget=False, retry_cap=3
    ),
    # D5: adapter-detected provider throttling says nothing about the job —
    # bounded retry without consuming an attempt.
    outcomes.RATE_LIMITED: PolicyDecision(action="retry", consumes_attempt=False, retry_cap=3),
    # Adapter terminal proof from CLI-owned text: block + alert.
    outcomes.AUTH: PolicyDecision(action="block", alert=True),
    outcomes.BILLING: PolicyDecision(action="block", alert=True),
    outcomes.INVALID_INVOCATION: PolicyDecision(action="block", alert=True),
    outcomes.BUDGET: PolicyDecision(action="block", alert=True),
    # Timeout / spawn failure / unproven nonzero exit: default-retryable with
    # backoff (the phase-1 decision, preserved).
    outcomes.TIMEOUT: PolicyDecision(action="retry"),
    outcomes.SPAWN_FAILURE: PolicyDecision(action="retry"),
    outcomes.PROBE_TIMEOUT: PolicyDecision(action="retry"),
    outcomes.UNKNOWN: PolicyDecision(action="retry"),
    # Operator cancel: terminal, audited, no retry.
    outcomes.CANCELLED: PolicyDecision(action="halt"),
    # Attempts exhausted: block + alert (requeue resets the budget and
    # preserves sessions).
    ATTEMPTS_EXHAUSTED: PolicyDecision(action="block", alert=True),
    CHAIN_TERMINAL: PolicyDecision(action="block", alert=True),
}


# Recorded failure codes -> §6 signals. Adapter marker codes keep their
# historical names in events and attempt rows (billing_or_credits,
# health_budget_too_low); this map is how they reach the policy table.
SIGNAL_BY_CATEGORY: dict[str, str] = {
    "auth": outcomes.AUTH,
    "billing_or_credits": outcomes.BILLING,
    "invalid_invocation": outcomes.INVALID_INVOCATION,
    "health_budget_too_low": outcomes.BUDGET,
    "rate_limited": outcomes.RATE_LIMITED,
    "timeout": outcomes.TIMEOUT,
    "spawn_failure": outcomes.SPAWN_FAILURE,
    "probe_timeout": outcomes.PROBE_TIMEOUT,
    "cancelled": outcomes.CANCELLED,
    # Transient DB stalls during an attempt's bookkeeping (event append,
    # attempt-store write) are evidence about the DATABASE, not the job —
    # same D5 row as broken probe infrastructure: bounded free retry, no
    # attempt or resume budget spent (2026-08-03 incident: these blocked
    # healthy jobs terminally on attempt 1).
    "db_timeout": outcomes.INFRASTRUCTURE_FAILURE,
    "job_event_transient": outcomes.INFRASTRUCTURE_FAILURE,
}

# Bounded retries for a transiently stalled claim write, mirroring the
# INFRASTRUCTURE_FAILURE retry cap (D5: infrastructure failures never spend
# the job's own budget).
CLAIM_STALL_RETRIES = 3

# Attempt timeout when the submitter set none (policy["attempt_timeout_minutes"]).
DEFAULT_ATTEMPT_TIMEOUT_MINUTES = 60

# Once-per-process visibility for the emit-DSN privilege fallback.
_EMIT_DSN_FALLBACK_WARNED = False


class ProbeFailureError(RunnerError):
    """A non-valid ProbeReport crossing into the retry machinery.

    Carries the report so ``policy_signal`` routes on the probe's verdict.
    The code stays the caller-vocabulary outcome code for event/DB parity,
    and the carrier retryable flag serves the facade's chain-control wrap
    only — the engine itself decides through POLICY."""

    def __init__(self, report: ProbeReport) -> None:
        super().__init__(
            report.message,
            code=report.outcome_code or report.verdict,
            retryable=True,
            details=report.details,
        )
        self.report = report


def policy_signal(failure: RunnerError) -> str:
    """Map one failure to its §6 signal — evidence in, signal out; the
    decision itself always comes from one POLICY lookup."""
    if isinstance(failure, ProbeFailureError):
        return failure.report.verdict
    signal = SIGNAL_BY_CATEGORY.get(failure.code)
    if signal is not None:
        return signal
    if failure.code.endswith("_stdin"):
        # Adapter stdin failures ('<harness>_stdin'): the CLI died before
        # receiving the prompt — a spawn failure in §6 vocabulary.
        return outcomes.SPAWN_FAILURE
    if not failure.retryable or failure.alert:
        # Chain-control raises carry their own terminal proof; they block
        # exactly as before the contract.
        return CHAIN_TERMINAL
    return outcomes.UNKNOWN


def dispatch_alert(
    notifier: Callable[[str, str], None] | None, message: str, severity: str = "error"
) -> None:
    """POLICY alert=True facts go to the registered notifier callback; with
    none registered the fact still lands on stderr rather than vanishing."""
    if notifier is None:
        print(f"WARNING: no notifier registered for alert: {message[:500]}", file=sys.stderr)
        return
    try:
        notifier(message, severity)
    except Exception as exc:  # advisory: alerting must never kill the run
        print(f"WARNING: notifier callback failed ({exc!r}): {message[:200]}", file=sys.stderr)


def filtered_hook_events(
    event_log: Path,
    run_id: str,
    job: RunnerJob,
    attempt: int,
) -> list[dict[str, Any]]:
    try:
        lines = event_log.read_text().splitlines()
    except FileNotFoundError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            event.get("run_id") == run_id
            and event.get("job_stable_id") == job.key
            and int(event.get("attempt") or -1) == attempt
        ):
            events.append(event)
    return events


def record_hook_progress(
    args: argparse.Namespace,
    adapter: HarnessAdapter,
    run_id: str,
    job: RunnerJob,
    attempt: int,
    seen_hooks: set[str],
) -> None:
    """Drain new captured hook events, normalized per the adapter's dialect."""
    for event in filtered_hook_events(adapter.hook_event_log(), run_id, job, attempt):
        event_key = json.dumps(event, sort_keys=True)
        if event_key in seen_hooks:
            continue
        seen_hooks.add(event_key)
        normalized = adapter.normalize_hook_event(event, job.agent_ref)
        if normalized is None:
            continue
        kind, message = normalized
        run_job_event(
            args.database_url,
            "progress",
            job,
            message,
            attempt=attempt,
            event_name=kind,
            fatal=False,
        )


def record_stream_progress(
    args: argparse.Namespace,
    job: RunnerJob,
    attempt: int,
    tail: JsonlTail,
    parser: Any,
) -> None:
    """Drain new CLI stdout lines and append them as one batched DB update."""
    events: list[StreamEvent] = []
    for line in tail.read_new_lines():
        events.extend(parser.parse_line(line))
    if not events:
        return
    run_job_event(
        args.database_url,
        "progress",
        job,
        None,
        attempt=attempt,
        batch=[
            {
                "event": event.event,
                "message": event.message,
                "current": event.current,
                "total": event.total,
                "tok_input": event.tok_input,
                "tok_cache_write": event.tok_cache_write,
                "tok_cache_read": event.tok_cache_read,
                "tok_output": event.tok_output,
                "cost_usd": event.cost_usd,
            }
            for event in events
        ],
        fatal=False,
    )


class AttemptValidationGate:
    """Throttle in-loop output validation while an agent is still running.

    Re-validating every poll tick is wasteful and can be outright harmful:
    ``expensive`` jobs (probe_spec data — today's phase 3/4/6 spawn a
    validator subprocess, a dry-run import, or a full URL re-sweep) only
    re-validate at most once per MIN_INTERVAL_SECONDS. Validation only
    re-runs when the output file (or the review sibling the facade's
    ``review_watch`` closure names) actually changed.
    """

    MIN_INTERVAL_SECONDS = 60.0

    def __init__(self, out_path: Path, review_path: Path | None, expensive: bool) -> None:
        self.out_path = out_path
        self.review_path = review_path
        self.expensive = expensive
        self.last_signature: tuple[Any, ...] | None = None
        self.last_validated_at = 0.0

    def _signature(self) -> tuple[Any, ...]:
        def stat_signature(path: Path | None) -> tuple[Any, ...]:
            if path is None:
                return ()
            try:
                stat = path.stat()
            except OSError:
                return (None,)
            return (stat.st_mtime_ns, stat.st_size)

        return (stat_signature(self.out_path), stat_signature(self.review_path))

    def should_validate(self) -> bool:
        signature = self._signature()
        if signature == self.last_signature:
            return False
        if self.expensive and time.monotonic() - self.last_validated_at < self.MIN_INTERVAL_SECONDS:
            # Changed but rate-limited: leave last_signature stale so the next
            # tick past the interval picks the change up.
            return False
        self.last_signature = signature
        self.last_validated_at = time.monotonic()
        return True


def maybe_validate_attempt(
    output_path: Path,
    probes: dict[str, Callable[..., Any]],
    attempt: int,
) -> AttemptResult | None:
    """A valid attempt result when the primary probe (and the review probe,
    for jobs that carry one) passes; None otherwise."""
    report = probes["primary"](output_path, attempt)
    if report.verdict != outcomes.VALID:
        return None
    review = probes.get("review")
    if review is not None and review(output_path).verdict != outcomes.VALID:
        return None
    return AttemptResult(path=output_path, data=report.data or {}, attempt=attempt)


# Environment names an agent process may inherit from the engine regardless
# of harness: baseline shell/OS plumbing plus TLS/proxy configuration. Names
# holding secrets (DSNs, API keys) are NOT here — a job that needs one
# declares it in required_env, an adapter that needs one lists it in
# env_passthrough(), and the operator can extend via
# RUNNER_AGENT_ENV_PASSTHROUGH (comma-separated names).
AGENT_ENV_SAFE_NAMES = (
    "PATH",
    "HOME",
    "SHELL",
    "USER",
    "LOGNAME",
    "TERM",
    "TMPDIR",
    "TZ",
    "PYTHONPATH",
    # Runner state override: hook processes must write where the engine
    # reads, or all hook telemetry silently lands in the wrong tree.
    "RUNNER_STATE_DIR",
    # Container/sandbox marker some CLIs require to accept elevated
    # permission modes when running as root (e.g. claude bypassPermissions
    # in a Modal container). A marker, not a secret.
    "IS_SANDBOX",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
)
AGENT_ENV_SAFE_PREFIXES = (
    "LANG",
    "LC_",
    "XDG_",
    "SSL_CERT",
    "REQUESTS_CA",
    "CURL_CA",
    "NODE_EXTRA_CA",
)


def agent_base_env(adapter: HarnessAdapter, job: RunnerJob) -> dict[str, str]:
    """The inherited half of the agent environment.

    Filtered by default: the engine's own environment carries operator
    secrets (database DSNs, provider keys, unrelated project variables) that
    agents with shell access could read — so agents get a safe baseline plus
    exactly what the job declared (``required_env``), what the adapter needs
    for its CLI's auth (``env_passthrough``), and any operator-listed extras
    (RUNNER_AGENT_ENV_PASSTHROUGH). RUNNER_AGENT_ENV=inherit restores the
    historical full-copy behavior."""
    if os.environ.get("RUNNER_AGENT_ENV") == "inherit":
        return os.environ.copy()
    allowed = set(AGENT_ENV_SAFE_NAMES)
    allowed.update(job.required_env)
    allowed.update(adapter.env_passthrough())
    extra = os.environ.get("RUNNER_AGENT_ENV_PASSTHROUGH", "")
    allowed.update(name.strip() for name in extra.split(",") if name.strip())
    return {
        name: value
        for name, value in os.environ.items()
        if name in allowed or name.startswith(AGENT_ENV_SAFE_PREFIXES)
    }


def agent_env(
    adapter: HarnessAdapter,
    run_id: str,
    job: RunnerJob,
    attempt: int,
    output_path: Path,
    database_url: str,
) -> dict[str, str]:
    """The environment stamped onto agent CLI (and hook) processes.

    RUNNER_* is the runner-native attribution set read by `agent-runner
    emit` and the hook capture; the legacy UFLO_* names are co-emitted for
    one release, then removed. RUNNER_EMIT_DSN carries the restricted
    ``runner_emitter`` DSN when the engine's own environment provides one
    (step 10.5 — agents get INSERT-on-events and nothing else), else the
    engine's database url, so the emit CLI never needs the DSN on argv.
    RUNNER_GROUP_KEY rides alongside because the INSERT-only emit path can
    no longer look the group up from the jobs row. PYTHONPATH
    gets the runner package's src dir prepended so `python3 -m agent_runner
    ...` works inside agent shells and hook processes without a pip
    install. RUNNER_PYTHON carries the engine's own interpreter — the one
    proven to have psycopg — so the CLI's process entry can re-exec onto it
    when the shell's bare `python3` lacks the driver (otherwise every
    in-agent emit would take the advisory exit-0 path and lose its row)."""
    import agent_runner

    emit_dsn = os.environ.get("RUNNER_EMIT_DSN")
    if not emit_dsn:
        # Falling back to the engine's full-privilege store DSN keeps emits
        # working, but hands every agent shell a DSN that can do far more
        # than INSERT events. Warn once per process so a deployment missing
        # the restricted emitter DSN is visible, not silent.
        global _EMIT_DSN_FALLBACK_WARNED
        if not _EMIT_DSN_FALLBACK_WARNED:
            _EMIT_DSN_FALLBACK_WARNED = True
            print(
                "WARNING: RUNNER_EMIT_DSN is not set; agent environments "
                "receive the engine's full store DSN. Provision the "
                "restricted runner_emitter DSN for this deployment.",
                file=sys.stderr,
            )
        emit_dsn = database_url

    env = agent_base_env(adapter, job)
    env.update(
        {
            "UFLO_RUN_ID": run_id,
            "UFLO_JOB_STABLE_ID": job.key,
            "UFLO_ATTEMPT": str(attempt),
            "UFLO_OUTPUT_PATH": str(output_path),
            "UFLO_AGENT_NAME": job.agent_ref,
            "UFLO_PHASE": job.task_type,
            "UFLO_BACKEND": job.harness,
            "RUNNER_RUN_ID": run_id,
            "RUNNER_JOB_KEY": job.key,
            "RUNNER_ATTEMPT": str(attempt),
            "RUNNER_OUTPUT_PATH": str(output_path),
            "RUNNER_AGENT_NAME": job.agent_ref,
            "RUNNER_PHASE": job.task_type,
            "RUNNER_BACKEND": job.harness,
            "RUNNER_GROUP_KEY": job.group_key,
            "RUNNER_PROJECT_ID": project_id(),
            "RUNNER_EMIT_DSN": emit_dsn,
            "RUNNER_PYTHON": sys.executable,
            "AGENT_RUNNER_PROJECT_ROOT": str(util.project_root()),
        }
    )
    package_src = str(Path(agent_runner.__file__).resolve().parents[1])
    env["PYTHONPATH"] = package_src + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    return env


def template_from_submit_spec(job: Any) -> str | None:
    """The pre-substitution prompt template carried by the submitted job
    (``prompt_template``/``prompt_sha256`` from SubmitRequest.prompt_ref);
    None when the job carries no template. The digest, when present, MUST be
    resume_prompt_fingerprint of the template text — the submitted bytes ARE
    the resume identity (D2) — so a mismatch is a client-side build bug
    surfaced loudly as invalid_submit rather than silently re-hashed (which
    could orphan every existing session)."""
    template = getattr(job, "prompt_template", None)
    if not template:
        return None
    expected = getattr(job, "prompt_sha256", None)
    if expected and expected != resume_prompt_fingerprint(template):
        raise RunnerError(
            "Submitted prompt_ref sha256 does not match its template bytes; "
            "the digest must be resume_prompt_fingerprint(template).",
            code="invalid_submit",
            retryable=False,
            alert=False,
        )
    return template


def runner_variables(
    run_id: str,
    job_key: str,
    attempt: int,
    directory: Path,
    resource_variables: dict[str, str] | None = None,
) -> dict[str, str]:
    """Substitution values for the D2 closed variable set, bound at attempt
    start. RUNNER_RUN_ID carries the run id and RUNNER_JOB_KEY the job's own
    key — each token now substitutes exactly what its name promises (the
    historical RUNNER_JOB_KEY-carries-the-run-id aliasing is gone).
    RUNNER_OUTPUT_PATH is the attempt's output directory; templates append
    their own artifact filenames. ``resource_variables`` is the
    provisioned-resource overlay the attempt loop computes: every registered
    provider's null variables, overlaid by the live resources' values — so
    resource tokens are always supplied (null when nothing was launched),
    exactly as before the retype."""
    variables = {
        "RUNNER_ATTEMPT": str(attempt),
        "RUNNER_RUN_ID": run_id,
        "RUNNER_JOB_KEY": job_key,
        "RUNNER_OUTPUT_PATH": str(directory),
    }
    variables.update(resource_variables or {})
    return variables


def repair_attempt(
    args: argparse.Namespace,
    adapter: HarnessAdapter,
    job: RunnerJob,
    run_id: str,
    run_dir: Path,
    attempt: int,
    out_path: Path,
    stdout_path: Path,
    report: ProbeReport,
    probes: dict[str, Callable[..., Any]],
) -> AttemptResult | None:
    """Message the attempt's own session to fix a validation defect in place
    instead of burning a full re-run. The session already holds the research
    context, so a repair costs seconds. The follow-up message is the probe's
    own repair_message (§6); any failure here returns None and the normal
    retry machinery takes over."""
    message = report.repair_message
    if not message:
        return None
    session_ref = adapter.session_ref_from_log(stdout_path)
    if not session_ref:
        return None
    directory = attempt_dir(run_dir, job, attempt)
    followup = adapter.build_followup(job, directory, session_ref)
    if followup is None:
        return None
    run_job_event(
        args.database_url,
        "progress",
        job,
        f"Output failed validation ({report.outcome_code}); messaging the "
        f"{adapter.display_name} session to repair",
        attempt=attempt,
    )
    try:
        with followup.stdout_path.open("w") as stdout, followup.stderr_path.open("w") as stderr:
            process = subprocess.Popen(
                followup.command,
                cwd=util.project_root(),
                env=agent_env(adapter, run_id, job, attempt, out_path, args.database_url),
                stdin=subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                text=True,
            )
            if process.stdin is None:
                return None
            try:
                process.stdin.write(message)
                process.stdin.close()
            except BrokenPipeError:
                return None
            deadline = time.monotonic() + 15 * 60
            last_beat = time.monotonic()
            while process.poll() is None:
                last_beat = poll_heartbeat(args, job, process, last_beat)
                if time.monotonic() > deadline:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    return None
                time.sleep(args.poll_seconds)
    except OSError:
        return None
    repaired = probes["primary"](out_path, attempt)
    if repaired.verdict != outcomes.VALID:
        run_job_event(
            args.database_url,
            "progress",
            job,
            "Repair pass did not produce valid output; falling back to a full retry",
            attempt=attempt,
        )
        return None
    run_job_event(
        args.database_url,
        "progress",
        job,
        "Repair pass fixed the output; accepting the attempt",
        attempt=attempt,
    )
    return AttemptResult(path=out_path, data=repaired.data or {}, attempt=attempt)


def run_agent_job_once(
    adapter: HarnessAdapter,
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    run_dir: Path,
    attempt: int,
    resume_state: dict[str, Any] | None = None,
    *,
    template: str | None = None,
    probes: dict[str, Callable[..., Any]] | None = None,
    hooks: dict[str, Callable[..., Any]] | None = None,
    resources: dict[str, Any] | None = None,
) -> AttemptResult:
    # resume_state is the caller's view of this attempt: attempt_id (the
    # attempts row this launch owns — migration 007's identity), candidate_id
    # + directory when the DB claim won, session_recorded once the resumed CLI
    # proved it opened a session. run_with_retries reads it to close the
    # attempt out and to release a claim the attempt consumed but never used.
    #
    # template is the D2 substitution contract's input: the job's
    # PRE-substitution prompt template exactly as submitted (the text behind
    # SubmitRequest.prompt_ref["template"]; prompt_ref["sha256"] must equal
    # resume_prompt_fingerprint of it). Run-varying values appear only as
    # {{RUNNER_*}}/{{RESOURCE:*}} tokens; the engine fingerprints the
    # template AS RECEIVED and substitutes runner_variables at attempt start,
    # so resume identity is decided by the submitted bytes.
    if resume_state is None:
        resume_state = {}
    probes = probes or {}
    hooks = hooks or {}
    resources = resources or {}
    out_path = attempt_output_path(run_dir, job, attempt)
    directory = attempt_dir(run_dir, job, attempt)
    on_attempt_start = hooks.get("on_attempt_start")
    if on_attempt_start is not None:
        on_attempt_start(run_dir, attempt)
    provisioned: list[Any] = []
    prompt_path = directory / "prompt.md"
    try:
        # Declared resources (D4): the client registered a provider per kind;
        # provision/teardown sequencing around the attempt is the engine's.
        for spec in job.resource_specs:
            provider = resources.get(spec.get("kind"))
            if provider is not None:
                provisioned.append(provider.provision(job.key, attempt, directory))
        # Resource substitution values: every registered provider's null
        # variables, overlaid by what was actually provisioned — resource
        # tokens are always supplied (null when nothing launched).
        resource_variables: dict[str, str] = {}
        for provider in resources.values():
            null_variables = getattr(provider, "null_variables", None)
            if null_variables is not None:
                resource_variables.update(null_variables())
        for resource in provisioned:
            resource_variables.update(resource.variables())

        if template is None:
            raise RunnerError(
                f"{job.key} has no prompt template; submit prompt_ref before running.",
                code="invalid_submit",
                retryable=False,
                alert=False,
            )

        # The fingerprint hashes the PRE-substitution template: run-varying
        # values are still {{RUNNER_*}}/{{RESOURCE:*}} tokens there, so resume
        # identity is invariant across runs/attempts by construction (D2).
        fingerprint = resume_prompt_fingerprint(template)
        prompt = substitute(
            template,
            runner_variables(run_id, job.key, attempt, directory, resource_variables),
        )

        # Recorded BEFORE the claim: the resume chain is a self-FK, so the
        # consuming statement needs this attempt's row to exist (007). The
        # row is the attempt's identity from here on — attempt numbers repeat
        # across runs and key nothing.
        attempt_id = record_attempt_start(args, job, run_id, attempt, fingerprint, directory)
        resume_state["attempt_id"] = attempt_id

        resume_session: tuple[str, Path] | None = None
        if not args.force_rerun and bool(job.policy.get("resume")):
            # The pipeline_attempts store is the ONLY source of resume rights
            # (design §7.5): the legacy filesystem matchers were deleted at
            # extraction step 4, once every session worth having was
            # DB-tracked. The claim path is pinned by
            # tests/test_resume_claim_sql.py; a None claim means a fresh
            # session, full stop.
            claimed = claim_resumable_attempt(
                args, job, attempt_id, fingerprint, run_id, attempt
            )
            if claimed is not None:
                claimed_session, claimed_dir, claimed_id = claimed
                resume_session = (claimed_session, claimed_dir)
                resume_state["candidate_id"] = claimed_id
                resume_state["directory"] = claimed_dir
        if resume_session:
            session_ref, resumed_dir = resume_session
            # The preamble is client-overridable submit data: the default
            # text is vocabulary-neutral, and a client whose output contract
            # has its own naming can supply policy["resume_preamble"].
            prompt = str(job.policy.get("resume_preamble") or RESUME_PREAMBLE) + prompt
            run_job_event(
                args.database_url,
                "progress",
                job,
                f"Resuming interrupted {adapter.display_name} {adapter.session_noun} "
                f"{session_ref} (from {resumed_dir.parent.parent.name})",
                attempt=attempt,
                event_name="session_resume",
            )

        write_text(prompt_path, prompt)

        run_job_event(
            args.database_url,
            "progress",
            job,
            f"Started {adapter.start_label} {job.task_type} attempt {attempt}",
            attempt=attempt,
            event_name="attempt_started",
        )

        if resume_session:
            spawn = adapter.build_resume(job, directory, resume_session[0])
        else:
            spawn = adapter.build_spawn(job, directory)
        env = agent_env(adapter, run_id, job, attempt, out_path, args.database_url)
        env.update(adapter.bind_credentials())
        env.update(adapter.env_overrides())
        seen_hooks: set[str] = set()
        stream_tail = JsonlTail(spawn.stdout_path)
        stream_parser = adapter.stream_parser()
        with spawn.stdout_path.open("w") as stdout, spawn.stderr_path.open("w") as stderr:
            try:
                process = subprocess.Popen(
                    spawn.command,
                    cwd=util.project_root(),
                    env=env,
                    stdin=subprocess.PIPE,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                )
            except OSError as exc:
                # The OS refused the fork/exec (ENOMEM at fan-out width is
                # the realistic case): no CLI ever started, so this is a
                # spawn failure in §6 vocabulary — default-retryable with
                # backoff, not the terminal unhandled-exception catch-all.
                raise RunnerError(
                    f"Failed to spawn {adapter.display_name}: {exc}",
                    code="spawn_failure",
                    retryable=True,
                ) from exc
            try:
                if process.stdin is None:
                    raise RunnerError(
                        f"{adapter.display_name} stdin was not available.",
                        code=f"{adapter.name}_stdin",
                        retryable=True,
                    )
                try:
                    process.stdin.write(prompt)
                    process.stdin.close()
                except BrokenPipeError as exc:
                    raise RunnerError(
                        f"{adapter.display_name} exited before receiving the prompt.",
                        code=f"{adapter.name}_stdin",
                        retryable=True,
                    ) from exc

                # Timeout is submit data; a submitter that never set one gets
                # the documented default instead of a bare KeyError blocking
                # the job as 'unhandled_exception'.
                timeout_minutes = int(
                    job.policy.get("attempt_timeout_minutes")
                    or DEFAULT_ATTEMPT_TIMEOUT_MINUTES
                )
                deadline = time.monotonic() + timeout_minutes * 60
                valid_output_reported = False
                session_recorded = False
                review_watch = probes.get("review_watch")
                validation_gate = AttemptValidationGate(
                    out_path,
                    review_watch(out_path) if review_watch is not None else None,
                    job.expensive,
                )
                last_beat = time.monotonic()
                while process.poll() is None:
                    last_beat = poll_heartbeat(args, job, process, last_beat)
                    record_hook_progress(args, adapter, run_id, job, attempt, seen_hooks)
                    record_stream_progress(args, job, attempt, stream_tail, stream_parser)
                    if not session_recorded:
                        live_session_ref = adapter.session_ref_from_log(spawn.stdout_path)
                        if live_session_ref:
                            record_attempt_session(args, job, attempt_id, live_session_ref)
                            session_recorded = True
                            resume_state["session_recorded"] = True
                    if (
                        not valid_output_reported
                        and validation_gate.should_validate()
                        and maybe_validate_attempt(out_path, probes, attempt)
                    ):
                        valid_output_reported = True
                        run_job_event(
                            args.database_url,
                            "progress",
                            job,
                            f"{adapter.display_name} wrote valid output; waiting for process exit",
                            attempt=attempt,
                            fatal=False,
                        )
                    if time.monotonic() > deadline:
                        process.terminate()
                        try:
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            process.kill()
                        raise RunnerError(
                            f"Timed out waiting for {adapter.display_name} output.",
                            code="timeout",
                            retryable=True,
                        )
                    time.sleep(args.poll_seconds)
                record_hook_progress(args, adapter, run_id, job, attempt, seen_hooks)
                record_stream_progress(args, job, attempt, stream_tail, stream_parser)
            except BaseException:
                # Never leak a live agent child: any exception here (job-event
                # DB failure, validator crash, KeyboardInterrupt) must not
                # leave the CLI running as an orphan burning provider budget.
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                raise

        if process.returncode != 0:
            # Output validity, not the CLI exit code, decides the attempt: a
            # crash during shutdown after the deliverable is written must not
            # burn the completed work. Terminal auth/billing failures still
            # classify correctly because validation fails when no output file
            # was written.
            result = maybe_validate_attempt(out_path, probes, attempt)
            if result:
                run_job_event(
                    args.database_url,
                    "progress",
                    job,
                    f"{adapter.display_name} exited {process.returncode} after writing "
                    "valid output; accepting the attempt",
                )
                return result
            error_text = adapter.error_report(spawn.stdout_path, spawn.stderr_path)
            raise adapter.classify_failure(
                error_text or f"{adapter.display_name} exited {process.returncode}."
            )

        report = probes["primary"](out_path, attempt)
        # D5: an infrastructure_failure verdict means the probe's own
        # infrastructure broke — it says nothing about the agent's work. The
        # policy row re-probes under its own bounded cap, consuming neither
        # max_attempts nor the session's resume budget (nothing is re-claimed
        # here, so the session is untouched by construction).
        infra = POLICY[outcomes.INFRASTRUCTURE_FAILURE]
        infra_rounds = 0
        while (
            report.verdict == outcomes.INFRASTRUCTURE_FAILURE
            and not infra.consumes_attempt
            and infra_rounds < (infra.retry_cap or 0)
        ):
            infra_rounds += 1
            run_job_event(
                args.database_url,
                "progress",
                job,
                f"Probe infrastructure failure ({report.outcome_code}); re-probing "
                f"{infra_rounds}/{infra.retry_cap} without consuming the attempt budget",
                attempt=attempt,
                fatal=False,
            )
            report = probes["primary"](out_path, attempt)
        if report.verdict == outcomes.INFRASTRUCTURE_FAILURE:
            # The separate cap is spent: escalate to the default-retryable
            # path (backoff, consumes an attempt) so a persistently broken
            # probe cannot spin in place forever.
            raise RunnerError(
                f"{report.message} (probe infrastructure retry cap of {infra.retry_cap} spent)",
                code=report.outcome_code or report.verdict,
                retryable=True,
                details=report.details,
            )
        if report.verdict != outcomes.VALID:
            if (
                POLICY[report.verdict].repair_eligible
                and adapter.capabilities.followup
                and job.repair_rounds > 0
            ):
                # Repair rounds (probe_spec data, default two for the jobs
                # that carry them): fixing one defect can surface the next (a
                # restored instructor still missing course_roles; a re-found
                # syllabus URL that is itself dead).
                repair_report = report
                for _ in range(job.repair_rounds):
                    repaired = repair_attempt(
                        args, adapter, job, run_id, run_dir, attempt,
                        out_path, spawn.stdout_path, repair_report, probes,
                    )
                    if repaired:
                        return repaired
                    retry_report = probes["primary"](out_path, attempt)
                    if retry_report.verdict == outcomes.VALID:
                        return AttemptResult(
                            path=out_path, data=retry_report.data or {}, attempt=attempt
                        )
                    if (
                        retry_report.message == repair_report.message
                        or not POLICY[retry_report.verdict].repair_eligible
                    ):
                        break
                    repair_report = retry_report
            raise ProbeFailureError(report)
        review = probes.get("review")
        if review is not None:
            # The facade-built review probe (today: phase 3's independent
            # reviewer gate); a missing review file arrives as an invalid
            # report — same POLICY row (retry, consumes the attempt), same
            # recorded code as the old direct raise.
            review_report = review(out_path)
            if review_report.verdict != outcomes.VALID:
                raise ProbeFailureError(review_report)
        return AttemptResult(path=out_path, data=report.data or {}, attempt=attempt)
    finally:
        for resource in provisioned:
            resource.close()


def run_with_retries(
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    run_dir: Path,
    *,
    template: str | None = None,
    probes: dict[str, Callable[..., Any]] | None = None,
    hooks: dict[str, Callable[..., Any]] | None = None,
    notifier: Callable[[str, str], None] | None = None,
    resources: dict[str, Any] | None = None,
) -> AttemptResult:
    """The engine entry point (called by the facade's await_outcome): claim,
    run, judge through POLICY, retry/block/halt. Success promotion is the
    client's act and happens facade-side after this returns (design §3).

    ``args`` is anything carrying the RunnerConfig fields; missing optional
    fields are filled with the documented defaults (the old implicit
    argparse.Namespace attribute contract, made explicit)."""
    args = RunnerConfig.coerce(args)
    adapter = get_adapter(job.harness)
    # D5 counters are in-process state (no DDL tonight): bounded free retries
    # per signal, outside the job's max_attempts budget.
    free_retries: dict[str, int] = {}
    # The caller's short display name for messages (artifact_contract's
    # attempt_dir_name — historically the GTM spec key, kept for event-text
    # parity).
    job_name = job.attempt_dir_name or job.key

    claim_stalls = 0
    while True:
        try:
            claim = claim_job(args.database_url, job, run_id)
        except RunnerError as exc:
            # A transient stall on the claim write must not abort the job:
            # the row is still 'queued', and once this run exits NOTHING ever
            # claims queued rows again — the 2026-08-03 strand. Bounded like
            # the other infrastructure retries; past the cap, surface the
            # stranding explicitly so the caller can reconcile.
            if exc.code != "db_timeout":
                raise
            claim_stalls += 1
            if claim_stalls > CLAIM_STALL_RETRIES:
                raise RunnerError(
                    f"{job_name}: claim timed out {claim_stalls} time(s); the row is "
                    "likely still 'queued' and nothing picks queued rows up after "
                    "this run exits — requeue or rerun the phase.",
                    code="db_timeout",
                    retryable=True,
                    alert=True,
                    details=exc.details,
                ) from exc
            delay = args.retry_backoff_seconds[
                min(claim_stalls - 1, len(args.retry_backoff_seconds) - 1)
            ]
            print(
                f"{job_name}: claim timed out "
                f"(stall {claim_stalls}/{CLAIM_STALL_RETRIES}); retrying in {delay}s."
            )
            if not args.no_sleep:
                time.sleep(delay)
            continue
        claim_stalls = 0
        if not claim["claimed"]:
            status = claim["status"]
            wait_seconds = claim["wait_seconds"]
            if status == "queued" and wait_seconds:
                # Retry backoff not yet due — possibly inherited from a killed
                # orchestrator; the DB schedule, not process memory, decides.
                if args.no_sleep:
                    db_rows(
                        args.database_url,
                        "UPDATE jobs SET next_retry_at = now() WHERE project_id = %s AND job_key = %s;",
                        [project_id(), job.key],
                    )
                else:
                    print(f"{job_name}: retry due in {wait_seconds}s; waiting.")
                    # Sleep in heartbeat-sized slices: a 900-1800s backoff with
                    # no run heartbeat would let a second orchestrator flip the
                    # live lease to 'abandoned' and take over mid-run.
                    deadline = time.monotonic() + wait_seconds + 1
                    while (remaining := deadline - time.monotonic()) > 0:
                        time.sleep(min(remaining, HEARTBEAT_SECONDS))
                        if deadline - time.monotonic() > 0:
                            job_heartbeat(args.database_url, job)
                continue
            if status == "running":
                raise RunnerError(
                    f"{job.key} is already running elsewhere (claimed_by another "
                    "process with a fresh heartbeat). If that process is dead, the reaper will "
                    "requeue the job once the heartbeat goes stale.",
                    code="job_already_running",
                    retryable=False,
                    alert=False,
                )
            raise RunnerError(
                f"{job.key} is {status} and cannot be claimed. "
                "A fresh orchestrator start requeues failed/blocked/cancelled jobs; "
                "use --force-rerun to reset everything.",
                code=f"job_{status}",
                retryable=False,
                alert=False,
            )

        attempt = claim["attempt"]
        max_attempts = claim["max_attempts"]
        failure: RunnerError
        resume_state: dict[str, Any] = {}
        try:
            result = run_agent_job_once(
                adapter,
                args,
                job,
                run_id,
                run_dir,
                attempt,
                resume_state=resume_state,
                template=template,
                probes=probes,
                hooks=hooks,
                resources=resources,
            )
            record_attempt_outcome(
                args, job, resume_state.get("attempt_id"), "succeeded"
            )
            # Success promotion (canonical copy + siblings) is the client's
            # act — it runs facade-side right after this returns (design §3).
            return result
        except RunnerError as exc:
            failure = exc
        except Exception as exc:  # catch-all boundary: nothing may strand 'running'
            failure = RunnerError(
                f"{job_name} unhandled exception: {exc!r}",
                code="unhandled_exception",
                retryable=False,
                alert=True,
                details=traceback.format_exc(),
            )

        record_attempt_outcome(
            args, job, resume_state.get("attempt_id"), "failed", failure.code
        )
        if resume_state.get("candidate_id") is not None and not resume_state.get("session_recorded"):
            # The attempt consumed a resume candidate but died before its own
            # session ref was recorded, so the consumed row points at a
            # session_id-NULL attempt and the lineage would end here. The CLI
            # is already dead (run_agent_job_once never leaks a live child),
            # so releasing the owner's claim cannot race a live resume.
            unconsume_attempt(
                args, job, resume_state["candidate_id"],
                resume_state.get("attempt_id"), resume_state["directory"],
            )
        message = f"{job_name} attempt {attempt} failed: {failure}"

        # One POLICY lookup decides everything below; the branches only act.
        signal = policy_signal(failure)
        decision = POLICY[signal]

        if decision.action == "halt":
            # Operator cancel: terminal, audited, no retry. The guard in
            # the events module keeps the row 'cancelled'; just record the event
            # for the audit trail and surface the stop.
            run_job_event(
                args.database_url,
                "progress",
                job,
                message,
                attempt=attempt,
                event_name="cancelled",
                fatal=False,
            )
            raise failure

        try:
            run_job_event(args.database_url, "fail", job, message, attempt=attempt)
        except RunnerError as exc:
            # The fail record is audit; mark_retry/mark_blocked below own the
            # row's state (they overwrite its status write). A transient
            # stall here must not strand the row 'running' by aborting them.
            if exc.code != "job_event_transient":
                raise
            print(
                f"WARNING: fail-event append stalled for {job.key}; "
                "continuing to the state update.",
                file=sys.stderr,
            )

        if decision.action == "block":
            mark_blocked(
                args.database_url,
                job,
                message,
                category=failure.code,
                details=failure.details,
            )
            if decision.alert:
                dispatch_alert(notifier, message, "error")
            failure.alert = False
            raise failure

        # decision.action == "retry"
        if not decision.consumes_attempt:
            # D5: this failure says nothing about the job — hand back the
            # attempt claim_job just counted, bounded by the row's own cap.
            free_retries[signal] = free_retries.get(signal, 0) + 1
            if free_retries[signal] <= (decision.retry_cap or 0):
                delay = args.retry_backoff_seconds[
                    min(attempt - 1, len(args.retry_backoff_seconds) - 1)
                ]
                mark_retry(
                    args.database_url,
                    job,
                    f"{message}; retrying in {delay}s (not consuming the attempt budget)",
                    delay,
                    consume_attempt=False,
                )
                continue
            # The separate cap is spent; fall through to the counted path.

        if attempt >= max_attempts:
            exhausted = POLICY[ATTEMPTS_EXHAUSTED]
            final = f"{job_name} failed after {attempt} attempts: {failure}"
            mark_blocked(
                args.database_url,
                job,
                final,
                category=failure.code,
                details=failure.details,
            )
            if exhausted.alert:
                dispatch_alert(notifier, final, "error")
            raise RunnerError(
                final, code="max_attempts_exceeded", retryable=False, alert=False
            ) from failure

        delay = args.retry_backoff_seconds[min(attempt - 1, len(args.retry_backoff_seconds) - 1)]
        mark_retry(args.database_url, job, f"{message}; retrying in {delay}s", delay)
        # The next claim_job call enforces (and sleeps out) the backoff.
