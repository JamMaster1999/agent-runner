"""Outcome-to-retry mapping: what each attempt outcome means to Temporal.

One table, ruled (agent_runner.md): ``rate_limited`` backs off long and
free; ``infra``/``spawn_failure``/``timeout``/``stalled``/``invalid_schema``
are ordinary retries the server reschedules — a dead worker's replacement
picks them up; ``auth`` is non-retryable and fails the activity fast, for
the project's workflow to alert on. Everything runs under the caller's
retry-policy backstop — this module recommends one but the project's
activity options are the authority.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from agent_runner import outcomes
from agent_runner.runtime import AttemptReport


def application_error_for(
    report: AttemptReport,
    *,
    rate_limit_backoff: timedelta,
    reset_cap: timedelta,
    retry_by: datetime | None = None,
    now: datetime | None = None,
) -> ApplicationError:
    """The ApplicationError one non-valid attempt outcome raises.

    ``type`` is the outcome word, so workflows and retry policies route on
    the same vocabulary the attempt ended with. ``details`` carries the
    evidence (``failure_details``) so the error that reaches history is the
    whole report, not its first line. ``rate_limited`` carries
    ``next_retry_delay`` — the long, free backoff that overrides the retry
    policy's interval for exactly this attempt, or the wait until the
    CLI's own reset time when it named one (``rate_limit_delay``), capped
    at ``reset_cap``. ``retry_by`` is the last moment a retry may still
    start and finish (the activity's deadline less a margin): a reset
    beyond it fails the attempt non-retryable now instead of idling to
    the backstop. A ``retry_by`` already behind ``now`` is the server's
    timeout to report, not ours. ``auth`` is non-retryable: the activity
    fails fast and the caller alerts."""
    message = report.error or f"attempt ended {report.outcome}"
    if report.outcome == outcomes.RATE_LIMITED:
        now = now or datetime.now(timezone.utc)
        resets_at = report.resets_at
        if resets_at is not None and retry_by is not None and now < retry_by < resets_at:
            return ApplicationError(
                f"{message} — the limit lifts at {resets_at.isoformat(timespec='seconds')}, "
                f"past the last retry window of this activity "
                f"({retry_by.isoformat(timespec='seconds')})",
                failure_details(report),
                type=report.outcome,
                non_retryable=True,
            )
        return ApplicationError(
            message,
            failure_details(report),
            type=report.outcome,
            non_retryable=False,
            next_retry_delay=rate_limit_delay(resets_at, rate_limit_backoff, reset_cap, now),
        )
    return ApplicationError(
        message,
        failure_details(report),
        type=report.outcome,
        non_retryable=report.outcome in outcomes.TERMINAL,
    )


RESET_DELAY_FLOOR = timedelta(seconds=30)


def rate_limit_delay(
    resets_at: datetime | None,
    default: timedelta,
    cap: timedelta,
    now: datetime | None = None,
) -> timedelta:
    """How long a rate-limited attempt waits: until the CLI's reset time
    when known (floored, so a reset already past retries promptly, and
    capped by the caller's ``cap``), else the configured backoff."""
    if resets_at is None:
        return default
    wait = resets_at - (now or datetime.now(timezone.utc))
    return min(max(wait, RESET_DELAY_FLOOR), cap)


def failure_details(report: AttemptReport) -> dict[str, Any]:
    """The one details payload a failed attempt leaves in history: this
    attempt's record under ``attempt`` (outcome, the CLI-owned text behind
    it, the session to resume, its timing and spend) and the records of
    the attempts before it under ``attempts``. Every record is bounded, so
    a chatty CLI can never push a failure past the payload limit. The
    core runner leaves ``report.attempts`` empty; then ``attempt`` is
    None."""
    *prior, own = report.attempts or (None,)
    return {"attempt": own, "attempts": prior}


def recommended_retry_policy(
    *,
    initial_interval: timedelta = timedelta(seconds=30),
    maximum_interval: timedelta = timedelta(minutes=10),
    maximum_attempts: int = 6,
) -> RetryPolicy:
    """The config backstop a project can hang its activity on: bounded
    attempts, exponential backoff, and the terminal outcomes declared
    non-retryable so an auth failure never spins silently."""
    return RetryPolicy(
        initial_interval=initial_interval,
        maximum_interval=maximum_interval,
        maximum_attempts=maximum_attempts,
        non_retryable_error_types=list(outcomes.TERMINAL),
    )
