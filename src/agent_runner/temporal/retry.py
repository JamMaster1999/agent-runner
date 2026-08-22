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

from datetime import timedelta
from typing import Any

from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from agent_runner import outcomes
from agent_runner.runtime import AttemptReport


def application_error_for(
    report: AttemptReport, *, rate_limit_backoff: timedelta
) -> ApplicationError:
    """The ApplicationError one non-valid attempt outcome raises.

    ``type`` is the outcome word, so workflows and retry policies route on
    the same vocabulary the attempt ended with. ``details`` carries the
    evidence (``failure_details``) so the error that reaches history is the
    whole report, not its first line. ``rate_limited`` carries
    ``next_retry_delay`` — the long, free backoff that overrides the retry
    policy's interval for exactly this attempt. ``auth`` is non-retryable:
    the activity fails fast and the caller alerts."""
    message = report.error or f"attempt ended {report.outcome}"
    if report.outcome == outcomes.RATE_LIMITED:
        return ApplicationError(
            message,
            failure_details(report),
            type=report.outcome,
            non_retryable=False,
            next_retry_delay=rate_limit_backoff,
        )
    return ApplicationError(
        message,
        failure_details(report),
        type=report.outcome,
        non_retryable=report.outcome in outcomes.TERMINAL,
    )


DETAIL_LIMIT = 2000


def failure_details(report: AttemptReport) -> dict[str, Any]:
    """The one details payload a failed attempt leaves in history: the
    outcome word, the CLI-owned text behind it, the session to resume, and
    the attempts that failed before this one. Bounded, so a chatty CLI can
    never push a failure past the payload limit."""
    return {
        "outcome": report.outcome,
        "detail": report.detail[-DETAIL_LIMIT:],
        "session_ref": report.session_ref,
        "prior_attempts": list(report.prior_attempts),
    }


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
