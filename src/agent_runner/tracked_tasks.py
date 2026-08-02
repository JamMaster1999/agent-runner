"""Tracked-task primitives for deterministic script (import) jobs:
claim-dedupe and terminal failure records (D9, design doc §5 op 10).

Step-5 retype: entry points take the generic ``RunnerJob`` and raise/accept
``RunnerError``. The old ``run_script_with_heartbeat`` helper was deleted in
the same pass — heartbeat-preserving execution is GTM-side sugar composing
the task ops (``orchestrate_foundation.run_task_with_heartbeat``, plan §2).
"""

from __future__ import annotations

import argparse

from agent_runner import events as runner_events
from agent_runner.events import run_job_event
from agent_runner.jobstore import claim_job, mark_blocked
from agent_runner.runtime import RunnerError, RunnerJob, project_id
from agent_runner.util import db_rows


def claim_script_job(args: argparse.Namespace, job: RunnerJob, message: str) -> None:
    """Claim a deterministic script (import) job row before running it.

    Import jobs have no retry backoff; a queued row with a leftover
    next_retry_at is forced due rather than waited on.
    """
    for _ in range(2):
        claim = claim_job(args.database_url, job, runner_events.JOB_EVENT_RUN_ID or "")
        if claim["claimed"]:
            run_job_event(
                args.database_url,
                "progress",
                job,
                message,
                event_name="attempt_started",
                fatal=False,
            )
            return
        if claim["status"] == "queued" and claim["wait_seconds"]:
            db_rows(
                args.database_url,
                "UPDATE jobs SET next_retry_at = now() WHERE project_id = %s AND job_key = %s;",
                [project_id(), job.key],
            )
            continue
        break
    raise RunnerError(
        f"{job.key} cannot be claimed (status {claim['status']}).",
        code=f"job_{claim['status']}",
        retryable=False,
        alert=True,
    )


def fail_script_job(args: argparse.Namespace, job: RunnerJob, failure: RunnerError) -> None:
    run_job_event(args.database_url, "fail", job, str(failure), fatal=False)
    mark_blocked(
        args.database_url,
        job,
        str(failure),
        category=failure.code,
        details=failure.details,
    )
