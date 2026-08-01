"""pipeline_events writes: the run-id global, event SQL, and job_event calls.

The facade sets JOB_EVENT_RUN_ID via module attribute
(``events.JOB_EVENT_RUN_ID = run_id``) once per process (acquire_lease, R2).
Step-5 retype: callers hand in the generic ``RunnerJob``; the event columns
``phase``/``backend`` carry ``task_type``/``harness`` — identical values,
runner vocabulary.
"""

from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.util import PROJECT_ROOT, ROOT


# Set once per orchestrator process so every event element carries the run id
# without threading it through all run_job_event call sites.
JOB_EVENT_RUN_ID: str | None = None


def append_event_sql(
    job: RunnerJob,
    event: str,
    message: str,
    data: dict[str, Any] | None = None,
) -> tuple[str, list[Any]]:
    """(sql, params) for a parameterized INSERT appending one pipeline_events
    row for a job.

    Executed by the lifecycle helpers in the same transaction as the guarded
    pipeline_jobs UPDATE it fences (jobstore.mark_retry/mark_blocked);
    ``core/job_event.py`` handles the general event-writing path. The jsonb
    payload binds as serialized text through ``%s::jsonb`` so building the
    statement needs no driver import. JOB_EVENT_RUN_ID is read at call time.
    """
    payload = json.dumps(data, separators=(",", ":")) if data else None
    sql = """
INSERT INTO pipeline_events (job_id, job_stable_id, run_id, phase, backend, event, message, data, group_key)
SELECT id, stable_id, %s, %s, %s, %s, %s, %s::jsonb, group_key
FROM pipeline_jobs WHERE stable_id = %s
""".strip()
    return sql, [
        JOB_EVENT_RUN_ID,
        job.task_type,
        job.harness,
        event,
        message,
        payload,
        job.key,
    ]


def run_job_event(
    url: str,
    command: str,
    job: RunnerJob,
    message: str | None,
    *,
    current: int | None = None,
    total: int | None = None,
    attempt: int | None = None,
    event_name: str | None = None,
    batch: list[dict[str, Any]] | None = None,
    fatal: bool = True,
) -> None:
    """Append event(s) to pipeline_events and update the job's progress columns.

    ``batch`` appends many stream-derived events in one psql round trip.
    ``fatal=False`` downgrades a failed update to a stderr warning — used for
    advisory progress from stream tails, where a transient psql hiccup must
    not kill a multi-minute agent attempt.

    ``url`` is not passed on argv: job_event.py resolves DATABASE_URL from
    the environment (set at startup), keeping the URL out of process listings.
    """
    # The core/job_event.py subprocess hop survives step 6 verbatim,
    # reached through the configured project root (AGENT_RUNNER_PROJECT_ROOT);
    # it dies at step 7 when the `agent-runner emit` CLI lands.
    args = [
        sys.executable,
        str(ROOT / "core" / "job_event.py"),
        command,
        job.key,
        "--phase",
        job.task_type,
        "--backend",
        job.harness,
    ]
    if message is not None:
        args += ["--message", message]
    if JOB_EVENT_RUN_ID:
        args += ["--run-id", JOB_EVENT_RUN_ID]
    if attempt is not None:
        args += ["--attempt", str(attempt)]
    if event_name is not None:
        args += ["--event-name", event_name]
    if current is not None:
        args += ["--current", str(current)]
    if total is not None:
        args += ["--total", str(total)]
    input_text = None
    if batch:
        args += ["--batch-json", "-"]
        input_text = json.dumps(batch)
    try:
        result = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            input=input_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=120,
        )
        failure_text = (result.stdout or "") + "\n" + (result.stderr or "")
        failed = result.returncode != 0
    except subprocess.TimeoutExpired:
        failure_text = "job_event.py timed out after 120s"
        failed = True
    if failed:
        if not fatal:
            print(
                f"WARNING: advisory job event update failed for {job.key}: "
                f"{failure_text.strip()[:500]}",
                file=sys.stderr,
            )
            return
        raise RunnerError(
            "pipeline_jobs event update failed.",
            code="job_event_failed",
            retryable=False,
            alert=True,
            details=failure_text,
        )
