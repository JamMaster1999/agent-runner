"""pipeline_events writes: the run-id global, event SQL, and the direct
event/lifecycle update path.

The facade sets JOB_EVENT_RUN_ID via module attribute
(``events.JOB_EVENT_RUN_ID = run_id``) once per process (acquire_lease, R2).
Step-5 retype: callers hand in the generic ``RunnerJob``; the event columns
``phase``/``backend`` carry ``task_type``/``harness`` — identical values,
runner vocabulary.

Step 7: the client-repo job_event script (and the subprocess hop to it) is
gone — the SQL it owned lives here as direct functions over the runner's own
transport (``util.db_rows``). ``emit_event`` is the one entry point: one
statement appends the event row(s) and runs the guarded pipeline_jobs
update in a single round trip; the ``agent-runner emit`` CLI
(``agent_runner.cli``) and the engine's ``run_job_event`` both dispatch onto
it. The guards are load-bearing and preserved exactly:

- ``status <> 'cancelled'`` on every pipeline_jobs update (late writers
  cannot resurrect a cancelled job).
- ``attempt_count = N`` when an attempt is given: a late writer from a
  reaped-and-reclaimed attempt cannot flip the row.
- ``finish`` never resurrects blocked/failed rows; ``fail`` never overwrites
  a terminal row. The event row still lands unconditionally for the audit
  trail.
"""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from typing import Any

from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.util import db_rows


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
    ``emit_event`` below handles the general event-writing path. The jsonb
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


def as_integer(value: int | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Expected an integer, got {value!r}.")
    return value


def as_number(value: int | float | None) -> int | float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"Expected a number, got {value!r}.")
    return value


def event_rows(call: SimpleNamespace, batch: list[dict]) -> tuple[list[str], list[object]]:
    """(placeholder rows, params) of (event, message, progress_current,
    progress_total, tok_input, tok_cache_write, tok_cache_read, tok_output,
    cost_usd) VALUES entries.

    For batches, also folds the last non-null message/current/total back into
    ``call`` so the denormalized pipeline_jobs columns reflect the newest
    state. The typed-usage slots come only from batch entries (migration
    031); plain lifecycle calls never carry usage, so non-batch rows bind
    five NULLs.
    """
    if batch:
        if call.command != "progress":
            raise ValueError("batch events are only supported for the progress command.")
        rows = []
        for entry in batch:
            current = None if entry.get("current") is None else int(entry["current"])
            total = None if entry.get("total") is None else int(entry["total"])
            rows.append(
                (
                    str(entry.get("event") or "progress"),
                    entry.get("message"),
                    current,
                    total,
                    as_integer(entry.get("tok_input")),
                    as_integer(entry.get("tok_cache_write")),
                    as_integer(entry.get("tok_cache_read")),
                    as_integer(entry.get("tok_output")),
                    as_number(entry.get("cost_usd")),
                )
            )
            if entry.get("message") is not None:
                call.message = entry["message"]
            if current is not None:
                call.current = current
            if total is not None:
                call.total = total
    else:
        rows = [
            (call.event_name or call.command, call.message, call.current, call.total)
            + (None, None, None, None, None)
        ]

    rendered: list[str] = []
    params: list[object] = []
    for index, (event, message, current, total, *typed) in enumerate(rows):
        # The first row's casts type the VALUES columns: binding does NOT
        # make them redundant — an all-NULL column would resolve as text and
        # fail the integer inserts with a DatatypeMismatch.
        casts = (
            ("::text", "::text", "::integer", "::integer",
             "::bigint", "::bigint", "::bigint", "::bigint", "::numeric")
            if index == 0
            else ("",) * 9
        )
        rendered.append(
            "(%s{}, %s{}, %s{}, %s{}, %s{}, %s{}, %s{}, %s{}, %s{})".format(*casts)
        )
        params += [event, message, as_integer(current), as_integer(total), *typed]
    return rendered, params


def update_guard(call: SimpleNamespace) -> tuple[str, list[object]]:
    """(WHERE fencing, params) for the pipeline_jobs UPDATE (never the event
    INSERT):

    - ``status <> 'cancelled'`` always (late writers cannot resurrect).
    - ``attempt_count = N`` when an attempt is given: a late writer from a
      reaped attempt cannot flip a row another worker has re-claimed.
    - ``finish`` only lands on queued/running/succeeded rows (queued covers
      the resume-reuse and template-job paths; never resurrects blocked or
      failed rows), ``fail`` only on queued/running rows (never overwrites
      an already-terminal status).
    """
    guard = "stable_id = %s AND status <> 'cancelled'"
    params: list[object] = [call.stable_id]
    if call.attempt is not None:
        guard += " AND attempt_count = %s"
        params.append(as_integer(call.attempt))
    if call.command == "finish":
        guard += " AND status IN ('queued', 'running', 'succeeded')"
    elif call.command == "fail":
        guard += " AND status IN ('queued', 'running')"
    return guard, params


def update_sql(call: SimpleNamespace, batch: list[dict]) -> tuple[str, list[object]]:
    """The single CTE for this invocation, as (sql, params)."""
    command = call.command
    guard_sql, guard_params = update_guard(call)

    if command == "heartbeat":
        runs_cte = ""
        runs_params: list[object] = []
        if call.run_id:
            runs_cte = """,
runs AS (
  UPDATE pipeline_runs
  SET heartbeat_at = now()
  WHERE run_id = %s AND status = 'running'
  RETURNING 1
)"""
            runs_params = [call.run_id]
        sql = f"""
WITH j AS (
  SELECT stable_id, status FROM pipeline_jobs WHERE stable_id = %s
),
upd AS (
  UPDATE pipeline_jobs
  SET heartbeat_at = now(), updated_at = now()
  WHERE {guard_sql}
  RETURNING 1
){runs_cte}
SELECT j.stable_id, j.status FROM j;
"""
        return sql, [call.stable_id, *guard_params, *runs_params]

    rows, values_params = event_rows(call, batch)

    # Agents report against an earlier-discovered total, so 'PROGRESS: 110/107'
    # happens. The pipeline_jobs CHECK (progress_current <= progress_total)
    # would fail the whole statement and discard every batched event row, so
    # sanitize the denormalized columns only: a zero/negative total carries no
    # usable progress, and current > total widens the total. Event rows keep
    # the raw values for the audit trail (pipeline_events has no CHECK).
    current, total = call.current, call.total
    if total is not None and total <= 0:
        current, total = None, None
    elif current is not None and total is not None and current > total:
        total = current

    assignments = [
        "updated_at = now()",
        "heartbeat_at = now()",
        "progress_updated_at = now()",
    ]
    assignment_params: list[object] = []
    if current is not None:
        if total is not None:
            assignments.append("progress_current = %s")
            assignment_params.append(as_integer(current))
        else:
            # No total in this call: clamp against the stored total so the
            # CHECK can never fail on a current-only update.
            assignments.append(
                "progress_current = LEAST(%s, coalesce(progress_total, %s))"
            )
            assignment_params += [as_integer(current), as_integer(current)]
    if total is not None:
        if current is not None:
            assignments.append("progress_total = %s")
            assignment_params.append(as_integer(total))
        else:
            # No current in this call: never shrink below the stored current.
            assignments.append(
                "progress_total = GREATEST(%s, coalesce(progress_current, %s))"
            )
            assignment_params += [as_integer(total), as_integer(total)]
    if call.message is not None:
        assignments.append("progress_message = %s")
        assignment_params.append(call.message)

    if command == "start":
        pass  # Pure event append; the orchestrator's claim owns the status flip.
    elif command == "progress":
        assignments += [
            "status = CASE WHEN status = 'queued' THEN 'running' ELSE status END",
            "started_at = COALESCE(started_at, now())",
        ]
    elif command == "finish":
        assignments += [
            "status = 'succeeded'",
            "finished_at = now()",
            "error_message = NULL",
            "error_details = '{}'::jsonb",
        ]
    elif command == "fail":
        assignments += [
            "status = 'failed'",
            "finished_at = now()",
            "error_message = %s",
        ]
        # None binds as NULL, matching the old sql_literal(None) rendering.
        assignment_params.append(call.message)
    else:
        raise ValueError(f"Unsupported command: {command}")

    # Hoisted so the f-string below has no backslash expressions (Python 3.11).
    rows_sql = ",\n    ".join(rows)
    assignments_sql = ",\n    ".join(assignments)

    sql = f"""
WITH j AS (
  SELECT id, stable_id, status, group_key FROM pipeline_jobs WHERE stable_id = %s
),
ins AS (
  INSERT INTO pipeline_events
    (job_id, job_stable_id, run_id, phase, backend, attempt, group_key,
     event, message, progress_current, progress_total,
     tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd)
  SELECT j.id, j.stable_id,
         %s, %s,
         %s, %s, j.group_key,
         v.event, v.message, v.progress_current, v.progress_total,
         v.tok_input, v.tok_cache_write, v.tok_cache_read, v.tok_output, v.cost_usd
  FROM j
  CROSS JOIN (VALUES
    {rows_sql}
  ) AS v(event, message, progress_current, progress_total,
         tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd)
  RETURNING 1
),
upd AS (
  UPDATE pipeline_jobs
  SET
    {assignments_sql}
  WHERE {guard_sql}
  RETURNING 1
)
SELECT j.stable_id, j.status FROM j;
"""
    params: list[object] = [
        call.stable_id,
        call.run_id,
        call.phase,
        call.backend,
        as_integer(call.attempt),
        *values_params,
        *assignment_params,
        *guard_params,
    ]
    return sql, params


DB_TIMEOUT_SECONDS = 60


def emit_event(
    url: str,
    command: str,
    stable_id: str,
    *,
    run_id: str | None = None,
    phase: str | None = None,
    backend: str | None = None,
    attempt: int | None = None,
    event_name: str | None = None,
    message: str | None = None,
    current: int | None = None,
    total: int | None = None,
    batch: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """One database round trip for a lifecycle command: append the event
    row(s) and run the guarded pipeline_jobs update; returns the job's
    (stable_id, status) for the heartbeat cancel poll.

    ``batch`` appends many stream-derived events in the same single
    statement — one transaction regardless of how many events it drained.
    Raises RunnerError('job_missing') when no pipeline_jobs row matches;
    transport failures surface as the util module's db_timeout/db_error.
    """
    call = SimpleNamespace(
        command=command,
        stable_id=stable_id,
        run_id=run_id,
        phase=phase,
        backend=backend,
        attempt=attempt,
        event_name=event_name,
        message=message,
        current=current,
        total=total,
    )
    sql, params = update_sql(call, list(batch or []))
    # Single-try (retry=False): the CTE appends event rows with no
    # idempotency key, and under autocommit a server-side commit whose reply
    # is lost looks exactly like a failed try — a transport replay would
    # double-insert the audit rows (and re-run the guarded update). The old
    # client-repo job_event script was single-try for the same reason.
    rows = db_rows(url, sql, params, timeout=DB_TIMEOUT_SECONDS, retry=False)
    if not rows:
        # The j CTE found no job row — the old empty-output contract.
        raise RunnerError(
            f"No pipeline_jobs row matched stable_id {stable_id!r}.",
            code="job_missing",
            retryable=False,
            alert=True,
        )
    job_stable_id, status = rows[0]
    return job_stable_id, status


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

    ``batch`` appends many stream-derived events in one round trip.
    ``fatal=False`` downgrades a failed update to a stderr warning — used for
    advisory progress from stream tails, where a transient DB hiccup must
    not kill a multi-minute agent attempt.

    Step 7: dispatches directly onto ``emit_event`` above — the old
    client-repo subprocess hop is gone.
    """
    try:
        emit_event(
            url,
            command,
            job.key,
            run_id=JOB_EVENT_RUN_ID,
            phase=job.task_type,
            backend=job.harness,
            attempt=attempt,
            event_name=event_name,
            message=message,
            current=current,
            total=total,
            batch=batch,
        )
    except (Exception, SystemExit) as exc:
        # SystemExit included: the transport's missing-driver guidance must
        # honor the same advisory contract as any other failure here.
        details = getattr(exc, "details", "") or ""
        failure_text = f"{exc}" + (f"\n{details}" if details else "")
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
        ) from exc
