"""events writes: the run-id global, event SQL, and the direct
event/lifecycle update path.

The facade sets JOB_EVENT_RUN_ID via module attribute
(``events.JOB_EVENT_RUN_ID = run_id``) once per process (acquire_lease, R2);
it lands in the ``lease_ref`` routing column. Step-9 retype: every
statement targets the runner schema (``jobs``/``events``/``leases``,
project-scoped); the Python keyword surface keeps the historical
``phase``/``backend`` spellings so callers are untouched — they bind the
``task_type``/``harness`` columns.

Step 7: the client-repo job_event script (and the subprocess hop to it) is
gone — the SQL it owned lives here as direct functions over the runner's own
transport (``util.db_rows``). Two entry points since extraction step 10.5:

- ``emit_event`` — the ENGINE path (full-privilege DSN): one statement
  appends the event row(s) and runs the guarded jobs update in a single
  round trip; ``run_job_event`` dispatches onto it.
- ``append_agent_events`` — the AGENT path (``agent-runner emit`` CLI,
  restricted ``runner_emitter`` DSN): a pure parameterized INSERT into
  events with no jobs SELECT/UPDATE and no RETURNING, because the emitter
  role holds INSERT on events and nothing else (migration 004's no-FK
  design: routing columns come from RUNNER_* attribution, orphan rows are
  legal). Job state — status flips, progress mirrors, heartbeats — is the
  engine's act alone.

The engine-path guards are load-bearing and preserved exactly:

- ``status <> 'cancelled'`` on every jobs update (late writers cannot
  resurrect a cancelled job).
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

from agent_runner.runtime import RunnerError, RunnerJob, project_id
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
    """(sql, params) for a parameterized INSERT appending one events row for
    a job.

    Executed by the lifecycle helpers in the same transaction as the guarded
    jobs UPDATE it fences (jobstore.mark_retry/mark_blocked); ``emit_event``
    below handles the general event-writing path. The jsonb payload binds as
    serialized text through ``%s::jsonb`` so building the statement needs no
    driver import. JOB_EVENT_RUN_ID is read at call time and lands in
    lease_ref; group_key rides from the job row (SELECT form kept so the
    caller never supplies it).
    """
    payload = json.dumps(data, separators=(",", ":")) if data else None
    sql = """
INSERT INTO events (project_id, job_key, group_key, lease_ref, harness, task_type, kind, message, data)
SELECT project_id, job_key, group_key, %s, %s, %s, %s, %s, %s::jsonb
FROM jobs WHERE project_id = %s AND job_key = %s
""".strip()
    return sql, [
        JOB_EVENT_RUN_ID,
        job.harness,
        job.task_type,
        event,
        message,
        payload,
        project_id(),
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
    ``call`` so the denormalized jobs columns reflect the newest
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


def append_events_sql(call: SimpleNamespace, batch: list[dict]) -> tuple[str, list[object]]:
    """(sql, params) for the agent emit path: a pure INSERT into events.

    Runs under the restricted ``runner_emitter`` role (INSERT on events,
    nothing else — db/roles/020), so it must not read jobs, update jobs, or
    RETURNING anything. Every routing column binds from the caller's
    RUNNER_* attribution; a job_key with no jobs row is a legal orphan by
    migration 004's design. The VALUES casts come from ``event_rows``.
    """
    rows, values_params = event_rows(call, batch)
    rows_sql = ",\n    ".join(rows)
    sql = f"""
INSERT INTO events
  (project_id, job_key, group_key, lease_ref, harness, task_type, attempt,
   kind, message, progress_current, progress_total,
   tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd)
SELECT %s, %s, %s, %s, %s, %s, %s,
       v.event, v.message, v.progress_current, v.progress_total,
       v.tok_input, v.tok_cache_write, v.tok_cache_read, v.tok_output, v.cost_usd
FROM (VALUES
    {rows_sql}
  ) AS v(event, message, progress_current, progress_total,
         tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd);
""".strip()
    params: list[object] = [
        project_id(),
        call.stable_id,
        call.group_key,
        call.run_id,
        call.backend,
        call.phase,
        as_integer(call.attempt),
        *values_params,
    ]
    return sql, params


def append_agent_events(
    url: str,
    command: str,
    stable_id: str,
    *,
    group_key: str | None = None,
    run_id: str | None = None,
    phase: str | None = None,
    backend: str | None = None,
    attempt: int | None = None,
    event_name: str | None = None,
    message: str | None = None,
    current: int | None = None,
    total: int | None = None,
    batch: list[dict[str, Any]] | None = None,
) -> None:
    """Append event row(s) only — the ``agent-runner emit`` entry point
    (extraction step 10.5).

    No jobs read, no jobs update, no return value: under the INSERT-only
    emitter role a status/cancel poll is impossible by construction, and job
    state is the engine's act. Single-try for the same
    no-idempotency-key reason as ``emit_event``.
    """
    call = SimpleNamespace(
        command=command,
        stable_id=stable_id,
        group_key=group_key,
        run_id=run_id,
        phase=phase,
        backend=backend,
        attempt=attempt,
        event_name=event_name,
        message=message,
        current=current,
        total=total,
    )
    sql, params = append_events_sql(call, list(batch or []))
    db_rows(url, sql, params, timeout=DB_TIMEOUT_SECONDS, retry=False)


def update_guard(call: SimpleNamespace) -> tuple[str, list[object]]:
    """(WHERE fencing, params) for the jobs UPDATE (never the event
    INSERT):

    - ``status <> 'cancelled'`` always (late writers cannot resurrect).
    - ``attempt_count = N`` when an attempt is given: a late writer from a
      reaped attempt cannot flip a row another worker has re-claimed.
    - ``finish`` only lands on queued/running/succeeded rows (queued covers
      the resume-reuse and template-job paths; never resurrects blocked or
      failed rows), ``fail`` only on queued/running rows (never overwrites
      an already-terminal status).
    """
    guard = "project_id = %s AND job_key = %s AND status <> 'cancelled'"
    params: list[object] = [project_id(), call.stable_id]
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
  UPDATE leases
  SET heartbeat_at = now()
  WHERE project_id = %s AND lease_ref = %s AND status = 'held'
  RETURNING 1
)"""
            runs_params = [project_id(), call.run_id]
        sql = f"""
WITH j AS (
  SELECT job_key, status FROM jobs WHERE project_id = %s AND job_key = %s
),
upd AS (
  UPDATE jobs
  SET heartbeat_at = now(), updated_at = now()
  WHERE {guard_sql}
  RETURNING 1
){runs_cte}
SELECT j.job_key, j.status FROM j;
"""
        return sql, [project_id(), call.stable_id, *guard_params, *runs_params]

    rows, values_params = event_rows(call, batch)

    # Agents report against an earlier-discovered total, so 'PROGRESS: 110/107'
    # happens. The jobs CHECK (progress_current <= progress_total) would fail
    # the whole statement and discard every batched event row, so sanitize
    # the denormalized columns only: a zero/negative total carries no usable
    # progress, and current > total widens the total. Event rows keep the
    # raw values for the audit trail (events has no CHECK — 004 keeps them
    # truthful on purpose).
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
  SELECT project_id, job_key, status, group_key FROM jobs WHERE project_id = %s AND job_key = %s
),
ins AS (
  INSERT INTO events
    (project_id, job_key, group_key, lease_ref, harness, task_type, attempt,
     kind, message, progress_current, progress_total,
     tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd)
  SELECT j.project_id, j.job_key, j.group_key,
         %s, %s, %s, %s,
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
  UPDATE jobs
  SET
    {assignments_sql}
  WHERE {guard_sql}
  RETURNING 1
)
SELECT j.job_key, j.status FROM j;
"""
    params: list[object] = [
        project_id(),
        call.stable_id,
        call.run_id,
        call.backend,
        call.phase,
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
    row(s) and run the guarded jobs update; returns the job's
    (job_key, status) for the heartbeat cancel poll.

    ``batch`` appends many stream-derived events in the same single
    statement — one transaction regardless of how many events it drained.
    Raises RunnerError('job_missing') when no jobs row matches; transport
    failures surface as the util module's db_timeout/db_error.
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
            f"No jobs row matched job_key {stable_id!r}.",
            code="job_missing",
            retryable=False,
            alert=True,
        )
    job_key, status = rows[0]
    return job_key, status


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
    """Append event(s) to events and update the job's progress columns.

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
        # Preserve the transport's transient/terminal split: a db_timeout is
        # retryable evidence about the DATABASE, not about the job, and
        # collapsing it to retryable=False routed healthy jobs to terminal
        # 'blocked' on attempt 1 (2026-08-03 incident, PS-5 stall). Only a
        # transient cause stays retryable; job_missing/db_error stay terminal.
        transient = bool(getattr(exc, "retryable", False))
        raise RunnerError(
            "jobs event update failed.",
            code="job_event_transient" if transient else "job_event_failed",
            retryable=transient,
            alert=not transient,
            details=failure_text,
        ) from exc
