"""pipeline_jobs store: upsert/claim, heartbeats, leases, reaper, retention.

Step-5 retype: every entry point takes the generic ``RunnerJob`` (or plain
scalars) and raises ``RunnerError``; the pipeline_jobs/pipeline_runs column
values are byte-identical to the pre-retype rows — ``phase``/``backend``/
``agent_name`` carry ``task_type``/``harness``/``agent_ref``.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from typing import Any

from agent_runner.util import clean_params
from agent_runner import events
from agent_runner.events import append_event_sql
from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.util import PROJECT_ROOT, ROOT, db_rows, db_tx


def ensure_job(
    url: str,
    job: RunnerJob,
    *,
    max_attempts: int,
    force: bool,
) -> None:
    """Upsert the job row for this run.

    Default: metadata upsert that requeues only terminal-failure rows
    (failed/blocked/cancelled — a fresh manual start IS the manual
    intervention, so their attempt bookkeeping resets). Rows that are
    succeeded, queued with live retry state, or running (the reaper's
    responsibility) are left untouched, which is what lets a restarted
    orchestrator resume the backoff schedule instead of restarting it.

    ``force`` (--force-rerun) restores the old full reset.

    GTM_RUN_REPLAY (set by the Modal run Function on incarnations after the
    first) means this start is an auto-replay of the same intended run, not
    a manual intervention: the upsert is metadata-only — no requeue, no
    attempt reset (a replayed run would otherwise grant every terminally
    failed job a fresh attempt budget per incarnation), and no force reset
    (a replayed --force-rerun must resume, not restart from zero).
    """
    # Deferred (step 7): GTM_RUN_REPLAY stays the env gate verbatim; the
    # generalization to a submit-level 'replay' flag is a behavior change
    # that ships with the facade hardening, not with this relocation.
    replay = bool(os.environ.get("GTM_RUN_REPLAY"))
    assignments = [
        "institution_id = EXCLUDED.institution_id",
        "phase = EXCLUDED.phase",
        "unit_type = EXCLUDED.unit_type",
        "unit_key = EXCLUDED.unit_key",
        "agent_name = EXCLUDED.agent_name",
        "backend = EXCLUDED.backend",
        "output_path = EXCLUDED.output_path",
        "max_attempts = EXCLUDED.max_attempts",
        # Always-on metadata (migration 031): replay/force/default all
        # backfill group_key/labels onto pre-031 rows.
        "group_key = EXCLUDED.group_key",
        "labels = EXCLUDED.labels",
        "updated_at = now()",
    ]
    if replay:
        pass  # metadata-only: replay resumes whatever state the run left
    elif force:
        assignments += [
            "status = 'queued'",
            "progress_current = 0",
            "progress_total = NULL",
            "progress_message = NULL",
            "progress_updated_at = NULL",
            "attempt_count = 0",
            "next_retry_at = NULL",
            "started_at = NULL",
            "finished_at = NULL",
            "heartbeat_at = NULL",
            "claimed_by = NULL",
            "claimed_at = NULL",
            "error_message = NULL",
            "error_details = '{}'::jsonb",
        ]
    else:
        terminal = ["'failed'", "'blocked'", "'cancelled'"]
        requeue = f"pipeline_jobs.status IN ({', '.join(terminal)})"
        assignments += [
            f"status = CASE WHEN {requeue} THEN 'queued' ELSE pipeline_jobs.status END",
            f"attempt_count = CASE WHEN {requeue} THEN 0 ELSE pipeline_jobs.attempt_count END",
            f"next_retry_at = CASE WHEN {requeue} THEN NULL ELSE pipeline_jobs.next_retry_at END",
            f"finished_at = CASE WHEN {requeue} THEN NULL ELSE pipeline_jobs.finished_at END",
            f"claimed_by = CASE WHEN {requeue} THEN NULL ELSE pipeline_jobs.claimed_by END",
            f"claimed_at = CASE WHEN {requeue} THEN NULL ELSE pipeline_jobs.claimed_at END",
            f"error_message = CASE WHEN {requeue} THEN NULL ELSE pipeline_jobs.error_message END",
            f"error_details = CASE WHEN {requeue} THEN '{{}}'::jsonb ELSE pipeline_jobs.error_details END",
        ]

    sql = f"""
INSERT INTO pipeline_jobs (
  id,
  stable_id,
  institution_id,
  phase,
  status,
  unit_type,
  unit_key,
  agent_name,
  backend,
  output_path,
  max_attempts,
  group_key,
  labels,
  created_at,
  updated_at
)
VALUES (
  %s::uuid,
  %s,
  %s::uuid,
  %s,
  'queued',
  'institution',
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s::jsonb,
  now(),
  now()
)
ON CONFLICT (stable_id) DO UPDATE SET
  {", ".join(assignments)};
"""
    db_rows(
        url,
        sql,
        [
            str(uuid.uuid5(uuid.UUID("da5d14f7-c638-47a4-a8d2-7c08d84df608"), job.key)),
            job.key,
            # TRANSITIONAL business ref (client_refs) — dies at cutover,
            # plan §3.
            job.client_refs.get("institution_id"),
            job.task_type,
            job.group_key,
            job.agent_ref,
            job.harness,
            job.canonical_relpath,
            max_attempts,
            # group_key + labels arrive verbatim from the submit request
            # (display strings only).
            job.group_key,
            json.dumps(job.labels),
        ],
    )


HEARTBEAT_SECONDS = 60


def job_heartbeat(url: str, job: RunnerJob) -> str | None:
    """Bump job (and run) heartbeats; returns the job's current status.

    The returned status is the cancel poll: an operator setting a job to
    'cancelled' is noticed within one heartbeat interval. Advisory — any
    failure returns None rather than killing a multi-minute agent attempt.
    """
    # The core/job_event.py subprocess hop survives step 6 verbatim,
    # reached through the configured project root (AGENT_RUNNER_PROJECT_ROOT);
    # it dies at step 7 when the `agent-runner emit` CLI lands.
    command = [
        sys.executable,
        str(ROOT / "core" / "job_event.py"),
        "heartbeat",
        job.key,
    ]
    if events.JOB_EVENT_RUN_ID:
        command += ["--run-id", events.JOB_EVENT_RUN_ID]
    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        print(
            f"WARNING: heartbeat failed for {job.key}: "
            f"{(result.stderr or result.stdout or '').strip()[:500]}",
            file=sys.stderr,
        )
        return None
    output = (result.stdout or "").strip()
    _, _, status = output.rpartition("status=")
    return status or None


def poll_heartbeat(
    args: argparse.Namespace,
    job: RunnerJob,
    process: subprocess.Popen,
    last_beat: float,
) -> float:
    """Heartbeat tick for the agent poll loops; terminates the child on cancel."""
    if time.monotonic() - last_beat < HEARTBEAT_SECONDS:
        return last_beat
    status = job_heartbeat(args.database_url, job)
    if status == "cancelled":
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
        raise RunnerError(
            f"{job.key} was cancelled by the operator.",
            code="cancelled",
            retryable=False,
            alert=False,
        )
    return time.monotonic()


def ownership_guard_sql() -> tuple[str, list[str | None]]:
    """(WHERE fragment, params) fencing lifecycle writes to the job this
    process still owns: claim_job stamps claimed_by/run_id, so a job reaped
    and re-claimed by another worker no longer matches and a late local
    failure cannot clobber the live attempt."""
    return "AND claimed_by = %s AND run_id = %s", [WORKER_ID, events.JOB_EVENT_RUN_ID]


def mark_retry(
    url: str,
    job: RunnerJob,
    message: str,
    delay_seconds: int,
    *,
    consume_attempt: bool = True,
) -> None:
    # consume_attempt=False (D5, phase-2 step 7): a rate-limit/infrastructure
    # retry hands back the attempt claim_job just counted, leaving the job's
    # remaining max_attempts untouched; the bound on such retries lives in
    # the engine's in-process counter, not here.
    attempt_reset = "" if consume_attempt else "\n    attempt_count = GREATEST(attempt_count - 1, 0),"
    guard_sql, guard_params = ownership_guard_sql()
    update_sql = f"""
UPDATE pipeline_jobs
SET
  status = 'queued',{attempt_reset}
  next_retry_at = now() + (%s || ' seconds')::interval,
  claimed_by = NULL,
  claimed_at = NULL,
  progress_message = %s,
  progress_updated_at = now(),
  heartbeat_at = now(),
  updated_at = now()
WHERE stable_id = %s AND status <> 'cancelled'
  {guard_sql};
"""
    event_sql, event_params = append_event_sql(
        job, "retry_waiting", message, {"delay_seconds": delay_seconds}
    )

    def script(conn) -> int:
        updated = conn.execute(
            update_sql,
            clean_params([delay_seconds, message, job.key, *guard_params]),
        ).rowcount
        if updated:
            # The event is fenced on the guarded UPDATE landing — same
            # transaction, so both commit or neither does.
            conn.execute(event_sql, clean_params(event_params))
        return updated

    if not db_tx(url, script):
        print(
            f"WARNING: {job.key} retry not recorded — lost ownership "
            f"(reaped and re-claimed by another worker?); abandoning local attempt.",
            file=sys.stderr,
        )


def mark_blocked(
    url: str,
    job: RunnerJob,
    message: str,
    *,
    category: str,
    details: str = "",
) -> None:
    guard_sql, guard_params = ownership_guard_sql()
    update_sql = f"""
UPDATE pipeline_jobs
SET
  status = 'blocked',
  finished_at = now(),
  heartbeat_at = now(),
  claimed_by = NULL,
  claimed_at = NULL,
  error_message = %s,
  error_details = jsonb_build_object('category', %s::text, 'details', %s::text),
  updated_at = now()
WHERE stable_id = %s AND status <> 'cancelled'
  {guard_sql};
"""
    event_sql, event_params = append_event_sql(job, "blocked", message, {"category": category})

    def script(conn) -> int:
        updated = conn.execute(
            update_sql,
            clean_params(
                [
                    message,
                    category,
                    details[-4000:] if details else "",
                    job.key,
                    *guard_params,
                ]
            ),
        ).rowcount
        if updated:
            # Same fence as mark_retry: event and UPDATE commit together.
            conn.execute(event_sql, clean_params(event_params))
        return updated

    if not db_tx(url, script):
        print(
            f"WARNING: {job.key} blocked-mark not recorded — lost ownership "
            f"(reaped and re-claimed by another worker?); abandoning local attempt.",
            file=sys.stderr,
        )


WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


LEASE_POLL_SECONDS = 5
LEASE_WAIT_TIMEOUT_SECONDS = 3 * 3600  # a queued run waits out a full run ahead of it (~2h)


def acquire_run_lease(
    url: str,
    run_id: str,
    *,
    lease_key: str,
    institution_id: str,
    stale_seconds: int,
    poll_seconds: float = LEASE_POLL_SECONDS,
    timeout_seconds: float = LEASE_WAIT_TIMEOUT_SECONDS,
) -> None:
    """Take the per-institution run lease, reaping a stale holder first.

    ``lease_key`` is the caller's opaque name for the lease (today the
    institution stable_id — operator prints only); ``institution_id`` is the
    TRANSITIONAL business column the pipeline_runs row still keys on (dies
    at cutover, plan §3).

    Lease, not advisory lock: DB access is short-lived one-shot connections,
    so a session-scoped lock cannot be held. The partial unique index
    pipeline_runs_one_running_per_institution enforces single-holder; a holder
    whose heartbeat is older than ``stale_seconds`` is flipped to 'abandoned'
    inside the same transaction so takeover is race-free.

    A held lease queues rather than fails: the acquire is retried every
    ``poll_seconds`` until ``timeout_seconds`` expires, so a concurrent run
    for the same institution waits out the one ahead of it (a full run can
    take ~2h, hence the generous default). Each retry re-runs the stale-holder
    reap above, so a holder that dies mid-wait is taken over once its
    heartbeat crosses ``stale_seconds``. The uncontended path is unchanged:
    the first attempt acquires immediately, no sleep.
    """
    reap_sql = """
UPDATE pipeline_runs
SET status = 'abandoned', finished_at = now()
WHERE institution_id = %s::uuid
  AND status = 'running'
  AND heartbeat_at < now() - (%s || ' seconds')::interval;
"""
    insert_sql = """
INSERT INTO pipeline_runs (run_id, institution_id, claimed_by)
VALUES (%s, %s::uuid, %s)
ON CONFLICT (institution_id) WHERE status = 'running' DO NOTHING
RETURNING run_id;
"""

    def acquire(conn):
        # The stale-holder reap and the insert commit together, so takeover
        # stays race-free — the old BEGIN..COMMIT script, statement by
        # statement. A returned row means this run holds the lease.
        conn.execute(reap_sql, [institution_id, stale_seconds])
        return conn.execute(insert_sql, [run_id, institution_id, WORKER_ID]).fetchone()

    holder_sql = """
SELECT run_id || ' held by ' || claimed_by || ', heartbeat ' ||
       COALESCE(EXTRACT(EPOCH FROM now() - heartbeat_at)::int::text, '?') || 's ago'
FROM pipeline_runs
WHERE institution_id = %s::uuid AND status = 'running';
"""

    def current_holder() -> str | None:
        rows = db_rows(url, holder_sql, [institution_id])
        return rows[0][0] if rows else None

    deadline = time.monotonic() + timeout_seconds
    waiting = False
    while True:
        if db_tx(url, acquire) is not None:
            if waiting:
                print(f"Run lease acquired for {lease_key}; proceeding.")
            return
        now = time.monotonic()
        if now >= deadline:
            holder = current_holder()
            raise RunnerError(
                f"Another orchestrator run still holds the lease for this institution "
                f"after a {int(timeout_seconds)}s wait: {holder or 'unknown'}. The wait "
                f"loop reaps stale holders (heartbeat > {stale_seconds}s), so the holder "
                f"is live and heartbeating; retry after it finishes.",
                code="run_lease_held",
                retryable=False,
                alert=False,
            )
        if not waiting:
            waiting = True
            holder = current_holder()
            print(
                f"Run lease for {lease_key} is held: {holder or 'unknown'}. "
                f"Queueing behind it — waiting up to {int(timeout_seconds)}s, polling every "
                f"{int(poll_seconds)}s (a holder whose heartbeat goes stale after "
                f"{stale_seconds}s is reaped on the next poll)."
            )
        time.sleep(min(poll_seconds, deadline - now))


def release_run_lease(url: str, run_id: str, outcome: str) -> None:
    """Release the per-institution run lease (the pipeline_runs UPDATE).

    This is the LEASE half of what used to be finalize_run: pipeline_runs is
    runner state and stays written with the same outcome vocabulary, so old
    dashboards keep rendering. The run SUMMARY — the pipeline manager's
    judgment of the run — is GTM's and lives in enrichment_runs (written by
    the orchestrator's finalize_run wrapper via GTM's manager-events module).
    """
    db_rows(
        url,
        """
UPDATE pipeline_runs
SET status = %s, finished_at = now()
WHERE run_id = %s AND status = 'running';
""",
        [outcome, run_id],
    )


def interrupt_run_jobs(url: str, run_id: str) -> list[str]:
    """Flag a run's still-'running' jobs for the stranded-job reaper.

    The `interrupt` protocol op's store write (step 4) — SQL moved verbatim
    from orchestrate_foundation.mark_run_jobs_interrupted; WORKER_ID is
    stamped runner-side. heartbeat_at = NULL is what reap_stale_jobs already
    treats as stale, so the next orchestrator start (or the recovery cron)
    requeues these rows immediately instead of waiting out the staleness
    window. Ownership-guarded like the other lifecycle writes: only rows
    this process claimed for this run are touched — jobs a concurrent run
    owns stay put. Returns 'stable_id -> interrupted' lines for the
    shutdown log.
    """
    sql = """
WITH marked AS (
  UPDATE pipeline_jobs
  SET heartbeat_at = NULL, updated_at = now()
  WHERE status = 'running'
    AND run_id = %s
    AND claimed_by = %s
  RETURNING id, stable_id, phase, backend, attempt_count, group_key
)
INSERT INTO pipeline_events (job_id, job_stable_id, run_id, phase, backend, attempt, group_key, event, message)
SELECT id, stable_id, %s, phase, backend, attempt_count, group_key, 'interrupted',
       'SIGTERM shutdown by ' || %s || '; left for stranded-job recovery'
FROM marked
RETURNING job_stable_id || ' -> interrupted';
"""
    return [row[0] for row in db_rows(url, sql, [run_id, WORKER_ID, run_id, WORKER_ID])]


def local_worker_alive(claimed_by: str | None) -> bool:
    """True when ``claimed_by`` names a still-running process on THIS host.

    claimed_by is WORKER_ID ("host:pid"). A merely-busy local job — an import
    blocking in psql, an agent between heartbeats — must never be reaped out
    from under the process that owns it, or a second orchestrator claims it
    and runs a concurrent import of the same tables (F3). Only this host's
    pids can be checked, so remote workers still rely on the heartbeat alone.
    """
    if not claimed_by:
        return False
    host, _, pid_text = claimed_by.rpartition(":")
    if host != socket.gethostname() or not pid_text.isdigit():
        return False
    try:
        os.kill(int(pid_text), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # alive, just owned by another user
    except OSError:
        return False
    return True


def live_local_claim_ids(url: str, *, stale_seconds: int) -> list[str]:
    """stable_ids of stale-heartbeat 'running' rows still held by a live local
    process — the reaper's exemption list."""
    rows = db_rows(
        url,
        """
SELECT claimed_by, stable_id
FROM pipeline_jobs
WHERE status = 'running'
  AND claimed_by IS NOT NULL
  AND (heartbeat_at IS NULL OR heartbeat_at < now() - (%s || ' seconds')::interval);
""",
        [stale_seconds],
    )
    return [
        stable_id for claimed_by, stable_id in rows if stable_id and local_worker_alive(claimed_by)
    ]


def reap_stale_jobs(url: str, run_id: str, *, stale_seconds: int) -> None:
    """Requeue (or block, if attempts are exhausted) jobs stranded in 'running'.

    A stranded row means an orchestrator died without writing fail/blocked —
    its heartbeat stopped advancing. Global on purpose: the stale threshold
    protects concurrently running institutions. Rows whose claimed_by is a
    live process on this host are exempt: they are busy, not stranded.
    """
    try:
        exempt = live_local_claim_ids(url, stale_seconds=stale_seconds)
    except RunnerError as exc:
        # Advisory: if the liveness probe cannot run, reap nothing rather than
        # risk requeueing a live job.
        print(f"WARNING: reaper liveness probe failed ({exc}); skipping reap.", file=sys.stderr)
        return
    if exempt:
        print(f"Reaper: skipping live local job(s): {', '.join(sorted(exempt))}")
    exempt_clause = "AND NOT (stable_id = ANY(%s))" if exempt else ""
    sql = f"""
WITH reaped AS (
  UPDATE pipeline_jobs
  SET
    status = CASE WHEN attempt_count >= max_attempts THEN 'blocked' ELSE 'queued' END,
    error_message = CASE WHEN attempt_count >= max_attempts
                         THEN 'reaped: stale heartbeat with max attempts exhausted'
                         ELSE error_message END,
    error_details = CASE WHEN attempt_count >= max_attempts
                         THEN jsonb_build_object('category', 'reaped_max_attempts')
                         ELSE error_details END,
    claimed_by = NULL,
    claimed_at = NULL,
    next_retry_at = now(),
    updated_at = now()
  WHERE status = 'running'
    AND (heartbeat_at IS NULL OR heartbeat_at < now() - (%s || ' seconds')::interval)
    {exempt_clause}
  RETURNING id, stable_id, phase, backend, attempt_count, status, group_key
)
INSERT INTO pipeline_events (job_id, job_stable_id, run_id, phase, backend, attempt, group_key, event, message)
SELECT id, stable_id, %s, phase, backend, attempt_count, group_key, 'reaped',
       'stale heartbeat; set to ' || status || ' by ' || %s
FROM reaped
RETURNING job_stable_id || ' -> reaped';
"""
    params: list = [stale_seconds]
    if exempt:
        params.append(exempt)
    params += [run_id, WORKER_ID]
    reaped = [row[0] for row in db_rows(url, sql, params)]
    if reaped:
        print(f"Reaper: {'; '.join(reaped)}")
        # Deferred import: the registry pulls in the full adapter stack,
        # which the job store otherwise never needs.
        from agent_runner.harness import registered_adapters

        adapters = registered_adapters()
        names = "/".join(adapter.name for adapter in adapters)
        patterns = "|".join(
            pattern for adapter in adapters for pattern in adapter.orphan_patterns()
        )
        print(
            "Reaper note: if the previous orchestrator was hard-killed, check for orphan "
            f"{names} processes still writing attempt dirs (pgrep -fl '{patterns}')."
        )


def requeue_job(url: str, stable_id: str) -> None:
    """Operator recovery command (--requeue-job): put a terminally-stopped job
    back in the queue without touching its attempt history.

    status -> 'queued'; error state, next_retry_at, and the attempt budget
    are cleared — but pipeline_attempts rows are left untouched, so the next
    attempt still reopens the prior session (standard recovery: fix the bug
    -> requeue -> the session continues). Refuses queued/running/succeeded
    rows.
    """
    sql = """
WITH upd AS (
  UPDATE pipeline_jobs
  SET
    status = 'queued',
    attempt_count = 0,
    next_retry_at = NULL,
    finished_at = NULL,
    claimed_by = NULL,
    claimed_at = NULL,
    error_message = NULL,
    error_details = '{}'::jsonb,
    updated_at = now()
  WHERE stable_id = %s
    AND status IN ('blocked', 'failed', 'cancelled')
  RETURNING id, stable_id, phase, backend, group_key
),
ev AS (
  INSERT INTO pipeline_events (job_id, job_stable_id, phase, backend, group_key, event, message)
  SELECT id, stable_id, phase, backend, group_key, 'requeued',
         'Requeued by operator (' || %s || '); session-resume eligibility preserved'
  FROM upd
  RETURNING 1
)
SELECT stable_id || ' -> queued' FROM upd;
"""
    hits = db_rows(url, sql, [stable_id, WORKER_ID])
    if hits:
        print(
            f"Requeued {stable_id}: status -> queued, error state cleared, "
            "session-resume eligibility preserved."
        )
        return
    status_rows = db_rows(url, "SELECT status FROM pipeline_jobs WHERE stable_id = %s;", [stable_id])
    if not status_rows:
        raise SystemExit(f"No pipeline_jobs row matched stable_id {stable_id!r}.")
    raise SystemExit(
        f"{stable_id} is {status_rows[0][0]!r} — only blocked/failed/cancelled jobs can be requeued."
    )


EVENT_RETENTION_DAYS = 30


def prune_old_events(url: str) -> None:
    removed = len(
        db_rows(
            url,
            "DELETE FROM pipeline_events WHERE at < now() - (%s || ' days')::interval RETURNING 1;",
            [EVENT_RETENTION_DAYS],
        )
    )
    if removed:
        print(f"Pruned {removed} pipeline_events rows older than {EVENT_RETENTION_DAYS} days.")


def claim_job(url: str, job: RunnerJob, run_id: str) -> dict[str, Any]:
    """Atomically claim the job for one attempt.

    Claims 'queued' rows whose retry backoff is due, plus 'succeeded' rows —
    run_with_retries is only entered when the canonical output is missing or
    invalid, and a succeeded row without its file is incoherent state that
    must re-run rather than deadlock. Returns {"claimed": True, attempt,
    max_attempts} or {"claimed": False, status, wait_seconds}.
    """
    sql = """
WITH candidate AS (
  SELECT id FROM pipeline_jobs
  WHERE stable_id = %s
    AND status IN ('queued', 'succeeded')
    AND (next_retry_at IS NULL OR next_retry_at <= now())
  FOR UPDATE SKIP LOCKED
),
claimed AS (
  UPDATE pipeline_jobs p
  SET
    status = 'running',
    claimed_by = %s,
    claimed_at = now(),
    run_id = %s,
    attempt_count = p.attempt_count + 1,
    started_at = COALESCE(p.started_at, now()),
    finished_at = NULL,
    heartbeat_at = now(),
    next_retry_at = NULL,
    error_message = NULL,
    error_details = '{}'::jsonb,
    updated_at = now()
  FROM candidate
  WHERE p.id = candidate.id
  RETURNING p.id, p.stable_id, p.phase, p.backend, p.attempt_count, p.max_attempts, p.group_key
),
ev AS (
  INSERT INTO pipeline_events (job_id, job_stable_id, run_id, phase, backend, attempt, group_key, event, message)
  SELECT id, stable_id, %s, phase, backend, attempt_count, group_key, 'start',
         'Claimed by ' || %s || ' (attempt ' || attempt_count || ' of ' || max_attempts || ')'
  FROM claimed
  RETURNING 1
)
SELECT attempt_count, max_attempts FROM claimed;
"""
    rows = db_rows(
        url, sql, [job.key, WORKER_ID, run_id, run_id, WORKER_ID]
    )
    if rows:
        attempt, max_attempts = rows[0]
        return {"claimed": True, "attempt": attempt, "max_attempts": max_attempts}

    status_sql = """
SELECT status,
       CASE WHEN next_retry_at IS NULL THEN NULL
            ELSE GREATEST(0, CEIL(EXTRACT(EPOCH FROM next_retry_at - now())))::int END
FROM pipeline_jobs WHERE stable_id = %s;
"""
    status_rows = db_rows(url, status_sql, [job.key])
    if not status_rows:
        raise RunnerError(
            f"No pipeline_jobs row for {job.key}.",
            code="job_missing",
            retryable=False,
            alert=True,
        )
    status, wait = status_rows[0]
    # NULL next_retry_at arrives as None (the psql-era text mapping produced
    # 0 here); every consumer truthiness-tests wait_seconds, so both read as
    # "no backoff pending".
    return {"claimed": False, "status": status, "wait_seconds": wait}
