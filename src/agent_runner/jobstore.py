"""jobs store: upsert/claim, heartbeats, leases, reaper, retention.

Step-9 retype: every statement targets the runner database's own schema
(db/migrations — ``jobs``/``events``/``leases``), scoped by
``runtime.project_id()``. The caller-facing surface is unchanged: entry
points take the generic ``RunnerJob`` (or plain scalars) and raise
``RunnerError``. What used to be client columns rides as data now —
``agent_ref``/``artifact_contract``/``policy`` jsonb, display keys inside
``labels`` (opaque, caller-owned).
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from typing import Any

from agent_runner.util import clean_params
from agent_runner import events
from agent_runner.events import append_event_sql
from agent_runner.runtime import RunnerError, RunnerJob, project_id
from agent_runner.util import db_rows, db_tx


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

    RUNNER_RUN_REPLAY (set by a supervisor on incarnations after the first;
    the legacy GTM_RUN_REPLAY spelling is honored for one release) means
    this start is an auto-replay of the same intended run, not a manual
    intervention: the upsert is metadata-only — no requeue, no attempt reset
    (a replayed run would otherwise grant every terminally failed job a
    fresh attempt budget per incarnation), and no force reset (a replayed
    --force-rerun must resume, not restart from zero).
    """
    # Tenant self-registration must precede the first jobs INSERT on a fresh
    # database (jobs.project_id FKs projects, and 001 seeds nothing): submit
    # can legally be the first store write of a process (--prepare-only,
    # protocol op 1), so the lease-time registration alone is not enough.
    ensure_project(url)
    replay = bool(
        os.environ.get("RUNNER_RUN_REPLAY") or os.environ.get("GTM_RUN_REPLAY")
    )
    assignments = [
        "task_type = EXCLUDED.task_type",
        "harness = EXCLUDED.harness",
        "agent_ref = EXCLUDED.agent_ref",
        "artifact_contract = EXCLUDED.artifact_contract",
        "policy = EXCLUDED.policy",
        "max_attempts = EXCLUDED.max_attempts",
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
        requeue = f"jobs.status IN ({', '.join(terminal)})"
        assignments += [
            f"status = CASE WHEN {requeue} THEN 'queued' ELSE jobs.status END",
            f"attempt_count = CASE WHEN {requeue} THEN 0 ELSE jobs.attempt_count END",
            f"next_retry_at = CASE WHEN {requeue} THEN NULL ELSE jobs.next_retry_at END",
            f"finished_at = CASE WHEN {requeue} THEN NULL ELSE jobs.finished_at END",
            f"claimed_by = CASE WHEN {requeue} THEN NULL ELSE jobs.claimed_by END",
            f"claimed_at = CASE WHEN {requeue} THEN NULL ELSE jobs.claimed_at END",
            f"error_message = CASE WHEN {requeue} THEN NULL ELSE jobs.error_message END",
            f"error_details = CASE WHEN {requeue} THEN '{{}}'::jsonb ELSE jobs.error_details END",
        ]

    sql = f"""
INSERT INTO jobs (
  project_id,
  job_key,
  group_key,
  task_type,
  harness,
  agent_ref,
  labels,
  artifact_contract,
  policy,
  status,
  max_attempts,
  created_at,
  updated_at
)
VALUES (
  %s,
  %s,
  %s,
  %s,
  %s,
  %s::jsonb,
  %s::jsonb,
  %s::jsonb,
  %s::jsonb,
  'queued',
  %s,
  now(),
  now()
)
ON CONFLICT (project_id, job_key) DO UPDATE SET
  {", ".join(assignments)};
"""
    db_rows(
        url,
        sql,
        [
            project_id(),
            job.key,
            job.group_key,
            job.task_type,
            job.harness,
            # AgentDef as data (002): name now, per-harness config and the
            # prompt body ref as the submit surface grows into them.
            json.dumps({"name": job.agent_ref}),
            # Labels are the caller's opaque vocabulary and always win;
            # unit_key defaults to the group key for display consumers but a
            # caller-supplied value is never overwritten (the runner invents
            # no caller vocabulary of its own).
            json.dumps({"unit_key": job.group_key, **job.labels}),
            json.dumps({"canonical_path": job.canonical_relpath}),
            json.dumps({"max_attempts": max_attempts}),
            max_attempts,
        ],
    )


HEARTBEAT_SECONDS = 60


def job_heartbeat(url: str, job: RunnerJob) -> str | None:
    """Bump job (and lease) heartbeats; returns the job's current status.

    The returned status is the cancel poll: an operator setting a job to
    'cancelled' is noticed within one heartbeat interval. Advisory — any
    failure returns None rather than killing a multi-minute agent attempt.
    """
    # Step 7: direct call into the runner's own event SQL (SystemExit
    # included for the transport's missing-driver guidance — advisory here).
    try:
        _, status = events.emit_event(
            url, "heartbeat", job.key, run_id=events.JOB_EVENT_RUN_ID
        )
    except (Exception, SystemExit) as exc:
        print(
            f"WARNING: heartbeat failed for {job.key}: {str(exc).strip()[:500]}",
            file=sys.stderr,
        )
        return None
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
    process still owns: claim_job stamps claimed_by/lease_ref, so a job
    reaped and re-claimed by another worker no longer matches and a late
    local failure cannot clobber the live attempt."""
    return "AND claimed_by = %s AND lease_ref = %s", [WORKER_ID, events.JOB_EVENT_RUN_ID]


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
UPDATE jobs
SET
  status = 'queued',{attempt_reset}
  next_retry_at = now() + (%s || ' seconds')::interval,
  claimed_by = NULL,
  claimed_at = NULL,
  progress_message = %s,
  progress_updated_at = now(),
  heartbeat_at = now(),
  updated_at = now()
WHERE project_id = %s AND job_key = %s AND status <> 'cancelled'
  {guard_sql};
"""
    event_sql, event_params = append_event_sql(
        job, "retry_waiting", message, {"delay_seconds": delay_seconds}
    )

    def script(conn) -> int:
        updated = conn.execute(
            update_sql,
            clean_params([delay_seconds, message, project_id(), job.key, *guard_params]),
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
UPDATE jobs
SET
  status = 'blocked',
  finished_at = now(),
  heartbeat_at = now(),
  claimed_by = NULL,
  claimed_at = NULL,
  error_message = %s,
  error_details = jsonb_build_object('category', %s::text, 'details', %s::text),
  updated_at = now()
WHERE project_id = %s AND job_key = %s AND status <> 'cancelled'
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
                    project_id(),
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
# A queued run waits out a full run ahead of it. The default is sized for
# multi-hour agent runs; RUNNER_LEASE_WAIT_SECONDS overrides per deployment.
LEASE_WAIT_TIMEOUT_SECONDS = int(
    os.environ.get("RUNNER_LEASE_WAIT_SECONDS") or 3 * 3600
)


def ensure_project(url: str) -> None:
    """Register this tenant's projects row (idempotent).

    001 seeds nothing: the tenant is declared by RUNNER_PROJECT_ID and
    self-registers on first contact — jobs/attempts/leases FK onto projects,
    so this must run before the first run's writes (acquire_run_lease calls
    it)."""
    db_rows(
        url,
        """
INSERT INTO projects (project_id, name)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
""",
        [project_id(), project_id()],
    )


def acquire_run_lease(
    url: str,
    run_id: str,
    *,
    lease_key: str,
    stale_seconds: int,
    poll_seconds: float = LEASE_POLL_SECONDS,
    timeout_seconds: float = LEASE_WAIT_TIMEOUT_SECONDS,
) -> None:
    """Take the run lease for ``lease_key``, reaping a stale holder first.

    ``lease_key`` is the caller's opaque exclusivity name — with the step-9
    schema it IS the key the partial unique index locks on.

    Also self-registers the tenant's projects row (``ensure_project``): this
    is the first store write of every run, and jobs/attempts/leases FK onto
    projects.

    Lease, not advisory lock: DB access is short-lived one-shot connections,
    so a session-scoped lock cannot be held. The partial unique index
    ``leases_one_held_per_key`` enforces single-holder; a holder whose
    heartbeat is older than ``stale_seconds`` is flipped to 'expired'
    inside the same transaction so takeover is race-free.

    A held lease queues rather than fails: the acquire is retried every
    ``poll_seconds`` until ``timeout_seconds`` expires, so a concurrent run
    for the same lease key waits out the one ahead of it (a full run can
    take hours, hence the generous default). Each retry re-runs the
    stale-holder reap above, so a holder that dies mid-wait is taken over
    once its heartbeat crosses ``stale_seconds``. The uncontended path is
    unchanged: the first attempt acquires immediately, no sleep.
    """
    ensure_project(url)
    reap_sql = """
UPDATE leases
SET status = 'expired', finished_at = now()
WHERE project_id = %s
  AND lease_key = %s
  AND status = 'held'
  AND heartbeat_at < now() - (%s || ' seconds')::interval;
"""
    insert_sql = """
INSERT INTO leases (project_id, lease_ref, lease_key, holder)
VALUES (%s, %s, %s, %s)
ON CONFLICT (project_id, lease_key) WHERE status = 'held' DO NOTHING
RETURNING lease_ref;
"""

    def acquire(conn):
        # The stale-holder reap and the insert commit together, so takeover
        # stays race-free — the old BEGIN..COMMIT script, statement by
        # statement. A returned row means this run holds the lease.
        conn.execute(reap_sql, [project_id(), lease_key, stale_seconds])
        return conn.execute(
            insert_sql, [project_id(), run_id, lease_key, WORKER_ID]
        ).fetchone()

    holder_sql = """
SELECT lease_ref || ' held by ' || holder || ', heartbeat ' ||
       COALESCE(EXTRACT(EPOCH FROM now() - heartbeat_at)::int::text, '?') || 's ago'
FROM leases
WHERE project_id = %s AND lease_key = %s AND status = 'held';
"""

    def current_holder() -> str | None:
        rows = db_rows(url, holder_sql, [project_id(), lease_key])
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
                f"Another run still holds the lease for {lease_key!r} "
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
    """Release the run lease (the leases UPDATE).

    This is the LEASE half of what used to be finalize_run. Release is
    release however the work went (005's status mapping: every caller
    outcome lands 'released'); the run SUMMARY — the pipeline manager's
    judgment — stays a client concern. ``outcome`` is the caller's opaque
    run-outcome vocabulary and is RECORDED, not discarded: it lands on the
    audit trail as a 'lease_released' event fenced to the release actually
    happening (same statement), since the leases outcome column carries a
    CHECK the caller's vocabulary must not be forced through.
    """
    db_rows(
        url,
        """
WITH released AS (
  UPDATE leases
  SET status = 'released', finished_at = now()
  WHERE project_id = %s AND lease_ref = %s AND status = 'held'
  RETURNING lease_key
)
INSERT INTO events (project_id, lease_ref, kind, message)
SELECT %s, %s, 'lease_released',
       'Run lease released for ' || lease_key || ' (caller outcome: ' || %s || ')'
FROM released;
""",
        [project_id(), run_id, project_id(), run_id, outcome or "unspecified"],
    )


def interrupt_run_jobs(url: str, run_id: str) -> list[str]:
    """Flag a run's still-'running' jobs for the stranded-job reaper.

    The `interrupt` protocol op's store write (step 4) — heartbeat_at = NULL
    is what reap_stale_jobs already treats as stale, so the next
    orchestrator start (or the recovery cron) requeues these rows
    immediately instead of waiting out the staleness window.
    Ownership-guarded like the other lifecycle writes: only rows this
    process claimed for this run are touched — jobs a concurrent run owns
    stay put. Returns 'job_key -> interrupted' lines for the shutdown log.
    """
    sql = """
WITH marked AS (
  UPDATE jobs
  SET heartbeat_at = NULL, updated_at = now()
  WHERE project_id = %s
    AND status = 'running'
    AND lease_ref = %s
    AND claimed_by = %s
  RETURNING job_key, group_key, task_type, harness, attempt_count
)
INSERT INTO events (project_id, job_key, group_key, lease_ref, harness, task_type, attempt, kind, message)
SELECT %s, job_key, group_key, %s, harness, task_type, attempt_count, 'interrupted',
       'SIGTERM shutdown by ' || %s || '; left for stranded-job recovery'
FROM marked
RETURNING job_key || ' -> interrupted';
"""
    return [
        row[0]
        for row in db_rows(
            url,
            sql,
            [project_id(), run_id, WORKER_ID, project_id(), run_id, WORKER_ID],
        )
    ]


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
    """job_keys of stale-heartbeat 'running' rows still held by a live local
    process — the reaper's exemption list."""
    rows = db_rows(
        url,
        """
SELECT claimed_by, job_key
FROM jobs
WHERE project_id = %s
  AND status = 'running'
  AND claimed_by IS NOT NULL
  AND (heartbeat_at IS NULL OR heartbeat_at < now() - (%s || ' seconds')::interval);
""",
        [project_id(), stale_seconds],
    )
    return [
        job_key for claimed_by, job_key in rows if job_key and local_worker_alive(claimed_by)
    ]


def reap_stale_jobs(url: str, run_id: str, *, stale_seconds: int) -> None:
    """Requeue (or block, if attempts are exhausted) jobs stranded in 'running'.

    A stranded row means an orchestrator died without writing fail/blocked —
    its heartbeat stopped advancing. Global on purpose: the stale threshold
    protects concurrently running groups. Rows whose claimed_by is a
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
    exempt_clause = "AND NOT (job_key = ANY(%s))" if exempt else ""
    sql = f"""
WITH reaped AS (
  UPDATE jobs
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
  WHERE project_id = %s
    AND status = 'running'
    AND (heartbeat_at IS NULL OR heartbeat_at < now() - (%s || ' seconds')::interval)
    {exempt_clause}
  RETURNING job_key, group_key, task_type, harness, attempt_count, status
)
INSERT INTO events (project_id, job_key, group_key, lease_ref, harness, task_type, attempt, kind, message)
SELECT %s, job_key, group_key, %s, harness, task_type, attempt_count, 'reaped',
       'stale heartbeat; set to ' || status || ' by ' || %s
FROM reaped
RETURNING job_key || ' -> reaped';
"""
    params: list = [project_id(), stale_seconds]
    if exempt:
        params.append(exempt)
    params += [project_id(), run_id, WORKER_ID]
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
            "Reaper note: if the previous runner process was hard-killed, check for orphan "
            f"{names} processes still writing attempt dirs (pgrep -fl '{patterns}')."
        )


def requeue_job(url: str, job_key: str) -> None:
    """Operator recovery command (--requeue-job): put a terminally-stopped job
    back in the queue without touching its attempt history.

    status -> 'queued'; error state, next_retry_at, and the attempt budget
    are cleared — but attempts rows are left untouched, so the next attempt
    still reopens the prior session (standard recovery: fix the bug ->
    requeue -> the session continues). Refuses queued/running/succeeded
    rows.
    """
    sql = """
WITH upd AS (
  UPDATE jobs
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
  WHERE project_id = %s
    AND job_key = %s
    AND status IN ('blocked', 'failed', 'cancelled')
  RETURNING job_key, group_key, task_type, harness
),
ev AS (
  INSERT INTO events (project_id, job_key, group_key, harness, task_type, kind, message)
  SELECT %s, job_key, group_key, harness, task_type, 'requeued',
         'Requeued by operator (' || %s || '); session-resume eligibility preserved'
  FROM upd
  RETURNING 1
)
SELECT job_key || ' -> queued' FROM upd;
"""
    hits = db_rows(url, sql, [project_id(), job_key, project_id(), WORKER_ID])
    if hits:
        print(
            f"Requeued {job_key}: status -> queued, error state cleared, "
            "session-resume eligibility preserved."
        )
        return
    status_rows = db_rows(
        url,
        "SELECT status FROM jobs WHERE project_id = %s AND job_key = %s;",
        [project_id(), job_key],
    )
    if not status_rows:
        raise SystemExit(f"No jobs row matched job_key {job_key!r}.")
    raise SystemExit(
        f"{job_key} is {status_rows[0][0]!r} — only blocked/failed/cancelled jobs can be requeued."
    )


EVENT_RETENTION_DAYS = 30


def prune_old_events(url: str) -> None:
    removed = len(
        db_rows(
            url,
            "DELETE FROM events WHERE at < now() - (%s || ' days')::interval RETURNING 1;",
            [EVENT_RETENTION_DAYS],
        )
    )
    if removed:
        print(f"Pruned {removed} events rows older than {EVENT_RETENTION_DAYS} days.")


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
  SELECT id FROM jobs
  WHERE project_id = %s
    AND job_key = %s
    AND status IN ('queued', 'succeeded')
    AND (next_retry_at IS NULL OR next_retry_at <= now())
  FOR UPDATE SKIP LOCKED
),
claimed AS (
  UPDATE jobs p
  SET
    status = 'running',
    claimed_by = %s,
    claimed_at = now(),
    lease_ref = %s,
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
  RETURNING p.project_id, p.job_key, p.group_key, p.task_type, p.harness,
            p.attempt_count, p.max_attempts
),
ev AS (
  INSERT INTO events (project_id, job_key, group_key, lease_ref, harness, task_type, attempt, kind, message)
  SELECT project_id, job_key, group_key, %s, harness, task_type, attempt_count, 'start',
         'Claimed by ' || %s || ' (attempt ' || attempt_count || ' of ' || max_attempts || ')'
  FROM claimed
  RETURNING 1
)
SELECT attempt_count, max_attempts FROM claimed;
"""
    rows = db_rows(
        url, sql, [project_id(), job.key, WORKER_ID, run_id, run_id, WORKER_ID]
    )
    if rows:
        attempt, max_attempts = rows[0]
        return {"claimed": True, "attempt": attempt, "max_attempts": max_attempts}

    status_sql = """
SELECT status,
       CASE WHEN next_retry_at IS NULL THEN NULL
            ELSE GREATEST(0, CEIL(EXTRACT(EPOCH FROM next_retry_at - now())))::int END
FROM jobs WHERE project_id = %s AND job_key = %s;
"""
    status_rows = db_rows(url, status_sql, [project_id(), job.key])
    if not status_rows:
        raise RunnerError(
            f"No jobs row for {job.key}.",
            code="job_missing",
            retryable=False,
            alert=True,
        )
    status, wait = status_rows[0]
    # NULL next_retry_at arrives as None (the psql-era text mapping produced
    # 0 here); every consumer truthiness-tests wait_seconds, so both read as
    # "no backoff pending".
    return {"claimed": False, "status": status, "wait_seconds": wait}
