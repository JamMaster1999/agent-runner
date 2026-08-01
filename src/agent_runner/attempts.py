"""pipeline_attempts store: attempt records and session-resume claims.

Extraction step 6: this is the attempt STORE half — record/claim/unconsume,
fingerprints, the data-root path contract — speaking the generic
``RunnerJob`` against the bridge ``pipeline_attempts`` table (still the GTM
database, same table names, until the step-9 cutover; runner-owned
migrations arrive at step 8). The CLIENT half (validate -> decide reuse ->
promote behind ``get_artifacts``/``await_outcome``) stayed in the GTM tree
at ``core/runner/attempts.py``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.util import ROOT, db_rows, write_text

# ---------------------------------------------------------------------------
# Attempt STORE half (runner vocabulary — moves at step 6).
# ---------------------------------------------------------------------------


RESUME_PREAMBLE = (
    "RESUME: You are resuming your own earlier session for this exact job; "
    "it was interrupted before the output file was written. Reuse the "
    "research already in this conversation — do not redo items you fully "
    "finished — and complete the remaining ones. Evidence you fetched "
    "earlier in this conversation counts as seen this run. The packet below "
    "is identical to the one you were given; the _meta object and output "
    "path are NEW and replace the old ones.\n\n"
)


def resume_prompt_fingerprint(template: str) -> str:
    """SHA-256 of the PRE-substitution prompt template (D2). Two attempts
    with the same fingerprint received identical work — the definition of a
    resumable pair. Run-varying values (run id, attempt, output directory,
    CDP endpoint) exist only as {{RUNNER_*}}/{{RESOURCE:*}} tokens in the
    template, so the fingerprint is invariant across runs by construction —
    no un-substitution, no preamble stripping (the engine fingerprints before
    prepending RESUME_PREAMBLE). Attempt rows recorded before the template
    contract carry old-style fingerprints and simply never match: resuming
    those sessions falls through to a fresh attempt (design §7.1/D2)."""
    return hashlib.sha256(template.encode()).hexdigest()


def data_root() -> Path:
    """The root attempt paths are stored relative to: GTM_DATA_ROOT when set
    (the Volume mount inside a Sandbox), else the repo root — so the same
    rows work unchanged on the Mac."""
    override = os.environ.get("GTM_DATA_ROOT")
    return Path(override).resolve() if override else ROOT


def attempt_dir_for_db(directory: Path) -> str:
    """attempt_dir as stored in pipeline_attempts: relative to the data root,
    so a row written on one machine resolves under another machine's mount
    point. A directory outside the root falls back to its absolute form."""
    try:
        return str(directory.resolve().relative_to(data_root()))
    except ValueError:
        return str(directory)


def resolve_attempt_dir(stored: str) -> Path:
    """A stored attempt_dir back to a live path. Absolute rows (everything
    written before the data-root indirection) are used as-is."""
    path = Path(stored)
    return path if path.is_absolute() else data_root() / path


def record_attempt_start(
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    attempt: int,
    fingerprint: str,
    directory: Path,
) -> None:
    """Register the attempt in pipeline_attempts before launch. Bookkeeping:
    a DB hiccup here must not kill the attempt itself. attempt_dir is stored
    relative to the data root (attempt_dir_for_db) so the row stays valid
    across Mac/Volume mount points.

    Also drops a ``pipeline_attempt.json`` marker in the attempt dir so the
    legacy filesystem resume matchers skip DB-tracked attempts entirely:
    resume rights for them are decided solely by claim_resumable_attempt."""
    try:
        write_text(
            directory / "pipeline_attempt.json",
            json.dumps(
                {
                    "job_stable_id": job.key,
                    "run_id": run_id,
                    "attempt": attempt,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
    except OSError:
        pass
    try:
        db_rows(
            args.database_url,
            """
            INSERT INTO pipeline_attempts
              (job_stable_id, run_id, attempt, phase, backend, prompt_fingerprint, attempt_dir)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, job_stable_id, attempt)
            DO UPDATE SET prompt_fingerprint = EXCLUDED.prompt_fingerprint,
                          attempt_dir = EXCLUDED.attempt_dir;
            """,
            [
                job.key,
                run_id,
                attempt,
                job.task_type,
                job.harness,
                fingerprint,
                attempt_dir_for_db(directory),
            ],
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts insert failed for {job.key}: {exc}")


def record_attempt_session(
    args: argparse.Namespace, job: RunnerJob, run_id: str, attempt: int, session_id: str
) -> None:
    try:
        db_rows(
            args.database_url,
            """
            UPDATE pipeline_attempts SET session_id = %s
            WHERE run_id = %s
              AND job_stable_id = %s
              AND attempt = %s AND session_id IS NULL;
            """,
            [session_id, run_id, job.key, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts session update failed for {job.key}: {exc}")


def record_attempt_outcome(
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    attempt: int,
    outcome: str,
    failure_category: str | None = None,
) -> None:
    try:
        db_rows(
            args.database_url,
            """
            UPDATE pipeline_attempts
            SET outcome = %s,
                failure_category = %s,
                finished_at = now()
            WHERE run_id = %s
              AND job_stable_id = %s
              AND attempt = %s;
            """,
            [outcome, failure_category, run_id, job.key, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts outcome update failed for {job.key}: {exc}")


def mark_session_consumed(directory: Path, session_id: str, run_id: str, attempt: int) -> None:
    """Filesystem marker mirroring the DB consumption, so the legacy
    filesystem matcher can never re-pick a session the DB already claimed."""
    try:
        write_text(
            directory / "resume_consumed.json",
            json.dumps(
                {
                    "session_id": session_id,
                    "resumed_by_run_id": run_id,
                    "resumed_by_attempt": attempt,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
    except OSError:
        pass


# Max resumes OF ONE SESSION without a valid output before the next attempt
# starts a fresh session (2026-07-28 resume policy). The counter is the
# consumption chain behind the candidate row, not every consumption this job
# ever made: a brand-new session always starts with a full budget.
RESUME_BUDGET = 3

# Guard against a malformed consumption cycle turning the chain walk into an
# infinite recursion; no real chain approaches this.
RESUME_CHAIN_MAX_DEPTH = 100


def claim_resumable_attempt(
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    attempt: int,
    fingerprint: str,
) -> tuple[str, Path, int] | None:
    """Atomically consume the newest resumable prior attempt for this job +
    fingerprint. Resume policy (2026-07-28): a prior session is resumed
    ALWAYS — interrupted, failed in any category, or blocked-then-requeued —
    except when the prompt fingerprint differs (a changed prompt is a new
    request; message history is never tampered with) or the resume budget is
    spent.

    The budget is per SESSION, not per job: ``chain`` walks backwards from the
    candidate through the attempts it descends from (each row records the
    run/attempt that consumed it, and pipeline_attempts is unique on
    (run_id, job_stable_id, attempt)), so it counts how many times THIS
    session has already been resumed. A fresh session — one nobody has
    resumed yet — has an empty chain and a full budget, so a job is never
    permanently unresumable (R7).

    Verify-before-consume (Modal step 2 item 5): the first statement only
    NOMINATES the candidate; the claim is consumed by a second UPDATE that
    runs after this machine has checked it can actually see the candidate's
    attempt directory. A claimant without the files (transcript on another
    machine's disk) returns None with the row left unconsumed for a claimant
    that can open it — and a crash between the two statements consumes
    nothing. The consuming UPDATE keeps the `consumed_by_run_id IS NULL`
    predicate as the race guard: two concurrent claimants can never resume
    the same session, the loser's UPDATE simply matches zero rows. The chain
    behind a candidate is fixed once the candidate row exists, so moving the
    budget gate into the nomination SELECT loses no atomicity.

    Returns (session_id, attempt_dir, candidate row id); the id lets the
    engine release the claim via unconsume_attempt if the resumed attempt
    dies before ever recording a session ref of its own."""
    try:
        rows = db_rows(
            args.database_url,
            """
            WITH RECURSIVE candidate AS (
              SELECT id, run_id, attempt, session_id, attempt_dir
              FROM pipeline_attempts
              WHERE job_stable_id = %(job)s
                AND backend = %(backend)s
                AND prompt_fingerprint = %(fingerprint)s
                AND session_id IS NOT NULL
                AND consumed_by_run_id IS NULL
              ORDER BY id DESC LIMIT 1
            ),
            chain AS (
              SELECT a.id, a.run_id, a.attempt, 1 AS depth
              FROM pipeline_attempts a
              JOIN candidate c
                ON a.consumed_by_run_id = c.run_id
               AND a.consumed_by_attempt = c.attempt
              WHERE a.job_stable_id = %(job)s
              UNION ALL
              SELECT p.id, p.run_id, p.attempt, ch.depth + 1
              FROM pipeline_attempts p
              JOIN chain ch
                ON p.consumed_by_run_id = ch.run_id
               AND p.consumed_by_attempt = ch.attempt
              WHERE p.job_stable_id = %(job)s
                AND ch.depth < %(max_depth)s
            )
            SELECT id, session_id, attempt_dir
            FROM candidate
            WHERE (SELECT count(*) FROM chain) < %(budget)s;
            """,
            {
                "job": job.key,
                "backend": job.harness,
                "fingerprint": fingerprint,
                "max_depth": RESUME_CHAIN_MAX_DEPTH,
                "budget": RESUME_BUDGET,
            },
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts resume claim failed for {job.key}: {exc}")
        return None
    if not rows:
        return None
    # Typed columns: an attempt_dir containing '|' no longer corrupts the
    # old text parse of 'id|session_id|attempt_dir'.
    candidate_id, session_id, directory = rows[0]
    resumed_dir = resolve_attempt_dir(directory)
    if os.environ.get("GTM_DATA_ROOT") and not resumed_dir.is_dir():
        # Locality guard, Sandbox only (GTM_DATA_ROOT set): attempt dirs and
        # CLI homes share one Volume, so a missing dir means the session
        # transcript is not on this machine either. Do not burn the claim.
        # On the Mac transcripts live under the provider CLI's home
        # directory and survive a pruned .local/runs, so the claim
        # proceeds regardless.
        return None
    try:
        consumed = db_rows(
            args.database_url,
            """
            UPDATE pipeline_attempts
            SET consumed_by_run_id = %s,
                consumed_by_attempt = %s,
                consumed_at = now()
            WHERE id = %s
              AND consumed_by_run_id IS NULL
            RETURNING id;
            """,
            [run_id, attempt, candidate_id],
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts resume claim failed for {job.key}: {exc}")
        return None
    # No RETURNING row = another claimant won the verify-before-consume race.
    if not consumed:
        return None
    mark_session_consumed(resumed_dir, session_id, run_id, attempt)
    return session_id, resumed_dir, candidate_id


def unconsume_attempt(
    args: argparse.Namespace,
    job: RunnerJob,
    candidate_id: int,
    run_id: str,
    attempt: int,
    directory: Path,
) -> None:
    """Release a consumed candidate whose resuming attempt died without ever
    recording a session ref of its own: that attempt's row has session_id
    NULL, so the candidate's lineage would end here and the session's
    research be redone. Owner-guarded — only the consuming run/attempt may
    release, and the engine calls this only after that attempt's CLI is dead
    — so the claim-time `consumed_by_run_id IS NULL` race guard is intact:
    at no instant can two claimants hold the same session."""
    try:
        db_rows(
            args.database_url,
            """
            UPDATE pipeline_attempts
            SET consumed_by_run_id = NULL,
                consumed_by_attempt = NULL,
                consumed_at = NULL
            WHERE id = %s
              AND consumed_by_run_id = %s
              AND consumed_by_attempt = %s;
            """,
            [candidate_id, run_id, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: pipeline_attempts un-consume failed for {job.key}: {exc}")
        return
    # Mirror mark_session_consumed: the filesystem marker follows the DB row.
    try:
        (directory / "resume_consumed.json").unlink(missing_ok=True)
    except OSError:
        pass
