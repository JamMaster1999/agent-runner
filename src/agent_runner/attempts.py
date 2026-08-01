"""attempts store: attempt records and session-resume claims.

Extraction step 6: this is the attempt STORE half — record/claim/unconsume,
fingerprints, the data-root path contract — speaking the generic
``RunnerJob``. Step-9 retype: against the runner database's own ``attempts``
table (003). The resume chain is the single self-FK
``consumed_by_attempt_id`` and the budget gate is the precomputed
``resume_depth`` — the recursive chain walk is gone. The CLIENT half
(validate -> decide reuse -> promote behind ``get_artifacts``/
``await_outcome``) stayed in the GTM tree at ``core/runner/attempts.py``.

BRIDGE NOTE (flagged in docs/step9_cutover.md): the new key
(project_id, job_key, attempt) has no run dimension, so a --force-rerun —
which resets the jobs attempt counter — collides with the earlier run's
rows for the same attempt numbers. record_attempt_start treats that
collision as a fresh-attempt overwrite (session/outcome/consumption
cleared). The pre-cutover schema kept those rows apart by run_id; if the
lost history matters, the successor is a global per-job attempt ordinal —
a design decision for the window review, not improvised here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from agent_runner.runtime import RunnerError, RunnerJob, project_id
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
    """workspace_ref as stored in attempts: relative to the data root, so a
    row written on one machine resolves under another machine's mount
    point. A directory outside the root falls back to its absolute form."""
    try:
        return str(directory.resolve().relative_to(data_root()))
    except ValueError:
        return str(directory)


def resolve_attempt_dir(stored: str) -> Path:
    """A stored workspace_ref back to a live path. Absolute rows (everything
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
    """Register the attempt in attempts before launch. Bookkeeping: a DB
    hiccup here must not kill the attempt itself. workspace_ref is stored
    relative to the data root (attempt_dir_for_db) so the row stays valid
    across Mac/Volume mount points.

    The conflict target is the new key (project_id, job_key, attempt): a
    same-run re-upsert refreshes fingerprint/workspace as before, and a
    force-rerun collision with an EARLIER run's row becomes a fresh-attempt
    overwrite — session/outcome/consumption cleared, resume_depth reset
    (see the module docstring's bridge note).

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
            INSERT INTO attempts
              (project_id, job_key, attempt, harness, lease_ref, prompt_fingerprint, workspace_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (project_id, job_key, attempt)
            DO UPDATE SET prompt_fingerprint = EXCLUDED.prompt_fingerprint,
                          workspace_ref = EXCLUDED.workspace_ref,
                          lease_ref = EXCLUDED.lease_ref,
                          harness = EXCLUDED.harness,
                          session_ref = NULL,
                          outcome = NULL,
                          error_code = NULL,
                          outcome_code = NULL,
                          consumed_by_attempt_id = NULL,
                          resume_depth = 0,
                          finished_at = NULL;
            """,
            [
                project_id(),
                job.key,
                attempt,
                job.harness,
                run_id,
                fingerprint,
                attempt_dir_for_db(directory),
            ],
        )
    except RunnerError as exc:
        print(f"WARNING: attempts insert failed for {job.key}: {exc}")


def record_attempt_session(
    args: argparse.Namespace, job: RunnerJob, run_id: str, attempt: int, session_id: str
) -> None:
    # (project, job_key, attempt) IS the key now; run_id stays in the
    # signature for callers but the row identity no longer includes it.
    del run_id
    try:
        db_rows(
            args.database_url,
            """
            UPDATE attempts SET session_ref = %s
            WHERE project_id = %s
              AND job_key = %s
              AND attempt = %s AND session_ref IS NULL;
            """,
            [session_id, project_id(), job.key, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: attempts session update failed for {job.key}: {exc}")


def record_attempt_outcome(
    args: argparse.Namespace,
    job: RunnerJob,
    run_id: str,
    attempt: int,
    outcome: str,
    failure_category: str | None = None,
) -> None:
    del run_id  # row identity is (project, job_key, attempt) now
    try:
        db_rows(
            args.database_url,
            """
            UPDATE attempts
            SET outcome = %s,
                error_code = %s,
                finished_at = now()
            WHERE project_id = %s
              AND job_key = %s
              AND attempt = %s;
            """,
            [outcome, failure_category, project_id(), job.key, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: attempts outcome update failed for {job.key}: {exc}")


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
# consumption chain behind the candidate row — resume_depth, precomputed at
# consume time — not every consumption this job ever made: a brand-new
# session always starts with a full budget.
RESUME_BUDGET = 3


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

    The budget is per SESSION, not per job: ``resume_depth`` is the length of
    the consumption chain behind the candidate, precomputed when each
    consumer claims (parent + 1; the recursive walk died with the step-9
    schema — 003's attempts_resume_idx plus a flat ``resume_depth < budget``
    predicate is the whole nomination). A fresh session — one nobody has
    resumed yet — has depth 0 and a full budget, so a job is never
    permanently unresumable (R7).

    Verify-before-consume (Modal step 2 item 5): the first statement only
    NOMINATES the candidate; the claim is consumed by a second statement that
    runs after this machine has checked it can actually see the candidate's
    attempt directory. A claimant without the files (transcript on another
    machine's disk) returns None with the row left unconsumed for a claimant
    that can open it — and a crash between the two statements consumes
    nothing. The consuming UPDATE keeps the ``consumed_by_attempt_id IS
    NULL`` predicate as the race guard: two concurrent claimants can never
    resume the same session, the loser's UPDATE simply matches zero rows.
    The same statement resolves THIS attempt's row by the new key
    (project, job_key, attempt) and stamps its resume_depth as the
    candidate's + 1 — the insert-path precompute 003 documents.

    Returns (session_id, attempt_dir, candidate row id); the id lets the
    engine release the claim via unconsume_attempt if the resumed attempt
    dies before ever recording a session ref of its own. ``run_id`` no
    longer enters the DB chain (single self-FK) — it survives only in the
    filesystem consumption marker, display only."""
    try:
        rows = db_rows(
            args.database_url,
            """
            SELECT id, session_ref, workspace_ref
            FROM attempts
            WHERE project_id = %(project)s
              AND job_key = %(job)s
              AND harness = %(backend)s
              AND prompt_fingerprint = %(fingerprint)s
              AND session_ref IS NOT NULL
              AND consumed_by_attempt_id IS NULL
              AND resume_depth < %(budget)s
            ORDER BY id DESC LIMIT 1;
            """,
            {
                "project": project_id(),
                "job": job.key,
                "backend": job.harness,
                "fingerprint": fingerprint,
                "budget": RESUME_BUDGET,
            },
        )
    except RunnerError as exc:
        print(f"WARNING: attempts resume claim failed for {job.key}: {exc}")
        return None
    if not rows:
        return None
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
            WITH claimer AS (
              SELECT id FROM attempts
              WHERE project_id = %(project)s AND job_key = %(job)s AND attempt = %(attempt)s
            ),
            consumed AS (
              UPDATE attempts a
              SET consumed_by_attempt_id = claimer.id
              FROM claimer
              WHERE a.id = %(candidate)s
                AND a.consumed_by_attempt_id IS NULL
              RETURNING a.resume_depth
            )
            UPDATE attempts a
            SET resume_depth = consumed.resume_depth + 1
            FROM consumed, claimer
            WHERE a.id = claimer.id
            RETURNING a.id;
            """,
            {
                "project": project_id(),
                "job": job.key,
                "attempt": attempt,
                "candidate": candidate_id,
            },
        )
    except RunnerError as exc:
        print(f"WARNING: attempts resume claim failed for {job.key}: {exc}")
        return None
    # No RETURNING row = another claimant won the verify-before-consume race
    # (or this attempt's own row is missing — its advisory insert failed —
    # in which case nothing was consumed either).
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
    recording a session ref of its own: that attempt's row has session_ref
    NULL, so the candidate's lineage would end here and the session's
    research be redone. Owner-guarded — the consumer pointer must resolve to
    THIS attempt's row by the new key, and the engine calls this only after
    that attempt's CLI is dead — so the claim-time
    ``consumed_by_attempt_id IS NULL`` race guard is intact: at no instant
    can two claimants hold the same session. Unconsume is one column now
    (003 dropped consumed_at), so it can never half-clear a pair."""
    del run_id  # the owner guard resolves through (project, job_key, attempt)
    try:
        db_rows(
            args.database_url,
            """
            UPDATE attempts
            SET consumed_by_attempt_id = NULL
            WHERE id = %s
              AND consumed_by_attempt_id = (
                SELECT id FROM attempts
                WHERE project_id = %s AND job_key = %s AND attempt = %s
              );
            """,
            [candidate_id, project_id(), job.key, attempt],
        )
    except RunnerError as exc:
        print(f"WARNING: attempts un-consume failed for {job.key}: {exc}")
        return
    # Mirror mark_session_consumed: the filesystem marker follows the DB row.
    try:
        (directory / "resume_consumed.json").unlink(missing_ok=True)
    except OSError:
        pass
