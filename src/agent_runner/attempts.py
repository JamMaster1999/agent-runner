"""attempts store: attempt records and session-resume claims.

Extraction step 6: this is the attempt STORE half — record/claim/unconsume,
fingerprints, the data-root path contract — speaking the generic
``RunnerJob``. Step-9 retype: against the runner database's own ``attempts``
table (003). The resume chain is the single self-FK
``consumed_by_attempt_id`` and the budget gate is the precomputed
``resume_depth`` — the recursive chain walk is gone. The CLIENT half
(validate -> decide reuse -> promote behind ``get_artifacts``/
``await_outcome``) stayed in the GTM tree at ``core/runner/attempts.py``.

IDENTITY (migration 007): an attempt IS its row id. ``attempt`` is a display
ordinal that repeats across runs for one job — requeue and --force-rerun
both reset the jobs attempt counter — so it can key nothing. record_attempt_start
inserts and returns the id; every later write for that attempt (session ref,
outcome, consumption, release) addresses it. That is also why the engine now
records the attempt BEFORE claiming a resume candidate: the chain is a self-FK,
so the consuming statement needs the claiming row to already exist.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from agent_runner import util
from agent_runner.runtime import RunnerError, RunnerJob, project_id
from agent_runner.util import db_rows, write_text

# ---------------------------------------------------------------------------
# Attempt STORE half (runner vocabulary — moves at step 6).
# ---------------------------------------------------------------------------


# Default resume preamble, vocabulary-neutral: it names no client output
# convention. A client whose contract has its own naming supplies
# policy["resume_preamble"] at submit (the engine prefers it).
RESUME_PREAMBLE = (
    "RESUME: You are resuming your own earlier session for this exact job; "
    "it was interrupted before the output file was written. Reuse the "
    "research already in this conversation — do not redo items you fully "
    "finished — and complete the remaining ones. Evidence you fetched "
    "earlier in this conversation counts as seen this run. The work packet "
    "below is identical to the one you were given; any run identifiers and "
    "the output path in it are NEW and replace the old ones.\n\n"
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
    """The root attempt paths are stored relative to: RUNNER_DATA_ROOT when
    set (a Volume mount inside a sandboxed deployment; the legacy
    GTM_DATA_ROOT spelling is honored for one release), else the project
    root — so the same rows work unchanged on a workstation."""
    override = os.environ.get("RUNNER_DATA_ROOT") or os.environ.get("GTM_DATA_ROOT")
    return Path(override).resolve() if override else util.project_root()


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
) -> int | None:
    """Register the attempt in attempts before launch and return its row id —
    the handle every later write for this attempt uses. Bookkeeping: a DB
    hiccup here must not kill the attempt itself, so a failed insert returns
    None and the attempt runs untracked (no session ref, no outcome row, and
    no resume claim: the chain's self-FK has nothing to point at).

    A plain INSERT, never an upsert: two launches are two attempts even when
    they carry the same ordinal (migration 007). workspace_ref is stored
    relative to the data root (attempt_dir_for_db) so the row stays valid
    across Mac/Volume mount points.

    Also drops a ``pipeline_attempt.json`` marker in the attempt dir (the
    historical filename is kept deliberately: markers already on disk are
    part of the attempt-dir contract and renaming would orphan them) so the
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
        rows = db_rows(
            args.database_url,
            """
            INSERT INTO attempts
              (project_id, job_key, attempt, harness, lease_ref, prompt_fingerprint, workspace_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
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
        return None
    return rows[0][0] if rows else None


def record_attempt_session(
    args: argparse.Namespace, job: RunnerJob, attempt_id: int | None, session_id: str
) -> None:
    """Stamp the session ref on THIS attempt's row, first write wins."""
    if attempt_id is None:
        return
    try:
        db_rows(
            args.database_url,
            """
            UPDATE attempts SET session_ref = %s
            WHERE id = %s AND session_ref IS NULL;
            """,
            [session_id, attempt_id],
        )
    except RunnerError as exc:
        print(f"WARNING: attempts session update failed for {job.key}: {exc}")


def record_attempt_outcome(
    args: argparse.Namespace,
    job: RunnerJob,
    attempt_id: int | None,
    outcome: str,
    failure_category: str | None = None,
) -> None:
    if attempt_id is None:
        return  # the insert never landed; there is no row to close out
    try:
        db_rows(
            args.database_url,
            """
            UPDATE attempts
            SET outcome = %s,
                error_code = %s,
                finished_at = now()
            WHERE id = %s;
            """,
            [outcome, failure_category, attempt_id],
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
    attempt_id: int | None,
    fingerprint: str,
    run_id: str,
    attempt: int,
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
    The same statement stamps THIS attempt's resume_depth as the candidate's
    + 1 — the insert-path precompute 003 documents.

    ``attempt_id`` is the CLAIMING attempt's row — the chain's self-FK points
    at it, so it must already be inserted (migration 007; the engine records
    the attempt before it claims). None means the insert never landed, and an
    attempt the store cannot name may not consume a session: it would end the
    lineage with a row nothing can follow.

    Returns (session_id, attempt_dir, candidate row id); the id lets the
    engine release the claim via unconsume_attempt if the resumed attempt
    dies before ever recording a session ref of its own. ``run_id`` and
    ``attempt`` no longer enter the DB chain — they survive only in the
    filesystem consumption marker, display only."""
    if attempt_id is None:
        return None
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
              AND id <> %(claimer)s
            ORDER BY id DESC LIMIT 1;
            """,
            {
                "project": project_id(),
                "job": job.key,
                "backend": job.harness,
                "fingerprint": fingerprint,
                "budget": RESUME_BUDGET,
                # Belt and braces: this attempt's own row is inserted by now
                # and a session ref would make it look nominable.
                "claimer": attempt_id,
            },
        )
    except RunnerError as exc:
        print(f"WARNING: attempts resume claim failed for {job.key}: {exc}")
        return None
    if not rows:
        return None
    candidate_id, session_id, directory = rows[0]
    resumed_dir = resolve_attempt_dir(directory)
    data_root_set = bool(
        os.environ.get("RUNNER_DATA_ROOT") or os.environ.get("GTM_DATA_ROOT")
    )
    if data_root_set and not resumed_dir.is_dir():
        # Locality guard, sandboxed deployments only (data root set): attempt
        # dirs and CLI homes share one Volume, so a missing dir means the
        # session transcript is not on this machine either. Do not burn the
        # claim. On a workstation transcripts live under the provider CLI's
        # home directory and survive a pruned runs dir, so the claim
        # proceeds regardless.
        return None
    try:
        consumed = db_rows(
            args.database_url,
            """
            WITH consumed AS (
              UPDATE attempts
              SET consumed_by_attempt_id = %(claimer)s
              WHERE id = %(candidate)s
                AND consumed_by_attempt_id IS NULL
              RETURNING resume_depth
            )
            UPDATE attempts
            SET resume_depth = consumed.resume_depth + 1
            FROM consumed
            WHERE attempts.id = %(claimer)s
            RETURNING attempts.id;
            """,
            {
                "claimer": attempt_id,
                "candidate": candidate_id,
            },
        )
    except RunnerError as exc:
        print(f"WARNING: attempts resume claim failed for {job.key}: {exc}")
        return None
    # No RETURNING row = another claimant won the verify-before-consume race:
    # its UPDATE matched zero rows, so the depth stamp had nothing to join.
    if not consumed:
        return None
    mark_session_consumed(resumed_dir, session_id, run_id, attempt)
    return session_id, resumed_dir, candidate_id


def unconsume_attempt(
    args: argparse.Namespace,
    job: RunnerJob,
    candidate_id: int,
    attempt_id: int | None,
    directory: Path,
) -> None:
    """Release a consumed candidate whose resuming attempt died without ever
    recording a session ref of its own: that attempt's row has session_ref
    NULL, so the candidate's lineage would end here and the session's
    research be redone. Owner-guarded — the consumer pointer must BE this
    attempt's row — and the engine calls this only after that attempt's CLI
    is dead, so the claim-time ``consumed_by_attempt_id IS NULL`` race guard
    is intact: at no instant can two claimants hold the same session.

    The exact inverse of the claim: one statement releases the candidate and
    rolls this attempt's resume_depth back to 0, so a released claim leaves
    nothing behind. Unconsume is one column now (003 dropped consumed_at), so
    it can never half-clear a pair."""
    if attempt_id is None:
        return
    try:
        db_rows(
            args.database_url,
            """
            WITH released AS (
              UPDATE attempts
              SET consumed_by_attempt_id = NULL
              WHERE id = %(candidate)s
                AND consumed_by_attempt_id = %(claimer)s
              RETURNING id
            )
            UPDATE attempts
            SET resume_depth = 0
            FROM released
            WHERE attempts.id = %(claimer)s;
            """,
            {"candidate": candidate_id, "claimer": attempt_id},
        )
    except RunnerError as exc:
        print(f"WARNING: attempts un-consume failed for {job.key}: {exc}")
        return
    # Mirror mark_session_consumed: the filesystem marker follows the DB row.
    try:
        (directory / "resume_consumed.json").unlink(missing_ok=True)
    except OSError:
        pass
