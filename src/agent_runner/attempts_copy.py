"""Step-9 cutover copy: GTM ``pipeline_attempts`` -> runner ``attempts``.

The one-time history migration (extraction plan §4 step 9). Attempts are the
only client-side history with forward value — the session-resume registry —
so this is the only table the cutover copies; jobs/events/leases start empty
in the runner database.

The copy implements the hazard documented on the ``attempts`` table
(db/migrations/003, as amended by 007), which is the authority here:

1. RENUMBER — a CHOICE now, not a constraint. 007 dropped
   UNIQUE (project_id, job_key, attempt) from the target, because attempt
   numbers repeat across runs of one job and key nothing there either, so a
   straight insert of the old numbers would now be accepted. This tool still
   renumbers every job's attempts 1..n in (created_at, id) order: repeated
   ordinals read as duplicates in the run view, the rule is deterministic,
   and unlike the dedupe rule the old constraint tempted, it drops NOTHING.

2. TWO-PASS CHAIN RE-LINK. The old chain is the pair
   (consumed_by_run_id, consumed_by_attempt); the new chain is the surrogate
   self-FK consumed_by_attempt_id, and surrogate ids do not exist until the
   rows are inserted — while the renumber in (1) edits exactly the
   coordinates the pair names. Pass one inserts with the FK NULL while
   remembering each row's OLD (run_id, job_stable_id, attempt) triple; pass
   two resolves each pair through that memory, never through the new key.

3. ``resume_depth`` BACKFILL, once, after the re-link. Depth counts the
   consumption chain BEHIND a row (attempts.py's nomination walk): a fresh
   session that resumed nothing is 0, and each consumer is its consumed
   row's depth + 1 — so an exhausted chain's head keeps a depth that FAILS
   ``resume_depth < RESUME_BUDGET`` instead of re-arming. (Anchoring depth 0
   at the unconsumed HEADS and walking the other way inverts the meaning
   and would hand every exhausted session a fresh budget.)

Idempotent by preflight, not by ON CONFLICT: a target that already holds
exactly the planned rows reports "already copied" and exits clean; a target
holding anything else refuses loudly. All target writes ride one
transaction — a failure anywhere leaves the runner DB untouched.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Cycle guard for the depth walk, mirroring attempts.RESUME_CHAIN_MAX_DEPTH;
# no real chain approaches this.
BACKFILL_MAX_DEPTH = 100

SOURCE_QUERY = """
    SELECT job_stable_id, run_id, attempt, backend, prompt_fingerprint,
           session_id, attempt_dir, outcome, failure_category,
           consumed_by_run_id, consumed_by_attempt, created_at, finished_at
    FROM pipeline_attempts
    ORDER BY job_stable_id, created_at, id;
"""

INSERT_QUERY = """
    INSERT INTO attempts (project_id, job_key, attempt, harness, lease_ref,
                          prompt_fingerprint, session_ref, workspace_ref,
                          outcome, error_code, created_at, finished_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
"""

# Depth 0 = rows that consumed nothing (no other row names them as its
# consumer); each consumer is its consumed row's depth + 1. See part 3 above.
BACKFILL_QUERY = """
    WITH RECURSIVE chain AS (
      SELECT a.id, 0 AS depth
      FROM attempts a
      WHERE a.project_id = %(project)s
        AND NOT EXISTS (SELECT 1 FROM attempts x
                        WHERE x.project_id = %(project)s
                          AND x.consumed_by_attempt_id = a.id)
      UNION ALL
      SELECT a.consumed_by_attempt_id, chain.depth + 1
      FROM attempts a
      JOIN chain ON a.id = chain.id
      WHERE a.consumed_by_attempt_id IS NOT NULL
        AND chain.depth < %(max_depth)s
    )
    UPDATE attempts SET resume_depth = chain.depth
    FROM chain
    WHERE attempts.id = chain.id
      AND attempts.project_id = %(project)s;
"""


@dataclass(frozen=True)
class PlannedRow:
    """One source row with its renumbered attempt and old-key coordinates."""

    old_triple: tuple[str, str, int]  # (run_id, job_stable_id, attempt)
    new_attempt: int
    consumed_by: tuple[str, str, int] | None  # old triple of the CONSUMER
    malformed_consumer: bool  # half-cleared pair: run set, attempt NULL
    values: tuple[Any, ...]  # INSERT parameters minus project/attempt


@dataclass
class CopyPlan:
    rows: list[PlannedRow]
    source_count: int
    linked_count: int
    malformed_count: int


def build_plan(source_rows: list[tuple[Any, ...]]) -> CopyPlan:
    """Renumber per job in (created_at, id) order and pre-resolve chain links.

    ``source_rows`` must come from SOURCE_QUERY (its ORDER BY is the
    renumber order).
    """
    rows: list[PlannedRow] = []
    counters: dict[str, int] = {}
    linked = 0
    malformed = 0
    for (job, run_id, attempt, backend, fingerprint, session_id, attempt_dir,
         outcome, failure_category, consumed_run, consumed_attempt,
         created_at, finished_at) in source_rows:
        counters[job] = counters.get(job, 0) + 1
        consumed_by: tuple[str, str, int] | None = None
        bad = False
        if consumed_run is not None and consumed_attempt is not None:
            consumed_by = (consumed_run, job, consumed_attempt)
            linked += 1
        elif consumed_run is not None or consumed_attempt is not None:
            # The half-cleared pair the old two-column unconsume could leave.
            # It cannot be resolved to a consumer; it lands NULL (reads as
            # unconsumed) and is counted so the operator sees it happened.
            bad = True
            malformed += 1
        rows.append(PlannedRow(
            old_triple=(run_id, job, attempt),
            new_attempt=counters[job],
            consumed_by=consumed_by,
            malformed_consumer=bad,
            values=(job, backend, run_id, fingerprint, session_id,
                    attempt_dir, outcome, failure_category, created_at,
                    finished_at),
        ))
    return CopyPlan(rows=rows, source_count=len(rows), linked_count=linked,
                    malformed_count=malformed)


def copy_attempts(source_conn: Any, target_conn: Any, *, project: str = "gtm",
                  dry_run: bool = False) -> dict[str, Any]:
    """Run the copy; returns the verification summary it also prints.

    ``target_conn`` must already have passed assert_runner_target and must
    have the migrations applied (the attempts table exists).
    """
    with source_conn.cursor() as cur:
        cur.execute(SOURCE_QUERY)
        plan = build_plan(cur.fetchall())

    with target_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM attempts WHERE project_id = %s", (project,))
        existing = cur.fetchone()[0]
    if existing:
        if existing == plan.source_count:
            summary = _verify(target_conn, plan, project, note="already copied — nothing to do")
            _report(summary)
            return summary
        raise SystemExit(
            f"target already holds {existing} attempts rows for project "
            f"{project!r} but the source plan has {plan.source_count} — "
            "refusing to guess. Inspect the target before re-running."
        )

    if dry_run:
        summary = {
            "note": "dry run — no target writes",
            "source_rows": plan.source_count,
            "chain_links": plan.linked_count,
            "malformed_pairs": plan.malformed_count,
        }
        _report(summary)
        return summary

    # One transaction: inserts, re-link, depth backfill — all or nothing.
    new_ids: dict[tuple[str, str, int], int] = {}
    unresolved = 0
    with target_conn.cursor() as cur:
        for row in plan.rows:
            (job, backend, run_id, fingerprint, session_id, attempt_dir,
             outcome, failure_category, created_at, finished_at) = row.values
            cur.execute(INSERT_QUERY, (
                project, job, row.new_attempt, backend, run_id, fingerprint,
                session_id, attempt_dir, outcome, failure_category,
                created_at, finished_at,
            ))
            new_ids[row.old_triple] = cur.fetchone()[0]
        for row in plan.rows:
            if row.consumed_by is None:
                continue
            consumer_id = new_ids.get(row.consumed_by)
            if consumer_id is None:
                # The pair names a row the source no longer holds; NULL reads
                # as unconsumed — counted, reported, operator judges.
                unresolved += 1
                continue
            cur.execute(
                "UPDATE attempts SET consumed_by_attempt_id = %s "
                "WHERE id = %s AND project_id = %s",
                (consumer_id, new_ids[row.old_triple], project),
            )
        cur.execute(BACKFILL_QUERY, {"project": project, "max_depth": BACKFILL_MAX_DEPTH})
    target_conn.commit()

    summary = _verify(target_conn, plan, project, note="copied")
    summary["unresolved_links"] = unresolved
    _report(summary)
    return summary


def _verify(target_conn: Any, plan: CopyPlan, project: str, *, note: str) -> dict[str, Any]:
    with target_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), count(consumed_by_attempt_id), "
            "       count(*) FILTER (WHERE consumed_by_attempt_id IS NULL), "
            "       COALESCE(max(resume_depth), 0) "
            "FROM attempts WHERE project_id = %s",
            (project,),
        )
        total, linked, unconsumed, max_depth = cur.fetchone()
    return {
        "note": note,
        "source_rows": plan.source_count,
        "target_rows": total,
        "chain_links_expected": plan.linked_count,
        "chain_links_present": linked,
        "unconsumed_rows": unconsumed,
        "malformed_pairs": plan.malformed_count,
        "max_resume_depth": max_depth,
    }


def _report(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        print(f"{key}: {value}")
