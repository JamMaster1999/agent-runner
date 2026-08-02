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

Idempotent by an exact preflight, not by ON CONFLICT: every copy-owned field,
consumer FK, and resume depth must match the source-derived plan before an
existing target is accepted. A target holding anything else refuses loudly.
The same exact comparison runs before the initial copy commits. All target
writes ride one transaction — a failure anywhere rolls the runner DB back.
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


@dataclass(frozen=True)
class ExpectedTargetRow:
    """Every deterministic target field owned by the one-time copy."""

    key: tuple[str, int]  # (job_key, renumbered attempt)
    fields: tuple[Any, ...]  # target snapshot columns 1..19, including key
    consumer_key: tuple[str, int] | None
    resume_depth: int


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
            # Keep it visible in the plan; the safe-to-copy gate refuses it
            # because landing NULL would silently make the session resumable.
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


def _plan_expectations(plan: CopyPlan) -> dict[tuple[str, int], ExpectedTargetRow]:
    """Validate source-chain semantics and derive the exact target snapshot.

    A half-present old consumer pair cannot be represented by the target's
    single FK without changing "consumed" into "resumable", so it is fatal.
    Full pairs must resolve, chains must be linear/acyclic, and their depth
    must fit the same guard as the SQL backfill.
    """
    if plan.malformed_count:
        raise SystemExit(
            f"source plan has {plan.malformed_count} malformed consumer pair(s);"
            " the target single-FK state cannot preserve their semantics."
            " No target rows were written."
        )

    old_to_key: dict[tuple[str, str, int], tuple[str, int]] = {}
    rows_by_key: dict[tuple[str, int], PlannedRow] = {}
    for row in plan.rows:
        job = row.values[0]
        key = (job, row.new_attempt)
        if row.old_triple in old_to_key or key in rows_by_key:
            raise SystemExit(
                "source plan contains duplicate attempt identity;"
                " refusing an ambiguous copy. No target rows were written."
            )
        old_to_key[row.old_triple] = key
        rows_by_key[key] = row

    predecessor_by_consumer: dict[tuple[str, int], tuple[str, int]] = {}
    consumer_by_key: dict[tuple[str, int], tuple[str, int] | None] = {}
    unresolved = 0
    branched = 0
    for key, row in rows_by_key.items():
        if row.consumed_by is None:
            consumer_by_key[key] = None
            continue
        consumer_key = old_to_key.get(row.consumed_by)
        if consumer_key is None:
            unresolved += 1
            continue
        consumer_by_key[key] = consumer_key
        if consumer_key in predecessor_by_consumer:
            branched += 1
        else:
            predecessor_by_consumer[consumer_key] = key
    if unresolved:
        raise SystemExit(
            f"source plan has {unresolved} unresolved consumer link(s);"
            " refusing to re-arm those sessions. No target rows were written."
        )
    if branched:
        raise SystemExit(
            f"source plan has {branched} branched consumer link(s);"
            " resume depth would be ambiguous. No target rows were written."
        )

    depths: dict[tuple[str, int], int] = {}
    for key in rows_by_key:
        depth = 0
        current = key
        seen = {current}
        while current in predecessor_by_consumer:
            current = predecessor_by_consumer[current]
            if current in seen:
                raise SystemExit(
                    "source plan contains a consumer cycle;"
                    " resume depth is undefined. No target rows were written."
                )
            seen.add(current)
            depth += 1
            if depth > BACKFILL_MAX_DEPTH:
                raise SystemExit(
                    "source plan exceeds the guarded resume-chain depth;"
                    " refusing a truncated backfill. No target rows were written."
                )
        depths[key] = depth

    expected: dict[tuple[str, int], ExpectedTargetRow] = {}
    for key, row in rows_by_key.items():
        (job, backend, run_id, fingerprint, session_id, attempt_dir,
         outcome, failure_category, created_at, finished_at) = row.values
        fields = (
            job,
            row.new_attempt,
            backend,
            None,  # account_id has no source field
            run_id,
            None,  # request_identity has no source field
            fingerprint,
            session_id,
            attempt_dir,
            outcome,
            failure_category,
            None,  # outcome_code has no source field
            None, None, None, None, None,  # token counts + cost
            created_at,
            finished_at,
        )
        expected[key] = ExpectedTargetRow(
            key=key,
            fields=fields,
            consumer_key=consumer_by_key[key],
            resume_depth=depths[key],
        )
    return expected


TARGET_SNAPSHOT_QUERY = """
    SELECT id, job_key, attempt,
           harness, account_id, lease_ref, request_identity,
           prompt_fingerprint, session_ref, workspace_ref,
           outcome, error_code, outcome_code,
           tok_input, tok_cache_write, tok_cache_read, tok_output, cost_usd,
           created_at, finished_at,
           consumed_by_attempt_id, resume_depth
    FROM attempts
    WHERE project_id = %s
    ORDER BY job_key, attempt, id;
"""

# Serialize this one-time operation per project in the TARGET database. The
# transaction-scoped lock is taken before the count and released only by an
# explicit commit or rollback in ``copy_attempts``.
COPY_LOCK_QUERY = """
    SELECT pg_advisory_xact_lock(
        hashtextextended('agent-runner:attempts-copy:' || %s, 0)
    );
"""


def _compare_target_snapshot(
    plan: CopyPlan,
    expected: dict[tuple[str, int], ExpectedTargetRow],
    target_rows: list[tuple[Any, ...]],
    *,
    note: str,
) -> dict[str, Any]:
    """Require field-for-field correspondence, not aggregate equivalence."""
    actual: dict[tuple[str, int], tuple[Any, ...]] = {}
    duplicate_keys = 0
    duplicate_ids = len({row[0] for row in target_rows}) != len(target_rows)
    for row in target_rows:
        key = (row[1], row[2])
        if key in actual:
            duplicate_keys += 1
        else:
            actual[key] = row

    expected_keys = set(expected)
    actual_keys = set(actual)
    field_mismatches = 0
    link_mismatches = 0
    depth_mismatches = 0
    for key in expected_keys & actual_keys:
        wanted = expected[key]
        row = actual[key]
        if tuple(row[1:20]) != wanted.fields:
            field_mismatches += 1
        expected_consumer_id = (
            None
            if wanted.consumer_key is None
            else actual.get(wanted.consumer_key, (None,))[0]
        )
        if row[20] != expected_consumer_id:
            link_mismatches += 1
        if row[21] != wanted.resume_depth:
            depth_mismatches += 1

    problems = []
    if len(target_rows) != len(expected):
        problems.append("row-count")
    if expected_keys != actual_keys:
        problems.append("row-identity")
    if duplicate_keys or duplicate_ids:
        problems.append("duplicate-identity")
    if field_mismatches:
        problems.append("copied-fields")
    if link_mismatches:
        problems.append("consumer-links")
    if depth_mismatches:
        problems.append("resume-depth")
    if problems:
        raise SystemExit(
            "target attempts do not exactly match the source plan "
            f"({', '.join(problems)}); refusing to treat the copy as complete."
        )

    linked = sum(row[20] is not None for row in target_rows)
    max_depth = max((row[21] for row in target_rows), default=0)
    return {
        "note": note,
        "source_rows": plan.source_count,
        "target_rows": len(target_rows),
        "chain_links_expected": plan.linked_count,
        "chain_links_present": linked,
        "unresolved_links": 0,
        "unconsumed_rows": len(target_rows) - linked,
        "malformed_pairs": plan.malformed_count,
        "max_resume_depth": max_depth,
        "exact_match": True,
    }


def _verify_exact(
    target_conn: Any,
    plan: CopyPlan,
    expected: dict[tuple[str, int], ExpectedTargetRow],
    project: str,
    *,
    note: str,
) -> dict[str, Any]:
    with target_conn.cursor() as cur:
        cur.execute(TARGET_SNAPSHOT_QUERY, (project,))
        target_rows = cur.fetchall()
    return _compare_target_snapshot(plan, expected, target_rows, note=note)


def copy_attempts(source_conn: Any, target_conn: Any, *, project: str = "gtm",
                  dry_run: bool = False) -> dict[str, Any]:
    """Run the copy; returns the verification summary it also prints.

    ``target_conn`` must already have passed assert_runner_target and must
    have the migrations applied (the attempts table exists). Autocommit must
    be disabled: the advisory lock and exact verification protect one target
    transaction, not a sequence of independently committed statements.
    """
    if getattr(target_conn, "autocommit", False):
        raise SystemExit(
            "attempts copy requires target autocommit=False so its lock, writes,"
            " exact verification, and rollback share one transaction."
        )
    with source_conn.cursor() as cur:
        cur.execute(SOURCE_QUERY)
        plan = build_plan(cur.fetchall())
    # Fail closed on malformed/unresolved/cyclic/branched source topology
    # before taking the target lock. Dry-run validates this source plan but
    # deliberately predicts no post-copy target metrics.
    expected = _plan_expectations(plan)

    try:
        with target_conn.cursor() as cur:
            # Without the lock, two empty-target preflights could both insert
            # the whole history: attempt ordinals are intentionally non-unique.
            cur.execute(COPY_LOCK_QUERY, (project,))
            cur.execute(
                "SELECT count(*) FROM attempts WHERE project_id = %s", (project,)
            )
            existing = cur.fetchone()[0]

        if existing:
            summary = _verify_exact(
                target_conn,
                plan,
                expected,
                project,
                note="already copied — exact source-plan match",
            )
            target_conn.commit()  # releases the transaction-scoped lock
            _report(summary)
            return summary

        if dry_run:
            summary = {
                "note": (
                    "dry run — source plan is safe; no rows copied and no "
                    "post-copy target metrics claimed"
                ),
                "source_rows_planned": plan.source_count,
                "target_rows_observed_before_copy": existing,
                "source_chain_links_planned": plan.linked_count,
                "source_malformed_pairs": plan.malformed_count,
            }
            target_conn.rollback()  # no writes; releases the lock explicitly
            _report(summary)
            return summary

        # One transaction: inserts, re-link, depth backfill, exact verify.
        new_ids: dict[tuple[str, str, int], int] = {}
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
                cur.execute(
                    "UPDATE attempts SET consumed_by_attempt_id = %s "
                    "WHERE id = %s AND project_id = %s",
                    (new_ids[row.consumed_by], new_ids[row.old_triple], project),
                )
                if cur.rowcount != 1:
                    raise SystemExit(
                        "consumer-link update did not affect exactly one row;"
                        " refusing the copy."
                    )
            cur.execute(
                BACKFILL_QUERY,
                {"project": project, "max_depth": BACKFILL_MAX_DEPTH},
            )
        # This reads the uncommitted rows on the same connection. Any field,
        # FK, or depth mismatch raises into the explicit rollback below.
        summary = _verify_exact(
            target_conn, plan, expected, project, note="copied — exact match"
        )
        target_conn.commit()
    except BaseException:
        target_conn.rollback()
        raise
    _report(summary)
    return summary


def _report(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        print(f"{key}: {value}")
