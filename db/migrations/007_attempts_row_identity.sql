-- 007: the attempt ROW ID is the attempt's identity.
--
-- 003 keyed attempts on (project_id, job_key, attempt), reasoning only about
-- the step-9 HISTORY COPY. The constraint is wrong for LIVE writes too, and
-- the store code cannot be written against it:
--
--   * attempt numbers legitimately repeat for one job. jobs.attempt_count is
--     reset by every requeue and by --force-rerun, so a later run's attempt 1
--     is a DIFFERENT attempt from the earlier run's attempt 1. Under the old
--     schema run_id was part of the key and kept them apart; here they
--     collide, and an upsert on collision overwrites the earlier row —
--     including the session_ref that resume exists to find.
--   * the resume chain is a self-FK now (consumed_by_attempt_id), so the
--     consuming statement needs the CLAIMING attempt's row id. Resolving it
--     from (project_id, job_key, attempt) is ambiguous exactly when the
--     numbers repeat, and resolves to nothing at all on the ordering the
--     engine actually uses (claim first, insert after).
--
-- So: drop the uniqueness, keep the column as a display ordinal, and let
-- attempts.id — which the insert already returns — be what every later write
-- (session ref, outcome, consumption) addresses. The lookup index stays,
-- non-unique, because the number is still what the UI and the events stream
-- print beside an attempt.
--
-- Consequence for the step-9 copy: no renumbering is REQUIRED any more (the
-- constraint that forced it is gone), so a straight copy of the old attempt
-- numbers is now valid. db/copy_attempts.py still renumbers per job, because
-- repeated ordinals read as duplicates in the run view; that is now a
-- cosmetic choice, not a constraint.

DO $$
DECLARE
  constraint_name text;
BEGIN
  SELECT conname INTO constraint_name
    FROM pg_constraint
   WHERE conrelid = 'attempts'::regclass
     AND contype = 'u'
     AND pg_get_constraintdef(oid) = 'UNIQUE (project_id, job_key, attempt)';
  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE attempts DROP CONSTRAINT %I', constraint_name);
  END IF;
END
$$;

-- Same columns, no uniqueness: "the attempts of this job, in order" is still
-- a first-class lookup (run view, event pane, the copy tool's plan).
CREATE INDEX IF NOT EXISTS attempts_job_attempt_idx
  ON attempts (project_id, job_key, attempt);

COMMENT ON COLUMN attempts.attempt IS
$$
Display ordinal within the job's CURRENT run — not an identity. Requeue and
--force-rerun both reset jobs.attempt_count, so this number repeats across
runs for the same job (007 dropped the uniqueness that assumed otherwise).
The identity of an attempt is its row id, which is what the session-ref,
outcome and consumption writes address.
$$;

COMMENT ON TABLE attempts IS
$$
One row per agent attempt launch; the session-resume registry.

CUTOVER NOTE — read before writing the step-9 history copy. Two parts; 007
retired a third (the unique key that forced a dedupe-or-renumber pass, see
that migration's header).

1. THE RESUME CHAIN IS RE-LINKED, NEVER COPIED. The old chain is stored as
   the PAIR (consumed_by_run_id, consumed_by_attempt) — matched two columns
   at a time because run_id was part of the old key. Here it is one
   surrogate, consumed_by_attempt_id -> attempts(id), and surrogate ids do
   not exist until the rows are inserted. So the copy is two passes, in this
   order:
     a. Insert with consumed_by_attempt_id NULL, remembering each row's OLD
        (run_id, job_stable_id, attempt) triple against the new id.
     b. UPDATE attempts SET consumed_by_attempt_id = the new id of the row
        whose OLD triple equals this row's (consumed_by_run_id,
        job_stable_id, consumed_by_attempt) — resolved through the mapping
        from (a), never through target-table coordinates (a renumbering pass
        edits exactly those).
   A consumer that fails to resolve would leave NULL behind, which reads as
   'unconsumed' and makes a dead session resumable again. The sanctioned
   copy refuses unresolved full pairs and half-present pairs before writing;
   it never accepts that lossy state.

2. resume_depth NEEDS A BACKFILL. It is DEFAULT 0 and has no source column:
   the old schema computed depth by walking the chain with a recursive CTE
   at claim time. A straight INSERT..SELECT therefore hands EVERY
   already-exhausted session a fresh budget at cutover — re-arming the
   resume loop RESUME_BUDGET exists to stop, on precisely the sessions that
   earned their way out of it. DIRECTION MATTERS: depth 0 belongs to rows
   that CONSUMED NOTHING (no other row names them as its consumer), and
   each consumer is its consumed row's depth + 1 — that is what makes the
   nomination predicate `resume_depth < budget` equal the old claim-time
   walk, which counts the chain BEHIND the candidate. Anchoring 0 at the
   unconsumed heads and walking the other way INVERTS the meaning and
   re-arms every exhausted chain. After the pass-1 re-link, walk once:
       WITH RECURSIVE chain AS (
         SELECT a.id, 0 AS depth FROM attempts a
          WHERE NOT EXISTS (SELECT 1 FROM attempts x
                            WHERE x.consumed_by_attempt_id = a.id)
         UNION ALL
         SELECT a.consumed_by_attempt_id, chain.depth + 1 FROM attempts a
           JOIN chain ON a.id = chain.id
          WHERE a.consumed_by_attempt_id IS NOT NULL)
       UPDATE attempts SET resume_depth = chain.depth FROM chain
        WHERE attempts.id = chain.id;
   Run it ONCE, at cutover only — db/copy_attempts.py does exactly this.
   Afterwards the insert path precomputes depth as parent + 1 and no walk
   ever runs again.

Column successors: lease_ref <- run_id, session_ref <- session_id,
workspace_ref <- attempt_dir, error_code <- failure_category.

DELIBERATELY ABSENT — pipeline_attempts columns with NO successor, dropped
on purpose by the step-9 copy (the counterpart of 002's block of the same
name):
- consumed_at. The consumption TIMESTAMP is gone; the consumption FACT is
  `consumed_by_attempt_id IS NOT NULL`, which is what every predicate in
  the claim path actually tests, and the time is the consuming row's
  created_at one hop away. Releasing a claim becomes
  `SET consumed_by_attempt_id = NULL` — one column, so unconsume can no
  longer half-clear a pair.
- phase. An attempt hangs off its job_key and the task vocabulary lives on
  jobs.task_type. Copying it here would put caller vocabulary in a second
  place to drift.
$$;
