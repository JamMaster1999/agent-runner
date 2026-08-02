-- 003: attempts — successor of GTM's pipeline_attempts (migration 026), the
-- DB-driven session-resume registry.
--
-- One row per agent attempt launch, keyed by a prompt fingerprint (the
-- SHA-256 of the PRE-substitution prompt template, so run-varying tokens
-- never enter it). A later run resumes an interrupted session by atomically
-- consuming the newest unconsumed row with the same job + fingerprint, which
-- makes resume decisions idempotent and race-free across restarts. The
-- session transcripts themselves live wherever the harness put them
-- (workspace_ref points at the attempt's workspace).

CREATE TABLE IF NOT EXISTS attempts (
  id                     bigserial PRIMARY KEY,
  project_id             text NOT NULL REFERENCES projects(project_id),
  job_key                text NOT NULL,
  attempt                integer NOT NULL,

  harness                text NOT NULL,
  account_id             bigint,
  lease_ref              text NOT NULL,

  request_identity       text,
  prompt_fingerprint     text NOT NULL,
  session_ref            text,
  workspace_ref          text NOT NULL,

  outcome                text,
  error_code             text,
  outcome_code           text,

  consumed_by_attempt_id bigint REFERENCES attempts(id),
  resume_depth           integer NOT NULL DEFAULT 0,

  tok_input              bigint,
  tok_cache_write        bigint,
  tok_cache_read         bigint,
  tok_output             bigint,
  cost_usd               numeric,

  created_at             timestamptz NOT NULL DEFAULT now(),
  finished_at            timestamptz,

  UNIQUE (project_id, job_key, attempt),
  CHECK (outcome IS NULL OR outcome IN ('succeeded', 'failed'))
);

-- Mirrors pipeline_attempts_resume_idx: the nomination SELECT reads the
-- newest unconsumed row carrying a session for this job + fingerprint.
CREATE INDEX IF NOT EXISTS attempts_resume_idx
  ON attempts (project_id, job_key, prompt_fingerprint, id DESC)
  WHERE session_ref IS NOT NULL AND consumed_by_attempt_id IS NULL;

COMMENT ON TABLE attempts IS
$$
One row per agent attempt launch; the session-resume registry.

CUTOVER HAZARD — read before writing the step-9 history copy. THREE parts,
all consequences of the same key change; part 1 alone is not enough.

1. UNIQUENESS. Today's pipeline_attempts is unique on
   (run_id, job_stable_id, attempt), so attempt numbers legitimately REPEAT
   across runs for the same job: a --force-rerun resets
   pipeline_jobs.attempt_count to 0 and the next run starts again at
   attempt 1. This table drops run_id from the key — uniqueness is
   (project_id, job_key, attempt). A straight
       INSERT INTO attempts SELECT ... FROM pipeline_attempts
   WILL therefore violate the unique constraint on any job that was ever
   force-rerun. The copy MUST dedupe (keep the newest row per
   (job_key, attempt)) or renumber (dense_rank over run_id, created_at)
   before inserting. Verify the row counts match your chosen rule, not the
   source table's count.

2. THE RESUME CHAIN IS RE-LINKED, NEVER COPIED. The old chain is stored as
   the PAIR (consumed_by_run_id, consumed_by_attempt) — matched two columns
   at a time because run_id was part of the old key. Here it is one
   surrogate, consumed_by_attempt_id -> attempts(id), and surrogate ids do
   not exist until the rows are inserted. Worse: whichever rule part 1
   picks DESTROYS the mapping that pair resolves through, because it edits
   exactly the (run_id, attempt) coordinates the pair names. So the copy is
   two passes, in this order:
     a. Insert with consumed_by_attempt_id NULL, carrying each row's OLD
        (run_id, job_stable_id, attempt) triple in a scratch column or a
        side table.
     b. UPDATE attempts SET consumed_by_attempt_id = the new id of the row
        whose OLD triple equals this row's (consumed_by_run_id,
        job_stable_id, consumed_by_attempt) — resolved through the mapping
        from (a), never through the new key.
   A row whose consumer fails to resolve would land NULL, which reads as
   'unconsumed' and makes a dead session resumable again. The sanctioned
   copy therefore renumbers without dropping rows and refuses any unresolved
   full pair or half-present pair before writing; it never accepts a NULL as
   a lossy substitute for source consumption state.

3. resume_depth NEEDS A BACKFILL. It is DEFAULT 0 and has no source column:
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
   re-arms every exhausted chain. After the pass-2 re-link, walk once:
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

COMMENT ON COLUMN attempts.lease_ref IS
$$
Opaque handle of the lease this attempt ran under (successor of
pipeline_attempts.run_id). Plain text, no FK to leases — leases are pruned,
attempt history is not.

It is no longer part of any key: see the cutover hazard in the table comment.
$$;

COMMENT ON COLUMN attempts.session_ref IS
$$
Harness session / thread id, opaque (successor of session_id). Recorded by
the poll loop as soon as the CLI prints it. A row with a session_ref and no
consumer is what makes an attempt resumable.
$$;

COMMENT ON COLUMN attempts.workspace_ref IS
$$
Opaque handle of the attempt's private workspace (successor of attempt_dir):
a data-root-relative path on a workstation, an object-storage prefix on
Modal. The field never changes shape when the storage does.
$$;

COMMENT ON COLUMN attempts.outcome IS
$$
NULL while running — and forever if the worker died mid-attempt. A NULL
outcome with a session_ref is the classic resumable row. Failure detail
lives in error_code (runner vocabulary) and outcome_code (caller
vocabulary, opaque).
$$;

COMMENT ON COLUMN attempts.consumed_by_attempt_id IS
$$
The attempt that resumed this one, as a single self-FK — replacing the
(consumed_by_run_id, consumed_by_attempt) pair, which had to be matched two
columns at a time because run_id was part of the old key. NULL means
unconsumed, which is the claim-time race guard: the consuming UPDATE keeps
`consumed_by_attempt_id IS NULL` in its predicate, so two claimants can
never resume the same session.

This is the ONLY foreign key between runner tables in this schema.
$$;

COMMENT ON COLUMN attempts.resume_depth IS
$$
How many times THIS session has already been resumed, precomputed at insert
(parent's resume_depth + 1). Replaces the recursive-CTE chain walk in
attempts.claim_resumable_attempt: the resume budget becomes a plain
`resume_depth < N` predicate in the nomination SELECT, with no recursion and
no depth guard. A fresh session is 0, so it always starts with a full budget.
$$;

COMMENT ON COLUMN attempts.prompt_fingerprint IS
'SHA-256 of the pre-substitution prompt template. Equal fingerprints mean two attempts received identical work — the definition of a resumable pair.';
