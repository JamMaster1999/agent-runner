-- Byte-identical snapshot of GTM db/migrations/026_create_pipeline_attempts.sql
-- (extraction step 6). This is the COMPAT-BRIDGE schema: the runner still
-- points at the GTM database with the GTM table names; runner-owned
-- migrations supersede this snapshot at extraction step 8.

-- Session-resume registry: one row per agent attempt launch, keyed by a
-- prompt fingerprint (the prompt text with run-varying tokens — run id,
-- attempt number, output path, RESUME preamble — stripped). A later run
-- resumes an interrupted session by atomically consuming the newest
-- unconsumed row with the same job + fingerprint, so resume decisions are
-- idempotent and race-free across orchestrator restarts. The session
-- transcripts themselves stay on the worker's disk (~/.claude, ~/.codex)
-- until the virtual-fs migration.

CREATE TABLE IF NOT EXISTS pipeline_attempts (
  id                  bigserial PRIMARY KEY,
  job_stable_id       text NOT NULL,
  run_id              text NOT NULL,
  attempt             integer NOT NULL,
  phase               text NOT NULL,
  backend             text NOT NULL,
  prompt_fingerprint  text NOT NULL,
  session_id          text,
  attempt_dir         text NOT NULL,
  outcome             text,
  failure_category    text,
  consumed_by_run_id  text,
  consumed_by_attempt integer,
  consumed_at         timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),
  finished_at         timestamptz,

  UNIQUE (run_id, job_stable_id, attempt),
  CHECK (outcome IS NULL OR outcome IN ('succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS pipeline_attempts_resume_idx
  ON pipeline_attempts (job_stable_id, prompt_fingerprint, id DESC)
  WHERE session_id IS NOT NULL AND consumed_by_run_id IS NULL;

COMMENT ON TABLE pipeline_attempts IS
'One row per agent attempt launch; the DB-driven session-resume registry.';
COMMENT ON COLUMN pipeline_attempts.session_id IS
'Claude session id / Codex thread id, recorded by the orchestrator poll loop as soon as the CLI prints it.';
COMMENT ON COLUMN pipeline_attempts.outcome IS
'NULL while running — and forever if the orchestrator died mid-attempt. A NULL outcome with a session_id is what makes an attempt resumable; failed attempts are resumable only for rate_limit/timeout categories.';
