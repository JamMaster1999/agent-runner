-- 002: jobs — successor of GTM's pipeline_jobs (migrations 009 + 031 + 032).
--
-- A job is identified by the opaque pair (project_id, job_key) and grouped by
-- the opaque group_key. GTM keeps writing '{inst}__{phase}__{backend}' as its
-- job_key and the institution stable_id as its group_key; the runner treats
-- both as strings and never splits them.
--
-- What used to be columns of GTM vocabulary is now submit-time DATA:
-- agent_ref / prompt_ref / artifact_contract / probe_spec / resource_specs /
-- required_env / policy carry the SubmitRequest payload as jsonb, so a new
-- phase, a new unit granularity or a new output layout needs no migration.
--
-- ONE DECLARED DIVERGENCE from the schema authority (design doc §4), listed
-- in full in docs/schema.md §6: prompt_ref is `jsonb` NULL here, not
-- `text NOT NULL`. See its column comment for the reason and the cost.
-- artifact_contract and policy match the authority exactly — NOT NULL with
-- NO default, so a job cannot be submitted without an output contract or a
-- retry policy (a DEFAULT '{}' here would have made both silently optional).

CREATE TABLE IF NOT EXISTS jobs (
  id                  bigserial PRIMARY KEY,
  project_id          text NOT NULL REFERENCES projects(project_id),
  job_key             text NOT NULL,
  group_key           text NOT NULL,

  task_type           text NOT NULL,
  harness             text NOT NULL,
  agent_ref           jsonb NOT NULL,
  labels              jsonb NOT NULL DEFAULT '{}'::jsonb,

  prompt_ref          jsonb,
  request_identity    text,
  artifact_contract   jsonb NOT NULL,
  probe_spec          jsonb,
  resource_specs      jsonb,
  required_env        text[],
  policy              jsonb NOT NULL,

  status              text NOT NULL DEFAULT 'queued',
  attempt_count       integer NOT NULL DEFAULT 0,
  max_attempts        integer NOT NULL DEFAULT 3,
  next_retry_at       timestamptz,

  progress_current    integer NOT NULL DEFAULT 0,
  progress_total      integer,
  progress_message    text,
  progress_updated_at timestamptz,

  claimed_by          text,
  claimed_at          timestamptz,
  lease_ref           text,
  account_id          bigint,

  error_code          text,
  outcome_code        text,
  error_message       text,
  error_details       jsonb NOT NULL DEFAULT '{}'::jsonb,

  started_at          timestamptz,
  finished_at         timestamptz,
  heartbeat_at        timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),

  UNIQUE (project_id, job_key),

  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'blocked', 'cancelled')),
  CHECK (progress_current >= 0),
  CHECK (progress_total IS NULL OR progress_total >= 0),
  CHECK (progress_total IS NULL OR progress_current <= progress_total),
  CHECK (attempt_count >= 0),
  CHECK (max_attempts > 0),
  CHECK (jsonb_typeof(error_details) = 'object'),
  CHECK (jsonb_typeof(artifact_contract) = 'object'),
  CHECK (jsonb_typeof(policy) = 'object')
);

CREATE INDEX IF NOT EXISTS jobs_project_group_idx
  ON jobs (project_id, group_key);

CREATE INDEX IF NOT EXISTS jobs_project_status_retry_idx
  ON jobs (project_id, status, next_retry_at);

-- The reaper predicate: status = 'running' AND heartbeat_at stale, global
-- across projects (the stale threshold is what protects concurrent groups).
CREATE INDEX IF NOT EXISTS jobs_status_heartbeat_idx
  ON jobs (status, heartbeat_at);

CREATE INDEX IF NOT EXISTS jobs_project_lease_idx
  ON jobs (project_id, lease_ref);

COMMENT ON TABLE jobs IS
$$
One row per job the runner owns, successor of GTM's pipeline_jobs.

DELIBERATELY ABSENT — do not add these back:
- institution_id and every other business foreign key. The runner never
  joins a caller's tables; group_key is the only grouping handle it has.
- school_id / department_id / term_id / unit_type / unit_key. Fan-out
  granularity is caller vocabulary; it rides in labels (display only) and
  inside job_key, which the runner does not parse.
- input_path / output_path. Path layout is submit data now
  (artifact_contract: canonical_path, attempt_dir_name, output_filename).
- agent_name (a bare string). agent_ref is jsonb: the AgentDef name, the
  per-harness config table, and the prompt body reference.
- the frozen events jsonb array. The event trail is the events table (004),
  read by id cursor.
- any CHECK on task_type or harness. The pipeline_jobs phase CHECK forced
  five migrations whose entire content was editing a list of caller strings
  (GTM 012, 013, 019, 024, 025). task_type and harness are opaque text here
  and must stay that way.

The CHECKs that ARE present are runner vocabulary and load-bearing.
$$;

COMMENT ON COLUMN jobs.job_key IS
$$
Opaque caller-stable job id, unique within a project. GTM passes
'{institution}__{phase}__{backend}' verbatim; the runner stores and returns
it and never parses it.
$$;

COMMENT ON COLUMN jobs.group_key IS
'Opaque grouping key for list/filter (GTM passes the institution stable_id). Never parsed.';

COMMENT ON COLUMN jobs.task_type IS
$$
Opaque caller task vocabulary, successor of pipeline_jobs.phase.

NO CHECK, ever — see the table comment. Adding a task type is a caller-side
change with no migration.
$$;

COMMENT ON COLUMN jobs.harness IS
'Adapter registry key (successor of pipeline_jobs.backend). Opaque text, no CHECK.';

COMMENT ON COLUMN jobs.agent_ref IS
'AgentDef as data: {name, per-harness config table, prompt body ref}.';

COMMENT ON COLUMN jobs.labels IS
'Display strings only — nothing parses them. Absorbs the old unit_type/unit_key pair.';

COMMENT ON COLUMN jobs.prompt_ref IS
$$
{template, sha256} — the submit-time prompt contract. sha256 MUST be the
resume fingerprint digest of the PRE-substitution template, or prior
sessions stop matching in attempts (003).

DECLARED DIVERGENCE from design §4, which specifies
`prompt_ref text NOT NULL -- template blob, sha-addressed`. Two changes, both
deliberate:

- **jsonb, not text.** The code already carries a pair, not a blob:
  SubmitRequest.prompt_ref is a dict and RunnerJob keeps prompt_template +
  prompt_sha256 side by side. A text column would have to re-encode that
  pair into a string every caller then parses.
- **NULL, not NOT NULL.** Late binding is legal: a template that depends on
  an earlier job's output submits without one, then upserts the template
  before the outcome is awaited.

COST, stated plainly: the template BODY now lives inline in every jobs row
instead of behind a content-addressed reference. That is the storage story
the design's 'sha-addressed' wording avoided. If row size ever bites, the
successor is {ref, sha256} pointing at a blob store — a data change inside
this same jsonb, not a migration.
$$;

COMMENT ON COLUMN jobs.artifact_contract IS
$$
Where this job's output lives, as data: {canonical_path, attempt_dir_name,
output_filename}. Replaces pipeline_jobs.output_path.

NOT NULL with NO DEFAULT, matching design §4. A default of '{}' would let a
job be submitted with no output contract at all and still satisfy every
constraint in this file; the submit path must supply one.
$$;

COMMENT ON COLUMN jobs.policy IS
$$
{max_attempts, backoff[], attempt_timeout_s, resume, resume_budget} — the
retry/resume policy as data.

NOT NULL with NO DEFAULT, matching design §4, for the same reason as
artifact_contract: a job with no retry policy is a submit-time bug, not a
row to accept quietly. Callers that want the runner's defaults send them
explicitly.
$$;

COMMENT ON COLUMN jobs.status IS
$$
Current job state.

queued: ready to run or waiting out next_retry_at.
running: claimed by a worker and heartbeating.
succeeded: completed successfully.
failed: stopped with an error.
blocked: needs manual intervention.
cancelled: intentionally stopped; late writers can never resurrect it.
$$;

COMMENT ON COLUMN jobs.progress_current IS
$$
Denormalized newest progress. The CHECK trio on progress_current /
progress_total is deliberately preserved from pipeline_jobs and is
load-bearing: the event-writing path (src/agent_runner/events.py) sanitizes
these two columns before every update precisely because the CHECK exists —
agents legitimately report 110/107 against an earlier-discovered total, and
without the sanitize the whole statement (including the batched event rows)
would fail. Event rows keep the raw values; events has no such CHECK.
$$;

COMMENT ON COLUMN jobs.lease_ref IS
$$
Opaque handle of the lease this job was claimed under (successor of
pipeline_jobs.run_id). Plain text with NO foreign key to leases: leases are
pruned, job rows outlive them.
$$;

COMMENT ON COLUMN jobs.account_id IS
'Account the claim drew from (accounts.id, migration 006). Plain bigint, no FK: accounts may be retired while job history stays.';

COMMENT ON COLUMN jobs.error_code IS
'Runner failure vocabulary (RunnerError.code). Caller-facing judgment goes in outcome_code, which is opaque.';

COMMENT ON COLUMN jobs.outcome_code IS
'Caller vocabulary, opaque to the runner: the client''s own verdict for this job.';
