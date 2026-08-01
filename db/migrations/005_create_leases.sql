-- 005: leases — the LEASE half of GTM's pipeline_runs (migration 032).
--
-- pipeline_runs was two things at once: a lock, and a run summary. Only the
-- lock is the runner's. The run summary — what the pipeline manager thinks
-- happened, including its succeeded_with_failures verdict — is GTM's and
-- lives in GTM's enrichment_runs. Nothing in this schema knows that word.
--
-- Lease, not advisory lock: the runner's DB access is short-lived one-shot
-- connections, so a session-scoped lock cannot be held across an agent run.
-- The partial unique index below is the actual lock; a holder whose
-- heartbeat_at goes stale is flipped to 'expired' by the next acquirer inside
-- the same transaction, so takeover is race-free.

CREATE TABLE IF NOT EXISTS leases (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id    text NOT NULL REFERENCES projects(project_id),
  lease_ref     text NOT NULL,
  lease_key     text NOT NULL,
  holder        text NOT NULL,
  status        text NOT NULL DEFAULT 'held',
  kind          text NOT NULL DEFAULT 'exclusive',
  labels        jsonb NOT NULL DEFAULT '{}'::jsonb,

  outcome       text,
  error_code    text,
  error_message text,

  started_at    timestamptz NOT NULL DEFAULT now(),
  heartbeat_at  timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,

  CHECK (status IN ('held', 'released', 'expired')),
  CHECK (kind IN ('exclusive', 'tracked_task')),
  CHECK (outcome IS NULL OR outcome IN ('succeeded', 'failed'))
);

-- THE lock: one held lease per (project, lease_key). The acquire path relies
-- on this index existing — it inserts with
--   ON CONFLICT (project_id, lease_key) WHERE status = 'held' DO NOTHING
-- and reads the returned row as proof it holds the lease.
--
-- It is also the ONLY uniqueness on this table, deliberately. lease_ref
-- carries NO unique constraint: a global UNIQUE on it would be a DIFFERENT
-- constraint from this index, so ON CONFLICT (project_id, lease_key) cannot
-- absorb it and DO NOTHING cannot swallow it — the insert would raise and
-- abort the caller's transaction instead of reporting 'someone else holds
-- it'. Two everyday acquires hit exactly that: one holder taking a SECOND
-- lease_key under the same handle (kind='tracked_task' writes many such rows
-- per run), and re-acquiring a lease_key after release with the same handle.
-- A global UNIQUE would also be unscoped by project_id, so two tenants could
-- never reuse a handle string — against 001's structural multi-tenancy.
CREATE UNIQUE INDEX IF NOT EXISTS leases_one_held_per_key
  ON leases (project_id, lease_key) WHERE status = 'held';

-- Lookup by handle: 'every lease this run took'. Non-unique, per the above.
CREATE INDEX IF NOT EXISTS leases_project_lease_ref_idx
  ON leases (project_id, lease_ref);

-- The reaper predicate: status = 'held' AND heartbeat_at older than the
-- stale window. Takeover happens inside the ACQUIRING transaction (see the
-- header), so this index sits on the hot path of every acquire — jobs got
-- the identical index for the identical need (002, jobs_status_heartbeat_idx).
CREATE INDEX IF NOT EXISTS leases_status_heartbeat_idx
  ON leases (status, heartbeat_at);

COMMENT ON TABLE leases IS
$$
Named exclusivity with stale takeover — the lock half of pipeline_runs.

STATUS MAPPING from today's pipeline_runs vocabulary, for the step-9 copy:
  running                   -> held
  succeeded                 -> released
  succeeded_with_failures   -> released
  failed                    -> released
  cancelled                 -> released
  abandoned                 -> expired

STATUS has no opinion about how the work went; release is release, and the
three statuses stay lease states. In particular succeeded_with_failures is a
GTM RUN outcome living in GTM's enrichment_runs and must NOT appear anywhere
in this schema.

The TERMINAL RECORD is a separate axis: outcome / error_code / error_message
(design D9(a) — claim-dedupe, heartbeat, terminal record). A tracked_task is
a deterministic unit of work with a real verdict, and the protocol's
fail_task op needs somewhere to land it; today's failure path writes a
blocked status plus an error message and would otherwise have no column
here. Status says whether the lease is still held; outcome says how the work
ended. An exclusive lease normally leaves all three NULL.

gen_random_uuid() is built into PostgreSQL since 13 — no pgcrypto extension
is required.
$$;

COMMENT ON COLUMN leases.lease_ref IS
$$
The opaque text handle jobs.lease_ref, events.lease_ref and
attempts.lease_ref point at (today's run_id string). Those columns carry NO
foreign key to this table on purpose: leases get pruned, while job, event
and attempt rows outlive them.

NOT UNIQUE, and it must stay that way — one handle covers every lease that
run took, and the acquire path breaks the moment a second constraint exists
on this table. The reasoning is with leases_one_held_per_key above.
$$;

COMMENT ON COLUMN leases.lease_key IS
'What is being locked, opaque to the runner (today: the institution stable_id). The partial unique index makes this the exclusivity name.';

COMMENT ON COLUMN leases.holder IS
'Worker identity holding the lease, formatted host:pid.';

COMMENT ON COLUMN leases.status IS
$$
held: this row owns lease_key right now.
released: the holder finished, however the work went.
expired: the holder stopped heartbeating and a later acquirer reaped it.
$$;

COMMENT ON COLUMN leases.kind IS
$$
exclusive: a real lock — at most one held row per lease_key.
tracked_task: a claim-dedupe record for a deterministic (import) task, which
uses the same held/released/expired lifecycle but is never killed on cancel,
only flagged. This is the kind that fills outcome / error_code /
error_message: a tracked task has a verdict, and D9(a) requires it be
recorded.
$$;

COMMENT ON COLUMN leases.outcome IS
$$
How the work under this lease ended: succeeded | failed, NULL while held (or
forever, for an exclusive lock nobody judges).

Deliberately NOT folded into status. status is the lock's own lifecycle and
a lease is released the same way whatever happened; the verdict is the
tracked-task half of D9(a). Same two-word vocabulary as attempts.outcome,
and no caller words: a client's own judgment is opaque and belongs in its
tables, never in this CHECK.
$$;

COMMENT ON COLUMN leases.error_code IS
'Runner failure vocabulary (RunnerError.code) for a failed tracked task — same column meaning as jobs.error_code.';

COMMENT ON COLUMN leases.error_message IS
'Display-only failure text for a failed tracked task, already redacted. Nothing parses it.';

COMMENT ON COLUMN leases.heartbeat_at IS
'Freshness of the holder. Older than the acquirer''s stale window means the lease is reapable to expired in the acquiring transaction.';
