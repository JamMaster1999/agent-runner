-- 004: events — successor of GTM's pipeline_events (migrations 015 + 031 +
-- 032), plus the NOTIFY trigger the dashboard listens on.
--
-- Append-only telemetry, one row per event. The bigserial id is THE consumer
-- contract: every reader pages with `WHERE id > $cursor ORDER BY id`, and
-- nothing anywhere parses an event message. Routing columns (job_key,
-- group_key, lease_ref, harness, task_type, attempt) are plain nullable text
-- the writer supplies from its own attribution — there is no FK and no
-- lookup, because the emitter role (db/roles) may only INSERT.
--
-- project_id is the one routing column that CANNOT be NULL, and the one the
-- writer has no way to derive: an INSERT-only role cannot read a parent row
-- to find it. It therefore carries a DEFAULT — see its column comment.

CREATE TABLE IF NOT EXISTS events (
  id               bigserial PRIMARY KEY,
  project_id       text NOT NULL DEFAULT 'gtm',
  job_key          text,
  group_key        text,
  lease_ref        text,
  attempt          integer,

  harness          text,
  task_type        text,
  account_id       bigint,

  kind             text NOT NULL,
  message          text,

  progress_current integer,
  progress_total   integer,

  tok_input        bigint,
  tok_cache_write  bigint,
  tok_cache_read   bigint,
  tok_output       bigint,
  cost_usd         numeric,

  data             jsonb,
  at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_project_job_id_idx
  ON events (project_id, job_key, id);

CREATE INDEX IF NOT EXISTS events_project_group_id_idx
  ON events (project_id, group_key, id);

CREATE INDEX IF NOT EXISTS events_project_lease_id_idx
  ON events (project_id, lease_ref, id);

-- The retention prune predicate: DELETE FROM events WHERE at < now() - interval.
CREATE INDEX IF NOT EXISTS events_at_idx
  ON events (at);

COMMENT ON TABLE events IS
$$
Append-only event trail, successor of pipeline_events.

NO foreign keys, by design. project_id does not reference projects and
job_key does not reference jobs: the restricted runner_emitter role
(db/roles) holds INSERT on this table and nothing else, so an insert can
neither read a parent row nor be validated against one. Every routing column
is therefore whatever the writer supplied from its RUNNER_* attribution
environment — job_key, group_key, lease_ref, harness, task_type, attempt,
and project_id, which alone is NOT NULL and therefore defaulted.
Orphan rows (a job_key with no job) are legal and expected.

Retention: rows older than the runner's retention window are pruned; the
events_at_idx index exists for that DELETE.
$$;

COMMENT ON COLUMN events.id IS
$$
The cursor contract. Consumers read WHERE id > $cursor ORDER BY id and never
parse messages; the NOTIFY payload below carries this id so a listener can
re-query rather than read event bodies off the channel.
$$;

COMMENT ON COLUMN events.project_id IS
$$
The tenant this event belongs to — NOT NULL, and the only routing column an
emitter cannot derive.

DEFAULT 'gtm' matches the single project row 001 seeds, and exists because
of the INSERT-only grant: runner_emitter holds no SELECT anywhere, so an
emit can never look a project up from job_key, and the emit CLI's
attribution environment carries no project concept at all. Without the
default every `agent-runner emit` insert would fail the NOT NULL.

Multi-tenant successor, when a second project row exists: agent
environments carry RUNNER_PROJECT_ID alongside the other RUNNER_*
attribution and the emit path sends it explicitly. The default stays as the
single-tenant floor; it is not a substitute for sending the value.
$$;

COMMENT ON COLUMN events.kind IS
$$
Namespaced event kind: job.* | attempt.* | harness.* | hook.* |
agent.progress | account.*

NO CHECK, deliberately. New harness and hook kinds appear whenever a
provider CLI changes its stream; an enum here would mean a migration per
provider release.
$$;

COMMENT ON COLUMN events.message IS
'DISPLAY ONLY, already redacted. Nothing parses it — typed values live in their own columns.';

COMMENT ON COLUMN events.lease_ref IS
$$
Lease this event was emitted under, matching jobs.lease_ref (successor of
pipeline_events.run_id). Kept because the live write path stamps a run id on
every insert and the run viewer lists a run's events by it. Plain text, no
FK.
$$;

COMMENT ON COLUMN events.progress_current IS
$$
Raw reported progress, unsanitized on purpose. Unlike jobs there is no
CHECK here, so an agent reporting 110/107 lands truthfully in the audit
trail while the denormalized jobs columns get the clamped values.
$$;

COMMENT ON COLUMN events.cost_usd IS
'Typed usage; set only on the turn/result completion kinds whose message renders the number. NULL means the value was never reported.';

-- Payload is deliberately tiny: pg_notify is capped at 8KB, and listeners
-- re-query events WHERE id > $cursor instead of reading bodies off the
-- channel. Channel name is exactly 'runner_events'.
--
-- FOUR keys, not the design sketch's three. job_key is here because the one
-- live listener routes on TWO things, not one: the dashboard's runner-frame
-- handler uses the job key to pick the institution (group_key replaces that
-- correctly) AND to decide whether the frame belongs to the job whose event
-- pane is currently open. Nothing else in a three-key payload can answer the
-- second question, so without job_key the open job's live feed silently
-- degrades to the poll fallback at step 9. One more short string is nowhere
-- near the 8KB cap.
--
-- No elevated rights needed: pg_notify requires no privilege, so the
-- INSERT-only runner_emitter role fires this trigger fine. Do NOT mark it
-- SECURITY DEFINER — that would run every event insert as the owner.
CREATE OR REPLACE FUNCTION runner_events_notify() RETURNS trigger AS $fn$
BEGIN
  PERFORM pg_notify('runner_events', json_build_object(
    'id', NEW.id,
    'project_id', NEW.project_id,
    'job_key', NEW.job_key,
    'group_key', NEW.group_key,
    'kind', NEW.kind
  )::text);
  RETURN NULL;
END
$fn$ LANGUAGE plpgsql;

-- OR REPLACE (PG14+) keeps this file re-runnable top to bottom.
CREATE OR REPLACE TRIGGER runner_events_notify_trg
  AFTER INSERT ON events
  FOR EACH ROW EXECUTE FUNCTION runner_events_notify();

COMMENT ON FUNCTION runner_events_notify() IS
'Fires the tiny id-cursor payload on the runner_events channel after each insert. Not SECURITY DEFINER: pg_notify needs no privilege.';
