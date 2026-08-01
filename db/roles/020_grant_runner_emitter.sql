-- roles/020: the per-database grants that make runner_emitter INSERT-only.
--
-- Runs on every `agent-runner migrate` (see 010 for why role provisioning is
-- not ledgered): re-applying is the repair path for grant drift, which no
-- ledger can detect. Needs only ownership of the granted objects — the same
-- role that created the tables — so it applies on a managed provider whose
-- app role cannot CREATE ROLE, as long as the role itself already exists.
--
-- THREE consequences of INSERT-only, and how the tables already resolve
-- them:
--
-- (a) Today's event insert resolves the job row first:
--       INSERT INTO pipeline_events ... SELECT id, group_key
--       FROM pipeline_jobs WHERE stable_id = <the job key>
--     That is impossible without SELECT. Resolution, per the opaque-text
--     rule: events.job_key is plain text with no FK and no lookup, and
--     group_key / task_type / harness / attempt are supplied by the writer
--     from its RUNNER_* attribution environment. Do NOT add a (id, key)
--     lookup view and do NOT grant SELECT on anything to work around this —
--     the missing join is the point.
--
-- (b) Today the same statement also UPDATEs the pipeline_jobs progress
--     columns. Under this role that degrades to a pure event append.
--     Ownership of the jobs progress columns moves to the engine's
--     stream-batch path, which connects with the full DSN. So: grant NOTHING
--     on jobs.
--
-- (c) THE EMITTER CANNOT READ BACK THE ID IT JUST WROTE. RETURNING a real
--     column needs SELECT on that column, so
--         INSERT INTO events (...) VALUES (...) RETURNING id
--     fails 'permission denied for table events' while RETURNING 1 (a
--     constant, no column read) succeeds. Today's emit path only ever
--     returns a constant, so nothing breaks — but events.id is the cursor
--     contract (004), and the first emit path that wants its OWN cursor
--     will hit this. It is not a grant to widen: a writer that needs to
--     follow its own events reads them back over the full DSN, or tracks
--     them by the attribution it already supplied.
--
-- Step-9 flip, for the record: only the VALUE of RUNNER_EMIT_DSN changes —
-- from the full DSN to this role's DSN — and the jobs UPDATE drops out of
-- the emit path. No schema change is involved.

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM runner_emitter;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM runner_emitter;

GRANT USAGE ON SCHEMA public TO runner_emitter;
GRANT INSERT ON TABLE events TO runner_emitter;
GRANT USAGE ON SEQUENCE events_id_seq TO runner_emitter;
