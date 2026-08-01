-- 001: projects — the runner's tenant row and the FK anchor the rest of the
-- runner-owned schema hangs off (extraction step 8).
--
-- Step 8 stops the compat bridge: until now the runner has written GTM's
-- pipeline_jobs / pipeline_events / pipeline_runs / pipeline_attempts in the
-- GTM database. These migrations author the runner's OWN tables under their
-- final names — projects, jobs, attempts, events, leases, accounts,
-- account_usage. Step 9 flips the code onto them and copies attempt history.
--
-- Two rules hold across every file here:
--
-- 1. Cross-database references are OPAQUE TEXT, never foreign keys. job_key,
--    group_key, lease_ref, session_ref and workspace_ref are strings the
--    runner stores and hands back but never parses, and nothing here points
--    at a GTM table. The single sanctioned FK between runner tables is
--    attempts.consumed_by_attempt_id -> attempts(id) (the resume chain).
-- 2. No caller vocabulary in CHECK constraints. task_type and harness are
--    opaque text with no CHECK at all: the old pipeline_jobs phase CHECK
--    cost five churn migrations (GTM 012, 013, 019, 024, 025) and this
--    schema makes that class of migration structurally impossible.
--
-- Files apply in filename order and each depends on the previous. Every file
-- is idempotent top to bottom, and plain SQL only — the applier pipes whole
-- files through psycopg, so psql meta-commands and psql variables are out.

CREATE TABLE IF NOT EXISTS projects (
  project_id  text PRIMARY KEY,
  name        text,
  token_hash  text,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- The single tenant of the bridge era. Re-running is a no-op.
INSERT INTO projects (project_id, name)
VALUES ('gtm', 'Uflo GTM pipeline')
ON CONFLICT DO NOTHING;

COMMENT ON TABLE projects IS
$$
One row per client tenant. Every runner-owned table is keyed by project_id,
so a second client is a row here rather than a schema change.

Multi-tenancy is structural, not yet enforced: the runner has no HTTP
binding today, so callers reach the store in-process with the full DSN and
there is exactly one project row ('gtm').
$$;

COMMENT ON COLUMN projects.token_hash IS
$$
Hash of the project's API token — never the token itself.

Token issuance is stubbed single-tenant until the HTTP binding exists, so
this column stays NULL for now. The column is here so that turning auth on
is a provisioning step, not a migration.
$$;
