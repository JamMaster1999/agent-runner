-- roles/010: runner_emitter — the CLUSTER-GLOBAL half of the restricted
-- credential agent processes emit with. Grants are the next file.
--
-- NOT A MIGRATION, and not in db/migrations, for three reasons:
--
-- 1. CREATE ROLE is cluster-global while the ledger is per-database. A
--    ledgered role file records "applied" in one database about an object
--    that lives outside it, and re-running the chain against a second
--    database in the same cluster would claim to create a role that is
--    already there.
-- 2. The ledger cannot see grant drift. One REVOKE by an operator and the
--    applier still says 'No pending migrations' forever after, with emits
--    failing 'permission denied for table events' and no repair path. Role
--    provisioning is therefore RE-APPLIED ON EVERY `agent-runner migrate`,
--    idempotently: running it is the repair.
-- 3. It needs a privilege the schema chain does not. CREATE ROLE requires
--    superuser or CREATEROLE-with-admin; the table chain needs neither. As
--    the last numbered migration this file aborted the run AFTER 001-006
--    had committed, leaving a half-provisioned database with a green ledger
--    on any managed provider whose app role is not privileged (Railway at
--    step 9, PlanetScale at step 12). Split out, the tables land and ledger
--    cleanly and only role provisioning fails — repairable on its own with
--    `agent-runner migrate --roles-only` run as a privileged role.
--
-- REQUIRED PRIVILEGE: superuser, or CREATEROLE. The applier checks
-- (pg_roles.rolsuper OR rolcreaterole for current_user) before running this
-- file: unprivileged AND the role already exists, it skips this file and
-- still applies the grants; unprivileged AND the role is missing, it stops
-- and prints this file's path for a superuser to run. `--skip-roles`
-- suppresses both.
--
-- WHAT THE ROLE IS FOR: an agent CLI runs untrusted-ish code in a workspace
-- and needs to append events. Today it does that with the full DSN, which
-- means an agent that reads its own environment holds write access to every
-- table. This role holds INSERT on events and NOTHING else — see 020 for
-- the grants and the three consequences of INSERT-only.

DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'runner_emitter') THEN
    CREATE ROLE runner_emitter LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END
$role$;

-- NO PASSWORD HERE, and never add one: a literal password in a git-tracked
-- file is a credential leak, and writing PASSWORD with a colon-quoted psql
-- variable would break the applier, which pipes whole files through psycopg
-- and has no psql variables at all. The role is created LOGIN with no
-- password, so it cannot authenticate under scram until provisioned.
--
-- OPERATOR, at step-9 provisioning, out of band and never committed:
--     ALTER ROLE runner_emitter PASSWORD '<generated>';
-- then put that DSN in RUNNER_EMIT_DSN only.

COMMENT ON ROLE runner_emitter IS
'Event-append-only credential for agent processes: INSERT on events plus its sequence, nothing else. Password provisioned out of band; the DSN belongs in RUNNER_EMIT_DSN only.';
