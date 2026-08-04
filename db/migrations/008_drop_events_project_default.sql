-- 008: events.project_id loses its DEFAULT — the last place a client's
-- tenant name ('gtm') was baked into the runner schema.
--
-- 004 defaulted project_id because the INSERT-only emitter role cannot look
-- a tenant up from a parent row. That reasoning still holds, but the value
-- no longer needs a schema default: every writer now carries its tenant
-- explicitly — the engine stamps RUNNER_PROJECT_ID into every agent
-- environment and every runner INSERT (engine paths, `agent-runner emit`,
-- lifecycle CTEs) binds project_id from runtime.project_id(), which refuses
-- to run without RUNNER_PROJECT_ID set. A defaulted tenant silently
-- misattributed any writer that forgot to send one; now that writer fails
-- the NOT NULL loudly instead.
--
-- Idempotent: DROP DEFAULT on a column with no default is a no-op.

ALTER TABLE events ALTER COLUMN project_id DROP DEFAULT;

COMMENT ON COLUMN events.project_id IS
$$
The tenant this event belongs to — NOT NULL, no default.

Every writer sends it explicitly from its RUNNER_PROJECT_ID attribution
(the engine stamps the variable into agent environments alongside the other
RUNNER_* names). A writer with no tenant configured fails loudly rather
than having its rows silently attributed to somebody else's tenant.
$$;
