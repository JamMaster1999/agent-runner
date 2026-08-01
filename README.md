# agent-runner

Generic agent-CLI job runner, extracted from the Uflo GTM production
pipeline (GTM `docs/runner_extraction_plan.md` §4, step 6). One engine loop
runs every harness; provider differences (Claude Code, Codex) live in
adapter modules under `agent_runner/harness/`. The public interface a client
may import is defined by plan §2: the protocol dataclasses
(`agent_runner.protocol`), the runtime types (`agent_runner.runtime`), the
outcome vocabulary, and `templates.substitute` — GTM's facade
(`core/runner/local.py`) is the one sanctioned deep importer during the
bridge.

## Bridge status (steps 6 → 9)

- The store modules (`jobstore`, `events`, `attempts`) still point at the
  **GTM database with the historical table names**
  (`pipeline_jobs`/`pipeline_runs`/`pipeline_events`/`pipeline_attempts`),
  reached through the DSN the client passes. Runner-owned migrations land at
  step 8; the cutover is step 9. Rollback = revert the client's dependency
  bump.
- Path constants (`util.ROOT`/`PROJECT_ROOT`) resolve through the
  `AGENT_RUNNER_PROJECT_ROOT` environment variable, set by the GTM bootstrap
  shim (`core/_runner_path.py`) or a test header.
- The `core/job_event.py` subprocess hop and the claude/codex hook-twin
  scripts survive verbatim; both die at step 7 (`agent-runner emit`,
  parameterized hook capture).
- **Modal deploys are blocked between step 6 landing and step 10** (wheel
  embedding; do not touch GTM `modal/image.py` before then) — Modal images
  do not contain this sibling checkout.

## Install / run

- Editable install: `pip install -e .` (psycopg ships as a dependency but is
  lazily imported; `import agent_runner` needs neither the driver nor a DB).
- No-pip: put `src/` on `sys.path` (the tests' own headers do this), which
  is exactly the path the GTM bootstrap shim uses on the Mac.
- Tests: `python -m unittest discover tests` — DB-backed tests need psycopg
  plus a reachable Postgres (`GTM_TEST_DATABASE_URL` or the local 55432
  instance) and skip cleanly otherwise; they self-provision a scratch
  database from `tests/fixtures/pipeline_attempts.sql`.

`docs/events.md` is the runner event catalog (moved with the modules).
