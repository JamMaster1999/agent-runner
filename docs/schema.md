# Runner DB Schema

What `db/migrations/` and `db/roles/` in this repo create, how each table
maps back to the GTM `pipeline_*` table it succeeds, the two rules that
shape the whole thing, and every divergence from the design doc (§6).
Authored at extraction step 8; the code still writes the old tables until
step 9 flips it (`docs/runner_extraction_plan.md` §3–§4 in GTM, design
doc §4).

## 1. Where the runner's state lives

Two schemas exist at once during the bridge:

- **Until the step-9 cutover**, the package writes GTM's tables in the GTM
  database — `pipeline_jobs`, `pipeline_events`, `pipeline_runs`,
  `pipeline_attempts` — reached through the DSN the client passes. That is
  the compat bridge, not the destination.
- **`db/migrations/` here** is the runner's own schema under its final
  names, applied to the runner's own database. Nothing applies it to the
  GTM database, ever.

That last sentence is enforced by code, not by trust. The table names here
are generic (`jobs`, `events`), the emitter role is cluster-global, and
during the bridge `runner_dsn` still points at GTM — so "applied to the
wrong database" is the likely operator mistake, not an exotic one. Two
guards, both in `agent_runner.migrations`:

1. **The DSN never falls back to `DATABASE_URL`.** It resolves
   `--database-url` > `RUNNER_DSN` and stops. `DATABASE_URL` is the
   *client's* variable: GTM's `core/db.py` reads exactly it, this repo's own
   CI `full` job sets it to the GTM database, and so does the Modal Secret.
2. **The applier refuses a client-looking target** before writing anything:
   any GTM business or `pipeline_*` table present, or any ledger row under a
   foreign prefix (`graph-migrations/`, `crm-migrations/` — GTM shares that
   ledger with the CRM runner). The refusal names the database and the
   tells. `--i-know-this-is-the-runner-db` is the only way through, and it
   warns on every apply instead of going quiet.

The runner DB is hosted on **Railway from cutover, not the local Postgres
cluster**. Two consumers already reach only Railway — Modal containers and
the deployed dashboard — and no sync will ever cover runner tables, so a
local-cluster runner DB would blind both. One runner DB per deployment: Mac
runs and Modal runs are the same deployment, writing the same rows.

## 2. Rename map

### `pipeline_jobs` → `jobs`

| `pipeline_jobs` | `jobs` | Note |
|---|---|---|
| `stable_id` | `job_key` | opaque text; identity is `UNIQUE (project_id, job_key)` |
| `phase` | `task_type` | opaque, no CHECK |
| `backend` | `harness` | adapter registry key |
| `agent_name` | `agent_ref` | jsonb `AgentDef` (name + harness config table + body ref) |
| `output_path` | `artifact_contract.canonical_path` | jsonb contract, not a bare path column |
| `run_id` | `lease_ref` | |
| `group_key`, `labels` | same names | carried over from GTM migration 031 |
| status, progress\_\*, attempt/retry, claim, heartbeat, timestamps | same names | unchanged |
| `error_message`, `error_details` | same names | joined by `error_code` (runner vocabulary) and `outcome_code` (caller vocabulary, opaque) |

Dropped outright: `institution_id` and every business FK,
`school_id`/`department_id`/`term_id`, `unit_type`/`unit_key` (→ `labels`),
`input_path`, the frozen `events` jsonb, and the phase CHECK.

### `pipeline_events` → `events`

| `pipeline_events` | `events` | Note |
|---|---|---|
| `id` | `id` | still the cursor contract for every consumer |
| `job_id` | — | gone: no FK across a database boundary |
| `job_stable_id` | `job_key` | opaque text, **no** `job_id` FK |
| `event` | `kind` | namespaced: `job.*` \| `attempt.*` \| `harness.*` \| `hook.*` \| `agent.progress` \| `account.*` |
| `run_id` | `lease_ref` | |
| `phase` / `backend` | `task_type` / `harness` | |
| `message` | `message` | display-only; consumers read columns, never parse it |
| `tok_*`, `cost_usd`, `group_key` | same names | unchanged |

### `pipeline_attempts` → `attempts`

| `pipeline_attempts` | `attempts` | Note |
|---|---|---|
| `job_stable_id` | `job_key` | |
| `run_id` | `lease_ref` | |
| `backend` | `harness` | |
| `phase` | — | **dropped, no successor**: an attempt hangs off its `job_key`, and the task vocabulary lives once, on `jobs.task_type` |
| `session_id` | `session_ref` | opaque harness session/thread id |
| `attempt_dir` | `workspace_ref` | local path now, object prefix on Modal |
| `failure_category` | `error_code` | |
| `consumed_at` | — | **dropped, no successor**: the consumption *fact* is `consumed_by_attempt_id IS NOT NULL` (what every claim predicate tests) and the *time* is the consuming row's `created_at`. Unconsume becomes one column, so it can no longer half-clear a pair |
| `consumed_by_run_id` + `consumed_by_attempt` | `consumed_by_attempt_id` | self-FK; the resume chain becomes one column |
| — | `resume_depth` | new, precomputed — replaces the recursive-CTE budget walk |
| `UNIQUE (run_id, job_stable_id, attempt)` | `UNIQUE (project_id, job_key, attempt)` | see the copy hazard in §5 — three parts, not one |

### `pipeline_runs` → `leases`

Its **lease function only**: `run_id` → `lease_ref`, `institution_id` →
`lease_key` (opaque), `claimed_by` → `holder`. Status maps
`running` → `held`; `succeeded` / `succeeded_with_failures` / `failed` /
`cancelled` → `released`; `abandoned` → `expired`.

The run *summary* is GTM's and lives in GTM's `enrichment_runs`.
`succeeded_with_failures` deliberately does not exist in this schema — it is
a caller's judgment of a run, not a lease state.

Two things about this table are easy to get wrong:

- **`lease_ref` carries no UNIQUE.** The partial unique index
  `(project_id, lease_key) WHERE status='held'` is the table's only
  uniqueness, and it must stay that way: a second constraint is a *different*
  constraint, so `ON CONFLICT (project_id, lease_key) … DO NOTHING` cannot
  absorb it and the acquire raises instead of reporting "already held". One
  holder taking a second `lease_key` (`kind='tracked_task'` does this many
  times per run) and re-acquiring a released key both hit that.
- **A tracked task records its verdict here**: `outcome`
  (`succeeded`/`failed`), `error_code`, `error_message`. Design D9(a) is
  claim-dedupe + heartbeat + *terminal record*, and `status` is not it —
  release is release however the work went. An exclusive lock leaves all
  three NULL.

### New here, and one thing that is not

New: `projects` (`project_id='gtm'` seeded; token issuance stubbed
single-tenant), `accounts`, `account_usage`.

Explicitly **not** here: `run_requests`. It is chain intake — Modal
dispatcher plus fuse — ruled GTM-owned. The runner never creates or reads
it.

## 3. The two rules that shape the schema

**(a) Cross-database references survive as opaque text, never FKs.**
`job_key`, `group_key`, `lease_ref`, `session_ref` and `workspace_ref` are
text the runner stores and hands back but never parses. GTM keeps its
`'{inst}__{phase}__{backend}'` job keys verbatim and passes the institution
stable_id as `group_key`. Downstream consequence: the dashboard's
`split_part` parsing and its `institutions` JOIN both die. The single
sanctioned FK is `attempts.consumed_by_attempt_id` → `attempts(id)`.

**(b) No caller vocabulary in CHECK constraints.** `task_type` and `harness`
are unconstrained text. This is why the schema cannot repeat the
`pipeline_jobs` phase-CHECK churn (GTM migrations 012/013/019/024/025) — a
new phase is a new string, not a migration. A test enforces it.

## 4. The `runner_emitter` role

Provisioned from **`db/roles/`, not `db/migrations/`** — the chain is six
files, and role provisioning is a seventh thing that is not a migration:

- `CREATE ROLE` is cluster-global while the ledger is per-database, so a
  ledgered role file records "applied" in one database about an object that
  lives outside it.
- **The ledger cannot see grant drift.** One `REVOKE INSERT ON events` and
  the applier still says `No pending migrations` forever while every emit
  fails `permission denied for table events`. So `db/roles/` is *not*
  ledgered and re-applies on every `agent-runner migrate`: **re-running is
  the repair**, and `--roles-only` is the narrow form of it.
- **It needs a privilege the schema chain does not** (see below), and as the
  last numbered migration a failure there aborted *after* 001–006 had
  committed — a half-provisioned database with a green-looking ledger.

**Required privilege: superuser, or `CREATEROLE`.** The applier checks
before running the cluster file. Unprivileged with the role already present:
it skips that file and still applies the grants (what a managed provider's
app role can do). Unprivileged with the role missing: it stops and prints
the file for a superuser to run — the tables are already applied and
ledgered, so only the role is outstanding. `--skip-roles` opts out entirely.
This is what makes the chain safe on Railway at step 9 and PlanetScale at
step 12, where the app role is not a superuser; both are still untested
against a non-superuser applier, and this split is what keeps that from
being a cutover-blocking discovery.

The role agent processes write events as, and nothing more (verbatim from
`db/roles/`):

```sql
CREATE ROLE runner_emitter LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
GRANT USAGE  ON SCHEMA public   TO runner_emitter;
GRANT INSERT ON events          TO runner_emitter;
GRANT USAGE  ON SEQUENCE events_id_seq TO runner_emitter;
```

No SELECT anywhere — in particular none on `jobs`.

Created with **no password on purpose**: credentials never live in a
git-tracked migration. Provisioning at step 9 is
`ALTER ROLE runner_emitter PASSWORD '<generated>'`, and that DSN goes into
`RUNNER_EMIT_DSN` only. Agent environments carry `RUNNER_EMIT_DSN` plus the
`RUNNER_*` attribution and never `DATABASE_URL` — that is the leak class
this role exists to keep closed.

INSERT-only breaks three things. All are answered by the schema, not by
widening the grant:

1. **The job lookup.** Today an event insert resolves the job row with
   `SELECT id, group_key FROM pipeline_jobs WHERE stable_id=%s`. INSERT-only
   cannot. Answer: `events.job_key` is opaque text with no FK and no lookup;
   `group_key`, `task_type`, `harness` and `attempt` come from the writer's
   `RUNNER_*` attribution. No `(id, key)` view was granted either — the
   opaque-text rule already made the lookup unnecessary.
2. **The jobs progress UPDATE.** Today emit also updates the `pipeline_jobs`
   progress columns. Under this role emit degrades to a pure event append;
   the progress columns become the engine's to write, through its
   stream-batch path on the full DSN.
3. **The emitter cannot read back the id it just wrote.** `RETURNING` a real
   column needs SELECT on it, so `RETURNING id` fails while `RETURNING 1`
   (a constant) succeeds. Today's emit path only returns a constant, so
   nothing breaks — but `events.id` *is* the cursor contract, and the first
   emit path wanting its own cursor hits this. Not a grant to widen: a
   writer that must follow its own events reads them over the full DSN, or
   tracks them by the attribution it already supplied.

`events.project_id` is the one routing column that is `NOT NULL` and the one
an emitter cannot derive (no SELECT, and the emit attribution environment
has no project concept). It therefore carries `DEFAULT 'gtm'`, matching the
single seeded `projects` row — without it every `agent-runner emit` insert
would fail at the step-9 flip. When a second project exists, agent
environments carry `RUNNER_PROJECT_ID` next to the other `RUNNER_*`
attribution and emit sends it explicitly; the default is the single-tenant
floor, not a substitute for sending the value.

The NOTIFY trigger rides an ordinary `AFTER INSERT` trigger owned by the
table owner and is **not** security-definer: `pg_notify` needs no privilege,
so an INSERT by `runner_emitter` fires it normally. Channel `runner_events`,
payload `{id, project_id, job_key, group_key, kind}`; listeners re-query by
id cursor (NOTIFY is 8KB-capped). GTM's own channel stays separate — the
dashboard runs both listeners and multiplexes them into SSE frames tagged by
source.

`job_key` is in that payload — four keys, where design §4 sketched three —
because the one live listener routes on two questions, not one: *which
institution is this?* (`group_key` answers that, replacing the `split_part`
parse) and *does this belong to the job whose event pane is open?* Nothing
in a three-key payload answers the second, so its live feed would silently
degrade to the poll fallback at cutover. One short string, against an 8KB
cap.

## 5. What flips at step 9

- Provision the runner DB on Railway; apply this repo's migrations to it
  with `RUNNER_DSN` pointing at it (never `DATABASE_URL` — §1). If Railway's
  app role cannot `CREATE ROLE`, the chain still applies; run
  `db/roles/010_create_runner_emitter_role.sql` once as a privileged role,
  then `agent-runner migrate --roles-only` for the grants.
- `ALTER ROLE runner_emitter PASSWORD '<generated>'` out of band; that DSN
  goes into `RUNNER_EMIT_DSN` and nowhere else.
- Flip `runner_dsn` from the GTM database to the runner DB, between runs.
- Point the package's SQL at the new names (`jobs`/`events`/`attempts`/
  `leases`). Step 8 authored the schema; it did not retype the SQL.
- Copy `pipeline_attempts` → `attempts` — **not one `INSERT..SELECT`.** The
  key change from `(run_id, job_stable_id, attempt)` to
  `(project_id, job_key, attempt)` has three consequences, spelled out in
  full in 003's table comment:
  1. **Uniqueness.** `--force-rerun` resets `attempt_count` to 0, so attempt
     numbers repeat across runs; the copy must dedupe or renumber first.
  2. **The resume chain is re-linked, never copied.** The old chain is the
     pair `(consumed_by_run_id, consumed_by_attempt)`; the new one is a
     surrogate id that does not exist until the rows are inserted — and the
     dedupe/renumber from (1) destroys the very coordinates that pair
     resolves through. Insert with the link NULL and the old triple carried
     along, then resolve it in a second pass. Rows whose consumer got
     dropped resolve to NULL and read as *resumable again*, which is the
     argument for renumbering over deduping.
  3. **`resume_depth` needs a backfill.** It defaults to 0 with no source
     column, so a straight copy hands every exhausted session a fresh
     budget — re-arming exactly the resume loop `RESUME_BUDGET` exists to
     stop. Walk the re-linked chain once (recursive CTE in 003's comment),
     at cutover only — and mind the direction: depth 0 is the rows that
     consumed *nothing*; each consumer is its consumed row's depth + 1.
     Anchoring 0 at the unconsumed heads inverts the meaning and re-arms
     every exhausted chain.

  `db/copy_attempts.py` implements all three parts (renumber, two-pass
  re-link, corrected-direction backfill) in one target transaction, with
  `--dry-run` and an idempotent already-copied preflight; use it rather
  than hand-rolling the SQL.
- Set `RUNNER_EMIT_DSN`'s *value* to the `runner_emitter` DSN and drop the
  jobs UPDATE from the emit path. The emit CLI already falls back to the
  full DSN, so both behaviors survive the bridge; no schema change is needed
  for the flip.
- Dashboard, in the **same** deploy: second read-only pool on the runner DB,
  second listener, runner-source SQL rewritten from `pipeline_*` to
  `jobs`/`events`. Shipping it after the flip would leave live agent
  activity invisible.
- Old `pipeline_*` tables freeze read-only, dropped after a 30-day retention
  window.

## 6. Every divergence from design §4

Design doc §4 is the schema authority for step 8. Where the SQL differs from
its sketch, it is listed here — the whole list, so a reviewer comparing the
two never meets an unannounced change.

| Where | Design §4 | Here | Why |
|---|---|---|---|
| `jobs.prompt_ref` | `text NOT NULL` (sha-addressed blob) | `jsonb`, nullable | The code carries a pair (`{template, sha256}`), not a blob; late binding is legal. **Cost:** template bodies now live inline in every jobs row instead of behind a content-addressed reference. |
| `events` NOTIFY payload | `{id, project_id, group_key, kind}` | `+ job_key` | The live listener routes on two questions; three keys answer one (§4). |
| `events.project_id` | `NOT NULL` | `NOT NULL DEFAULT 'gtm'` | An INSERT-only emitter cannot derive it, and no attribution variable supplies one yet (§4). |
| `leases` | no `lease_ref` | `lease_ref text NOT NULL`, **not** unique | Jobs/events/attempts all point at a lease by handle; it needs to exist here. Non-unique for the acquire path's sake (§2). |
| `leases` | status only | `+ outcome`, `error_code`, `error_message` | D9(a) requires a *terminal record* for tracked tasks; `status` is the lock's lifecycle, not a verdict. |
| `accounts`, `account_usage` | no project column | `project_id text NOT NULL REFERENCES projects` on both; `UNIQUE (project_id, harness, label)`; `accounts.created_at` | Credential pools do not cross tenants, and the plan puts `project_id` on every table from day one. `account_usage`'s PK stays `(account_id, window_start)` — `account_id` already implies the project. |
| `account_usage` | `tok_input, tok_output` | `+ tok_cache_write, tok_cache_read` | Both of its sources (`attempts`, `events`) carry all four, and cache reads are most of the real volume; a lossy rollup cannot drive rotation or cost decisions. |
| role provisioning | one schema | `db/roles/`, outside the ledger | Cluster-global object, invisible grant drift, and a privilege the chain does not need (§4). |

Not divergences, but worth stating because they read like them:
`artifact_contract` and `policy` are `NOT NULL` with **no default**, exactly
as the design specifies — a default would let a job be submitted with no
output contract and no retry policy while passing every constraint.
