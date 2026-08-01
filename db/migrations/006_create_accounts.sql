-- 006: accounts + account_usage — the harness credential pool and its usage
-- windows.
--
-- New tables: GTM never had these. An account is one usable identity on one
-- harness (a Claude subscription, a Codex login), which the runner rotates
-- across when one hits a limit. jobs.account_id and attempts.account_id
-- record which one an attempt drew from, as plain bigints with no FK so an
-- account can be retired without touching history.
--
-- ONE DECLARED DIVERGENCE from design §4 (docs/schema.md §6 lists every
-- divergence): both tables carry project_id, which the design's sketch
-- omits on accounts and account_usage alike. Credential pools do not cross
-- tenants and the plan puts project_id on every table from day one — so the
-- pool is scoped, UNIQUE (project_id, harness, label) lets two tenants each
-- call an account 'primary', and a usage sweep is tenant-scoped without
-- joining back through accounts. accounts.created_at is the same
-- house-style addition every other table here carries.

CREATE TABLE IF NOT EXISTS accounts (
  id              bigserial PRIMARY KEY,
  project_id      text NOT NULL REFERENCES projects(project_id),
  harness         text NOT NULL,
  label           text NOT NULL,
  secret_ref      text,

  status          text NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'cooldown', 'disabled')),
  disabled_reason text,

  concurrent_cap  integer NOT NULL DEFAULT 1,
  cooldown_until  timestamptz,
  last_used_at    timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),

  UNIQUE (project_id, harness, label)
);

COMMENT ON TABLE accounts IS
$$
One usable identity on one harness, and the rotation state the scheduler
reads. harness is the same opaque adapter registry key as jobs.harness — no
CHECK on it here either.
$$;

COMMENT ON COLUMN accounts.secret_ref IS
$$
A POINTER into the SecretStore — never a credential.

Nothing in this database ever holds a token, a cookie, or a password. The
column names where the secret lives; resolving it is the worker's job at
launch time.
$$;

COMMENT ON COLUMN accounts.status IS
$$
active: eligible for new claims.
cooldown: rate-limited until cooldown_until, then eligible again.
disabled: taken out of rotation by an operator; disabled_reason says why.
$$;

COMMENT ON COLUMN accounts.concurrent_cap IS
'How many attempts may hold this account at once. 1 is the safe default for a personal subscription.';

CREATE TABLE IF NOT EXISTS account_usage (
  account_id      bigint NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  project_id      text NOT NULL REFERENCES projects(project_id),
  window_start    timestamptz NOT NULL,
  requests        bigint NOT NULL DEFAULT 0,
  tok_input       bigint NOT NULL DEFAULT 0,
  tok_cache_write bigint NOT NULL DEFAULT 0,
  tok_cache_read  bigint NOT NULL DEFAULT 0,
  tok_output      bigint NOT NULL DEFAULT 0,
  cost_usd        numeric NOT NULL DEFAULT 0,

  PRIMARY KEY (account_id, window_start)
);

-- Tenant-scoped sweeps ('this project's spend this week') without joining
-- back through accounts.
CREATE INDEX IF NOT EXISTS account_usage_project_window_idx
  ON account_usage (project_id, window_start);

COMMENT ON TABLE account_usage IS
$$
Rolled-up usage per account per window — the input to rotation and cost
decisions, which cannot run on regex over event messages.

One row per (account, window_start), upserted as attempts report usage. The
per-event breakdown stays in events; this is the aggregate.

The token columns MIRROR ITS TWO SOURCES EXACTLY — attempts (003) and events
(004) both carry tok_input / tok_cache_write / tok_cache_read / tok_output,
so the rollup carries all four. Cache reads dominate real token volume on
both harnesses; a rollup missing them would be lossy against the very
columns that exist to keep usage out of message regexes.

project_id is denormalized from accounts (an account already belongs to a
project) so a tenant sweep needs no join. It is a divergence from design §4
— see docs/schema.md §6 — and the reason the PRIMARY KEY stays
(account_id, window_start): account_id already implies the project, so
adding it to the key would only widen every upsert.
$$;

COMMENT ON COLUMN account_usage.tok_cache_read IS
'Cached-prompt tokens read back. Usually the LARGEST of the four counters — dropping it would understate real volume by most of it.';

COMMENT ON COLUMN account_usage.window_start IS
'Start of the accounting window this row totals (the window length is the scheduler''s policy, not a schema fact).';
