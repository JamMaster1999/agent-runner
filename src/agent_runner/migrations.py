"""Apply the runner database's migrations with a small schema_migrations ledger.

Extraction step 8: the runner owns its own database (plan §3 — `projects`,
`jobs`, `attempts`, `events`, `leases`, `accounts`, `account_usage`), so it
owns the applier too. The ledger is one table
`schema_migrations(filename text PRIMARY KEY, applied_at timestamptz)` whose
keys are '<dir>/<file>' — 'runner-migrations/<name>.sql' for the files in
db/migrations. That key format mirrors GTM's db/apply_migrations.py, which
shares its ledger with the Uflo CRM runner; this ledger is shared with
nobody, so the prefix is here for shape parity (and to keep a future second
migration source from colliding), never for coordination.

Deliberate differences from the GTM applier:

- **Transport is psycopg, not psql.** GTM's applier is that repo's one
  sanctioned psql holdout; here the repo dependency is psycopg and CI must
  not need a client binary. Each migration runs as one explicit transaction
  on one connection: execute the whole file text, insert the ledger row,
  commit — so a half-applied file can never record itself. psycopg sends a
  parameterless query over the simple protocol, which is what makes a
  multi-statement .sql file work in a single execute() (the same thing
  tests/test_resume_claim_sql.py relies on to apply a migration file).
  Migration files must therefore NOT contain their own BEGIN/COMMIT: the
  transaction is this module's.
- **No GTM imports.** The DSN resolves locally: explicit argument >
  RUNNER_DSN. Never DATABASE_URL — see ``resolve_url``.
- **Self-contained.** psycopg is imported lazily and `agent_runner.util` is
  never imported, so this module stays importable with no driver, no
  database, and no AGENT_RUNNER_PROJECT_ROOT (util raises at import time
  without it) — which is what keeps the stdlib CI job able to import it.

TWO GUARDS STAND BETWEEN THIS APPLIER AND SOMEBODY ELSE'S DATABASE. The
runner's table names are generic (``jobs``, ``events``), its emitter role is
cluster-global, and the likeliest operator mistake in this era is running
the chain against the CLIENT database the same machine already has
configured — during the bridge, `runner_dsn` still points there:

1. ``resolve_url`` does not read DATABASE_URL at all. That variable names
   the client's database on every machine that has both, in this repo's own
   CI `full` job, and in the Modal Secret.
2. ``assert_runner_target`` refuses a target carrying client tables or
   foreign ledger rows, unless the caller passes ``allow_foreign`` (the
   ``--i-know-this-is-the-runner-db`` flag).

Role provisioning (db/roles) is NOT ledgered and re-applies on every run:
the ledger cannot see a revoked grant, so re-running IS the repair path. It
is also the only part needing CREATEROLE, and it runs after the schema
chain, so an unprivileged applier still lands and ledgers every table.

The migrations directory is <repo root>/db/migrations and the roles
directory <repo root>/db/roles, both resolved from ``__file__``. Running
``agent-runner migrate`` from an INSTALLED WHEEL has no repo checkout and
therefore no db/ directory: set AGENT_RUNNER_MIGRATIONS_DIR (and
AGENT_RUNNER_ROLES_DIR) to directories of .sql files in that case.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


# Ledger key prefix for this directory's migrations (see module docstring).
RUNNER_PREFIX = "runner-migrations/"

# …/src/agent_runner/migrations.py -> …/src/agent_runner -> …/src -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[2]

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
"""

# Tables that prove the target database belongs to a CLIENT. Any one of them
# means this is the GTM/CRM database, not the runner's: the pipeline_* four
# are the compat-bridge tables the runner itself wrote until step 8, the rest
# are GTM business tables. Nothing here is a runner table, so the list can
# never reject a correct target.
CLIENT_TABLES = (
    "pipeline_jobs",
    "pipeline_events",
    "pipeline_runs",
    "pipeline_attempts",
    "pipeline_manager_events",
    "run_requests",
    "enrichment_runs",
    "institutions",
    "instructors",
    "departments",
    "sections",
    "courses",
    "teaching_assignments",
)

# The one file in db/roles that touches the CLUSTER (CREATE ROLE, COMMENT ON
# ROLE) and therefore needs superuser or CREATEROLE. Everything else there is
# per-database GRANTs the owner of the tables can run.
CLUSTER_ROLE_FILE = "010_create_runner_emitter_role.sql"
EMITTER_ROLE = "runner_emitter"


def _psycopg():
    """The lazily imported driver, with loud guidance when it is absent.

    Deliberately a local copy of util's helper rather than an import of it:
    this module must stay importable without AGENT_RUNNER_PROJECT_ROOT.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise SystemExit(
            "psycopg is required to apply migrations; install with "
            "`pip install 'psycopg[binary]'` (or run under an interpreter "
            "that has it)."
        ) from exc
    return psycopg


def migrations_dir() -> Path:
    """The directory of .sql files: env override, else <repo root>/db/migrations."""
    override = os.environ.get("AGENT_RUNNER_MIGRATIONS_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return _REPO_ROOT / "db" / "migrations"


def migration_paths() -> list[Path]:
    """Every migration, in filename order (the apply order).

    Filenames are zero-padded (001_…, 002_…), so lexicographic sort IS the
    dependency order.
    """
    directory = migrations_dir()
    if not directory.is_dir():
        raise SystemExit(
            f"Migrations directory not found: {directory}. Running from an "
            "installed wheel? Set AGENT_RUNNER_MIGRATIONS_DIR to a directory "
            "of .sql files."
        )
    return sorted(directory.glob("*.sql"))


def roles_dir() -> Path:
    """The directory of role/grant .sql files: env override, else db/roles."""
    override = os.environ.get("AGENT_RUNNER_ROLES_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return _REPO_ROOT / "db" / "roles"


def role_paths() -> list[Path]:
    """Every role/grant file, in filename order (role first, then grants).

    An absent directory is empty, not fatal: role provisioning is optional
    (``--skip-roles``) and a wheel without AGENT_RUNNER_ROLES_DIR simply has
    no files to run, while a missing MIGRATIONS dir is always an error.
    """
    directory = roles_dir()
    if not directory.is_dir():
        return []
    return sorted(directory.glob("*.sql"))


def resolve_url(explicit: str | None = None) -> str:
    """The runner DSN: explicit argument > RUNNER_DSN. NEVER DATABASE_URL.

    DATABASE_URL is the CLIENT's variable, not the runner's: GTM's
    core/db.py reads exactly it, this repo's own CI `full` job sets it to the
    GTM database, and so does the Modal Secret. A fallback onto it turned a
    no-flag ``agent-runner migrate`` into: seven generically named tables
    (jobs, events, …), a NOTIFY trigger, a cluster-global runner_emitter
    role and 'runner-migrations/*' rows, all created inside the shared
    GTM/CRM database — which shares its ledger with the CRM's migration
    runner and would have accepted every one of those rows silently.

    The runner DSN has its own name and no default. If RUNNER_DSN is what
    DATABASE_URL says, say so once, in the environment.
    """
    url = explicit or os.environ.get("RUNNER_DSN")
    if not url:
        extra = ""
        if os.environ.get("DATABASE_URL"):
            extra = (
                " DATABASE_URL is set, and is deliberately NOT used: it names"
                " the client's database, not the runner's. Set RUNNER_DSN"
                " explicitly (they may be different databases; during the"
                " bridge they usually are)."
            )
        raise SystemExit(
            "No runner database URL. Set RUNNER_DSN or use a CLI secret selector."
            + extra
        )
    return url


def _ledger_columns(conn: Any) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = 'schema_migrations'"
    ).fetchall()
    return {row[0] for row in rows}


def client_tables_present(conn: Any) -> list[str]:
    """CLIENT_TABLES that exist in the target's public schema."""
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = 'public' AND table_name = ANY(%s)"
        " ORDER BY table_name",
        (list(CLIENT_TABLES),),
    ).fetchall()
    return [row[0] for row in rows]


def foreign_ledger_prefixes(conn: Any) -> list[str]:
    """Ledger key prefixes in the target that are not the runner's.

    GTM writes 'graph-migrations/…' and the CRM runner 'crm-migrations/…'
    into the same table this applier uses. Their presence means the ledger
    belongs to someone else and this database does too.
    """
    if "filename" not in _ledger_columns(conn):
        return []
    rows = conn.execute("SELECT DISTINCT filename FROM schema_migrations").fetchall()
    prefixes = {
        row[0].split("/", 1)[0] + "/"
        for row in rows
        if row[0] and "/" in row[0] and not row[0].startswith(RUNNER_PREFIX)
    }
    return sorted(prefixes)


def assert_runner_target(conn: Any, *, allow_foreign: bool = False) -> None:
    """Refuse to write the runner schema into somebody else's database.

    docs/schema.md promises the migrations never touch the GTM database;
    this function is what makes that a property of the code rather than a
    sentence in a doc. Two independent tells, either one fatal:

    - a CLIENT_TABLES table in the target (GTM business tables, or the
      pipeline_* bridge tables the runner itself wrote until step 8);
    - a ledger row under a foreign prefix (the GTM/CRM shared ledger).

    Both are cheap, both are read-only, and neither can fire on a correct
    target: a runner database has none of those tables and no ledger rows
    but its own. ``allow_foreign`` (``--i-know-this-is-the-runner-db``) is
    the deliberate override, and it warns rather than going quiet — a
    one-database deployment that really does co-host both schemas has to say
    so on every apply.
    """
    tables = client_tables_present(conn)
    prefixes = foreign_ledger_prefixes(conn)
    if not tables and not prefixes:
        return

    database = conn.execute("SELECT current_database()").fetchone()[0]
    tells = []
    if tables:
        tells.append("client tables present (" + ", ".join(tables) + ")")
    if prefixes:
        tells.append("foreign ledger rows under " + ", ".join(prefixes))
    detail = f"Target database {database!r}: " + "; ".join(tells) + "."

    if allow_foreign:
        print(f"WARNING: {detail} Proceeding: --i-know-this-is-the-runner-db.")
        return

    raise SystemExit(
        f"REFUSING to apply the runner schema. {detail}\n"
        "This looks like a CLIENT database, not the runner's. Applying here "
        "would create generically named tables (jobs, events, …), a "
        "cluster-global role, and 'runner-migrations/*' rows in a ledger "
        "another migration runner shares.\n"
        "Point RUNNER_DSN (or the selected secret input) at the runner database, or pass "
        "--i-know-this-is-the-runner-db if the two really are one database."
    )


def ensure_ledger(conn: Any) -> None:
    """Create schema_migrations if missing; refuse an unrecognized one.

    Refusing beats guessing: applying migrations under a key format the
    existing ledger does not use would silently re-run every file.
    """
    columns = _ledger_columns(conn)
    if not columns:
        conn.execute(LEDGER_DDL)
        conn.commit()
        return
    if "filename" in columns:
        return
    raise SystemExit(
        "schema_migrations exists but has no 'filename' column — unknown "
        "ledger format, refusing to guess."
    )


def applied_keys(conn: Any) -> set[str]:
    """Bare filenames already applied (the 'runner-migrations/' prefix stripped).

    Rows under any other prefix belong to another migration source and are
    ignored, exactly as the shared-ledger format intends.
    """
    rows = conn.execute("SELECT filename FROM schema_migrations").fetchall()
    return {
        row[0][len(RUNNER_PREFIX):]
        for row in rows
        if row[0] and row[0].startswith(RUNNER_PREFIX)
    }


def apply_migration(conn: Any, path: Path) -> None:
    """One migration + its ledger row, in one transaction.

    The commit is the only thing that records the file as applied, so a
    failure anywhere in the file leaves the database and the ledger both
    untouched. Failures are loud and name the file.
    """
    try:
        conn.execute(path.read_text())
        conn.execute(
            "INSERT INTO schema_migrations (filename) VALUES (%s)",
            (RUNNER_PREFIX + path.name,),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise SystemExit(f"Migration {path.name} FAILED (rolled back): {exc}") from exc


def run_role_file(conn: Any, path: Path) -> None:
    """One db/roles file, in its own transaction, with NO ledger row.

    Not ledgering is the point: these files re-run every time, which is the
    only repair path for grant drift a ledger cannot see (one REVOKE and an
    'already applied' row would keep the applier quiet forever while every
    emit failed).
    """
    try:
        conn.execute(path.read_text())
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise SystemExit(
            f"Role provisioning {path.name} FAILED (rolled back): {exc}\n"
            "The schema chain is unaffected — it committed and ledgered "
            "before this ran. Re-run just this part with "
            "`agent-runner migrate --roles-only` as a privileged role."
        ) from exc


def can_provision_roles(conn: Any) -> bool:
    """Whether current_user may CREATE ROLE (superuser or CREATEROLE)."""
    row = conn.execute(
        "SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user"
    ).fetchone()
    return bool(row and row[0])


def role_exists(conn: Any, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,)).fetchall())


def _provision_roles(conn: Any) -> list[str]:
    """Apply db/roles on an open connection; return the filenames run.

    Runs after the schema chain, always, ledger or no ledger. The one file
    that touches the cluster is skipped when current_user cannot CREATE ROLE
    AND the role is already there — the grants still apply, which is what a
    managed provider's app role can do and needs. If the role is missing and
    cannot be created, stop with the exact file a superuser must run: half
    the provisioning is worse than none, because the emitter DSN would
    authenticate against nothing.
    """
    paths = role_paths()
    if not paths:
        return []

    privileged = can_provision_roles(conn)
    ran: list[str] = []
    for path in paths:
        if path.name == CLUSTER_ROLE_FILE and not privileged:
            if not role_exists(conn, EMITTER_ROLE):
                raise SystemExit(
                    f"Role provisioning needs CREATEROLE, which the connected "
                    f"role does not have, and {EMITTER_ROLE} does not exist.\n"
                    "The schema chain is fully applied and ledgered — only "
                    "the emitter role is missing.\n"
                    f"Have a superuser run {path} against this database (or "
                    "`agent-runner migrate --roles-only` with a privileged "
                    "DSN), then re-run. `--skip-roles` proceeds without it, "
                    "leaving RUNNER_EMIT_DSN with nothing to authenticate as."
                )
            print(
                f"Skipping {path.name}: {EMITTER_ROLE} exists and the connected"
                " role cannot CREATE ROLE (grants still re-applied)."
            )
            continue
        print(f"Provisioning {path.name}...")
        run_role_file(conn, path)
        ran.append(path.name)
    return ran


def apply_pending(
    url: str | None = None,
    *,
    dry_run: bool = False,
    with_roles: bool = True,
    roles_only: bool = False,
    allow_foreign: bool = False,
) -> list[str]:
    """Apply every not-yet-applied migration; return the filenames applied.

    An empty list means nothing was pending — the role files still re-ran,
    because they are the drift repair path and are not ledgered.
    ``dry_run`` prints what would run and returns without connecting.
    ``roles_only`` skips the schema chain entirely (the repair invocation)
    and always returns []. ``allow_foreign`` forwards to
    ``assert_runner_target``.
    """
    migrations = [] if roles_only else migration_paths()
    if not migrations and not roles_only:
        raise SystemExit(f"No migrations found in {migrations_dir()}")
    roles = role_paths() if (with_roles or roles_only) else []

    if dry_run:
        for path in migrations:
            print(path.name)
        for path in roles:
            print(f"{path.name} (roles, re-applied every run, not ledgered)")
        return [path.name for path in migrations]

    # Config before driver: a missing DSN is the likelier operator mistake and
    # its message is the more useful one when both are missing.
    dsn = resolve_url(url)
    conn = _psycopg().connect(dsn, autocommit=False)
    try:
        # Before anything is written: is this even the runner's database?
        assert_runner_target(conn, allow_foreign=allow_foreign)
        pending: list[Path] = []
        if not roles_only:
            ensure_ledger(conn)
            applied = applied_keys(conn)
            pending = [path for path in migrations if path.name not in applied]
            if not pending:
                print("No pending migrations.")
            else:
                for path in pending:
                    print(f"Applying {path.name}...")
                    apply_migration(conn, path)
                print(f"Applied {len(pending)} migration(s).")
        if roles:
            _provision_roles(conn)
        return [path.name for path in pending]
    finally:
        conn.close()
