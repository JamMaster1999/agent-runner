"""Small shared helpers for the runner modules, including the DB transport.

Extraction step 6 (the one sanctioned body swap of the relocation, scheduled
by the pre-move docstring): ``db_rows``/``db_tx`` stopped delegating to the
GTM transport and are the runner's OWN psycopg layer — one parameterized
statement / one transaction, connect_timeout + server-side statement_timeout,
one retry on ``OperationalError`` (``db_rows(retry=False)`` opts
non-idempotent statements out) then a retryable
``RunnerError(code="db_timeout")``; any other database failure is terminal
``RunnerError(code="db_error", alert=True)``. psycopg is imported lazily so
``import agent_runner`` stays driver-free and the stdlib-only suites pass
without psycopg; the first actual DB use without the driver raises the same
loud SystemExit guidance as before. The step-5 ``_runner_error`` conversion
shim died with the delegation.

Interpreter re-exec is an entry-point concern, never a transport one: GTM
entry points hop via the GTM transport's ``reexec_with_psycopg``
(core/db.py), and the runner CLI's process entry
(``agent_runner.cli.reexec_with_driver``) hops onto RUNNER_PYTHON before
its handlers reach this module's lazy driver import. The client-repo
job_event script died at step 7.

``ROOT``/``PROJECT_ROOT`` are no longer ``__file__``-derived (meaningless
post-move): they come from the ``AGENT_RUNNER_PROJECT_ROOT`` environment
variable — set by the client's bootstrap shim, the engine's agent
environment, or a test header — and point at the client project workspace
(attempt dirs and the runner state directory). Resolution is LAZY (PEP 562
module __getattr__ + the ``project_root()``/``state_dir()`` accessors): the
package imports fine without the variable, and only the first actual path
use raises when it is unset.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from agent_runner.runtime import RunnerError


def project_root() -> Path:
    """The client project workspace root, from AGENT_RUNNER_PROJECT_ROOT.

    Read at call time: importing the package never requires the variable —
    only spawning agents, resolving attempt dirs, or writing runner state
    does. Raises loudly when unset."""
    root_env = os.environ.get("AGENT_RUNNER_PROJECT_ROOT")
    if not root_env:
        raise RuntimeError(
            "AGENT_RUNNER_PROJECT_ROOT is not set. The runner resolves the "
            "client project workspace (attempt dirs, runner state logs) "
            "through this variable. Set it to the workspace root before "
            "using path-dependent runner operations."
        )
    return Path(root_env).resolve()


def state_dir() -> Path:
    """Where the runner keeps its local state (hook logs, notification log,
    debug artifacts): RUNNER_STATE_DIR when set, else ``<project_root>/.local``
    (the historical layout)."""
    override = os.environ.get("RUNNER_STATE_DIR")
    if override:
        return Path(override).resolve()
    return project_root() / ".local"


def __getattr__(name: str):
    # Back-compat lazy module attributes: `from agent_runner.util import ROOT`
    # resolves at the importing module's import time, so modules that need
    # import-without-env must call project_root() at use time instead.
    if name in ("ROOT", "PROJECT_ROOT"):
        return project_root()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _psycopg():
    """The lazily imported driver; loud guidance when absent (the stdlib
    suites import this module freely — only actual DB use requires psycopg)."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise SystemExit(
            "psycopg is required for this script; install with "
            "`pip install 'psycopg[binary]'`."
        ) from exc
    return psycopg


def clean_params(params):
    """Strip NUL bytes from str params — Postgres text cannot store them, and
    agent stream JSON legally encodes \\u0000 (lossless for any storable
    value). Accepts a sequence or mapping, or None. ``db_rows`` applies this
    automatically; ``db_tx`` scripts binding agent-derived text apply it
    themselves."""
    if params is None:
        return None
    if isinstance(params, dict):
        return {
            key: value.replace("\x00", "") if isinstance(value, str) else value
            for key, value in params.items()
        }
    return [
        value.replace("\x00", "") if isinstance(value, str) else value
        for value in params
    ]


def timeout_conninfo(url: str, timeout: int) -> str:
    """conninfo enforcing ``timeout`` server-side: connect_timeout bounds the
    dial and statement_timeout makes the SERVER kill an overrunning statement
    (a client-side kill would leave the query running in the server)."""
    from psycopg import conninfo  # lazy: stdlib suite runs without psycopg

    return conninfo.make_conninfo(
        url,
        connect_timeout=timeout,
        options=f"-c statement_timeout={timeout * 1000}",
    )


def _timeout_error(
    timeout: int, tries: int, details: str, cause: Exception
) -> RunnerError:
    # A transient DB stall (fan-out contention, a long import holding locks)
    # must surface as a retryable RunnerError — never escape as an unhandled
    # exception that blocks the job with zero retries.
    return RunnerError(
        f"database call timed out after {timeout}s "
        f"({tries} {'try' if tries == 1 else 'tries'}).",
        code="db_timeout",
        retryable=True,
        alert=False,
        details=details,
    )


def _db_error(cause: Exception) -> RunnerError:
    return RunnerError(
        "database command failed",
        code="db_error",
        retryable=False,
        alert=True,
        details=str(cause),
    )


def db_rows(
    url: str, sql: str, params=None, *, timeout: int = 60, retry: bool = True
) -> list[tuple]:
    """Run one parameterized statement; returns its rows ([] when the
    statement produces no result set). The timeout signal — QueryCanceled
    from statement_timeout, or the connect_timeout dial, both
    OperationalError — is retried once, then raised as retryable
    'db_timeout'; any other database failure is terminal 'db_error'.

    ``retry=False`` makes the timeout single-try, for statements that are
    NOT safe to replay: under autocommit a server-side commit whose reply is
    lost (connection dropped before the fetch) is indistinguishable from a
    failed try, so replaying a non-idempotent statement (the events-append
    CTE) would double-apply it."""
    psycopg = _psycopg()
    target = timeout_conninfo(url, timeout)
    tries = (1, 0) if retry else (0,)
    for tries_left in tries:
        try:
            with psycopg.connect(target, autocommit=True) as conn:
                cursor = conn.execute(sql, clean_params(params))
                return cursor.fetchall() if cursor.description is not None else []
        except psycopg.OperationalError as exc:
            if not tries_left:
                raise _timeout_error(timeout, len(tries), sql[:2000], exc) from exc
        except psycopg.Error as exc:
            raise _db_error(exc) from exc


def db_tx(url: str, script: Callable[[Any], Any], *, timeout: int = 60):
    """Run ``script(connection)`` inside ONE transaction: committed when the
    script returns, rolled back on exception (psycopg semantics). Callers own
    parameter hygiene inside the script (``clean_params`` on agent-derived
    text). Same error contract as ``db_rows``; the failed try committed
    nothing, so the one timeout retry replays the script on clean state."""
    psycopg = _psycopg()
    target = timeout_conninfo(url, timeout)
    label = f"transaction script {getattr(script, '__qualname__', repr(script))}"
    for tries_left in (1, 0):
        try:
            with psycopg.connect(target) as conn:
                return script(conn)
        except psycopg.OperationalError as exc:
            if not tries_left:
                raise _timeout_error(timeout, 2, label, exc) from exc
        except psycopg.Error as exc:
            raise _db_error(exc) from exc


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_tail(path: Path, limit: int = 12000) -> str:
    """Last ``limit`` bytes of a file as replacement-decoded text; "" when
    missing. Generic tail reader for CLI stderr/log tails (moved here from
    the GTM artifacts module at extraction step 4 — the harness adapters'
    only use of that module was this helper)."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    return data[-limit:].decode("utf-8", errors="replace")


def write_text(path: Path, text: str) -> None:
    """mkdir-then-write (same precedent as ``read_tail``): the engine and the
    attempts store write attempt-dir files without a GTM import."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def shell_quote(value: str | Path) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"
