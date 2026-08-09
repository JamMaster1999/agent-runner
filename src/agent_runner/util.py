"""Small shared helpers for the runner modules.

``ROOT``/``PROJECT_ROOT`` are not ``__file__``-derived (meaningless for an
installed package): they come from the ``AGENT_RUNNER_PROJECT_ROOT``
environment variable — set by the consuming project's bootstrap, the
engine's agent environment, or a test header — and point at the project
workspace (attempt dirs and the runner state directory). Resolution is LAZY
(PEP 562 module __getattr__ + the ``project_root()``/``state_dir()``
accessors): the package imports fine without the variable, and only the
first actual path use raises when it is unset.

The DB transport that used to live here (``db_rows``/``db_tx``) died with
the platform half at the stage-3 carve-out: the runner owns no store any
more, so the package is stdlib-only.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path


def project_root() -> Path:
    """The project workspace root, from AGENT_RUNNER_PROJECT_ROOT.

    Read at call time: importing the package never requires the variable —
    only spawning agents, resolving attempt dirs, or writing runner state
    does. Raises loudly when unset."""
    root_env = os.environ.get("AGENT_RUNNER_PROJECT_ROOT")
    if not root_env:
        raise RuntimeError(
            "AGENT_RUNNER_PROJECT_ROOT is not set. The runner resolves the "
            "project workspace (attempt dirs, runner state logs) through "
            "this variable. Set it to the workspace root before using "
            "path-dependent runner operations."
        )
    return Path(root_env).resolve()


def state_dir() -> Path:
    """Where the runner keeps its local state (hook logs, debug artifacts):
    RUNNER_STATE_DIR when set, else ``<project_root>/.local`` (the
    historical layout)."""
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_tail(path: Path, limit: int = 12000) -> str:
    """Last ``limit`` bytes of a file as replacement-decoded text; "" when
    missing. Generic tail reader for CLI stderr/log tails."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    return data[-limit:].decode("utf-8", errors="replace")


def write_text(path: Path, text: str) -> None:
    """mkdir-then-write: attempt-dir files land without a caller import."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def shell_quote(value: str | Path) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"
