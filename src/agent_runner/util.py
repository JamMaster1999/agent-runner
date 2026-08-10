"""Small shared helpers for the runner modules.

Paths are not ``__file__``-derived (meaningless for an installed package):
they come from the ``AGENT_RUNNER_PROJECT_ROOT`` environment variable — set
by the consuming project's bootstrap, the engine's agent environment, or a
test header — and point at the project workspace (attempt dirs and the
runner state directory). Resolution is LAZY (the ``project_root()``/
``state_dir()`` accessors read at call time): the package imports fine
without the variable, and only the first actual path use raises when it is
unset.
"""

from __future__ import annotations

import os
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
