"""Secret-value inputs for operator CLIs.

Database URLs must not be literal command-line arguments: process listings,
shell history, and command-event capture all preserve argv.  Operator tools
therefore select a named environment variable or a file containing exactly
one value.  Files are accepted only when they are regular and private to the
owner (mode 0600 or stricter on POSIX).

Only selector names and paths may appear in diagnostics.  Secret values are
never interpolated into an error.
"""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _from_environment(name: str, *, label: str) -> str:
    if not _ENV_NAME.fullmatch(name):
        raise SystemExit(
            f"{label} environment selector must be a variable name, not a value."
        )
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{label} environment variable {name!r} is unset or empty.")
    return value


def _from_file(path_value: str | Path, *, label: str) -> str:
    path = Path(path_value).expanduser()
    try:
        metadata = path.stat()
    except OSError as exc:
        raise SystemExit(f"Cannot read {label} secret file {path}: {exc.strerror}.") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"{label} secret file is not a regular file: {path}")
    if os.name == "posix" and metadata.st_mode & 0o077:
        raise SystemExit(
            f"{label} secret file is accessible by group/others: {path}."
            " Run chmod 600 on it and retry."
        )
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        detail = getattr(exc, "strerror", None) or type(exc).__name__
        raise SystemExit(f"Cannot read {label} secret file {path}: {detail}.") from None

    # A final newline is conventional for secret files; any second line is
    # ambiguous and usually means an env file was passed by mistake.
    if raw.endswith("\r\n"):
        value = raw[:-2]
    elif raw.endswith(("\r", "\n")):
        value = raw[:-1]
    else:
        value = raw
    if not value:
        raise SystemExit(f"{label} secret file is empty: {path}")
    if "\n" in value or "\r" in value:
        raise SystemExit(
            f"{label} secret file must contain exactly one value: {path}"
        )
    return value


def secret_value(
    *,
    label: str,
    env_name: str | None = None,
    file_path: str | Path | None = None,
    default_env: str | None = None,
) -> str:
    """Resolve one secret from an env *name* or a private one-value file.

    Callers should put ``env_name`` and ``file_path`` in an argparse mutually
    exclusive group.  ``default_env`` is selected only when neither explicit
    selector was supplied.
    """
    if env_name is not None and file_path is not None:
        raise SystemExit(f"Choose only one {label} secret source.")
    if file_path is not None:
        return _from_file(file_path, label=label)
    selected_env = env_name or default_env
    if selected_env is None:
        raise SystemExit(f"Choose an environment variable or secret file for {label}.")
    return _from_environment(selected_env, label=label)
