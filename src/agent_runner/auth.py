"""Auth: volume-backed CLI credential files, the Modal model (ruling D1).

The mechanism that preserved subscription economics on Modal, owned here as
core's sessions duty: credential files are seeded ONCE from the environment
onto a volume-backed CLI home, refreshed tokens the CLI writes back persist
to the volume, and tokens are normalized on read — a terminal-wrapped paste
into a secrets dashboard can embed a line break mid-token, which produced a
live production 401 from a valid token (2026-07-30).

The per-provider knowledge (which file, which variable) lives in each
harness adapter's ``prepare_home``/``bind_credentials``; this module is the
provider-neutral surface and stays import-light so adapters can use its
helpers without a cycle.
"""

from __future__ import annotations

import os
from pathlib import Path


def normalize_token(value: str) -> str:
    """A credential token with every whitespace character removed. Tokens
    never legitimately contain whitespace; embedded line breaks from wrapped
    pastes must not reach a CLI."""
    return "".join(value.split())


def seed_credential_file(path: Path, value: str) -> bool:
    """Write ``value`` to ``path`` (mode 0600) only when the file does not
    exist yet — seeded once; a refreshed credential the CLI already wrote is
    never clobbered by the stale seed. Returns True when it wrote.

    The content lands via a pid-unique sibling temp file and one atomic
    rename, so no reader ever observes a partially written (or empty)
    credential file — the touch-then-write it replaces left the file empty
    for a moment, which a concurrently starting CLI read as broken auth."""
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.seed.{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, value.encode("utf-8"))
    finally:
        os.close(fd)
    os.replace(tmp, path)
    return True
