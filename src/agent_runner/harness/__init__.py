"""Name-keyed harness adapter registry.

The registry key is an adapter's ``name`` and equals the spec's harness
value. Runner core dispatches through ``get_adapter`` and never branches on
a harness name; every provider difference lives in one adapter module. A
new harness is one adapter class plus one ``register`` call — zero core
edits.
"""

from __future__ import annotations

from agent_runner.harness.base import Capabilities, HarnessAdapter, SpawnSpec
from agent_runner.runtime import RunnerError

_REGISTRY: dict[str, HarnessAdapter] = {}


def register(adapter: HarnessAdapter) -> HarnessAdapter:
    """Register an adapter under its name; last registration wins."""
    _REGISTRY[adapter.name] = adapter
    return adapter


def get_adapter(backend: str) -> HarnessAdapter:
    try:
        return _REGISTRY[backend]
    except KeyError:
        raise RunnerError(
            f"No harness adapter registered for backend {backend!r} "
            f"(registered: {', '.join(sorted(_REGISTRY)) or 'none'}).",
            code="unknown_backend",
            retryable=False,
            alert=True,
        ) from None


def registered_adapters() -> list[HarnessAdapter]:
    """Adapters in registration order."""
    return list(_REGISTRY.values())


# Built-in adapters.
from agent_runner.harness.claude_code import ClaudeCodeAdapter  # noqa: E402
from agent_runner.harness.codex import CodexAdapter  # noqa: E402

register(CodexAdapter())
register(ClaudeCodeAdapter())
