"""Stub HTTP binding: the HTTP transport's serialization skeleton
(design doc §5; extraction step 7).

``StubHTTPRunner`` wraps any inner ``RunnerProtocol`` binding and forces,
for every operation, the full wire round trip a real HTTP transport would
perform: each wire-dataclass argument is serialized (``to_wire`` ->
``json.dumps``), crosses the "wire" as JSON text, and is rebuilt
(``json.loads`` -> ``from_wire``) before the inner binding sees it — and
any return value makes the same hop back. Lists of wire dataclasses hop
element-wise; ``None``/``str``/plain values cross as JSON scalars.
Anything that cannot survive JSON raises (``json.dumps``'s TypeError
surfaces) instead of slipping through, so a live object smuggled across
the seam is caught now, not at the service phase.

Test double only: no sockets, no HTTP dependency, stdlib imports only —
``import agent_runner`` stays driver-free and the stdlib suite runs it.
The REAL HTTP transport replaces this at the runner's service phase and
must pass the same conformance suite (client-side
tests/test_protocol_conformance.py) unchanged.
"""

from __future__ import annotations

import inspect
import json
from typing import Any

from agent_runner.protocol import (
    OPERATIONS,
    WIRE_DATACLASSES,
    RunnerProtocol,
    from_wire,
    to_wire,
)


def _wire_key(key: Any) -> str:
    """A dict key as a JSON object key carries it: str crosses as itself;
    int/float/bool/None coerce to strings exactly as ``json.dumps`` renders
    object keys; anything else raises its TypeError — keys must not bypass
    serialization any more than values do."""
    if isinstance(key, str):
        return key
    return next(iter(json.loads(json.dumps({key: None}))))


def _wire_hop(value: Any) -> Any:
    """One full wire hop: the value as it looks on the far side of an HTTP
    request or response. Wire dataclasses go ``to_wire`` -> JSON text ->
    ``from_wire``; lists/tuples and dict values hop element-wise (dict keys
    through ``_wire_key``); anything else must already be
    JSON-representable — ``json.dumps`` raises TypeError otherwise, and
    that surfacing IS the contract (the stub must never silently bypass
    serialization)."""
    if type(value) in WIRE_DATACLASSES:
        return from_wire(type(value), json.loads(json.dumps(to_wire(value))))
    if isinstance(value, (list, tuple)):
        return [_wire_hop(item) for item in value]
    if isinstance(value, dict):
        return {_wire_key(key): _wire_hop(item) for key, item in value.items()}
    return json.loads(json.dumps(value))


def _proxy(name: str):
    """A concrete proxy for one §5 operation: hop every argument across the
    wire, call the inner binding, hop the result back."""
    protocol_method = getattr(RunnerProtocol, name)

    def operation(self, *args: Any, **kwargs: Any) -> Any:
        hopped_args = [_wire_hop(arg) for arg in args]
        hopped_kwargs = {key: _wire_hop(value) for key, value in kwargs.items()}
        return _wire_hop(getattr(self._inner, name)(*hopped_args, **hopped_kwargs))

    # The conformance suite compares each binding's signatures against the
    # ABC's; carry the protocol's over verbatim (inspect.signature honors
    # ``__signature__`` before it looks at the *args/**kwargs shell).
    operation.__name__ = name
    operation.__qualname__ = f"StubHTTPRunner.{name}"
    operation.__doc__ = protocol_method.__doc__
    operation.__signature__ = inspect.signature(protocol_method)  # type: ignore[attr-defined]
    return operation


class StubHTTPRunner(RunnerProtocol):
    """Wire-round-trip proxy over an inner binding (see module docstring).
    Every §5 operation is a generated ``_proxy`` — the dispatch is uniform,
    so hand-writing the near-identical methods would only invite drift from
    ``OPERATIONS``."""

    def __init__(self, inner: RunnerProtocol) -> None:
        self._inner = inner


for _operation in OPERATIONS:
    setattr(StubHTTPRunner, _operation, _proxy(_operation))
del _operation

# ABCMeta snapshotted __abstractmethods__ at class creation, before the loop
# above attached the proxies; recompute it so only genuinely still-abstract
# names remain (none today — but an operation added to the ABC and missed by
# OPERATIONS stays abstract and fails instantiation loudly).
StubHTTPRunner.__abstractmethods__ = frozenset(
    name
    for name in StubHTTPRunner.__abstractmethods__
    if getattr(getattr(StubHTTPRunner, name), "__isabstractmethod__", False)
)
