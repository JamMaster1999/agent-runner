"""The classification vocabulary: every attempt ends with exactly one
outcome (agent_runner.md, stage-3 carve-out).

One closed set, eight words. The runner supplies the evidence (adapter
marker tables over CLI-owned error text, spawn/timeout/stall conditions,
the caller's validation verdict) and ``run_attempt`` ends every attempt
with exactly one of these on its ``AttemptReport``. What an outcome means
for retries is the caller's decision — the optional ``agent_runner.temporal``
layer ships the ruled mapping (rate_limited backs off long and free, infra
retries on another worker, auth fails fast and alerts).
"""

from __future__ import annotations

# The attempt produced output the caller's validation accepted. Output
# validity beats exit code: a CLI that crashes during shutdown after the
# deliverable is written still ends ``valid``.
VALID = "valid"

# The output failed the caller's contract and repair (when available) did
# not fix it. Repair = on this outcome, the runner messages the project's
# auto-generated repair text into the still-open session before giving up.
INVALID_SCHEMA = "invalid_schema"

# The provider throttled the CLI (evidence from CLI-owned error text only).
RATE_LIMITED = "rate_limited"

# Infrastructure broke — the CLI died without terminal proof, the machine
# misbehaved, or the failure is simply unproven. Says nothing about the
# job; another worker may succeed.
INFRA = "infra"

# The CLI itself reported auth expiry or billing/quota exhaustion. Terminal:
# retrying spends money on a broken account, so this fails fast and the
# caller alerts.
AUTH = "auth"

# The attempt overran its time budget and the CLI was terminated.
TIMEOUT = "timeout"

# The CLI was alive but had stopped producing: nothing reached its stream
# for the stall window, so the runner terminated it. Its own liveness proved
# nothing — a wedged CLI whose helper process died under it still holds the
# process open — and another attempt is free to succeed.
STALLED = "stalled"

# No CLI ever validly started: missing binary, fork/exec refusal, a broken
# invocation, or stdin closed before the prompt arrived.
SPAWN_FAILURE = "spawn_failure"

OUTCOMES = (
    VALID,
    INVALID_SCHEMA,
    RATE_LIMITED,
    INFRA,
    AUTH,
    TIMEOUT,
    STALLED,
    SPAWN_FAILURE,
)

# Outcomes that are terminal proof: retrying cannot help and the caller
# should fail fast and alert.
TERMINAL = (AUTH,)
