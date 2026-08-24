"""agent_runner.temporal — the ready-made Temporal activity wrapper
(optional; ``pip install agent-runner[temporal]``).

Called from INSIDE a project's activity, never the other way around:

- a heartbeat pump runs while the CLI does — liveness is the heartbeat,
  never a wall clock
- ``session_ref`` and progress ride heartbeat details, so a retry resumes
  the CLI session instead of restarting
- outcomes map to retry decisions: ``rate_limited`` backs off long and
  free, ``infra`` retries on another worker, ``auth`` fails fast (the
  project's workflow alerts — agent-runner never notifies anyone), all
  under the caller's retry-policy backstop
- checkpoint folders are prepared before spawn and every checkpoint's term
  stamp is verified before resume — mismatch discards, logs loudly, runs
  fresh
- a resume budget caps how often one session is reopened; past it the
  attempt falls back to a fresh session, recorded
- ``run_sandboxed_attempt`` runs the same attempt inside a sandbox the
  project opened, heartbeating only on what it fetched from its stream

Core never imports this package; importing it without the ``temporalio``
distribution fails with install guidance.
"""

from __future__ import annotations

try:
    import temporalio  # noqa: F401
except ImportError as exc:  # pragma: no cover - environment-dependent
    raise ImportError(
        "agent_runner.temporal needs the 'temporalio' package. Install the "
        "optional extra: pip install 'agent-runner[temporal]'."
    ) from exc

from agent_runner.temporal.activity import (  # noqa: E402,F401
    CheckpointSpec,
    TemporalRunConfig,
    run_agent_attempt,
)
from agent_runner.temporal.retry import (  # noqa: E402,F401
    application_error_for,
    recommended_retry_policy,
)
from agent_runner.temporal.sandbox import run_sandboxed_attempt  # noqa: E402,F401
