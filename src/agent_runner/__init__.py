"""agent_runner: a generic agent-CLI job runner.

The runner speaks only its own vocabulary (``RunnerJob``/``RunnerError``,
the wire protocol in ``agent_runner.protocol``); everything client-shaped —
agent configuration, prompt templates, artifact contracts, probe/resource
specs, policy — arrives as submit DATA or facade-built closures. The store
is the runner's own schema (db/migrations: projects/jobs/attempts/events/
leases), scoped by the client-declared RUNNER_PROJECT_ID tenant. Path
constants resolve lazily through the ``AGENT_RUNNER_PROJECT_ROOT``
environment variable (see ``agent_runner.util``).

Historical note: the package was extracted from a production enrichment
pipeline (2026-07/08); client-specific behavior found leaking after the
split was removed in 0.3.0.

This init stays import-light on purpose: no submodule imports here, so
``import agent_runner`` is side-effect free, driver-free, and needs no
environment.
"""

__version__ = "0.3.0"
