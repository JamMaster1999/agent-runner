"""agent_runner: a generic agent-CLI job runner.

Extracted from the Uflo GTM production pipeline at extraction-plan §4 step 6
(a relocation — steps 3-5 already severed and retyped the modules). The
runner speaks only its own vocabulary (``RunnerJob``/``RunnerError``, the
wire protocol in ``agent_runner.protocol``); everything pipeline-shaped
arrives as submit data or facade-built closures.

Bridge status: until step 9 the store modules point at the client's GTM
database with the historical ``pipeline_jobs``/``pipeline_runs``/
``pipeline_events``/``pipeline_attempts`` table names, reached through the
DSN the facade passes. Path constants resolve through the
``AGENT_RUNNER_PROJECT_ROOT`` environment variable (see ``agent_runner.util``).

This init stays import-light on purpose: no submodule imports here, so
``import agent_runner`` is side-effect free, driver-free, and needs no
environment.
"""

__version__ = "0.1.0"
