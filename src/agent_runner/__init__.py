"""agent_runner: the hands that run agents.

Everything involved in running an agent CLI process and telling
the truth about what happened — spawn, stream, classify, repair, sessions,
auth, workdirs, hygiene, isolation. A library shared across projects; each
consumer is one caller. It knows nothing about pipelines, prompts,
contracts, receipts, or business data — it runs agents.

Core (`agent_runner.attempt.run_attempt` and friends) is stdlib-only and
imports zero Temporal. The optional layers install on demand:

- ``agent_runner.temporal`` (``pip install agent-runner[temporal]``) — the
  ready-made Temporal activity wrapper: heartbeat pump, session_ref in
  heartbeat details, the ruled outcome-to-retry mapping, checkpoint
  term-stamp verification, resume budget with fresh-session fallback.
- ``agent_runner.resources`` — provisioning for declared resources
  (``cdp_browser`` spawns Chrome and hands its endpoint in as a template
  value). Projects that declare nothing carry no browser dependencies.
- ``agent_runner.executor`` / ``workspace`` / ``remote`` — sandboxes: one
  adaptor for where an attempt runs (Modal, or this host), the workspace
  keeper that backs the sandbox's tree up to S3 (``pip install
  agent-runner[s3]``, on when ``AGENT_RUNNER_STATE_S3`` is set), and the
  protocol between the attempt inside and the supervisor outside.

This init stays import-light on purpose: no submodule imports here, so
``import agent_runner`` is side-effect free and needs no environment.
"""

__version__ = "1.2.0"
