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
- ``agent_runner.state`` (``pip install agent-runner[s3]``) — the state
  mirror: session transcripts and checkpoint folders written through to
  S3, so a retry resumes on any host instead of only the one that ran
  before. Off unless ``AGENT_RUNNER_STATE_S3`` is set.

This init stays import-light on purpose: no submodule imports here, so
``import agent_runner`` is side-effect free and needs no environment.
"""

__version__ = "1.1.0"
