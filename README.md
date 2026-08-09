# agent-runner

The hands that run agents: everything involved in running an agent CLI process (Claude Code, Codex) and telling the truth about what happened. A library shared across projects; each consumer calls it from inside its own activities. It knows nothing about pipelines, prompts, contracts, receipts, or business data — it runs agents.

The platform half this repo used to carry (jobs, attempts, events, leases over Postgres, and the engine loop that served them) was deleted at the GTM Temporal rewrite's stage 3. Git history is the archive.

## Core — zero Temporal imports, stdlib-only

One entry point: `agent_runner.attempt.run_attempt(spec, task, workdir, ...)` — spawn an agent CLI with an agent definition and a task message (both already rendered upstream), stream its live output, and end the attempt with exactly one outcome.

- **spawn** — `RunSpec` + `AgentDef` in, CLI process out; provider command shapes live in the harness adapters (`agent_runner/harness/`), core never branches on a provider name
- **stream** — `StreamEvent` telemetry via `on_event`: progress, tool calls, token usage and cost, plus captured provider hook events
- **classify** — every attempt ends with exactly one outcome from `agent_runner.outcomes`: `valid` · `invalid_schema` · `rate_limited` · `infra` · `auth` · `timeout` · `spawn_failure`
- **repair** — on `invalid_schema`, the caller's auto-generated repair message goes into the still-open session (`Verdict.repair_message`) before the attempt gives up
- **sessions** — `session_ref` extraction for resume; `sessions.prepare_session_homes` points CLI homes at a durable volume so transcripts and sessions survive workers
- **auth** — volume-backed CLI credential files (the Modal model): seeded once from the environment, refreshed tokens persist to the volume, tokens normalized on read (`agent_runner.auth`)
- **workdirs** — the folders a model is handed: attempt workspaces and term-scoped checkpoint dirs with term-stamp verification (`agent_runner.workdirs`)
- **hygiene** — the CLI child is always reaped, on every exit path
- **isolation** — agents get a filtered environment: a safe baseline plus exactly what the spec declared (`required_env`) and what the adapter's CLI needs (`agent_runner.isolation`)

Validation is the project's: `run_attempt(validate=...)` takes a closure returning a `Verdict`; agent-runner never parses or judges output content itself.

## Optional modules

- `agent_runner.temporal` (`pip install 'agent-runner[temporal]'`) — the ready-made Temporal activity wrapper, called from inside a project's activity: heartbeat pump while the CLI runs, `session_ref` + progress in heartbeat details (a retry resumes the session), the ruled outcome-to-retry mapping (`rate_limited` backs off long and free, `infra` retries on another worker, `auth` fails fast for the caller to alert on), checkpoint folders prepared before spawn with term stamps verified before resume, and a resume budget with fresh-session fallback.
- `agent_runner.resources` — provisioning for a spec's declared `resource_specs`: `cdp_browser` spawns Chrome and hands its endpoint in as a `{{RESOURCE:cdp_browser.*}}` template value. Projects that declare nothing carry no browser dependencies.

## The base image

`Dockerfile` builds the worker base image (published by `.github/workflows/publish-image.yml` to ghcr.io): pinned claude/codex CLIs with provenance (OCI labels + `/etc/agent-runner-provenance.json`), Chrome, and the package with the temporal extra. Consumer projects extend it and add only their own code, config, and hooks. What enters the image is an allowlist — three named COPY paths — never a blocklist.

## Install / test

- Editable install: `pip install -e .` (core has zero dependencies); `pip install -e '.[temporal]'` for the activity wrapper.
- Tests: `python -m unittest discover tests`. The suite is token-free: the fake-CLI rig (`tests/fake_cli/fake-cli`) stands in for the real CLIs via the `RUNNER_CODEX_CLI` / `RUNNER_CLAUDE_CLI` overrides, so spawn/stream/classify/repair run end to end with zero spend. Temporal-less runs skip the temporal suite cleanly; CI runs both modes and fails the build on any Temporal import leak into core.
