"""Claude Code harness adapter: every claude-CLI-specific behavior in one
module (design doc §2). Spawn/resume command shapes, session extraction, the
5-hook map, the error-report dialect, terminal markers, env quirks, and
health commands — moved verbatim from the pre-adapter modules (phase-2
step 5).

The legacy filesystem resume matcher (packet-membership match over
.local/runs) was DELETED at extraction step 4 (design §7.5), not ported:
every resumable session is DB-tracked in the attempts store, and resume
rights belong solely to claim_resumable_attempt — pinned by
tests/test_resume_claim_sql.py. This module imports no GTM modules.

Step-5 retype: spawn builders take the generic ``RunnerJob`` (agent name =
``job.agent_ref``); the attempt-timeout/resume-eligibility slots left the
adapter contract — both are submit data (``job.policy``)."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, ClassVar

from agent_runner import util
from agent_runner.harness.base import AgentDef, Capabilities, HarnessAdapter, SpawnSpec
from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.harness.claude_stream import ClaudeStreamParser


def yaml_scalar(value: object) -> str:
    """One frontmatter value as a YAML scalar (moved verbatim from the
    client's sync_agents at extraction step 7)."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)  # JSON strings are valid YAML double-quoted scalars
    if not isinstance(value, (int, float, bool)):
        raise RunnerError(
            f"claude agent config: unsupported frontmatter value: {value!r}",
            code="agent_render",
            retryable=False,
        )
    return str(value)


def claude_session_id(stdout_path: Path) -> str | None:
    """The session id from a Claude --print stream-json log, for
    `claude --resume` follow-ups on an interrupted attempt."""
    try:
        with stdout_path.open() as fh:
            for line in fh:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                session_id = payload.get("session_id")
                if session_id:
                    return str(session_id)
    except OSError:
        return None
    return None


class ClaudeCodeAdapter(HarnessAdapter):
    """The Claude Code CLI (`claude --print --output-format stream-json`,
    prompt on stdin)."""

    name: ClassVar[str] = "claude"
    display_name: ClassVar[str] = "Claude"
    start_label: ClassVar[str] = "Claude"
    session_noun: ClassVar[str] = "session"
    # resume: `claude --resume <session>`. followup=False: no in-session
    # repair today — validation failure goes straight to retry (the central
    # degradation). doctor=False: `claude doctor` exists but is unstructured
    # and optional (--run-claude-doctor); the health path is auth status plus
    # a capped live probe, i.e. the no-doctor degradation.
    capabilities: ClassVar[Capabilities] = Capabilities(
        resume=True,
        followup=False,
        hooks=True,
        doctor=False,
        final_message_artifact=False,
    )
    # Identical to the codex table on purpose: the pre-adapter code matched
    # one shared list for both CLIs, and splitting it per dialect would be a
    # behavior change, not a move. Prune per dialect deliberately.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
        ("health_budget_too_low", ("error_max_budget_usd", "reached maximum budget")),
        (
            "auth",
            (
                "authentication_error",
                "oauth token has expired",
                "token expired",
                "please run /login",
                "not logged in",
                "login required",
                "invalid api key",
                "api key not found",
            ),
        ),
        (
            "billing_or_credits",
            (
                "billing_error",
                "credit balance is too low",
                "insufficient_quota",
                "payment required",
                "out of credit",
            ),
        ),
        (
            "invalid_invocation",
            (
                "unknown option",
                "unknown argument",
                "unexpected argument",
                "unrecognized argument",
                "invalid value for",
                "no such subcommand",
            ),
        ),
    )

    def resolve_binary(self) -> str | None:
        return shutil.which("claude")

    def health_checks(self, args: argparse.Namespace) -> None:
        """Auth status, the optional doctor pass, and a capped live probe."""
        self.run_health_command(["claude", "auth", "status"], args.health_timeout_seconds)
        if args.run_claude_doctor:
            self.run_health_command(["claude", "doctor"], args.health_timeout_seconds)
        self.run_health_command(
            [
                "claude",
                "--print",
                "--output-format",
                "json",
                "--tools",
                "",
                "--max-budget-usd",
                str(args.claude_health_budget_usd),
                "--model",
                args.claude_health_model,
                "Healthcheck only. Reply with OK.",
            ],
            args.health_timeout_seconds,
        )

    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The `.claude/agents/<name>.md` dialect: YAML frontmatter between
        `---` lines (header comment first, then name + description, then the
        config keys in dict order), a blank line, then the verbatim body
        (moved verbatim from the client's sync_agents at extraction step 7)."""
        lines = ["---", f"# {header}"]
        lines.append(f"name: {yaml_scalar(agent.name)}")
        lines.append(f"description: {yaml_scalar(agent.description)}")
        for key, value in agent.config.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {yaml_scalar(item)}" for item in value)
            else:
                lines.append(f"{key}: {yaml_scalar(value)}")
        lines.append("---")
        return "\n".join(lines) + "\n\n" + agent.body

    def build_spawn(self, job: RunnerJob, directory: Path) -> SpawnSpec:
        # The rendered discovery file is this adapter's own materialize_agent
        # dialect; a missing one used to surface as an opaque CLI error that
        # burned the full retry budget — check it up front and fail loudly.
        agent_path = util.project_root() / ".claude" / "agents" / f"{job.agent_ref}.md"
        if not agent_path.is_file():
            raise RunnerError(
                f"{job.key}: rendered Claude agent not found: {agent_path}. "
                "Materialize the agent (materialize_agent -> "
                ".claude/agents/<name>.md under the project root) before "
                "spawning.",
                code="missing_claude_agent",
                retryable=False,
                alert=True,
            )
        command = [
            "claude",
            "--agent",
            job.agent_ref,
            "--permission-mode",
            "bypassPermissions",
        ]
        # Tool restrictions are submit DATA (policy["disallowed_tools"]), not
        # a business rule baked into every spawn: the old hard-coded
        # "--disallowedTools=Read,Bash" was one client's no-local-reads
        # policy applied silently to every client.
        disallowed = job.policy.get("disallowed_tools") or []
        if disallowed:
            command.append("--disallowedTools=" + ",".join(str(t) for t in disallowed))
        # Setting-source isolation as submit data: e.g. ["project"] keeps the
        # operator's user-global Claude state (plugins, skills, personal
        # memory) out of production sessions — the claude-side counterpart of
        # a client pinning its codex CODEX_HOME.
        sources = job.policy.get("setting_sources")
        if sources is not None:
            command += ["--setting-sources", ",".join(str(s) for s in sources)]
        command += [
            "--print",
            "--verbose",
            "--output-format",
            "stream-json",
            "--include-hook-events",
        ]
        return SpawnSpec(
            command=command,
            stdout_path=directory / "claude.stdout.log",
            stderr_path=directory / "claude.stderr.log",
        )

    def build_resume(self, job: RunnerJob, directory: Path, session_ref: str) -> SpawnSpec:
        spawn = self.build_spawn(job, directory)
        return SpawnSpec(
            command=spawn.command + ["--resume", session_ref],
            stdout_path=spawn.stdout_path,
            stderr_path=spawn.stderr_path,
        )

    def env_overrides(self) -> dict[str, str]:
        return {"CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS": "10000"}

    def env_passthrough(self) -> tuple[str, ...]:
        # CLI auth + config-home names the filtered agent env must inherit.
        return ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "CLAUDE_CONFIG_DIR")

    def session_ref_from_log(self, stdout_path: Path) -> str | None:
        return claude_session_id(stdout_path)

    def stream_parser(self) -> ClaudeStreamParser:
        return ClaudeStreamParser()

    def hook_event_log(self) -> Path:
        return util.state_dir() / "claude_hooks" / "events.jsonl"

    def normalize_hook_event(
        self, event: dict[str, Any], agent_name: str
    ) -> tuple[str, str] | None:
        hook_name = event.get("hook_event_name")
        agent_label = event.get("agent_type") or agent_name
        if hook_name == "SessionStart":
            return "hook_session_start", f"Claude session started: {agent_name}"
        if hook_name == "SubagentStart":
            return "hook_subagent_start", f"Claude subagent started: {agent_label}"
        if hook_name == "SubagentStop":
            return "hook_subagent_stop", f"Claude subagent stopped: {agent_label}"
        if hook_name == "Stop":
            return "hook_stop", f"Claude stop hook fired: {agent_name}"
        if hook_name == "SessionEnd":
            return "hook_session_end", f"Claude session ended: {event.get('reason') or 'unknown'}"
        return None

    def stream_error_line(self, payload: dict[str, Any]) -> str | None:
        """claude --print emits a final `result` event."""
        if (payload.get("type") or "") == "result":
            subtype = str(payload.get("subtype") or "")
            if subtype == "success" and not payload.get("is_error"):
                return None
            detail = str(payload.get("result") or payload.get("error") or "")
            return f"claude result {subtype}: {detail}".strip()
        return None

    def orphan_patterns(self) -> list[str]:
        return ["claude"]
