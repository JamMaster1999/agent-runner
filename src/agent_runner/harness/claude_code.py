"""Claude Code harness adapter: every claude-CLI-specific behavior in one
module. Spawn/resume command shapes, session extraction, the 5-hook map,
the error-report dialect, terminal markers, env quirks, and the
volume-backed credential model — the runner core never spells this CLI's
name.

The Claude dialect is file-based: agents spawn by name from a rendered
discovery file under ``<project_root>/.claude/agents/``. ``prepare_agent``
writes that file from an ``AgentDef``, so spawn still takes an agent
definition and a task message with nothing authored on disk by the caller."""

from __future__ import annotations

import glob
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Mapping

from agent_runner import outcomes, util
from agent_runner.auth import normalize_token, seed_credential_file
from agent_runner.harness.base import (
    COMMON_TERMINAL_MARKERS,
    AgentDef,
    Capabilities,
    HarnessAdapter,
    SpawnSpec,
)
from agent_runner.runtime import RunnerError, RunSpec
from agent_runner.harness.claude_stream import ClaudeStreamParser
from agent_runner.util import write_text


def claude_command() -> str | None:
    """PATH lookup with the RUNNER_CLAUDE_CLI environment override (the
    override is also what points the fake-CLI test rig at a stub)."""
    override = os.environ.get("RUNNER_CLAUDE_CLI")
    if override and Path(override).exists():
        return override
    return shutil.which("claude")


def yaml_scalar(value: object) -> str:
    """One frontmatter value as a YAML scalar."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)  # JSON strings are valid YAML double-quoted scalars
    if not isinstance(value, (int, float, bool)):
        raise RunnerError(
            f"claude agent config: unsupported frontmatter value: {value!r}",
            code="agent_render",
            retryable=False,
        )
    return str(value)


class ClaudeCodeAdapter(HarnessAdapter):
    """The Claude Code CLI (`claude --print --output-format stream-json`,
    prompt on stdin)."""

    name: ClassVar[str] = "claude"
    display_name: ClassVar[str] = "Claude"
    start_label: ClassVar[str] = "Claude"
    session_noun: ClassVar[str] = "session"
    # resume/followup: `claude --resume <session>`.
    # doctor=False: `claude doctor` exists but is unstructured.
    capabilities: ClassVar[Capabilities] = Capabilities(
        resume=True,
        followup=True,
        hooks=True,
        doctor=False,
        final_message_artifact=False,
    )
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
        COMMON_TERMINAL_MARKERS
    )

    def prepare_home(self, volume_root: Path, env: Mapping[str, str]) -> dict[str, str]:
        """CLAUDE_CONFIG_DIR on the volume (config, credentials, and session
        transcripts live under it); the credentials file seeded once from
        the CLAUDE_CREDENTIALS_JSON environment value when supplied. A
        CLAUDE_CODE_OAUTH_TOKEN riding the environment is re-exported
        normalized so a wrapped paste never reaches the CLI."""
        home = Path(volume_root) / "claude-home"
        home.mkdir(parents=True, exist_ok=True)
        seed = env.get("CLAUDE_CREDENTIALS_JSON")
        if seed:
            seed_credential_file(home / ".credentials.json", seed)
        overrides = {"CLAUDE_CONFIG_DIR": str(home)}
        token = env.get("CLAUDE_CODE_OAUTH_TOKEN")
        if token:
            overrides["CLAUDE_CODE_OAUTH_TOKEN"] = normalize_token(token)
        return overrides

    def bind_credentials(self) -> dict[str, str]:
        """Token normalization on read (ruling D1): whatever token rides the
        engine environment reaches the CLI whitespace-free."""
        token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        if token:
            return {"CLAUDE_CODE_OAUTH_TOKEN": normalize_token(token)}
        return {}

    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The `.claude/agents/<name>.md` dialect: YAML frontmatter between
        `---` lines (header comment first, then name + description, then the
        config keys in dict order), a blank line, then the verbatim body."""
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

    def prepare_agent(self, agent: AgentDef) -> dict[str, Any] | None:
        """Write the discovery file the CLI resolves ``--agent <name>``
        from; config rides the file, so the returned agent_config is None."""
        text = self.materialize_agent(agent, "GENERATED by agent-runner — do not edit")
        write_text(
            util.project_root() / ".claude" / "agents" / f"{agent.name}.md", text
        )
        return None

    def build_spawn(self, spec: RunSpec, directory: Path) -> SpawnSpec:
        command_path = claude_command()
        if not command_path:
            raise RunnerError(
                "Required command not found: claude",
                code="missing_command",
                retryable=False,
                alert=True,
            )
        # The rendered discovery file is this adapter's own materialize_agent
        # dialect; a missing one used to surface as an opaque CLI error that
        # burned the full retry budget — check it up front and fail loudly.
        agent_path = util.project_root() / ".claude" / "agents" / f"{spec.agent_ref}.md"
        if not agent_path.is_file():
            raise RunnerError(
                f"{spec.key}: rendered Claude agent not found: {agent_path}. "
                "Pass the agent definition (run_attempt(agent=...)) or "
                "materialize it (prepare_agent) before spawning.",
                code="missing_claude_agent",
                retryable=False,
                alert=True,
            )
        command = [
            command_path,
            "--agent",
            spec.agent_ref,
            "--permission-mode",
            "bypassPermissions",
        ]
        # Tool restrictions are caller DATA (policy.disallowed_tools), not
        # a business rule baked into every spawn.
        if spec.policy.disallowed_tools:
            command.append(
                "--disallowedTools=" + ",".join(str(t) for t in spec.policy.disallowed_tools)
            )
        # Setting-source isolation as caller data: e.g. ("project",) keeps the
        # operator's user-global Claude state (plugins, skills, personal
        # memory) out of production sessions — the claude-side counterpart of
        # pinning CODEX_HOME.
        if spec.policy.setting_sources is not None:
            command += [
                "--setting-sources",
                ",".join(str(s) for s in spec.policy.setting_sources),
            ]
        # Reasoning effort as caller data: the CLI accepts effort only as the
        # session-level --effort flag; agent frontmatter has no effort key.
        # Unknown values are warn-and-ignored by the CLI, so passthrough is
        # safe.
        if spec.policy.effort:
            command += ["--effort", str(spec.policy.effort)]
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

    def build_resume(self, spec: RunSpec, directory: Path, session_ref: str) -> SpawnSpec:
        spawn = self.build_spawn(spec, directory)
        return SpawnSpec(
            command=spawn.command + ["--resume", session_ref],
            stdout_path=spawn.stdout_path,
            stderr_path=spawn.stderr_path,
        )

    def build_followup(
        self, spec: RunSpec, directory: Path, session_ref: str
    ) -> SpawnSpec | None:
        if not claude_command():
            return None
        resume = self.build_resume(spec, directory, session_ref)
        return SpawnSpec(
            command=resume.command,
            stdout_path=directory / "claude.repair.stdout.log",
            stderr_path=directory / "claude.repair.stderr.log",
        )

    def env_overrides(self) -> dict[str, str]:
        return {"CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS": "10000"}

    def env_passthrough(self) -> tuple[str, ...]:
        # CLI auth + config-home names the filtered agent env must inherit.
        return (
            "CLAUDE_CODE_OAUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "CLAUDE_CONFIG_DIR",
            "RUNNER_CLAUDE_CLI",
        )

    def session_ref_from_event(self, payload: dict[str, Any]) -> str | None:
        # Every claude --print stream-json event carries session_id.
        session_id = payload.get("session_id")
        return str(session_id) if session_id else None

    def session_state(self, session_ref: str) -> tuple[Path, list[Path]] | None:
        """One transcript per session under CLAUDE_CONFIG_DIR, in a folder
        named after the cwd it ran in:
        ``projects/<escaped-project-path>/<session_id>.jsonl``. The folder
        name is the CLI's own escaping, so it is matched by glob and
        restored under the name it was written with."""
        home = os.environ.get("CLAUDE_CONFIG_DIR")
        if not home:
            return None
        home_path = Path(home)
        pattern = f"projects/*/{glob.escape(session_ref)}.jsonl"
        return home_path, sorted(home_path.glob(pattern))

    def stream_parser(self) -> ClaudeStreamParser:
        return ClaudeStreamParser()

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

    def stream_fatal(self, payload: dict[str, Any]) -> RunnerError | None:
        """Typed stream evidence that the attempt cannot succeed — the event's
        own fields decide the outcome, immune to CLI error-text rewording.

        A ``rate_limit_event`` with status ``rejected`` is the subscription
        cap (live tier, 2026-08-21: the CLI then prints a synthetic "You've
        hit your session limit" turn, exits 0, and the result carries
        ``is_error``); it classifies ``rate_limited`` with the reset time
        typed on the error, so the retry can wait exactly that long. A 401/403 ``api_retry`` event is a dead credential the
        CLI will retry ~10 times over ~20 minutes of exponential backoff
        (2026-08-09); the attempt must fail as auth now, not as timeout after
        the ladder runs out."""
        if payload.get("type") == "rate_limit_event":
            info = payload.get("rate_limit_info") or {}
            if info.get("status") != "rejected":
                return None
            message = f"claude rate_limit_event: {info.get('rateLimitType') or 'limit'} rejected"
            try:
                resets_at = datetime.fromtimestamp(int(info["resetsAt"]), tz=timezone.utc)
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                resets_at = None
            if resets_at is not None:
                message += f", resets {resets_at.isoformat(timespec='seconds')}"
            if info.get("overageDisabledReason"):
                message += f" ({info['overageDisabledReason']})"
            return RunnerError(
                message,
                code=outcomes.RATE_LIMITED,
                retryable=True,
                alert=False,
                details=message,
                resets_at=resets_at,
            )
        if payload.get("type") == "system" and payload.get("subtype") == "api_retry":
            try:
                status = int(payload.get("error_status") or 0)
            except (TypeError, ValueError):
                return None
            if status in (401, 403):
                message = (
                    f"claude api_retry: {payload.get('error') or 'auth failure'} "
                    f"(HTTP {status})"
                )
                return RunnerError(
                    message,
                    code=outcomes.AUTH,
                    retryable=False,
                    alert=True,
                    details=message,
                )
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
