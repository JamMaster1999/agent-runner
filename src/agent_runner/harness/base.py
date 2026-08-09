"""Harness adapter contract: Capabilities, SpawnSpec, AgentDef, and
HarnessAdapter.

The adapter is the load-bearing wall: runner core contains zero
harness-name branches, and every provider difference — binary discovery,
spawn/resume/followup command shapes, stream and hook dialects,
terminal-failure marker data, env quirks, credential-file models — lives in
one adapter module registered under the spec's harness name. The base class
carries the judgment-free shared machinery (marker scan, error-report
shell); adapters supply evidence and dialects only. Policy — what a failure
means for the run — stays with the caller (the Temporal layer ships the
ruled mapping).
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Mapping

from agent_runner import outcomes
from agent_runner.runtime import RunnerError, RunSpec
from agent_runner.util import read_tail


@dataclass(frozen=True)
class Capabilities:
    """Proven degradation flags, not speculation: every False path already
    ran in production. No ``resume`` -> every attempt is fresh; no
    ``followup`` -> validation failure goes straight to retry; no ``hooks``
    -> stream telemetry only."""

    resume: bool = False             # can reopen a session and continue
    followup: bool = False           # can inject a message into an existing session (repair)
    hooks: bool = False              # emits lifecycle hooks the runner can capture
    doctor: bool = False             # structured self-diagnosis command
    final_message_artifact: bool = False


@dataclass(frozen=True)
class SpawnSpec:
    """One CLI invocation: the command plus where its streams land. The
    prompt (or follow-up message) always arrives on stdin."""

    command: list[str]
    stdout_path: Path
    stderr_path: Path


@dataclass(frozen=True)
class AgentDef:
    """One agent definition crossing the adapter boundary as DATA: the
    runner never reads the caller's source tree. ``config`` is the agent's
    per-harness frontmatter table; ``body`` is the verbatim prompt — the
    caller guarantees the trailing newline."""

    name: str
    description: str
    config: dict[str, Any]
    body: str


class HarnessAdapter(ABC):
    """One agent CLI's contract with the attempt loop.

    ``name`` is the registry key and equals the spec's harness value. The
    display strings feed operator-facing event messages only; nothing ever
    parses them.
    """

    name: ClassVar[str]
    capabilities: ClassVar[Capabilities]
    display_name: ClassVar[str]      # "<display_name> wrote valid output; ..."
    start_label: ClassVar[str]       # "Started <start_label> attempt 2"
    session_noun: ClassVar[str]      # what this CLI calls a resumable session

    # Failure classification (2026-07-28 policy, restated in the stage-3
    # vocabulary): markers are matched ONLY against CLI-owned error text —
    # typed stream error events (error_report) or CLI stderr — never agent
    # transcript tails, whose web-research content can contain tokens like
    # '403' or 'api key' incidentally. Marker codes ARE outcome words
    # (agent_runner.outcomes); anything unmatched classifies ``infra``.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = ()

    # -- discovery ---------------------------------------------------------

    @abstractmethod
    def resolve_binary(self) -> str | None:
        """Path to the CLI binary (PATH plus any fallbacks, plus the
        RUNNER_<NAME>_CLI environment override); None when not installed."""

    # -- credentials & homes (the Modal model, ruling D1) ------------------

    def prepare_home(self, volume_root: Path, env: Mapping[str, str]) -> dict[str, str]:
        """Point this CLI's home at a directory under ``volume_root`` and
        seed its credential file there once from ``env`` when absent —
        refreshed tokens the CLI writes back then persist to the volume.
        Returns environment overrides (home + auth names). Default: no
        credential-file model."""
        return {}

    def bind_credentials(self) -> dict[str, str]:
        """Env for the attempt's credentials, normalized on read (token
        normalization is here so a corrupted operator paste never reaches
        the CLI)."""
        return {}

    # -- agent materialization ---------------------------------------------

    @abstractmethod
    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The complete discovery-file text for ``agent`` in this harness's
        dialect. ``header`` is a caller-supplied comment STRING with no
        comment syntax — the adapter wraps it in its own dialect. Render
        constraints raise RunnerError (code='agent_render', retryable=False)
        naming the harness."""

    def prepare_agent(self, agent: AgentDef) -> dict[str, Any] | None:
        """Make ``agent`` spawnable in this harness's dialect and return the
        effective ``agent_config`` for the spawn (None keeps the spec's).
        File-based dialects write their discovery file here; config-based
        dialects fold the body into the returned table. Default: the
        agent's config table, untouched."""
        return dict(agent.config)

    # -- spawn / resume / followup -----------------------------------------

    @abstractmethod
    def build_spawn(self, spec: RunSpec, directory: Path) -> SpawnSpec:
        """Fresh-attempt invocation."""

    @abstractmethod
    def build_resume(self, spec: RunSpec, directory: Path, session_ref: str) -> SpawnSpec:
        """Reopen ``session_ref`` and continue the same task."""

    def build_followup(
        self, spec: RunSpec, directory: Path, session_ref: str
    ) -> SpawnSpec | None:
        """Message injection into an existing session (the repair path);
        None when unsupported (``capabilities.followup`` is False) or the
        binary is missing — the attempt loop falls back to a plain retry."""
        return None

    def env_overrides(self) -> dict[str, str]:
        """Harness-specific env quirks layered over the shared agent env."""
        return {}

    def env_passthrough(self) -> tuple[str, ...]:
        """Environment names this harness's CLI needs inherited from the
        engine when the filtered agent environment is in effect (auth
        tokens, CLI home overrides). Names only — the attempt loop copies
        the values from its own environment when present."""
        return ()

    # -- session resume ----------------------------------------------------

    @abstractmethod
    def session_ref_from_log(self, stdout_path: Path) -> str | None:
        """The opaque session ref from a captured stdout stream, for resume
        follow-ups; None until the CLI has emitted it."""

    # -- telemetry ---------------------------------------------------------

    @abstractmethod
    def stream_parser(self) -> Any:
        """A fresh per-attempt stdout parser (``parse_line(line)`` ->
        StreamEvent list)."""

    @abstractmethod
    def hook_event_log(self) -> Path:
        """Where this harness's hook-capture wiring appends events."""

    @abstractmethod
    def normalize_hook_event(
        self, event: dict[str, Any], agent_name: str
    ) -> tuple[str, str] | None:
        """Convert one raw captured hook event to ``(event_name, message)``;
        None drops it."""

    # -- failure: the adapter supplies EVIDENCE, the caller supplies JUDGMENT

    @abstractmethod
    def stream_error_line(self, payload: dict[str, Any]) -> str | None:
        """One typed stream event rendered as CLI-owned error text; None for
        non-error events (the dialect hook under ``error_report``)."""

    def error_report(self, stdout_path: Path, stderr_path: Path) -> str:
        """The CLI's own error report for a failed attempt, for
        ``classify_failure``.

        Prefers the typed error events in the JSON stream on stdout (via the
        adapter's ``stream_error_line`` dialect hook); falls back to the
        stderr tail, which carries only CLI diagnostics. The stdout
        transcript tail is deliberately never used: it holds web-research
        tool output where auth/billing-looking tokens appear incidentally.
        """
        errors: list[str] = []
        try:
            with stdout_path.open() as fh:
                for line in fh:
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    error = self.stream_error_line(payload)
                    if error is not None:
                        errors.append(error)
        except OSError:
            pass
        if errors:
            return "\n".join(errors[-5:])
        return read_tail(stderr_path, 4000).strip()

    def classify(self, text: str) -> RunnerError | None:
        """Terminal-failure evidence from this harness's marker data; None
        means no proof and the core default (``infra``) applies. Callers
        must never pass agent transcript tails; use ``error_report`` for
        attempt logs."""
        lower = (text or "").lower()
        for code, markers in self.terminal_markers:
            if any(marker in lower for marker in markers):
                terminal = code in outcomes.TERMINAL
                return RunnerError(
                    f"{self.name} {code} failure reported by the CLI",
                    code=code,
                    retryable=not terminal,
                    alert=terminal,
                    details=text,
                )
        return None

    def classify_failure(self, text: str) -> RunnerError:
        """``classify`` plus the core default: no proof from the CLI's own
        text means ``infra`` — retryable, says nothing about the job."""
        classified = self.classify(text)
        if classified is not None:
            return classified
        return RunnerError(
            f"{self.name} attempt failed",
            code=outcomes.INFRA,
            retryable=True,
            alert=False,
            details=text,
        )

    # -- hygiene -----------------------------------------------------------

    def orphan_patterns(self) -> list[str]:
        """``pgrep -fl`` patterns matching this harness's agent processes,
        for a worker's orphan-process sweep."""
        return [self.name]
