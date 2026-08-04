"""Harness adapter contract: Capabilities, SpawnSpec, and HarnessAdapter
(design doc §2).

The adapter is the load-bearing wall: runner core contains zero harness-name
branches, and every provider difference — binary discovery, spawn/resume/
followup command shapes, stream and hook dialects, terminal-failure marker
data, env quirks, health commands — lives in one adapter module registered
under the job's harness name. The base class carries the judgment-free
shared machinery (marker scan, error-report shell, health-command runner);
adapters supply evidence and dialects only. Policy — what a failure means
for the job — stays in the engine; attempt timeout and resume eligibility
arrive as submit DATA (``job.policy``), so the adapter no longer carries
those slots (step-5 retype).
"""

from __future__ import annotations

import argparse
import json
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from agent_runner import util
from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.util import read_tail


@dataclass(frozen=True)
class Capabilities:
    """Proven degradation flags, not speculation (design doc §2.1): every
    False path already runs in production. No ``resume`` -> every attempt is
    fresh; no ``followup`` -> validation failure goes straight to retry; no
    ``hooks`` -> stream telemetry only; no ``doctor`` -> binary presence
    plus a capped live probe."""

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
    """One agent definition crossing the adapter boundary as DATA
    (extraction plan §2, gray zone 4): the runner never reads the client's
    source tree. ``config`` is the agent's per-harness frontmatter table
    (the [claude] / [codex] contents); ``body`` is the verbatim prompt —
    the caller guarantees the trailing newline."""

    name: str
    description: str
    config: dict[str, Any]
    body: str


class HarnessAdapter(ABC):
    """One agent CLI's contract with the engine (design doc §2.2).

    ``name`` is the registry key and equals the job's harness value. The
    display strings feed operator-facing event messages only; nothing ever
    parses them. Hook wiring is repo-static in phase 2 (the capture scripts
    under .claude/.codex); ``hook_event_log`` is where that wiring lands its
    events.
    """

    name: ClassVar[str]
    capabilities: ClassVar[Capabilities]
    display_name: ClassVar[str]      # "<display_name> wrote valid output; ..."
    start_label: ClassVar[str]       # "Started <start_label> phase5 attempt 2"
    session_noun: ClassVar[str]      # what this CLI calls a resumable session

    # Failure classification (2026-07-28 policy): markers are matched ONLY
    # against CLI-owned error text — typed stream error events (error_report),
    # CLI stderr, or the output of CLI health commands — never agent transcript
    # tails, whose web-research content can contain tokens like '403' or 'api
    # key' incidentally. Terminal only when the CLI itself reports auth expiry,
    # billing/quota exhaustion, or an invalid invocation; everything else,
    # including unparseable/ambiguous output, retries as code 'unknown'.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = ()

    # -- discovery & health ------------------------------------------------

    @abstractmethod
    def resolve_binary(self) -> str | None:
        """Path to the CLI binary (PATH plus any app-bundle fallbacks);
        None when not installed."""

    @abstractmethod
    def health_checks(self, args: argparse.Namespace) -> None:
        """Run this CLI's preflight health checks; raise RunnerError on a
        hard failure."""

    def run_health_command(self, command: list[str], timeout: int) -> str:
        try:
            result = subprocess.run(
                command,
                cwd=util.project_root(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            output = ((exc.stdout or "") if isinstance(exc.stdout, str) else "") + "\n" + (
                (exc.stderr or "") if isinstance(exc.stderr, str) else ""
            )
            raise RunnerError(
                f"{self.name} health command timed out: {' '.join(command[:3])}",
                code="health_timeout",
                retryable=True,
                details=output,
            ) from exc
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        if result.returncode != 0:
            failure = self.classify_failure(output)
            failure.details = output
            raise failure
        return output

    # -- credentials — the account-rotation hook ---------------------------

    def bind_credentials(self) -> dict[str, str]:
        """Env for the attempt's credentials. Phase 2 has exactly one
        secret_ref, 'local-login' — the Mac's logged-in CLI state — which
        binds to nothing (D6); a real store arrives with account rotation."""
        return {}

    # -- agent materialization ---------------------------------------------

    @abstractmethod
    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The complete discovery-file text for ``agent`` in this harness's
        dialect (extraction step 7: rendering moved behind the adapter API;
        authoring rules, validation, naming, and pruning stay with the
        client). ``header`` is a caller-supplied comment STRING with no
        comment syntax — the adapter wraps it in its own dialect — so the
        GENERATED-marker convention stays the client's. Render constraints
        raise RunnerError (code='agent_render', retryable=False) naming the
        harness."""

    # -- spawn / resume / followup -----------------------------------------

    @abstractmethod
    def build_spawn(self, job: RunnerJob, directory: Path) -> SpawnSpec:
        """Fresh-attempt invocation."""

    @abstractmethod
    def build_resume(self, job: RunnerJob, directory: Path, session_ref: str) -> SpawnSpec:
        """Reopen ``session_ref`` and continue the same job."""

    def build_followup(
        self, job: RunnerJob, directory: Path, session_ref: str
    ) -> SpawnSpec | None:
        """Message injection into an existing session (the repair path);
        None when unsupported (``capabilities.followup`` is False) or the
        binary is missing — the engine falls back to a plain retry."""
        return None

    def env_overrides(self) -> dict[str, str]:
        """Harness-specific env quirks layered over the shared agent env."""
        return {}

    def env_passthrough(self) -> tuple[str, ...]:
        """Environment names this harness's CLI needs inherited from the
        engine when the filtered agent environment is in effect (auth
        tokens, CLI home overrides). Names only — the engine copies the
        values from its own environment when present."""
        return ()

    # NOTE (step-5 retype): the ``attempt_timeout_minutes``/``resume_allowed``
    # adapter slots are DELETED — both are submit data now
    # (``job.policy["attempt_timeout_minutes"]`` / ``job.policy["resume"]``,
    # folded from the same args/phase logic by the client's submit_policy).

    # -- session resume ----------------------------------------------------

    @abstractmethod
    def session_ref_from_log(self, stdout_path: Path) -> str | None:
        """The opaque session ref from a captured stdout stream, for resume
        follow-ups; None until the CLI has emitted it (design doc §2.2
        session_ref_from, phase-2 shape: read from the attempt log)."""

    # NOTE (extraction step 4, design §7.5): the consume_legacy_session hook
    # and the per-harness filesystem resume matchers are DELETED, not ported.
    # Resume rights ride the pipeline_attempts store exclusively
    # (claim_resumable_attempt), pinned by tests/test_resume_claim_sql.py.

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

    # -- failure: the adapter supplies EVIDENCE, the engine supplies JUDGMENT

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
        means no terminal proof and the core default applies. Callers must
        never pass agent transcript tails; use ``error_report`` for attempt
        logs."""
        lower = (text or "").lower()
        for code, markers in self.terminal_markers:
            if any(marker in lower for marker in markers):
                return RunnerError(
                    f"{self.name} terminal failure: {code}",
                    code=code,
                    retryable=False,
                    alert=True,
                    details=text,
                )
        return None

    def classify_failure(self, text: str) -> RunnerError:
        """``classify`` plus the core default: no terminal proof from the
        CLI's own text means retryable, code 'unknown'."""
        classified = self.classify(text)
        if classified is not None:
            return classified
        return RunnerError(
            f"{self.name} attempt failed",
            code="unknown",
            retryable=True,
            alert=False,
            details=text,
        )

    # -- hygiene -----------------------------------------------------------

    def orphan_patterns(self) -> list[str]:
        """``pgrep -fl`` patterns matching this harness's agent processes,
        for the reaper's orphan-process hint."""
        return [self.name]
