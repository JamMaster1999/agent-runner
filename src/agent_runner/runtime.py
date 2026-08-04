"""The runner's own job runtime and error type (extraction plan §4 item 5).

Step-5 retype: every runner module (engine, jobstore, events, the attempts
store half, tracked_tasks, the harness adapters) speaks ``RunnerJob`` — a
frozen view derived purely from the wire ``SubmitRequest`` — and raises
``RunnerError``. Client vocabulary stays client-side: a client's facade is
the one place both vocabularies meet, wrapping ``RunnerError`` into its own
error type at the boundary.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_runner.protocol import SubmitRequest


def project_id() -> str:
    """The tenant every store statement scopes on (step-9 schema).

    Read at call time, not import time, so a test (or a future multi-tenant
    engine) can flip RUNNER_PROJECT_ID without re-importing the store
    modules. There is deliberately NO default: a silent fallback tenant
    means a second client that forgets the variable reads and writes the
    first client's rows — data comingling with no error. The client's
    bootstrap declares its own tenant once (RUNNER_PROJECT_ID) and
    ``jobstore.ensure_project`` registers it on first contact.
    """
    value = os.environ.get("RUNNER_PROJECT_ID")
    if not value:
        raise RunnerError(
            "RUNNER_PROJECT_ID is not set. Every store statement scopes on "
            "the tenant, so the runner refuses to guess one: set "
            "RUNNER_PROJECT_ID in the environment (the client bootstrap is "
            "the right place) before any database-touching runner operation.",
            code="project_unset",
            retryable=False,
            alert=True,
        )
    return value


class RunnerError(Exception):
    """A runner-module failure crossing toward the facade.

    ``code`` keeps the historical category VALUES byte-for-byte (event rows,
    ``error_details.category``, ``attempts.error_code`` are
    unchanged); ``retryable`` False is terminal proof; ``alert`` is the
    runner's "unalerted operator-worthy fact" flag — client facades map it
    onto their own notify vocabulary.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "unknown",
        retryable: bool = True,
        alert: bool = False,
        details: str = "",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.alert = alert
        self.details = details


@dataclass(frozen=True)
class RunnerJob:
    """One job as the runner sees it: nothing here that did not arrive in a
    ``SubmitRequest``. ``task_type``/``labels`` are opaque caller vocabulary
    (display/dispatch data, never parsed). ``agent_config`` is the agent's
    per-harness config table as submit DATA: None means the submitter never
    said (a loud error when an agent_ref is named), {} means a deliberately
    unconfigured session — the runner never reads the client's source tree
    to find out.
    """

    key: str                          # SubmitRequest.job_key
    group_key: str
    task_type: str                    # opaque caller vocabulary
    harness: str                      # adapter registry key
    agent_ref: str
    agent_config: dict[str, Any] | None = None
    labels: dict[str, Any] = field(default_factory=dict)
    attempt_dir_name: str = ""        # artifact_contract["attempt_dir_name"]
    output_filename: str = ""         # artifact_contract["output_filename"]
    canonical_relpath: str = ""       # artifact_contract["canonical_path"] (repo-relative, raw)
    probe_key: str = ""               # probe_spec["probe"]
    repair_rounds: int = 0            # probe_spec["repair_rounds"]
    expensive: bool = False           # probe_spec["expensive"]
    resource_specs: tuple[dict[str, Any], ...] = ()
    required_env: tuple[str, ...] = ()
    policy: dict[str, Any] = field(default_factory=dict)
    # {"attempt_timeout_minutes": int, "resume": bool}
    max_attempts: int | None = None
    prompt_template: str | None = None  # prompt_ref["template"]
    prompt_sha256: str | None = None    # prompt_ref["sha256"]
    request_identity: str | None = None

    @classmethod
    def from_submit(cls, request: SubmitRequest) -> "RunnerJob":
        contract = request.artifact_contract or {}
        probe_spec = request.probe_spec or {}
        prompt_ref = request.prompt_ref or {}
        return cls(
            key=request.job_key,
            group_key=request.group_key,
            task_type=request.task_type,
            harness=request.harness,
            agent_ref=request.agent_ref,
            agent_config=(
                None if request.agent_config is None else dict(request.agent_config)
            ),
            labels=dict(request.labels or {}),
            attempt_dir_name=str(contract.get("attempt_dir_name") or ""),
            output_filename=str(contract.get("output_filename") or ""),
            canonical_relpath=str(contract.get("canonical_path") or ""),
            probe_key=str(probe_spec.get("probe") or ""),
            repair_rounds=int(probe_spec.get("repair_rounds") or 0),
            expensive=bool(probe_spec.get("expensive")),
            resource_specs=tuple(dict(spec) for spec in (request.resource_specs or [])),
            required_env=tuple(request.required_env or []),
            policy=dict(request.policy or {}),
            max_attempts=request.max_attempts,
            prompt_template=prompt_ref.get("template"),
            prompt_sha256=prompt_ref.get("sha256"),
            request_identity=request.request_identity,
        )


@dataclass
class RunnerConfig:
    """The engine's explicit configuration contract (formerly an implicit
    argparse.Namespace attribute surface). Every engine/store entry point
    that historically took ``args`` accepts anything carrying these
    attributes; ``coerce`` fills documented defaults for whatever a caller
    left off, so a second client's minimal config object works instead of
    replicating one client's CLI flag surface.
    """

    database_url: str
    poll_seconds: float = 10
    no_sleep: bool = False
    force_rerun: bool = False
    retry_backoff_seconds: tuple[int, ...] = (60, 300, 900, 1800)
    health_timeout_seconds: int = 120
    run_claude_doctor: bool = False
    claude_health_budget_usd: float = 1.0
    claude_health_model: str = "sonnet"
    codex_health_model: str = ""

    _FIELDS = (
        "database_url",
        "poll_seconds",
        "no_sleep",
        "force_rerun",
        "retry_backoff_seconds",
        "health_timeout_seconds",
        "run_claude_doctor",
        "claude_health_budget_usd",
        "claude_health_model",
        "codex_health_model",
    )

    @classmethod
    def coerce(cls, args: Any) -> Any:
        """``args`` unchanged when it already carries every engine field;
        otherwise a RunnerConfig built from what it has, defaults filling the
        rest. ``database_url`` is the one field with no default — a caller
        that omits it (or supplies it empty, even on an otherwise complete
        config object) gets a loud error here rather than an opaque driver
        failure deep in a poll loop."""
        if not getattr(args, "database_url", None):
            raise RunnerError(
                "Runner config needs database_url (the runner store DSN).",
                code="invalid_config",
                retryable=False,
                alert=True,
            )
        missing = [name for name in cls._FIELDS if not hasattr(args, name)]
        if not missing:
            return args
        values = {
            name: getattr(args, name)
            for name in cls._FIELDS
            if hasattr(args, name)
        }
        return cls(**values)


@dataclass
class AttemptResult:
    """One valid attempt output: path + parsed data + the attempt number.
    Shared vocabulary on both sides of the facade (clients may re-export
    it under their own import path)."""

    path: Path
    data: dict[str, Any]
    attempt: int


def attempt_dir(run_dir: Path, job: RunnerJob, attempt: int) -> Path:
    """The attempt's private workspace: ``{run_dir}/{attempt_dir_name}/
    attempt-NN`` (created on first touch) — byte-identical to the
    historical client layout it replaces engine-side."""
    path = run_dir / job.attempt_dir_name / f"attempt-{attempt:02d}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def attempt_output_path(run_dir: Path, job: RunnerJob, attempt: int) -> Path:
    """The attempt's primary output file inside its workspace. The filename
    is submit data (``artifact_contract["output_filename"]``), equal to the
    client's own output filename for every live job by construction."""
    return attempt_dir(run_dir, job, attempt) / job.output_filename
