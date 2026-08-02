"""The runner's own job runtime and error type (extraction plan §4 item 5).

Step-5 retype: every runner module (engine, jobstore, events, the attempts
store half, tracked_tasks, the harness adapters) speaks ``RunnerJob`` — a
frozen view derived purely from the wire ``SubmitRequest`` — and raises
``RunnerError``. GTM vocabulary (``JobRuntime``/``JobSpec``/``PipelineError``)
stays in ``core/runner/types.py`` for the chain driver; the local facade
(``core/runner/local.py``) is the one place both vocabularies meet, wrapping
``RunnerError`` into ``PipelineError`` at the boundary.
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
    modules. 'gtm' matches the projects row migration 001 seeds — the
    single-tenant floor.
    """
    return os.environ.get("RUNNER_PROJECT_ID", "gtm")


class RunnerError(Exception):
    """A runner-module failure crossing toward the facade.

    ``code`` keeps the historical category VALUES byte-for-byte (event rows,
    ``error_details.category``, ``pipeline_attempts.failure_category`` are
    unchanged); ``retryable`` False is terminal proof; ``alert`` is the
    runner's "unalerted operator-worthy fact" flag — the facade maps it back
    to GTM's ``notify_now``.
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
    (display/dispatch data, never parsed); ``client_refs`` is TRANSITIONAL
    ({"institution_id"}) and dies with the business FK at cutover (plan §3).
    """

    key: str                          # SubmitRequest.job_key
    group_key: str
    task_type: str                    # opaque caller vocabulary
    harness: str                      # adapter registry key
    agent_ref: str
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
    client_refs: dict[str, str] = field(default_factory=dict)  # TRANSITIONAL

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
            client_refs=dict(request.client_refs or {}),
        )


@dataclass
class AttemptResult:
    """One valid attempt output: path + parsed data + the attempt number.
    Shared vocabulary on both sides of the facade (re-exported from
    ``core/runner/types.py`` for the GTM import path)."""

    path: Path
    data: dict[str, Any]
    attempt: int


def attempt_dir(run_dir: Path, job: RunnerJob, attempt: int) -> Path:
    """The attempt's private workspace: ``{run_dir}/{attempt_dir_name}/
    attempt-NN`` (created on first touch) — byte-identical to the GTM
    GTM ``artifacts.attempt_dir`` layout it replaces engine-side."""
    path = run_dir / job.attempt_dir_name / f"attempt-{attempt:02d}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def attempt_output_path(run_dir: Path, job: RunnerJob, attempt: int) -> Path:
    """The attempt's primary output file inside its workspace. The filename
    is submit data (``artifact_contract["output_filename"]``), equal to the
    GTM ``phase_output_filename`` for every live job by construction."""
    return attempt_dir(run_dir, job, attempt) / job.output_filename
