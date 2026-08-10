"""Live tier: REAL codex/claude CLI processes, real token spend, run on purpose.

The fake-CLI rig (tests/test_attempt_fake_cli.py) proves the attempt loop against scripted
streams; this tier proves the adapters against the CLIs they actually wrap — spawn dialects,
session resume that really recalls context, repair into a genuinely open session, and the
failure surfaces (auth, rate limit, usage limit) as the real binaries render them.

Gated three ways, strictest first:
- RUN_LIVE=1 must be set — CI never sets it, so the default suite stays token-free
- each harness skips when its binary is missing
- the temporal module skips when temporalio is not installed

Every test isolates AGENT_RUNNER_PROJECT_ROOT and RUNNER_STATE_DIR into tmp_path: discovery
files, attempt dirs, and hook logs never touch the operator's real project or CLI homes.
Auth-failure tests additionally isolate CLAUDE_CONFIG_DIR / CODEX_HOME so the operator's real
credentials are never read (or clobbered) by a test that wants a broken login.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from agent_runner.harness.base import AgentDef
from agent_runner.harness.claude_code import claude_command
from agent_runner.harness.codex import codex_command
from agent_runner.runtime import RunSpec, Verdict

LIVE = os.environ.get("RUN_LIVE") == "1"

require_live = pytest.mark.skipif(not LIVE, reason="live tier: set RUN_LIVE=1 to spend tokens")
require_claude = pytest.mark.skipif(claude_command() is None, reason="claude CLI not installed")
require_codex = pytest.mark.skipif(codex_command() is None, reason="codex CLI not installed")

# Small model, tiny prompts: the tier proves plumbing, not intelligence.
CLAUDE_AGENT = AgentDef(
    name="live-probe",
    description="agent-runner live-tier probe agent",
    config={"model": "haiku"},
    body="You are a minimal test agent. Do exactly what the task says, then stop. "
    "No commentary beyond what the task asks for.\n",
)

CODEX_AGENT = AgentDef(
    name="live-probe",
    description="agent-runner live-tier probe agent",
    config={},
    body="You are a minimal test agent. Do exactly what the task says, then stop. "
    "No commentary beyond what the task asks for.\n",
)


@pytest.fixture()
def project_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    monkeypatch.setenv("AGENT_RUNNER_PROJECT_ROOT", str(root))
    monkeypatch.setenv("RUNNER_STATE_DIR", str(root / ".runner-state"))
    return root


@pytest.fixture()
def workdir(project_root: Path) -> Path:
    directory = project_root / "attempt-1"
    directory.mkdir()
    return directory


def claude_spec(**policy_overrides) -> RunSpec:
    # setting_sources=["project"] keeps the operator's user-global Claude
    # state out, exactly as production spawns do.
    policy = {"setting_sources": ["project"], **policy_overrides}
    required_env = tuple(policy.pop("required_env", ()))
    repair_rounds = int(policy.pop("repair_rounds", 0))
    return RunSpec(
        key="live-claude",
        harness="claude",
        agent_ref=CLAUDE_AGENT.name,
        policy=policy,
        required_env=required_env,
        repair_rounds=repair_rounds,
    )


def codex_spec(**policy_overrides) -> RunSpec:
    policy = dict(policy_overrides)
    required_env = tuple(policy.pop("required_env", ()))
    repair_rounds = int(policy.pop("repair_rounds", 0))
    return RunSpec(
        key="live-codex",
        harness="codex",
        agent_ref=CODEX_AGENT.name,
        policy=policy,
        required_env=required_env,
        repair_rounds=repair_rounds,
    )


def file_check(name: str, needle: str, repair: str | None = None):
    """A validate closure: workdir/<name> must exist and contain <needle>
    (case-insensitive). ``repair`` becomes the repair_message on a miss."""

    def validate(workdir: Path) -> Verdict:
        path = workdir / name
        if not path.is_file():
            return Verdict(
                valid=False,
                message=f"{name} was not written",
                repair_message=repair,
            )
        text = path.read_text()
        if needle.lower() not in text.lower():
            return Verdict(
                valid=False,
                message=f"{name} does not contain {needle!r}",
                repair_message=repair,
            )
        return Verdict(valid=True, data={"text": text})

    return validate
