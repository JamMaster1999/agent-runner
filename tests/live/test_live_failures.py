"""Live failure surfaces, zero tokens: auth against isolated (broken) CLI homes, and
rate-limit / usage-limit against the stub endpoint — the real binary's whole client
stack runs; only the far end is ours. See stub_api for why."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent_runner import outcomes
from agent_runner.attempt import run_attempt

from .conftest import (
    CLAUDE_AGENT,
    CODEX_AGENT,
    claude_spec,
    codex_spec,
    require_claude,
    require_codex,
    require_live,
)
from .stub_api import (
    ANTHROPIC_RATE_LIMIT,
    ANTHROPIC_USAGE_LIMIT,
    OPENAI_RATE_LIMIT,
    OPENAI_USAGE_LIMIT,
    StubAPI,
)

pytestmark = [require_live]

TINY_TASK = "Reply with the word OK and stop."


@pytest.fixture()
def broken_claude_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty CLAUDE_CONFIG_DIR plus a syntactically-plausible-but-invalid API
    key: the CLI can never see the operator's real login."""
    home = tmp_path / "claude-home"
    home.mkdir()
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-api03-live-tier-invalid-key")
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    return home


@pytest.fixture()
def broken_codex_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "codex-home"
    home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    return home


@require_claude
def test_claude_auth_failure(workdir: Path, broken_claude_home: Path) -> None:
    report = run_attempt(
        claude_spec(), TINY_TASK, workdir, agent=CLAUDE_AGENT, timeout_minutes=3
    )
    assert report.outcome == outcomes.AUTH, (report.outcome, report.error, report.detail)


@require_codex
def test_codex_auth_failure(workdir: Path, broken_codex_home: Path) -> None:
    report = run_attempt(
        codex_spec(), TINY_TASK, workdir, agent=CODEX_AGENT, timeout_minutes=3
    )
    assert report.outcome == outcomes.AUTH, (report.outcome, report.error, report.detail)


def _claude_against_stub(status_body, workdir, monkeypatch, broken_home) -> str:
    with StubAPI(*status_body) as stub:
        monkeypatch.setenv("ANTHROPIC_BASE_URL", stub.url)
        report = run_attempt(
            claude_spec(required_env=("ANTHROPIC_BASE_URL",)),
            TINY_TASK,
            workdir,
            agent=CLAUDE_AGENT,
            timeout_minutes=4,
        )
        assert stub.hits > 0, "the CLI never called the stub endpoint"
    assert report.outcome == outcomes.RATE_LIMITED, (
        report.outcome,
        report.error,
        report.detail,
    )
    return report.detail


def _codex_against_stub(status_body, workdir, monkeypatch, home: Path) -> str:
    # codex ignores OPENAI_BASE_URL (probed live, 2026-08-09); the sanctioned
    # route is a model_providers table in the agent config, delivered as -c
    # overrides, with the key arriving through the provider's env_key.
    from agent_runner.harness.base import AgentDef

    with StubAPI(*status_body) as stub:
        monkeypatch.setenv("OPENAI_API_KEY", "sk-live-tier-fake")
        agent = AgentDef(
            name=CODEX_AGENT.name,
            description=CODEX_AGENT.description,
            config={
                "model_provider": "stub",
                "model_providers": {
                    "stub": {
                        "name": "stub",
                        "base_url": stub.url + "/v1",
                        "env_key": "OPENAI_API_KEY",
                        "wire_api": "responses",
                    }
                },
            },
            body=CODEX_AGENT.body,
        )
        report = run_attempt(
            codex_spec(), TINY_TASK, workdir, agent=agent, timeout_minutes=4
        )
        assert stub.hits > 0, "the CLI never called the stub endpoint"
    assert report.outcome == outcomes.RATE_LIMITED, (
        report.outcome,
        report.error,
        report.detail,
    )
    return report.detail


@require_claude
def test_claude_rate_limited(workdir, monkeypatch, broken_claude_home) -> None:
    _claude_against_stub(ANTHROPIC_RATE_LIMIT, workdir, monkeypatch, broken_claude_home)


@require_claude
def test_claude_usage_limit_classifies_rate_limited(
    workdir, monkeypatch, broken_claude_home
) -> None:
    detail = _claude_against_stub(
        ANTHROPIC_USAGE_LIMIT, workdir, monkeypatch, broken_claude_home
    )
    assert "usage limit" in detail.lower()


@require_codex
def test_codex_rate_limited(workdir, monkeypatch, broken_codex_home) -> None:
    _codex_against_stub(OPENAI_RATE_LIMIT, workdir, monkeypatch, broken_codex_home)


@require_codex
def test_codex_usage_limit_classifies_rate_limited(
    workdir, monkeypatch, broken_codex_home
) -> None:
    detail = _codex_against_stub(
        OPENAI_USAGE_LIMIT, workdir, monkeypatch, broken_codex_home
    )
    assert "usage limit" in detail.lower()
