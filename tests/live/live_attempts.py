"""Live token runs: valid output, session resume that recalls context, in-session
repair, and the timeout reap — against the real CLIs. See conftest for gating."""

from __future__ import annotations

from pathlib import Path

from agent_runner import outcomes
from agent_runner.attempt import run_attempt

from .conftest import (
    CLAUDE_AGENT,
    CODEX_AGENT,
    claude_spec,
    codex_spec,
    file_check,
    require_claude,
    require_codex,
    require_live,
)

pytestmark = [require_live]

CODEWORD = "MANGO-42"


@require_claude
def test_claude_valid_file_write(workdir: Path) -> None:
    report = run_attempt(
        claude_spec(),
        "Write exactly the word PONG into the file {{RUNNER_OUTPUT_PATH}}/out.txt, then stop.",
        workdir,
        agent=CLAUDE_AGENT,
        validate=file_check("out.txt", "PONG"),
        timeout_minutes=6,
    )
    assert report.outcome == outcomes.VALID, report.error
    assert report.session_ref, "session ref never surfaced from the stream"
    assert report.usage.tok_output > 0, "usage telemetry recorded nothing"


@require_codex
def test_codex_valid_file_write(workdir: Path) -> None:
    report = run_attempt(
        codex_spec(),
        "Write exactly the word PONG into the file {{RUNNER_OUTPUT_PATH}}/out.txt, then stop.",
        workdir,
        agent=CODEX_AGENT,
        validate=file_check("out.txt", "PONG"),
        timeout_minutes=6,
    )
    assert report.outcome == outcomes.VALID, report.error
    assert report.session_ref, "thread id never surfaced from the stream"


def _resume_recalls_codeword(spec_factory, agent, project_root: Path) -> None:
    first_dir = project_root / "attempt-first"
    first_dir.mkdir()
    first = run_attempt(
        spec_factory(),
        f"Remember this codeword: {CODEWORD}. Reply with OK and stop. Do not write any files.",
        first_dir,
        agent=agent,
        timeout_minutes=6,
    )
    assert first.outcome == outcomes.VALID, first.error
    assert first.session_ref, "no session ref to resume"

    second_dir = project_root / "attempt-second"
    second_dir.mkdir()
    second = run_attempt(
        spec_factory(),
        "Write the codeword from earlier in this conversation into the file "
        "{{RUNNER_OUTPUT_PATH}}/codeword.txt, then stop.",
        second_dir,
        agent=agent,
        session_ref=first.session_ref,
        session_usage=first.session_usage,
        validate=file_check("codeword.txt", CODEWORD),
        timeout_minutes=6,
    )
    assert second.outcome == outcomes.VALID, second.error
    assert second.resumed
    assert CODEWORD.lower() in (second.data or {}).get("text", "").lower()
    # The second attempt's own spend, measured from the first's total: a
    # resumed run pays for its own turn, never again for the first.
    assert second.usage.tok_output > 0, "usage telemetry recorded nothing for the resumed attempt"
    assert second.session_usage.tok_output > first.session_usage.tok_output
    assert second.session_usage.tok_output - first.session_usage.tok_output == second.usage.tok_output


@require_claude
def test_claude_resume_recalls_context(project_root: Path) -> None:
    _resume_recalls_codeword(claude_spec, CLAUDE_AGENT, project_root)


@require_codex
def test_codex_resume_recalls_context(project_root: Path) -> None:
    _resume_recalls_codeword(codex_spec, CODEX_AGENT, project_root)


@require_claude
def test_claude_repair_into_open_session(workdir: Path) -> None:
    """Round 1 writes only what the task asked; the validator demands a second
    file and supplies the repair message. The repair must land in the SAME
    session and fix the output without a fresh attempt."""
    repair = (
        f"Your output is incomplete. Also write exactly the word BETA into the file "
        f"{workdir}/beta.txt, then stop."
    )

    def validate(directory: Path):
        alpha = file_check("alpha.txt", "ALPHA")(directory)
        if not alpha.valid:
            return alpha
        return file_check("beta.txt", "BETA", repair=repair)(directory)

    report = run_attempt(
        claude_spec(repair_rounds=2),
        "Write exactly the word ALPHA into the file {{RUNNER_OUTPUT_PATH}}/alpha.txt, then stop. "
        "Do not create any other files.",
        workdir,
        agent=CLAUDE_AGENT,
        validate=validate,
        timeout_minutes=8,
    )
    assert report.outcome == outcomes.VALID, report.error
    assert report.repair_rounds_used >= 1, "repair never ran — outcome was reached some other way"


@require_codex
def test_codex_repair_into_open_session(workdir: Path) -> None:
    """Round 1 writes only what the task asked; the validator demands a second
    file and supplies the repair message. The repair must land in the SAME
    session and fix the output without a fresh attempt."""
    repair = (
        f"Your output is incomplete. Also write exactly the word BETA into the file "
        f"{workdir}/beta.txt, then stop."
    )

    def validate(directory: Path):
        alpha = file_check("alpha.txt", "ALPHA")(directory)
        if not alpha.valid:
            return alpha
        return file_check("beta.txt", "BETA", repair=repair)(directory)

    report = run_attempt(
        codex_spec(repair_rounds=2),
        "Write exactly the word ALPHA into the file {{RUNNER_OUTPUT_PATH}}/alpha.txt, then stop. "
        "Do not create any other files.",
        workdir,
        agent=CODEX_AGENT,
        validate=validate,
        timeout_minutes=8,
    )
    assert report.outcome == outcomes.VALID, report.error
    assert report.repair_rounds_used >= 1, "repair never ran — outcome was reached some other way"


@require_claude
def test_claude_timeout_reaps(workdir: Path) -> None:
    report = run_attempt(
        claude_spec(),
        "Write a very long, detailed 3000-word essay about the history of container "
        "shipping into {{RUNNER_OUTPUT_PATH}}/essay.txt.",
        workdir,
        agent=CLAUDE_AGENT,
        timeout_minutes=0.05,
    )
    assert report.outcome == outcomes.TIMEOUT, report.error
