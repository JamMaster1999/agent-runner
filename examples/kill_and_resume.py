"""Kill the worker, keep the agent's memory.

Step 1 starts a real agent session. The agent learns something, and then this
process exits without saving any of the agent's work. Pretend the machine died.

Step 2 runs in a brand new process. It resumes the same session by its handle
and asks the agent to write down what it learned. The agent remembers, because
the conversation lives with the CLI, not with your dead process.

Run it:

    python examples/kill_and_resume.py learn
    python examples/kill_and_resume.py recall

Feel free to make step 1 more violent: start it and kill -9 the process while
it runs. As long as the session handle was captured, recall still works. In
production the handle rides a Temporal heartbeat, so a retry on a different
machine resumes the same conversation. That is the whole trick.
"""

import os
import sys
from pathlib import Path

from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec, Verdict

os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(Path.cwd()))

HANDLE_FILE = Path("runs/kill-demo/session_ref")
CODEWORD_TASK = (
    "Remember this codeword: TANGERINE-7. Reply with OK and stop. "
    "Do not write any files."
)
RECALL_TASK = (
    "Write the codeword from earlier in this conversation into the file "
    "{{RUNNER_OUTPUT_PATH}}/recalled.txt, then stop."
)

AGENT = AgentDef(
    name="rememberer",
    description="a demo agent with something to remember",
    config={"model": "haiku"},
    body="Do exactly what the task says, then stop.\n",
)


def learn() -> None:
    workdir = Path("runs/kill-demo/learn")
    workdir.mkdir(parents=True, exist_ok=True)

    # on_session fires the moment the CLI reveals its session handle, before
    # the run finishes. Persisting it immediately is what makes a crash at any
    # later moment survivable.
    def save_handle(ref: str) -> None:
        HANDLE_FILE.parent.mkdir(parents=True, exist_ok=True)
        HANDLE_FILE.write_text(ref)
        print(f"session handle captured: {ref}")

    report = run_attempt(
        RunSpec(key="kill-demo", harness="claude"),
        CODEWORD_TASK,
        workdir,
        agent=AGENT,
        on_session=save_handle,
        timeout_minutes=6,
    )
    print(f"agent learned the codeword ({report.outcome}). Now this process dies.")


def recall() -> None:
    ref = HANDLE_FILE.read_text().strip()
    workdir = Path("runs/kill-demo/recall")
    workdir.mkdir(parents=True, exist_ok=True)

    def check(directory: Path) -> Verdict:
        path = directory / "recalled.txt"
        if not path.is_file():
            return Verdict(valid=False, message="recalled.txt missing")
        return Verdict(valid=True, data={"text": path.read_text()})

    report = run_attempt(
        RunSpec(key="kill-demo", harness="claude"),
        RECALL_TASK,
        workdir,
        agent=AGENT,
        session_ref=ref,   # resume the dead process's conversation
        validate=check,
        timeout_minutes=6,
    )
    print(f"resumed: {report.resumed}, outcome: {report.outcome}")
    print(f"the new process never knew the codeword. The agent did: {report.data['text'].strip()}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("learn", "recall"):
        sys.exit("usage: python examples/kill_and_resume.py learn|recall")
    learn() if sys.argv[1] == "learn" else recall()
