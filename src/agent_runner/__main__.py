"""``python3 -m agent_runner`` — the packaged CLI entry point.

Hook processes reach it without a pip install: the attempt loop's agent
environment prepends the package's src dir to PYTHONPATH.
"""

from agent_runner.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
