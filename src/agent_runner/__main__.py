"""``python3 -m agent_runner`` — the packaged CLI entry point.

Agent shells and hook processes reach it without a pip install: the
engine's ``agent_env`` prepends the package's src dir to PYTHONPATH. The
bare ``python3`` those shells resolve may lack psycopg, so the process
entry re-execs onto RUNNER_PYTHON (also from ``agent_env``) first.
"""

from agent_runner.cli import main, reexec_with_driver

if __name__ == "__main__":
    reexec_with_driver()
    raise SystemExit(main())
