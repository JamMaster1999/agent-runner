"""Isolation: the environment an agent process is handed (agent_runner.md).

Production agents run in a clean home: no operator-personal skills,
settings, secrets, or files ever reach a production prompt. The engine's
own environment carries operator secrets (database DSNs, provider keys,
unrelated project variables) that agents with shell access could read — so
agents get a safe baseline plus exactly what the spec declared
(``required_env``), what the adapter needs for its CLI's auth
(``env_passthrough``), and any operator-listed extras
(RUNNER_AGENT_ENV_PASSTHROUGH, comma-separated names).
RUNNER_AGENT_ENV=inherit restores full-copy behavior for debugging.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from agent_runner import util

# Environment names an agent process may inherit from the engine regardless
# of harness: baseline shell/OS plumbing plus TLS/proxy configuration. Names
# holding secrets (DSNs, API keys) are NOT here — a spec that needs one
# declares it in required_env, an adapter that needs one lists it in
# env_passthrough().
AGENT_ENV_SAFE_NAMES = (
    "PATH",
    "HOME",
    "SHELL",
    "USER",
    "LOGNAME",
    "TERM",
    "TMPDIR",
    "TZ",
    "PYTHONPATH",
    # Runner state override: hook processes must write where the engine
    # reads, or all hook telemetry silently lands in the wrong tree.
    "RUNNER_STATE_DIR",
    # Container/sandbox marker some CLIs require to accept elevated
    # permission modes when running as root. A marker, not a secret.
    "IS_SANDBOX",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
)
AGENT_ENV_SAFE_PREFIXES = (
    "LANG",
    "LC_",
    "XDG_",
    "SSL_CERT",
    "REQUESTS_CA",
    "CURL_CA",
    "NODE_EXTRA_CA",
)


def agent_base_env(adapter, spec) -> dict[str, str]:
    """The inherited half of the agent environment (filtered by default)."""
    if os.environ.get("RUNNER_AGENT_ENV") == "inherit":
        return os.environ.copy()
    allowed = set(AGENT_ENV_SAFE_NAMES)
    allowed.update(spec.required_env)
    allowed.update(adapter.env_passthrough())
    extra = os.environ.get("RUNNER_AGENT_ENV_PASSTHROUGH", "")
    allowed.update(name.strip() for name in extra.split(",") if name.strip())
    return {
        name: value
        for name, value in os.environ.items()
        if name in allowed or name.startswith(AGENT_ENV_SAFE_PREFIXES)
    }


def agent_env(adapter, spec, run_id: str, attempt: int, workdir: Path) -> dict[str, str]:
    """The environment stamped onto agent CLI (and hook) processes.

    RUNNER_* is the attribution set the hook-capture scripts read; the
    PYTHONPATH prepend makes ``python3 -m agent_runner hook <provider>``
    resolve inside agent shells and hook processes without a pip install.
    """
    import agent_runner

    env = agent_base_env(adapter, spec)
    env.update(
        {
            "RUNNER_RUN_ID": run_id,
            "RUNNER_JOB_KEY": spec.key,
            "RUNNER_ATTEMPT": str(attempt),
            "RUNNER_OUTPUT_PATH": str(workdir),
            "RUNNER_AGENT_NAME": spec.agent_ref,
            "RUNNER_TASK_TYPE": spec.task_type,
            "RUNNER_BACKEND": spec.harness,
            "AGENT_RUNNER_PROJECT_ROOT": str(util.project_root()),
            "RUNNER_PYTHON": sys.executable,
        }
    )
    package_src = str(Path(agent_runner.__file__).resolve().parents[1])
    env["PYTHONPATH"] = package_src + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    return env
