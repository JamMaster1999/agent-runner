"""The ``cdp_browser`` resource: spawn Chrome, hand its endpoint in as a
template value (agent_runner.md).

Ported from the production CDP lifecycle: a headless Chrome with
``--remote-debugging-port=0``, the live endpoint read from the profile's
``DevToolsActivePort`` file, values delivered as JSON-encoded scalars so
one task template renders a quoted string when the runner supplies a
browser and ``null`` when the agent manages its own.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from agent_runner.runtime import RunnerError

VARIABLE_NAMES = (
    "cdp_browser.endpoint",
    "cdp_browser.websocket_url",
    "cdp_browser.profile_dir",
    "cdp_browser.log_path",
)


def cdp_variables(values: dict[str, str | None]) -> dict[str, str]:
    """The ``{{RESOURCE:cdp_browser.*}}`` substitution mapping: JSON-encoded
    scalars keyed by the bare token names."""
    return {
        f"RESOURCE:{name}": json.dumps(values.get(name))
        for name in VARIABLE_NAMES
    }


def find_browser_binary() -> Path | None:
    env_path = os.environ.get("CHROME_PATH")
    candidates = [
        env_path,
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("chrome"),
        shutil.which("msedge"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists():
            return path
    return None


def parse_devtools_active_port(text: str) -> tuple[str, str]:
    """(endpoint, websocket_url) from a DevToolsActivePort file's text: line
    one is the port, line two the browser websocket path."""
    lines = text.splitlines()
    port = lines[0].strip() if lines else ""
    ws_path = lines[1].strip() if len(lines) > 1 else ""
    if not port:
        raise ValueError("DevToolsActivePort carries no port yet")
    endpoint = f"http://127.0.0.1:{port}"
    websocket_url = f"ws://127.0.0.1:{port}{ws_path}" if ws_path else ""
    return endpoint, websocket_url


def wait_for_cdp_endpoint(profile_dir: Path, timeout_seconds: float) -> tuple[str, str]:
    active_port = profile_dir / "DevToolsActivePort"
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            return parse_devtools_active_port(active_port.read_text())
        except FileNotFoundError:
            last_error = f"{active_port} not created yet"
        except (OSError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RunnerError(
        f"Timed out waiting for the CDP browser endpoint. {last_error}",
        code="cdp_browser_start_failed",
        retryable=True,
    )


@dataclass
class CdpBrowser:
    """One live browser resource."""

    process: subprocess.Popen
    endpoint: str
    websocket_url: str
    profile_dir: Path
    log_path: Path

    def scratch(self) -> Path:
        """The folder this resource churns on its own (Chrome rewrites its
        profile constantly): never evidence that the agent is working."""
        return self.profile_dir

    def variables(self) -> dict[str, str]:
        return cdp_variables(
            {
                "cdp_browser.endpoint": self.endpoint,
                "cdp_browser.websocket_url": self.websocket_url,
                "cdp_browser.profile_dir": str(self.profile_dir),
                "cdp_browser.log_path": str(self.log_path),
            }
        )

    def close(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


class CdpBrowserProvider:
    """Provider for ``resource_specs`` kind ``cdp_browser``."""

    kind = "cdp_browser"

    def __init__(self, *, sandbox: bool | None = None, timeout_seconds: float = 30.0):
        # Containers running Chrome as root need --no-sandbox; default to
        # the RUNNER_CHROME_NO_SANDBOX environment switch.
        if sandbox is None:
            sandbox = not os.environ.get("RUNNER_CHROME_NO_SANDBOX")
        self.sandbox = sandbox
        self.timeout_seconds = timeout_seconds

    def null_variables(self) -> dict[str, str]:
        """Every token as JSON null — supplied even when nothing launches,
        so templates never see a leftover ``{{RESOURCE:*}}`` token."""
        return cdp_variables({})

    def provision(self, key: str, attempt: int, directory: Path) -> CdpBrowser:
        browser = find_browser_binary()
        if browser is None:
            raise RunnerError(
                "No Chrome/Chromium binary found for the cdp_browser "
                "resource. Set CHROME_PATH or drop the resource declaration.",
                code="missing_browser",
                retryable=False,
                alert=True,
            )
        profile_dir = Path(directory) / "cdp-profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        log_path = Path(directory) / "cdp-browser.log"
        command = [
            str(browser),
            "--headless=new",
            *([] if self.sandbox else ["--no-sandbox", "--disable-setuid-sandbox"]),
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--remote-debugging-address=127.0.0.1",
            "--remote-debugging-port=0",
            f"--user-data-dir={profile_dir}",
            "about:blank",
        ]
        with log_path.open("w") as log:
            process = subprocess.Popen(
                command, stdout=log, stderr=subprocess.STDOUT, text=True
            )
        try:
            endpoint, websocket_url = wait_for_cdp_endpoint(
                profile_dir, self.timeout_seconds
            )
        except BaseException:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise
        return CdpBrowser(
            process=process,
            endpoint=endpoint,
            websocket_url=websocket_url,
            profile_dir=profile_dir,
            log_path=log_path,
        )
