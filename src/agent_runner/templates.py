"""Submit-time prompt templates: the D2 closed variable set and substitution.

Prompt builders emit templates containing only these variables; the engine
substitutes them at attempt start. The resume fingerprint hashes the
PRE-substitution template, so run-varying values (run id, attempt, output
directory, CDP endpoint) are unrepresentable in a resume identity by
construction — no un-substitution ever happens.

Contract: ``substitute`` raises on a template variable it was not given a
value for, and on any ``{{...}}`` token still present after substitution
(malformed names, tokens smuggled in through substituted values).
"""

from __future__ import annotations

import re

# The closed variable set (design doc D2). Builders reference these constants
# so a typo'd token fails loudly at substitution time instead of reaching an
# agent as literal braces.
#
# RUNNER_ATTEMPT      — the attempt number (bare integer).
# RUNNER_RUN_ID       — the run id this attempt executes under (the lease
#                       ref). This is what fills run-identity slots in
#                       output metadata and the embedded `agent-runner emit`
#                       --run-id.
# RUNNER_JOB_KEY      — the job's own submit key. (Historically this token
#                       carried the RUN id; that aliasing is gone — each
#                       token substitutes exactly what its name promises.)
# RUNNER_OUTPUT_PATH  — the attempt's private output DIRECTORY; templates
#                       append their own artifact filenames (the filename is
#                       submit-time knowledge). One directory variable covers
#                       the primary output and every sibling artifact, which
#                       a single-file variable could not.
# RESOURCE:cdp_browser.* — attributes of the declared CDP browser resource
#                       kind (D4), for clients that register such a provider.
#                       Substitution values are JSON-encoded scalars, so one
#                       template renders a quoted string when the caller
#                       supplies a browser and null when the agent manages
#                       its own.
RUNNER_ATTEMPT = "{{RUNNER_ATTEMPT}}"
RUNNER_RUN_ID = "{{RUNNER_RUN_ID}}"
RUNNER_JOB_KEY = "{{RUNNER_JOB_KEY}}"
RUNNER_OUTPUT_PATH = "{{RUNNER_OUTPUT_PATH}}"
CDP_BROWSER_ENDPOINT = "{{RESOURCE:cdp_browser.endpoint}}"
CDP_BROWSER_WEBSOCKET_URL = "{{RESOURCE:cdp_browser.websocket_url}}"
CDP_BROWSER_PROFILE_DIR = "{{RESOURCE:cdp_browser.profile_dir}}"
CDP_BROWSER_LOG_PATH = "{{RESOURCE:cdp_browser.log_path}}"

# Tokens whose substitution values arrive already JSON-encoded (bare int,
# quoted string, or null). When one appears as a value inside a json.dumps'd
# template block it must be unquoted first — the substituted value's own JSON
# encoding decides the final shape.
JSON_VALUE_TOKENS = (
    RUNNER_ATTEMPT,
    CDP_BROWSER_ENDPOINT,
    CDP_BROWSER_WEBSOCKET_URL,
    CDP_BROWSER_PROFILE_DIR,
    CDP_BROWSER_LOG_PATH,
)

_TOKEN_RE = re.compile(r"\{\{([A-Za-z0-9_.:]+)\}\}")
_LEFTOVER_RE = re.compile(r"\{\{[^{}]*\}\}")


class TemplateError(ValueError):
    """A template referenced a variable it was not given a value for, or a
    {{...}} token survived substitution."""


def substitute(template: str, variables: dict[str, str]) -> str:
    """Render a prompt template with the given variable values.

    ``variables`` maps bare names (no braces) to substitution values, e.g.
    ``{"RUNNER_ATTEMPT": "1"}``. Values for extra variables the template does
    not use are allowed; a template variable with no value is an error."""

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            raise TemplateError(f"Unknown template variable {{{{{name}}}}}")
        return str(variables[name])

    rendered = _TOKEN_RE.sub(replace, template)
    leftover = _LEFTOVER_RE.search(rendered)
    if leftover:
        raise TemplateError(
            f"Unsubstituted template token remains after substitution: {leftover.group(0)!r}"
        )
    return rendered
