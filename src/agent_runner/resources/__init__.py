"""agent_runner.resources — provisioning for a run's declared resources.

A ``RunSpec`` may declare ``resource_specs`` (e.g. ``{"kind":
"cdp_browser"}``); the caller registers a provider per kind and hands the
mapping to ``run_attempt(resources=...)``. The attempt loop provisions
before spawn, substitutes each resource's values into the task's
``{{RESOURCE:*}}`` tokens, and closes everything after — projects that
declare nothing carry no browser dependencies and never import this
package.

Provider contract (duck-typed, no base class needed):

- ``provision(key, attempt, directory)`` returns a live resource with
  ``variables() -> dict[str, str]`` (JSON-encoded scalar values for the
  template tokens) and ``close()``
- ``null_variables() -> dict[str, str]`` supplies the same tokens as JSON
  ``null`` so one template renders with or without the resource
"""

from agent_runner.resources.cdp_browser import CdpBrowserProvider  # noqa: F401


def providers() -> dict[str, type]:
    """Every provider by resource kind — what a request's ``resources``
    names resolve to inside a sandbox."""
    return {CdpBrowserProvider.kind: CdpBrowserProvider}
