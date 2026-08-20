# syntax=docker/dockerfile:1
# The agent-runner worker BASE image (ruling D2): pinned agent CLIs with
# provenance, Chrome, and the agent-runner package itself. Consumer
# projects extend it — a project's worker Dockerfile adds only its own
# code, config, and hooks; the two repos never install into each other's
# stack.
#
# Image hygiene, learned the hard way (thermos modal H1, 2026-08-06): what
# enters the image is an ALLOWLIST, never a blocklist. A dockerignore
# blocklist once leaked operator shell_snapshots and sqlite WAL/SHM
# sidecars into a production image while the base file was excluded. This
# Dockerfile therefore COPYs exactly three named paths (pyproject.toml,
# README.md, src/) — a state file invented tomorrow stays out by
# construction, and no operator dot-dir can ever ride along.
#
# The CLI versions are pinned like a dependency (thermos modal M3):
# unpinned, any rebuild — a toolchain edit, a cache miss — silently jumped
# the deployed agent runtime with no review and no way to reproduce the
# prior image. Bump the pins deliberately (edit + PR + smoke), never as a
# rebuild side effect. Provenance (versions, source commit, build date)
# lands in the OCI labels and /etc/agent-runner-provenance.json.

FROM python:3.13-slim-bookworm

# --- pinned toolchain (bump deliberately; see header) ---------------------
ARG CLAUDE_CODE_VERSION=2.1.220
ARG CODEX_CLI_VERSION=0.146.0
# --- build provenance (supplied by the publish workflow) ------------------
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="agent-runner base" \
      org.opencontainers.image.description="Worker base image: pinned agent CLIs, Chrome, and the agent-runner package." \
      org.opencontainers.image.source="https://github.com/JamMaster1999/agent-runner" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      io.agent-runner.claude-code-version="${CLAUDE_CODE_VERSION}" \
      io.agent-runner.codex-cli-version="${CODEX_CLI_VERSION}"

# System deps: Chrome for the cdp_browser resource, procps for the orphan
# sweep's pgrep, git + curl + certs as CLI baseline.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        chromium \
        procps \
        git \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Node 22 (the CLIs require >= 22; Debian ships older) + the pinned CLIs.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g \
        "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
        "@openai/codex@${CODEX_CLI_VERSION}" \
    && npm cache clean --force

# The package itself, with the Temporal activity wrapper and the S3 state
# mirror — workers are the audience of this image, and a worker is only
# fungible if it can fetch another host's sessions and checkpoints. COPY is
# the allowlist (see header): exactly these three paths, nothing else, no
# operator dot-dirs by construction.
COPY pyproject.toml /opt/agent-runner/pyproject.toml
COPY README.md /opt/agent-runner/README.md
COPY src /opt/agent-runner/src
RUN pip install --no-cache-dir "/opt/agent-runner[temporal,s3]"

# Machine-readable provenance inside the image.
RUN printf '{\n  "git_sha": "%s",\n  "build_date": "%s",\n  "claude_code_version": "%s",\n  "codex_cli_version": "%s",\n  "chromium": "%s",\n  "node": "%s"\n}\n' \
        "${GIT_SHA}" "${BUILD_DATE}" "${CLAUDE_CODE_VERSION}" "${CODEX_CLI_VERSION}" \
        "$(chromium --version 2>/dev/null | tr -d '\n')" \
        "$(node --version | tr -d '\n')" \
        > /etc/agent-runner-provenance.json \
    && cat /etc/agent-runner-provenance.json

# Containers run the CLIs as root: IS_SANDBOX lets claude accept elevated
# permission modes; RUNNER_CHROME_NO_SANDBOX makes the cdp_browser resource
# pass --no-sandbox (Chrome's sandbox cannot start as root in most
# container runtimes). CHROME_PATH points the resource at the apt binary.
ENV PYTHONUNBUFFERED=1 \
    IS_SANDBOX=1 \
    RUNNER_CHROME_NO_SANDBOX=1 \
    CHROME_PATH=/usr/bin/chromium
