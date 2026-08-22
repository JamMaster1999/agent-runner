"""Codex stream dialect: ``codex exec --json`` JSONL -> StreamEvents.

Lands beside its adapter (extraction step 6, plan §1); pure line-in/
events-out so captured ``codex.stdout.jsonl`` files replay offline.

Usage contract: the TYPED StreamEvent fields (tok_*, cost_usd) are the
consumer API — event messages are display-only. ``turn.completed.usage``
is the thread's running total, not the turn's own spend (see the
adapter's ``usage_cumulative`` capability); the stream carries no dollars.
"""

from __future__ import annotations

from typing import Any

from agent_runner.harness.stream import (
    PROGRESS_LINE,
    StreamEvent,
    clip,
    parse_json_dict,
    progress_events,
    typed_token,
)


class CodexStreamParser:
    """Parse ``codex exec --json`` JSONL into StreamEvents.

    Item lifecycle events are deduplicated on ``(item_id, status)`` so a
    replayed or re-emitted item never produces duplicate progress rows.
    """

    def __init__(self) -> None:
        self._seen: set[tuple[str, str, str]] = set()

    def parse_line(self, line: str) -> list[StreamEvent]:
        payload = parse_json_dict(line)
        if payload is None:
            return []
        kind = payload.get("type") or ""

        if kind == "thread.started":
            return [StreamEvent("session_started", f"Codex thread started: {payload.get('thread_id')}")]
        if kind == "turn.started":
            return [StreamEvent("turn_started", "Codex turn started")]
        if kind == "turn.completed":
            usage = payload.get("usage") or {}
            return [
                StreamEvent(
                    "turn_completed",
                    "Codex turn completed",
                    tok_input=typed_token(usage.get("input_tokens")),
                    tok_cache_write=typed_token(usage.get("cache_write_input_tokens")),
                    tok_cache_read=typed_token(usage.get("cached_input_tokens")),
                    tok_output=typed_token(usage.get("output_tokens")),
                )
            ]
        if kind == "turn.failed":
            error = (payload.get("error") or {}).get("message") or "unknown error"
            return [StreamEvent("turn_failed", clip(f"Codex turn failed: {error}"))]
        if kind == "error":
            return [StreamEvent("stream_error", clip(f"Codex stream error: {payload.get('message')}"))]
        if kind not in {"item.started", "item.updated", "item.completed"}:
            return []

        item = payload.get("item") or {}
        item_id = str(item.get("id") or "")
        status = "started" if kind == "item.started" else str(item.get("status") or kind)
        dedupe_key = (item_id, str(item.get("type") or ""), status)
        if item_id and dedupe_key in self._seen:
            return []
        self._seen.add(dedupe_key)
        return self._item_events(kind, item)

    def _item_events(self, kind: str, item: dict[str, Any]) -> list[StreamEvent]:
        item_type = item.get("type") or ""

        if item_type == "agent_message" and kind == "item.completed":
            text = item.get("text") or ""
            events = progress_events(text, "codex")
            remainder = PROGRESS_LINE.sub("", text).strip()
            if remainder:
                events.append(StreamEvent("agent_message", clip(f"Codex: {remainder}")))
            return events

        if item_type == "command_execution":
            command = clip(item.get("command") or "", 160)
            if kind == "item.started":
                return [StreamEvent("command_started", f"Running: {command}")]
            if kind == "item.completed":
                exit_code = item.get("exit_code")
                duration = item.get("duration_ms")
                suffix = f" (exit {exit_code}" + (f", {duration} ms)" if duration is not None else ")")
                events = [StreamEvent("command_completed", f"Finished: {command}{suffix}")]
                events.extend(progress_events(item.get("aggregated_output") or "", "codex"))
                return events
            return []

        if item_type == "collab_tool_call":
            tool = item.get("tool") or "collab"
            agents_states = item.get("agents_states") or {}
            if tool == "spawn_agent":
                if kind == "item.started":
                    return [StreamEvent("subagent_spawning", clip(f"Spawning subagent: {item.get('prompt') or ''}"))]
                if kind == "item.completed":
                    receivers = ", ".join(item.get("receiver_thread_ids") or []) or "unknown thread"
                    return [StreamEvent("subagent_started", f"Subagent spawned: {receivers}")]
            if kind == "item.completed":
                parts = []
                for thread_id, state in agents_states.items():
                    state = state or {}
                    fragment = f"{thread_id[-8:]}: {state.get('status')}"
                    if state.get("message"):
                        fragment += f" — {state['message']}"
                    parts.append(fragment)
                detail = clip("; ".join(parts)) if parts else "no agent states"
                return [StreamEvent("subagent_update", f"Subagent {tool} completed: {detail}")]
            return []

        if item_type == "mcp_tool_call":
            label = f"{item.get('server')}.{item.get('tool')}"
            if kind == "item.started":
                return [StreamEvent("tool_started", f"Tool call: {label}")]
            if kind == "item.completed":
                return [StreamEvent("tool_completed", f"Tool finished: {label} ({item.get('status')})")]
            return []

        if item_type == "web_search" and kind == "item.completed":
            return [StreamEvent("web_search", clip(f"Web search: {item.get('query')}"))]

        if item_type == "file_change" and kind == "item.completed":
            changes = item.get("changes") or []
            paths = ", ".join(str(change.get("path")) for change in changes[:5])
            more = f" (+{len(changes) - 5} more)" if len(changes) > 5 else ""
            return [StreamEvent("file_change", clip(f"Files changed: {paths}{more}"))]

        if item_type == "error":
            return [StreamEvent("stream_error", clip(f"Codex error: {item.get('message')}"))]

        return []
