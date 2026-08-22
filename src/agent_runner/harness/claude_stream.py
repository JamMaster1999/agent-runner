"""Claude Code stream dialect: ``claude --print --output-format stream-json``
JSONL -> StreamEvents.

Lands beside its adapter (extraction step 6, plan §1); pure line-in/
events-out so captured ``claude.stdout.log`` files replay offline.

Usage contract: the TYPED StreamEvent fields (tok_*, cost_usd) are the
consumer API — event messages are display-only. The ``result`` event's
``usage`` and ``total_cost_usd`` cover that invocation alone (a resumed
run reports its own spend, never the session's); ``usage`` counts the
main agent loop only, ``total_cost_usd`` includes subagents.
"""

from __future__ import annotations

from agent_runner.harness.stream import (
    PROGRESS_LINE,
    StreamEvent,
    clip,
    parse_json_dict,
    progress_events,
    typed_token,
)


class ClaudeStreamParser:
    """Parse ``claude --output-format stream-json`` JSONL into StreamEvents."""

    def __init__(self) -> None:
        # tool_result blocks carry only tool_use_id; remember names from the
        # originating tool_use blocks so completions can be labeled.
        self._tool_names: dict[str, str] = {}

    def parse_line(self, line: str) -> list[StreamEvent]:
        payload = parse_json_dict(line)
        if payload is None:
            return []
        kind = payload.get("type") or ""
        subagent = "[subagent] " if payload.get("parent_tool_use_id") else ""

        if kind == "system":
            subtype = payload.get("subtype") or ""
            if subtype == "init":
                return [
                    StreamEvent(
                        "session_started",
                        f"Claude session started: {payload.get('model')} ({payload.get('session_id')})",
                    )
                ]
            if subtype in {"task_started", "task_updated", "task_notification"}:
                detail = payload.get("description") or payload.get("status") or payload.get("task_id") or ""
                return [StreamEvent("subagent_update", clip(f"Claude task {subtype.split('_')[1]}: {detail}"))]
            return []

        if kind == "assistant":
            events: list[StreamEvent] = []
            content = (payload.get("message") or {}).get("content") or []
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    text = block.get("text") or ""
                    events.extend(progress_events(text, "claude"))
                    remainder = PROGRESS_LINE.sub("", text).strip()
                    if remainder:
                        events.append(StreamEvent("agent_message", clip(f"{subagent}Claude: {remainder}")))
                elif block_type == "tool_use":
                    name = block.get("name") or "tool"
                    tool_input = block.get("input") or {}
                    if block.get("id"):
                        self._tool_names[block["id"]] = name
                    if name == "Task":
                        events.append(
                            StreamEvent(
                                "subagent_started",
                                clip(f"Spawning subagent: {tool_input.get('subagent_type')} — {tool_input.get('description') or ''}"),
                            )
                        )
                    else:
                        summary = (
                            tool_input.get("command")
                            or tool_input.get("file_path")
                            or tool_input.get("query")
                            or tool_input.get("url")
                            or tool_input.get("prompt")
                            or ""
                        )
                        events.append(StreamEvent("tool_started", clip(f"{subagent}Tool call: {name} {summary}".rstrip())))
            return events

        if kind == "user":
            events = []
            content = (payload.get("message") or {}).get("content") or []
            for block in content:
                if not isinstance(block, dict) or block.get("type") != "tool_result":
                    continue
                text = ""
                inner = block.get("content")
                if isinstance(inner, str):
                    text = inner
                elif isinstance(inner, list):
                    text = " ".join(part.get("text") or "" for part in inner if isinstance(part, dict))
                events.extend(progress_events(text, "claude"))
                name = self._tool_names.pop(block.get("tool_use_id") or "", None) or "tool"
                if block.get("is_error"):
                    events.append(StreamEvent("tool_failed", clip(f"{subagent}Tool failed: {name} — {text or 'no output'}")))
                else:
                    events.append(StreamEvent("tool_completed", f"{subagent}Tool finished: {name}"))
            return events

        if kind == "result":
            subtype = payload.get("subtype") or "unknown"
            duration = payload.get("duration_ms")
            turns = payload.get("num_turns")
            event = "result_success" if subtype == "success" else "result_error"
            detail = f"Claude result: {subtype}"
            if duration is not None:
                detail += f" ({duration} ms, {turns} turns)"
            usage = payload.get("usage") or {}
            cost = payload.get("total_cost_usd")
            if subtype != "success" and payload.get("result"):
                detail += f" — {clip(str(payload.get('result')), 160)}"
            return [
                StreamEvent(
                    event,
                    detail,
                    tok_input=typed_token(usage.get("input_tokens")),
                    tok_cache_write=typed_token(usage.get("cache_creation_input_tokens")),
                    tok_cache_read=typed_token(usage.get("cache_read_input_tokens")),
                    tok_output=typed_token(usage.get("output_tokens")),
                    cost_usd=cost if isinstance(cost, (int, float)) and not isinstance(cost, bool) else None,
                )
            ]

        return []
