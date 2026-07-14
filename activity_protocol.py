"""Shared, display-safe activity records for the private chat engines.

The PWA must be able to replay the work a model visibly performed (commands,
searches, file edits, MCP calls, and plans) without treating private model
reasoning as an activity.  Both Claude Code and Codex app-server emit this
small JSON-safe shape over the existing engine stream:

    {"id", "kind", "title", "status", "detail"?}

``detail`` is deliberately bounded and redacts common credentials.  It is
shown only in a user-opened PWA disclosure, but command/MCP outputs can still
contain environment secrets and must not be copied through unfiltered.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable, Optional


MAX_ACTIVITY_DETAIL_CHARS = 12_000
MAX_TITLE_TARGET_CHARS = 96
_SENSITIVE_KEY_RE = re.compile(
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|"
    r"password|passwd|secret|cookie|credential|private[_-]?key)",
    re.IGNORECASE,
)
_INLINE_SECRET_RE = re.compile(
    r"(?i)(\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|"
    r"password|passwd|secret|cookie|credential)\b\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_BEARER_RE = re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+")


def _truncate(text: str, limit: int) -> str:
    text = text or ""
    return text if len(text) <= limit else text[:limit] + "\n…（已截断）"


def _redact_text(text: str) -> str:
    text = _INLINE_SECRET_RE.sub(r"\1[已隐藏]", text or "")
    return _BEARER_RE.sub(r"\1[已隐藏]", text)


def _scrub(value: Any, *, depth: int = 0) -> Any:
    """Copy a JSON-ish value while removing obvious credential fields."""
    if depth > 8:
        return "[嵌套过深，已省略]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            out[key] = "[已隐藏]" if _SENSITIVE_KEY_RE.search(key) else _scrub(
                raw_value, depth=depth + 1
            )
        return out
    if isinstance(value, (list, tuple)):
        return [_scrub(v, depth=depth + 1) for v in value]
    if isinstance(value, str):
        return _redact_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _redact_text(str(value))


def _render(value: Any) -> str:
    cleaned = _scrub(value)
    if isinstance(cleaned, str):
        return cleaned
    try:
        return json.dumps(cleaned, ensure_ascii=False, indent=2, default=str)
    except (TypeError, ValueError):
        return _redact_text(str(cleaned))


def detail_from_fields(fields: Iterable[tuple[str, Any]]) -> Optional[str]:
    lines: list[str] = []
    for label, value in fields:
        if value is None or value == "" or value == [] or value == {}:
            continue
        lines.append(f"{label}:\n{_render(value)}")
    if not lines:
        return None
    return _truncate("\n\n".join(lines), MAX_ACTIVITY_DETAIL_CHARS)


def _target(value: Any) -> str:
    text = _redact_text(str(value or "")).strip().replace("\n", " ")
    return _truncate(text, MAX_TITLE_TARGET_CHARS).replace("\n…（已截断）", "…")


def _title(base: str, target: Any = None) -> str:
    target_text = _target(target)
    return f"{base} · {target_text}" if target_text else base


def _status(raw: Any, *, stage: str = "completed", errored: bool = False) -> str:
    if errored:
        return "failed"
    raw_text = str(raw or "").lower()
    if stage == "started" or raw_text in {"inprogress", "in_progress", "running", "pending"}:
        return "running"
    if any(word in raw_text for word in ("fail", "error", "denied", "cancel")):
        return "failed"
    return "completed"


def make_activity(
    *,
    activity_id: Any,
    kind: str,
    title: str,
    status: str = "completed",
    detail: Optional[str] = None,
) -> dict[str, str]:
    out = {
        "id": _target(activity_id) or f"{kind}:{_target(title)}",
        "kind": _target(kind) or "tool",
        "title": _truncate(_redact_text(title), 180),
        "status": status if status in {"running", "completed", "failed"} else "completed",
    }
    if detail:
        out["detail"] = _truncate(_redact_text(detail), MAX_ACTIVITY_DETAIL_CHARS)
    return out


def claude_activity(
    name: Any,
    arguments: Any = None,
    result: Any = None,
    *,
    activity_id: Any = None,
    stage: str = "completed",
    is_error: bool = False,
) -> dict[str, str]:
    """Build a bounded record from a Claude Code ``tool_use`` block."""
    tool = str(name or "tool")
    low = tool.lower()
    args = arguments if isinstance(arguments, dict) else {}
    if low == "bash":
        base, target, display_kind = "Bash", args.get("command"), "bash"
    elif low in {"read", "glob"}:
        base, target, display_kind = ("Read" if low == "read" else "Glob"), (
            args.get("file_path") or args.get("path") or args.get("pattern")
        ), low
    elif low in {"edit", "multiedit", "write", "notebookedit"}:
        base, target, display_kind = tool, args.get("file_path") or args.get("path"), low
    elif low == "grep":
        base, target, display_kind = "Search", args.get("pattern") or args.get("path"), "search"
    elif low == "websearch":
        base, target, display_kind = "Search", args.get("query"), "search"
    elif low == "webfetch":
        base, target, display_kind = "Fetch", args.get("url"), "fetch"
    elif low == "task":
        base, target, display_kind = "Task", args.get("description") or args.get("prompt"), "task"
    elif low == "todowrite":
        base, target, display_kind = "Todo", None, "todo"
    elif low.startswith("mcp__"):
        base, target, display_kind = "MCP", tool, "mcp"
    else:
        base, target, display_kind = tool, tool, low or "tool"
    return make_activity(
        activity_id=activity_id or f"claude:{tool}:{_target(target)}",
        kind=display_kind,
        title=_title(base, target),
        status=_status(None, stage=stage, errored=is_error),
        detail=detail_from_fields((("工具", tool), ("输入", args), ("结果", result))),
    )


def codex_activity(item: Any, *, stage: str = "completed") -> Optional[dict[str, str]]:
    """Build a display record from one public Codex app-server item."""
    if not isinstance(item, dict):
        return None
    kind = str(item.get("type") or "")
    if kind in {"", "userMessage", "agentMessage", "reasoning"}:
        return None
    if kind == "commandExecution":
        base, target, display_kind = "Bash", item.get("command"), "bash"
        fields = (("命令", item.get("command")), ("工作目录", item.get("cwd")),
                  ("退出码", item.get("exitCode")), ("耗时毫秒", item.get("durationMs")),
                  ("输出", item.get("aggregatedOutput")))
    elif kind in {"fileChange", "patchApply"}:
        base, target, display_kind = "Edit", item.get("path") or item.get("changes"), "edit"
        fields = (("变更", item.get("changes")), ("状态", item.get("status")))
    elif kind == "webSearch":
        base, target, display_kind = "Search", item.get("query"), "search"
        fields = (("查询", item.get("query")), ("动作", item.get("action")))
    elif kind == "mcpToolCall":
        base, target, display_kind = "MCP", "/".join(
            p for p in (str(item.get("server") or ""), str(item.get("tool") or "")) if p
        ), "mcp"
        fields = (("服务器", item.get("server")), ("工具", item.get("tool")),
                  ("输入", item.get("arguments")), ("结果", item.get("result")),
                  ("错误", item.get("error")), ("耗时毫秒", item.get("durationMs")))
    elif kind in {"toolCall", "dynamicToolCall"}:
        base, target, display_kind = str(item.get("tool") or item.get("name") or "Tool"), item.get("tool") or item.get("name"), "tool"
        fields = (("工具", item.get("tool") or item.get("name")),
                  ("输入", item.get("arguments")), ("结果", item.get("contentItems")),
                  ("成功", item.get("success")), ("耗时毫秒", item.get("durationMs")))
    elif kind == "collabAgentToolCall":
        base, target, display_kind = "Task", item.get("tool"), "task"
        fields = (("工具", item.get("tool")), ("任务", item.get("prompt")),
                  ("状态", item.get("agentsStates")))
    elif kind == "subAgentActivity":
        base, target, display_kind = "Task", item.get("kind"), "task"
        fields = (("活动", item.get("kind")), ("代理", item.get("agentPath")))
    elif kind == "plan":
        base, target, display_kind = "Plan", item.get("text"), "plan"
        fields = (("计划", item.get("text")),)
    elif kind == "imageView":
        base, target, display_kind = "Image", item.get("path"), "image"
        fields = (("路径", item.get("path")),)
    elif kind == "imageGeneration":
        base, target, display_kind = "Image", item.get("savedPath"), "image"
        fields = (("结果", item.get("result")), ("保存位置", item.get("savedPath")),
                  ("状态", item.get("status")))
    elif kind == "contextCompaction":
        base, target, fields, display_kind = "Compaction", None, (), "compaction"
    elif kind == "sleep":
        base, target, display_kind = "Sleep", item.get("durationMs"), "sleep"
        fields = (("等待毫秒", item.get("durationMs")),)
    else:
        base, target, display_kind = kind or "Tool", kind, kind or "tool"
        fields = (("内容", item),)
    return make_activity(
        activity_id=item.get("id") or f"codex:{kind}:{_target(target)}",
        kind=display_kind,
        title=_title(base, target),
        status=_status(item.get("status"), stage=stage, errored=bool(item.get("error"))),
        detail=detail_from_fields(fields),
    )


def codex_plan_activity(params: Any) -> Optional[dict[str, str]]:
    if not isinstance(params, dict):
        return None
    steps = params.get("plan")
    if not isinstance(steps, list):
        return None
    completed = sum(1 for step in steps if isinstance(step, dict) and step.get("status") == "completed")
    detail_steps = [
        {"status": step.get("status"), "step": step.get("step")}
        for step in steps if isinstance(step, dict)
    ]
    return make_activity(
        activity_id=f"plan:{params.get('turnId') or 'current'}",
        kind="plan",
        title=_title("Plan", f"{completed}/{len(detail_steps)}"),
        status="completed" if detail_steps and completed == len(detail_steps) else "running",
        detail=detail_from_fields((("说明", params.get("explanation")), ("步骤", detail_steps))),
    )


def merge_activities(existing: Iterable[Any], incoming: Iterable[Any]) -> list[dict[str, str]]:
    """Merge updates by id while preserving order and richer old/new details."""
    out: list[dict[str, str]] = []
    by_id: dict[str, int] = {}
    for raw in [*existing, *incoming]:
        if not isinstance(raw, dict):
            continue
        activity = {k: v for k, v in raw.items() if isinstance(v, str)}
        aid = activity.get("id")
        if not aid or not activity.get("title"):
            continue
        if aid not in by_id:
            by_id[aid] = len(out)
            out.append(activity)
            continue
        prev = out[by_id[aid]]
        merged = {**prev, **activity}
        if not activity.get("detail") and prev.get("detail"):
            merged["detail"] = prev["detail"]
        out[by_id[aid]] = merged
    return out
