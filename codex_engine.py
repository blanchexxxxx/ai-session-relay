#!/usr/bin/env python3
"""
Codex 第二身体引擎核心(#346 · Phase B-core)。

管理一个**持久** `codex app-server --stdio` 子进程(NDJSON JSON-RPC),把一轮 Codex 对话
驱动成 Codex 专属的结构化事件流：正文 / commentary / 公开 thinking summary /
工具活动 / token usage 分通道输出。Codex thinking 和 token 字段不复用 CC wire
contract，上层按 `brain.target=="codex"` 显式路由。

协议锚点见同目录 `README.md`(0.144.3 实测契约)。会话续接靠 thread 落盘 + 指针文件
`state directory/codex_last_thread`:重启后 `thread/resume{threadId}`(不必同一 app-server 进程,
从 CODEX_HOME 磁盘加载);读不到或收到明确 thread 不存在错误 → `thread/start` 新 thread 并回写指针；
transport 静默只降级观察 rollout，绝不据此清指针。

shadow(Phase B):本模块**不接** relay backend dispatch。smoke:`python3 smoke_codex_engine.py`。
"""
from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import signal
import sys
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, Optional

_ENGINE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ENGINE_ROOT not in sys.path:
    sys.path.insert(0, _ENGINE_ROOT)

import activity_protocol

logger = logging.getLogger("codex_engine")

STATE_DIR = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_STATE_DIR", "~/.local/state/ai-session-relay")
))
os.makedirs(STATE_DIR, exist_ok=True)
CODEX_CWD = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_WORKSPACE", os.getcwd())
))
THREAD_PTR = os.environ.get(
    "CODEX_THREAD_PTR", os.path.join(STATE_DIR, "codex_last_thread")
)
CODEX_MODEL = os.environ.get("CODEX_MODEL", "")
TURN_TIMEOUT = float(os.environ.get("CODEX_TURN_TIMEOUT", "600"))
REQ_TIMEOUT = float(os.environ.get("CODEX_REQ_TIMEOUT", "60"))
RESUME_TIMEOUT = float(os.environ.get("CODEX_RESUME_TIMEOUT", "15"))
TRANSCRIPT_POLL_SECONDS = float(os.environ.get("CODEX_TRANSCRIPT_POLL_SECONDS", "1"))
ROLLOUT_START_TIMEOUT = float(os.environ.get("CODEX_ROLLOUT_START_TIMEOUT", "15"))
HEARTBEAT_SECONDS = float(os.environ.get("CODEX_HEARTBEAT_SECONDS", "5"))
CODEX_SESSIONS_DIR = os.environ.get(
    "CODEX_SESSIONS_DIR", os.path.expanduser("~/.codex/sessions")
)
_CONTINUATION_TRIGGER_RATIO = 0.75
_CONTINUATION_HEADROOM_TOKENS = 60_000
_CONTINUATION_TRIGGER_CAP = 250_000
_ROLLOUT_PATH_CACHE: dict[str, str] = {}
_ENGINE_RECOVERY_RETRIES = 1
_USE_POSIX_PROCESS_GROUP = os.name == "posix"
_POSIX_KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)


class _TurnPhase(str, Enum):
    SENT_UNCONFIRMED = "sent_unconfirmed"
    ACKNOWLEDGED = "acknowledged"
    STARTED = "started"
    COMPLETED = "completed"


@dataclass(frozen=True)
class _RolloutTurnState:
    progressed: bool = False
    turn_id: Optional[str] = None
    completed: bool = False
    full: str = ""
    commentary_full: str = ""
    codex_turn_usage: Optional[dict] = None


@dataclass(frozen=True)
class _RolloutContinuationState:
    last_completed_usage: Optional[dict] = None
    thread_compacted: bool = False
    latest_turn_completed: bool = False


class _TurnStartUnconfirmed(ConnectionError):
    def __init__(self, path: Optional[str], offset: int) -> None:
        super().__init__("turn/start produced no RPC ack or rollout progress")
        self.rollout_path = path
        self.rollout_offset = offset


def _app_server_process_kwargs() -> dict:
    if _USE_POSIX_PROCESS_GROUP:
        return {"start_new_session": True}
    return {}


def _kill_app_server_process(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    if _USE_POSIX_PROCESS_GROUP:
        try:
            os.killpg(proc.pid, _POSIX_KILL_SIGNAL)
            return
        except ProcessLookupError:
            return
        except OSError:
            logger.warning(
                "codex app-server process-group kill failed; falling back to launcher",
                exc_info=True,
            )
    proc.kill()

# 只请求**可展示的推理摘要**，不是原始私有推理。0.144.3 的 prolite 实测表明，必须在
# thread start/resume 一并声明模型支持摘要；只在 turn/start 传 ``summary: auto`` 不会产生
# reasoning item。每处取新 dict，避免 JSON-RPC 调用方意外修改共享常量。
_REASONING_SUMMARY_MODE = "detailed"
_CLIENT_INFO = {"name": "ai-session-relay", "version": "0.1.0"}


def _reasoning_summary_config() -> dict:
    """app-server 的 thread 配置：强制请求公开 reasoning summary。"""
    return {
        "model_reasoning_summary": _REASONING_SUMMARY_MODE,
        "model_supports_reasoning_summaries": True,
    }


def _initialize_params() -> dict:
    """启用 app-server 的实验性摘要事件面；只使用公开协议字段。"""
    return {
        "clientInfo": dict(_CLIENT_INFO),
        "capabilities": {"experimentalApi": True},
    }

# item/completed 里这些类型仍透给旧的紧凑「工具活动」提示；完整细节改走
# activity_protocol 的结构化 ``activity`` 事件，PWA 可实时/历史同样回放。
_TOOL_ITEM_TYPES = {
    "commandExecution", "fileChange", "mcpToolCall", "toolCall",
    "webSearch", "patchApply", "localShellCall",
}


def _find_rollout_path(thread_id: str) -> Optional[str]:
    """Locate Codex's persisted rollout for one thread."""
    if not thread_id:
        return None
    cached = _ROLLOUT_PATH_CACHE.get(thread_id)
    if cached and os.path.isfile(cached):
        return cached
    matches = glob.glob(
        os.path.join(CODEX_SESSIONS_DIR, "**", f"*{thread_id}*.jsonl"),
        recursive=True,
    )
    if not matches:
        return None
    path = max(matches, key=os.path.getmtime)
    _ROLLOUT_PATH_CACHE[thread_id] = path
    return path


def _rollout_cursor(thread_id: str) -> tuple[Optional[str], int]:
    """Snapshot the rollout tail immediately before ``turn/start``."""
    path = _find_rollout_path(thread_id)
    if not path:
        return None, 0
    try:
        return path, os.path.getsize(path)
    except OSError:
        return None, 0


def _rollout_has_progress(path: Optional[str], offset: int) -> bool:
    if not path:
        return False
    try:
        return os.path.getsize(path) > offset
    except OSError:
        return False


def _turn_state_from_rollout(
    path: Optional[str], offset: int,
) -> _RolloutTurnState:
    """Read one serialized turn after a pre-send cursor from durable rollout."""
    if not path:
        return _RolloutTurnState()
    commentary: list[str] = []
    final_text: list[str] = []
    usage: Optional[dict] = None
    progressed = False
    turn_id: Optional[str] = None
    completed = False
    try:
        with open(path, "rb") as stream:
            stream.seek(max(0, offset))
            for raw_line in stream:
                progressed = True
                try:
                    obj = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                payload = obj.get("payload") or {}
                if obj.get("type") == "event_msg":
                    event_type = payload.get("type")
                    event_turn_id = payload.get("turn_id")
                    if event_type == "task_started":
                        if turn_id is None and isinstance(event_turn_id, str):
                            turn_id = event_turn_id
                    elif event_type == "token_count" and isinstance(payload.get("info"), dict):
                        usage = _map_rollout_usage(payload["info"])
                    elif event_type == "task_complete":
                        if turn_id is None and isinstance(event_turn_id, str):
                            turn_id = event_turn_id
                        if not event_turn_id or event_turn_id == turn_id:
                            completed = True
                            break
                    continue
                if obj.get("type") != "response_item":
                    continue
                if payload.get("type") == "reasoning":
                    continue
                if payload.get("type") != "message" or payload.get("role") != "assistant":
                    continue
                text = "".join(
                    item.get("text") or ""
                    for item in (payload.get("content") or [])
                    if isinstance(item, dict) and item.get("type") == "output_text"
                )
                if not text:
                    continue
                if payload.get("phase") == "commentary":
                    commentary.append(text)
                else:
                    final_text.append(text)
    except OSError:
        return _RolloutTurnState()
    return _RolloutTurnState(
        progressed=progressed,
        turn_id=turn_id,
        completed=completed,
        full="".join(final_text),
        commentary_full="".join(commentary),
        codex_turn_usage=usage,
    )


def _completed_turn_from_rollout(
    path: Optional[str], offset: int,
) -> Optional[dict[str, Any]]:
    """Recover only a completed turn's public commentary, answer and usage."""
    state = _turn_state_from_rollout(path, offset)
    if not state.completed or not state.full:
        return None
    recovered = {
        "full": state.full,
        "commentary_full": state.commentary_full,
        "codex_turn_usage": state.codex_turn_usage,
    }
    if state.turn_id:
        recovered["turn_id"] = state.turn_id
    return recovered


def _rollout_continuation_state(thread_id: str) -> _RolloutContinuationState:
    """Return completed usage plus any native-compaction evidence for a thread."""
    path = _find_rollout_path(thread_id)
    if not path:
        return _RolloutContinuationState()
    completed_usage: Optional[dict] = None
    active_usage: Optional[dict] = None
    thread_compacted = False
    active_started = False
    latest_turn_completed = False
    try:
        with open(path, "rb") as f:
            for raw_line in f:
                try:
                    obj = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                payload = obj.get("payload") or {}
                if obj.get("type") == "event_msg":
                    event_type = payload.get("type")
                    if event_type == "task_started":
                        active_started = True
                        active_usage = None
                        latest_turn_completed = False
                    elif event_type == "token_count" and active_started:
                        info = payload.get("info")
                        if isinstance(info, dict):
                            active_usage = _map_rollout_usage(info)
                    elif event_type == "task_complete" and active_started:
                        if active_usage is not None:
                            completed_usage = active_usage
                        latest_turn_completed = True
                    continue
                if obj.get("type") == "compacted":
                    thread_compacted = True
    except OSError:
        return _RolloutContinuationState()
    return _RolloutContinuationState(
        last_completed_usage=completed_usage,
        thread_compacted=thread_compacted,
        latest_turn_completed=latest_turn_completed,
    )


def _continuation_trigger_tokens(model_context_window: int) -> int:
    window = max(1, int(model_context_window))
    return max(1, min(
        int(window * _CONTINUATION_TRIGGER_RATIO),
        window - _CONTINUATION_HEADROOM_TOKENS,
        _CONTINUATION_TRIGGER_CAP,
    ))


def _continuation_rotation_reason(
    state: _RolloutContinuationState,
) -> Optional[str]:
    if state.thread_compacted:
        return "native_compact"
    usage = state.last_completed_usage or {}
    window = usage.get("model_context_window")
    total_tokens = usage.get("total_tokens")
    if not isinstance(window, (int, float)) or window <= 0:
        return None
    if not isinstance(total_tokens, (int, float)) or total_tokens <= 0:
        return None
    trigger = _continuation_trigger_tokens(int(window))
    if int(total_tokens) >= trigger:
        return f"context_pressure:{int(total_tokens)}>={trigger}/{int(window)}"
    return None


def _notification_turn_id(obj: dict) -> Optional[str]:
    params = obj.get("params") or {}
    turn_id = params.get("turnId")
    if isinstance(turn_id, str):
        return turn_id
    turn = params.get("turn") or {}
    value = turn.get("id") if isinstance(turn, dict) else None
    return value if isinstance(value, str) else None


def _read_thread_ptr() -> Optional[str]:
    try:
        with open(THREAD_PTR, "r", encoding="utf-8") as f:
            v = f.read().strip()
            return v or None
    except FileNotFoundError:
        return None
    except Exception:  # noqa: BLE001
        logger.exception("read thread ptr failed")
        return None


def _write_thread_ptr(thread_id: str) -> bool:
    try:
        os.makedirs(os.path.dirname(THREAD_PTR), exist_ok=True)
        tmp = THREAD_PTR + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(thread_id)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, THREAD_PTR)
        return True
    except Exception:  # noqa: BLE001
        logger.exception("write thread ptr failed")
        return False


def _clear_thread_ptr(expected: Optional[str] = None) -> bool:
    """Delete a known-bad persisted thread pointer without racing a newer writer."""
    try:
        if expected is not None:
            with open(THREAD_PTR, "r", encoding="utf-8") as f:
                if f.read().strip() != expected:
                    logger.warning("skip clearing codex thread ptr; it changed meanwhile")
                    return False
        os.remove(THREAD_PTR)
        logger.warning("cleared unusable codex thread ptr: %s", expected or "(unspecified)")
        return True
    except FileNotFoundError:
        return False
    except Exception:  # noqa: BLE001
        logger.exception("clear thread ptr failed")
        return False


def _explicit_thread_unavailable(exc: BaseException) -> bool:
    """Only explicit semantic thread failures may delete the durable pointer."""
    message = str(exc).lower()
    if "thread" not in message:
        return False
    return any(marker in message for marker in (
        "not found",
        "does not exist",
        "unknown thread",
        "invalid thread",
        "thread unavailable",
        "failed to load thread",
        "corrupt",
    ))


# 当前 thread 是为哪个 brain epoch 建的(切换感知 · 无缝切 session)。
THREAD_EPOCH_PTR = os.path.join(STATE_DIR, "codex_thread_epoch")


def _read_thread_epoch() -> int:
    """当前 thread 的 epoch。无 / 读不到 = -1(任何真 epoch 都比它新 → 首次切换起干净 thread)。"""
    try:
        with open(THREAD_EPOCH_PTR, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:  # noqa: BLE001
        return -1


def _write_thread_epoch(epoch: int) -> None:
    try:
        os.makedirs(os.path.dirname(THREAD_EPOCH_PTR), exist_ok=True)
        tmp = THREAD_EPOCH_PTR + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(str(int(epoch)))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, THREAD_EPOCH_PTR)
    except Exception:  # noqa: BLE001
        logger.exception("write thread epoch failed")


def _tool_label(item: dict) -> str:
    """把工具 item 压成一句友好小字(顶端活动条用)。"""
    t = item.get("type") or "tool"
    for k in ("command", "name", "title", "path", "query"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return f"{t}: {v.strip()[:60]}"
    return t


def _map_usage(tu: Optional[dict]) -> dict:
    """直译 app-server ``thread/tokenUsage/updated.tokenUsage`` 的 Codex 语义。

    tokenUsage 实测形状(0.144.3):`{total:{…累计整个 thread…}, last:{…本轮…}, modelContextWindow}`。
    **必须取 `last`(本轮)**——取 `total` 会把整段 thread 的累计 token 当成「这一轮」显示,
    随 thread 越滚越大。`last` 缺则仅为兼容旧输入形状回落 `total`。

    返回值保留 app-server 原生口径；不制造 CC 才有的 cache creation/write
    字段，也不把 cached input 改名成 CC cache-read contract。
    """
    tu = tu or {}
    u = tu.get("last") or tu.get("total") or {}
    inp = int(u.get("inputTokens") or 0)
    cached = int(u.get("cachedInputTokens") or 0)
    out = int(u.get("outputTokens") or 0)
    reasoning_out = int(u.get("reasoningOutputTokens") or 0)
    context_window = tu.get("modelContextWindow")
    return {
        "input_tokens": inp,
        "cached_input_tokens": cached,
        "output_tokens": out,
        "reasoning_output_tokens": reasoning_out,
        "total_tokens": int(u.get("totalTokens") or (inp + out)),
        "model_context_window": (
            int(context_window) if isinstance(context_window, (int, float)) else None
        ),
    }


def _map_rollout_usage(info: Optional[dict]) -> dict:
    """Map persisted snake_case last-turn usage to the live contract."""
    info = info or {}
    usage = info.get("last_token_usage") or info.get("total_token_usage") or {}
    return {
        "input_tokens": int(usage.get("input_tokens") or 0),
        "cached_input_tokens": int(usage.get("cached_input_tokens") or 0),
        "output_tokens": int(usage.get("output_tokens") or 0),
        "reasoning_output_tokens": int(usage.get("reasoning_output_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
        "model_context_window": (
            int(info["model_context_window"])
            if isinstance(info.get("model_context_window"), (int, float))
            else None
        ),
    }


class CodexAppServer:
    """持久 codex app-server(--stdio · NDJSON JSON-RPC)· 单连接 · 一次一轮(锁)。"""

    def __init__(self) -> None:
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._reqid = 0
        self._pending: dict[int, "asyncio.Future"] = {}
        self._notif_q: Optional["asyncio.Queue"] = None   # 非 None 时把通知路由给当前轮
        self._reader: Optional[asyncio.Task] = None
        self._stderr: Optional[asyncio.Task] = None
        self._turn_lock = asyncio.Lock()
        self._served_model = CODEX_MODEL or "default"
        self._rate_limited = False
        self._turn_request_sent = False
        self._transport_desynced = False
        self._resume_unconfirmed = False
        self._last_turn_id: Optional[str] = None
        self._thread_id: Optional[str] = None   # 一个 engine 生命期一个活跃 thread(首轮 resolve)

    # ── 生命周期 ──────────────────────────────────────────────────────────
    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            "codex", "app-server", "--stdio",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **_app_server_process_kwargs(),
        )
        self._reader = asyncio.create_task(self._read_loop())
        self._stderr = asyncio.create_task(self._drain_stderr())
        try:
            await self._request("initialize", _initialize_params())
        except Exception:  # noqa: BLE001 · 新 CLI 不认 experimentalApi 时仍能正常聊天
            logger.warning("codex initialize experimentalApi 被拒 → 回落稳定握手", exc_info=True)
            await self._request("initialize", {"clientInfo": dict(_CLIENT_INFO)})
        await self._notify("initialized", None)
        logger.info("codex app-server initialized")

    async def close(self) -> None:
        """Make this app-server instance unusable and wake every waiting request."""
        self._fail_pending("codex app-server closed")
        self._notif_q = None
        self._thread_id = None

        current = asyncio.current_task()
        tasks = [
            task for task in (self._reader, self._stderr)
            if task is not None and task is not current
        ]
        self._reader = None
        self._stderr = None
        for task in tasks:
            task.cancel()

        proc = self.proc
        self.proc = None
        if proc:
            try:
                if proc.returncode is None:
                    _kill_app_server_process(proc)
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("codex app-server did not exit after kill")
            except Exception:  # noqa: BLE001
                logger.debug("codex app-server close failed", exc_info=True)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── 底层 JSON-RPC(NDJSON)──────────────────────────────────────────────
    async def _send(self, obj: dict) -> None:
        assert self.proc and self.proc.stdin
        self.proc.stdin.write((json.dumps(obj) + "\n").encode())
        await self.proc.stdin.drain()

    async def _notify(self, method: str, params: Optional[dict]) -> None:
        msg: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        await self._send(msg)

    async def _begin_request(
        self, method: str, params: dict,
    ) -> tuple[int, "asyncio.Future"]:
        """Write one RPC request and return its response future without waiting."""
        self._reqid += 1
        rid = self._reqid
        fut: "asyncio.Future" = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut
        try:
            await self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        except BaseException:
            self._pending.pop(rid, None)
            if not fut.done():
                fut.cancel()
            raise
        return rid, fut

    @staticmethod
    def _unwrap_response(method: str, response: dict) -> dict:
        if "error" in response:
            raise RuntimeError(f"{method} error: {response['error']}")
        return response.get("result") or {}

    async def _request(
        self, method: str, params: dict, *, timeout: Optional[float] = None,
    ) -> dict:
        rid, fut = await self._begin_request(method, params)
        try:
            response = await asyncio.wait_for(
                fut, timeout=REQ_TIMEOUT if timeout is None else timeout,
            )
        finally:
            self._pending.pop(rid, None)
        return self._unwrap_response(method, response)

    def _fail_pending(self, reason: str) -> None:
        """Unblock RPC callers when the stdio connection cannot make progress."""
        pending = list(self._pending.values())
        self._pending.clear()
        for fut in pending:
            if not fut.done():
                fut.set_exception(ConnectionError(reason))

    async def _read_loop(self) -> None:
        proc = self.proc
        assert proc and proc.stdout
        while True:
            line = await proc.stdout.readline()
            if not line:
                logger.warning("codex app-server stdout EOF")
                self._fail_pending("codex app-server stdout closed")
                return
            s = line.decode(errors="replace").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:  # noqa: BLE001
                continue
            if "id" in obj and ("result" in obj or "error" in obj):
                fut = self._pending.pop(obj["id"], None)
                if fut and not fut.done():
                    fut.set_result(obj)
            elif "id" in obj and obj.get("method"):
                # server→client **请求**(带 id · 多为工具/命令审批)· agent自主 →
                # 一律 approved(bypassPermissions 等价)· 否则工具调用挂死/被 cancel。
                asyncio.create_task(self._auto_approve(obj))
            elif obj.get("method"):
                if self._notif_q is not None:
                    self._notif_q.put_nowait(obj)

    async def _drain_stderr(self) -> None:
        proc = self.proc
        assert proc and proc.stderr
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            logger.debug("[codex stderr] %s", line.decode(errors="replace").rstrip()[:200])

    async def _auto_approve(self, req: dict) -> None:
        """server→client 审批请求一律放行(agent自主 · bypassPermissions 等价)。

        两种响应形状,按 method 路由(实测 · 见 README):
          · `mcpServer/elicitation/request`(MCP 工具调用审批 · mode=form)→ 元素协议 elicitation ·
            响应 `{"action":"accept","content":{}}`(**给错成 {decision} 会被当拒绝** · 踩过)。
          · Exec/ApplyPatch/命令 审批(ReviewDecision)→ `{"decision":"approved"}`。
        未知请求给 elicitation-accept 兜底(比 decision 更泛用)。"""
        method = req.get("method") or ""
        ml = method.lower()
        if any(k in ml for k in ("exec", "command", "patch", "filechange")):
            # Exec/ApplyPatch/命令/文件改 审批 → ReviewDecision
            result: dict = {"decision": "approved"}
        else:
            # MCP 工具审批(mcpServer/elicitation/request · mode=form)+ 其它 → elicitation accept
            result = {"action": "accept", "content": {}}
        try:
            await self._send({"jsonrpc": "2.0", "id": req["id"], "result": result})
            logger.info("codex auto-approved server request: %s → %s", method, result)
        except Exception:  # noqa: BLE001
            logger.exception("auto-approve failed: %s", method)

    async def _inject_handoff(
        self, thread_id: str, handoff: Optional[str], *, required: bool = False,
    ) -> bool:
        if not handoff:
            return False
        try:
            await self._request("thread/inject_items", {
                "threadId": thread_id,
                "items": [{
                    "type": "message", "role": "developer",
                    "content": [{"type": "input_text", "text": handoff}],
                }],
            })
            logger.info("codex handoff injected (%d chars) → %s", len(handoff), thread_id)
            return True
        except Exception:  # noqa: BLE001
            if required:
                raise
            logger.exception("handoff inject failed; fresh thread remains usable")
            return False

    # ── thread 续接 / 空窗口接班 ──────────────────────────────────────────
    async def _start_fresh_thread(self, *, persist: bool = True) -> str:
        """thread/start 一个干净新 thread(空窗口)· 写指针 · 返回 id。

        approvalPolicy=never:agent自主用工具不弹审批 = Claude bypassPermissions 等价;
        仍来审批请求时 _auto_approve 兜底放行。
        """
        start_params = {
            "cwd": CODEX_CWD, "serviceName": "ai-session-relay",
            "sessionStartSource": "startup",
            "approvalPolicy": "never",
            "config": _reasoning_summary_config(),
        }
        try:
            res = await self._request("thread/start", start_params)
        except Exception:  # noqa: BLE001 · 摘要配置 rot 时不挡agent起新会话
            logger.warning("codex thread/start 摘要配置被拒 → 无摘要回落", exc_info=True)
            start_params.pop("config", None)
            res = await self._request("thread/start", start_params)
        tid = (res.get("thread") or {}).get("id")
        if not tid:
            raise RuntimeError("thread/start 没拿到 thread.id")
        if persist and not _write_thread_ptr(tid):
            raise OSError("failed to persist fresh codex thread pointer")
        return tid

    async def _rotate_for_continuation(
        self, old_thread_id: str, handoff: Optional[str], reason: str,
    ) -> str:
        if not handoff:
            logger.warning(
                "codex continuation rotation deferred(no handoff): thread=%s reason=%s",
                old_thread_id, reason,
            )
            return old_thread_id
        try:
            new_thread_id = await self._start_fresh_thread(persist=False)
            await self._inject_handoff(new_thread_id, handoff, required=True)
            if not _write_thread_ptr(new_thread_id):
                raise OSError("failed to commit codex continuation pointer")
            logger.warning(
                "codex continuation rotated: old=%s new=%s reason=%s",
                old_thread_id, new_thread_id, reason,
            )
            return new_thread_id
        except Exception:  # noqa: BLE001
            logger.exception(
                "codex continuation rotation failed; keeping old thread=%s reason=%s",
                old_thread_id, reason,
            )
            return old_thread_id

    async def _maybe_rotate_for_continuation(
        self, thread_id: str, handoff: Optional[str],
    ) -> str:
        state = await asyncio.to_thread(_rollout_continuation_state, thread_id)
        reason = _continuation_rotation_reason(state)
        if not reason:
            return thread_id
        return await self._rotate_for_continuation(thread_id, handoff, reason)

    async def _resolve_thread(self, handoff: Optional[str] = None) -> str:
        """重启 / 冷起(非切换)→ resume 指针里的当前 thread；仅明确失效才起新。

        resume 成功或仅 ack 静默都沿用旧 thread；只有明确 missing/corrupt 才清坏指针。
        起新 thread(冷启无历史):直接写入指针，后续由调用方按需注入 handoff。
        """
        ptr = _read_thread_ptr()
        if ptr:
            try:
                # 老 thread 也要覆写这两个设置；不能为了拿摘要起新 thread 而丢掉上下文。
                await self._request("thread/resume", {
                    "threadId": ptr,
                    "config": _reasoning_summary_config(),
                }, timeout=RESUME_TIMEOUT)
                logger.info("codex thread resumed: %s", ptr)
                return ptr
            except RuntimeError as exc:
                if _explicit_thread_unavailable(exc):
                    _clear_thread_ptr(ptr)
                    self._thread_id = None
                    raise RuntimeError(
                        "codex thread explicitly unavailable; app-server recycle required"
                    ) from exc
                # 配置被未来 CLI 拒绝不等于 thread 失效；先用裸 resume 保住上下文。
                logger.warning("thread/resume 摘要配置被拒(%s)→ 无摘要重试", ptr, exc_info=True)
                try:
                    await self._request(
                        "thread/resume", {"threadId": ptr}, timeout=RESUME_TIMEOUT,
                    )
                    logger.warning("codex thread 已无摘要续接: %s", ptr)
                    return ptr
                except RuntimeError as bare_exc:
                    if _explicit_thread_unavailable(bare_exc):
                        _clear_thread_ptr(ptr)
                        self._thread_id = None
                        raise RuntimeError(
                            "codex thread explicitly unavailable; app-server recycle required"
                        ) from bare_exc
                    logger.warning(
                        "thread/resume failed without proof of thread loss(%s); preserving pointer",
                        ptr, exc_info=True,
                    )
                    raise
                except (asyncio.TimeoutError, ConnectionError, OSError):
                    logger.warning(
                        "bare thread/resume transport silent(%s); preserving pointer",
                        ptr, exc_info=True,
                    )
                    self._resume_unconfirmed = True
                    return ptr
            except (asyncio.TimeoutError, ConnectionError, OSError):
                logger.warning(
                    "thread/resume transport silent(%s); preserving pointer and observing rollout",
                    ptr, exc_info=True,
                )
                self._resume_unconfirmed = True
                return ptr
        tid = await self._start_fresh_thread()
        await self._inject_handoff(tid, handoff)
        logger.info("codex thread started (no-switch): %s", tid)
        return tid

    async def _new_session(self, handoff: Optional[str] = None) -> str:
        """空窗口接班(切到 Codex 的新任期):起**干净新 thread** + 注入 handoff(最近对话)。

        PDF §6.2:thread/start(**不 resume 旧 thread** · 避免串上一任期噪声)→ thread/inject_items
        把 handoff 作为 developer context 追加进模型可见历史(**不启 turn** · 切换瞬间不乱回复)。
        下一句真消息才 turn/start —— 那时agent已"知道刚才发生了什么",无缝接上。
        """
        tid = await self._start_fresh_thread()
        await self._inject_handoff(tid, handoff)
        logger.info("codex NEW session (switch): %s", tid)
        return tid

    # ── 一轮 → cc-event 流 ────────────────────────────────────────────────
    async def stream_turn(
        self, text: str, *, model: Optional[str] = None, effort: Optional[str] = None,
        epoch: Optional[int] = None, handoff: Optional[str] = None,
        client_message_id: Optional[str] = None,
    ) -> AsyncIterator[dict]:
        """驱动一轮 Codex · yield Codex 专属 thinking/usage 契约。

        epoch/handoff(无缝切 session):epoch 比当前 thread 的 epoch **新** = 切到 Codex 的
        新任期 → 空窗口接班(干净新 thread + 注入 handoff);否则续用当前 thread /(冷起)resume。
        """
        async with self._turn_lock:
            if epoch is not None and epoch > _read_thread_epoch():
                self._thread_id = await self._new_session(handoff)
                _write_thread_epoch(epoch)
            elif self._thread_id is None:
                persisted = _read_thread_ptr()
                if persisted:
                    candidate = await self._maybe_rotate_for_continuation(
                        persisted, handoff,
                    )
                    if candidate != persisted:
                        self._thread_id = candidate
                    else:
                        self._thread_id = await self._resolve_thread(handoff)
                else:
                    self._thread_id = await self._resolve_thread(handoff)
            else:
                self._thread_id = await self._maybe_rotate_for_continuation(
                    self._thread_id, handoff,
                )
            thread_id = self._thread_id
            self._served_model = model or CODEX_MODEL or "default"
            q: "asyncio.Queue" = asyncio.Queue()
            self._notif_q = q
            text_parts: list[str] = []
            # ``reasoning`` only receives Codex's explicitly public reasoning
            # summaries. Never read raw reasoning deltas: those can be private
            # chain-of-thought. Public agent commentary is a separate, ordinary
            # message channel and must not be shown as a thinking chain.
            codex_thinking: list[str] = []
            commentary: list[str] = []
            agent_message_phases: dict[str, Optional[str]] = {}
            agent_message_delta_seen: set[str] = set()
            activities: list[dict] = []
            usage: dict = _map_usage(None)
            self._rate_limited = False
            self._turn_request_sent = False
            self._transport_desynced = False

            def record_activity(activity: Optional[dict]) -> Optional[dict]:
                nonlocal activities
                if activity:
                    activities = activity_protocol.merge_activities(activities, [activity])
                return activity

            try:
                client_message_id = client_message_id or f"relay-{uuid.uuid4()}"
                params: dict = {"threadId": thread_id,
                                "clientUserMessageId": client_message_id,
                                "input": [{"type": "text", "text": text}],
                                "approvalPolicy": "never",
                                # 与 thread 配置双保险：请求每轮都返回可展示的详细摘要。
                                "summary": _REASONING_SUMMARY_MODE}
                if model or CODEX_MODEL:
                    params["model"] = model or CODEX_MODEL
                if effort:
                    params["effort"] = effort
                rollout_path, rollout_offset = _rollout_cursor(thread_id)
                self._turn_request_sent = True
                deferred_request_id, deferred_request = await self._begin_request(
                    "turn/start", params,
                )

                loop = asyncio.get_running_loop()
                turn_started_at = loop.time()
                deadline = turn_started_at + TURN_TIMEOUT
                rollout_start_deadline = turn_started_at + ROLLOUT_START_TIMEOUT
                next_heartbeat = turn_started_at + HEARTBEAT_SECONDS
                request_acknowledged = False
                saw_live_turn_event = False
                active_turn_id: Optional[str] = None
                phase = _TurnPhase.SENT_UNCONFIRMED
                while True:
                    if deferred_request is not None and deferred_request.done():
                        response = deferred_request.result()
                        result = self._unwrap_response("turn/start", response)
                        acknowledged_turn_id = (result.get("turn") or {}).get("id")
                        if isinstance(acknowledged_turn_id, str):
                            if active_turn_id and active_turn_id != acknowledged_turn_id:
                                raise RuntimeError("turn/start ack disagrees with rollout turn id")
                            active_turn_id = acknowledged_turn_id
                        deferred_request = None
                        request_acknowledged = True
                        if phase == _TurnPhase.SENT_UNCONFIRMED:
                            phase = _TurnPhase.ACKNOWLEDGED
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError()
                    try:
                        obj = await asyncio.wait_for(
                            q.get(),
                            timeout=min(max(0.1, TRANSCRIPT_POLL_SECONDS), remaining),
                        )
                    except asyncio.TimeoutError:
                        rollout_state = await asyncio.to_thread(
                            _turn_state_from_rollout, rollout_path, rollout_offset,
                        )
                        if rollout_state.turn_id:
                            if active_turn_id and active_turn_id != rollout_state.turn_id:
                                raise RuntimeError("rollout turn id changed within one serialized turn")
                            active_turn_id = rollout_state.turn_id
                        if rollout_state.progressed:
                            phase = _TurnPhase.STARTED
                        salvaged = (
                            {
                                "turn_id": rollout_state.turn_id,
                                "full": rollout_state.full,
                                "commentary_full": rollout_state.commentary_full,
                                "codex_turn_usage": rollout_state.codex_turn_usage,
                            }
                            if rollout_state.completed and rollout_state.full
                            else None
                        )
                        if not salvaged:
                            if (
                                phase == _TurnPhase.SENT_UNCONFIRMED
                                and loop.time() >= rollout_start_deadline
                            ):
                                raise _TurnStartUnconfirmed(rollout_path, rollout_offset)
                            now = loop.time()
                            if HEARTBEAT_SECONDS > 0 and now >= next_heartbeat:
                                yield {"thinking_status": {
                                    "verb": None,
                                    "tokens": None,
                                    "elapsed_s": max(1, int(now - turn_started_at)),
                                }}
                                next_heartbeat = now + HEARTBEAT_SECONDS
                            continue

                        full = salvaged["full"]
                        public_commentary = salvaged["commentary_full"]
                        recovered_usage = salvaged.get("codex_turn_usage")
                        if isinstance(recovered_usage, dict):
                            usage = recovered_usage
                        current_text = "".join(text_parts)
                        current_commentary = "".join(commentary)
                        if not (
                            full.startswith(current_text)
                            and public_commentary.startswith(current_commentary)
                        ):
                            continue
                        commentary_suffix = public_commentary[len(current_commentary):]
                        text_suffix = full[len(current_text):]
                        if commentary_suffix:
                            commentary.append(commentary_suffix)
                            yield {"commentary_delta": commentary_suffix}
                        if text_suffix:
                            text_parts.append(text_suffix)
                            yield {"delta": text_suffix}

                        phase = _TurnPhase.COMPLETED
                        self._last_turn_id = active_turn_id
                        self._transport_desynced = (
                            not request_acknowledged and not saw_live_turn_event
                        )
                        logger.warning(
                            "codex turn completed from rollout: thread=%s turn=%s transport_desynced=%s",
                            thread_id, active_turn_id, self._transport_desynced,
                        )
                        yield {
                            "done": True,
                            "full": "".join(text_parts),
                            "parts": [{"type": "text", "text": "".join(text_parts)}],
                            "codex_thinking_full": "".join(codex_thinking),
                            "commentary_full": "".join(commentary),
                            "activities": activities,
                            "usage_source": "codex",
                            "codex_turn_tokens": usage["total_tokens"],
                            "codex_turn_usage": usage,
                            "model": self._served_model,
                            "rate_limited": self._rate_limited,
                            "recovered_from_rollout": True,
                        }
                        return
                    m = obj.get("method") or ""
                    p = obj.get("params") or {}
                    event_turn_id = _notification_turn_id(obj)
                    if event_turn_id:
                        if (
                            event_turn_id == getattr(self, "_last_turn_id", None)
                            and active_turn_id is None
                        ):
                            continue
                        if active_turn_id and event_turn_id != active_turn_id:
                            continue
                        if active_turn_id is None:
                            active_turn_id = event_turn_id
                        saw_live_turn_event = True
                        if phase == _TurnPhase.SENT_UNCONFIRMED:
                            phase = _TurnPhase.STARTED
                    if m == "item/started":
                        it = p.get("item") or {}
                        item_id = it.get("id")
                        if it.get("type") == "agentMessage" and isinstance(item_id, str):
                            phase = it.get("phase")
                            agent_message_phases[item_id] = phase if isinstance(phase, str) else None
                        else:
                            activity = record_activity(
                                activity_protocol.codex_activity(it, stage="started")
                            )
                            if activity:
                                yield {"activity": activity}
                    elif m == "item/agentMessage/delta":
                        d = p.get("delta") or ""
                        if d:
                            item_id = p.get("itemId")
                            if isinstance(item_id, str):
                                agent_message_delta_seen.add(item_id)
                            phase = agent_message_phases.get(item_id) if isinstance(item_id, str) else None
                            if phase == "commentary":
                                commentary.append(d)
                                yield {"commentary_delta": d}
                            else:
                                # None/unknown remains legacy-compatible final text.
                                text_parts.append(d)
                                yield {"delta": d}
                    elif m == "item/reasoning/summaryTextDelta":
                        # This is Codex's explicitly public thinking summary. Do
                        # not consume raw reasoning text deltas: they may be
                        # private chain-of-thought.
                        d = p.get("delta") or ""
                        if d:
                            codex_thinking.append(d)
                            yield {"codex_thinking_delta": d}
                    elif m == "item/completed":
                        it = p.get("item") or {}
                        it_t = it.get("type")
                        if it_t == "agentMessage":
                            item_id = it.get("id")
                            phase = it.get("phase")
                            if not isinstance(phase, str) and isinstance(item_id, str):
                                phase = agent_message_phases.get(item_id)
                            t = it.get("text")
                            if isinstance(item_id, str):
                                missing_delta = item_id not in agent_message_delta_seen
                            elif phase == "commentary":
                                missing_delta = not commentary
                            else:
                                # Legacy notifications without itemId cannot be
                                # correlated, so retain the former no-duplicate
                                # fallback for final text.
                                missing_delta = not text_parts
                            if isinstance(t, str) and t and missing_delta:
                                if phase == "commentary":
                                    commentary.append(t)
                                    yield {"commentary_delta": t}
                                else:
                                    # Final answer fallback when the delta stream
                                    # was absent or incomplete.
                                    text_parts.append(t)
                        elif it_t in _TOOL_ITEM_TYPES:
                            activity = record_activity(
                                activity_protocol.codex_activity(it, stage="completed")
                            )
                            if activity:
                                yield {"activity": activity}
                            yield {"tool_activity": {"steps": [_tool_label(it)]}}
                        else:
                            activity = record_activity(
                                activity_protocol.codex_activity(it, stage="completed")
                            )
                            if activity:
                                yield {"activity": activity}
                    elif m == "turn/plan/updated":
                        activity = record_activity(activity_protocol.codex_plan_activity(p))
                        if activity:
                            yield {"activity": activity}
                    elif m == "thread/tokenUsage/updated":
                        usage = _map_usage(p.get("tokenUsage"))
                    elif m == "account/rateLimits/updated":
                        rl = (p.get("rateLimits") or {}).get("primary") or {}
                        # usedPercent≈100 视为压力(仅记录 · 不强判耗尽 · safe-by-default)
                        if isinstance(rl.get("usedPercent"), (int, float)) and rl["usedPercent"] >= 100:
                            self._rate_limited = True
                    elif m == "turn/failed":
                        phase = _TurnPhase.COMPLETED
                        self._last_turn_id = active_turn_id
                        full = "".join(text_parts)
                        yield {"done": True, "full": full,
                               "parts": [{"type": "text", "text": full}] if full else [],
                               "codex_thinking_full": "".join(codex_thinking),
                               "commentary_full": "".join(commentary),
                               "activities": activities,
                               "usage_source": "codex",
                               "codex_turn_tokens": usage["total_tokens"],
                               "codex_turn_usage": usage, "model": self._served_model,
                               "rate_limited": self._rate_limited, "error": "turn_failed"}
                        return
                    elif m == "turn/completed":
                        phase = _TurnPhase.COMPLETED
                        self._last_turn_id = active_turn_id
                        break

                full = "".join(text_parts)
                yield {"done": True, "full": full,
                       "parts": [{"type": "text", "text": full}] if full else [],
                       "codex_thinking_full": "".join(codex_thinking),
                       "commentary_full": "".join(commentary),
                       "activities": activities,
                       "usage_source": "codex",
                       "codex_turn_tokens": usage["total_tokens"],
                       "codex_turn_usage": usage, "model": self._served_model,
                       "rate_limited": self._rate_limited}
            finally:
                if "deferred_request_id" in locals() and deferred_request_id is not None:
                    self._pending.pop(deferred_request_id, None)
                if "deferred_request" in locals() and deferred_request is not None:
                    if deferred_request.done():
                        try:
                            deferred_request.exception()
                        except (asyncio.CancelledError, Exception):
                            pass
                    else:
                        deferred_request.cancel()
                self._notif_q = None


# 进程级单例(与 cc_engine 的 _target_model 类似 · 一个引擎一个 app-server)。
_ENGINE: Optional[CodexAppServer] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> CodexAppServer:
    global _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is None or _ENGINE.proc is None or _ENGINE.proc.returncode is not None:
            _ENGINE = CodexAppServer()
            await _ENGINE.start()
        return _ENGINE


async def _discard_engine(engine: CodexAppServer) -> None:
    """Retire one wedged singleton before a safe, one-time turn retry."""
    global _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is engine:
            _ENGINE = None
        await engine.close()


async def stream_codex_turn(
    text: str, *, model: Optional[str] = None, effort: Optional[str] = None,
    epoch: Optional[int] = None, handoff: Optional[str] = None,
) -> AsyncIterator[dict]:
    """Recover an unconfirmed send after transport death and rollout reconciliation."""
    client_message_id = f"relay-{uuid.uuid4()}"
    for attempt in range(_ENGINE_RECOVERY_RETRIES + 1):
        eng = await get_engine()
        emitted = False
        emitted_payload = False
        try:
            async for ev in eng.stream_turn(
                text, model=model, effort=effort, epoch=epoch, handoff=handoff,
                client_message_id=client_message_id,
            ):
                emitted = True
                if "thinking_status" not in ev:
                    emitted_payload = True
                yield ev
            if getattr(eng, "_transport_desynced", False):
                logger.warning(
                    "codex transport silent but rollout completed; keeping app-server warm"
                )
            return
        except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError) as exc:
            logger.warning("codex turn failed (attempt=%d emitted=%s); recycle engine",
                           attempt + 1, emitted, exc_info=True)
            turn_request_sent = bool(getattr(eng, "_turn_request_sent", False))
            await _discard_engine(eng)
            if isinstance(exc, _TurnStartUnconfirmed) and attempt < _ENGINE_RECOVERY_RETRIES:
                if not _rollout_has_progress(exc.rollout_path, exc.rollout_offset):
                    logger.warning(
                        "retrying reconciled unconfirmed turn on a fresh app-server"
                    )
                    continue
                logger.error("rollout advanced during recycle; refusing ambiguous replay")
            if (
                not emitted_payload and not turn_request_sent
                and attempt < _ENGINE_RECOVERY_RETRIES
            ):
                logger.info("retrying silent codex turn once on a fresh app-server")
                continue
            raise
