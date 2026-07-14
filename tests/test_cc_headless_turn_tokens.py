"""
cc_headless Claude Code 专属 usage contract 单测。

来源 = `claude -p` result 事件的聚合 `usage`。保留 Anthropic 原生字段名；
`context_input_tokens = input + cache_creation_input + cache_read_input`，不得映射成 Codex
的 input/cache 字段或旧通用 `new/cache_read/cache_write/out`。

本测覆盖(纯异步 · mock 子进程 · 无 Pi / 无真 claude · 永远能跑):
  ① 原生四项 + context 输入及本轮吞吐计算正确。
  ② result.usage → attempt 的 CC 专属字段；缺失时回落 assistant message.usage。
  ③ done 只输出 usage_source/cc_turn_*，不输出旧通用字段。

project root/ 不在 repo import 路径(模块以 bare `cc_headless` 导入)· 这里把它加进
sys.path(跟 test_cc_headless_timeout_carry.py 同手法)。
"""
import asyncio
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import cc_headless  # noqa: E402


# ── fake 子进程(同 timeout-carry 测的最小复刻)────────────────────────────────
class _FakeStdout:
    def __init__(self, lines):
        self._queue = [(ln + "\n").encode("utf-8") for ln in lines]

    async def readline(self):
        if self._queue:
            return self._queue.pop(0)
        return b""  # EOF


class _FakeProc:
    def __init__(self, lines):
        self.stdout = _FakeStdout(lines)
        self.stderr = None
        self.returncode = 0  # 已退出 · finally 不 kill

    def kill(self):  # pragma: no cover
        pass

    async def wait(self):
        return self.returncode


def _patch_spawn(monkeypatch, lines):
    async def _fake_exec(*args, **kwargs):
        return _FakeProc(lines)

    monkeypatch.setattr(cc_headless.asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(cc_headless, "write_last_session", lambda *_a, **_k: None)
    monkeypatch.setattr(cc_headless, "_read_turn_thinking", lambda *_a, **_k: "")


async def _collect(args, sid):
    attempts = []
    async for ev in cc_headless._stream_one_spawn(args, sid):
        if "_attempt" in ev:
            attempts.append(ev["_attempt"])
    return attempts


# ① Claude Code 原生 usage + 派生 context ─────────────────────────────────────
def test_extract_cc_turn_usage_preserves_native_fields_and_derives_context():
    usage = {
        "input_tokens": 100,
        "cache_creation_input_tokens": 50,
        "cache_read_input_tokens": 140000,
        "output_tokens": 300,
    }
    assert cc_headless._extract_cc_turn_usage(usage) == {
        "input_tokens": 100,
        "cache_creation_input_tokens": 50,
        "cache_read_input_tokens": 140000,
        "context_input_tokens": 140150,
        "output_tokens": 300,
    }
    assert cc_headless._extract_cc_turn_tokens(usage) == 140450


def test_extract_cc_turn_usage_partial_fields_default_zero():
    # 缺 cache_* → 当 0(不报错)· 只 input+output。
    usage = cc_headless._extract_cc_turn_usage(
        {"input_tokens": 10, "output_tokens": 40})
    assert usage == {
        "input_tokens": 10,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "context_input_tokens": 10,
        "output_tokens": 40,
    }
    assert cc_headless._extract_cc_turn_tokens(
        {"input_tokens": 10, "output_tokens": 40}) == 50


def test_extract_cc_turn_usage_invalid_or_zero_is_none():
    assert cc_headless._extract_cc_turn_usage(None) is None
    assert cc_headless._extract_cc_turn_usage({}) is None
    assert cc_headless._extract_cc_turn_usage("nope") is None
    # 全 0(流式残片)→ None(别返 0 当真值)。
    assert cc_headless._extract_cc_turn_usage(
        {"input_tokens": 0, "output_tokens": 0}) is None
    assert cc_headless._extract_cc_turn_usage(
        {"input_tokens": -1, "output_tokens": 2}) is None


# ② result.usage 在场 → attempt 只带 CC 专属 usage ────────────────────────────
def test_result_usage_flows_into_cc_attempt_contract(monkeypatch):
    text_delta = {"type": "stream_event", "event": {
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "嗯"}}}
    result_ev = {"type": "result", "subtype": "success", "result": "嗯",
                 "duration_ms": 1000,
                 "usage": {"input_tokens": 100, "cache_creation_input_tokens": 50,
                           "cache_read_input_tokens": 140000, "output_tokens": 300}}
    _patch_spawn(monkeypatch, [json.dumps(text_delta), json.dumps(result_ev)])

    attempts = asyncio.run(_collect(["claude"], "sid-x"))

    assert len(attempts) == 1
    assert attempts[0]["cc_turn_tokens"] == 140450
    assert attempts[0]["cc_turn_usage"] == {
        "input_tokens": 100,
        "cache_creation_input_tokens": 50,
        "cache_read_input_tokens": 140000,
        "context_input_tokens": 140150,
        "output_tokens": 300,
    }
    assert "turn_tokens" not in attempts[0]
    assert "turn_usage" not in attempts[0]


# ③ result 无 usage → 回落末条 assistant message.usage(红队补洞)─────────────────
def test_falls_back_to_assistant_message_usage(monkeypatch):
    text_delta = {"type": "stream_event", "event": {
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": "hi"}}}
    # assistant 完整 message 带 usage(与 forge.last_assistant_usage 同源)。
    assistant_ev = {"type": "assistant", "message": {
        "usage": {"input_tokens": 10, "cache_read_input_tokens": 2000,
                  "output_tokens": 40}}}
    # result 事件**故意不带** usage(模拟 CLI 版本漂移)。
    result_ev = {"type": "result", "subtype": "success", "result": "hi"}
    _patch_spawn(monkeypatch, [
        json.dumps(text_delta), json.dumps(assistant_ev), json.dumps(result_ev)])

    attempts = asyncio.run(_collect(["claude"], "sid-x"))

    assert len(attempts) == 1
    assert attempts[0]["cc_turn_tokens"] == 2050, "result 缺 usage 必须回落 assistant message.usage"
    assert attempts[0]["cc_turn_usage"]["context_input_tokens"] == 2010


# result 无 usage 且无 assistant 事件 → CC usage None(不瞎编)──────────────────
def test_no_usage_anywhere_is_none(monkeypatch):
    result_ev = {"type": "result", "subtype": "success", "result": "hi"}
    _patch_spawn(monkeypatch, [json.dumps(result_ev)])

    attempts = asyncio.run(_collect(["claude"], "sid-x"))

    assert len(attempts) == 1
    assert attempts[0]["cc_turn_tokens"] is None
    assert attempts[0]["cc_turn_usage"] is None


# ③ done wire contract：只写 CC 专属字段 ─────────────────────────────────────
def test_build_done_ev_uses_only_cc_usage_contract():
    usage = cc_headless._extract_cc_turn_usage({
        "input_tokens": 1,
        "cache_creation_input_tokens": 3,
        "cache_read_input_tokens": 2,
        "output_tokens": 4,
    })
    ev = cc_headless._build_done_ev(
        "hi", [], "", None, cc_turn_tokens=10, cc_turn_usage=usage)
    assert ev["usage_source"] == "cc"
    assert ev["cc_turn_tokens"] == 10
    assert ev["cc_turn_usage"] == usage
    assert "turn_tokens" not in ev
    assert "turn_usage" not in ev

    empty = cc_headless._build_done_ev("hi", [], "", None)
    assert empty["usage_source"] == "cc"
    assert "cc_turn_tokens" not in empty
    assert "cc_turn_usage" not in empty


def test_legacy_attempt_keys_are_read_only_compatibility():
    legacy = {
        "reasoning_full": "旧 CC thinking",
        "turn_tokens": 10,
        "turn_usage": {
            "new": 1, "cache_write": 3, "cache_read": 2, "out": 4, "total": 10,
        },
    }
    assert cc_headless._attempt_cc_value(
        legacy, "cc_thinking_full", "reasoning_full") == "旧 CC thinking"
    assert cc_headless._attempt_cc_value(
        legacy, "cc_turn_tokens", "turn_tokens") == 10
    assert cc_headless._attempt_cc_usage(legacy) == {
        "input_tokens": 1,
        "cache_creation_input_tokens": 3,
        "cache_read_input_tokens": 2,
        "context_input_tokens": 6,
        "output_tokens": 4,
    }
    assert cc_headless._attempt_cc_tokens(legacy) == 10
