"""#346 — Codex app-server wedge recovery stays safe and single-retry."""
from __future__ import annotations

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import codex_engine  # noqa: E402


def _rollout_line(payload_type, payload):
    return json.dumps({"type": payload_type, "payload": payload}, ensure_ascii=False)


async def _collect(stream):
    return [event async for event in stream]


def test_completed_rollout_recovers_only_public_content_and_last_usage(tmp_path):
    rollout = tmp_path / "rollout-thread-1.jsonl"
    rollout.write_text(_rollout_line("response_item", {
        "type": "message", "role": "assistant", "phase": "final_answer",
        "content": [{"type": "output_text", "text": "old answer"}],
    }) + "\n", encoding="utf-8")
    cursor = rollout.stat().st_size
    tail = [
        _rollout_line("response_item", {
            "type": "reasoning", "encrypted_content": "private-must-not-surface",
        }),
        _rollout_line("response_item", {
            "type": "message", "role": "assistant", "phase": "commentary",
            "content": [{"type": "output_text", "text": "working"}],
        }),
        _rollout_line("response_item", {
            "type": "message", "role": "assistant", "phase": "final_answer",
            "content": [{"type": "output_text", "text": "new answer"}],
        }),
        _rollout_line("event_msg", {
            "type": "token_count",
            "info": {
                "total_token_usage": {"total_tokens": 99999},
                "last_token_usage": {
                    "input_tokens": 10, "cached_input_tokens": 2,
                    "output_tokens": 5, "reasoning_output_tokens": 1,
                    "total_tokens": 15,
                },
                "model_context_window": 258400,
            },
        }),
        _rollout_line("event_msg", {"type": "task_complete"}),
    ]
    with rollout.open("a", encoding="utf-8") as stream:
        stream.write("\n".join(tail) + "\n")

    recovered = codex_engine._completed_turn_from_rollout(str(rollout), cursor)

    assert recovered["full"] == "new answer"
    assert recovered["commentary_full"] == "working"
    assert recovered["codex_turn_usage"]["total_tokens"] == 15
    assert "private-must-not-surface" not in repr(recovered)
    assert "old answer" not in repr(recovered)


def test_incomplete_rollout_is_never_recovered(tmp_path):
    rollout = tmp_path / "rollout-thread-1.jsonl"
    rollout.write_text(_rollout_line("response_item", {
        "type": "message", "role": "assistant", "phase": "final_answer",
        "content": [{"type": "output_text", "text": "not finished"}],
    }) + "\n", encoding="utf-8")

    assert codex_engine._completed_turn_from_rollout(str(rollout), 0) is None


@pytest.mark.asyncio
async def test_fresh_engine_turn_completes_without_waiting_for_rpc_ack(monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._turn_lock = asyncio.Lock()
    server._thread_id = "thread-1"
    server._notif_q = None
    server._rate_limited = False
    server._served_model = "default"
    server._rollout_only = False
    server._pending = {}

    async def begin_request(_method, _params):
        future = asyncio.get_running_loop().create_future()
        server._pending[9] = future
        return 9, future

    recovered = iter([None, {
        "full": "fast reply",
        "commentary_full": "",
        "codex_turn_usage": {
            "input_tokens": 10, "cached_input_tokens": 2,
            "output_tokens": 5, "reasoning_output_tokens": 1,
            "total_tokens": 15, "model_context_window": 258400,
        },
    }])
    monkeypatch.setattr(server, "_begin_request", begin_request)
    monkeypatch.setattr(codex_engine, "_rollout_cursor", lambda _tid: ("rollout", 10))
    monkeypatch.setattr(
        codex_engine, "_completed_turn_from_rollout", lambda _path, _offset: next(recovered),
    )
    monkeypatch.setattr(codex_engine, "_rollout_has_progress", lambda _path, _offset: True)
    monkeypatch.setattr(codex_engine, "TRANSCRIPT_POLL_SECONDS", 0.001)

    events = await asyncio.wait_for(_collect(server.stream_turn("hello")), timeout=0.5)

    assert events[-1]["full"] == "fast reply"
    assert events[-1]["codex_turn_tokens"] == 15
    assert server._pending == {}


@pytest.mark.asyncio
async def test_any_turn_surfaces_explicit_rpc_error(monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._turn_lock = asyncio.Lock()
    server._thread_id = "thread-1"
    server._notif_q = None
    server._rate_limited = False
    server._served_model = "default"
    server._rollout_only = False
    server._pending = {}

    async def begin_request(_method, _params):
        future = asyncio.get_running_loop().create_future()
        future.set_result({"error": {"message": "bad input"}})
        server._pending[10] = future
        return 10, future

    monkeypatch.setattr(server, "_begin_request", begin_request)
    monkeypatch.setattr(codex_engine, "_rollout_cursor", lambda _tid: ("rollout", 10))

    with pytest.raises(RuntimeError, match="bad input"):
        await _collect(server.stream_turn("hello"))
    assert server._pending == {}


@pytest.mark.asyncio
async def test_any_turn_fails_fast_without_ack_or_progress(monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._turn_lock = asyncio.Lock()
    server._thread_id = "thread-1"
    server._notif_q = None
    server._rate_limited = False
    server._served_model = "default"
    server._rollout_only = False
    server._pending = {}

    async def begin_request(_method, _params):
        future = asyncio.get_running_loop().create_future()
        server._pending[11] = future
        return 11, future

    monkeypatch.setattr(server, "_begin_request", begin_request)
    monkeypatch.setattr(codex_engine, "_rollout_cursor", lambda _tid: ("rollout", 10))
    monkeypatch.setattr(codex_engine, "_completed_turn_from_rollout", lambda _p, _o: None)
    monkeypatch.setattr(codex_engine, "_rollout_has_progress", lambda _p, _o: False)
    monkeypatch.setattr(codex_engine, "TRANSCRIPT_POLL_SECONDS", 0.001)
    monkeypatch.setattr(codex_engine, "ROLLOUT_START_TIMEOUT", 0.002)

    with pytest.raises(ConnectionError, match="no RPC ack or rollout progress"):
        await asyncio.wait_for(_collect(server.stream_turn("hello")), timeout=0.2)
    assert server._pending == {}


@pytest.mark.asyncio
async def test_long_silent_turn_emits_keepalive_until_rollout_completes(monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._turn_lock = asyncio.Lock()
    server._thread_id = "thread-1"
    server._notif_q = None
    server._rate_limited = False
    server._served_model = "default"
    server._rollout_only = False
    server._pending = {}

    async def begin_request(_method, _params):
        future = asyncio.get_running_loop().create_future()
        server._pending[12] = future
        return 12, future

    polls = 0

    def completed(_path, _offset):
        nonlocal polls
        polls += 1
        if polls < 5:
            return None
        return {
            "full": "eventual reply",
            "commentary_full": "",
            "codex_turn_usage": codex_engine._map_usage(None),
        }

    monkeypatch.setattr(server, "_begin_request", begin_request)
    monkeypatch.setattr(codex_engine, "_rollout_cursor", lambda _tid: ("rollout", 10))
    monkeypatch.setattr(codex_engine, "_completed_turn_from_rollout", completed)
    monkeypatch.setattr(codex_engine, "_rollout_has_progress", lambda _p, _o: True)
    monkeypatch.setattr(codex_engine, "TRANSCRIPT_POLL_SECONDS", 0.001)
    monkeypatch.setattr(codex_engine, "HEARTBEAT_SECONDS", 0.002)

    events = await _collect(server.stream_turn("hello"))

    assert any(event.get("thinking_status") for event in events)
    assert events[-1]["full"] == "eventual reply"


@pytest.mark.asyncio
async def test_request_timeout_removes_its_pending_future(monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._reqid = 0
    server._pending = {}

    async def fake_send(_obj):
        return None

    monkeypatch.setattr(server, "_send", fake_send)
    monkeypatch.setattr(codex_engine, "REQ_TIMEOUT", 0.001)

    with pytest.raises(asyncio.TimeoutError):
        await server._request("thread/resume", {"threadId": "bad-thread"})

    assert server._pending == {}


@pytest.mark.asyncio
async def test_connection_loss_unblocks_every_pending_request():
    server = object.__new__(codex_engine.CodexAppServer)
    future = asyncio.get_running_loop().create_future()
    server._pending = {1: future}

    server._fail_pending("stdout closed")

    assert server._pending == {}
    with pytest.raises(ConnectionError, match="stdout closed"):
        await future


@pytest.mark.asyncio
async def test_resume_transport_silence_preserves_pointer_and_uses_rollout(tmp_path, monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    ptr = tmp_path / "codex_last_thread"
    ptr.write_text("bad-thread", encoding="utf-8")
    calls = []

    async def failing_request(method, params, **kwargs):
        calls.append((method, params, kwargs))
        raise ConnectionError("app-server stopped responding")

    async def must_not_start_fresh():
        raise AssertionError("fresh thread must wait for process recycle")

    monkeypatch.setattr(codex_engine, "THREAD_PTR", str(ptr))
    monkeypatch.setattr(server, "_request", failing_request)
    monkeypatch.setattr(server, "_start_fresh_thread", must_not_start_fresh)

    assert await server._resolve_thread() == "bad-thread"

    assert calls == [
        ("thread/resume", {
            "threadId": "bad-thread",
            "config": codex_engine._reasoning_summary_config(),
        }, {"timeout": codex_engine.RESUME_TIMEOUT}),
    ]
    assert ptr.read_text(encoding="utf-8") == "bad-thread"
    assert server._rollout_only is True


@pytest.mark.asyncio
async def test_explicit_missing_thread_clears_pointer_and_recycles(tmp_path, monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    server._thread_id = "missing-thread"
    ptr = tmp_path / "codex_last_thread"
    ptr.write_text("missing-thread", encoding="utf-8")

    async def missing_request(_method, _params, **_kwargs):
        raise RuntimeError("thread/resume error: thread not found")

    monkeypatch.setattr(codex_engine, "THREAD_PTR", str(ptr))
    monkeypatch.setattr(server, "_request", missing_request)

    with pytest.raises(RuntimeError, match="explicitly unavailable"):
        await server._resolve_thread()

    assert not ptr.exists()
    assert server._thread_id is None


@pytest.mark.asyncio
async def test_generic_resume_rpc_error_never_deletes_pointer(tmp_path, monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    ptr = tmp_path / "codex_last_thread"
    ptr.write_text("existing-thread", encoding="utf-8")

    async def invalid_request(_method, _params, **_kwargs):
        raise RuntimeError("Invalid request: unsupported config field")

    monkeypatch.setattr(codex_engine, "THREAD_PTR", str(ptr))
    monkeypatch.setattr(server, "_request", invalid_request)

    with pytest.raises(RuntimeError, match="unsupported config"):
        await server._resolve_thread()

    assert ptr.read_text(encoding="utf-8") == "existing-thread"


@pytest.mark.asyncio
async def test_resume_retries_without_config_only_after_explicit_schema_error(
    tmp_path, monkeypatch,
):
    server = object.__new__(codex_engine.CodexAppServer)
    ptr = tmp_path / "codex_last_thread"
    ptr.write_text("existing-thread", encoding="utf-8")
    calls = []

    async def request(method, params, **kwargs):
        calls.append((method, params, kwargs))
        if len(calls) == 1:
            raise RuntimeError("config rejected")
        return {}

    monkeypatch.setattr(codex_engine, "THREAD_PTR", str(ptr))
    monkeypatch.setattr(server, "_request", request)

    assert await server._resolve_thread() == "existing-thread"
    assert calls == [
        ("thread/resume", {
            "threadId": "existing-thread",
            "config": codex_engine._reasoning_summary_config(),
        }, {"timeout": codex_engine.RESUME_TIMEOUT}),
        ("thread/resume", {"threadId": "existing-thread"}, {
            "timeout": codex_engine.RESUME_TIMEOUT,
        }),
    ]


@pytest.mark.asyncio
async def test_silent_failure_recycles_and_retries_once(monkeypatch):
    class SilentFailure:
        calls = 0

        async def stream_turn(self, *_args, **_kwargs):
            self.calls += 1
            raise asyncio.TimeoutError()
            yield {}  # pragma: no cover - keeps this an async generator

    class WorkingEngine:
        calls = 0

        async def stream_turn(self, *_args, **_kwargs):
            self.calls += 1
            yield {"delta": "recovered"}

    broken = SilentFailure()
    healthy = WorkingEngine()
    engines = [broken, healthy]
    discarded = []

    async def fake_get_engine():
        return engines.pop(0)

    async def fake_discard(engine):
        discarded.append(engine)

    monkeypatch.setattr(codex_engine, "get_engine", fake_get_engine)
    monkeypatch.setattr(codex_engine, "_discard_engine", fake_discard)

    events = [event async for event in codex_engine.stream_codex_turn("hello")]

    assert events == [{"delta": "recovered"}]
    assert broken.calls == 1
    assert healthy.calls == 1
    assert discarded == [broken]


@pytest.mark.asyncio
async def test_visible_failure_recycles_but_never_replays_turn(monkeypatch):
    class PartiallyVisibleFailure:
        calls = 0

        async def stream_turn(self, *_args, **_kwargs):
            self.calls += 1
            yield {"tool_activity": {"steps": ["called a tool"]}}
            raise RuntimeError("connection wedged after side effect")

    broken = PartiallyVisibleFailure()
    get_calls = 0
    discarded = []

    async def fake_get_engine():
        nonlocal get_calls
        get_calls += 1
        return broken

    async def fake_discard(engine):
        discarded.append(engine)

    monkeypatch.setattr(codex_engine, "get_engine", fake_get_engine)
    monkeypatch.setattr(codex_engine, "_discard_engine", fake_discard)

    events = []
    with pytest.raises(RuntimeError, match="side effect"):
        async for event in codex_engine.stream_codex_turn("hello"):
            events.append(event)

    assert events == [{"tool_activity": {"steps": ["called a tool"]}}]
    assert get_calls == 1
    assert broken.calls == 1
    assert discarded == [broken]


@pytest.mark.asyncio
async def test_silent_failure_after_turn_send_is_never_replayed(monkeypatch):
    class SentThenSilent:
        _turn_request_sent = True
        calls = 0

        async def stream_turn(self, *_args, **_kwargs):
            self.calls += 1
            raise asyncio.TimeoutError()
            yield {}  # pragma: no cover

    broken = SentThenSilent()
    get_calls = 0
    discarded = []

    async def fake_get_engine():
        nonlocal get_calls
        get_calls += 1
        return broken

    async def fake_discard(engine):
        discarded.append(engine)

    monkeypatch.setattr(codex_engine, "get_engine", fake_get_engine)
    monkeypatch.setattr(codex_engine, "_discard_engine", fake_discard)

    with pytest.raises(asyncio.TimeoutError):
        await _collect(codex_engine.stream_codex_turn("hello"))

    assert get_calls == 1
    assert broken.calls == 1
    assert discarded == [broken]
