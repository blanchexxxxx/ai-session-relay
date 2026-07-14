"""#346 — Codex app-server wedge recovery stays safe and single-retry."""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import codex_engine  # noqa: E402


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
async def test_resume_failure_clears_pointer_and_never_starts_on_same_process(tmp_path, monkeypatch):
    server = object.__new__(codex_engine.CodexAppServer)
    ptr = tmp_path / "codex_last_thread"
    ptr.write_text("bad-thread", encoding="utf-8")
    calls = []

    async def failing_request(method, params):
        calls.append((method, params))
        raise ConnectionError("app-server stopped responding")

    async def must_not_start_fresh():
        raise AssertionError("fresh thread must wait for process recycle")

    monkeypatch.setattr(codex_engine, "THREAD_PTR", str(ptr))
    monkeypatch.setattr(server, "_request", failing_request)
    monkeypatch.setattr(server, "_start_fresh_thread", must_not_start_fresh)

    with pytest.raises(RuntimeError, match="recycle required"):
        await server._resolve_thread()

    assert calls == [
        ("thread/resume", {
            "threadId": "bad-thread",
            "config": codex_engine._reasoning_summary_config(),
        }),
        ("thread/resume", {"threadId": "bad-thread"}),
    ]
    assert not ptr.exists()


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
