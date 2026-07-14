from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import relay_server  # noqa: E402


def test_switch_persists_pending_handoff(tmp_path, monkeypatch):
    monkeypatch.setattr(relay_server, "STATE_DIR", tmp_path)
    monkeypatch.setattr(relay_server, "STATE_FILE", tmp_path / "state.json")
    state = relay_server._default_state()
    target = "codex" if state["provider"] == "claude" else "claude"

    assert relay_server._switch(state, target) is True
    loaded = relay_server._load_state()

    assert loaded["provider"] == target
    assert loaded["epoch"] == 1
    assert loaded["pending_switch"] is True


def test_handoff_is_bounded_and_neutral():
    history = [
        {"role": "user", "text": "u" * 800},
        {"role": "assistant", "text": "a" * 800},
    ]
    handoff = relay_server._handoff(history)

    assert len(handoff) <= relay_server.HANDOFF_CHARS
    assert "User:" in handoff
    assert "Assistant:" in handoff


@pytest.mark.asyncio
async def test_claude_switch_receives_handoff_once(monkeypatch):
    seen = []

    async def fake_stream(prompt, model=None, effort=None):
        seen.append(prompt)
        yield {"done": True, "full": "ok", "parts": [{"type": "text", "text": "ok"}]}

    monkeypatch.setattr(relay_server.cc_headless, "stream_headless", fake_stream)
    events = [event async for event in relay_server._events(
        "claude", "new message", model=None, effort=None, switched=True,
        epoch=1, handoff="User: old message",
    )]

    assert events[-1]["full"] == "ok"
    assert "User: old message" in seen[0]
    assert seen[0].endswith("new message")
