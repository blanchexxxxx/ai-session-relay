#!/usr/bin/env python3
"""One local NDJSON endpoint for seamless Claude Code/Codex sessions."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from pathlib import Path
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

import cc_headless
import codex_engine

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("ai_session_relay")

STATE_DIR = Path(os.path.expanduser(
    os.environ.get("AI_RELAY_STATE_DIR", "~/.local/state/ai-session-relay")
)).resolve()
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "relay_state.json"
HISTORY_MESSAGES = max(2, int(os.environ.get("AI_RELAY_HISTORY_MESSAGES", "60")))
HANDOFF_CHARS = max(1000, int(os.environ.get("AI_RELAY_HANDOFF_CHARS", "24000")))
DEFAULT_PROVIDER = os.environ.get("AI_RELAY_PROVIDER", "").strip().lower()
if not DEFAULT_PROVIDER:
    DEFAULT_PROVIDER = "claude" if shutil.which("claude") else "codex"
if DEFAULT_PROVIDER == "cc":
    DEFAULT_PROVIDER = "claude"
if DEFAULT_PROVIDER not in {"claude", "codex"}:
    DEFAULT_PROVIDER = "claude"

app = FastAPI(title="ai-session-relay", version="0.1.0")
_TURN_LOCK = asyncio.Lock()


def _default_state() -> dict:
    return {"provider": DEFAULT_PROVIDER, "epoch": 0, "pending_switch": False, "history": []}


def _load_state() -> dict:
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        provider = _provider(raw.get("provider"))
        epoch = max(0, int(raw.get("epoch", 0)))
        history = raw.get("history")
        if not isinstance(history, list):
            history = []
        history = [
            {"role": x.get("role"), "text": x.get("text")}
            for x in history[-HISTORY_MESSAGES:]
            if isinstance(x, dict)
            and x.get("role") in {"user", "assistant"}
            and isinstance(x.get("text"), str)
            and x.get("text").strip()
        ]
        return {
            "provider": provider,
            "epoch": epoch,
            "pending_switch": bool(raw.get("pending_switch", False)),
            "history": history,
        }
    except Exception:
        return _default_state()


def _save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_FILE)


def _provider(value: object) -> str:
    provider = str(value or DEFAULT_PROVIDER).strip().lower()
    if provider == "cc":
        provider = "claude"
    if provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' (or 'cc') or 'codex'")
    return provider


def _handoff(history: list[dict]) -> str:
    if not history:
        return ""
    header = (
        "[Recent conversation handoff from the other engine. Continue naturally; "
        "do not announce or repeat this handoff.]"
    )
    lines = []
    for item in history[-HISTORY_MESSAGES:]:
        label = "User" if item["role"] == "user" else "Assistant"
        lines.append(f"{label}: {item['text'].strip()}")
    body = "\n".join(lines)
    budget = max(1, HANDOFF_CHARS - len(header) - 1)
    return header + "\n" + body[-budget:]


def _switch(state: dict, provider: str) -> bool:
    if state["provider"] == provider:
        return False
    state["provider"] = provider
    state["epoch"] = int(state.get("epoch", 0)) + 1
    state["pending_switch"] = True
    _save_state(state)
    return True


async def _events(
    provider: str,
    text: str,
    *,
    model: str | None,
    effort: str | None,
    switched: bool,
    epoch: int,
    handoff: str | None,
) -> AsyncIterator[dict]:
    if provider == "codex":
        async for event in codex_engine.stream_codex_turn(
            text,
            model=model,
            effort=effort,
            epoch=epoch if switched else None,
            handoff=handoff if switched else None,
        ):
            yield event
        return

    prompt = text
    if switched and handoff:
        prompt = f"{handoff}\n\n[Current user message]\n{text}"
    async for event in cc_headless.stream_headless(
        prompt, model=model, effort=effort,
    ):
        yield event


@app.get("/health")
async def health() -> dict:
    state = _load_state()
    return {
        "ok": True,
        "provider": state["provider"],
        "epoch": state["epoch"],
        "claude_cli": bool(shutil.which("claude")),
        "codex_cli": bool(shutil.which("codex")),
    }


@app.get("/provider")
async def get_provider() -> dict:
    state = _load_state()
    return {"provider": state["provider"], "epoch": state["epoch"]}


@app.post("/provider")
async def set_provider(req: Request):
    try:
        body = await req.json()
        provider = _provider(body.get("provider"))
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    async with _TURN_LOCK:
        state = _load_state()
        changed = _switch(state, provider)
    return {"ok": True, "provider": provider, "epoch": state["epoch"], "changed": changed}


@app.post("/chat_stream")
async def chat_stream(req: Request):
    try:
        body = await req.json()
        text = str(body.get("text") or "").strip()
        if not text:
            raise ValueError("text is required")
        requested_provider = body.get("provider")
        model = str(body.get("model") or "").strip() or None
        effort = str(body.get("effort") or "").strip() or None
        explicit_handoff = str(body.get("handoff") or "").strip() or None
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    async def generate():
        async with _TURN_LOCK:
            state = _load_state()
            try:
                provider = _provider(requested_provider or state["provider"])
            except ValueError as exc:
                yield json.dumps({"done": True, "full": "", "parts": [],
                                  "error": str(exc)}, ensure_ascii=False) + "\n"
                return

            handoff = explicit_handoff[-HANDOFF_CHARS:] if explicit_handoff else _handoff(state["history"])
            changed_now = _switch(state, provider)
            switched = changed_now or bool(state.get("pending_switch"))
            state["pending_switch"] = False
            state["history"].append({"role": "user", "text": text})
            state["history"] = state["history"][-HISTORY_MESSAGES:]
            _save_state(state)
            final = ""
            try:
                async for event in _events(
                    provider, text, model=model, effort=effort,
                    switched=switched, epoch=state["epoch"], handoff=handoff,
                ):
                    if event.get("done"):
                        final = str(event.get("full") or "").strip()
                        event.setdefault("provider", provider)
                    yield json.dumps(event, ensure_ascii=False) + "\n"
            except Exception as exc:  # noqa: BLE001
                logger.exception("%s turn failed", provider)
                yield json.dumps({
                    "done": True, "full": final,
                    "parts": ([{"type": "text", "text": final}] if final else []),
                    "provider": provider, "error": f"{type(exc).__name__}: {exc}",
                }, ensure_ascii=False) + "\n"
            finally:
                if final:
                    state = _load_state()
                    state["history"].append({"role": "assistant", "text": final})
                    state["history"] = state["history"][-HISTORY_MESSAGES:]
                    _save_state(state)

    return StreamingResponse(generate(), media_type="application/x-ndjson")


if __name__ == "__main__":
    host = os.environ.get("AI_RELAY_HOST", "127.0.0.1")
    port = int(os.environ.get("AI_RELAY_PORT", "8900"))
    uvicorn.run(app, host=host, port=port, log_level="info")
