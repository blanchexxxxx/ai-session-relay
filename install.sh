#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-auto}"
if [[ "$MODE" == "claude" || "$MODE" == "codex" || "$MODE" == "both" ]]; then
  shift
else
  MODE="auto"
fi
WORKSPACE="${1:-${AI_RELAY_WORKSPACE:-$HOME/ai-workspace}}"
STATE_DIR="${AI_RELAY_STATE_DIR:-$HOME/.local/state/ai-session-relay}"

command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
HAS_CLAUDE=0
HAS_CODEX=0
command -v claude >/dev/null && HAS_CLAUDE=1
command -v codex >/dev/null && HAS_CODEX=1
if [[ "$HAS_CLAUDE" == 0 && "$HAS_CODEX" == 0 ]]; then
  echo "Install and sign in to Claude Code or Codex first." >&2
  exit 1
fi
if [[ "$MODE" == "claude" && "$HAS_CLAUDE" == 0 ]]; then
  echo "Mode 'claude' requires the claude CLI." >&2; exit 1
fi
if [[ "$MODE" == "codex" && "$HAS_CODEX" == 0 ]]; then
  echo "Mode 'codex' requires the codex CLI." >&2; exit 1
fi
if [[ "$MODE" == "both" && ("$HAS_CLAUDE" == 0 || "$HAS_CODEX" == 0) ]]; then
  echo "Mode 'both' requires both claude and codex CLIs." >&2; exit 1
fi

if [[ "$MODE" == "codex" || ("$MODE" == "auto" && "$HAS_CLAUDE" == 0) ]]; then
  DEFAULT_PROVIDER="codex"
else
  DEFAULT_PROVIDER="claude"
fi

mkdir -p "$WORKSPACE" "$STATE_DIR"
python3 -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/python" -m pip install --upgrade pip
"$ROOT/.venv/bin/python" -m pip install -r "$ROOT/requirements.txt"

if command -v claude >/dev/null; then
  claude --version | head -n 1
fi
if command -v codex >/dev/null; then
  codex --version | head -n 1
fi

# Reuse one existing persona file for both CLIs when only one name exists.
if [[ -f "$WORKSPACE/CLAUDE.md" && ! -e "$WORKSPACE/AGENTS.md" ]]; then
  ln -s CLAUDE.md "$WORKSPACE/AGENTS.md"
elif [[ -f "$WORKSPACE/AGENTS.md" && ! -e "$WORKSPACE/CLAUDE.md" ]]; then
  ln -s AGENTS.md "$WORKSPACE/CLAUDE.md"
fi

if command -v systemctl >/dev/null && systemctl --user show-environment >/dev/null 2>&1; then
  UNIT_DIR="$HOME/.config/systemd/user"
  UNIT="$UNIT_DIR/ai-session-relay.service"
  mkdir -p "$UNIT_DIR"
  cat >"$UNIT" <<EOF
[Unit]
Description=AI Session Relay (Claude Code + Codex)
After=network-online.target

[Service]
Type=simple
WorkingDirectory="$WORKSPACE"
Environment="AI_RELAY_WORKSPACE=$WORKSPACE"
Environment="AI_RELAY_STATE_DIR=$STATE_DIR"
Environment="AI_RELAY_PROVIDER=$DEFAULT_PROVIDER"
Environment="PATH=$PATH"
ExecStart="$ROOT/.venv/bin/python" "$ROOT/relay_server.py"
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now ai-session-relay.service
  echo "Installed user service: ai-session-relay.service"
else
  echo "systemd user service unavailable; start with: $ROOT/run.sh $DEFAULT_PROVIDER"
fi

echo "Mode:      $MODE (default provider: $DEFAULT_PROVIDER)"
echo "Workspace: $WORKSPACE"
echo "State:     $STATE_DIR"
echo "Endpoint:  http://127.0.0.1:8900/chat_stream"
