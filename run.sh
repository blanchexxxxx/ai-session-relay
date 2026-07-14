#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${1:-}" == "claude" || "${1:-}" == "codex" ]]; then
  export AI_RELAY_PROVIDER="$1"
fi
export AI_RELAY_WORKSPACE="${AI_RELAY_WORKSPACE:-$HOME/ai-workspace}"
export AI_RELAY_STATE_DIR="${AI_RELAY_STATE_DIR:-$HOME/.local/state/ai-session-relay}"
mkdir -p "$AI_RELAY_WORKSPACE" "$AI_RELAY_STATE_DIR"
cd "$AI_RELAY_WORKSPACE"
exec "$ROOT/.venv/bin/python" "$ROOT/relay_server.py"
