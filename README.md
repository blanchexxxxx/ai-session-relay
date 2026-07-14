# AI Session Relay

A small, frontend-agnostic relay that keeps Claude Code sessions and Codex threads alive, then hands recent context across when you switch engines.

It is intentionally only a local HTTP/NDJSON service. There is no web UI, database, account system, memory product, or bundled frontend.

## What it keeps

- Claude Code: persistent session pointer, safe resume fallback, transcript forge/trim, and empty-turn recovery.
- Codex: persistent `app-server`, `thread/resume`, bad-thread cleanup, and one safe retry after a silent wedge.
- Engine switching: one local JSON state file with the active provider, a monotonic epoch, and a bounded recent handoff.
- Streaming: the existing `delta`, activity, provider-specific thinking/usage, and final `done` NDJSON events.

## Five-minute setup

Requirements: Python 3.10+, and at least one installed and signed-in CLI. To use seamless switching, sign in to both.

- Claude Code installation and login: <https://docs.anthropic.com/en/docs/claude-code/getting-started>
- Codex login: run `codex login` (or `codex login --device-auth` on a headless machine).

Choose only what you need:

```bash
git clone https://github.com/blanchexxxxx/ai-session-relay.git
cd ai-session-relay

./install.sh claude /path/to/your/ai-workspace  # Claude Code continuity only
./install.sh codex  /path/to/your/ai-workspace  # Codex continuity only
./install.sh both   /path/to/your/ai-workspace  # seamless Claude/Codex switching
```

The workspace argument is optional and defaults to `~/ai-workspace`. If it contains only `CLAUDE.md` or only `AGENTS.md`, the installer links the missing filename to the existing file so both engines receive the same identity/instructions. If your frontend already injects all context, the workspace can stay empty.

On Linux with a working user-level systemd session, installation also enables `ai-session-relay.service`. Else run `./run.sh`.

## Connect an existing frontend

Use one base URL: `http://127.0.0.1:8900`.

Continue the active engine:

```bash
curl -sN http://127.0.0.1:8900/chat_stream \
  -H 'content-type: application/json' \
  -d '{"text":"Hello"}'
```

Switch to Codex and send the next message in the same request:

```bash
curl -sN http://127.0.0.1:8900/chat_stream \
  -H 'content-type: application/json' \
  -d '{"provider":"codex","text":"Continue from where we left off"}'
```

Switch back with `"provider":"claude"` (alias `"cc"` is accepted). The `provider` field is optional after switching. You can also call `POST /provider` first; the relay remembers that a handoff is pending for the next message.

Minimal request:

```json
{"text":"...", "provider":"claude|codex", "model":"optional", "effort":"optional"}
```

Optional `handoff` overrides the relay-built recent-context handoff for that switch. This is useful when an existing frontend already owns the authoritative conversation history.

The response is `application/x-ndjson`. Consume zero or more streaming events, then treat the last event with `"done": true` as authoritative. Its `full` field is the final assistant text.

Health and current provider:

```bash
curl -s http://127.0.0.1:8900/health
curl -s http://127.0.0.1:8900/provider
```

## Configuration

No environment variables are required. Optional variables:

| Variable | Default |
|---|---|
| `AI_RELAY_WORKSPACE` | current directory (`run.sh`: `~/ai-workspace`) |
| `AI_RELAY_STATE_DIR` | `~/.local/state/ai-session-relay` |
| `AI_RELAY_PROVIDER` | Claude if installed, otherwise Codex |
| `AI_RELAY_HOST` / `AI_RELAY_PORT` | `127.0.0.1` / `8900` |
| `AI_RELAY_HISTORY_MESSAGES` | `60` |
| `AI_RELAY_HANDOFF_CHARS` | `24000` |
| `CODEX_MODEL` | Codex CLI default |

Existing MCP configuration remains owned by each CLI; this relay does not require or rewrite it.

## Security

The service deliberately listens on loopback. Do not expose it directly to a LAN or the internet: both agents can use tools under your OS account, and this extraction preserves the trusted-local autonomous approval behavior of the original engine. Put authentication and TLS in your own backend if remote access is required.

## 中文：先判断你是哪一种人

### 1. 只想让 Claude Code 无缝换窗

你只需要安装并登录 Claude Code，不需要安装 Codex：

```bash
./install.sh claude 你的AI工作目录
```

relay 会记住 Claude 的 session id。正常重启服务、关掉终端、机器重启后，下一条消息会继续原 session；会话太肥时，会自动保留最近完整轮次生成一个可续接的新 session，避免撞上下文窗口。

### 2. 只想让 Codex 无缝换窗

你只需要安装并登录 Codex，不需要安装 Claude Code：

```bash
codex login
./install.sh codex 你的AI工作目录
```

没有浏览器的 VPS 可以先运行 `codex login --device-auth`，在手机或电脑浏览器完成授权。relay 会保存 Codex thread id；服务重启后通过 `thread/resume` 接回去。坏 thread 或卡死的 app-server 会被自动丢弃并重建。

### 3. 想在 Claude Code 和 Codex 之间无缝切换

两个 CLI 都安装并登录一次：

```bash
./install.sh both 你的AI工作目录
```

你的前端仍然只接一个 `/chat_stream`。平时只传 `text`；换引擎的那一条加：

```json
{"provider":"codex","text":"接着刚才的话说"}
```

或：

```json
{"provider":"claude","text":"接着刚才的话说"}
```

relay 会自动递增切换 epoch，并把最近对话交给新引擎。你的前端如果已经保存完整聊天记录，也可以在切换那一条传 `handoff`，覆盖 relay 自动生成的近场交接。

## 中文：运行环境到底是什么

这个项目不是云服务，也不是前端。它是运行在你自己机器上的一个本地后端进程。

最少需要：

- Python 3.10 或更高版本。
- Claude Code、Codex 二选一；需要双引擎切换时两个都装。
- 对应账号已经登录。
- 一个固定的 AI 工作目录。Claude 在这里读 `CLAUDE.md`，Codex 在这里读 `AGENTS.md`；只有其中一个文件时，安装器会给另一个名字建立软链接。
- 机器能联网调用模型。

机器怎么选：

- 只在电脑前使用：自己的笔记本或台式机就够了。开机时能用，关机后不能用。
- 希望手机随时聊、AI 24 小时在线：需要一台 24 小时开着的电脑、迷你主机、树莓派、NAS，或一台 VPS。
- 新人优先推荐 Linux（Ubuntu/Debian/Raspberry Pi OS）。安装脚本能自动注册 systemd user service。
- Windows 推荐放在 WSL2 里运行。原生 Windows 不会自动安装 systemd 服务；可以在 WSL 里运行 `./run.sh`。
- macOS 可以运行 `./run.sh`，但当前脚本不会替你创建 launchd 服务。

Linux 机器如果没有用户登录也要开机自启，再执行一次：

```bash
sudo loginctl enable-linger "$USER"
systemctl --user enable --now ai-session-relay.service
```

检查是否正常：

```bash
curl http://127.0.0.1:8900/health
systemctl --user status ai-session-relay.service
```

`/health` 里的 `claude_cli` / `codex_cli` 表示命令是否找得到，不代表账号额度一定可用。真正的登录、权限或额度错误会出现在 `/chat_stream` 最后一条 `done.error` 和服务日志里。

## 中文：让手机连到你的 AI

最推荐 Tailscale：它把手机和运行 relay 的机器放进同一个私人网络，不需要把端口暴露到公网。

### 第一步：服务器安装 Tailscale

Linux 官方安装方式：

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

终端会给出登录链接。用浏览器登录后，在 Tailscale 管理页确认这台机器已经在线。官方说明见 [Install Tailscale on Linux](https://tailscale.com/docs/install/linux)。

### 第二步：手机安装 Tailscale

在 iOS App Store 或 Android 应用商店安装 Tailscale，用同一个 tailnet 账号登录，并允许它建立 VPN 连接。完整平台入口见 [Tailscale installation](https://tailscale.com/docs/install)。

### 第三步：优先暴露你自己的前端，不直接暴露 relay

假设你的前端在同一台机器的 `127.0.0.1:5173`：

```bash
tailscale serve 5173
```

终端会显示一个只在 tailnet 内可访问的 HTTPS 地址，例如：

```text
https://your-machine.your-tailnet.ts.net
```

手机连着 Tailscale 时打开这个地址。你的前端后端应在服务器内部调用 `http://127.0.0.1:8900/chat_stream`。Tailscale Serve 的当前用法和 HTTPS 前置条件见 [Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve)。

为什么不建议 `AI_RELAY_HOST=0.0.0.0`：那会让局域网设备绕过 Tailscale 直接碰到 relay。relay 本身没有账号系统，而且 agent 可以执行本机工具。保持 relay 监听 `127.0.0.1`，让 Tailscale Serve 或你自己的带鉴权后端做代理更安全。

不要误用 `tailscale funnel`。Funnel 是公开互联网入口；Serve 才是仅 tailnet 内可访问的入口。

### “手机能打开”不等于“手机有锁屏推送”

这个 relay 只负责请求、流式回复、session 续接和引擎切换，不会主动发通知。

如果你希望 AI 主动找你时手机锁屏弹消息，还需要你的前端/业务后端接一种通知通道，例如：

- Web Push（PWA/iOS 16.4+ 或普通浏览器通知）。
- Telegram Bot。
- ntfy、Gotify 等自托管通知服务。

推荐让原有前端保存 push token，并在它判断“AI 有主动消息”时发送通知；不要把通知账号、手机 token 或用户数据库塞进 relay。relay 保持单一职责，后续升级最省事。

## 中文：还推荐配什么

- systemd user service：崩溃自动拉起、开机自启；`install.sh` 在 Linux 上会尽量自动配置。
- Tailscale：手机/笔记本远程访问，避免公网开端口。
- 你原有前端的持久聊天记录：relay 只保留有限的切换 handoff，不是聊天数据库。
- MCP：记忆库、Home Assistant、浏览器等工具继续配置在 Claude/Codex 自己的 MCP 配置里；relay 不接管 token。
- 状态备份：备份 `~/.local/state/ai-session-relay/` 可以保留指针与切换状态，但不要公开；真正的 Claude/Codex transcript 仍由各 CLI 自己存放。
- 日志轮转：长期运行时给 systemd/journald 或你自己的日志系统设置保留上限。

## 中文：给接入前端的人

这个库没有前端。现有前端只需要接一个 `POST http://127.0.0.1:8900/chat_stream`：

```bash
curl -sN http://127.0.0.1:8900/chat_stream \
  -H 'content-type: application/json' \
  -d '{"text":"你好"}'
```

这是 NDJSON，不是一个完整 JSON。前端要逐行解析；看到最后一条 `done: true` 后，以 `full` 为权威全文。不要把所有 `delta` 和 `full` 再拼一次，否则正文会重复。

浏览器前端不要直接跨域调用 relay；本项目故意没有打开全网 CORS。推荐由你已有的后端同机调用 relay，再把 SSE/WebSocket/NDJSON 转给浏览器。

## 给 AI 的排雷记录

实现级故障、恢复不变量和不要重复踩的坑集中在 [`docs/TROUBLESHOOTING_FOR_AI.md`](docs/TROUBLESHOOTING_FOR_AI.md)。这份文档面向接手代码的 AI/开发者，直接使用技术术语。

## License

AGPL-3.0-or-later. Copyright © 2026 blanche.x. Extracted from the production-proven session continuity engine in the original project, with private identity, infrastructure, and memory-system coupling removed.
