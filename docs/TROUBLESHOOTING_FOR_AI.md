# Troubleshooting for AI maintainers

本文件记录实现不变量、已验证失败模式和恢复策略。面向接手代码的 AI/开发者，不是新人教程。

## 系统边界

- 单实例、单用户、单活动 turn。`relay_server._TURN_LOCK` 串行化两种引擎；不要在未设计 per-user state/locks 前改成多租户。
- relay 不拥有完整聊天数据库。`relay_state.json.history` 仅是有界切换 handoff；权威聊天历史应由调用方保存。
- Claude transcript 由 Claude Code 自己落在其项目会话目录；Codex thread 由 `CODEX_HOME` 持久化。relay 只保存指针。
- 对外契约是 NDJSON。流中 `delta`/activity 用于实时展示；最后一条 `done=true` 的 `full`/`parts`/usage 是权威收口。

## Claude Code session continuity

### cwd 是 session 身份的一部分

- Claude Code transcript project dir 由 cwd 派生。`AI_RELAY_WORKSPACE` 改路径后，即使指针 SID 不变，也可能表现为 session 不存在或“失忆”。
- systemd `WorkingDirectory`、`AI_RELAY_WORKSPACE`、`run.sh` 的 `cd` 必须指向同一个稳定目录。
- 不要用临时目录、部署版本目录或每次 release 都变化的 symlink target 作为 workspace。

### 指针写入必须原子化

- `headless_last_session` 用临时文件 + `os.replace`。不要改成直接覆盖；进程崩溃会留下半截 SID。
- 指针丢失时，`_fallback_resume_sid` 会从健康 transcript 选择最近会话。选择依据不能只看文件 mtime：一次失败 resume 可能 bump mtime，把旧/坏 session 永久推到最新。
- `.forged-*`、不可 resume、超 `resume_max` 的 session 必须从 fallback 候选排除。

### 新 SID 必须是标准带连字符 UUID

- Forge 生成 SID 必须使用 `str(uuid.uuid4())`。
- `uuid.uuid4().hex` 在部分 Claude Code 版本会被 `--resume` 以 “is not a UUID” 拒绝，导致裁剪成功但新 session 永远续不上。

### 不要在 result 事件到达后立即杀子进程

- `claude -p --output-format stream-json` 可能先发 result，再异步完成 transcript 收尾。
- 立即 `terminate/kill` 会出现“当前回复客户端看到了，但 assistant turn 未写进 transcript”；下一轮 resume 看不到上一条回答，表现为重复打招呼、复述、上下文断层。
- 保留有界 exit grace；超时后再清理。

### stdout/stderr 必须同时排水

- stdout 是 NDJSON；stderr 可能持续输出诊断。只读 stdout、不 drain stderr，管道写满后子进程可假死。
- 解析失败的单行应跳过并记录，不能让整个 turn 因一个非 JSON 诊断行崩掉。

### 空退只允许静默重试一次

- Claude CLI 在额度瞬态、session 锁、CLI 抖动下可能 EOF 且无正文；同 SID 重发一次已验证可自愈。
- 只有尚未向客户端 yield text/reasoning，且 transcript salvage 也没有本轮完整输出时才允许重试。
- 已 yield 任意可见正文、reasoning 或可能产生工具副作用后禁止重放；否则会重复回答、重复写文件、重复调用外部工具。
- `part_break`/activity 本身不等于正文，但工具 activity 可能代表副作用已发生；改重试条件时必须保守。

### salvage 必须验证 user turn 边界

- 从 transcript 救回复前，末尾真实 user 文本必须与本轮 prompt 匹配；否则可能把上一轮 assistant 输出误当本轮。
- 切换到 Claude 时 relay 会把 handoff 与 current message 合成一个 prompt；salvage 比较必须使用实际送入 Claude 的完整 prompt。

### thinking flag 不是稳定公共契约

- `--thinking-display summarized` 在部分版本可用，但不应视为长期公共 CLI 保证。
- 当前实现检测 unknown/rejected flag，关闭该 flag 后静默重试一次；删除这条自愈会把 CLI flag 变更伪装成“额度用完/整轮没出声”。
- reasoning summary 是可选显示通道；不能影响正文成功路径。

### Forge 必须在干净 round 边界裁

- 不允许从任意 JSONL index 切。必须保留完整 user → assistant/tool chain，且保持 `tool_use` / `tool_result` 配对。
- system、summary、file-history、progress、synthetic 等非真实对话事件不能计成 user round。
- base64 图片/sidecar 会让 raw 文件体积远大于模型 token usage；触发判断同时考虑 usage、raw 估算和轮数。
- ForgeNoProgress 不能继续原 SID 无限重试。应归档坏/肥 SID，fallback 到健康候选或全新 session。
- 运行时从 `result.modelUsage.contextWindow` 学模型窗口并写 `model_windows.json`。未知模型使用保守窗口；不要硬编码未公开模型 slug。
- retain 目标按完整轮数，token 预算作为安全帽。只按模型窗口 token 比例会导致切模型时灾难性重裁。

### 不要把可变注入反复烙进 retained transcript

- 上层若每轮在 user 文本中拼时间、状态或检索结果，Forge 必须识别边界并只保留真实 user 文本；否则过期注入会永久占 retain 预算并污染未来上下文。
- 本仓 relay 的 handoff 只在切换当轮注入，不应每轮重复。

## Codex thread continuity

### app-server 握手顺序不可变

- 每条 stdio 连接只 `initialize` 一次，然后发送 `initialized` notification，再调用 thread/turn 方法。
- experimental capability 或 reasoning-summary config 被未来版本拒绝时，应回落稳定握手/裸 resume；不能为了拿 summary 丢 thread 上下文。

### response、notification、server request 三类消息必须分流

- 带 `id` + `result/error`：完成 `_pending[id]`。
- 带 `id` + `method`：server→client request，需要响应；不能塞进 turn notification queue。
- 只有 `method`：notification，路由到当前 turn queue。
- request id timeout/完成后必须从 `_pending` 删除；否则 future 泄漏并可能把后续 response 错路由。

### 审批响应 shape 不统一

- exec/command/patch/file-change 请求使用 `{"decision":"approved"}`。
- `mcpServer/elicitation/request` 使用 `{"action":"accept","content":{}}`。
- shape 发错会被服务端视为拒绝或一直挂起。
- 当前项目是 trusted-local autonomous 模式，`approvalPolicy=never` 并自动批准。若改安全模型，需要同时设计远端审批协议，不能只关 `_auto_approve`，否则工具 turn 会无期限等待。

### resume 失败后不要在同一 stdio 连接继续 thread/start

- 已捕获失败链：`thread/resume` timeout/failure 后，同一持久 app-server 上紧接 `thread/start` 也 timeout。
- 这不是普通“坏 thread”单点问题；失败请求可能已污染连接/reader/pending 状态。
- 正确恢复：条件清理坏 pointer → close/kill 当前 app-server → 新建连接并重新 initialize → silent turn 重试一次。
- transport timeout/EOF 不是“摘要配置 schema 被拒”。前者只等一次有界 `CODEX_RESUME_TIMEOUT` 后回收；只有收到明确 RPC error 时，才允许去掉可选 config 裸 resume 一次。

### rollout 完成不代表 RPC response 通道健康

- Codex app-server 可能已经接收并完成 `turn/start`、把 `task_complete` 写入 rollout，却漏掉对应 RPC ack 和 turn notifications。
- 每轮发送前记录该 thread rollout 的 byte offset；live queue 静默时只读 offset 后的新记录。必须同时看到 `task_complete` 和 final `output_text` 才能收口，且绝不读取 raw/encrypted reasoning。
- 一次完成态回收后进入 rollout-only：后续 `turn/start` 只发送一次，立刻观察 rollout，不先白等通用 RPC timeout。明确 RPC error 仍立即失败；既无 ack 又无 rollout 新字节满 `CODEX_ROLLOUT_START_TIMEOUT` 才回收。
- rollout-only 的未决 response future 在成功、异常和取消路径都必须清理，避免迟到 response 污染下一轮。

### 清坏指针必须防竞态

- `_clear_thread_ptr(expected)` 删除前重新读取当前值；只有仍等于 expected 才删。
- 不带 expected 的盲删可能删除另一协程刚写入的新 thread pointer。

### Codex 重试闸以“turn 是否可能已发送”为硬边界

- `stream_codex_turn` 只有在 `turn/start` 尚未尝试写出时，才允许新进程重试一次。
- 一旦尝试写出 turn，即使还没有任何 delta/activity，也必须假设模型或工具可能已执行，禁止重放。
- app-server EOF 时要 fail 全部 `_pending` future；否则调用者一直等到各自 timeout。

### token usage 必须取 last，不取 total

- `thread/tokenUsage/updated.tokenUsage` 常见形状含 `total`（整个 thread 累计）和 `last`（本轮）。
- UI/计费面板的本轮 usage 必须优先 `last`；取 `total` 会随着 thread 增长，看起来每轮越来越贵。
- `modelContextWindow` 位于 tokenUsage 外层，映射时单独保留。

### 只展示公开 reasoning summary

- 可展示 thinking 只来自公开 summary 事件，如 `item/reasoning/summaryTextDelta`。
- 不读取或转发 raw reasoning/chain-of-thought。
- commentary 是普通公开 agent message phase，与 reasoning summary 分通道，不能混标。

### 一个 app-server 连接一次只跑一轮

- `_turn_lock` 与单 `_notif_q` 是配套设计。去掉锁后两个 turn 的 notification 会串流。
- 若要并发，必须一用户/一 thread 建独立连接或实现按 turn/thread id 完整路由，不能共享当前单 queue。

## Claude ↔ Codex switching

### epoch 只在 provider 真变化时递增

- 同 provider 重复选择不 bump epoch；否则每轮都会被误判成新任期，Codex 不断创建新 thread。
- `relay_state.json` 是 epoch 权威；Codex 另存已接收 epoch，用 `incoming > current` 判定是否空窗接班。

### POST /provider 必须保留 pending_switch

- 切换 endpoint 与下一条 chat 是两个请求。只修改 provider 而不保存 pending 标志，会导致下一条看到“provider 已相同”，从而漏掉 handoff。
- `pending_switch` 在下一条 turn 组装 handoff 后清零，并与 user message 同一次原子状态写入。

### 两侧接班方式不对称是刻意的

- Codex：切入新任期时 `thread/start` 干净 thread，再用 `thread/inject_items` 注入 handoff，不触发模型乱回复。
- Claude：CLI 没有等价的独立 inject-items 路径，切入当轮把 handoff 前置到 current user prompt。
- 不要强行让两边调用完全同形状；保持对外 NDJSON 同形即可。

### handoff 必须有界且可被调用方覆盖

- 本地 history 只保留最近 `AI_RELAY_HISTORY_MESSAGES`，文本再受 `AI_RELAY_HANDOFF_CHARS` 限制。
- relay history 不是权威数据库；调用方有完整历史时应在切换请求传 `handoff`。
- handoff header 必须明确“作为最近背景、不要复述/宣布”，否则模型容易先总结交接而不是自然续聊。

### 状态落盘与 assistant 收口

- user message 在启动 turn 前落盘，防进程中途死掉后完全丢输入。
- assistant 只在拿到权威 done.full 后追加。异常中只有零散 delta、没有 done 时，不把不完整文本伪装成完整历史。
- 状态文件写入使用 temp + replace；不要直接 truncate。

## Network and deployment

- 默认只监听 `127.0.0.1`。relay 无 HTTP auth；不要直接绑定公网或 LAN。
- 手机访问优先暴露现有前端，并让前端后端在同机调用 relay。不要让浏览器跨域直连 relay；本仓刻意不启 CORS。
- Tailscale Serve 代理 loopback 服务时，保持后端只监听 loopback。不要切换成 Funnel；Funnel 是公网暴露。
- Tailscale 只解决连通性，不产生 Web Push/锁屏通知。主动通知应由业务后端/Web Push/Telegram/ntfy 等独立通道负责。
- systemd user service 在无人登录的 headless Linux 上需要 linger；否则重启后服务可能直到用户登录才起来。
- `Restart=on-failure` 能抓进程退出，抓不到逻辑 wedge；Codex wedge 由进程内 recycle 处理，Claude 空退由单轮重试处理。

## Diagnosis order

1. `GET /health`：CLI 是否在 PATH、当前 provider/epoch 是否合理。
2. 查看 systemd status/journal，确认 workspace/state env 与手工运行一致。
3. 检查 `relay_state.json`、Claude/Codex 指针是否可写、是否指向已存在会话。
4. 单独运行 `claude -p ... --output-format stream-json --verbose` 或 Codex app-server smoke，区分 CLI/auth 与 relay 映射故障。
5. 若 Codex resume 连带 thread/start timeout，回收 app-server，不要只删 thread pointer 后原连接重试。
6. 若 Claude 当前回复可见但下一轮忘记，优先检查 result 后过早 kill、cwd 漂移、transcript assistant turn 未落盘。
7. 若正文重复，检查调用方是否同时拼了 delta 和 done.full，或引擎是否在已输出后错误重试。
8. 若切换后没拿到交接，检查 epoch、pending_switch、handoff 长度与 provider 是否真的变化。
