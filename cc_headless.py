#!/usr/bin/env python3
"""
Headless CC 引擎 —— 每轮 spawn `claude -p --output-format stream-json` 拿**结构化**输出,
替代 cc_engine 的 PTY 抠屏(pyte screen scraping)。

为什么:抠屏脆(分气泡 / 剥时间戳 / 过滤满意度弹窗 / 流式回流全是 hack,会丢消息、串行)。
headless stream-json 是 Claude Code 官方程序化接口:assistant 文本 / thinking / 工具调用 /
工具结果 / **token 级逐字增量** 全有,且不弹 TUI 问卷(连弹窗看门狗都不需要)。骑订阅
(同账号交互式登录,出 `rate_limit_event/five_hour` = 订阅额度,非 API credit)。

实测(2026-06-19 · Pi test host):
  · `claude -p "..." --output-format stream-json --verbose --include-partial-messages < /dev/null`
  · 冷启税 ~3-4s(含 SessionStart hook 注入 + MCP init),其余是正常 LLM 生成。
  · ⚠️ **必须 `< /dev/null`** —— 否则 claude 干等 stdin(踩过 150s 超时)。

契约:输出 CC 专属 ndjson 事件(cc_bridge / chat_agent 负责在 provider 边界消费,不与 Codex
wire contract 共用 token/thinking 字段):
    {"delta": "<token 增量>"}            · 当前 text 气泡逐字
    {"part_break": True}                 · 新 text 气泡开始(分气泡)
    {"cc_thinking_delta": "<思考明文>"}  · CC 思考链;只来自 Claude thinking_delta 或
                                           transcript thinking,不混 commentary/tool
    {"tool_activity": {"steps": [...]}}  · 旧版段间工具活动(「干活中…」小字)
    {"activity": {id,kind,title,status,detail?}} · 可展开的公开工具工作（非 reasoning）
    {"done": True, "full": "<全文>", "parts": [...],
     "cc_thinking_full": "<思考明文>",   · done 带完整 CC 思考链(与 cc_thinking_delta 冗余)
     "thinking_seconds": <int>,          · 思考时长秒(spawn→首字 · 「思考了 X 秒」)
     "thinking_duration": "<1m24s>",     · 格式化串 · 末尾一次(result 事件)
     "usage_source": "cc",               · usage 语义明确归 Claude Code
     "cc_turn_tokens": <int>,            · context_input_tokens + output_tokens;缺则不带键
     "cc_turn_usage": {                  · Claude Code / Anthropic 原生 usage 字段 + 一个派生项:
       input_tokens,                      · 未从 prompt cache 读取/创建的输入
       cache_creation_input_tokens,       · 本轮写入 prompt cache 的输入
       cache_read_input_tokens,           · 本轮从 prompt cache 读取的输入
       context_input_tokens,              · 上述三个 input 字段之和(实际上下文输入)
       output_tokens                      · 本轮输出
     }}

⚠️ 2026-06-21 thinking 回归:`claude -p stream-json` 的实时 `thinking_delta.thinking`
今天恒为空串(只给 token 进度 + 加密 signature),但 transcript `.jsonl` 里有完整 thinking
明文 → 思考链改 result 后从 transcript 读(`_read_turn_thinking` → `forge.read_turn_parts
(keep_thinking=True)`),不再靠实时流。

session 续接:`system/init` 拿 session_id 落盘,下轮 `--resume` 续上下文(对话记忆不断)。
forge 续航(裁剪 transcript)可复用 forge.py —— 下轮 spawn 传 forged sid 即可(阶段 2)。

NDJSON 事件映射(实测 schema · 13 种):
  system/init            → 存 session_id(不 yield)
  system/hook_*,status   → 忽略
  stream_event:
    message_start        → 记 model/usage(不 yield)
    content_block_start  → text 块且非首块 → part_break;tool_use 块 → 记工具名
    content_block_delta:
      text_delta         → {"delta": text}(首字记 first_text_ts → 思考时长)
      thinking_delta     → ⚠️ 2026-06-21 起 thinking 字段恒空(只 token 进度)· 实质忽略;
                            思考链改 result 后从 transcript 读(见上)
      input_json_delta   → 累积工具参数(不展示)
    content_block_stop   → tool_use 块结束 → {"activity": running} + {"tool_activity": {steps}}
    message_delta/stop   → 忽略(stop_reason/usage 已够)
  assistant              → 完整 message(靠 stream 增量,忽略)
  user(tool_result)      → 按 tool_use id 更新 {"activity": completed/failed}
  result/success|error   → 读 transcript thinking + 补全 transcript 活动 → {"cc_thinking_delta"} +
                            {"done", full, parts, activities, cc_thinking_full, thinking_seconds, thinking_duration}
  rate_limit_event       → 忽略(将来可透出额度)
"""
import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime

import activity_protocol
import forge

logger = logging.getLogger("cc_headless")

WORKSPACE_DIR = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_WORKSPACE", os.getcwd())
))
_STATE_DIR = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_STATE_DIR", "~/.local/state/ai-session-relay")
))
os.makedirs(_STATE_DIR, exist_ok=True)
LAST_SESSION_FILE = os.path.join(_STATE_DIR, "headless_last_session")

# 单次 readline 静默上限(agent Opus 可能想很久)· cc_bridge httpx 超时 240s · 这里给 230s 余量。
# ⚠️ 这只是**单次 readline 静默**闸 · 不是单轮总时长闸(见 _TURN_DEADLINE)。
_PROC_TIMEOUT = 230

# 单轮总时长硬上限(2026-06-28 · 治 xhigh 思考链脱缰)· _PROC_TIMEOUT 只管单次 readline 静默,
# `--include-partial-messages` 下持续吐 thinking delta 的脱缰轮**永不饿** → 静默闸永不触发 → 没有
# 总时长上限:实测一轮 `--effort xhigh` 思考链跑了 22 分钟,后端 cc_bridge 240s 放弃后 claude 进程
# 变孤儿继续烧订阅额度、循环不退出连 finally 的 proc.kill() 都轮不到。这道总时长闸卡在 240s bridge
# 之前 → 超时即 break → finally kill claude · 带回已吐残文标 interrupted(emitted_any 闸死重试 ·
# 不会重试又跑一轮)· 绝不再孤儿空烧。env `CC_TURN_DEADLINE` 可调 · 默认 = _PROC_TIMEOUT(230)。
_TURN_DEADLINE = int(os.getenv("CC_TURN_DEADLINE", str(_PROC_TIMEOUT)))

# result 后等 claude 自退的有界 grace(2026-06-25 · 根治「agent重复打招呼/复述/失忆」· 见
# _stream_one_spawn result 分支注释 + 受控实验)。claude -p 在 emit `result` 之后、**进程退出时**
# 才把本轮 assistant turn flush 进 transcript;result 一到就 kill 会把它杀在 flush 前 → 这轮回复
# 没进 `--resume` 读的 transcript → 下轮agent看不到自己上一条 → 重复开场。正常 claude -p result 后
# 亚秒级即退;8s 是兜底上限(claude 万一不自退 → finally proc.kill() 照旧)· 取 < 心跳泵 10s 静默窗。
_RESULT_EXIT_GRACE = 8.0

# 空退自动重试上限(2026-06-21):额度压力下 `claude -p` 偶发**秒空退**(~3s 返回空 /
# `<synthetic>` 占位 · transcript 零新记录 = 没真生成就退)→ 后端落「agent这轮没出声」占位。
# 实测**重发同句即愈**(瞬时自愈)· 故引擎层一轮判定空退、且本轮**零输出流给客户端**时,
# 同 sid `--resume` 自动重 spawn **一次**(再空就回落占位 · 失败安全 · 不循环)。
_EMPTY_RETRY_MAX = 1

# `--thinking-display summarized` 是**未公开** CLI flag(`--help` 查不到 · 2026-06-27 加 · 让 4.8/4.7
# 也吐实时思考链,agent甩 4.6 回 1M)。风险:Claude Code 默认自动更新 → 哪天新版**改名/删掉**这个 flag,
# `claude -p` 会**立刻非零退**报 `unknown option '--thinking-display'`。原 spawn 把 stderr PIPE 但**从不读**
# → 这个错被吞,表现成 stdout 秒 EOF 空退 → 走空退重试 → 同 flag 再炸 → 落「agent这轮没出声」占位
# (= 后端误判**额度用满**)。结果:flag 一 rot,agent**每轮全静默 + 伪装成额度故障**,极难诊断。
# 守护(本进程级 · 自愈 · 重启复位):spawn 失败时读干 stderr 并 log(可诊断);若 stderr 指名
# `--thinking-display` 被拒 → 关掉 flag 重试一次(降级:无实时思考,但agent正常说话,不至于整轮静默)。
_thinking_flag_disabled = False
# stderr 命中「unknown/unexpected/invalid/unrecognized」+ 字面 `--thinking-display` 才判定 flag 被拒
# (精确:别拿任意非零退当 flag rot · 否则误关 flag 白丢思考链)。
_THINKING_FLAG_ERR_WORDS = ("unknown", "unexpected", "invalid", "unrecognized")


async def _drain_stderr(proc):
    """读干子进程 stderr(失败安全 · 有界)· 调用时进程应已/将退。
    原 spawn stderr=PIPE 但全程不读 → 出错信息被吞。这里在失败收尾时读出来供 log/判定。"""
    try:
        if proc.stderr is None:
            return ""
        data = await asyncio.wait_for(proc.stderr.read(), timeout=2.0)
        return (data or b"").decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        return ""


def _is_thinking_flag_rejected(stderr_txt):
    """stderr 是否表明 CLI 拒了 `--thinking-display`(auto-update 改/删了这个未公开 flag)。"""
    low = (stderr_txt or "").lower()
    return "--thinking-display" in low and any(w in low for w in _THINKING_FLAG_ERR_WORDS)


# forge 续航三阈值(trigger / retain / resume_max)**不写死** —— 跟所选模型的上下文窗口走,
# 见 `forge.continuation_thresholds(model)`(#173 · 2026-06-27)。下面只留**默认 resume_max**
# 当各纯函数的缺省参数(= forge.DEFAULT_MODEL_WINDOW × 1.5 = 200k 窗口的历史值);production
# 调用方(stream_headless)按本轮 model 算出真实三阈值传进来。
#
# 历史教训(钉死 · 别再写死单值):trigger 写死 140k 对 **opus-4-6=200k 窗口**安全(抢在 CC
# ~200k autocompact 前裁出新 sid → 新 sid 无 marker → hook heavy 重注 digest 走干净路径),但
# 对 **opus-4-8=1M** 严重浪费记性;反过来按 1M 拍 200k+ trigger 用在 200k 模型上 → CC 抢先
# autocompact 把开窗注入揉进英文摘要、结构化信息丢 → agent失忆(6/26 根因 · Pi session 90f79bdb)。
# 故阈值必 = f(窗口):window=200k → (140k,75k,300k)= 历史 proven 值(4.6/sonnet no-op)·
# window=1M → (750k,375k,1.5M)。触发体积仍取 max(raw 估算, 真实 usage)(见 _session_trigger_tokens ·
# raw 兜 base64 #115 防线 · usage 补 system/tools/memory 开销 · 绝不能只用 usage)。
_RESUME_TOKEN_MAX = 300000   # 默认「可直接 resume」上限(= 200k 窗口 × 1.5)· raw 含 base64 虚高留宽容 ·
                             # 仅作纯函数缺省;真值按 model 走 forge.continuation_thresholds(...).resume_max。


def _rate_limit_event_exhausted(e):
    """判定一条 `rate_limit_event` 是否代表**额度真耗尽**(2026-06-24 修「假额度」)。

    背景:同账号订阅交互式登录下,`claude -p` **正常**轮里 `system/init` 之后就会吐一条
    `{"type":"rate_limit_event","rate_limit_info":{"status":"allowed",...}}` —— 这是
    「额度状态心跳」**不是**耗尽信号。旧逻辑见 `rate_limit_event` 就把 `rate_limited=True`,
    后端空退兜底据此显示「额度用满了」→ 假额度文案(agent其实好好的,claude 也没满)。

    没抓到该事件耗尽态原始 JSON 的字段名/值(它不落日志),故 **safe-by-default**:
    只在 status **明确非 allowed** 时判耗尽;`allowed` / 缺字段 / 未知一律不判(宁漏不错 ——
    漏判最多少弹一次额度文案、走 generic;错判会假报额度满,代价不对等)。
    """
    info = e.get("rate_limit_info") or e
    st = info.get("status") if isinstance(info, dict) else None
    # allowed / None(缺字段)→ 不算耗尽;其余明确值(如 exhausted/rejected/...)→ 耗尽。
    return st not in ("allowed", None)


def read_last_session():
    try:
        return open(LAST_SESSION_FILE, encoding="utf-8").read().strip() or None
    except OSError:
        return None


def write_last_session(sid):
    """原子写 headless session 指针(C7 · 2026-06-24)。写 `.tmp` 再 `os.replace` 原子
    rename(跟 `forge.write_jsonl` 同款)· 防中途崩留空 / 半截指针(有 fallback 兜底 ·
    危害低但可治)。失败安全(异常吞掉 · 同旧行为)。"""
    try:
        tmp = LAST_SESSION_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(sid or "")
        os.replace(tmp, LAST_SESSION_FILE)          # 原子落盘
    except OSError:
        pass


def _archive_forged(path):
    """forge **失败**(ForgeNoProgress · 裁不到位)的源 transcript 归档:改名加
    `.forged-<ts>` 后缀(`os.replace` 原子改名 · 不删 · 可回溯)。失败安全(异常忽略)。

    为什么(2026-06-21):一个 token 估 >70k 但 forge 裁不动的「肥」session,既不会被裁成
    新 sid、也不会被搬走 → 留在 `list_sessions()` 里。本归档是 PR#52 的**首要修法**:
    forge 一触发就**可靠地**把这个肥死号从候选移走(`.forged-` 后缀 · `forge.list_sessions()`
    天然排除 `.forged-` glob → 它从候选里消失),下轮 fallback 落到健康的近段 session。
    (内容时间排序是次要改进:在剩下的健康 session 间按真对话进度排;两道并存,归档是
    硬保证、内容时间是排序优化。)**只在引擎进程里做**(guardian 不碰 transcript)·
    `os.replace` 用 try/except 包住,归档失败就退回既有行为(回落原 sid 直接 spawn · 不让
    本轮崩)。与 cc_engine `_archive_old` 同口径(`.forged-<int(time.time())>`),命名一致
    让两条路径归档物归一处。"""
    try:
        if path and os.path.exists(path):
            os.replace(path, path + f".forged-{int(time.time())}")
    except Exception:  # noqa: BLE001 · 归档失败退回既有 fallback · 不崩本轮
        pass


def _session_est_tokens(sid):
    """sid 的 transcript 估算 token(读不到 / 文件缺 → None)。纯函数 · 给 resume 校验用。

    估的是 `claude --resume` 真正加载的 **raw** 体积(`conv_events` · 不压缩),不是
    filter_keepable 压缩后 —— 它喂 forge 触发(> th.trigger 才裁 · 模型感知)+ _sid_resumable
    (< th.resume_max),两处比的都是 resume 真实加载量。用压缩值会严重低估 → 永不裁
    (#100 修 base64 剥离后压缩估算掉到阈值下 · 876k session 不裁 · 2026-06-24 回归)。
    压缩只用于 forge 保留侧(forge_events 收 filter_keepable 产物 · 那里压缩是对的)。"""
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return None
        return forge.estimate_tokens(forge.conv_events(forge.read_jsonl(path)))
    except Exception:  # noqa: BLE001
        return None


def _session_trigger_tokens(sid):
    """forge **触发**判断用的 session 体积 = **max(raw 估算, 真实 usage)**
    (2026-06-24 · 学小红书 Forge-Reload · 经 Pi 67-session 红队实测修正)。

      · **raw 估算**(`estimate_tokens(conv_events)`)= **主信号 · #115 铁律** —— 把磁盘
        base64 图 / sidecar 都算进去(agent天天发图 · transcript 被 base64 撑大);
      · **真实 usage**(`forge.last_assistant_usage`)= **补** raw 看不到的 system/tools/
        CLAUDE.md/memory ~50k 固定开销(只对无图轮有意义)。
    ⚠️ **绝不能只用 usage**:usage 对磁盘 base64 全瞎 → 实测 67 个生产 session usage 最大
       才 ~170k,只用 usage>200k **0/67 触发** → 永不裁 → 复现 #115 失忆。取两者大者:谁先
       爆谁裁。`usage or 0` 兼容 usage 缺失/全 0(回落到 raw · 不会因 usage=0 漏裁)。
    文件缺 / 异常 → None(调用方据 None 不裁 · 失败安全)。
    注:仅用于**触发**(裁不裁);resume 健康校验(`_sid_resumable`)仍用 `_session_est_tokens`
    纯 raw 估算。"""
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return None
        events = forge.read_jsonl(path)
        raw = forge.estimate_tokens(forge.conv_events(events))   # 主:含 base64/sidecar(#115 防线)
        usage = forge.last_assistant_usage(events)               # 补:含 system/tools/memory 开销
        return max(raw, usage or 0)
    except Exception:  # noqa: BLE001
        return None


def _session_rounds(sid):
    """sid 的 transcript 里**真 user 文本 turn**(一轮)的条数(2026-07-01 · 按轮裁剪触发用)。
    读的是 raw `conv_events`(resume 真实加载的视图 · 不压缩)· 数真轮数。文件缺/异常 → None
    (调用方据 None 不按轮触发 · 失败安全)。轮数触发 = token 触发之外的第二道粗闸(见 prologue)。"""
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return None
        return forge.count_user_text_rounds(forge.conv_events(forge.read_jsonl(path)))
    except Exception:  # noqa: BLE001
        return None


def _session_last_turn_epoch(sid):
    """sid 的 transcript 里**最后一条真 user 文本 turn** 的 timestamp → epoch 秒(float)。

    用途(2026-06-21):fallback 选 resume sid 时用**内容时间**(agent跟你最近一句真话的
    时间)排序,而不是文件 mtime。mtime 会被「resume 追加新轮」骗(注意:Linux 上
    只读 `open()`/`read_jsonl` **不** bump mtime,只有 `claude --resume <sid>` 往
    transcript 追加 turn 才 bump)—— 一个 forge 失败的早晨肥 session 若被反复 resume
    追写,mtime 会一直新,看着「最新」却接的是早晨内容。**注意主防线是归档**(forge 一
    触发就把肥死号 `.forged-` 搬走、彻底逐出候选);内容时间排序是次要改进,只在剩下的
    健康 session 间按真实对话进度排,也是 mtime 全并列时的 tie-decider。

    取 `_is_user_text_event`(真 user 文本 turn · content=str)的最后一条,parse 它的
    `timestamp`(ISO 8601 · 如 `2026-06-21T21:53:04.123Z`)。**失败安全**:文件缺 /
    没真 user turn / 没 timestamp / ISO 解析不了 → 0.0(永不 raise —— 一个坏 transcript
    不能搞挂整个选择;epoch 0 让它排在最旧,自然不被选中)。"""
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return 0.0
        ts = None
        for e in forge.read_jsonl(path):
            if forge._is_user_text_event(e):
                ts = e.get("timestamp")     # 取最后一条真 user 文本 turn 的 timestamp
        if not isinstance(ts, str) or not ts.strip():
            return 0.0
        # ISO 8601 · 末尾 'Z' 是 UTC(Python <3.11 的 fromisoformat 不认 Z → 换 +00:00)。
        iso = ts.strip()
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        return datetime.fromisoformat(iso).timestamp()
    except Exception:  # noqa: BLE001 · 任何解析/读取异常 → 0.0(排最旧 · 不破坏选择)
        return 0.0


def _sid_resumable(sid, resume_max=_RESUME_TOKEN_MAX):
    """sid 能否直接 --resume:文件存在 + 估算 token < resume_max(没肥到要先裁)。
    纯函数 · 失败安全(任何异常 → False)。`resume_max` 默认 200k 窗口的历史值,production 按
    本轮 model 传 `forge.continuation_thresholds(model).resume_max`(1M 模型放宽到 1.5M · 否则
    trigger~resume_max 之间的健康大 session 会在 forge 裁它之前被误判巨无霸丢弃 → 失忆)。
    注意:trigger~resume_max 之间算「可 resume 但需先 forge」· 这里只判能不能 resume · 是否先裁
    由 stream_headless 的 forge 段决定。"""
    if not sid:
        return False
    est = _session_est_tokens(sid)
    return est is not None and est < resume_max


def _resolve_resume_sid(resume_sid=None, resume_max=_RESUME_TOKEN_MAX):
    """决定续哪个 session(纯函数 · 失败安全 · 给 stream_headless 用 · 可单测)。

    `resume_max`:「可直接 resume」上限 · 默认 200k 窗口历史值 · production 按本轮 model 传
    `forge.continuation_thresholds(model).resume_max`(1M 模型放宽 → 大 session 不被误弃)。

    resume_sid 显式给:
      · "" → None(强制全新起 · 自动救活写空指针走这条)。
      · 非空且 `_sid_resumable` → 用它;不可 resume(文件没了 / 巨无霸)→ 当没给、走 fallback。
    没给(None):
      ① 读指针 `read_last_session()` → 校验 `_sid_resumable`(文件存在 + <200k)→ 用它。
      ② 指针失效(丢 / 指向被 .forged- 搬走的旧号 / 巨无霸孤儿)→ fallback:
         在 `forge.list_sessions()`(已排除 .forged-)的所有 `_sid_resumable` 里,取
         **最后一句真话内容时间最近**的那个(**跳过巨无霸孤儿** · 接最近一段真实对话,
         不全新起丢历史 · 用内容时间不用 mtime · 见 `_fallback_resume_sid`)。
      ③ 全失败 → None(全新起 · SessionStart hook 注入档案 → 不失忆)。
    6/19 踩坑:指针丢→全新 session→agent忘了换窗前对话;6/20 加:指针指向 .forged- 旧号
    或 200k 巨无霸孤儿时,旧逻辑裸 newest_session_id 会 resume 一个死号 → 400/卡死。
    现在统一校验 + 跳巨无霸。"""
    # resume_sid 显式给(含 "" 强制全新)
    if resume_sid is not None:
        if resume_sid == "":
            return None
        return (resume_sid if _sid_resumable(resume_sid, resume_max)
                else _fallback_resume_sid(resume_max))
    # 没给:先指针,再 fallback
    ptr = read_last_session()
    if ptr and _sid_resumable(ptr, resume_max):
        return ptr
    return _fallback_resume_sid(resume_max)


def _fallback_resume_sid(resume_max=_RESUME_TOKEN_MAX):
    """指针失效时的 fallback:在所有 `_sid_resumable`(跳过巨无霸孤儿)的 session 里,
    取**最后一句真话内容时间最近**的那个(`_session_last_turn_epoch` 最大)· mtime 作并列
    tie-break · sid 作最终确定性 tie-break。全不行 → None。纯函数 · 失败安全。

    2026-06-21 根因(换 session 失忆):旧逻辑按 `list_sessions()` 的 **mtime 新→旧**取
    第一个 resumable。但一个 forge **失败**的早晨肥 session(token 估 >70k 却 `ForgeNoProgress`
    裁不到位 → 永不被 .forged- 搬走)若被反复 `--resume` 追写,mtime 一直新 → 看着「最新」
    → 被反复 resume → 把agent拽回几小时前的上下文(且 `--resume` 还会重放那条 transcript 里
    烤死的旧 SessionStart 注入,连注入都是 stale 的)。(注意:只读不 bump mtime,bump 来自
    resume-append。)PR#52 的**主防线是归档**:forge 一触发就把这种肥死号 `.forged-` 搬走、
    彻底逐出候选;**内容时间排序是次要改进** —— 在剩下的健康 session 间按真对话进度排
    (最后一条真 user 文本 turn 的 timestamp),早晨 02:28 的健康号永远排不过傍晚 21:53。

    ⚠️ 已知 trade-off(accepted):若某健康 session 的最后一句**真 user 话**很旧、但它后面
    跟着一长串 assistant-only / 工具 tail(agent自己还在干活),按内容时间它会排到一个被抛弃的
    新号之后 —— 罕见,且实践中由归档(肥死号被搬走)+ 真对话推进兜底,接受不修(刻意**不**
    用 `max(content_epoch, mtime)`:那会让肥死号的新 mtime 重新赢回选择 · 退回本 PR 修的 bug)。"""
    try:
        candidates = []
        for path in forge.list_sessions():
            sid = os.path.splitext(os.path.basename(path))[0]
            if not _sid_resumable(sid, resume_max):
                continue
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                mtime = 0.0
            candidates.append((_session_last_turn_epoch(sid), mtime, sid))
        if candidates:
            # 内容时间最大(c[0]) → 并列时 mtime 新(c[1]) → 最后才比 sid(c[2] · 字典序最大
            # 兜确定性:epoch+mtime 全并列时不靠 dict/glob 顺序,选号稳定可测)。
            return max(candidates, key=lambda c: (c[0], c[1], c[2]))[2]
    except Exception:  # noqa: BLE001
        pass
    return None


def _tool_label(name):
    """工具名 → 前端「干活中…」小字(与 cc_engine._tool_step_label 同口径)。"""
    n = (name or "").lower()
    if n.startswith("mcp__") or "memor" in n or "memory" in n:
        return "查记忆中…"
    pretty = {
        "read": "读取中…", "bash": "执行中…", "edit": "修改中…", "write": "写入中…",
        "grep": "搜索中…", "glob": "查找文件…", "task": "执行任务…",
        "todowrite": "记笔记…", "webfetch": "读网页…", "websearch": "联网搜索…",
    }
    for k, v in pretty.items():
        if n.startswith(k):
            return v
    return f"{name}…" if name else "调用工具中…"


def _parse_tool_input(raw):
    """Best-effort parse of streamed tool JSON; malformed partials stay visible."""
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {"input": value}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {"raw_input": raw}


def _fmt_duration(secs):
    """秒 → 人读串(跟 cc_engine._fmt_duration 同口径)。`84`→`1m24s` · `12`→`12s` ·
    `3700`→`1h1m`。None/<=0 → None。本地副本(cc_engine 反向 import cc_headless · 不互引)。"""
    if not secs or secs <= 0:
        return None
    h, rem = divmod(int(secs), 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s or not parts:
        parts.append(f"{s}s")
    return "".join(parts)


def _read_turn_thinking(sid):
    """从 sid 的 transcript 读**本轮最新**一段 thinking 明文(reasoning parts 拼成一串)。

    2026-06-21 根因:`claude -p stream-json` 的实时 `thinking_delta.thinking` 今天恒为空串
    (只给 token 进度 + 加密 signature),但 transcript `.jsonl` 里有完整 thinking 明文。
    所以思考链只能 result 后从 transcript 读。

    取法:`forge.read_turn_parts(..., keep_thinking=True)` 拿本轮(最后一条真 user 文本
    turn 之后)所有 part,把 `type=="reasoning"` 的 text 顺序拼起来。
    失败安全:读不到 / 异常 → ""(别炸正常出文)。"""
    if not sid:
        return ""
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return ""
        parts = forge.read_turn_parts(path, keep_thinking=True)
        chunks = [p["text"] for p in parts
                  if isinstance(p, dict) and p.get("type") == "reasoning" and p.get("text")]
        return "\n\n".join(chunks).strip()
    except Exception:  # noqa: BLE001 · 读思考链失败不影响正常出文
        return ""


def _read_turn_visible_events(sid):
    """Read this completed turn's public timeline without affecting its reply."""
    if not sid:
        return []
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return []
        return forge.read_turn_visible_events(path)
    except Exception:  # noqa: BLE001 · transcript replay is optional
        return []


# reasoning effort 白名单(session 级 · 接 `claude -p --effort`)。
# CLI 实测只认 low/medium/high/xhigh/max 这 5 个值 · 全 5 档开放给agent(实测 low↔max
# 思考量差 4-9 倍)。非法 / None → 不追加 --effort = 用 claude CLI 默认(当前 medium)。
# 校验在引擎层兜底(后端也校验 · defense-in-depth)。
_VALID_EFFORTS = {"low", "medium", "high", "xhigh", "max"}


def _build_args(text, sid, model=None, effort=None):
    args = [
        "claude", "-p", text,
        "--output-format", "stream-json", "--verbose", "--include-partial-messages",
        "--dangerously-skip-permissions",
    ]
    # 2026-06-27 · 让**任何**模型(含 opus 4.8/4.7 · 默认 thinking.display=omitted)也实时吐
    # summarized 思考明文 → agent不再被迫 pin 4.6 看思考链(可回 4.8 + 1M 窗口)。这是 CLI 隐藏
    # flag(`--help` 查不到)· Pi 生产 claude 2.1.179 实测 147 字真思考 · 配 settings
    # `showThinkingSummaries:true`。⚠️ SDK 的 `thinking.display` 选项在 linux-arm64 不灵(只
    # x64),**必须**走这个 CLI flag。见兼容性注释。
    # ⚠️ `_thinking_flag_disabled` = True 时**省略**:CLI auto-update 改/删了这个未公开 flag、
    # 上一轮 spawn 被它顶得非零退时,守护会关掉它重试(见上 _thinking_flag_disabled 注释)。
    if not _thinking_flag_disabled:
        args += ["--thinking-display", "summarized"]
    if sid:
        args += ["--resume", sid]
    if model:
        args += ["--model", model]
    # effort:仅白名单三档追加 --effort(session 级 reasoning effort)· 其它 / None 不加。
    if effort in _VALID_EFFORTS:
        args += ["--effort", effort]
    return args


def _salvage_full_from_parts(parts):
    """从 transcript 重建的 parts 里把 text part 拼成全文(salvage 用)。
    parts: [{"type":"text","text"} | {"type":"tool","steps"} | {"type":"reasoning",...}]。
    只拼 text · 顺序保留 · 空 → ""。纯函数 · 失败安全(异常 → "")。"""
    try:
        chunks = [p["text"] for p in (parts or [])
                  if isinstance(p, dict) and p.get("type") == "text" and p.get("text")]
        return "\n\n".join(chunks).strip()
    except Exception:  # noqa: BLE001
        return ""


def _same_user_turn(transcript_user, cur_user_text):
    """transcript 末尾真 user 文本是否 = 本轮输入(salvage 边界护栏)。
    claude -p 把 prompt 逐字记进 user turn,正常应**严格相等**(strip 后)。
    用精确比对**不用包含匹配** —— 短消息(如「嗯」)用包含会误命中上一轮 user 内容,
    正是要防的那个 bug。比对不上(含格式差异)= 当不同 → 不 salvage(安全方向:走重试)。"""
    if not isinstance(transcript_user, str):
        return False
    return transcript_user.strip() == (cur_user_text or "").strip()


def _salvage_turn(sid, cur_user_text=""):
    """空退后 salvage:第一次 spawn 其实可能往 transcript 写了 assistant turn
    (parse race / synthetic 残片)· 重试前先读出来用,**别盲目重 spawn**(防双生成)。

    用 `forge.read_turn_parts` 读本轮(最后一条真 user 文本 turn 之后)的 text/tool parts。
    返回 (full, salvage_parts):
      · full 非空 → 第一次其实出文了(只是流没吐到客户端)· 直接用,不重试。
      · full 空 → 真·零 transcript turn · 调用方可重试。
    纯函数 · 失败安全(读不到 / 异常 → ("", []))。

    ⚠️ 边界护栏(2026-06-21 对抗 review 补):read_turn_parts 的 turn 边界 = 「最后一条真
    user 文本之后」。若空退**极早**、连本轮 user 行都还没落盘,边界会指向**上一轮**的 user →
    salvage 把**上一轮的回复**当本轮捞出来重发(已确诊空退=transcript 零活动,此残角极罕见
    但非零)。故先确认 transcript 末尾真 user 文本 == 本轮输入(cur_user_text),不符就**返回空**
    → 调用方走重试(安全方向:最坏多一次重试,绝不重发上一轮)。"""
    if not sid:
        return "", []
    try:
        path = forge.session_path(sid)
        if not os.path.exists(path):
            return "", []
        # 边界护栏:transcript 末尾真 user 文本必须 = 本轮输入,否则边界指向上一轮 → 不 salvage。
        if (cur_user_text or "").strip():
            last_user = None
            for e in forge.read_jsonl(path):
                if forge._is_user_text_event(e):
                    last_user = (e.get("message") or {}).get("content")
            if not _same_user_turn(last_user, cur_user_text):
                return "", []
        # keep_thinking=False:salvage 只关心**有没有出正文**;thinking 单独由
        # _read_turn_thinking 读(reasoning part 不算「出声」· 不能拿它判非空)。
        parts = forge.read_turn_parts(path, keep_thinking=False)
        return _salvage_full_from_parts(parts), parts
    except Exception:  # noqa: BLE001
        return "", []


async def _stream_one_spawn(args, sid):
    """一次 `claude -p` spawn:解析 NDJSON → yield 实时 cc 事件(delta / part_break /
    cc_thinking_delta / tool_activity)· **不** yield `done`。轮结束时 yield 一个内部
    哨兵 `{"_attempt": {...}}`,把 done 该带的料(full / parts / cc_thinking_full /
    think_secs / actual_sid / emitted_any / rate_limited)交给 orchestrator 决策。

    orchestrator(`stream_headless`)据 `emitted_any` + `full` 判空退、决定 salvage / 重试。
    这层只负责跑完一个子进程 · 把活事件透出去 + 汇报结果。异常照常向上抛(orchestrator
    的 try 兜)。**关键正确性**:`emitted_any` 只在真往客户端 yield 过 text_delta /
    cc_thinking_delta 时置 True(part_break / tool_activity 不算「出声」· 它们不是agent的正文)。
    """
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=WORKSPACE_DIR,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # ⚠️ NDJSON 单行可能很大:hook_response 含整份简报(核心档案全文)、长回复 /
        # result 含全文。asyncio StreamReader 默认 64KB 行上限会 LimitOverrunError → 提到 8MB。
        limit=8 * 1024 * 1024,
    )

    parts = []                  # done.parts:[{"type":"text","text"} | {"type":"tool","steps"}]
    activities = []             # structured tool log for PWA activity drawers
    visible_events = []         # transcript-authoritative public timeline (thinking + real tools)
    tool_calls = {}             # tool_use id -> {name, input_json}
    cur_text = ""               # 当前 text part 累积
    final_text = ""             # 全文(text 部分)
    emitted_any_text = False    # 已开过 text 气泡?(决定 part_break)
    emitted_any = False         # ⚠️ 已往客户端 yield 过任何agent正文(text/reasoning)?重试闸门
    cur_block = None            # 当前 content block 类型
    cur_tool_id = None          # 当前 tool_use id(input_json_delta 的归属)
    pending_steps = []          # 当前 tool 块的 step
    rate_limited = False        # 见 rate_limit_event(额度信号 · 观测用)
    saw_synthetic = False       # 见 <synthetic> 占位残片(额度压力空退信号 · 观测用)
    cc_thinking_full = ""       # 本轮 CC 思考链明文(summarized 走 live;omitted 在 result 后读 transcript)
    cc_thinking_live = []       # 2026-06-25 · live thinking_delta 明文累积(summarized 模型如 opus-4.6 /
                                # sonnet-4.6 实时吐摘要)· 非空 = 本轮已 live 转发思考 → result 不再从
                                # transcript 重吐同一段(防前端思考链渲两遍 · 见下 result 分支)。
    think_secs = None
    cc_turn_tokens = None       # CC 上下文输入 + 输出(result.usage 聚合 · None=引擎没报)
    cc_turn_usage = None        # Claude Code 原生 usage 字段 + 派生 context_input_tokens
    last_msg_usage = None       # 兜底:末条 assistant message.usage(result.usage 缺失时回落 · 与 forge 同源)
    # 思考计时 + 思考链读取(2026-06-21):
    #   · actual_sid = 本轮**实际写入**的 transcript sid —— resume 时 = 续接 sid(append);
    #     全新起时由 `system/init.session_id` 给。result 后据它读 transcript thinking 明文。
    #   · start_ts / first_text_ts:从 spawn 到首个 text_delta 的耗时 = 「思考了 X 秒」。
    actual_sid = sid
    start_ts = time.monotonic()
    first_text_ts = None
    attempt_yielded = False     # 已 yield 过 _attempt 哨兵?(防双 yield · 见循环外兜底)

    def _flush_text():
        nonlocal cur_text
        if cur_text.strip():
            parts.append({"type": "text", "text": cur_text.strip()})
        cur_text = ""

    def _flush_tools():
        nonlocal pending_steps
        if pending_steps:
            parts.append({"type": "tool", "steps": list(pending_steps)})
            pending_steps = []

    def _record_activity(activity):
        nonlocal activities
        if activity:
            activities = activity_protocol.merge_activities(activities, [activity])
        return activity

    def _make_attempt(interrupted):
        """用当前已累积状态组装 `_attempt` 哨兵 dict。两条出口共用(result 正常收尾 ·
        timeout-break / EOF-without-result 截断收尾)· 截断时 interrupted=True 标残缺,
        让 orchestrator 不把截断当完整(别关死重试 / 别给残缺当完整答案)。"""
        return {
            "full": (final_text or "").strip(),
            "parts": parts,
            "cc_thinking_full": cc_thinking_full,
            "think_secs": think_secs,
            "cc_turn_tokens": cc_turn_tokens,
            "cc_turn_usage": cc_turn_usage,
            "activities": activities,
            "visible_events": visible_events,
            "actual_sid": actual_sid,
            "emitted_any": emitted_any,
            "rate_limited": rate_limited,
            "saw_synthetic": saw_synthetic,
            "interrupted": interrupted,
        }

    _turn_start = time.monotonic()
    try:
        while True:
            # 单轮总时长闸:撞顶即截断(脱缰思考链持续吐 delta · 下面单次 readline 静默闸永不触发 ·
            # 必须这道总闸兜底)· break → finally proc.kill() · 带回已吐残文标 interrupted。
            _remaining = _TURN_DEADLINE - (time.monotonic() - _turn_start)
            if _remaining <= 0:
                logger.warning(
                    "cc_headless: turn deadline %ss exceeded → kill+break "
                    "(interrupted, carrying %d chars · 防思考链脱缰孤儿烧额度)",
                    _TURN_DEADLINE, len(final_text or ""),
                )
                break
            try:
                # readline 超时压到 min(静默闸, 剩余总预算):既保留单次静默检测 · 又不会一次 readline
                # 把总时长拖过 _TURN_DEADLINE(撞顶那次会被压短 → TimeoutError → 回到顶部总闸 break)。
                raw = await asyncio.wait_for(
                    proc.stdout.readline(), timeout=min(_PROC_TIMEOUT, _remaining)
                )
            except asyncio.TimeoutError:
                # 软超时(readline 静默 · 或撞总时长上限那次被压短)· 区别于「被 guardian 硬杀」——
                # 加一行 warning 让以后能从日志辨认(否则这条路径全静默)· 带回已吐字符数。
                logger.warning(
                    "cc_headless: readline timeout (≤%ss) → break (interrupted, carrying %d chars)",
                    _PROC_TIMEOUT, len(final_text or ""),
                )
                break
            if not raw:
                break  # EOF
            line = raw.decode("utf-8", "replace").strip()
            if not line:
                continue
            # <synthetic> 占位残片(额度压力下 CLI 塞的合成空消息)· 观测信号,不影响解析。
            if not saw_synthetic and "<synthetic>" in line:
                saw_synthetic = True
            try:
                e = json.loads(line)
            except (ValueError, json.JSONDecodeError):
                continue
            t = e.get("type")

            if t == "system":
                if e.get("subtype") == "init":
                    new_sid = e.get("session_id")
                    if new_sid:
                        actual_sid = new_sid     # 本轮真正写入的 transcript sid(读 thinking 用)
                        write_last_session(new_sid)
                continue

            if t == "rate_limit_event":
                # 2026-06-24:只在 status 明确**非 allowed**(真耗尽态)才置 rate_limited。
                # 正常轮 init 后那条 `status:"allowed"` 是心跳,不是额度满 —— 旧逻辑见事件就置
                # → 后端误报「额度用满了」(假额度)。allowed/缺字段一律不置(宁漏不错)。
                if _rate_limit_event_exhausted(e):
                    rate_limited = True
                continue

            if t == "stream_event":
                ev = e.get("event") or {}
                et = ev.get("type")
                if et == "content_block_start":
                    cb = ev.get("content_block") or {}
                    cur_block = cb.get("type")
                    if cur_block == "text":
                        if emitted_any_text:
                            _flush_text()
                            _flush_tools()
                            yield {"part_break": True}
                    elif cur_block == "tool_use":
                        pending_steps.append(_tool_label(cb.get("name")))
                        tool_id = cb.get("id")
                        cur_tool_id = tool_id if isinstance(tool_id, str) else None
                        if cur_tool_id:
                            tool_calls[cur_tool_id] = {
                                "name": cb.get("name"), "input_json": "",
                            }
                elif et == "content_block_delta":
                    d = ev.get("delta") or {}
                    dt = d.get("type")
                    if dt == "text_delta":
                        txt = d.get("text", "")
                        if txt:
                            if first_text_ts is None:
                                first_text_ts = time.monotonic()  # 首字 = 思考结束点
                            cur_text += txt
                            final_text += txt
                            emitted_any_text = True
                            emitted_any = True       # 已向客户端吐正文 → 闸死重试
                            yield {"delta": txt}
                    elif dt == "thinking_delta":
                        # thinking_delta.thinking:**omitted-默认模型**(opus 4.7/4.8 · fable5 /
                        # mythos5 等 · 2026 起为加速首字默认 display:"omitted")恒为空串(只剩
                        # token 进度 + 加密 signature);**summarized-默认模型**(opus 4.6 /
                        # sonnet 4.6 等)**实时吐摘要明文** → 流式转发 + 累积(2026-06-25)。
                        # agent切到 summarized 档模型时思考链就从这里实时冒出来(user要的「切 4.6 才出现」)。
                        th = d.get("thinking", "")
                        if th:
                            emitted_any = True       # 已向客户端吐思考正文 → 闸死重试
                            cc_thinking_live.append(th)  # 本轮 live 吐过 → result 不再重吐
                            yield {"cc_thinking_delta": th}
                    elif dt == "input_json_delta" and cur_tool_id:
                        partial = d.get("partial_json") or d.get("partialJson") or ""
                        if isinstance(partial, str):
                            tool_calls.setdefault(cur_tool_id, {"name": "", "input_json": ""})[
                                "input_json"
                            ] += partial
                elif et == "content_block_stop":
                    if cur_block == "tool_use" and pending_steps:
                        current = tool_calls.get(cur_tool_id or "", {})
                        activity = _record_activity(activity_protocol.claude_activity(
                            current.get("name"), _parse_tool_input(current.get("input_json")),
                            activity_id=cur_tool_id, stage="running",
                        ))
                        if activity:
                            # A declared tool can already have a side effect. Do not replay it
                            # merely because the turn later produced no prose.
                            emitted_any = True
                            yield {"activity": activity}
                        yield {"tool_activity": {"steps": list(pending_steps)}}
                    cur_block = None
                    cur_tool_id = None
                # message_start / message_delta / message_stop → 忽略
                continue

            if t == "user":
                # Tool completions arrive as synthetic user messages in Claude's structured
                # stream. Pair them with the original tool_use id for an expandable result.
                content = (e.get("message") or {}).get("content")
                if isinstance(content, list):
                    for block in content:
                        if not isinstance(block, dict) or block.get("type") != "tool_result":
                            continue
                        tool_id = block.get("tool_use_id")
                        current = tool_calls.get(tool_id) if isinstance(tool_id, str) else None
                        if not current:
                            continue
                        activity = _record_activity(activity_protocol.claude_activity(
                            current.get("name"), _parse_tool_input(current.get("input_json")),
                            block.get("content"), activity_id=tool_id, stage="completed",
                            is_error=bool(block.get("is_error")),
                        ))
                        if activity:
                            yield {"activity": activity}
                continue

            if t == "result":
                _flush_text()
                _flush_tools()
                # result.result 是 CLI 给的权威全文;没有就回落已累积 final_text。
                # 写回 final_text 让 _make_attempt 取到同一个值(它读 final_text)。
                final_text = (e.get("result") or final_text or "").strip()

                # 学模型上下文窗口(#173 · 不写死 forge 阈值):result.modelUsage.<model>.contextWindow
                # 是 CC 自己报的真实窗口 → 持久 → 下轮 forge 阈值用真值(opus-4-8 实测 1M)。失败安全。
                forge.remember_model_windows(e.get("modelUsage"))

                # 本轮真实花销 token 总量(user要的「每一轮真实花销」)· result.usage 是权威
                # 聚合(含工具多 model 调用),缺失 / 全 0 时回落末条 assistant message.usage。
                cc_turn_usage = (_extract_cc_turn_usage(e.get("usage"))
                                 or _extract_cc_turn_usage(last_msg_usage))
                cc_turn_tokens = _extract_cc_turn_tokens(cc_turn_usage)

                # ── 根治「agent重复打招呼/复述/失忆」(2026-06-25 · 受控实验实锤)──────────
                # claude -p emit `result` 之后、**进程退出时**才把本轮 assistant turn flush 进
                # transcript(`--resume` 读的那个文件)。旧逻辑 result 一到就 yield 哨兵 + return
                # → finally `proc.kill()` 把 claude **杀在 flush 之前** → 这轮回复(已流给客户端 +
                # 已落后端 chat_messages)**没进 transcript** → 下轮 `--resume` 看到 user turn 悬空
                # → claude 补一条 `<synthetic>`「No response requested.」收口 → agent看不到自己上一条
                # → 重复开场 / 复述 /「我脑子里没那条」。Pi 受控实验双臂实锤:**不杀让它正常退出 →
                # 真 assistant turn 落盘;result 一到就 kill → 真 turn 丢、只剩 synthetic**。
                # 修法:result 后**先等 claude 自己退出**(它是 `-p` 一次性进程 · 退前 flush 完
                # transcript)再读 thinking + yield 哨兵 + return。退出后 transcript 已含本轮 →
                # 下轮 resume 看得到。有界 grace 兜底(万一不自退 → finally proc.kill() 照旧 ·
                # 不比现状更糟);grace < 心跳泵 10s 静默窗,正常亚秒级退,延迟落在「正文已流完、
                # done 还没发」之间,无感。读 thinking 也挪到退出后(transcript flush 完 · 读更全)。
                try:
                    await asyncio.wait_for(proc.wait(), timeout=_RESULT_EXIT_GRACE)
                except asyncio.TimeoutError:
                    logger.warning(
                        "cc_headless: claude 未在 %ss 内自退(result 后)· 走 finally 兜底 kill · "
                        "本轮 assistant turn 可能没 flush 进 transcript", _RESULT_EXIT_GRACE)
                except Exception:  # noqa: BLE001 · wait 异常不阻断收尾
                    pass

                # ── 思考链(2026-06-25 · 兼容 summarized / omitted 两类模型)──
                # · summarized 模型(opus 4.6 / sonnet 4.6 …):live thinking_delta 已实时吐过
                #   明文(cc_thinking_live 非空)→ 这里只拼成 cc_thinking_full 落 done·
                #   **绝不再 yield cc_thinking_delta** —— 否则前端思考链
                #   渲两遍(live 增量一遍 + transcript 全量一遍)· 这正是本次改动要防的回归。
                # · omitted 模型(opus 4.7/4.8 …):live thinking_delta 恒空(cc_thinking_live 空)·
                #   摘要不下发 · transcript 通常也空 → cc_thinking_full 读出来一般为空 · 读到啥
                #   (老版本 / 将来 CLI 变更)就全量 yield 一次(保留 2026-06-21 既有行为)。
                if cc_thinking_live:
                    cc_thinking_full = "".join(cc_thinking_live).strip()  # 已 live 吐过 · 不重发
                else:
                    cc_thinking_full = _read_turn_thinking(actual_sid)
                    if cc_thinking_full:
                        emitted_any = True           # 已向客户端吐思考正文 → 闸死重试
                        yield {"cc_thinking_delta": cc_thinking_full}

                # Some Claude versions do not emit user/tool_result live. Rebuild once the
                # transcript has flushed so historical replay still contains the full work.
                if actual_sid:
                    try:
                        activities = activity_protocol.merge_activities(
                            activities,
                            forge.read_turn_activities(forge.session_path(actual_sid)),
                        )
                    except Exception:  # noqa: BLE001 - activity capture is fail-open
                        pass
                    visible_events = _read_turn_visible_events(actual_sid)

                # ── 思考时长「思考了 X 秒」──
                # = spawn 到首个 text_delta 的耗时(纯思考期 · 不含正文生成);
                # 没首字(agent没出文)→ 回落 result.duration_ms(整轮耗时)。
                if first_text_ts is not None:
                    think_secs = int(round(first_text_ts - start_ts))
                else:
                    _dur_ms = e.get("duration_ms")
                    think_secs = int(round(_dur_ms / 1000)) if isinstance(_dur_ms, (int, float)) and _dur_ms > 0 else None
                think_secs = think_secs if (think_secs and think_secs > 0) else None

                # ⚠️ 不在这里 yield done —— 交 orchestrator 决策(空退可能要 salvage / 重试)。
                # result 拿到 = 正常收尾:yield 哨兵后 **return**(不再空转等 EOF)·
                # 标 attempt_yielded 防循环外兜底重复 yield。`return` 在 async generator
                # 合法 · finally 的 proc.kill()/wait() 照跑。
                attempt_yielded = True
                yield {"_attempt": _make_attempt(interrupted=False)}
                return

            if t == "assistant":
                # 完整 message → 正文/思考靠 stream 增量,这里只**顺手记 usage** 当兜底:
                # result.usage 是权威聚合(优先用),但版本漂移 / 罕见缺失时回落末条
                # assistant message.usage(与 forge.last_assistant_usage 同源 · 已知必带)。
                _mu = (e.get("message") or {}).get("usage")
                if isinstance(_mu, dict):
                    last_msg_usage = _mu
                continue

        # 循环正常退出(timeout-break / EOF-without-result)而 result 分支没跑到 →
        # 已流给客户端的正文(final_text)会绕过 _attempt 丢掉。这里兜底:flush 残余 ·
        # yield 一个 **interrupted=True** 的 _attempt 带回已吐正文(orchestrator 据 emitted_any
        # 收口、不当空退丢)。interrupted 标残缺 · 别假装完整收尾(否则关死重试 + 残缺当完整)。
        if not attempt_yielded:
            _flush_text()
            _flush_tools()
            att = _make_attempt(interrupted=True)
            # spawn 没正常出 result(EOF / 软超时)且**零正文** · 读干此前被吞的 stderr:
            # ① 失败诊断(原 stderr=PIPE 但从不读 → 错误全静默)· ② 检测未公开 flag
            # `--thinking-display` 被 CLI 拒(auto-update 改/删了它)→ 标 flag_rejected,让
            # orchestrator 关 flag 重试一次(自愈 · 见模块顶 _thinking_flag_disabled 注释)。
            # _drain_stderr 有界 2s:进程已退(EOF)秒回错误;还活着(软超时)读不到 EOF → 2s 回 ""
            # (软超时本就已等 _PROC_TIMEOUT · +2s 可忽略)。gate 在零正文上:出过字 ≠ flag rot。
            if not att["full"]:
                stderr_txt = await _drain_stderr(proc)
                if stderr_txt.strip():
                    logger.warning(
                        "cc_headless spawn no-result · stderr: %s",
                        stderr_txt.strip()[:600],
                    )
                    if _is_thinking_flag_rejected(stderr_txt):
                        att["flag_rejected"] = True
            yield {"_attempt": att}
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        try:
            await proc.wait()
        except Exception:  # noqa: BLE001
            pass


def _extract_cc_turn_usage(usage):
    """Claude Code `result.usage` → CC 专属 usage。

    保留 Claude Code / Anthropic 的原生字段名，不翻译成 Codex 的 input/cache contract。
    `input_tokens` 只代表未命中/未创建 prompt cache 的输入；本轮实际上下文输入是三种
    input token 的和，因此额外派生 `context_input_tokens`：

      context_input_tokens = input_tokens
                           + cache_creation_input_tokens
                           + cache_read_input_tokens

    result.usage 是含工具多 model 调用的聚合权威值；缺失、负数、非法或全 0 → None。"""
    if not isinstance(usage, dict):
        return None
    try:
        input_tokens = int(usage.get("input_tokens") or 0)
        cache_creation_input_tokens = int(usage.get("cache_creation_input_tokens") or 0)
        cache_read_input_tokens = int(usage.get("cache_read_input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)
    except (TypeError, ValueError):
        return None
    values = (
        input_tokens, cache_creation_input_tokens,
        cache_read_input_tokens, output_tokens,
    )
    if any(value < 0 for value in values) or not any(values):
        return None
    context_input_tokens = (
        input_tokens + cache_creation_input_tokens + cache_read_input_tokens
    )
    return {
        "input_tokens": input_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
        "context_input_tokens": context_input_tokens,
        "output_tokens": output_tokens,
    }


def _extract_cc_turn_tokens(usage):
    """Claude Code usage → context input + output 的本轮吞吐量。

    这是 `cc_turn_tokens` 的值；只用于兼容需要单值的 CC 消费方。PWA 拆项应优先读取
    `cc_turn_usage`，不得把这个单值解释成 Codex token usage。"""
    cc_usage = _extract_cc_turn_usage(usage)
    if cc_usage is None:
        return None
    return cc_usage["context_input_tokens"] + cc_usage["output_tokens"]


# 诊断日志(2026-06-30 · **临时** · 跑一两天验完即删):每轮把 usage 拆项 + wall-clock 追加到
# usage_log.ndjson —— 回答 ① CC 缓存是不是每轮被 hook 注入打废(背靠背连发看 cache_write 高不高)
# ② TTL 是 5min 还是 1h(故意晾空档后 cache_read 翻不翻 cache_write)。失败安全 · 永不 break 本轮。
_USAGE_LOG_PATH = os.path.join(_STATE_DIR, "usage_log.ndjson")


def _log_turn_usage(usage_dict, sid, model):
    if not isinstance(usage_dict, dict):
        return
    try:
        line = json.dumps({
            "ts": round(time.time(), 1),
            "sid": (sid or "")[:8],
            "model": model or "",
            **usage_dict,
        }, ensure_ascii=False)
        with open(_USAGE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        logger.info("cc_headless turn_usage: %s", line)
    except Exception:  # noqa: BLE001 · 诊断日志永不 break 本轮
        pass


def _build_done_ev(full, parts, cc_thinking_full, think_secs,
                   rate_limited=False, saw_synthetic=False, interrupted=False,
                   cc_turn_tokens=None, cc_turn_usage=None,
                   activities=None, visible_events=None):
    """把一次 CC attempt 组装成对外 `done`；不输出 Codex/旧通用 usage/thinking 键。"""
    done_ev = {
        "done": True,
        "full": (full or "").strip(),
        "parts": parts or [],
        "usage_source": "cc",
    }
    # 只接受 Claude thinking_delta / transcript thinking 汇成的 CC 专属全文。
    if cc_thinking_full:
        done_ev["cc_thinking_full"] = cc_thinking_full
    if think_secs is not None:
        done_ev["thinking_seconds"] = think_secs
        done_ev["thinking_duration"] = _fmt_duration(think_secs)
    # 额度信号透传(2026-06-22):引擎本轮探到 rate_limit_event / <synthetic> 占位残片时
    # 带上 → 后端据此把空退占位换成「额度用满了过阵回来」(区分该等 vs 抽风重发)。
    # 只在为真时带键(老下游不认这俩键也无碍 · 失败安全)。
    if rate_limited:
        done_ev["rate_limited"] = True
    if saw_synthetic:
        done_ev["saw_synthetic"] = True
    # interrupted:本轮被截断(timeout-break / EOF-without-result)· 已吐正文是残缺的。
    # 透出给后端 → 别把残缺当完整收尾(别关死重试 / 别给残缺当完整答案)· 只在为真时带键。
    if interrupted:
        done_ev["interrupted"] = True
    # CC 本轮吞吐单值(result.usage 聚合)· 只供仍需单值的 CC 消费方；拆项以
    # cc_turn_usage 的 Claude Code 原生字段为准。
    if isinstance(cc_turn_tokens, int) and cc_turn_tokens > 0:
        done_ev["cc_turn_tokens"] = cc_turn_tokens
    if isinstance(cc_turn_usage, dict) and (
            cc_turn_usage.get("context_input_tokens") or cc_turn_usage.get("output_tokens")):
        done_ev["cc_turn_usage"] = cc_turn_usage
    if isinstance(activities, list) and activities:
        done_ev["activities"] = activities
    if isinstance(visible_events, list):
        done_ev["visible_events"] = visible_events
    return done_ev


def _attempt_cc_value(attempt, cc_key, legacy_key):
    """读取旧 `_attempt` fixture/进程内输入的兼容层；对外 done 永远只写 CC 专属键。

    `_attempt` 不是 wire event，但老测试或滚动重载期间的旧 producer 可能仍给通用键。
    兼容只能发生在读侧，禁止把 legacy key 再写回新 done。"""
    if not isinstance(attempt, dict):
        return None
    if cc_key in attempt:
        return attempt.get(cc_key)
    return attempt.get(legacy_key)


def _attempt_cc_usage(attempt):
    """把新/旧 `_attempt` usage 都规范成 CC 原生字段；旧形状绝不向外透传。"""
    if not isinstance(attempt, dict):
        return None
    parsed = _extract_cc_turn_usage(attempt.get("cc_turn_usage"))
    if parsed is not None:
        return parsed
    legacy = attempt.get("turn_usage")
    if not isinstance(legacy, dict):
        return None
    return _extract_cc_turn_usage({
        "input_tokens": legacy.get("new"),
        "cache_creation_input_tokens": legacy.get("cache_write"),
        "cache_read_input_tokens": legacy.get("cache_read"),
        "output_tokens": legacy.get("out"),
    })


def _attempt_cc_tokens(attempt):
    """优先由规范化 CC usage 算吞吐量；无拆项时才读兼容单值。"""
    usage = _attempt_cc_usage(attempt)
    if usage is not None:
        return usage["context_input_tokens"] + usage["output_tokens"]
    return _attempt_cc_value(attempt, "cc_turn_tokens", "turn_tokens")


async def stream_headless(text, resume_sid=None, model=None, effort=None):
    """spawn `claude -p` stream-json,解析 NDJSON → yield cc_engine ndjson 事件。

    resume_sid: 指定续哪个 session(默认读 last-good)· 传 "" 强制全新起。
    effort: reasoning effort(medium/high/xhigh · session 级 `--effort`)· None/非法 → 用
        claude CLI 默认。白名单校验在 `_build_args`(只这三档追加 --effort · 见 `_VALID_EFFORTS`)。
    异常(spawn 失败 / 解析炸)由调用方 except 兜(同 PTY 路径,yield error / 抛)。

    空退自动重试(2026-06-21):额度压力下 `claude -p` 偶发**秒空退**(transcript 零新
    记录就退)→ 实测重发同句即愈。本函数检测「本轮零正文 + 客户端零输出」→ 先 salvage
    transcript(防第一次其实写了 turn 只是没流到客户端)· 真空才同 sid `--resume` 重 spawn
    **一次**(`_EMPTY_RETRY_MAX`)。**铁律**:一旦已 yield 过任何 text/reasoning 给客户端
    (`emitted_any`)· **绝不重试**(否则前端看到重复正文)。重试间隙不发事件 → cc_engine
    心跳泵 10s 静默自动补 `thinking_status`(前端不超时 · 本函数不主动断心跳)。"""
    global _thinking_flag_disabled  # flag_rejected 自愈分支要写它(关掉 --thinking-display)
    # ⚠️ 孪生(2026-06-29 · E3):下面这段「阈值 → _resolve_resume_sid → forge 裁 / ForgeNoProgress
    # 归档+fallback」编排骨架在 `cc_sdk._prepare_resume_sid`(SDK 引擎旁路)里有一份**复制**(重活
    # 都调本模块/forge 同名函数 · 只编排双份)。**改这段(加 case / 调顺序)必同步改 cc_sdk 那份**。
    # 模型感知 forge 续航阈值(#173 · 不写死):按本轮 model 的上下文窗口算 trigger/retain/resume_max。
    # window=200k → (140k,75k,300k)= 历史值(4.6/sonnet no-op);1M → (750k,375k,1.5M)。窗口运行时
    # 从 CC result.modelUsage 学到(见下 result 分支 remember_model_windows)· 冷启回落 fallback 表。
    th = forge.continuation_thresholds(model)
    # sid 解析抽到纯函数 `_resolve_resume_sid`(读指针 → 校验文件存在 + < resume_max → fallback
    # 跳巨无霸孤儿 → 全失败 None)· 失败安全 · 可单测。指针失效自愈,不全新起丢历史。
    try:
        sid = _resolve_resume_sid(resume_sid, resume_max=th.resume_max)
    except Exception:  # noqa: BLE001 · 解析炸就全新起(失败安全)
        sid = None

    # forge 续航裁剪:sid 的 transcript 太肥(> th.trigger · 模型感知)时,裁成新 sid 再
    # spawn,否则 --resume 整段灌进去会爆窗口 / 拖慢冷启。整段 try/except 包住:
    #   · ForgeNoProgress(裁不到位)→ **归档**这个肥死号(.forged-)+ 回落健康近段 session;
    #   · 其它环节失败 → 回落原 sid 直接 spawn(裁剪是优化,不是必须)。
    if sid:
        try:
            est = _session_trigger_tokens(sid)   # 2026-06-24:触发优先真实 usage(回落 raw 估算)
            rounds = _session_rounds(sid)        # 2026-07-01:粗轮数(按轮触发的第二道闸)
            # 触发(粗闸):token 超 th.trigger **或** 轮数超 (keep_rounds + margin)。retain 目标
            # 已改**按轮**(见下 keep_rounds=)· token trigger 仍留作旧粗闸(base64/固定开销撑爆时)。
            over_tokens = est is not None and est > th.trigger
            over_rounds = (rounds is not None
                           and rounds > forge.FORGE_KEEP_ROUNDS + forge.FORGE_ROUND_TRIGGER_MARGIN)
            if over_tokens or over_rounds:
                # ⚠️ 必须带连字符的标准 UUID:claude -p --resume 收紧校验后**拒绝**无连字符
                # 的 uuid4().hex(报「is not a UUID」)→ forge 裁出的 sid resume 失败 → agent
                # 反复「没出声」(2026-06-21 根因)。cc_engine._gen_sid 一直是 str(uuid4()),
                # 这里历史遗留写成 .hex,对齐过来。
                new_sid = str(uuid.uuid4())
                # retain 目标改**按轮**(keep_rounds · 模型无关 · 治「切模型灾难重裁失忆」)+ token
                # 帽(retain_tokens=th.retain · 模型感知 · 按轮 retained 仍不许超它)。1M 模型下 30 轮
                # 远 < retain 帽 → 轮数说了算(模型无关);200k 模型下 token 帽兜底。
                forge.forge(forge.session_path(sid), new_sid,
                            retain_tokens=th.retain, keep_rounds=forge.FORGE_KEEP_ROUNDS)
                write_last_session(new_sid)
                sid = new_sid
        except forge.ForgeNoProgress:
            # 裁不到位的肥死号:旧逻辑回落「原 sid 直接 spawn」会把这个 >70k 巨无霸整段灌进去
            # (拖慢 / 爆窗口),且它每被 `--resume` 追写 mtime 又 bump(只读不 bump · 见
            # _fallback_resume_sid 注)→ 看着「最新」反复被 resume 选中,把agent拽回旧上下文(还
            # 重放它烤死的 stale hook 注入)。2026-06-21 修(**首要修法**):**归档**它
            # (`.forged-` · `forge.list_sessions()` 天然排除)→ 从候选彻底消失 → 回落一个
            # **内容时间最近**的健康 session 续接(内容时间排序是次要改进 · 排健康号之间)。
            # 归档失败安全:`_archive_forged` 吞异常,搬不动就退回既有行为(下面 fallback 拿不到
            # 健康号 → None → 全新起 · hook 注入兜底不失忆)。
            try:
                _archive_forged(forge.session_path(sid))
                sid = _fallback_resume_sid(th.resume_max)
                write_last_session(sid or "")
            except Exception:  # noqa: BLE001 · 归档/回落任一步炸 → 全新起(失败安全)
                sid = None
        except Exception:  # noqa: BLE001 · 裁剪失败回落原 sid
            pass

    attempt = 0
    while True:
        # ⚠️ resume 用本轮**实际写入**的 sid(spawn 内 system/init 落盘的 actual_sid)·
        # 重试同 sid `--resume`(同一条 user 输入续上文)· 不动 sid 生成/指针/forge 逻辑。
        args = _build_args(text, sid or None, model, effort)
        result = None
        async for ev in _stream_one_spawn(args, sid):
            if "_attempt" in ev:
                result = ev["_attempt"]      # 哨兵:轮结束汇报(不外吐)
                continue
            yield ev                          # 实时事件透传(delta / part_break / …)

        # spawn 没给 result(罕见:生成器 yield _attempt 前就抛)→ 当空轮处理(下面判空兜)。
        # 注:正常路径下 _stream_one_spawn 必 yield 一个 _attempt(result 收尾或截断兜底),
        # 此默认只在异常早退时命中。
        result = result or {
            "full": "", "parts": [], "cc_thinking_full": "", "think_secs": None,
            "cc_turn_tokens": None, "cc_turn_usage": None,
            "actual_sid": sid, "emitted_any": False,
            "activities": [], "visible_events": [], "rate_limited": False, "saw_synthetic": False, "interrupted": False,
        }
        # 续接:本轮 spawn 真正写入的 transcript sid(forge 裁过 / 全新起会变)→ 下轮重试
        # 同 sid `--resume`(指针已在 spawn 内 write_last_session)。
        sid = result.get("actual_sid") or sid
        # 诊断日志(2026-06-30 · 临时):本轮 usage 拆项落 usage_log.ndjson(分析缓存冷热 / TTL)。
        _log_turn_usage(_attempt_cc_usage(result), sid, model)

        full = (result.get("full") or "").strip()
        emitted_any = bool(result.get("emitted_any"))
        # 空退判定:本轮**没产出任何正文**(full 空)。emitted_any 单独把闸:
        # 哪怕 full 空,只要已 yield 过 text/reasoning 给客户端,也**绝不重试**(防双回复)。
        is_empty = not full

        # ── 已出文 / 已向客户端流出过正文 → 直接收口,不 salvage/不重试 ──
        # 截断(timeout-break / EOF-without-result)带回已吐正文的 attempt 落这里:
        # 透传 result["interrupted"] → done 标 interrupted=True(残缺不当完整 · 见 _build_done_ev)。
        if not is_empty or emitted_any:
            yield _build_done_ev(
                full, result.get("parts"),
                _attempt_cc_value(result, "cc_thinking_full", "reasoning_full"),
                result.get("think_secs"),
                rate_limited=bool(result.get("rate_limited")),
                saw_synthetic=bool(result.get("saw_synthetic")),
                interrupted=bool(result.get("interrupted")),
                cc_turn_tokens=_attempt_cc_tokens(result),
                cc_turn_usage=_attempt_cc_usage(result),
                activities=result.get("activities"),
                visible_events=result.get("visible_events"),
            )
            return

        # ── 此处:本轮零正文 + 客户端零输出 = 空退 ──
        # ⓪ flag 被 CLI 拒(auto-update 改/删了未公开的 `--thinking-display`)→ 不是真空退,
        #    是 spawn 被这个 flag 顶死。关掉 flag、同 sid 重试一次(自愈)· **不吃**空退重试
        #    预算(`attempt` 不增 · 这是确定性修复不是 flaky 空退)· 本进程级一次性:关了之后
        #    `_build_args` 不再带 flag → 不会再进本分支(不死循环)。降级:本进程后续无实时
        #    思考链,但agent正常说话,不至于像原来那样整轮静默 + 被后端误判成「额度用满」。
        if result.get("flag_rejected") and not _thinking_flag_disabled:
            _thinking_flag_disabled = True
            logger.warning(
                "cc CLI rejected --thinking-display (auto-update?) · disabled flag for this "
                "process · retry sid=%s without it (degraded: no live thinking chain)", sid,
            )
            continue

        # ① salvage 优先:第一次 spawn 可能其实往 transcript 写了 turn(parse race /
        #    synthetic 残片)· 读出来用 · 别盲目重 spawn(防双生成)。
        salv_full, salv_parts = _salvage_turn(sid, text)
        if salv_full:
            logger.warning(
                "cc empty-return but transcript has turn · salvaged (sid=%s · "
                "rate_limited=%s synthetic=%s) · no respawn",
                sid, result.get("rate_limited"), result.get("saw_synthetic"),
            )
            # salvage 出文了 → 现在**才**把它流给客户端(此前零输出 · 不会重复)·
            # 再带 done。思考链/时长复用本轮 result(salvage 只补正文)。salv_full 即
            # `_salvage_full_from_parts(salv_parts)`(text parts 拼成),直接当一拍 delta。
            yield {"delta": salv_full}
            _rf = (_attempt_cc_value(result, "cc_thinking_full", "reasoning_full")
                   or _read_turn_thinking(sid))
            if _rf:
                yield {"cc_thinking_delta": _rf}
            yield _build_done_ev(
                salv_full, salv_parts, _rf, result.get("think_secs"),
                rate_limited=bool(result.get("rate_limited")),
                saw_synthetic=bool(result.get("saw_synthetic")),
                cc_turn_tokens=_attempt_cc_tokens(result),
                cc_turn_usage=_attempt_cc_usage(result),
                activities=result.get("activities"),
                visible_events=_read_turn_visible_events(sid),
            )
            return

        # ② 真·零 transcript turn → 自动重试一次(同 sid `--resume` · 同一条输入)。
        if attempt < _EMPTY_RETRY_MAX:
            attempt += 1
            logger.warning(
                "cc empty-return (no transcript turn · rate_limited=%s synthetic=%s) · "
                "retry %d/%d · same sid=%s",
                result.get("rate_limited"), result.get("saw_synthetic"),
                attempt, _EMPTY_RETRY_MAX, sid,
            )
            # 重试间隙不 yield 任何事件 → cc_engine 心跳泵静默 10s 自动补 thinking_status
            # (前端不超时)。本函数不主动断心跳。
            continue

        # ③ 重试上限用尽仍空退 → 落回现有空退行为(空 done · 后端落「agent这轮没出声」占位)。
        logger.warning(
            "cc empty-return persists after %d retr%s (rate_limited=%s synthetic=%s) · "
            "fall back to placeholder · sid=%s",
            attempt, "y" if attempt == 1 else "ies",
            result.get("rate_limited"), result.get("saw_synthetic"), sid,
        )
        yield _build_done_ev(
            "", result.get("parts"),
            _attempt_cc_value(result, "cc_thinking_full", "reasoning_full"),
            result.get("think_secs"),
            rate_limited=bool(result.get("rate_limited")),
            saw_synthetic=bool(result.get("saw_synthetic")),
            activities=result.get("activities"),
            visible_events=result.get("visible_events"),
        )
        return
