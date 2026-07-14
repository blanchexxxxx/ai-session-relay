#!/usr/bin/env python3
"""
Swap / Forge / Route Guard —— Claude Code transcript 续航,给agent「不断层换窗」。

按 public-cc-swap-forge-guide v3 实现,适配单用户agent引擎(workspace)。核心原则:
  · 日常重启走 `claude --resume <last-good>`(Claude 自己续,**不动 transcript**)。
  · Forge 才重写 transcript:filter → clean boundary → tool primer → rewrite chain
    → 写新 <new_sid>.jsonl → resume 它。用于「上下文太肥」或「Route Guard 漂移恢复」。
  · 所有改写**失败安全**:任何异常由调用方(cc_engine)兜 → 回落全新 spawn = 今天行为。

真实 schema(本地 transcript 实测):
  事件 type ∈ {user, assistant, attachment, ai-title, last-prompt, queue-operation,
              file-history-snapshot}。
  对话事件(user/assistant/attachment)带 uuid / parentUuid / sessionId / cwd /
  gitBranch / version / timestamp / isSidechain。
  assistant.message 带 model(如 'claude-opus-4-8')/ content(text|thinking|tool_use)。
  tool_use 块:{type,id(toolu_…),name,input,caller};tool_result 在 user 事件的
  message.content 里:{tool_use_id,type,content}。

踩坑(2026-06-20 修 · fix/forge-selfheal):
  · find_clean_boundary 拉链后退 —— 旧逻辑"回退到 user"没区分**真 user 文本 turn**
    (content=str)和工具回合里的 user(content=list 含 tool_result)。后者被当干净
    边界 → 和"补全缺失 tool_use"约束交替把 cut 拉到 index 0 → retained==keepable
    (一点没裁)。agent跑 agent loop(密集 tool round)必触发。修法:回退只认
    `_is_user_text_event` 真文本 turn + 硬下限 floor(cut 不跌破按 retain 选的兜底)。
  · 孤儿堆积 —— forge_events 旧逻辑无条件先 write_jsonl 落盘,裁不到位也留文件 →
    guardian 每 10min 无退避重试 → 攒一堆垃圾(实测 70 个孤儿)。修法:**先在内存
    算完 retained、确认 est<=retain×1.5(_hard_cut 兜底强切保证)才落盘**,裁不到位
    raise ForgeNoProgress,根本不写文件。
  · CC v2.1.179 仍用**扁平 <sid>.jsonl 当 transcript**(UUID 目录只是 tool-results
    sidecar)· 别把 session 定位重写成目录格式 · newest_session_id glob *.jsonl
    (排除 .forged-*)是对的。
  · 指针/session 错配 —— 指针校验 + 统一 session 来源在 cc_headless / cc_engine 修。
"""
import collections
import copy
import glob
import json
import os
import re

import activity_protocol

WORKSPACE_DIR = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_WORKSPACE", os.getcwd())
))
CONV_TYPES = ("user", "assistant", "attachment")


class ForgeNoProgress(Exception):
    """forge 裁不到位(retained est 仍远超 retain 上限)→ 不落盘、向上抛。
    调用方(/forge cc_engine · inline forge cc_headless)catch 它 → 不留孤儿文件 +
    记错/告警。源头不造孤儿(见模块 docstring「孤儿堆积」踩坑)。"""


# ── transcript 定位 ───────────────────────────────────────────────
def project_dir(cwd=WORKSPACE_DIR):
    """Claude Code transcript 目录:~/.claude/projects/<encoded-cwd>/。
    编码规则(本地实测):'/' '\\' ':' 都替成 '-'。
    /home/user/ai-workspace → -home-user-ai-workspace;
    c:\\Users\\user\\ai-workspace → c--Users-user-ai-workspace。"""
    enc = cwd.replace("/", "-").replace("\\", "-").replace(":", "-")
    return os.path.expanduser(os.path.join("~/.claude/projects", enc))


def list_sessions(cwd=WORKSPACE_DIR):
    """该 project 下所有 session jsonl,按 mtime 新→旧。
    排除 forge 归档(`<sid>.jsonl.forged-<ts>`)—— `*.jsonl` glob 已天然不收它们
    (后缀不是 .jsonl),这里再显式过滤一道 `.forged-` 防将来归档命名变了误收
    被搬走的旧 transcript(踩坑:指针指向 .forged- 旧 sid → resume 失败)。"""
    files = [
        p for p in glob.glob(os.path.join(project_dir(cwd), "*.jsonl"))
        if ".forged-" not in os.path.basename(p)
    ]
    return sorted(files, key=os.path.getmtime, reverse=True)


def newest_session_id(cwd=WORKSPACE_DIR):
    s = list_sessions(cwd)
    return os.path.splitext(os.path.basename(s[0]))[0] if s else None


def session_path(sid, cwd=WORKSPACE_DIR):
    return os.path.join(project_dir(cwd), sid + ".jsonl")


def read_jsonl(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except (ValueError, json.JSONDecodeError):
                continue          # 损坏行直接跳(PDF:按行裁剪,坏行丢)
    return out


def write_jsonl(events, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    os.replace(tmp, path)          # 原子落盘
    return path


# ── 非对话载荷压缩(把 base64 图 / hook 注入简报踢出 forge 预算)─────────────
# 2026-06-23 修(self-hostedagent transcript 被「非对话载荷」撑爆 → forge 触发太频 → 重启风暴):
#   ① base64 内联图 —— `{"type":"image","source":{"type":"base64","data":"<巨>"}}` 块。
#      一张 JPEG = 57k+ token(实测单图 586k chars / chars÷3 ≈ 195k)· self-hosted图本就走 @path
#      原生看、不靠 transcript 二进制重放,留着它只是把 forge 预算占满 → 每 4-8 轮触发一次
#      forge。**压成占位文本块**(零体积、保住「这里有张图」的语义锚点)。出现位置(本地实测):
#        · user/assistant 事件 message.content[] 里(用户在 prompt 里贴图);
#        · tool_result 的 content[] 里(嵌一层)。
#   ② SessionStart hook 注入简报 —— 顶层 `attachment` 事件,`hookEvent == "SessionStart"`。
#      它是注入产物(身份卡 / 近况 / 最近对话锚点),**每次 resume 被 Claude Code 持久化进
#      transcript** 一份(每次 +~6.3k token)· 重放只会塞 stale 简报(下轮 hook 又重注一份
#      新鲜的)→ **整条丢弃**。其余 hookEvent(如 IDE 诊断)体积小、与对话相关,留着。
#
#   2026-06-23 第二轮修(治「切 session 失忆」· `fix/forge-base64-sidecar-briefing`):
#   上一轮 ① 只扫 `message.content`(且必须是 list);但真图躺在压缩器**够不到**的
#   sidecar 字段里,坐在 retained 窗口内把真对话挤出去 → retained:3 = 失忆点:
#     · `attachment.type=="file"` 事件的 **`attachment.content`**(嵌套 · 实测路径
#       `attachment.content.file.base64` · 单条 55-58k token)—— 这种事件**没有
#       `message.content`**,旧压缩器从不经过;
#     · 顶层 **`toolUseResult.file.base64`**(CC sidecar 字段 · JPEG magic `/9j/` ·
#       单条 ~41k token)· `message.content` 那条可能已被压成占位,但 base64 整份还
#       躺在 `toolUseResult` 里;
#     · `message.content` 是 **str** 且含 `data:image/...;base64,...`(内联 data-URI)。
#   修法:写一个**递归 base64 剥离器**(`_strip_base64_deep` · 遍历整条事件 dict/list,
#   把符合「图 base64 指纹」的长字符串换成占位,**保留结构键**),作用到每条 keepable
#   事件的 `message.content`(含 str)、`attachment.content`、顶层 `toolUseResult`。
#   指纹要稳、别误伤正文(见 `_looks_like_image_b64`):data:image data-URI · 或某个
#   `base64` 键的长值带图片 magic(JPEG `/9j/` · PNG `iVBOR`)。
#   ②(简报)判定改成**只看 hookEvent、不卡 wrapper type** —— CC 2.1.179 实际把简报
#   记成 `att.type == "hook_success"`(620+ 样本 · `hook_additional_context` 出现 0 次),
#   旧条件卡死 type 一条没踢。全库 hookEvent 只有 SessionStart 一种值 → 零误杀。
#
# 压缩用在 forge **保留侧**(`filter_keepable` → `forge_events` 收的就是它)· 让创建新
# session 的保留预算反映真对话量、不被图 / 简报占满(#100)。
# ⚠️ **触发 / 可-resume 估算不在这里** —— 它们估的是 `claude --resume` 加载的**原始**
# transcript(base64/简报全在 · resume 不压),必须走 raw 的 `conv_events`,不能用
# filter_keepable(2026-06-24 #100 回归:压缩估算掉到阈值下 → 876k 永不裁;见
# guardian._est_tokens / cc_headless._session_est_tokens)。
_ATTACH_DROP_HOOK_EVENTS = ("SessionStart",)

# 递归 base64 剥离的占位串(零体积 · 保「这里有张图」语义锚点 · self-hosted图走 @path 原生看)。
_B64_IMAGE_PLACEHOLDER = "[图·forge压缩·self-hosted走@path原生看]"
# `{"type":"image",...}` 整块 → 占位文本块的文案(2026-06-24 F4 · 治 cc 轮 API image error)。
# 引用式 image 块(`attachment.content` 的 `{"type":"image","file":{...}}`)即便 base64 已被
# #100 剥成占位、**块壳仍是 type=="image"** → Anthropic API 仍当图处理 → 撞
# `an image in the conversation could not be processed` → 整轮失败(agent「没出声」)。self-hosted图本走
# @path 原生看 · transcript 里这些 image 块冗余且害 API · 整块换成文本占位(保语义锚点)。
_IMAGE_BLOCK_PLACEHOLDER = "[图·forge剥除·self-hosted走@path原生看]"
# base64 长串判定门槛(超过这么多字符才考虑剥 · 防误伤短串 / 正文里偶现的 token)。
_B64_MIN_LEN = 1000
# 图片 magic 前缀(base64 编码后的头几字符 · 稳定不变):JPEG / PNG / GIF / WEBP(RIFF)。
_B64_IMAGE_MAGIC = ("/9j/", "iVBOR", "R0lGOD", "UklGR")
# 内联 data:image data-URI · 整段(含 payload)换占位。**子串**匹配:data-URI 常嵌在
# 用户正文里(`看图 data:image/...;base64,xxxx 你觉得呢`)· 只换 URI 段、保留前后正文。
# payload 用 [^\s"'<>)\]]+ 收到第一个空白 / 引号 / 闭合符为止(JSON 串里 base64 不含这些)。
_DATA_URI_RE = re.compile(r"data:image/[^\s;]+;base64,[A-Za-z0-9+/=]+")


def _looks_like_image_b64(s):
    """裸 base64 字符串是不是「图」指纹(用于 `base64` 键判定)?稳、别误伤正文:
    长度 > _B64_MIN_LEN 且以图片 magic 前缀开头。**不**单凭长度剥(那会误伤长正文);
    裸 base64 的「这是图」语境由调用点(`base64` 键名)提供。"""
    if not isinstance(s, str):
        return False
    return len(s) > _B64_MIN_LEN and any(s.startswith(m) for m in _B64_IMAGE_MAGIC)


def _image_block_placeholder():
    """`{"type":"image",...}` 整块 → 占位文本块(2026-06-24 F4)。
    与 `_image_placeholder_block` 区别:那个靠 source.media_type 标注图种(内联 base64 图块有);
    引用式 image 块(`attachment.content` 的 `{"type":"image","file":{...}}`)图种在 `file.type`、
    结构不一,这里统一用固定占位文案(零体积 · 保「这里有张图」语义锚点)。"""
    return {"type": "text", "text": _IMAGE_BLOCK_PLACEHOLDER}


def _strip_base64_deep(obj, _key=None):
    """递归遍历任意 dict / list / 标量,把「图 base64」长字符串换成占位文本、
    并把整个 `{"type":"image",...}` 块换成占位文本块,**保留其余结构键**。
    返回 (新对象, changed)。失败安全:不认的结构原样返回。

    判定(三条互补):
      · **整块 image**(2026-06-24 F4):任何 dict 的 `type=="image"` → 整块换成
        `{"type":"text","text": <占位>}`。覆盖引用式 image 块(`attachment.content` 的
        `{"type":"image","file":{base64,type,dimensions}}`)—— 这种块即便内联 base64 已被
        下面 ① 剥成占位、**块壳仍是 image** → Anthropic API 仍当图处理 → 撞
        `an image in the conversation could not be processed` → 整轮失败。**只动 type=="image"
        的 dict**,不碰 thinking 块的 `signature`(那是 type=="thinking" 块上的键 · extended-
        thinking 签名 · 必须保留)/ text / tool_use / tool_result。换块即返回(不再递归其内,
        整块都没了);
      · ① `base64` 键下的整串裸 base64(带图片 magic)→ 整串换占位(CC sidecar 的
        `attachment.content.file.base64` / `toolUseResult.file.base64` 都是裸 base64 ·
        靠键名 `base64` + magic + 长度三重确认 · 不误伤正文);
      · ② 任何字符串里的 data:image data-URI 段(无论挂在哪个键 · 可嵌在正文中间)→ 子串
        替换成占位 · 保留前后正文(`_DATA_URI_RE`)。
    顶层不拷贝原对象(只在有变更时按层 rebuild · 无变更零拷贝)。"""
    if isinstance(obj, str):
        # ① `base64` 键下的整串裸 base64(带图片 magic)→ 整串换占位。
        if _key == "base64" and _looks_like_image_b64(obj):
            return _B64_IMAGE_PLACEHOLDER, True
        # ② 内联 data:image data-URI 段(无视键名 · 可嵌正文中间)→ 子串换占位、保留前后正文。
        if "data:image/" in obj:
            new, n = _DATA_URI_RE.subn(_B64_IMAGE_PLACEHOLDER, obj)
            if n:
                return new, True
        return obj, False
    if isinstance(obj, dict):
        # F4:整个 image 块 → 占位文本块(引用式 / 内联式都换 · 不再递归其内)。
        #     **只**命中 type=="image" · signature(在 thinking 块上)/ text / tool_use /
        #     tool_result 都不是 image 块 · 天然不受影响。
        if obj.get("type") == "image":
            return _image_block_placeholder(), True
        changed = False
        out = {}
        for k, v in obj.items():
            nv, c = _strip_base64_deep(v, _key=k)
            out[k] = nv
            changed = changed or c
        return (out, True) if changed else (obj, False)
    if isinstance(obj, list):
        changed = False
        out = []
        for it in obj:
            nv, c = _strip_base64_deep(it, _key=_key)
            out.append(nv)
            changed = changed or c
        return (out, True) if changed else (obj, False)
    return obj, False


def _image_placeholder_block(src):
    """base64 图块 → 占位文本块(零体积 · 保「这里有张图」语义锚点)。
    无 filename 字段(内联图只有 type/source)· 用 media_type 标注图种。"""
    mt = ""
    if isinstance(src, dict):
        mt = src.get("media_type") or ""
    label = (mt.split("/")[-1] or "图").upper() if mt else "图"
    return {"type": "text", "text": f"[图:{label} · forge 压缩(self-hosted走 @path 原生看)]"}


def _compress_content_list(content):
    """递归压 content list 里的 image 块(含 tool_result 嵌套一层)。
    返回 (新 content, changed)。**任何** image 块(内联 base64 / 引用式 / 别的 source)→
    占位文本块;tool_result 的 content 递归压;其余块原样。

    2026-06-24 F4:原来只换 `source.type=="base64"` 的内联图块 · 漏掉引用式 image 块
    (无 base64 source · 块壳仍是 image · API 仍当图处理 → 撞 image error)。改成换**任何**
    type=="image" 块 —— 内联 base64 用 `_image_placeholder_block`(带 media_type 标图种)·
    其余(引用式等)用统一文案 `_image_block_placeholder`。"""
    if not isinstance(content, list):
        return content, False
    changed = False
    out = []
    for b in content:
        if not isinstance(b, dict):
            out.append(b)
            continue
        if b.get("type") == "image":
            # 内联 base64 图块带 source.media_type · 用它标图种;引用式无 source → 统一文案。
            src = b.get("source")
            if isinstance(src, dict) and src.get("type") == "base64":
                out.append(_image_placeholder_block(src))
            else:
                out.append(_image_block_placeholder())
            changed = True
            continue
        if b.get("type") == "tool_result" and isinstance(b.get("content"), list):
            new_inner, inner_changed = _compress_content_list(b["content"])
            if inner_changed:
                b2 = dict(b)
                b2["content"] = new_inner
                out.append(b2)
                changed = True
                continue
        out.append(b)
    return out, changed


def _compress_attachment(e):
    """把单个事件里的「非对话载荷」踢出 forge 预算。返回:
      · None       → **整条丢弃**(SessionStart hook 注入简报 attachment · 注入产物非对话);
      · 压缩后的副本 → base64 图(含 sidecar 字段)换占位文本(体积归零);
      · 原事件      → 没图也没简报(零拷贝 · 不动)。
    纯函数 · 不读盘 · 失败安全(结构不符就原样返回)。"""
    # ① SessionStart hook 注入简报 attachment → 丢弃。
    #    判定**只看 hookEvent、不卡 wrapper type**(CC 2.1.179 实际记成 `type=="hook_success"`,
    #    旧 `type=="hook_additional_context"` 卡死一条没踢 · 全库 hookEvent 仅 SessionStart 一种值)。
    if e.get("type") == "attachment":
        att = e.get("attachment")
        if isinstance(att, dict) and att.get("hookEvent") in _ATTACH_DROP_HOOK_EVENTS:
            return None
    out = e
    # ② message.content[] 里的 base64 图块 → 占位文本块(保留 list-content 图块语义 ·
    #    上一轮已对 · 保留)。
    msg = out.get("message")
    if isinstance(msg, dict):
        new_content, changed = _compress_content_list(msg.get("content"))
        if changed:
            e2 = dict(out)
            e2["message"] = dict(msg)
            e2["message"]["content"] = new_content
            out = e2
    # ③ 递归剥 sidecar / 内联 base64 图(压缩器够不到的三处 · 2026-06-23 第二轮):
    #    · message.content 是 str 含 data:image data-URI;
    #    · attachment.content(file 事件的嵌套 base64 · 无 message.content);
    #    · 顶层 toolUseResult(CC sidecar · toolUseResult.file.base64)。
    #    只 rebuild 有变更的字段 · 无变更零拷贝。
    for field in ("message", "attachment", "toolUseResult"):
        val = out.get(field)
        if val is None:
            continue
        new_val, changed = _strip_base64_deep(val)
        if changed:
            e2 = dict(out)
            e2[field] = new_val
            out = e2
    return out


def compress_attachments(events):
    """对一串事件逐个跑 `_compress_attachment`,丢掉返回 None 的(简报),其余压缩/原样。"""
    out = []
    for e in events:
        c = _compress_attachment(e)
        if c is not None:
            out.append(c)
    return out


# ── 裁剪 / 过滤 ───────────────────────────────────────────────────
def conv_events(events):
    """对话链事件(user/assistant/attachment · 去 sidechain)· **不压缩**。
    估 `claude --resume` 实际加载的 transcript 体积用 —— resume 不压(base64/简报全加载),
    压缩只发生在 forge 创建新 session 时。**触发/可-resume 判断必须用这个 raw 视图**,
    别用 filter_keepable(那是压缩后的保留预算视图 · 用它做触发会严重低估 resume 真实体积 ·
    2026-06-24 回归:#100 修好 base64 剥离后,压缩估算掉到阈值下 → 永不裁 → 876k 膨胀)。"""
    return [e for e in events
            if e.get("type") in CONV_TYPES and not e.get("isSidechain")]


def filter_keepable(events):
    """只留对话链(user/assistant/attachment),丢 sidecar
    (ai-title/last-prompt/queue-operation/file-history-snapshot)和 sidechain
    (subagent 侧链不带回主 session)· 并把非对话载荷(base64 图 / SessionStart hook
    注入简报)压出 forge 预算(`compress_attachments` · 2026-06-23)。

    ⚠️ 这是**压缩后的保留预算**视图(forge 创建新 session 时收的就是它 · #100 base64 剥离)。
    **别拿它做触发 / 可-resume 估算** —— resume 加载的是原始 transcript(base64/简报全在),
    触发要用 raw 的 `conv_events`(否则严重低估 → 永不裁 · 2026-06-24 #100 回归根因)。"""
    return compress_attachments(conv_events(events))


def _event_chars(e):
    return len(json.dumps(e, ensure_ascii=False))


def estimate_tokens(events):
    return sum(_event_chars(e) for e in events) // 3   # 粗估 chars/3(PDF 6.2)


def last_assistant_usage(events):
    """末尾 assistant 事件的**真实** context 体积 = input + cache_creation + cache_read
    (= claude -p 该轮实际处理的 prompt token 总量 · output 不算)。三项是输入上下文的
    不相交划分(新输入 + 新写缓存 + 读缓存),相加 = 该轮总 prompt 体积,不重复计。

    2026-06-24(Forge-Reload):它含 system prompt / tools / CLAUDE.md / memory
    那些**不在 transcript events 里**、但 resume 会重新加载的 ~50k 固定开销,`estimate_tokens`
    看不到这些 —— 所以**给 forge 触发当补充信号**。
    ⚠️ **它对磁盘 base64 图全瞎**(模型 prompt 不含磁盘里的 base64 串)—— agent天天发图,
    transcript 被 base64 撑大,但 usage 完全不反映(Pi 67-session 实测:文件 1.6MB / raw
    估 471k 的 session,usage 才 59k)。所以**绝不能拿它单独当触发**(只用 usage>200k 在
    67 个生产 session 0/67 触发 → 永不裁 → 复现 #115 失忆)。调用方必须 **`max(raw 估算,
    usage)`**:raw 兜 base64、usage 兜固定开销,谁大用谁(见 `cc_headless._session_trigger_tokens`
    / `guardian._est_tokens`)。
    从尾往前取第一条带**非零** usage 的 assistant;全 0(`stop_sequence` 流式残片 · 实测
    2/67 尾部命中)当没 usage 继续往前;没有 → None。失败安全(异常 → None)。"""
    try:
        for e in reversed(events):
            if e.get("type") != "assistant":
                continue
            u = (e.get("message") or {}).get("usage")
            if not isinstance(u, dict):
                continue
            inp = u.get("input_tokens")
            if inp is None:
                continue
            total = (int(inp)
                     + int(u.get("cache_creation_input_tokens") or 0)
                     + int(u.get("cache_read_input_tokens") or 0))
            if total <= 0:
                continue          # 全 0 usage 残片 → 当没 usage · 往前找真值(防返 0 骗过 None 判定)
            return total
    except Exception:  # noqa: BLE001
        pass
    return None


def _tool_use_ids(e):
    ids = set()
    msg = e.get("message")
    if isinstance(msg, dict) and isinstance(msg.get("content"), list):
        for b in msg["content"]:
            if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id"):
                ids.add(b["id"])
    return ids


def _tool_result_ids(e):
    ids = set()
    msg = e.get("message")
    if isinstance(msg, dict) and isinstance(msg.get("content"), list):
        for b in msg["content"]:
            if isinstance(b, dict) and b.get("type") == "tool_result" and b.get("tool_use_id"):
                ids.add(b["tool_use_id"])
    return ids


def _token_floor_index(events, retain_tokens):
    """硬下限 floor:从尾往前累计到 ~retain_tokens 的那个 index —— cut **不许**
    跌破它(再往前 retained 就超 retain 太多 = 没在裁)。返回 floor index(>=0)。
    空序列 → 0。退化(整段都不到 retain)→ 0(全留也合法,本来就没超)。"""
    if not events:
        return 0
    total = 0
    for i in range(len(events) - 1, -1, -1):
        total += _event_chars(events[i])
        if total // 3 >= retain_tokens:
            return i
    return 0


def find_clean_boundary(events, retain_tokens):
    """从尾部累计到 retain_tokens 选 cut → 扩展保证 tool_result 的 tool_use 都在
    retained 内(不切在工具回合中间)→ 回退到最近**真 user 文本 turn**(content=str,
    一轮的干净边界)。返回 retained 的起始 index。

    2026-06-20 修拉链:旧逻辑回退到"最近 user 事件"会停在工具回合里的 user
    (content=list 含 tool_result),那不是干净 turn 边界;和"补全缺失 tool_use"
    约束交替把 cut 拉到 0 → 一点没裁。现在:
      (a) 回退只认 `_is_user_text_event`(真 user 文本 turn · content=str)。
      (b) 硬下限 floor:cut 不跌破 `_token_floor_index`(按 retain 选的兜底)——
          即便找不到干净文本 turn,也保证 retained 不超 retain 太多(由 _hard_cut
          兜底强切到位),不再回退到 0。"""
    if not events:
        return 0
    floor = _token_floor_index(events, retain_tokens)
    # 1) 初始 cut = floor(按 token 从尾选的兜底点)
    cut = floor
    # 2) 迭代到不动点:两约束互相拉扯,反复满足直到都成立 ——
    #    (a) 从真 user 文本 turn 起头;(b) retained 里每个 tool_result 的 tool_use 都在内。
    #    关键:cut **不跌破 floor**(防拉链拖到 0、保证真在裁)。cut 只减不增,必收敛。
    while True:
        changed = False
        # (a) 回退到最近**真 user 文本** turn(不停在工具回合里的 user),但不破 floor
        while cut > floor and not _is_user_text_event(events[cut]):
            cut -= 1
            changed = True
        # (b) 扩展保 tool 配对(缺谁的 tool_use 就回捞),但同样不破 floor
        retained = events[cut:]
        need = set()
        have = set()
        for e in retained:
            need |= _tool_result_ids(e)
            have |= _tool_use_ids(e)
        missing = need - have
        if missing and cut > floor:
            found = False
            for j in range(cut - 1, floor - 1, -1):
                if _tool_use_ids(events[j]) & missing:
                    cut = j
                    changed = True
                    found = True
                    break
            if not found:
                break          # floor 内找不到配对 tool_use → 放弃,_hard_cut 后置兜底
        if not changed:
            break
    # C2(2026-06-24):收敛后**向后(向新)**对齐到 floor 处或之后第一个真 user 文本 turn。
    # 病根:cut 初始化为 floor,回退条件 `while cut > floor and not _is_user_text_event(...)`;
    # 当最优 user 文本轮落在 floor **之上**(index 更小),`cut==floor` 让条件立即 false、
    # 一步不回退 → cut 卡在 floor(常是半截 assistant)→ retained 从半截回复开头。
    # 向后对齐让 happy path 也从干净 user-text 轮起头(与 `_hard_cut` 已有的「floor 之后
    # 第一个 user-text」对齐一致 · 统一两条 path 的开头)。
    #
    # C2 过裁上限(对抗 review · 2026-06-24):向后对齐到 floor 之后第一个干净 user-text
    # turn,但**仅当它仍保住 ~半数 retain 预算**才接受;否则(尾巴是裸 user turn 等)
    # 保留 cut(≥floor 预算),别把 retained 塌成末条。第一个 user-text 是候选里 retained
    # 最大的(k 最小),它都不够半数→后面更不够→break 保 cut。
    for k in range(cut, len(events)):
        if _is_user_text_event(events[k]):
            if estimate_tokens(events[k:]) >= retain_tokens // 2:
                return k
            break
    return cut


# ── 按轮裁剪:计数 + 边界(2026-07-01 · feat/round-based-trim)────────────────────────
def count_user_text_rounds(events):
    """事件序列里**真 user 文本 turn**(一轮的开场 · `_is_user_text_event`)的条数 = 轮数。
    复用 `find_clean_boundary` 已在用的 turn 检测 · 不另造一套。空/无真 user turn → 0。"""
    return sum(1 for e in (events or []) if _is_user_text_event(e))


def find_round_boundary(events, keep_rounds, retain_tokens):
    """**按轮**选 retained 起点(2026-07-01 · 治「切模型 → 灾难性重裁 → 失忆」根因)。

    逻辑:留**最近 keep_rounds 轮** —— 从尾往前数第 keep_rounds 个真 user 文本 turn 的 index
    当 cut(该轮的开场就是干净边界,天然满足 `find_clean_boundary` 的「从真 user 文本 turn 起头」)。
    **模型无关**:留多少轮只看 keep_rounds,不随窗口缩放 → user切模型不再触发重裁。

    **token 安全帽**:按轮选出的 retained 仍受 `retain_tokens`(模型感知)约束 —— 若按轮 cut 的
    retained 估算 **> retain_tokens**,回落更紧的 `find_clean_boundary(events, retain_tokens)`
    (token 裁 · 取两者更靠后 = 裁更多那个,保证既 ≤ 轮数上限、又 ≤ token 上限)。1M 模型下 30 轮
    远 < 375k 帽 → 轮数说了算(实践模型无关);200k 模型下 30 轮若碰巧超 75k → token 帽兜底。

    tool 配对完整性 / 孤儿剔除 / base64 压缩全在下游 `forge_events` 装配里做(这里只定 cut 落点)。
    返回 retained 起始 index。失败安全:keep_rounds<=0 / 无轮 / 异常 → 回落纯 token
    `find_clean_boundary`(绝不比 token 裁更宽松)。"""
    if not events:
        return 0
    kr = int(keep_rounds) if keep_rounds else 0
    token_cut = find_clean_boundary(events, retain_tokens)
    if kr <= 0:
        return token_cut
    # 从尾往前数第 kr 个真 user 文本 turn 的 index(= 留最近 kr 轮的起点)。
    seen = 0
    round_cut = 0                      # 轮数不够 kr(整段 < kr 轮)→ 全留(cut=0)
    for i in range(len(events) - 1, -1, -1):
        if _is_user_text_event(events[i]):
            seen += 1
            if seen == kr:
                round_cut = i
                break
    # token 帽:按轮 cut 的 retained 若超 retain_tokens,取更靠后(裁更多)的 token_cut。
    # max(round_cut, token_cut):谁更靠后(index 更大 = retained 更小 = 裁更多)谁赢 →
    # 同时满足「≤ kr 轮」和「≤ retain token」两个上界(宁严不松)。
    if estimate_tokens(events[round_cut:]) > retain_tokens:
        return max(round_cut, token_cut)
    return round_cut


def has_complete_tool_round(events):
    uses = set()
    results = set()
    for e in events:
        uses |= _tool_use_ids(e)
        results |= _tool_result_ids(e)
    return bool(uses & results)


def _compress_tool_result(e):
    """压缩 tool_result 正文,只留很短摘要(PDF 6.5:别把大块旧输出带回新 session)。"""
    e2 = copy.deepcopy(e)
    msg = e2.get("message")
    if isinstance(msg, dict) and isinstance(msg.get("content"), list):
        for b in msg["content"]:
            if isinstance(b, dict) and b.get("type") == "tool_result":
                c = b.get("content")
                if isinstance(c, str) and len(c) > 200:
                    b["content"] = c[:200] + " …(forge 压缩)"
                elif isinstance(c, list):
                    b["content"] = "(forge 压缩:工具结果省略,仅保结构锚点)"
    return e2


def ensure_tool_primer(retained, all_keepable, min_rounds=1):
    """retained 缺完整工具回合时,从更早 keepable 回捞最近 min_rounds 个完整回合
    (tool_use 的 assistant + 配对 tool_result 的 user),压缩结果正文后插到最前。
    防 forge 后agent丢工具使用惯性(PDF 6.5)。"""
    if has_complete_tool_round(retained):
        return retained
    retained_uuids = {e.get("uuid") for e in retained}
    earlier = [e for e in all_keepable if e.get("uuid") not in retained_uuids]
    rounds = []
    for i in range(len(earlier) - 1, -1, -1):
        uses = _tool_use_ids(earlier[i])
        if not uses:
            continue
        for j in range(i + 1, len(earlier)):
            if _tool_result_ids(earlier[j]) & uses:
                rounds.append((earlier[i], earlier[j]))
                break
        if len(rounds) >= min_rounds:
            break
    if not rounds:
        return retained                      # 没可捞的,原样(调用方记日志)
    primers = []
    for use_ev, res_ev in reversed(rounds):
        primers.append(use_ev)
        primers.append(_compress_tool_result(res_ev))
    return primers + retained


# ── 重写 session 链 ───────────────────────────────────────────────
def rewrite_session_chain(events, new_sid):
    """sessionId 全换 new_sid;uuid/parentUuid 相对 kept 子集重连(第一条
    parentUuid=None 当干净根);tool_use/tool_result 的 id 不动(配对靠 tool_use_id,
    已在 boundary/primer 保完整)。顶层浅拷贝,不改 message 内部。"""
    out = []
    prev_uuid = None
    for e in events:
        e2 = dict(e)
        if "sessionId" in e2:
            e2["sessionId"] = new_sid
        if "uuid" in e2:
            e2["parentUuid"] = prev_uuid
            prev_uuid = e2["uuid"]
        out.append(e2)
    return out


def _drop_orphan_tool_events(events):
    """整对剔除切口处的孤儿 tool 事件 —— 保证留下的事件里 tool 配对完整(整对在/整对
    不在)。Anthropic API 硬约束:孤儿 tool_use(没配对 tool_result)或孤儿 tool_result
    (没配对 tool_use)都会 400。**别用 synthetic tool_result**(API 风险)。

    做法:扫一遍算出 have(全部 tool_use id)∩ need(全部 tool_result id)= 配对完整的
    id 集;任何**只**含未配对 tool_use 或未配对 tool_result 的 block 整块删掉,块删空的
    事件整条丢。保留的事件里 tool 块要么配对完整、要么是纯文本/thinking。"""
    have = set()
    need = set()
    for e in events:
        have |= _tool_use_ids(e)
        need |= _tool_result_ids(e)
    paired = have & need
    out = []
    for e in events:
        msg = e.get("message")
        if not (isinstance(msg, dict) and isinstance(msg.get("content"), list)):
            out.append(e)
            continue
        new_content = []
        for b in msg["content"]:
            if isinstance(b, dict):
                if b.get("type") == "tool_use" and b.get("id") not in paired:
                    continue                       # 孤儿 tool_use → 删
                if b.get("type") == "tool_result" and b.get("tool_use_id") not in paired:
                    continue                       # 孤儿 tool_result → 删
            new_content.append(b)
        if not new_content:
            continue                               # block 删空 → 整条事件丢
        e2 = dict(e)
        e2["message"] = dict(msg)
        e2["message"]["content"] = new_content
        out.append(e2)
    return out


def _hard_cut(events, retain_tokens):
    """硬兜底强切:无论多病态都保证裁到 retain 以内。从**尾**强切到 retain_tokens 以内,
    切口对齐到真 user 文本 turn(优先)或直接按 token floor 切;再**整对剔除**切口处的
    孤儿 tool 对(`_drop_orphan_tool_events`),保证留下事件 tool 配对完整。返回 retained 列表。

    用于 find_clean_boundary 后 retained est 仍 > retain×1.5(没裁到位 = 拉链/病态序列)
    的后置守卫。目标:**保证裁到位**(API 不 400 + 续航生效)。"""
    if not events:
        return []
    floor = _token_floor_index(events, retain_tokens)
    # 优先把切口对齐到 floor 之后**第一个**真 user 文本 turn(干净开头);找不到就用 floor。
    cut = floor
    for i in range(floor, len(events)):
        if _is_user_text_event(events[i]):
            cut = i
            break
    retained = events[cut:]
    # 切口对齐后仍可能含跨切口的孤儿 tool 对(前半截在窗外)→ 整对剔除,保配对完整。
    retained = _drop_orphan_tool_events(retained)
    return retained


# 尾注入分隔符标记(与 backend chat_agent `_user_msg_delim` 对齐 · 取稳定起始子串即可切)。
# chat_agent 每轮把 [时间前缀 + 今天/浮现注入] 拼在这道 ⬇️ 分隔符**之上**、user真话**之下**。
_END_INJECT_DELIM_MARK = "【⬇️ 下面这一句"


def _strip_end_injection(event):
    """剥掉一条 **user 文本 turn** 里 ⬇️ 分隔符之上的尾注入(时间前缀 + 今天/浮现数据),
    只留分隔符之后user的真话。forge 裁时对 retained 每条跑(换 sid 本就冷起 · 剥它不额外破缓存)。

    为什么:尾注入被 SDK 当 user turn 一起写进 transcript、**每轮复利**撑大 transcript → forge
    频繁裁半冷起(实测注入曾占 transcript 72%)。当前轮注入由 chat_agent 每轮 fresh 重注,历史
    那份是陈的 —— 剥了**不失忆**(agent照样每轮看到今天/浮现),只是不再让陈注入白占 retain 预算。
    见 docs/architecture/09-injection-cache-forge.md。

    纯函数 · 浅拷贝不改原 event · **fail-safe**:非 user / content 非 str / 无分隔符 / 切出空 /
    任何异常 → **原样返回**(forge 是agent命脉 · 绝不弄坏 transcript)。用 `rsplit` 取**最后**一道
    分隔符之后(防注入正文恰含分隔串)· 跳过分隔符行尾换行后取真话。"""
    try:
        msg = event.get("message")
        if not isinstance(msg, dict) or msg.get("role") != "user":
            return event
        content = msg.get("content")
        if not isinstance(content, str) or _END_INJECT_DELIM_MARK not in content:
            return event
        tail = content.rsplit(_END_INJECT_DELIM_MARK, 1)[-1]   # 分隔符起始之后(不含标记本身)
        nl = tail.find("\n")                                    # 分隔符那一行到第一个换行为止
        real = (tail[nl + 1:] if nl != -1 else tail).strip()   # 换行之后 = user真话
        if not real:
            return event                                       # 切出空(异常结构)→ 别弄丢整条
        new_event = dict(event)
        new_event["message"] = dict(msg)
        new_event["message"]["content"] = real
        return new_event
    except Exception:  # noqa: BLE001 · 任何异常原样返回 · 不弄坏 transcript
        return event


def forge_events(keepable, new_sid, out_dir, retain_tokens=60000, min_tool_rounds=1,
                 keep_rounds=None):
    """对已读+已 filter 的 keepable 事件做 forge:选边界 → 后置硬兜底强切
    → tool primer → rewrite chain → **确认 est 达标才** 写 <new_sid>.jsonl。

    边界选法(2026-07-01):
      · `keep_rounds` 给正整数 → **按轮**裁(`find_round_boundary` · 留最近 keep_rounds 轮 ·
        模型无关 · 治「切模型灾难重裁」)+ token 帽兜底(retained 仍 ≤ retain_tokens · 超则回落
        token 裁的更靠后点)。这是self-hosted cc 主路(cc_headless / cc_sdk 传本参)。
      · `keep_rounds` 为 None / <=0 → **按 token**裁(`find_clean_boundary` · 历史行为不变)。
    ⚠️ 无论走哪条,retained 起点都是**真 user 文本 turn**(两函数同一 `_is_user_text_event`
    检测)· 下游装配(base64 压缩 / 孤儿剔除 / tool primer / rewrite chain / ForgeNoProgress)
    **完全共用**、一字节不动 —— 本次只改 cut 落在哪,不改任何安全机制。

    2026-06-20 不造孤儿:**先在内存算完 retained、确认 est<=retain×1.5 才落盘**。
    边界没裁到位(拉链 / 病态密集 tool round)→ `_hard_cut` 从尾强切 + 整对剔除孤儿 tool 对
    兜底。强切后仍超 → raise ForgeNoProgress(根本不写文件,不留孤儿给 guardian 反复重试)。
    返回 (new_path, stats)。"""
    if keep_rounds and int(keep_rounds) > 0:
        cut = find_round_boundary(keepable, keep_rounds, retain_tokens)
    else:
        cut = find_clean_boundary(keepable, retain_tokens)
    retained = keepable[cut:]
    hard_threshold = int(retain_tokens * 1.5)
    hard_cut_applied = False
    # 后置守卫:find_clean_boundary 没裁到位(retained est 仍 > retain×1.5)→ 硬兜底强切。
    if estimate_tokens(retained) > hard_threshold:
        retained = _hard_cut(keepable, retain_tokens)
        hard_cut_applied = True
    # ⚠️ API 配对铁律:即便走 find_clean_boundary happy path,floor 也可能挡住「回捞
    # 缺失 tool_use」(`if not found: break`)→ retained 头部残留孤儿 tool_result(其
    # tool_use 在 floor 外)→ Anthropic API 400。**无条件**整对剔除孤儿 tool 对兜底
    # (cheap + 幂等 · _hard_cut 路径已剔过、重剔无害)· 保证落盘事件 tool 配对完整。
    retained = _drop_orphan_tool_events(retained)
    # C3(2026-06-24):retained **主体**里的大 tool_result(Bash/Read 大输出)也压成短摘要 ——
    # `_compress_tool_result` 原来只在 `ensure_tool_primer` 回捞的旧回合上调一次,retained 主体
    # 的大 tool_result 原样带进新 session 白占预算。这里对每条带 tool_result 的事件跑一遍
    # (`_compress_tool_result` 已 deepcopy + 幂等 + 阈值 · 只压 result 正文 · **不动**
    # tool_use_id / tool_use → 配对不破 · primer 路径重压也无害)。`_drop_orphan_tool_events`
    # 之后做,保证此刻只剩配对完整的 tool 对(不会压到将被剔除的孤儿)。
    retained = [_compress_tool_result(e) if _tool_result_ids(e) else e for e in retained]
    # #strip-inject(2026-07-07):retained 里每条 user turn 的尾注入(⬇️ 之上的时间前缀 + 今天/浮现)
    # 剥掉 —— 尾注入被 SDK 固化进 transcript、每轮复利撑大 → forge 频繁裁。裁时换新 sid = 本就冷起,
    # 剥它不额外破缓存;当前轮注入 chat_agent 每轮 fresh 重注 → 剥历史陈注入不失忆。见 09 doc。
    retained = [_strip_end_injection(e) for e in retained]
    before = len(retained)
    retained = ensure_tool_primer(retained, keepable, min_rounds=min_tool_rounds)
    rewritten = rewrite_session_chain(retained, new_sid)
    est = estimate_tokens(rewritten)
    # 源头不造孤儿:确认裁到位才落盘。强切后仍超 retain×1.5 = 真没法裁(几乎不可能,
    # 单条事件就超 retain×1.5 那种病态)→ 不写文件,raise 给调用方兜(不留孤儿)。
    if est > hard_threshold:
        raise ForgeNoProgress(
            f"forge no progress: retained est {est} > retain×1.5 {hard_threshold} "
            f"(keepable={len(keepable)}, retained={len(rewritten)})"
        )
    new_path = os.path.join(out_dir, new_sid + ".jsonl")
    write_jsonl(rewritten, new_path)
    stats = {
        "keepable": len(keepable),
        "retained": len(rewritten),
        "primer_added": len(rewritten) - before,
        "tool_round": has_complete_tool_round(rewritten),
        "est_tokens": est,
        "hard_cut": hard_cut_applied,
        "by_rounds": bool(keep_rounds and int(keep_rounds) > 0),   # 诊断:本次走按轮还是按 token
    }
    return new_path, stats


def forge(transcript_path, new_sid, retain_tokens=60000, min_tool_rounds=1, keep_rounds=None):
    """读 → filter → forge_events → 写 <new_sid>.jsonl(同 project dir)。
    `keep_rounds` 正整数 → 按轮裁(模型无关 · token 帽兜底);None/<=0 → 按 token 裁(历史行为)。
    返回 (new_path, stats)。异常由调用方兜(失败安全)。"""
    keepable = filter_keepable(read_jsonl(transcript_path))
    return forge_events(keepable, new_sid, os.path.dirname(transcript_path),
                        retain_tokens, min_tool_rounds, keep_rounds=keep_rounds)


# ── Route Guard ───────────────────────────────────────────────────
def normalize_model(m):
    return (m or "").strip().lower().replace("_", "-")


# 上下文窗口变体后缀(如 `[1m]` = 选 1M 窗口)· 只选窗口大小,不是不同模型。
_CTX_VARIANT_RE = re.compile(r"\[[^\]]*\]")


def route_model_key(m):
    """「是不是同一个模型」的比较 key = normalize 后再去掉窗口变体后缀(`[1m]` 等)。
    agent pin `claude-opus-4-6[1m]` 但 CC 回报的 `message.model` 常是裸 `claude-opus-4-6`
    (后缀只选窗口、不是新模型)· 若 Route Guard 按裸串精确比 → agent自己每条回复都被判
    「漂移」→ forge 当路由污染裁掉 → 每轮失忆(2026-06-26 同类根因)。故比较对窗口后缀
    不敏感。**只用于比较**:`model_window` 查表仍用带后缀的 normalize_model —— 要精确命中
    `[1m]` 的 1M fallback(见 MODEL_WINDOW_FALLBACK)· 不能在那里剥后缀。"""
    return _CTX_VARIANT_RE.sub("", normalize_model(m))


# ── 模型感知 forge 续航阈值(2026-06-27 · #173 · 不写死,跟所选模型的上下文窗口走)──────
# 为什么不能写死:agent可在 ⋮ 菜单切模型,各模型上下文窗口不同 —— opus-4-6 / sonnet-4-6 = 200k,
# **opus-4-8 = 1M**(Pi `claude -p` result.modelUsage 实测)。写死 140k 对 200k 模型安全(抢在
# CC ~200k autocompact 前裁)但对 1M 模型严重浪费记性;反过来按 1M 拍 200k+ 阈值用在 200k 模型
# 上 → CC 抢先 autocompact 揉烂注入 → 失忆(6/26 根因)。所以阈值必须 = f(当前模型窗口)。
#
# 窗口来源(三级 · 不写死):**运行时**从 CC `result` 事件 `modelUsage.<model>.contextWindow`
# 学到 + 持久(`remember_model_windows`)> 冷启 fallback 表 > 200k 默认。trigger/retain 都从
# **CC 自己报的窗口**算 → forge 裁切与 CC autocompact 用同一个数、天然一致(不靠猜)。
#
# 阈值公式(user 2026-06-27 拍「裁切保持在 CC 自动压缩线以内 ~75%」+ 保留肥图轮 headroom):
#   trigger    = min(window×0.75, window−60k)   —— 大窗口(1M)按 ~75% 封顶(留 ~25% 给 CC
#                ~92% autocompact 线之下);小窗口(200k)由 60k 绝对 headroom 兜底(单条 base64
#                肥图轮可 +~60k · #115)。→ 200k→140k(= 历史 proven 值)· 1M→750k。
#   retain     = window×0.375                    —— 200k→75k(历史值)· 1M→375k。
#   resume_max = window×1.5                       —— 「可直接 resume」上限(raw 估算含 base64 虚高 ·
#                故留 1.5× 宽容)。**必须 ≥ trigger**:否则 trigger 与 resume_max 之间的健康
#                session 会在 forge 裁它之前就被 resume 闸判成「巨无霸」丢弃 → 1M 上 400k 的好
#                session 没等到 750k 裁就被弃 → 失忆。→ 200k→300k(= 历史值)· 1M→1.5M。
# ⚠️ 关键安全属性:**window=200k 时三者 == 历史写死常量 (140k, 75k, 300k)** → 对 4.6/sonnet
#    完全是 no-op(行为不变),只有 CC 报 1M 的模型才用上更大的阈值。
_FORGE_STATE_DIR = os.path.abspath(os.path.expanduser(
    os.environ.get("AI_RELAY_STATE_DIR", "~/.local/state/ai-session-relay")
))
os.makedirs(_FORGE_STATE_DIR, exist_ok=True)
_MODEL_WINDOWS_FILE = os.path.join(_FORGE_STATE_DIR, "model_windows.json")
FORGE_TRIGGER_RATIO = 0.75
FORGE_RETAIN_RATIO = 0.375
FORGE_FAT_TURN_HEADROOM = 60_000
FORGE_RESUME_MAX_RATIO = 1.5
# 2026-06-30 · 大窗口(1M)绝对封顶 —— self-hosted cc 会话保持小(user要「剪到 ~30-60 轮」)。不封顶时
# opus-4-8 会涨到 750k-91万 token 逼近 1M 溢出 + 反复 forge 切致冷启 + 每轮全价重读巨上下文。
# 老对话不丢:压给 DAG vault(session_start @vault_recall 召回)。⚠️ 只夹**大窗口**:200k 模型
# trigger=140k/retain=75k 本就 < cap → no-op(不破 4.6/sonnet 历史安全属性)。可调:agent忘事抬
# RETAIN_CAP(80k≈30轮 → 120k≈60轮);冷启还多降 TRIGGER_CAP。
FORGE_TRIGGER_CAP = 250_000   # cut 上界(大窗口 fable/opus-4-8)· 2026-07-06 400k→250k(user:省 fable token)
                              # ≈ 地板(~117k:人设 52k + 20 轮 retain ~65k)+ √(40·地板·每轮增长~4k) ≈ 最优点。
                              # 133k 余量 → ~33 轮/forge · 不撞地板(≠ 2026-07-05 试的 200k:那时地板~200k 卡触发线狂裁)。
                              # 只影响 1M 模型(fable/opus-4-8);200k 模型(4.6/sonnet)trigger=window-60k=140k < cap 不受此帽。
FORGE_RETAIN_CAP = 80_000     # 切后保留上界(大窗口)≈ 近 30 轮(本来 1M→375k)· 余下走 DAG
# 冷启 fallback(运行时学到的会覆盖)· claude-opus-4-8=1M 系 2026-06-27 Pi 实测;其余保守按
# 官方标准 200k(真值会被运行时观测覆盖 · 保守=宁可早裁不晚裁致失忆)。
MODEL_WINDOW_FALLBACK = {}
DEFAULT_MODEL_WINDOW = 200_000   # 未知模型保守兜底

# ── 按轮裁剪(round-based retention · 2026-07-01 · feat/round-based-trim)──────────────
# 病根(2026-06-30 实锤 · 一天切 4 次 · 当前 session 丢了 15:32 前所有内容):forge 的 retain
# 目标一直是**纯 token 阈值**(`continuation_thresholds(model).retain` = window×0.375,大窗口再被
# `FORGE_RETAIN_CAP` 夹)。这套阈值**随模型窗口缩放** → user在 ⋮ 菜单**切模型**时,一个在 1M 窗口
# 模型(opus-4-8/4-7 · 学到 1M)下健康的 session,忽然被 200k 模型的 trigger(140k)判成超标 →
# forge 按 200k 的 retain(75k)猛裁 → 当天大半对话被丢 → agent失忆。**切模型不该触发灾难性重裁。**
#
# user决定:retain 目标改**按轮**(round)—— 留最近 ~N 轮,更老的交给 DAG vault(chat_messages +
# summary_nodes 是 lossless 的,裁掉不等于丢)。轮数**与模型窗口无关** → 切模型不再触发重裁,这是
# 治本。**外加 token 安全帽**:按轮选出的 retained 仍不许超当前模型的 retain 预算(小窗口 200k 兜底
# ~75k),超了就回落更紧的 `find_clean_boundary`(token 裁)。
#
# 一「轮」的定义:一个**真 user 文本 turn**(`_is_user_text_event` · content=str)+ 其后跟着的
# assistant / 工具事件,直到下一个真 user 文本 turn。**复用** `find_clean_boundary` 已在用的
# turn 检测(`_is_user_text_event`)· 不另造一套。轮边界 = 真 user 文本事件的 index。
#
# 实践上的模型无关性:大窗口模型下 20 轮远不到 retain 帽 → 轮数说了算(切进/切出
# 1M 模型都留同样 20 轮);200k 模型下若 20 轮碰巧超 75k,token 帽兜底裁到 ~75k(安全,极少见)。
# 2026-07-06:30→20(user省缓存)· 每轮都整份重读 retained transcript · 少留 10 轮 = 每轮读更省 +
# forge 触发线更远、冷起更少。20 轮仍够工作记忆,更早的走 DAG recall。
FORGE_KEEP_ROUNDS = int(os.getenv("FORGE_KEEP_ROUNDS", "20"))
# 粗触发的轮数余量:raw 轮数超过 (keep_rounds + margin) 也触发 forge(即便 token 还没到 trigger)。
# 保守给宽 —— 宁可少切(留更多轮),别每来一两轮就 forge。默认 20:60 轮才碰这条(30+20+缓冲)。
FORGE_ROUND_TRIGGER_MARGIN = int(os.getenv("FORGE_ROUND_TRIGGER_MARGIN", "20"))

Thresholds = collections.namedtuple("Thresholds", ["trigger", "retain", "resume_max"])


def _load_model_windows():
    """读已学到的窗口(`{normalize_model: window_int}`)· 每次读盘(进程间共享:cc_engine 学、
    guardian 读)· 失败安全 → 空 dict(回落 fallback 表)。文件小,频率低,直接读不缓存。"""
    try:
        with open(_MODEL_WINDOWS_FILE, encoding="utf-8") as f:
            d = json.load(f)
        return {normalize_model(k): int(v) for k, v in (d or {}).items()
                if isinstance(v, (int, float)) and v > 0}
    except Exception:  # noqa: BLE001
        return {}


def remember_model_windows(model_usage):
    """从 CC `result` 事件的 `modelUsage`(`{model: {contextWindow, ...}}`)学窗口 → 合并持久
    (原子替换 · 失败安全)。下一轮 forge 阈值即用真实窗口,不靠 fallback 猜。"""
    if not isinstance(model_usage, dict):
        return
    learned = _load_model_windows()
    changed = False
    for mid, info in model_usage.items():
        if not isinstance(info, dict):
            continue
        w = info.get("contextWindow")
        if isinstance(w, (int, float)) and w > 0:
            key = normalize_model(mid)
            if key and learned.get(key) != int(w):
                learned[key] = int(w)
                changed = True
    if changed:
        try:
            tmp = _MODEL_WINDOWS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(learned, f)
            os.replace(tmp, _MODEL_WINDOWS_FILE)   # 原子 · 防半截写被读到
        except Exception:  # noqa: BLE001 · 持久失败不致命(下轮再学)
            pass


def model_window(model):
    """所选模型上下文窗口(token)= 运行时学到 > fallback 表 > 默认 200k。"""
    key = normalize_model(model)
    if key:
        learned = _load_model_windows()
        if key in learned:
            return learned[key]
        if key in MODEL_WINDOW_FALLBACK:
            return MODEL_WINDOW_FALLBACK[key]
    return DEFAULT_MODEL_WINDOW


def continuation_thresholds(model):
    """据当前模型窗口算 forge 续航三阈值 `Thresholds(trigger, retain, resume_max)`(见上方注释)。
    window=200k 时 == 历史写死常量 (140000, 75000, 300000)。保证 retain < trigger ≤ resume_max。"""
    w = model_window(model)
    trigger = min(int(w * FORGE_TRIGGER_RATIO), w - FORGE_FAT_TURN_HEADROOM)
    retain = int(w * FORGE_RETAIN_RATIO)
    resume_max = int(w * FORGE_RESUME_MAX_RATIO)
    # 2026-06-30 · 大窗口绝对封顶(self-hosted cc 会话保持小 · 见 FORGE_*_CAP)。只夹大窗口:
    # 200k 模型 trigger=140k/retain=75k 本就 < cap → 这两行 no-op(历史安全属性不破)。
    # resume_max 不夹(会话本就被 trigger 压在 cap 下 · 留 1.5M 宽容兜 raw 估算虚高)。
    trigger = min(trigger, FORGE_TRIGGER_CAP)
    retain = min(retain, FORGE_RETAIN_CAP)
    # 防御性夹取(极小/异常窗口下别倒置):retain < trigger ≤ resume_max。
    retain = min(retain, max(trigger - 10_000, 1))
    resume_max = max(resume_max, trigger)
    return Thresholds(trigger, retain, resume_max)


def last_assistant_model(events):
    """transcript 里**最后一条** assistant 事件的 model(normalize 过)· 读不到 → None。
    用途:guardian 按 session **实际**模型(非意图模型)定 forge 阈值 · 避免漂移误判窗口。"""
    try:
        for e in reversed(events or []):
            if e.get("type") == "assistant":
                m = normalize_model((e.get("message") or {}).get("model"))
                if m:
                    return m
    except Exception:  # noqa: BLE001
        return None
    return None


def scan_route_guard(events, target_model):
    """PDF 7.3:扫 assistant 事件的 message.model,找第一次偏离 target 的位置。
    返回 None(无漂移)或 {drift_index, anchor_index, target_model, actual_model}。"""
    tgt = route_model_key(target_model)   # 窗口后缀不敏感(`[1m]` 与裸串视为同模型)
    if not tgt:
        return None
    for i, e in enumerate(events):
        if e.get("type") != "assistant":
            continue
        msg = e.get("message")
        actual = normalize_model(msg.get("model") if isinstance(msg, dict) else None)
        if actual and route_model_key(actual) != tgt:
            return {
                "drift_index": i,
                "anchor_index": _find_clean_boundary_before(events, i),
                "target_model": tgt,
                "actual_model": actual,
            }
    return None


def _find_clean_boundary_before(events, idx):
    """漂移点前找干净锚:最近一个不含 tool_result 的 user 事件 index。"""
    for j in range(idx - 1, -1, -1):
        e = events[j]
        if e.get("type") == "user" and not _tool_result_ids(e):
            return j
    return 0


def route_guard_check(keepable, target_model):
    """监控用(对 keepable 事件):只看**最新** assistant 事件的模型是否偏离 target。
    历史里的旧模型(user之前主动切过)不算漂移 —— 只判最新一条。
    返回 {drifted, actual_model, keep_until}:
      drifted=True 时 keep_until = 最后一个 model==target 的 assistant 在 keepable 里的
      index(forge 时保 keepable[:keep_until+1],丢后面被路由污染的漂移段)。
      若整段都没 target 模型的回复(keep_until=None),调用方应放弃自动恢复、只告警。"""
    tgt = route_model_key(target_model)   # 窗口后缀不敏感(`[1m]` 与裸串视为同模型)
    if not tgt:
        return {"drifted": False}
    asst_idx = [i for i, e in enumerate(keepable) if e.get("type") == "assistant"]
    if not asst_idx:
        return {"drifted": False}
    actual = normalize_model((keepable[asst_idx[-1]].get("message") or {}).get("model"))
    if not actual or route_model_key(actual) == tgt:
        return {"drifted": False, "actual_model": actual}
    keep_until = None
    for i in reversed(asst_idx):
        if route_model_key((keepable[i].get("message") or {}).get("model")) == tgt:
            keep_until = i
            break
    return {"drifted": True, "actual_model": actual, "keep_until": keep_until}


# ── 本轮落库内容:从 transcript 重建权威 parts(6/18) ──────────────────
# 背景:cc_engine 实时逐字 delta 走「抠屏」(够用)· 但收尾权威 parts 改从 transcript
# 重建 —— transcript 完整、结构化、零噪声(spinner / 命令输出 / 满意度问卷天然不在里头)·
# 治两个抠屏老病:① 长正文被屏高截断;② 工具输出被当成agent的话气泡。
#
# transcript 真实结构(本地实测,active sid 4c018d6a):
#   · 一行一事件 type ∈ {user, assistant, attachment, system, file-history-snapshot,
#     last-prompt, mode, permission-mode, ...}。
#   · **真 user 文本事件**(= 一轮的边界):type=="user" 且 message.content 是 **str**
#     (注入了记忆上下文的 prompt · 形如 `[现在 2026-06-18 22:51 ...]`)。
#     工具回合里的 user 事件 message.content 是 **list**(含 tool_result),不是边界。
#   · **assistant 事件 = 单 content block**:thinking / text / tool_use 各自独立成事件
#     (不是同一事件多 block)。text 块 `{"type":"text","text":"[06/18 22:51]\n\n正文…"}`,
#     正文 markdown / 换行 / 审计 tag(`<memories_used>…`)全在;段2+(查完工具后)
#     往往**不带** `[时间戳]` 前缀。
#   · 同轮「段-工具-段」真实存在(实测一轮 5 text / 4 tool):
#     text(段1) → tool_use+tool_result → text(段2) → … 顺序 = 屏上顺序。
#
# friendly 名映射跟抠屏版 `_tool_step_label` 对齐(Bash→执行中… / mcp__*memor*→查记忆中…)·
# 但**用 transcript 的 tool_use.name**(干净、不靠解析屏行)。
_TOOL_LABELS = {
    "Read": "读取中…", "Bash": "执行中…", "Edit": "修改中…",
    "Write": "写入中…", "Grep": "搜索中…", "Glob": "查找文件…",
    "Task": "执行任务…", "TodoWrite": "记笔记…", "WebSearch": "联网搜索…",
    "WebFetch": "读网页…", "MultiEdit": "修改中…", "NotebookEdit": "改笔记…",
}

# 段2+ 文本块开头可能残留的 `[MM/DD HH:MM]` 时间戳前缀(跟 cc_engine 抠屏版剥法一致)。
_TS_PREFIX_RE = re.compile(r"^\[\d{1,2}/\d{1,2}[^\]]*\]\s*")


def tool_name_label(name):
    """tool_use.name → 给前端显示的友好小字(跟 cc_engine `_tool_step_label` 对齐)。
    `Bash`→执行中… · `mcp__memory__search_memories`→查记忆中… · 未知→`<Name>…`。"""
    if not name:
        return "调用工具中…"
    if name.startswith("mcp__"):
        low = name.lower()
        if "memor" in low or "memory" in low:
            return "查记忆中…"
        return "调用工具中…"
    return _TOOL_LABELS.get(name, f"{name}…")


def _is_user_text_event(e):
    """真 user 文本事件(一轮边界):type=="user" 且 message.content 是 str。
    工具回合里的 user(content=list 含 tool_result)不算边界。"""
    if e.get("type") != "user":
        return False
    msg = e.get("message")
    return isinstance(msg, dict) and isinstance(msg.get("content"), str)


def _assistant_blocks(e):
    """assistant 事件的 content block 列表(单 block 也包成 list)。非 assistant→[]。"""
    if e.get("type") != "assistant":
        return []
    msg = e.get("message")
    if not isinstance(msg, dict):
        return []
    c = msg.get("content")
    if isinstance(c, list):
        return [b for b in c if isinstance(b, dict)]
    return []


def _strip_ts_prefix(text):
    """剥掉文本开头可能残留的 `[MM/DD HH:MM]` 时间戳前缀(段2+ 通常没有,有就去掉)。
    只剥**最前面**一个 · 保留正文里其余所有内容(含正文中间的方括号)。"""
    return _TS_PREFIX_RE.sub("", text or "", count=1)


def read_turn_parts(transcript_path, base_marker=None, keep_thinking=False):
    """从 transcript 重建agent**本轮**的权威落库 parts(段-工具-段 全保留)。

    本轮边界:`base_marker`(发消息前记的「当时最后一个 assistant uuid」)之后,
    或回落到「最后一条**真 user 文本事件**(content 是 str)」之后 —— 两者取**更靠后**
    那个 index 当起点(base_marker 优先 · 它是发消息那一刻的快照、最准;真 user 文本事件
    兜底,防 base_marker 没传/没命中)。从起点之后收所有 assistant block:
      · text  → `{"type":"text","text": 完整正文}`(剥开头时间戳 · markdown/审计 tag 全留)。
      · thinking → **默认丢弃**(不进 parts);`keep_thinking=True` 时产出
        `{"type":"reasoning","text": <thinking 明文>}`(给 headless done 带回思考链 ——
        2026-06-21 `claude -p stream-json` 实时 thinking_delta 恒空,只 transcript 有明文)。
      · 连续 tool_use(配对的 tool_result 在 user 事件里 · 这里不需要 result 正文)→
        合并成一个 `{"type":"tool","steps":[友好名…]}`(顶端工具小字)· name 映射 friendly。
    顺序 = 事件顺序(transcript 行序 = parentUuid 链序 = 屏上顺序)。

    keep_thinking 默认 False · 保持现有调用(cc_engine 落库)行为不变(thinking 仍丢);
    headless 取思考链的新调用显式传 True。

    返回 list[{"type":"text"|"reasoning","text":str} | {"type":"tool","steps":list[str]}],
    无内容→[]。读失败 / 文件不存在等异常**向上抛**,由调用方(cc_engine)回落抠屏(失败安全)。"""
    events = read_jsonl(transcript_path)        # 异常上抛 → 调用方兜
    if not events:
        return []

    # 1) 定起点 index(收 start 之后的事件)。
    start = -1
    # 1a) base_marker(发消息那刻的最后 assistant uuid)命中 → 从它之后收。
    if base_marker:
        for i, e in enumerate(events):
            if e.get("uuid") == base_marker:
                start = i
                break
    # 1b) 最后一条真 user 文本事件 → 兜底边界 · 跟 base_marker 取更靠后那个。
    last_user_text = -1
    for i, e in enumerate(events):
        if _is_user_text_event(e):
            last_user_text = i
    start = max(start, last_user_text)

    # 2) 从 start 之后顺序收 assistant block,段-工具-段成 parts。
    parts = []
    pending_steps = []          # 当前累积的连续 tool_use 友好名

    def _flush_tools():
        if pending_steps:
            parts.append({"type": "tool", "steps": list(pending_steps)})
            pending_steps.clear()

    for e in events[start + 1:]:
        for b in _assistant_blocks(e):
            bt = b.get("type")
            if bt == "thinking":
                if keep_thinking:
                    _flush_tools()              # thinking 块在文本/工具之间 · 保序
                    th = (b.get("thinking") or "").strip()
                    if th:
                        parts.append({"type": "reasoning", "text": th})
                continue                        # 默认:thinking 不进 parts
            if bt == "tool_use":
                pending_steps.append(tool_name_label(b.get("name")))
                continue
            if bt == "text":
                _flush_tools()                  # 工具块在两段文本之间 · 先落地
                txt = _strip_ts_prefix(b.get("text") or "").strip()
                if txt:
                    parts.append({"type": "text", "text": txt})
            # 其它 block 类型(罕见)忽略。
    _flush_tools()                              # 收尾残留工具步骤(agent查完没再说话)
    return parts


def read_turn_activities(transcript_path, base_marker=None):
    """Rebuild one turn's full public Claude Code tool log from transcript.

    ``read_turn_parts`` intentionally keeps only compact labels for old bubble
    splitting.  This companion preserves tool inputs and results for the PWA
    activity drawers, and also covers the legacy PTY path after reload.
    """
    events = read_jsonl(transcript_path)
    if not events:
        return []

    start = -1
    if base_marker:
        for i, event in enumerate(events):
            if event.get("uuid") == base_marker:
                start = i
                break
    for i, event in enumerate(events):
        if _is_user_text_event(event):
            start = max(start, i)

    activities = []
    pending: dict[str, dict] = {}
    for event in events[start + 1:]:
        if event.get("type") == "assistant":
            for block in _assistant_blocks(event):
                if block.get("type") != "tool_use":
                    continue
                tool_id = block.get("id")
                if not isinstance(tool_id, str) or not tool_id:
                    continue
                activity = activity_protocol.claude_activity(
                    block.get("name"), block.get("input"), activity_id=tool_id,
                    stage="running",
                )
                pending[tool_id] = {"name": block.get("name"), "input": block.get("input")}
                activities = activity_protocol.merge_activities(activities, [activity])
        if event.get("type") != "user":
            continue
        content = (event.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            tool_id = block.get("tool_use_id")
            known = pending.get(tool_id) if isinstance(tool_id, str) else None
            if not known:
                continue
            activity = activity_protocol.claude_activity(
                known.get("name"), known.get("input"), block.get("content"),
                activity_id=tool_id, stage="completed", is_error=bool(block.get("is_error")),
            )
            activities = activity_protocol.merge_activities(activities, [activity])
    return activities


def read_turn_visible_events(transcript_path, base_marker=None):
    """Rebuild one completed Claude turn's public chronological event timeline.

    This is the authoritative display order, so ordinary assistant text is an
    event too.  A consumer can therefore replay ``thinking -> text -> tool ->
    text`` without hoisting work above the answer or rendering the answer a
    second time from a separate bucket.  Tool results update the original tool
    slot rather than creating a second event.

    Transcript parsing is fail-open at callers: this helper raises its normal
    read errors so callers can keep a successful chat turn even when a
    transcript is unavailable.
    """
    events = read_jsonl(transcript_path)
    if not events:
        return []

    start = -1
    if base_marker:
        for i, event in enumerate(events):
            if event.get("uuid") == base_marker:
                start = i
                break
    for i, event in enumerate(events):
        if _is_user_text_event(event):
            start = max(start, i)

    visible_events = []
    event_index_by_id = {}
    pending = {}

    def _upsert(activity):
        """Replace a tool result in place while retaining its original slot."""
        if not isinstance(activity, dict):
            return
        activity_id = activity.get("id")
        if not isinstance(activity_id, str) or not activity_id:
            return
        previous_index = event_index_by_id.get(activity_id)
        if previous_index is None:
            event_index_by_id[activity_id] = len(visible_events)
            visible_events.append(activity)
            return
        previous = visible_events[previous_index]
        merged = {**previous, **activity}
        if not activity.get("detail") and previous.get("detail"):
            merged["detail"] = previous["detail"]
        visible_events[previous_index] = merged

    for event_number, event in enumerate(events[start + 1:], start + 1):
        if event.get("type") == "assistant":
            assistant_id = event.get("uuid") or event_number
            for block_number, block in enumerate(_assistant_blocks(event)):
                block_type = block.get("type")
                if block_type == "thinking":
                    # This is the same Claude-visible thinking field used by
                    # read_turn_parts(..., keep_thinking=True); signatures and
                    # any other raw/private reasoning payloads are never read.
                    thinking = (block.get("thinking") or "").strip()
                    if thinking:
                        visible_events.append({
                            "id": f"thinking:{assistant_id}:{block_number}",
                            "kind": "thinking",
                            "source": "cc",
                            "text": thinking,
                        })
                    continue
                if block_type == "text":
                    body = _strip_ts_prefix(block.get("text") or "").strip()
                    if body:
                        visible_events.append({
                            "id": f"text:{assistant_id}:{block_number}",
                            "kind": "text",
                            "source": "cc",
                            "text": body,
                        })
                    continue
                if block_type != "tool_use":
                    continue

                raw_tool_id = block.get("id")
                tool_id = raw_tool_id if isinstance(raw_tool_id, str) and raw_tool_id else (
                    f"transcript-tool:{event_number}:{block_number}"
                )
                activity = activity_protocol.claude_activity(
                    block.get("name"), block.get("input"), activity_id=tool_id,
                    stage="running",
                )
                activity["source"] = "cc"
                pending[tool_id] = {
                    "name": block.get("name"),
                    "input": block.get("input"),
                }
                _upsert(activity)
            continue

        if event.get("type") != "user":
            continue
        content = (event.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            tool_id = block.get("tool_use_id")
            known = pending.get(tool_id) if isinstance(tool_id, str) else None
            if not known:
                continue
            activity = activity_protocol.claude_activity(
                known.get("name"), known.get("input"), block.get("content"),
                activity_id=tool_id, stage="completed", is_error=bool(block.get("is_error")),
            )
            activity["source"] = "cc"
            _upsert(activity)

    return visible_events
