"""
#346 · Codex 引擎 token 用量映射(纯函数 · 无 DB / 网络)。

守user反馈的「token 数出错」根因:`thread/tokenUsage/updated.tokenUsage` 实测形状
`{total:{…累计整个 thread…}, last:{…本轮…}}`,必须取 `last`,取 `total` 会把整段 thread
累计当「这一轮」显示、随 thread 越滚越大。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import codex_engine  # noqa: E402


def test_map_usage_prefers_last_not_total():
    # last(本轮)跟 total(累计)故意给不同数 → 断言取的是 last。
    tu = {
        "total": {"totalTokens": 99999, "inputTokens": 90000,
                  "cachedInputTokens": 80000, "outputTokens": 9999},
        "last": {"totalTokens": 500, "inputTokens": 400,
                 "cachedInputTokens": 300, "outputTokens": 100,
                 "reasoningOutputTokens": 40},
        "modelContextWindow": 258400,
    }
    u = codex_engine._map_usage(tu)
    assert u == {
        "input_tokens": 400,
        "cached_input_tokens": 300,
        "output_tokens": 100,
        "reasoning_output_tokens": 40,
        "total_tokens": 500,          # 本轮 · 不是 99999 累计
        "model_context_window": 258400,
    }
    # Codex 契约不伪造 CC cache creation/write/new 字段。
    assert not ({"new", "cache_read", "cache_write", "cache_write_known"} & u.keys())


def test_map_usage_falls_back_to_total_when_no_last():
    # 冷起首轮引擎可能只给 total(或 last 缺)→ 回落 total,不崩。
    tu = {"total": {"totalTokens": 12245, "inputTokens": 12159,
                    "cachedInputTokens": 9984, "outputTokens": 86}}
    u = codex_engine._map_usage(tu)
    assert u["total_tokens"] == 12245
    assert u["cached_input_tokens"] == 9984
    assert u["input_tokens"] == 12159
    assert u["model_context_window"] is None


def test_map_usage_empty_safe():
    for empty in (None, {}, {"last": {}}):
        u = codex_engine._map_usage(empty)
        assert u == {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
            "model_context_window": None,
        }
