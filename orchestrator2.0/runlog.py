# -*- coding: utf-8 -*-
"""运行日志（runlog）：把每轮对话的关键中间产物落盘为 JSONL，供排障。

记录内容（事件 type，见 pipeline.py / store.py / gateway.py / server.py 埋点）：
    turn      本轮输入（conv/mode/query/state 摘要）
    intent    意图识别：完整提示词 + LLM 原始输出 + 解析结果 + 兜底路径
    llm       每次流式回答：阶段名 + 输出全文 + 耗时
    search    候选搜索结果摘要
    adhoc     LLM 生成的 Cypher（原文 + 参数 + 行数/错误）
    graph     发给图数据库的每条 Cypher（全文 + 参数 + 行数/错误/耗时）
    state     会话状态变迁（selected/候选数）
    error     未预期异常
    turn_end  本轮收尾（耗时/中断/错误标记/回答全文）

开关：运行时键 debug_log（界面「运行日志」面板热切换，settings.py 映射到
config.DEBUG_LOG）。默认开。文件超 RUNLOG_MAX_BYTES 轮转到 .1（覆盖旧备份）。
查看：页面侧栏「运行日志」按钮（/api/runlog* 接口，见 server.py）。

设计约束：日志绝不能影响主流程——event() 整体 try/except 吞错；写盘加进程
内锁（Flask threaded + SSE 在子线程里跑生成器，存在并发写）。
"""
from __future__ import print_function

import io
import json
import os
import threading
import time

import config

try:
    unicode     # py2
except NameError:
    unicode = str    # py3

RUNLOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "runlog.jsonl")
RUNLOG_MAX_BYTES = 20 * 1024 * 1024   # 超过则轮转 runlog.jsonl -> runlog.jsonl.1
_TAIL_WINDOW = 512 * 1024             # tail() 最多回读的窗口大小
_LOCK = threading.Lock()

# 显式真值表：config.get 对字符串 "0" 返回的是真值，不能直接 if 判断
_TRUTHY = (True, 1, u"1", "1", u"on", "on", u"true", "true")


def enabled():
    """记录开关：runtime debug_log / config.DEBUG_LOG，默认开"""
    try:
        v = config.get("DEBUG_LOG", u"1")
    except Exception:
        return True
    return v in _TRUTHY


def detail_enabled():
    """诊断明细开关（§13.7）：runtime 键 debug_log_detail，默认开。
    开=完整记录（Cypher 原文、LLM 指令与上下文全文、实体名称/参数值落盘），
    排障直接可读；关=瘦身模式（只记摘要、预览与计数，涉密场景可用）。
    该开关只影响日志内容，不影响任何查询结果。"""
    try:
        v = config.get("DEBUG_LOG_DETAIL", u"1")
    except Exception:
        return True
    return v in _TRUTHY


def _digest(v):
    """脱敏摘要：md5 前 8 位（会话 ID / 实体 ID / 名称等标识类字段）。"""
    import hashlib
    return hashlib.md5(u"{}".format(v if v is not None else u"")
                       .encode("utf-8")).hexdigest()[:8]


_SLIM_TEXT_MAX = 120
# 瘦身模式下做内容预览截断的字段（提示词/原始输出/回答全文/用户输入等）
_SLIM_TEXT_FIELDS = (u"prompt", u"context", u"raw", u"raw_text",
                     u"raw_retry", u"text", u"answer", u"query",
                     u"keyword", u"prompt_head", u"output", u"note",
                     u"error", u"err", u"repair_error", u"parsed")
# 瘦身模式下做脱敏摘要的字段（会话 ID 等）
_SLIM_HASH_FIELDS = (u"conv",)
# 瘦身模式下只留结构不留名称的字段
_SLIM_ENTITY_FIELDS = (u"selected", u"entity", u"subject")


def cypher_digest(statement, parameters=None):
    """§13.7 Cypher 瘦身摘要：只记 结构要素 + 长度 + 哈希，不记原文。
    结构要素 = 涉及的标签/关系类型（各取前 5 个）+ LIMIT 值。
    gateway.graph_run 的唯一 Cypher 埋点出口使用。"""
    try:
        s = statement
        if isinstance(s, bytes):
            s = s.decode("utf-8", "replace")
        s = u"{}".format(s or u"")
        import re as _re
        labels = _re.findall(u"[`:]`?([^`\\s()\\[\\],.:;|]+)`?", s)
        rels = _re.findall(u"[:\\-]+\\[`?([^`\\]\\s|,]+)`?\\]*->|\\[:`?([^`\\]]+)`?\\]",
                           s)
        rel_names = []
        for pair in rels:
            for x in pair:
                if x and x not in rel_names:
                    rel_names.append(x)
        lim = u""
        m = _re.search(u"LIMIT\\s+(\\d+)", s, _re.I)
        if m:
            lim = m.group(1)
        labels = [x for x in labels if x and x.upper() not in
                  (u"MATCH", u"WHERE", u"RETURN", u"WITH", u"OPTIONAL",
                   u"UNWIND", u"ORDER", u"BY", u"LIMIT", u"AS", u"DISTINCT",
                   u"COUNT", u"AND", u"OR", u"NOT", u"IN", u"CONTAINS")][:5]
        return (u"cypher#{} lbl=[{}] rel=[{}] limit={} len={}".format(
            _digest(s), u",".join(labels[:5]),
            u",".join(rel_names[:5]), lim or u"-", len(s)))
    except Exception:
        return u"cypher#(摘要生成失败)"


def param_keys(parameters=None):
    """§13.7 参数瘦身：只记参数键名（排序后逗号连接），不记参数值。"""
    try:
        if not isinstance(parameters, dict):
            return u""
        keys = sorted(u"{}".format(k) for k in parameters.keys())
        return u",".join(keys)
    except Exception:
        return u""


def _clean(v):
    """值清洗：bytes -> utf-8 文本（py2 下 LLM/图库返回可能是 str），容器递归"""
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return repr(v)
    if isinstance(v, dict):
        return {_clean(k): _clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v]
    return v


def _slim_value(v, key=None, depth=0):
    """瘦身模式字段处理：长文本→预览+长度；标识→摘要；Cypher→摘要；
    参数→键名；实体→结构（类型+摘要，不留名称）。递归上限 4 层。"""
    if depth > 4:
        return u"…"
    if isinstance(v, bytes):
        v = v.decode("utf-8", "replace")
    if key in _SLIM_HASH_FIELDS and v not in (None, u""):
        return _digest(v)
    if isinstance(v, unicode):
        s = v
        if key == u"cypher" and s:
            return cypher_digest(s)
        if key in _SLIM_ENTITY_FIELDS and s:
            return _digest(s)   # 实体名称/类型字段即使是短字符串也脱敏
        if s and len(s) > _SLIM_TEXT_MAX:
            return s[:_SLIM_TEXT_MAX] + u"…(len={})".format(len(s))
        return s
    if isinstance(v, dict):
        if key == u"params":
            return param_keys(v)
        if key in _SLIM_ENTITY_FIELDS:
            return {
                u"type": v.get(u"type") or v.get(u"business_type") or u"",
                u"id": _digest(v.get(u"id") if v.get(u"id") is not None else u""),
                u"name_len": len(u"{}".format(v.get(u"name") or u"")),
            }
        return dict((_slim_value(k, depth=depth + 1),
                     _slim_value(x, key=k, depth=depth + 1))
                    for k, x in v.items())
    if isinstance(v, (list, tuple)):
        out = [_slim_value(x, key=key, depth=depth + 1) for x in v[:5]]
        if len(v) > 5:
            out.append(u"…共{}项".format(len(v)))
        return out
    return v


# 线程本地轮次标识：run_pipeline 入口 bind(conv, turn) 一次，此后该线程
# 所有 event 自动携带，按轮串联日志（替代全调用点手工传 request_id）。
# Flask 线程池会复用线程，请求结束必须 bind() 解绑，否则残留值会串轮
_TLS = threading.local()


def bind(**tags):
    """绑定当前线程后续事件的公共字段（如 conv=会话ID, turn=轮序号）；
    无参调用解除绑定。同名字段以 event() 显式传入者优先。"""
    if not tags:
        _TLS.__dict__.pop("tags", None)
        return
    t = getattr(_TLS, "tags", None)
    if t is None:
        t = _TLS.tags = {}
    t.update(tags)


def event(etype, **fields):
    """追加一条事件；任何失败静默（日志不能拖垮主流程）。
    §13.7：诊断明细关（默认）时自动瘦身——完整 Cypher/参数值/实体名称/
    回答全文/长提示词不落盘；开（debug_log_detail=1）时完整记录。
    bind() 绑定的轮次标识（conv/turn）自动并入本线程的每条事件。"""
    if not enabled():
        return
    try:
        rec = {"ts": round(time.time(), 3), "type": etype}
        tags = getattr(_TLS, "tags", None)
        if tags:
            for k, v in tags.items():
                rec[k] = _clean(v)
        for k, v in fields.items():
            rec[k] = _clean(v)
        if not detail_enabled():
            slim = {}
            for k, v in rec.items():
                slim[k] = _slim_value(v, key=k) if k != u"type" else v
            rec = slim
        line = json.dumps(rec, ensure_ascii=False)
        if not isinstance(line, unicode):
            line = line.decode("utf-8")
        with _LOCK:
            _rotate_if_needed()
            with io.open(RUNLOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + u"\n")
    except Exception:
        pass


def _rotate_if_needed():
    try:
        if (os.path.exists(RUNLOG_PATH)
                and os.path.getsize(RUNLOG_PATH) > RUNLOG_MAX_BYTES):
            bak = RUNLOG_PATH + u".1"
            if hasattr(os, "replace"):
                os.replace(RUNLOG_PATH, bak)   # py3：POSIX/Windows 均可覆盖
            else:
                if os.path.exists(bak):        # py2 Windows：rename 拒绝覆盖
                    os.remove(bak)
                os.rename(RUNLOG_PATH, bak)
    except OSError:
        pass


def tail(limit=300):
    """读最后 limit 条，新的在前。坏行/半行跳过（轮转边界、并发写中途）。"""
    out = []
    try:
        if not os.path.exists(RUNLOG_PATH):
            return out
        size = os.path.getsize(RUNLOG_PATH)
        with io.open(RUNLOG_PATH, "rb") as f:
            if size > _TAIL_WINDOW:
                f.seek(size - _TAIL_WINDOW)
            data = f.read(_TAIL_WINDOW)
    except (IOError, OSError):
        return out
    text = data.decode("utf-8", "replace")
    lines = text.split(u"\n")
    if size > _TAIL_WINDOW and lines:
        lines = lines[1:]   # 窗口起点可能是被截断的半行
    for ln in reversed(lines):
        ln = ln.strip()
        if not ln:
            continue
        try:
            obj = json.loads(ln)
        except ValueError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
        if len(out) >= limit:
            break
    return out


def clear():
    """清空日志文件（界面「清空」按钮）"""
    with _LOCK:
        try:
            with io.open(RUNLOG_PATH, "w", encoding="utf-8"):
                pass
            return True
        except (IOError, OSError):
            return False


def size():
    """当前日志文件字节数（不存在视作 0）"""
    try:
        return os.path.getsize(RUNLOG_PATH)
    except OSError:
        return 0
