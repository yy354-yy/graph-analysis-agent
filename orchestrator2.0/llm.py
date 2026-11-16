# -*- coding: utf-8 -*-
"""LLM 算力层：dify / sfm 双后端，统一两个入口：

    llm_complete(instruction, context)   中间步骤（意图识别），非流式拿全文本
    llm_stream(instruction, context)     最终回答，流式 yield 文本块
    reset_caches()                       配置变更后清探测/会话缓存（settings 调用）

instruction = 角色指令（prompts.py 里的 *_PROMPT）
context     = 拼好的上下文/用户输入

平台侧一个 开始→LLM→回复 三节点工作流，双入参约定：
    Dify: inputs.query = context，inputs.metadata = instruction
          （自动探测 workflow/chatflow 两种应用类型，chatflow 的
           conversation_id 恒空保证每次调用相互独立、无记忆串味）
    SFM:  message.metadata = {"prompt": instruction}；正文默认（SFM_PROMPT_MODE
          =text）把 instruction 拼进 message.text 头部——2026-11 生产实证平台
          不一定把 metadata.prompt 注入 LLM system（Trace 中 system 为空串），
          正文通道必达，指令随正文走；metadata 模式回退旧行为
          （每次调用一次性 createSession，用完即弃，无跨调用会话记忆）
"""
from __future__ import print_function

import json
import re

import requests

import config

THINK_RE = re.compile(u"<think>.*?</think>", re.S)


def strip_think(text):
    return THINK_RE.sub(u"", text or u"").strip()


def _platform_error(platform, stage, detail):
    """§14.3 统一错误语义：底层异常/HTTP 细节归类为稳定错误码 + 可诊断短语，
    返回 RuntimeError（上层直接转述，不把原始失败内容拼进正常分析结论）。
    原始细节只进运行日志（llm_error 事件；瘦身模式默认脱敏截断）。"""
    low = u"{}".format(detail).lower()
    if u"401" in low or u"403" in low or u"unauthorized" in low \
            or u"forbidden" in low:
        code, brief = u"auth", u"鉴权失败（API Key/令牌无效或过期）"
    elif u"429" in low or (u"rate" in low and u"limit" in low):
        code, brief = u"rate_limit", u"平台限流，请稍后重试"
    elif u"timeout" in low or u"timed out" in low:
        code, brief = u"timeout", u"请求超时（可适当调大 LLM_TIMEOUT 或稍后重试）"
    elif u"connection" in low or u"max retries" in low \
            or u"unreachable" in low or u"refused" in low:
        code, brief = u"connect", u"平台地址不可达或网络中断，请检查 API 地址"
    elif u"context length" in low or u"maximum context" in low \
            or u"too long" in low:
        code, brief = u"context_too_long", u"上下文过长被平台拒绝，需缩小证据范围"
    elif u"expecting value" in low or u"json" in low:
        code, brief = u"bad_response", u"返回非 JSON（服务/代理异常或错误页）"
    elif u"cancel" in low or u"abort" in low:
        code, brief = u"cancelled", u"生成被平台取消"
    else:
        code, brief = u"other", u"平台调用失败，请查看运行日志 llm_error 事件"
    try:
        import runlog
        runlog.event(u"llm_error", platform=platform, stage=stage,
                     code=code, detail=u"{}".format(detail)[:300])
    except Exception:
        pass
    return RuntimeError(u"LLM 调用失败[{}:{}:{}]：{}".format(
        platform, stage, code, brief))


def _to_text(line):
    """D4：统一把 iter_lines 的产物归一化为 unicode。
    requests 的 iter_lines(decode_unicode=True) 在 py2 下依赖 Response.encoding，
    未设置时会返回 bytes；py3 下遇到异常编码可能抛错。统一按 utf-8 容错解码，
    SSE 事件解析分支就不用在 str/unicode/bytes 之间来回判定。"""
    if line is None:
        return u""
    if isinstance(line, bytes):
        try:
            return line.decode("utf-8", "replace")
        except Exception:
            return u""
    return line


# ---------------- Dify（自动兼容 workflow / chatflow 两种应用类型） ----------------
# 前提：开始节点有 query（chatflow 用 sys.query）+ metadata 两个入参，
# metadata 绑定为 LLM 系统提示词。
_dify_mode = {"mode": None}  # "workflow" | "chat"，首次调用自动探测


def _dify_headers():
    return {"Authorization": "Bearer " + config.get("DIFY_API_KEY"),
            "Content-Type": "application/json"}


def _dify_call(instruction, context, mode, timeout=None):
    """按应用类型调 Dify，返回 (kind, response)。kind: workflow/chat。
    timeout：None 用 config.LLM_TIMEOUT（兼容旧调用）"""
    if _dify_mode["mode"] in (None, "workflow"):
        payload = {"inputs": {"query": context or u"", "metadata": instruction},
                   "response_mode": mode, "user": "pipeline"}
        try:
            r = requests.post(config.get("DIFY_API_BASE") + "/workflows/run",
                              headers=_dify_headers(), json=payload,
                              timeout=timeout or config.LLM_TIMEOUT,
                              stream=(mode == "streaming"))
        except requests.RequestException as e:
            raise _platform_error(u"dify", u"workflow_run", e)
        if r.status_code == 400 and "not_workflow_app" in r.text:
            _dify_mode["mode"] = "chat"   # 是 Chatflow，换路由重试
        else:
            if r.status_code != 200:
                raise _platform_error(u"dify", u"workflow_run",
                                      u"HTTP {}:{}".format(r.status_code,
                                                           r.text[:200]))
            return "workflow", r
    # Chatflow：query 是用户消息，metadata 是开始节点变量；conversation_id 恒空
    # 保证每次调用相互独立（无记忆串味）
    payload = {"inputs": {"metadata": instruction}, "query": context or u"",
               "response_mode": mode, "conversation_id": "", "user": "pipeline"}
    try:
        r = requests.post(config.get("DIFY_API_BASE") + "/chat-messages",
                          headers=_dify_headers(), json=payload,
                          timeout=timeout or config.LLM_TIMEOUT,
                          stream=(mode == "streaming"))
    except requests.RequestException as e:
        raise _platform_error(u"dify", u"chat_messages", e)
    if r.status_code != 200:
        raise _platform_error(u"dify", u"chat_messages",
                              u"HTTP {}:{}".format(r.status_code, r.text[:200]))
    return "chat", r


def _dify_pick_output(kind, resp):
    """blocking 模式取完整回答"""
    try:
        data = resp.json()
    except ValueError as e:
        raise _platform_error(u"dify", u"parse", e)
    if kind == "chat":
        answer = data.get("answer")
        if answer:
            return answer
        raise _platform_error(u"dify", u"empty_answer",
                              json.dumps(data, ensure_ascii=False)[:200])
    d = (data.get("data") or {})
    if d.get("status") not in (None, "succeeded"):
        raise _platform_error(u"dify", u"workflow_failed",
                              u"{}:{}".format(d.get("status"), d.get("error")))
    outputs = d.get("outputs") or {}
    if isinstance(outputs.get("text"), str):
        return outputs["text"]
    for v in outputs.values():
        if isinstance(v, str) and v:
            return v
    raise _platform_error(u"dify", u"empty_output",
                          json.dumps(outputs, ensure_ascii=False)[:200])


def dify_complete(instruction, context, timeout=None):
    kind, resp = _dify_call(instruction, context, "blocking", timeout=timeout)
    return strip_think(_dify_pick_output(kind, resp))


def dify_stream(instruction, context):
    kind, resp = _dify_call(instruction, context, "streaming")
    yielded = False   # §14.3：断流/空流检测——一条增量都没有时明确报错
    # D4：拿 bytes，自己解；避免 py2 下 iter_lines(decode_unicode=True) 行为抖动
    for raw_line in resp.iter_lines(decode_unicode=False):
        line = _to_text(raw_line)
        if not line or not line.startswith(u"data:"):
            continue
        raw = line[len(u"data:"):].strip()
        if not raw or raw == u"[DONE]":
            continue
        try:
            evt = json.loads(raw)
        except ValueError:
            continue
        ev = evt.get("event")
        if ev == "text_chunk":          # workflow 流式
            t = (evt.get("data") or {}).get("text") or ""
            if t:
                yielded = True
                yield t
        elif ev == "message":           # chatflow 流式
            t = evt.get("answer") or ""
            if t:
                yielded = True
                yield t
        elif ev == "workflow_finished":
            d = evt.get("data") or {}
            if d.get("status") != "succeeded":
                raise _platform_error(u"dify", u"stream_finished",
                                      u"{}:{}".format(d.get("status"),
                                                      d.get("error")))
    if not yielded:
        raise _platform_error(u"dify", u"stream_empty",
                              u"流式返回无任何文本增量（断流/模型未输出/被取消）")


# ---------------- SFM（/run，每次调用一次性 createSession） ----------------
# 不使用模块级缓存：Flask threaded 模式下多用户并发会复用同一 sessionId，
# SFM 侧按 session 积累上下文时会跨用户串味。每次 LLM 调用都创建新会话，
# 用完即弃（和 Dify 的 conversation_id="" 等价）。


def _sfm_headers():
    return {"Authorization": "Bearer " + config.get("SFM_API_KEY"),
            "Content-Type": "application/json"}


def sfm_get_session(agent_code=None):
    # agent_code：None = 主智能体；light 调用传入更快的轻量智能体编码
    body = {"agentCode": agent_code or config.get("SFM_AGENT_CODE")}
    if config.get("SFM_AGENT_VERSION"):
        body["agentVersion"] = config.get("SFM_AGENT_VERSION")
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/createSession",
                          headers=_sfm_headers(), json=body, timeout=30)
    except requests.RequestException as e:
        return None, u"SFM 创建会话失败：{}".format(e)
    try:
        resp = r.json()
    except ValueError:
        resp = {}
    code = ((resp.get("data") or {}).get("uniqueCode")) if resp.get("success") else None
    if not code:
        return None, u"SFM 创建会话失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
    return code, None


# text 模式的指令/正文分隔线：让模型明确分清「规则」与「数据」两段
_PROMPT_SEP = u"\n\n────────────\n"


def _sfm_message_text(instruction, context):
    """SFM message.text 组装。text 模式（默认）：instruction 拼进正文头部，
    不依赖平台把 metadata.prompt 注入 LLM system——2026-11 生产 SFM Trace
    实证部分调用 system 渲染为空串，规则根本没进模型（意图/分析双双自由
    发挥），而正文通道始终送达；metadata 模式保持旧行为，指令交给平台。"""
    text = context or u""
    if config.get("SFM_PROMPT_MODE") == u"metadata":
        return text
    ins = (instruction or u"").strip()
    if not ins:
        return text
    return ins + _PROMPT_SEP + text


def _sfm_run_payload(session, instruction, context, stream):
    # metadata.prompt 两种模式都照带：平台侧 Trace 可见指令全文便于排障；
    # text 模式下平台注入与否都不影响（正文已含指令，重复无害）
    return {
        "sessionId": session,
        "stream": bool(stream),
        "delta": True,
        "trace": False,
        "message": {"text": _sfm_message_text(instruction, context),
                    "metadata": {"prompt": instruction},
                    "attachments": []},
    }


def _sfm_text_from_content(content):
    parts = []
    for c in content or []:
        if isinstance(c, dict) and c.get("type") == "text":
            t = c.get("text")
            v = t.get("value") if isinstance(t, dict) else t
            if v:
                parts.append(v)
    return u"".join(parts)


# 平台侧智能体偶发把回答包成 {"text": "..."} 信封：可能是合法 JSON，也可能是
# 带裸换行的坏 JSON，且闭引号后模型还会继续闲聊（2026-11-13 生产 SFM Studio
# 截图实证）。识别到信封开头就剥壳取内文；剥不动原样返回
_ENV_HEAD_RE = re.compile(u'^\\{\\s*"text"\\s*:')
_ENV_BODY_RE = re.compile(
    u'^\\{\\s*"text"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"', re.S)


def _unwrap_text_envelope(text):
    t = (text or u"").strip()
    if not _ENV_HEAD_RE.match(t):
        return text
    try:
        d = json.loads(t)
    except ValueError:
        d = None
    if isinstance(d, dict):
        v = d.get("text")
        if v is not None and not isinstance(v, (dict, list)):
            return _to_text(v)
    m = _ENV_BODY_RE.match(t)   # 坏 JSON：裸换行/越界续写，取到闭引号为止
    if m:
        s = m.group(1)
        # 手工反转义（先替反斜杠占位，防 \\n 被拆错）
        s = s.replace(u"\\\\", u"\x00")
        s = s.replace(u"\\n", u"\n").replace(u"\\r", u"\r") \
             .replace(u"\\t", u"\t").replace(u'\\"', u'"')
        s = s.replace(u"\x00", u"\\")
        return s
    return text


def sfm_complete(instruction, context, light=False, timeout=None, agent_code=None):
    # agent 解析优先级：显式 agent_code > light=True 时取 SFM_INTENT_AGENT_CODE
    # > None（主智能体）。timeout：None 用 config.LLM_TIMEOUT
    if agent_code is None:
        agent = (config.get("SFM_INTENT_AGENT_CODE") or None) if light else None
    else:
        agent = agent_code or None
    session, err = sfm_get_session(agent)
    if err:
        raise _platform_error(u"sfm", u"create_session", err)
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/run", headers=_sfm_headers(),
                          json=_sfm_run_payload(session, instruction, context, False),
                          timeout=timeout or config.LLM_TIMEOUT)
    except requests.RequestException as e:
        raise _platform_error(u"sfm", u"run", e)
    try:
        resp = r.json()
    except ValueError:
        raise _platform_error(u"sfm", u"parse",
                              u"HTTP {}:{}".format(r.status_code, r.text[:200]))
    if not resp.get("success"):
        raise _platform_error(u"sfm", u"run", u"{}".format(r.text[:200]))
    msg = ((resp.get("data") or {}).get("message")) or {}
    return strip_think(_unwrap_text_envelope(
        _sfm_text_from_content(msg.get("content"))))


def sfm_stream(instruction, context):
    session, err = sfm_get_session()
    if err:
        raise _platform_error(u"sfm", u"create_session", err)
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/run", headers=_sfm_headers(),
                          json=_sfm_run_payload(session, instruction, context, True),
                          stream=True, timeout=config.LLM_TIMEOUT)
    except requests.RequestException as e:
        raise _platform_error(u"sfm", u"run", e)
    if r.status_code != 200:
        raise _platform_error(u"sfm", u"run",
                              u"HTTP {}:{}".format(r.status_code, r.text[:200]))
    buf = []
    yielded = False   # §14.3：断流/空流检测——一条增量都没有时明确报错
    # D4：拿 bytes，自己解；避免 py2 下 iter_lines(decode_unicode=True) 行为抖动
    for raw_line in r.iter_lines(decode_unicode=False):
        line = _to_text(raw_line)
        if line.startswith(u"data:"):
            if buf:
                for t in _sfm_parse_event(u"\n".join(buf)):
                    if t:
                        yielded = True
                        yield t
                buf = []
            buf = [line[len(u"data:"):].lstrip()]
        elif line == u"":
            if buf:
                for t in _sfm_parse_event(u"\n".join(buf)):
                    if t:
                        yielded = True
                        yield t
                buf = []
        elif buf:
            buf.append(line)
    if buf:
        for t in _sfm_parse_event(u"\n".join(buf)):
            if t:
                yielded = True
                yield t
    if not yielded:
        raise _platform_error(u"sfm", u"stream_empty",
                              u"流式返回无任何文本增量（断流/模型未输出/被取消）")


def _sfm_parse_event(raw):
    """解析一条 SFM SSE 事件，返回 [文本增量]（message.delta）"""
    try:
        evt = json.loads(raw)
    except ValueError:
        return []
    if evt.get("object") != "message.delta":
        return []
    return [_sfm_text_from_content(evt.get("content"))]


# ---------------- 对外统一入口 ----------------
def llm_complete(instruction, context, light=False, timeout=None, agent_code=None):
    """中间步骤：非流式，返回完整文本（已剥 <think>）。
    instruction = 角色指令（进平台侧系统提示词），context = 上下文/用户输入。
    light=True：意图识别等轻量调用，SFM 配了 SFM_INTENT_AGENT_CODE 时走轻量智能体；
    timeout：分阶段读超时（None = config.LLM_TIMEOUT）；
    agent_code：显式指定智能体编码（如 adhoc 生成走 SFM_ADHOC_AGENT_CODE），
    仅 SFM 通道生效，Dify 无此概念忽略之"""
    if config.get("LLM_PLATFORM") == "dify":
        return dify_complete(instruction, context, timeout=timeout)
    return sfm_complete(instruction, context, light=light, timeout=timeout,
                        agent_code=agent_code)


def llm_stream(instruction, context):
    """最终回答：流式生成器，yield 文本块（think 由上层流式过滤器剥离）"""
    if config.get("LLM_PLATFORM") == "dify":
        return dify_stream(instruction, context)
    return sfm_stream(instruction, context)


def reset_caches():
    """配置变更后调用：清空平台探测缓存（SFM 会话不缓存，无需清理）"""
    _dify_mode["mode"] = None
