# -*- coding: utf-8 -*-
"""编排层：对话主流程。run_pipeline(query, conv_id) 生成器：

    yield ("status", 文本)   阶段提示（前端灰色状态行，不进历史）
    yield 文本块             最终回答（流式）
    yield ("error", 消息)    出错

流程与设计原则（确定性层与 LLM 层严格分工）：

    ┌─ 意图识别（LLM+规则兜底）→ search / targeted / fact / chat 四分支
    │
    ├─ chat：通用回答（注入数据概况，可答图谱/系统类问题；图库断连时概况带提示）
    ├─ fact：事实/清单直答（定位实体→确定性证据查询→直答，不套报告模板；
    │       模板查不到时 _try_adhoc：LLM 生成只读 Cypher 过校验后直查直答）
    ├─ targeted：state 里有已选业务/候选 → 序号/ID/全名定位 → 影响分析；
    │       旧 selected + 无关新名称 → 自动转 search（可退出已选业务）
    └─ search：搜索候选 → 0 个(模糊兜底/adhoc/未找到) / 1 个(直接分析) / 多个(列清单)

    影响分析 _analyze（所有路径汇合）：
        确定性证据查询（store.evidence_rows，代码构建，不经 LLM 写 Cypher）
        → 代码分桶预计算结论（受影响候选/上游依赖/按角色关联，直接/间接分层，
           受影响业务连带承载系统，证据路径写死）
        → LLM 仅按 ANALYZE_PROMPT 排版成报告

    确定性层（零误差）：搜索定位 / 证据查询 / 去重 / 方向分桶 / 序号解析
    LLM 层（只做擅长的）：理解意图、组织语言、给建议
"""
from __future__ import print_function

import json
import re
import sys
import traceback

import config
import intent_parser
import llm
import metrics
import prompts
import runlog
import store

log = config.get_logger("pipeline")

# 保留给其它逻辑使用的宽松 JSON 定位（_extract_json 不再依赖它来切片，
# 而是用花括号平衡扫描第一段合法 JSON，避免 LLM 多段/带前后文本时贪婪吞并，E4）
JSON_RE = re.compile(u"\\{.*\\}", re.S)


def _balanced_json_slice(text):
    """从文本里扫出第一段花括号平衡且 json.loads 成功的子串，失败返回 None。
    避开字符串字面量里的括号；比 r'\\{.*\\}' 贪婪匹配稳。"""
    if not text:
        return None
    s = text
    n = len(s)
    i = 0
    while i < n:
        if s[i] != u"{":
            i += 1
            continue
        depth = 0
        in_str = False
        esc = False
        for j in range(i, n):
            ch = s[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == u"\\":
                    esc = True
                elif ch == u"\"":
                    in_str = False
                continue
            if ch == u"\"":
                in_str = True
            elif ch == u"{":
                depth += 1
            elif ch == u"}":
                depth -= 1
                if depth == 0:
                    chunk = s[i:j + 1]
                    try:
                        obj = json.loads(chunk)
                        return obj if isinstance(obj, dict) else None
                    except ValueError:
                        break
        i += 1
    return None
# 句尾动作词：'…底账变更'的'变更'是动作不是名称；'企业变更登记'的'变更'在中间不受影响
_TRAIL_RE = re.compile(
    u"(的变更|变更|变化|修改|调整|的影响|影响|有什么|什么|怎么样|如何|"
    u"吗|啊|吧|呀|啦|呢|了|么|嘛|哈|噢|哟|哇)$")
# 统计类问题（问数量）：候选搜索有截断上限，必须走 count() 聚合而不是数样本
# （py2 re 不支持 \uXXXX 转义，汉字写面量）
_COUNT_Q_RE = re.compile(
    u"(有多少|有多少个|有几个|有几条|有几家|有几种|多少个|多少家|多少条|"
    u"多少种|多少家|几家|几种|若干|约有|约为|大约有|总共有|总共多少|"
    u"合计有|合计多少|数量是|数量为|总数|总量)")
# 追问句式（无业务名的清单跟进，如"具体哪16个"）：靠对话上下文延续话题
_FOLLOWUP_RE = re.compile(u"(具体哪|具体有哪|哪几个|哪些|列出来|列一下|列个清单|清单|明细|分别是什么|分别有哪|展开说说|展开讲讲)")
# Schema 元数据问题必须由代码读取官方模型回答，不能用 MATCH (n) 统计非空标签代替。
_NODE_TYPE_Q_RE = re.compile(u"(点类型|节点类型|节点标签|标签类型)")
_TYPE_FOLLOWUP_RE = re.compile(u"(哪.{0,4}种|分别.{0,4}(什么|哪些)|列.{0,3}(出|一下|清单))")
# 「」/""/""引号业务名 + 分析意图词：意图 LLM 输出不可解析被降级成 chat 时的
# 确定性抢救（生产平台弱模型场景），保证"分析「某业务」变更的影响"一定能走
# 图查询。三种引号都要认：用户常用 ASCII 双引号和中文弯引号包业务名
_BRACKET_NAME_RE = re.compile(u'[「"“]([^」"”]{2,40})[」"”]')
_ANALYSIS_HINT_RE = re.compile(u"(影响|变更|分析)")
# "修改X，评估一下影响"类问法是变更影响分析请求：意图偶发判成 fact 会掉进
# adoc 生成查询（弱平台两次都生成不出来，每次还白等几分钟），确定性纠正回 search
_CHANGE_RE = re.compile(u"(修改|变更|调整|改动)")
_IMPACT_RE = re.compile(u"(影响|评估|波及)")
# 规则抢答用：从"修改XX"里抠业务名（吃到标点为止，再截掉疑问短语）
_CHANGE_OBJ_RE = re.compile(
    u"(?:修改|变更|调整|改动)了?([一-龥A-Za-z0-9\\-]{2,30})")
_FAST_KW_SPLIT_RE = re.compile(u"有什么|有哪些|哪些|什么|怎么样|如何|影响|评估")
# 名字在前的变更问法："证单拟制变更""证单拟制的变更影响"——变更短语紧贴结尾，
# 其后最多跟 影响/分析/评估（"企业变更登记有什么影响"结尾是"么影响"，不匹配）
_CHANGE_TAIL_RE = re.compile(
    u"(?:的)?(?:变更|修改|调整|改动)(?:的)?(?:影响|评估|分析)?$")
_QUESTION_WORD_RE = re.compile(u"(哪些|什么|怎么|如何|多少|为什么|吗)")
# 动词前缀（全部 ≥2 字，避免误剥"请示审批"这类以单字动词开头的业务名）
_FAST_PREFIX_RE = re.compile(
    u"^(?:帮我|帮忙|麻烦|请帮我|看看|看下|看一下|查看|查下|查一下|"
    u"分析下|分析一下|我想|我要|给我)")


def _bracket_rescue(query):
    """query 形如「分析「某业务名」变更的影响」时返回引号内的业务名，否则 None。
    给 chat 分支兜底转 search 用（业务名与问句其余部分有引号明确分界，纯规则可靠）。"""
    q = query or u""
    m = _BRACKET_NAME_RE.search(q)
    if not m or not _ANALYSIS_HINT_RE.search(q):
        return None
    name = m.group(1).strip().strip(u"「」『』“”‘’\"' ").strip()
    return name or None


def _fast_intent(query, ctx):
    """高置信问式规则抢答：跳过意图 LLM 直接给意图（SFM 单次意图调用约 3 分钟，
    最常见的问法不该付这个成本）。返回 intent dict；None = 不命中，照常走 LLM。
    规则保守——只覆盖有明确结构信号的问式，宁可不抢答也不错判。"""
    q = query or u""
    state = ctx.get("state") or {}
    # 正在候选选择流程（回复序号）时不抢答，交给原流程强制 targeted
    if state.get("candidates") and store._ordinal(q):
        return None
    name = _bracket_rescue(q)
    if name:
        metrics.inc("intent.fast_bracket")
        return {"action": u"search", "keyword": name, "change_desc": u""}
    if _CHANGE_RE.search(q) and _IMPACT_RE.search(q):
        m = _CHANGE_OBJ_RE.search(q)
        if m:
            kw = clean_keyword(
                _FAST_KW_SPLIT_RE.split(m.group(1))[0].strip(u"的了，。, "))
            core = re.sub(u"[^一-龥A-Za-z0-9]+", u"", kw)
            if len(core) >= 2:
                metrics.inc("intent.fast_change_impact")
                return {"action": u"search", "keyword": kw, "change_desc": u""}
    # 名字在前、变更词收尾的问法（"证单拟制变更""证单拟制的变更影响"）：
    # 上面的 _CHANGE_OBJ_RE 只认名字在变更词之后的语序。这里只认整个前段
    # 就是名称的裸问式（变更词紧贴结尾、其后最多跟 影响/分析/评估）；
    # 带动词前缀（"帮我看下XX的变更"）中文无分词边界剥不干净、名称内含
    # 变更词（"企业变更登记有什么影响"）结尾对不上——一律不抢，宁可放给
    # 意图 LLM 也不错判
    m = _CHANGE_TAIL_RE.search(q.strip())
    if m:
        head = q.strip()[:m.start()].strip(u"的了这它该，。, ")
        for _ in range(2):   # "帮我+看下"类前缀可能叠加，剥两轮
            head = _FAST_PREFIX_RE.sub(u"", head)
        head = head.strip(u"的了这它该，。, ")
        core = re.sub(u"[^一-龥A-Za-z0-9]+", u"", head)
        if len(core) >= 2 and core == head \
                and not _QUESTION_WORD_RE.search(head):
            metrics.inc("intent.fast_change_suffix")
            return {"action": u"search", "keyword": head, "change_desc": u""}
    return None


def clean_keyword(kw):
    # 名称常被模型连「」『』“”引号一起返回，CONTAINS 匹配会 0 命中，统一剥掉；
    # 句尾动作词可能叠加（如"……’有什么影响"），循环剥离至稳定（上限 3 轮）
    src = (kw or u"").strip()
    core = src
    for _ in range(3):
        stripped = _TRAIL_RE.sub(u"", core).strip()
        if stripped == core:
            break
        core = stripped
    core = core or src
    return core.strip(u"「」『』“”‘’\"' ").strip() or core


def _kw_related(kw, name):
    """意图关键词与业务名是否相关。不相关=用户在提新业务/新话题。
    短关键词（<=1 字）或非中英字符时直接判为无关：
    例如 selected='税收优惠办理'，用户再提'税'，短字符 CONTAINS 会误判为相关而
    困在旧 selected 上（A6）。要求：至少 2 字 且 存在实质字符 且 实质子串关系。"""
    kw = _text(kw).strip()
    name = _text(name).strip()
    if not kw or not name:
        return False
    core = re.sub(u"[^一-龥A-Za-z0-9]+", u"", kw)
    if len(core) < 2:
        return False
    return core in name or name in core


def _text(v):
    """任意值转 unicode 文本（py2 下 str(中文) 会炸，统一走这里）"""
    if v is None:
        return u""
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return u"{}".format(v)


def _sample_rows_by_role(rows, per_group=40, total_cap=400):
    """按 (role, rel, dir, nb_type) 分桶后抽样，避免把全部 rows 塞 LLM。
    返回 (sampled, note)；note 非空表示抽样过，提示 LLM 原始行只是复核样本。"""
    if not rows:
        return [], u""
    total = len(rows)
    if total <= total_cap:
        return rows, u""
    buckets = {}
    order = []
    for r in rows:
        key = (_text(r.get("role")), _text(r.get("rel")),
               _text(r.get("dir")), _text(r.get("nb_type")))
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(r)
    sampled = []
    dropped = {}
    for key in order:
        bucket = buckets[key]
        keep = bucket[:per_group]
        sampled.extend(keep)
        if len(bucket) > per_group:
            dropped[key] = len(bucket) - per_group
    # 总上限再做一次整体封顶（避免分桶特别多时失控）
    extra = len(sampled) - total_cap
    if extra > 0:
        sampled = sampled[:total_cap]
    note = (u"（说明：原始行共 {total} 条，按角色分桶每桶最多留 {per} 条，"
            u"抽样后 {kept} 条进入上下文；完整候选以上面【内部判断材料】"
            u"分组为准，禁止用样本数量当总数）").format(
                total=total, per=per_group, kept=len(sampled))
    return sampled, note


def _strip_code_fence(text):
    """剥掉 LLM 常见的 ```json 围栏（只在整体被围栏包住时处理，避免误伤正文）"""
    s = (text or u"").strip()
    if not s.startswith(u"```"):
        return s
    s = re.sub(u"^```[a-zA-Z0-9_+-]*[ \\t]*\\r?\\n?", u"", s)
    if s.endswith(u"```"):
        s = s[:-3]
    return s.strip()


def _repair_literal_escapes(s):
    u"""把字符串字面量【之外】的字面量 \\n \\r \\t（反斜杠+字母两个字符）换成空格。
    生产平台偶发把换行以字面量形式回传，出现在字符串外时 json.loads 必失败；
    字符串内的合法转义必须原样保留，所以用 in_str/esc 状态机只动字符串外。"""
    out = []
    i, n = 0, len(s)
    in_str = esc = False
    while i < n:
        ch = s[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == u"\\":
                esc = True
            elif ch == u"\"":
                in_str = False
            i += 1
        else:
            if ch == u"\"":
                in_str = True
                out.append(ch)
                i += 1
            elif ch == u"\\" and i + 1 < n and s[i + 1] in u"nrt":
                out.append(u" ")
                i += 2
            else:
                out.append(ch)
                i += 1
    return u"".join(out)


def _extract_json(text):
    """从 LLM 输出抠 JSON（剥 <think>/代码围栏、修复字符串外的字面量 \\n），
    失败返回 {}。
    E4：用花括号平衡扫描首个合法 JSON，不再用 r'\\{.*\\}' 贪婪匹配——
    LLM 把两段 JSON 拼在一起或带花括号的解释文本时后者会一次吞光。"""
    text = llm.strip_think(text)
    text = _strip_code_fence(text)
    candidates = [text, _repair_literal_escapes(text)]
    for cand in candidates:
        obj = _balanced_json_slice(cand)
        if obj is not None:
            return obj
    # 兜底：旧的宽松正则（极端场景 LLM 返回只有尾段 }）
    for cand in candidates:
        m = JSON_RE.search(cand)
        if not m:
            continue
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict):
                return obj
        except ValueError:
            continue
    return {}


def _intent(query, ctx, tasks=False):
    """意图识别 + 兜底（对应节点 4/5）。tasks=True（自动模式）时附带
    qa/writing 扩展规则，action 可能多返回两个任务域。
    高置信问式先规则抢答（不调 LLM）；解析失败时用更严格的指令重试一次
    （生产平台偶发回传带字面量 \\n / 围栏的 JSON）。全部过程落 runlog。"""
    fast = _fast_intent(query, ctx)
    if fast is not None:
        runlog.event("intent", query=query, prompt=None, raw=None,
                     raw_retry=None,
                     parsed={"action": fast["action"],
                             "business_keyword": fast["keyword"],
                             "change_desc": fast["change_desc"]},
                     fallback=u"fast:规则抢答（未调LLM）")
        return fast
    # 输出协议开关：text3（固定三行文本 + Python 解析）默认关，走原 JSON 路径
    if (config.get("SFM_INTENT_FORMAT") or u"json") == u"text3":
        return _intent_text3(query, ctx, tasks)
    instruction = prompts.intent_prompt(tasks)
    context = prompts.context_block(
        store.history_text(ctx),
        json.dumps(ctx["state"], ensure_ascii=False)) \
        + u"\n【本轮输入】\n" + query
    # light=True：配置了 SFM_INTENT_AGENT_CODE 时意图走轻量智能体（见 llm.py）
    text = llm.llm_complete(instruction, context, light=True)
    r = _extract_json(text)
    raw_retry = None
    if not r:
        metrics.inc("intent.parse_fail")
        raw_retry = llm.llm_complete(
            instruction + u"\n注意：上一次的输出无法解析。请只输出一行合法 JSON："
            u"不要输出 \\n 字面量、markdown 代码块或任何解释文字。",
            context, light=True)
        r = _extract_json(raw_retry)
        if r:
            metrics.inc("intent.retry_ok")
    action = r.get("action")
    keyword = _text(r.get("business_keyword")).strip()
    # 模型常把名称连「」『』“”引号一起返回，CONTAINS 匹配会 0 命中，剥掉
    keyword = keyword.strip(u"「」『』“”‘’\"' ").strip()
    valid = ("search", "targeted", "fact", "chat") + \
        (("qa", "writing") if tasks else ())
    fallback = u""
    if action not in valid:
        # 兜底收紧：提取到了业务名 → search（宁可重搜，不可困在旧 selected 上）；
        # 没有业务名但有已选/候选 → targeted；都没有 → search（会在后面转 chat）
        state = ctx["state"]
        if keyword:
            action = "search"
            fallback = u"invalid_action→search(keyword)"
        elif state.get("selected") or state.get("candidates"):
            action = "targeted"
            fallback = u"invalid_action→targeted(state)"
        else:
            action = "search"
            fallback = u"invalid_action→search"
    change_desc = _text(r.get("change_desc")).strip()
    if action in ("qa", "writing"):
        result = {"action": action, "keyword": u"", "change_desc": change_desc}
    else:
        if action in ("search", "fact") and not keyword:
            action = "chat"
            fallback = fallback or u"no_keyword→chat"
        result = {"action": action, "keyword": keyword, "change_desc": change_desc}
    runlog.event("intent", query=query, prompt=instruction, context=context,
                 raw=text, raw_retry=raw_retry,
                 parsed={"action": result["action"],
                         "business_keyword": result["keyword"],
                         "change_desc": result["change_desc"]},
                 fallback=fallback or u"ok")
    return result


# ---- text3 意图提取（SFM_INTENT_FORMAT=text3 时替代上面的 JSON 路径）----
# 变更描述合并词面："再加上/同时"类=追加到旧变更；"换成/改为"类或直接给
# 新变更=本轮替换。只做词面判定，不澄清不猜语义
_APPEND_CHANGE_RE = re.compile(u"^(再加上|同时|叠加|追加|再加)")
_MERGE_CHANGE_MAX = 300


def _merge_change(old, new):
    """targeted 延续时合并变更描述。new 为空沿用 old；old 为空直接 new。"""
    old = _text(old).strip()
    new = _text(new).strip()
    if not new:
        return old
    if old and _APPEND_CHANGE_RE.match(new):
        return (old + u"；" + new)[-_MERGE_CHANGE_MAX:]
    return new


def _route_text3(parsed, err, state, tasks):
    """text3 语义结果 → 现有 action 契约，返回 (result, fallback)。
    映射：影响分析+新业务名→search；事实查询+新业务名→fact；
    影响分析+无主语+已选→targeted（合并变更）；事实查询+无主语+已选/候选→fact；
    普通聊天→chat；智能问答/写作仅自动模式。其余一切（解析失败/语义值未知/
    无主语且无状态）→ qa：通用回答、状态原样保留，绝不兜底 chat。"""
    if parsed:
        sem = parsed["intent"]
        kw = parsed["business_name"]
        chg = parsed["change_desc"]
        has_sel = bool(state.get("selected"))
        has_cand = bool(state.get("candidates"))
        if sem == u"chat":
            return ({"action": u"chat", "keyword": u"",
                     "change_desc": u""}, u"ok")
        if sem == u"qa" and tasks:
            return ({"action": u"qa", "keyword": u"",
                     "change_desc": u""}, u"ok")
        if sem == u"writing" and tasks:
            return ({"action": u"writing", "keyword": u"",
                     "change_desc": u""}, u"ok")
        if kw:
            action = u"search" if sem == u"impact" else u"fact"
            return ({"action": action, "keyword": kw,
                     "change_desc": chg}, u"ok")
        if sem == u"impact" and has_sel:
            return ({"action": u"targeted", "keyword": u"",
                     "change_desc": _merge_change(state.get("change_desc"),
                                                  chg)}, u"ok")
        if sem == u"fact" and (has_sel or has_cand):
            return ({"action": u"fact", "keyword": u"",
                     "change_desc": chg}, u"ok")
    return ({"action": u"qa", "keyword": u"", "change_desc": u""},
            err or u"unresolved→qa")


def _intent_text3(query, ctx, tasks=False):
    """text3 协议意图提取：SFM 只输出固定三行文本（意图/业务/变更），
    intent_parser 严格解析后映射回 action 契约（见 _route_text3）。
    不做 JSON 式重试——三行协议下重试收益低，失败直接进 qa 兜底。"""
    import time as _time
    state = ctx.get("state") or {}
    sel_name = _text((state.get("selected") or {}).get("name"))
    last_q = u""
    for m in reversed(ctx.get("history") or []):
        if m.get("role") == u"user":
            last_q = _text(m.get("content"))[:120]
            break
    instruction = prompts.intent_extract_prompt(tasks)
    context = (u"【当前已选业务】{}\n【上一轮问题】{}\n【当前用户输入】{}"
               .format(sel_name or u"无", last_q or u"无", query))
    t0 = _time.time()
    raw = llm.llm_complete(instruction, context, light=True)
    parsed, err = intent_parser.parse_intent_lines(raw)
    result, fallback = _route_text3(parsed, err, state, tasks)
    # 兜底强化（2026-11-13 生产）：text3 严格解析失败时不再直接进 qa——
    # ①问句带变更/影响/分析信号词 → 代码候选搜索（search 分支自带精确/全类/
    #   模糊三级兜底，未找到会如实提示，绝不编造）；
    # ②状态里有候选/已选且不像提问 → 视为选择/指名，交 targeted 定位
    #   （匹配不上会走澄清重列候选，确定性安全）。
    # qa 只收真闲聊/概念题，弱模型意图再疯也堵死"qa 编造报告"的入口。
    if result["action"] == u"qa" and parsed is None:
        if _CHANGE_RE.search(query) or _IMPACT_RE.search(query) \
                or _ANALYSIS_HINT_RE.search(query):
            result = {"action": u"search", "keyword": clean_keyword(query),
                      "change_desc": u""}
            fallback = u"text3fail→search_rescue"
            metrics.inc("intent.text3.search_rescue")
        elif (state.get("candidates") or state.get("selected")) \
                and not _QUESTION_WORD_RE.search(query):
            result = {"action": u"targeted", "keyword": u"", "change_desc": u""}
            fallback = u"text3fail→targeted_rescue"
            metrics.inc("intent.text3.targeted_rescue")
    metrics.inc("intent.text3." + (u"ok" if parsed else u"parse_fail"))
    runlog.event("intent", query=query, format=u"text3",
                 prompt=instruction, context=context,
                 raw=raw, raw_retry=None, parse_error=err,
                 parsed={"action": result["action"],
                         "business_keyword": result["keyword"],
                         "change_desc": result["change_desc"]},
                 fallback=fallback,
                 ms=int((_time.time() - t0) * 1000))
    return result


def run_pipeline(query, conv_id, mode=u"auto"):
    """主入口。mode：auto（自动分流）/ analysis / qa / writing，
    手动模式优先；yield str（回答文本块）或 ("error", 消息)。
    顶层 except 把 traceback 打到 stdout（O3）——线上排障靠 systemd 日志。
    O2：记录分支占比与总耗时。
    §13.4：请求开始即事务递增会话 turn_seq，本轮所有状态回写带
    expected_turn 条件更新，旧请求晚完成不得覆盖新请求的状态。"""
    metrics.inc("pipeline.requests")
    with metrics.Timer("pipeline.total"):
        try:
            expected_turn = store.begin_turn(conv_id)
        except Exception:
            expected_turn = None   # 写序控制失效时退化为旧行为，不阻断对话
        # 本线程后续所有 runlog 事件自动带 conv/turn，按轮串联日志；
        # 结束必须解绑——Flask 线程池复用线程，残留标识会串到别的会话
        runlog.bind(conv=conv_id, turn=expected_turn)
        try:
            for item in _run(query, conv_id, mode, expected_turn):
                yield item
        except Exception as e:  # LLM/图库等任何未预期异常
            metrics.inc("pipeline.exceptions")
            runlog.event("error", stage=u"pipeline", err=u"{}".format(e))
            try:
                log.exception(u"未预期异常：%s", e)
            except Exception:
                pass
            yield ("error", u"处理出错：{}".format(e))
        finally:
            runlog.bind()


# 写作续写短语（上次任务是写作时，这些修改类输入继续写作而非重判）
_WRITE_REV_RE = re.compile(
    u"(再.{0,6}(正式|口语|简洁|精简|详细|长|短一些|改|润色|写一版)|正式(一些|点)|"
    u"口语(一点|些)|缩短|加长|扩写|精简|润色|改写|篇幅|换个语气|语气(正式|轻松)|"
    u"再修改|再润色|简短(一些|点)|详细(一些|点))")
_MODES = (u"auto", u"analysis", u"qa", u"writing")

# adhoc 熔断：LLM 生成 Cypher 连续两轮（每轮含一次重试）全失败 → 30 分钟内
# 不再尝试（弱平台每次失败要白等数分钟），直接走代码兜底；任一次生成成功即复位
_adhoc_fail_streak = 0
_adhoc_disabled_until = 0.0
_ADHOC_BREAKER_SEC = 1800.0


def _run(query, conv_id, mode=u"auto", expected_turn=None):
    yield ("status", u"正在理解问题…")
    ctx = store.get_context(conv_id)
    state = ctx["state"]
    mode = (_text(mode) or u"auto").lower()
    if mode not in _MODES:
        mode = u"auto"
    runlog.event("turn", conv=conv_id, mode=mode, query=query,
                 state={"task": state.get("task"),
                        "selected": _text(
                            (state.get("selected") or {}).get("name")),
                        "candidates": len(state.get("candidates") or [])})

    # “点类型有多少/哪几种”是 Schema 元数据，不交给 LLM 生成 MATCH (n)。
    # 后者只能看到有实例的标签，会把本图定义的 15 类错误回答成非空的 6 类。
    recent_history = store.history_text(ctx)
    asks_node_types = bool(_NODE_TYPE_Q_RE.search(query))
    follows_node_types = (bool(_TYPE_FOLLOWUP_RE.search(query))
                          and bool(_NODE_TYPE_Q_RE.search(recent_history)))
    if mode in (u"auto", u"analysis") and (asks_node_types or follows_node_types):
        yield ("status", u"正在读取图 Schema…")
        yield store.node_type_overview()
        merged = dict(state)
        merged["task"] = u"analysis"
        store.save_state(conv_id, merged, expected_turn)
        return

    # ---- 任务域分流：手动模式优先；auto 扩展意图 + 写作续写守卫 ----
    task = u"analysis"
    if mode in (u"qa", u"writing"):
        task = mode
        intent = {"action": task, "keyword": u"", "change_desc": u""}
    else:
        if _text(state.get("task")) == u"writing" and _WRITE_REV_RE.search(query):
            # 上次任务是写作且本轮是修改类短语：继续修改当前文稿，不重判
            task = u"writing"
            intent = {"action": u"writing", "keyword": u"", "change_desc": u""}
        elif store._ordinal(query) and (state.get("candidates")
                                        or state.get("selected")):
            # 纯序号回复且状态里有候选/已选 = 选择行为：不经意图 LLM 直接定位。
            # 生产实测（2026-11-13）：弱模型意图对裸序号输入必然失灵（text3 解析
            # 全败→兜底 qa），而 qa 分支提前 return，_analysis 里的序号守卫够不到
            # → 6s 意图白等 + qa 拿对话历史里的候选列表编造"分析"（90s/4583字）。
            # 这里在任务域分流前就拦下，序号选择零 LLM 依赖。
            metrics.inc("intent.ordinal_shortcircuit")
            intent = {"action": u"targeted", "keyword": u"", "change_desc": u""}
        else:
            intent = _intent(query, ctx, tasks=(mode == u"auto"))
            if intent["action"] in (u"qa", u"writing"):
                task = intent["action"]

    if task == u"qa":
        metrics.inc("pipeline.task.qa")
        for chunk in _qa(query, ctx, conv_id, expected_turn):
            yield chunk
        return
    if task == u"writing":
        metrics.inc("pipeline.task.writing")
        for chunk in _writing(query, ctx, conv_id, expected_turn):
            yield chunk
        return
    metrics.inc("pipeline.task.analysis")

    # ---- 业务分析域（原四分支流程）；顺带收集回答供智能写作当素材 ----
    # C5：无论正常结束 / 中断（GeneratorExit）/ 异常，都要把 task/last_report 落盘，
    #     否则下一轮读到的 last_report 还是上一次的旧内容（状态与消息不同步）。
    buf = []
    try:
        for item in _analysis(query, intent, state, ctx, conv_id, expected_turn):
            if isinstance(item, tuple):
                yield item
            else:
                buf.append(item)
                yield item
    finally:
        report = u"".join(buf).strip()
        # 用 merge_state 做锁内 read-modify-write，_analysis 在生成器中可能已经
        # save_state 过 candidates/selected，这里只覆盖 task/last_report 两键（C4）
        patch = {"task": u"analysis"}
        if intent.get("action") != u"chat" and report:
            patch["last_report"] = report[-4000:]   # 分析结果作为写作素材
        try:
            store.merge_state(conv_id, patch, expected_turn)
        except Exception:
            pass


def _qa(query, ctx, conv_id, expected_turn=None):
    """智能问答：通用知识/概念解释（与图数据库无关），可引用最近分析结果"""
    yield ("status", u"正在思考…")
    state = ctx["state"]
    last = _text(state.get("last_report"))
    ctx_text = (u"【对话历史】\n{}\n".format(store.history_text(ctx) or u"（无）") +
                (u"【最近分析结果】（问题相关时可引用其中事实）\n{}\n"
                 .format(last[-2500:]) if last else u"") +
                u"【本轮输入】\n" + query)
    # C5：中断也要把 task 落盘，否则下一轮 state.task 还是上一次的值
    try:
        for chunk in _stream(prompts.QA_PROMPT, ctx_text, label=u"qa"):
            yield chunk
    finally:
        try:
            store.merge_state(conv_id, {"task": u"qa"}, expected_turn)
        except Exception:
            pass


def _writing(query, ctx, conv_id, expected_turn=None):
    """智能写作：起草/修改文稿。草稿与累计写作要求存会话状态——同一会话内
    连续修改（手动 writing 模式，或自动模式 + 修改类短语）始终基于当前文稿。"""
    state = ctx["state"]
    draft = _text(state.get("writing_draft"))
    material = _text(state.get("last_report"))
    reqs = [_text(r) for r in (state.get("writing_reqs") or [])
            if _text(r).strip()]
    if draft:
        yield ("status", u"正在修改文稿…")
        ctx_text = (u"【当前文稿】\n{}\n【历史写作要求】\n{}\n"
                    u"【分析素材】（事实依据，仅供核对）\n{}\n【修改要求】\n{}"
                    .format(draft, u"；".join(reqs[-8:]) or u"（无）",
                            material[-2000:] or u"（无）", query))
    else:
        yield ("status", u"正在起草…")
        ctx_text = (u"【写作要求】\n{}\n【分析素材】（事实依据，有则必须引用）\n{}\n"
                    u"【对话历史】\n{}"
                    .format(query, material[-3000:] or u"（无）",
                            store.history_text(ctx) or u"（无）"))
    # C5：中断也要把 draft/task/reqs 落盘；user 下次打开仍看到部分改稿
    buf = []
    try:
        for chunk in _stream(prompts.WRITING_PROMPT, ctx_text, label=u"writing"):
            buf.append(chunk)
            yield chunk
    finally:
        new_draft = u"".join(buf).strip()
        patch = {"task": u"writing",
                 "writing_reqs": (reqs + [query])[-8:]}
        if new_draft:
            patch["writing_draft"] = new_draft
        try:
            store.merge_state(conv_id, patch, expected_turn)
        except Exception:
            pass


def _analysis(query, intent, state, ctx, conv_id, expected_turn=None):
    # 纯序号/ID 回复且状态里有候选：一定是选择行为，强制 targeted
    # （意图识别偶尔会把"2"判成 search 重搜一轮，导致用户选择失效）
    if store._ordinal(query) and (state.get("candidates")
                                  or state.get("selected")):
        intent["action"] = "targeted"

    # ---------- chat 分支 ----------
    metrics.inc("pipeline.branch." + (_text(intent.get("action")) or "unknown"))
    if intent["action"] == "chat":
        # 统计/清单类追问（"总数多少""具体哪16个"）不带业务名，意图易误判成
        # chat 而拿全图概况回答（问企业开办总数却答 26775 个节点）。只要对话
        # 里有上下文，就转 fact 让 adhoc 延续话题；真问全图概况（无上下文）
        # 仍走 chat 的数据概况
        if (_COUNT_Q_RE.search(query) or _FOLLOWUP_RE.search(query)) \
                and (state.get("selected") or ctx["history"]):
            intent = {"action": "fact", "keyword": u"", "change_desc": u""}
        else:
            # 意图降级兜底：「」引号业务名 + 影响/变更/分析词 → 确定性转 search。
            # 意图 LLM 输出偶发不可解析时会被静默降级成 chat（_intent 尾部），
            # 这类问法必须保证走到图查询而不是让闲聊提示词硬答（生产事故场景）。
            # 工作台「以此业务继续分析」拼的正是这个句式，这里兜住后点节点分析
            # 也确定性走搜索，不依赖意图 LLM 是否正常
            rescued = _bracket_rescue(query)
            if rescued:
                metrics.inc("intent.bracket_guard")
                runlog.event("intent", query=query,
                             parsed={"action": u"search",
                                     "business_keyword": rescued,
                                     "change_desc": u""},
                             fallback=u"bracket_guard→search")
                intent = {"action": "search", "keyword": rescued,
                          "change_desc": u""}
            else:
                for chunk in _stream(prompts.DIRECT_ANSWER_PROMPT,
                                     prompts.context_block(store.history_text(ctx),
                                                           json.dumps(state, ensure_ascii=False))
                                     + u"\n【系统能力与数据概况】\n" + store.data_overview()
                                     + u"\n【本轮输入】\n" + query, label=u"chat"):
                    yield chunk
                return  # 状态原样保留，不写

    # ---------- fact 分支：事实/清单类问题，直答不套影响分析模板 ----------
    if intent["action"] == "fact":
        # 改道守卫：修改/变更类动作词 + 影响/评估类词同时出现，且提取到了业务名
        # → 是"变更影响分析"请求而非客观事实查询，纠正回 search 走候选搜索，
        # 不进 adhoc（LLM 生成 Cypher 在弱平台不可用，且单次调用耗时数分钟）
        if intent["keyword"] and _CHANGE_RE.search(query) \
                and _IMPACT_RE.search(query):
            metrics.inc("intent.fact_to_search_guard")
            runlog.event("intent", query=query,
                         parsed={"action": u"search",
                                 "business_keyword": intent["keyword"],
                                 "change_desc": intent.get("change_desc") or u""},
                         fallback=u"fact→search(change_impact)")
            intent = {"action": "search", "keyword": intent["keyword"],
                      "change_desc": intent.get("change_desc") or u""}
        else:
            for chunk in _fact(query, intent, state, conv_id, ctx,
                               expected_turn):
                yield chunk
            return

    # ---------- targeted 分支 ----------
    if intent["action"] == "targeted":
        selected, src = store.pick_selected(query, intent["keyword"], state)
        # 退出守卫：本轮消息没有任何显式选择（只是落在旧 selected 上），
        # 而意图里提取到了与该业务无关的新名称 → 用户在提新业务，转 search 重搜
        if (selected and src == u"selected" and intent["keyword"]
                and not _kw_related(intent["keyword"], selected.get("name"))):
            selected = None  # 落入下方 search 分支
        else:
            if not selected:
                for chunk in _stream(prompts.CLARIFY_PROMPT,
                                     prompts.context_block(store.history_text(ctx),
                                                           json.dumps(state, ensure_ascii=False))
                                     + u"\n【本轮输入】\n" + query, label=u"clarify"):
                    yield chunk
                return  # 保留原状态（候选还在）
            # 本轮是序号选择时，接回候选保存时的原始问题与变更描述
            if store._ordinal(query) and _text(state.get("original_query")):
                eff_query = _text(state.get("original_query"))
            else:
                eff_query = query
            eff_intent = {"change_desc": intent["change_desc"] or
                          _text(state.get("change_desc"))}
            for chunk in _analyze(eff_query, eff_intent, selected, ctx,
                                  conv_id, expected_turn):
                yield chunk
            return

    # ---------- search 分支 ----------
    yield ("status", u"正在搜索相关业务…")
    intent["keyword"] = clean_keyword(intent["keyword"])
    cand_labels = (store.core_sem().get("candidate_labels")
                   or config.CANDIDATE_LABELS)

    # 对象级变更优先：用户常说“修改某对象的业务规则”（如“卫生证书”），
    # 但该对象会命中多个业务子项。若图中存在唯一的非候选实体精确名称，
    # 直接以该实体为分析锚点，让 evidence_rows 按配置归并出全部相关业务，
    # 避免把同一对象拆成多个子项让用户盲选一个。
    exact_entities = store.business_search(intent["keyword"], labels=None, limit=50)
    candidate_set = set(cand_labels or [])
    exact_non_candidates = [c for c in exact_entities
                            if _text(c.get("name")).strip().lower() ==
                            _text(intent["keyword"]).strip().lower()
                            and c.get("business_type") not in candidate_set]
    if len(exact_non_candidates) == 1:
        selected = store._norm_selected(exact_non_candidates[0])
        if selected:
            runlog.event("search", keyword=intent["keyword"], stage=u"exact",
                         hits=1,
                         names=[_text(selected.get("name"))])
            for chunk in _analyze(query, intent, selected, ctx, conv_id,
                                  expected_turn):
                yield chunk
            return

    candidates = store.business_search(intent["keyword"],
                                       labels=cand_labels,
                                       limit=config.SEARCH_CANDIDATE_LIMIT)
    if len(candidates) == 0:
        # 全类兜底：业务类没命中，可能问的是系统/部门/表等支撑实体
        candidates = store.business_search(intent["keyword"], labels=None,
                                           limit=config.SEARCH_CANDIDATE_LIMIT)
    if len(candidates) == 0:
        # 模糊兜底：2 字滑窗再搜一次，命中则把相近业务列成可选候选
        # 分层模糊检索：候选业务无命中后再查全实体类型，优先返回对象/系统/
        # 部门等更高层节点；用户确认后由 evidence_rows 沿结构路径归并业务。
        fuzzy = store.fuzzy_search(intent["keyword"], limit=config.CANDIDATE_LIMIT,
                                   labels=None)
        if fuzzy:
            runlog.event("search", keyword=intent["keyword"], stage=u"fuzzy",
                         hits=len(fuzzy),
                         names=[_text(c.get("name")) for c in fuzzy[:10]])
            lines = [u"没有找到与“{}”精确匹配的业务，是不是想找以下业务？请回复序号选择："
                     .format(intent["keyword"])]
            for i, c in enumerate(fuzzy):
                status = store._text(c.get(u"办理状态")).strip()
                suffix = u"，" + status if status else u""
                lines.append(u"{}. 【{}】{}（{}{}）".format(
                    i + 1, c.get("business_type"), c.get("name"),
                    c.get("id"), suffix))
            # 先落盘再输出：候选列表一旦发出，用户回复序号必须能对上库里的
            # 候选快照；保存失败（该轮已被后续提问覆盖）就不展示，避免
            # "可选但选不中"的鬼状态
            if not store.save_state(conv_id, {
                    "step": u"等待用户选择（模糊匹配）",
                    "selected": None,
                    "candidates": fuzzy,
                    "original_query": query,
                    "change_desc": intent["change_desc"],
            }, expected_turn):
                yield (u"会话状态已被更新的提问覆盖，本次候选未保存，"
                       u"请重新发起查询。")
                return
            yield u"\n".join(lines) + u"\n"
            return
        for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                             u"【用户问题】{}\n【搜索关键词】{}".format(query, intent["keyword"]),
                             label=u"not_found"):
            yield chunk
        runlog.event("search", keyword=intent["keyword"], stage=u"not_found",
                     hits=0, names=[])
        store.save_state(conv_id, None, expected_turn)
        return

    runlog.event("search", keyword=intent["keyword"], stage=u"candidates",
                 hits=len(candidates),
                 names=[_text(c.get("name")) for c in candidates[:10]])

    if len(candidates) == 1:
        selected = store._norm_selected(candidates[0])
        for chunk in _analyze(query, intent, selected, ctx, conv_id,
                              expected_turn):
            yield chunk
        return

    # 多个候选：代码直接生成带序号的列表（确定性，序号与 state.candidates 顺序一致），
    # 不过 LLM——之前 LLM 排版会把序号丢掉/打乱顺序，导致用户回复序号对不上
    # 命中达到上限时用 count() 查真实匹配总数并在表头声明，避免用户误以为只有这些
    cap_note = u""
    if len(candidates) >= config.SEARCH_CANDIDATE_LIMIT:
        counts = store.keyword_counts(intent["keyword"], labels=cand_labels)
        total = sum(c for _, c in counts)
        cap_note = (u"（匹配到 {} 个，已按相关度列出前 {} 个；没找到请回复业务 "
                    u"ID 或换更具体的名称）".format(total, len(candidates)))
    lines = [u"找到 {} 个相关业务{}，请回复序号选择：".format(
        len(candidates), cap_note)]
    for i, c in enumerate(candidates):
        status = store._text(c.get(u"办理状态")).strip()
        suffix = u"，" + status if status else u""
        lines.append(u"{}. 【{}】{}（{}{}）".format(
            i + 1, c.get("business_type"), c.get("name"), c.get("id"), suffix))
    # 先落盘再输出（同模糊分支：用户序号回复必须对上库里已保存的候选快照）
    if not store.save_state(conv_id, {
            "step": u"等待用户选择",
            "selected": None,
            "candidates": candidates,
            "original_query": query,
            "change_desc": intent["change_desc"],
    }, expected_turn):
        yield (u"会话状态已被更新的提问覆盖，本次候选未保存，"
               u"请重新发起查询。")
        return
    yield u"\n".join(lines) + u"\n"
    return


def _try_adhoc(query, topic=None, hist=None):
    """自定义问题兜底：固定证据模板查不到时，让 LLM 按【图结构】生成一条
    只读 Cypher（store.graph_query 自带：写关键字/CALL 拒绝 + 标签/关系白名单
    校验 + 强制 LIMIT），执行后基于原始行直答。
    topic：业务关键词；hist：最近对话文本——追问类问题（"具体哪些/总数多少"）
    靠它们延续上下文，否则会脱离话题查全图。
    返回回答文本块列表；不适用/失败返回 []（上层走原 NOT_FOUND/空证据流程）。
    失败自动带错误信息重试一次。连续两轮全失败会熔断 30 分钟（见 _adhoc_fail_streak）。"""
    global _adhoc_fail_streak, _adhoc_disabled_until
    import time as _time
    if _adhoc_fail_streak >= 2 and _time.time() < _adhoc_disabled_until:
        metrics.inc("adhoc.circuit_open")
        runlog.event("adhoc", query=query, skipped=True,
                     note=u"熔断中：LLM 生成 Cypher 连续失败，约 {} 分钟后自动恢复"
                          .format(int((_adhoc_disabled_until - _time.time()) // 60) + 1))
        return []
    ctx = u"【图结构】\n{}".format(store.schema_text())
    if _text(topic).strip():
        ctx += u"\n【业务关键词】{}".format(_text(topic).strip())
    if _text(hist).strip():
        ctx += u"\n【最近对话】\n{}".format(_text(hist).strip())
    ctx += u"\n【用户问题】{}".format(query)
    adhoc_agent = config.get("SFM_ADHOC_AGENT_CODE") or None
    adhoc_timeout = (10, config.get_int("LLM_TIMEOUT_ADHOC_READ", 300))
    for _attempt in range(2):
        raw_text = llm.llm_complete(prompts.adhoc_cypher_prompt(), ctx,
                                    agent_code=adhoc_agent,
                                    timeout=adhoc_timeout)
        r = _extract_json(raw_text)
        cypher = u""
        params = {}
        repairs = []
        tpl_name = _text(r.get("template")).strip()
        if tpl_name:
            # 优先模板选单填槽：模型只做"选题+填参"，弱算力下确定性最高
            tpl = adhoc_template_by_name(tpl_name)
            if tpl:
                mp = r.get("params")
                if not isinstance(mp, dict):
                    mp = {}
                cypher, params, terr = _build_template_cypher(tpl, query, mp)
                if terr or not cypher:
                    log.info(u"adhoc 模板 %s 实例化失败：%s", tpl_name, terr)
                    cypher, params = u"", {}
                else:
                    metrics.inc("adhoc.template_used")
            else:
                metrics.inc("adhoc.template_miss")
        if not cypher:
            # 模板路径未成 → 自由生成回退；执行前过确定性修复层
            cypher = _text(r.get("cypher")).strip()
            params = r.get("parameters")
            if not isinstance(params, dict):
                params = {}
            if cypher:
                cypher, repairs, rerr = _repair_cypher(cypher)
                if rerr:
                    # 不可修复（如属性名错）：精确错误回填重试，不执行不瞎查
                    metrics.inc("adhoc.repair_fail")
                    log.warning(u"adhoc 修复失败：%s | %s", rerr, cypher)
                    runlog.event("adhoc", query=query, attempt=_attempt + 1,
                                 cypher=cypher, template=tpl_name or None,
                                 repair_error=rerr, raw_text=raw_text)
                    ctx += (u"\n【上次生成的查询有误：{}】"
                            u"请修正后重新输出 JSON").format(rerr[:400])
                    continue
                if repairs:
                    metrics.inc("adhoc.repaired")
        if not cypher:
            log.warning(u"adhoc 未生成有效 Cypher: %s", query)
            runlog.event("adhoc", query=query, attempt=_attempt + 1,
                         cypher=None, raw=r, raw_text=raw_text)
            ctx += u"\n【上次未生成有效 Cypher，请重新输出 JSON】"
            continue
        res = store.graph_query(cypher, params, stage=u"LLM生成Cypher")
        runlog.event("adhoc", query=query, attempt=_attempt + 1,
                     cypher=cypher, params=params,
                     template=tpl_name or None,
                     repaired=(u"；".join(repairs) or None),
                     rows=(len(res.get("rows") or [])
                           if res.get("success") else None),
                     error=_text(res.get("error")) or None)
        if res.get("success"):
            rows = res.get("rows") or []
            if not rows:
                log.info(u"adhoc 查询结果为空: %s | %s", query, cypher)
                _adhoc_fail_streak = 0   # 能生成合法 Cypher 并执行 = 生成能力正常
                return []  # 查询合法但结果为空，交给原"未找到"话术更合适
            # 截断判定：行数达到生成语句里的 LIMIT → 确定性断言"不全"，
            # 不依赖回答模型自觉
            lim_m = re.search(u"LIMIT\\s+(\\d+)", cypher, re.I)
            truncated = bool(lim_m and len(rows) >= int(lim_m.group(1)))
            log.info(u"adhoc ok: %d 行%s | %s | %s", len(rows),
                     u"（截断）" if truncated else u"", query, cypher)
            trunc_note = (u"\n【重要】结果行数已达到查询 LIMIT 上限，【确定不全】："
                          u"回答必须明确说明「仅列出前 {N} 条、总数更多」，"
                          u"并建议用户缩小范围（如指定某类/某名称）后再问，"
                          u"【禁止】把本次行数说成总数").format(N=len(rows)) \
                if truncated else u""
            ans_ctx = (u"【用户问题】{}\n{}{}【查询语句】{}\n【查询结果】（共 {} 行）\n{}{}"
                       ).format(query,
                                (u"【业务关键词】{}\n".format(_text(topic).strip())
                                 if _text(topic).strip() else u""),
                                (u"【最近对话】\n{}\n".format(_text(hist).strip())
                                 if _text(hist).strip() else u""),
                                cypher, len(rows),
                                json.dumps(rows, ensure_ascii=False), trunc_note)
            chunks = []
            for ch in _stream(prompts.ADHOC_ANSWER_PROMPT, ans_ctx,
                              label=u"adhoc_answer"):
                chunks.append(ch)
            if not chunks:
                return [u"（查询已执行，但未能生成回答，请换个问法）"]
            # 确定性截断声明：回答模型可能不守规矩，代码层面再保底一次
            if truncated:
                chunks.append(u"\n\n---\n（系统提示：本回答基于前 {} 条查询结果，"
                              u"已达查询上限，并非全量。如需完整清单，"
                              u"请缩小范围（指定某类/某名称）后再问。）".format(len(rows)))
            _adhoc_fail_streak = 0
            return chunks
        log.warning(u"adhoc 查询失败: %s | %s",
                    _text(res.get("error")), cypher)
        # P5：错误里有时夹着长长的 Cypher 栈/原始行，截断避免 ctx 失控
        err_short = _text(res.get("error"))[:400]
        ctx += (u"\n【上次生成的查询执行失败，原因：{}】"
                u"请修正后重新输出 JSON").format(err_short)
        # 再加一道整体上限：极端情况下 schema+hist+错误 可能顶到 token 限制
        if len(ctx) > 12000:
            ctx = ctx[:4000] + u"\n……（省略中段）……\n" + ctx[-8000:]
    # 两轮都没拿到可执行结果：累计连续失败，触发熔断阈值后一段时间内直接跳过
    _adhoc_fail_streak += 1
    if _adhoc_fail_streak >= 2:
        _adhoc_disabled_until = _time.time() + _ADHOC_BREAKER_SEC
    return []


def _resolve_label(raw):
    """原始词 → 真实标签：别名表命中优先，其次直接认 registry 标签；失败返回 u""。
    （原 _match_fact_template 内联逻辑抽出，供 fact 模板与 adhoc 模板选单共用）"""
    raw = _text(raw).strip()
    if not raw:
        return u""
    import gateway as _gateway
    reg = store.type_registry() or {}
    if raw in reg:
        return raw
    aliases = (_gateway.load_schema_notes() or {}).get("aliases") or {}
    if isinstance(aliases, dict):
        lbs = aliases.get(raw)
        if isinstance(lbs, list) and lbs and _text(lbs[0]).strip() in reg:
            return _text(lbs[0]).strip()
    return u""


def _build_template_cypher(tpl, query, model_params=None):
    """模板 → 可执行 (cypher, params, err)。标签/参数解析优先级：
    1) pattern 正则命名组（fact 模板原有路径，含别名解析）
    2) LLM 选模板时填的 params（kw=关键词、lbl=标签名，lbl 过 _resolve_label），
       覆盖/补足正则抽不到的参数（措辞对不上正则但模板对的场景）
    pattern 未命中且 LLM 也没填参数 → err（沿用旧行为：正则没匹配不裸跑模板）。
    返回的 params 已按最终语句里实际出现的 $参数过滤（多余参数不发给图库）。"""
    pat = _text(tpl.get("pattern"))
    cyp = _text(tpl.get("cypher"))
    if not pat or not cyp:
        return None, None, u"模板缺少 pattern/cypher"
    label_from = _text(tpl.get("label_from")).strip()
    groups = {}
    try:
        m = re.search(pat, _text(query).strip())
    except re.error as e:
        return None, None, u"模板 pattern 非法：{}".format(e)
    if m:
        groups = m.groupdict() or {}
    mp = model_params if isinstance(model_params, dict) else {}
    label = u""
    if label_from:
        label = _resolve_label(groups.get(label_from) or u"")
        if not label:
            label = _resolve_label(mp.get(u"lbl") or mp.get(u"label") or u"")
        if not label:
            return None, None, (u"模板「{}」需要标签参数但未能解析出真实标签"
                                u"（可用：{}）".format(_text(tpl.get("name")),
                                                       u"、".join(sorted(store.type_registry()))))
    np_prop = u""
    if label:
        np_prop = _text(((store.type_registry() or {}).get(label) or {})
                        .get("name_prop"))
    np_prop = np_prop or u"名称"
    params = {}
    for gname, gval in groups.items():
        if gname == label_from or gval is None:
            continue
        v = _text(gval).strip()
        if v:
            params[gname] = v
    for k, v in mp.items():
        kk = _text(k).strip()
        if not kk or kk in (u"lbl", u"label"):
            continue
        vv = _text(v).strip()
        if vv and kk not in params:
            params[kk] = vv
    if m is None and not params:
        # 正则没匹配上、LLM 也没填参数：不执行（避免无过滤裸查全库）
        return None, None, u"模板 pattern 未命中且无参数"
    try:
        cypher = cyp.format(lbl=label, np=np_prop)
    except (KeyError, IndexError, ValueError) as e:
        return None, None, u"模板参数替换失败：{}".format(e)
    used = set(re.findall(u"\\$([A-Za-z_][A-Za-z0-9_]*)", cypher))
    params = dict((k, v) for k, v in params.items() if k in used)
    return cypher, params, u""


def adhoc_template_by_name(name):
    """按名称取查询模板：精确匹配优先，其次唯一包含命中；找不到返回 None。"""
    import gateway as _gateway
    name = _text(name).strip()
    if not name:
        return None
    tpls = [t for t in ((_gateway.load_schema_notes() or {})
                        .get("query_templates") or [])
            if isinstance(t, dict) and _text(t.get("name")).strip()]
    for t in tpls:
        if _text(t.get("name")).strip() == name:
            return t
    hits = [t for t in tpls if name in _text(t.get("name"))]
    return hits[0] if len(hits) == 1 else None


def _match_fact_template(query):
    """按 schema_notes.query_templates 的正则匹配用户问题，命中则直接执行
    Cypher 直答。返回 (rows, template_name)；未命中/执行失败/结果为空 →
    (None, None)。标签/参数解析统一走 _build_template_cypher（与 adhoc
    模板选单共用同一套逻辑，改一处两处生效）。"""
    import gateway as _gateway
    templates = (_gateway.load_schema_notes() or {}).get("query_templates") or []
    q = _text(query).strip()
    if not templates or not q:
        return None, None
    for tpl in templates:
        if not isinstance(tpl, dict):
            continue
        cypher, params, terr = _build_template_cypher(tpl, q)
        if terr or not cypher:
            continue
        res = store.graph_query(cypher, params, stage=u"事实模板查询")
        if not res.get("success"):
            log.warning(u"fact template '%s' 执行失败: %s | %s",
                        tpl.get("name"), res.get("error"), cypher)
            continue
        rows = res.get("rows") or []
        if not rows:
            # 命中模板但图里没数据，继续试下一个模板（避免误兜底到空结果）
            continue
        return rows, _text(tpl.get("name")) or u""
    return None, None


# ---- adhoc Cypher 确定性修复器（执行前静态校验 + 自动修复） ----
# 弱模型生成翻车四类：JSON 脏（_extract_json 已管）/ 语法半角全角混 / 编造标签
# 关系 / 属性名错。本层管后三类：语法规范化 + 未知类型唯一近似映射改写；
# 属性名错不静默改写（改属性=改语义，猜不得），报精确错误回填重试。
_FULLWIDTH_MAP = {
    u"（": u"(", u"）": u")", u"，": u",", u"；": u";", u"：": u":",
    u"！": u"!", u"？": u"?", u"＝": u"=", u"“": u'"', u"”": u'"',
    u"‘": u"'", u"’": u"'",
}
_IDENT_START_RE = re.compile(u"[一-龥A-Za-z_]")
_IDENT_RUN_RE = re.compile(u"[一-龥A-Za-z_][一-龥A-Za-z0-9_]*")


def _skip_string(src, i):
    """src[i] 是引号（' 或 "）起始时，返回结束引号下标；未闭合返回 -1"""
    q = src[i]
    j = i + 1
    n = len(src)
    while j < n:
        ch = src[j]
        if ch == u"\\":
            j += 2
            continue
        if ch == q:
            return j
        j += 1
    return -1


def _scan_type_spans(src):
    """扫出 Cypher 里所有"类型位"（冒号引出的标签/关系名）：
    [(start, end, name, style)]，span 含前导冒号；
    style ∈ bt(反引号)/sq(单引号)/dq(双引号)/bare(裸标识符)。
    字符串字面量内部的冒号不扫；带反引号但无冒号前缀的是属性名，不归这里管。"""
    spans = []
    n = len(src)
    i = 0
    while i < n:
        ch = src[i]
        if ch in (u"'", u'"'):
            j = _skip_string(src, i)
            i = (j + 1) if j >= 0 else n
            continue
        if ch == u"`":
            j = src.find(u"`", i + 1)
            i = (j + 1) if j >= 0 else n
            continue
        if ch == u":":
            k = i + 1
            while k < n and src[k] in u" \t":
                k += 1
            if k < n and src[k] == u"`":
                j = src.find(u"`", k + 1)
                if j > k:
                    spans.append((i, j + 1, src[k + 1:j], u"bt"))
                    i = j + 1
                    continue
                i = n
                continue
            if k < n and src[k] in (u"'", u'"'):
                j = _skip_string(src, k)
                if j > k:
                    spans.append((i, j + 1, src[k + 1:j],
                                  u"sq" if src[k] == u"'" else u"dq"))
                    i = j + 1
                    continue
                i = n
                continue
            m = _IDENT_RUN_RE.match(src[k:])
            if m:
                spans.append((i, k + len(m.group(0)), m.group(0), u"bare"))
                i = k + len(m.group(0))
                continue
        i += 1
    return spans


def _scan_prop_spans(src):
    """扫出 var.prop 形式的属性引用：[(start, end, name, style)]，
    span 只覆盖属性名（含引号）。数字小数（1.5）与变长路径（*1..6）不算。"""
    spans = []
    n = len(src)
    i = 0
    while i < n:
        ch = src[i]
        if ch in (u"'", u'"'):
            j = _skip_string(src, i)
            i = (j + 1) if j >= 0 else n
            continue
        if ch == u"`":
            j = src.find(u"`", i + 1)
            i = (j + 1) if j >= 0 else n
            continue
        if ch == u"." and i > 0:
            prev = src[i - 1]
            nxt = src[i + 1] if i + 1 < n else u""
            if prev.isdigit() and (nxt.isdigit() or nxt == u"."):
                i += 1
                continue
            if not _IDENT_START_RE.match(prev):
                i += 1
                continue
            if nxt == u"`":
                j = src.find(u"`", i + 2)
                if j > i + 1:
                    spans.append((i + 1, j, src[i + 2:j], u"bt"))
                    i = j + 1
                    continue
                i = n
                continue
            m = _IDENT_RUN_RE.match(src[i + 1:])
            if m:
                spans.append((i + 1, i + 1 + len(m.group(0)), m.group(0), u"bare"))
                i = i + 1 + len(m.group(0))
                continue
        i += 1
    return spans


def _name_sim(a, b):
    """字符 bigram Dice 相似度（中英混合类型名的近似映射用）"""
    a = _text(a)
    b = _text(b)
    if a == b:
        return 1.0
    if len(a) < 2 or len(b) < 2:
        return 0.0
    ab = set(a[i:i + 2] for i in range(len(a) - 1))
    bb = set(b[i:i + 2] for i in range(len(b) - 1))
    if not ab or not bb:
        return 0.0
    return (2.0 * len(ab & bb)) / (len(ab) + len(bb))


def _nearest_type(name, known, threshold=0.4):
    """唯一近似匹配：相似度最高且明显领先第二名才认（宁可报错不可改错）"""
    scored = sorted(((_name_sim(name, k), k) for k in known), reverse=True)
    if not scored or scored[0][0] < threshold:
        return None
    if len(scored) > 1 and scored[0][0] - scored[1][0] < 0.1:
        return None
    return scored[0][1]


def _repair_cypher(cypher):
    """adhoc 生成的 Cypher 执行前修复。返回 (cypher, repairs, err)；
    err 非空 = 不可修复（附可用清单，回填 ctx 供重试）。repairs 是修复动作清单。
    - 语法规范化：字符串字面量外的全角符号、!= → <>、句尾分号
    - 类型位（标签/关系）：未知项按字符 bigram 唯一近似映射改写（统一反引号）
    - 属性：裸写属性补反引号；不存在的属性不猜，报精确错误"""
    import gateway as _gateway
    src = _text(cypher).strip()
    if not src:
        return u"", [], u"空查询"
    schema, serr = _gateway.build_schema()
    if serr or not (schema.get("node_types") or schema.get("relationship_types")):
        # 图结构拿不到：跳过静态修复，让执行层报错兜底
        return src, [], u""
    labels = set(_text(n.get("name")) for n in schema.get("node_types") or [])
    rels = set(_text(r.get("name")) for r in schema.get("relationship_types") or [])
    known = labels | rels
    props = set()
    for n in schema.get("node_types") or []:
        for p in (n.get("properties") or []):
            props.add(_text(p.get("name") if isinstance(p, dict) else p))
    props.discard(u"")

    # -- 1) 语法规范化（字符串字面量外） --
    repairs = []
    buf = []
    in_str = False
    esc = False
    i = 0
    n = len(src)
    while i < n:
        ch = src[i]
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == u"\\":
                esc = True
            elif ch == u"'":
                in_str = False
            i += 1
            continue
        if ch == u"'":
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == u"!" and i + 1 < n and src[i + 1] == u"=":
            buf.append(u"<>")
            repairs.append(u"!= → <>")
            i += 2
            continue
        rep = _FULLWIDTH_MAP.get(ch)
        if rep is not None:
            buf.append(rep)
            repairs.append(u"全角符号 {}→{}".format(ch, rep))
            i += 1
            continue
        buf.append(ch)
        i += 1
    norm = u"".join(buf).strip()
    if norm.endswith(u";"):
        norm = norm[:-1].rstrip()
        repairs.append(u"剥句尾分号")
    if not norm:
        return u"", repairs, u"空查询"
    if not known:
        return norm, repairs, u""

    # -- 2) 类型位校验/修复 --
    edits = []
    for start, end, name, style in _scan_type_spans(norm):
        if name in known:
            if style == u"bt":
                continue
            edits.append((start, end, u":`{}`".format(name)))
            repairs.append(u"类型 {} 的引号统一为反引号".format(name))
            continue
        near = _nearest_type(name, known)
        if near:
            edits.append((start, end, u":`{}`".format(near)))
            repairs.append(u"未知类型 {} → 近似映射 {}".format(name, near))
            continue
        return (norm, repairs,
                u"类型「{}」不存在且找不到唯一近似；可用节点类型：{}；可用关系类型：{}"
                .format(name,
                        u"、".join(sorted(labels)), u"、".join(sorted(rels))))

    # -- 3) 属性校验（裸写补反引号；不存在不猜） --
    if props:
        for start, end, name, style in _scan_prop_spans(norm):
            if style == u"bt":
                continue
            if name in props:
                if style == u"bare":
                    edits.append((start, end, u"`{}`".format(name)))
                    repairs.append(u"属性 {} 补反引号".format(name))
                continue
            return (norm, repairs,
                    u"属性「{}」不存在（改属性=改语义，系统不代改）；"
                    u"请只用【图结构】里的真实属性名，如：{}"
                    .format(name, u"、".join(sorted(props)[:40])))

    # -- 应用改写（倒序拼接，位置互不重叠：类型位带冒号、属性位不带） --
    if edits:
        edits.sort(key=lambda e: e[0], reverse=True)
        for start, end, new in edits:
            norm = norm[:start] + new + norm[end:]
    seen = set()
    repairs = [r for r in repairs if not (r in seen or seen.add(r))]
    return norm, repairs, u""


def _fact(query, intent, state, conv_id, ctx=None, expected_turn=None):
    """事实直答：定位业务（已选/搜索/模糊）→ 拉证据 → 直答用户问的维度；
    模板证据查不到时走 _adhoc（LLM 生成只读 Cypher 兜底）。
    追问类问题（"具体哪些/总数多少"）本身不含业务名，靠最近对话延续话题。"""
    yield ("status", u"正在查询业务信息…")
    hist = u"\n".join(u"{}: {}".format(m.get("role"),
                                       (m.get("content") or u"")[:150])
                      for m in ((ctx or {}).get("history") or [])[-3:])
    # 固定模板优先：schema_notes.query_templates 命中则直接执行 Cypher 直答，
    # 跳过 LLM 生成（高频问法：统计数量/清单/A 承载在哪些系统 等）
    tpl_rows, tpl_name = _match_fact_template(query)
    if tpl_rows is not None:
        yield ("status", u"模板命中：{}".format(tpl_name or u"query_template"))
        rel_meanings = json.dumps(
            (app_schema() or {}).get("relationship_meanings") or {},
            ensure_ascii=False)
        ctx_text = (u"【用户问题】{}\n【模板】{}\n【证据】（共 {} 行）\n{}\n"
                    u"【关系含义】{}".format(
                        query, tpl_name, len(tpl_rows),
                        json.dumps(tpl_rows, ensure_ascii=False),
                        rel_meanings))
        for chunk in _stream(prompts.FACT_PROMPT, ctx_text, label=u"fact"):
            yield chunk
        return
    topic = intent["keyword"] or u""
    sel, sel_src = store.pick_selected(query, intent["keyword"], state)
    # 退出守卫：只是落在旧 selected 上、而意图提取到了无关新名称 → 不沿用旧选择
    if (sel and sel_src == u"selected" and intent["keyword"]
            and not _kw_related(intent["keyword"], sel.get("name"))):
        sel = None
    # E3 共用：search 分支里才赋值，sel 分支默认留空
    candidates = None
    fuzzy_matched = False
    if sel:
        businesses = [sel]
        scope = u"已选定实体：{}".format(json.dumps(sel, ensure_ascii=False))
    else:
        # 追问句式优先：问法本身是"具体哪些/总数多少"且上下文有话题时，
        # 无论意图提取出什么关键词（常是"16个""哪16个"这类垃圾），都交给
        # adhoc 带最近对话延续话题
        if (_FOLLOWUP_RE.search(query) or _COUNT_Q_RE.search(query)) and hist:
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, intent["keyword"], hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【最近对话】\n{}".format(query, hist),
                                     label=u"not_found"):
                    yield chunk
            return
        intent["keyword"] = clean_keyword(intent["keyword"])
        # 不含汉字/字母的关键词（如"具体哪16个"被提取出的 16、16个）不是
        # 业务名，搜出来全是垃圾匹配，视作无关键词走追问链路
        if intent["keyword"] and not re.search(u"[一-龥A-Za-z]", intent["keyword"]):
            intent["keyword"] = u""
        # 无关键词但有对话上下文：追问句式（"具体哪些/总数多少"），直接让
        # adhoc 带着最近对话延续话题；比候选搜索拿垃圾匹配可靠
        if not intent["keyword"] and hist:
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【最近对话】\n{}".format(query, hist),
                                     label=u"not_found"):
                    yield chunk
            return
        # 关键词恰好是实体类型名（如"信息系统""业务事项"）：用户问的是整类
        # 实体，名称 CONTAINS 搜索只会命中描述里带该词的任意 10 条（以偏概全），
        # 直接走 adhoc 按问题生成聚合/清单查询
        if intent["keyword"] and intent["keyword"] in store.type_registry():
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【搜索关键词】{}".format(
                                         query, intent["keyword"]),
                                     label=u"not_found"):
                    yield chunk
            return
        # 统计类问题（"有几个/多少个"）：候选搜索有截断上限，拿截断样本数个数
        # 必然答错（16 个只搜到 10 个就答 10）；优先 adhoc 生成 count() 聚合，
        # 失败再用代码确定性 count 兜底——问数量的必须给数字，不能退化成列表
        if _COUNT_Q_RE.search(query):
            yield ("status", u"正在按问题生成统计查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
                return
            anchor = store.core_sem().get("anchor_label")
            counts = (store.keyword_counts(intent["keyword"], labels=[anchor])
                      if anchor else [])
            if not counts:
                counts = store.keyword_counts(intent["keyword"])
            if counts:
                total = sum(c for _, c in counts)
                lines = [u"「{}」相关实体共 **{}** 个（按名称/别名包含关键词精确统计）："
                         .format(intent["keyword"], total)]
                lines.extend(u"- 【{}】{} 个".format(l, c) for l, c in counts)
                lines.append(u"想看具体清单，可以问「{}都有哪些」。".format(
                    intent["keyword"]))
                yield u"\n".join(lines) + u"\n"
                return
        candidates = store.business_search(
            intent["keyword"],
            labels=(store.core_sem().get("candidate_labels")
                    or config.CANDIDATE_LABELS),
            limit=config.FACT_CANDIDATE_LIMIT)
        if not candidates:
            # 全类兜底：可能是系统/部门/表等支撑实体
            candidates = store.business_search(
                intent["keyword"], labels=None, limit=config.FACT_CANDIDATE_LIMIT)
        fuzzy_matched = False
        if not candidates:
            # adhoc 优先于 fuzzy：全量/统计/关系类问题（"都承载在哪些系统"）
            # 靠 fuzzy 只能拿到任意 10 个实体，会以偏概全；adhoc 能真实聚合。
            # adhoc 不适用（返回空）再 fuzzy（名称打不准的场景）
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
                return
            candidates = store.fuzzy_search(intent["keyword"],
                                            limit=config.CANDIDATE_LIMIT)
            fuzzy_matched = bool(candidates)
        if not candidates:
            for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                 u"【用户问题】{}\n【搜索关键词】{}".format(
                                     query, intent["keyword"])):
                yield chunk
            return
        businesses = candidates
        # 有名称完全匹配的候选时，只答它（用户问的是具体实体）
        kw = intent["keyword"].strip()
        exact = [c for c in candidates
                 if _text(c.get("name")).strip() == kw]
        if len(exact) == 1:
            businesses = exact
        # scope 列到 FACT_CANDIDATE_LIMIT，与证据查询对齐，不再硬切 30
        scope = u"匹配到的实体（{} 个）：{}".format(
            len(businesses),
            u"；".join(u"【{}】{}（{}）".format(c.get("business_type"),
                                              c.get("name"), c.get("id"))
                       for c in businesses[:config.FACT_CANDIDATE_LIMIT]))
        # 搜索命中数达到上限：说明实际可能更多（被截断），必须声明非全量，
        # 禁止把样本数当总数（"企业开办有 10 个业务"这种确定性错答就是这么来的）
        if len(candidates) >= config.FACT_CANDIDATE_LIMIT:
            scope += (u"\n（注意：搜索命中已达上限 {} 个，实际可能更多；回答必须说明"
                      u"仅覆盖这些实体，总数请用统计问法如\"XX共有多少个\"）"
                      .format(config.FACT_CANDIDATE_LIMIT))
        if fuzzy_matched:
            # fuzzy 是滑窗任意子集（如"系统"命中全部信息系统但只取前10个），
            # 必须让回答模型声明"仅覆盖这些实体、非全量"
            scope += u"\n（注意：以上实体为模糊匹配的部分结果，非全量；回答必须说明仅覆盖这些实体）"

    # 按实体类型分组拉证据（不同类型的主键/子结构不同）
    rows = []
    groups = {}
    for c in businesses:
        groups.setdefault(c.get("type") or c.get("business_type"), []).append(c)
    for etype, items in groups.items():
        rows.extend(store.fact_rows(etype, [i.get("id") for i in items]))
    rel_meanings = json.dumps(
        (app_schema() or {}).get("relationship_meanings") or {},
        ensure_ascii=False)
    ctx_text = (u"【用户问题】{}\n【实体范围】{}\n【证据】（共 {} 行）\n{}\n"
                u"【关系含义】{}".format(
                    query, scope, len(rows),
                    json.dumps(rows, ensure_ascii=False), rel_meanings))
    # E3：候选截断或模糊匹配时，LLM 若忘声明"非全量"就会被当成完整答案，
    # 这里不管模型怎么写，代码在流尾强制补一行提示
    truncated_hint = u""
    if candidates is not None and len(candidates) >= config.FACT_CANDIDATE_LIMIT:
        truncated_hint = (
            u"\n\n---\n（系统提示：本回答基于前 {N} 条候选，候选命中已达上限，"
            u"实际可能更多；并非全量。如需完整清单，请换更具体的名称或问"
            u"\"XX共有多少个\"等统计问法。）"
        ).format(N=config.FACT_CANDIDATE_LIMIT)
    elif fuzzy_matched:
        truncated_hint = (
            u"\n\n---\n（系统提示：以上实体由模糊匹配截取，并非全量。如需完整"
            u"清单，请换更具体的名称后再问。）")
    if rows:
        for chunk in _stream(prompts.FACT_PROMPT, ctx_text, label=u"fact"):
            yield chunk
        if truncated_hint:
            yield truncated_hint
    else:
        # 模板证据为空：该问题维度可能不在固定邻接模板里，adhoc 兜底直查
        yield ("status", u"正在按问题生成图查询…")
        adhoc_chunks = _try_adhoc(query, topic, hist)
        if adhoc_chunks:
            for chunk in adhoc_chunks:
                yield chunk
        else:
            for chunk in _stream(prompts.FACT_PROMPT, ctx_text, label=u"fact"):
                yield chunk
            if truncated_hint:
                yield truncated_hint
    # 单一业务时顺手记住，方便用户下一句直接"分析下影响"；
    # 候选列表保留（不清空），用户可用序号换选其他候选
    if sel or len(businesses) == 1:
        c = businesses[0]
        yield ("graph", {"entity": store._norm_selected(c) or {
            "type": c.get("business_type"), "id": c.get("id"),
            "name": c.get("name")}})
        store.save_state(conv_id, {
            "step": u"已选定业务",
            "selected": store._norm_selected(c),
            "candidates": (state.get("candidates") or []),
            "original_query": query,
            "change_desc": u"",
        }, expected_turn)


def app_schema():
    import gateway
    data, err = gateway.build_schema()
    return data if not err else {}


def _analyze(query, intent, selected, ctx, conv_id, expected_turn=None):
    """已确定业务 → selected 校验 → 分级证据查询 → 影响分析。
    会话状态维护在本包装层：仅当实体解析成功且分析未整体失败时才固化
    selected（支持追问）；否则清除 selected、保留候选列表供换选。
    分析主体见 stream_analysis（工作台节点分析共用）。"""
    result = None
    for item in stream_analysis(query, intent, selected):
        # ("result", {...}) 是 §13.3 内部结构化执行结果，只在此消费，
        # 不向对话流透出（工作台链路也应忽略该事件）
        if (isinstance(item, tuple) and item and item[0] == "result"):
            result = item[1] if len(item) > 1 and isinstance(item[1], dict) \
                else {}
            continue
        yield item
    candidates = (ctx.get("state") or {}).get("candidates") or []
    status = _text((result or {}).get(u"analysis_status"))
    if status in (u"success", u"partial"):
        # 分析有效：保留已选业务（支持追问）和候选列表（支持序号换选）。
        # partial（部分证据失败）也固化——候选已可用，仅证据不完整。
        store.save_state(conv_id, {
            "step": u"已选定业务",
            "selected": store._norm_selected(selected),
            "candidates": candidates,
            "original_query": query,
            "change_desc": intent.get("change_desc") or u"",
        }, expected_turn)
    else:
        # 实体未解析 / 分析整体失败：不固化 selected（防止追问打在无效实体上），
        # 保留候选列表以便换选；失败标记供前端/提示层参考
        store.merge_state(conv_id, {
            "selected": None,
            "step": u"待选定业务",
            "analysis_failed": True,
            "candidates": candidates,
        }, expected_turn)


def _evidence_graph(bundle, cap=60):
    """把证据 bundle 的全部行转成前端可直接渲染的节点/边（分析报告下方
    内嵌图）。复用 store.rows_to_graph 契约 B（证据行形状，按 dir 定方向、
    hop 标注直达/间接）；超 cap 按节点度数裁剪（锚点等高连接节点自然保留），
    再丢两端都被裁掉的边。返回 {"nodes","edges","truncated","total_nodes"}；
    没有任何可画行返回 None。"""
    rows = (list(bundle.get(u"dependency_candidates") or [])
            + list(bundle.get(u"direct_dependencies") or [])
            + list(bundle.get(u"indirect_dependencies") or [])
            + list(bundle.get(u"association_evidence") or [])
            + list(bundle.get(u"composition_evidence") or []))
    if not rows:
        return None
    g = store.rows_to_graph(rows, [u"src", u"src_type", u"rel", u"dir",
                                   u"nb_type", u"nb_name", u"hop"])
    nodes = g.get(u"nodes") or []
    edges = g.get(u"edges") or []
    if not nodes or not edges:
        return None
    total = len(nodes)
    if total > cap:
        deg = {}
        for e in edges:
            deg[e[u"source"]] = deg.get(e[u"source"], 0) + 1
            deg[e[u"target"]] = deg.get(e[u"target"], 0) + 1
        keep = set(sorted(deg, key=lambda k: (-deg[k], k))[:cap])
        nodes = [n for n in nodes if n[u"id"] in keep]
        edges = [e for e in edges
                 if e[u"source"] in keep and e[u"target"] in keep]
    return {u"nodes": nodes, u"edges": edges,
            u"truncated": total > cap, u"total_nodes": total}


def _schema_ref_slim(rows, anchor_type=None):
    """按本轮证据行裁剪 schema 参考文本：节点类型只留证据中出现过的
    （锚点类型兜底——证据行常缺 src_type，rows_to_graph 同样按 anchor 兜底），
    关系类型只留证据关系且两端类型都在证据里的 pair，关系含义只留证据
    关系的条目。防止无关 schema 行被模型误读成本轮事实（实测案例：
    组织树父类型「海关部门」被当成分析对象的主管部门写进报告）。
    证据行提取不到任何类型/关系时回退全量 store.schema_text()；
    影响规则是分析语义的依据（传播关系/方向），始终保留。"""
    rels = set()
    types = set()
    for r in rows:
        rel = _text(r.get("rel"))
        if rel:
            rels.add(rel)
        for k in (u"src_type", u"nb_type"):
            t = _text(r.get(k))
            if t:
                types.add(t)
    if not rels and not types:
        return store.schema_text()
    if anchor_type:
        types.add(_text(anchor_type))
    import gateway
    data, err = gateway.build_schema()
    if err:
        return u"（图结构读取失败：{}）".format(err)
    nodes = []
    for n in (data.get("node_types") or []):
        if _text(n.get("name")) not in types:
            continue
        props = u"、".join(p["name"] if isinstance(p, dict) else p
                           for p in (n.get("properties") or []))
        nodes.append(u"{}（唯一标识: {}，属性: {}，共{}条）".format(
            n.get("name"), n.get("unique_key"), props, n.get("count")))
    rel_lines = []
    for r in (data.get("relationship_types") or []):
        if _text(r.get("name")) not in rels:
            continue
        pairs = [p for p in (r.get("pairs") or [])
                 if all(_text(seg) in types for seg in p.split(u"->"))]
        if pairs:
            rel_lines.append(u"{}: {}（有向）".format(
                r.get("name"), u"、".join(pairs)))
    meanings = {}
    for k, v in (data.get("relationship_meanings") or {}).items():
        k = _text(k)
        if k in rels:
            meanings[k] = _text(v).strip() or k
    for k in rels:
        # 未配置含义的关系用字面意思兜底（中文关系名自描述），不留空词典
        if k not in meanings:
            meanings[k] = k
    parts = []
    if nodes:
        parts.append(u"节点类型：\n" + u"\n".join(nodes))
    if rel_lines:
        parts.append(u"关系类型：\n" + u"\n".join(rel_lines))
    parts.append(u"影响规则：" + json.dumps(
        data.get("influence_rules") or {}, ensure_ascii=False))
    parts.append(u"关系含义：" + json.dumps(meanings, ensure_ascii=False))
    return u"（仅本轮证据涉及的部分，未列出的类型/关系与本轮无关）\n" \
        + u"\n\n".join(parts)


def _node_props_brief(cache, ent_type, key, name):
    """节点关键属性摘要「属性=值；属性=值」（最多 4 项，值截 60 字）。
    查询失败/超过 40 个节点时返回空串，绝不影响主流程。
    cache 为本轮共享 dict，兼做去重与调用上限。"""
    et = _text(ent_type).strip()
    k = _text(key).strip()
    if not et or not k:
        return u""
    ck = (et, k)
    if ck in cache:
        return cache[ck]
    if len(cache) >= 40:
        return u""
    brief = u""
    try:
        d = store.node_props(et, k) or {}
        parts = []
        for p in (d.get(u"props") or {}):
            if not _text(p).strip():
                continue
            sv = _text(d[u"props"][p]).strip()
            if not sv or sv == _text(name).strip():
                continue
            if len(sv) > 60:
                sv = sv[:60] + u"…"
            parts.append(u"{}={}".format(p, sv))
            if len(parts) >= 4:
                break
        brief = u"；".join(parts)
    except Exception:
        brief = u""
    cache[ck] = brief
    return brief


def _fallback_report(selected, lines, upstream_text, rollup_bizs):
    """分析 LLM 失败/空流时的代码降级报告：内容全部来自查询结果
    （预计算证据行/承载系统/上游依赖/归属业务），零 LLM 参与——
    保证弱平台任何状态下分析链路都输出真实报告，绝不空手或编造。"""
    name = _text((selected or {}).get("name")) or u"分析对象"
    btype = _text((selected or {}).get("business_type"))
    out = [u"## {} 变更影响核查报告（系统降级版）\n".format(name),
           u"（大模型排版服务本次不可用，以下内容为系统直接根据图谱查询"
           u"结果整理的核查清单，全部来自真实查询数据。）\n"]
    head = u"**分析对象**：{}".format(name)
    if btype:
        head += u"（{}）".format(btype)
    out.append(head + u"\n")
    if rollup_bizs:
        out.append(u"**归属业务**：{}\n".format(u"、".join(rollup_bizs)))
    out.append(u"**查询到的相关事项**\n")
    if lines:
        out.append(u"\n".join(lines) + u"\n")
    else:
        out.append(u"本次查询未列出其他受影响业务；这不代表没有影响，"
                   u"建议结合实际业务进一步核实。\n")
    if upstream_text and upstream_text != u"（无）":
        out.append(u"\n**上游依赖条件**（被分析业务依赖它们，属前置条件，"
                   u"不是受影响方）\n{}\n".format(upstream_text))
    return out


def stream_analysis(query, intent, selected):
    """影响分析主体（无会话副作用）：selected 校验 → 分级证据查询 →
    代码分桶预计算 → LLM 排版。供对话 _analyze 与工作台节点智能分析
    （server /graph/analyze）共用。
    yield ("status", 文本) / 文本块 / ("graph", {"entity": ...}) /
    ("graph_data", 证据图 nodes/edges) / ("result", §13.3 结构化执行结果，
    供上层判断会话固化与重试) / ("error", 消息)。"""
    # §5.5：入口先校验 selected（类型存在 / ID 非空 / 主键有效），
    # 不合法直接报错，绝不带病查询
    yield ("status", u"正在校验选定的业务…")
    ref, sel_err = store.resolve_selected(selected)
    if sel_err:
        yield ("error", sel_err)
        yield ("result", {
            u"entity_resolved": False,
            u"analysis_status": u"failed",
            u"evidence_status": u"error",
            u"selected_valid": False,
            u"can_retry": False,
            u"selected": selected,
        })
        return

    # 校验通过：发图谱卡片（前端"展开图谱"在工作台打开该实体邻接图）
    yield ("graph", {"entity": selected})
    yield ("status", u"正在查询图数据库（关联/组成/依赖分级证据）…")
    bundle = store.build_evidence_bundle(ref.get("type"), ref.get("id_value"))

    if bundle.get(u"query_state") == u"failed":
        # O4：图库证据查询失败，先道歉再给出具体可执行的下一步，
        # 不要只甩一句"请稍后重试"
        err_short = _text(bundle.get(u"error"))[:300]
        for chunk in _stream(
                prompts.APOLOGIZE_PROMPT,
                u"【错误信息】{}\n【请在回答末尾附上以下建议】\n"
                u"- 稍等 1-2 分钟后重试同一条分析\n"
                u"- 若多次失败，到「系统配置 → 测试图库连接」确认 Galaxybase 地址"
                u"/账号/图名是否正确、服务是否可达\n"
                u"- 急需结果时可改用「图谱工作台」直接浏览该实体邻接，"
                u"查看结构关联（不走依赖传播推理）".format(err_short),
                label=u"apologize"):
            yield chunk
        yield ("result", {
            u"entity_resolved": True,
            u"analysis_status": u"failed",
            u"evidence_status": u"error",
            u"selected_valid": True,
            u"can_retry": True,
            u"selected": selected,
        })
        return

    # 报告下方内嵌证据图：证据行直接转 nodes/edges 发给前端（在报告流之前
    # 发——server 持久化时并入消息 graph 字段，客户端中途断开刷新也能还原）
    gdata = _evidence_graph(bundle)
    if gdata:
        yield ("graph_data", gdata)

    # ---- 代码预计算结论（不让 LLM 解码行方向，杜绝误读）----
    # 依赖候选 = 实体自身被依赖的传播行（直接/间接）+ 归并业务沿依赖链的行；
    # 关联证据按角色分组（排除依赖关系，依赖语义只来自已确认规则）；
    # 出边依赖 = 上游依赖条件；组成子结构仅作层级证据。
    dep_rels = set(_text(x) for x in (bundle.get(u"dependency_rels") or []))
    dep_rows = (list(bundle.get(u"dependency_candidates") or [])
                + list(bundle.get(u"direct_dependencies") or [])
                + list(bundle.get(u"indirect_dependencies") or []))
    assoc_rows = [r for r in (bundle.get(u"association_evidence") or [])
                  if _text(r.get("rel")) not in dep_rels]
    comp_rows = list(bundle.get(u"composition_evidence") or [])
    # 上游依赖条件：入边=依赖方（受影响候选），出边=被分析业务依赖的上游
    dep_out = [r for r in (bundle.get(u"association_evidence") or [])
               if _text(r.get("rel")) in dep_rels and r.get("dir") == u"出边"]
    rollup_bizs = bundle.get(u"rollup_bizs") or []
    query_state = _text(bundle.get(u"query_state")) or u"completed"
    limitations = [_text(x) for x in (bundle.get(u"limitations") or []) if _text(x)]
    # 节点属性摘要缓存：让报告能把每个条目的类型/关键属性列清楚
    _prop_cache = {}

    ent = store.entity_entry(ref.get("type"))
    role_defs = []
    for p in (ent.get("propagate") or []):
        rel = _text(p.get("rel"))
        if rel and rel in dep_rels:
            continue
        role_defs.append((rel, _text(p.get("role")) or rel,
                          u"出边" if _text(p.get("direction") or u"in").lower() == u"out"
                          else u"入边", _text(p.get("label")).strip()))

    lines = []
    if dep_rows:
        # by_src 按 ID 主键对齐（无 ID 时按名称回退），同名多主体不错位
        by_src = {}
        order = []
        for r in dep_rows:
            n = _text(r.get("src"))
            if not n:
                continue
            sid = _text(r.get("src_id"))
            key = sid if sid else n
            if key not in by_src:
                by_src[key] = {"name": n, "direct": False,
                               "rel": _text(r.get("rel")),
                               "nb_type": r.get("nb_type"), "anchors": []}
                order.append(key)
            e = by_src[key]
            if not e.get("rel"):
                e["rel"] = _text(r.get("rel"))
            if _text(r.get("hop")) == u"直接":
                e["direct"] = True
            a = _text(r.get("nb_name"))
            if a and a not in e["anchors"]:
                e["anchors"].append(a)

        # 承载系统补充查询：键与 by_src 一致（ID 精确匹配，名称回退）
        sys_map = {}
        try:
            sys_map = store.dependent_systems(
                [{"id": k if k != by_src[k]["name"] else "",
                  "name": by_src[k]["name"]} for k in order]) or {}
        except Exception as e:
            log.warning(u"dependent_systems 查询失败（不影响依赖候选）：%s", e)

        def dep_line(k, e):
            n = e["name"]
            sys_list = sys_map.get(k) or sys_map.get(n) or []
            sys_text = u"、".join(sys_list) if sys_list else u"未记录"
            anchors = u"、".join(e["anchors"]) if e["anchors"] else u"—"
            pb = _node_props_brief(_prop_cache, ref.get("type"), k, n)
            p_txt = u"｜" + pb if pb else u""
            return (u"- {}（{}{}｜承载系统：{}｜证据：{} -[{}]-> {}）"
                    .format(n, e["nb_type"], p_txt, sys_text, n,
                            _text(e.get("rel")) or u"依赖关系", anchors))

        direct = [(k, by_src[k]) for k in order if by_src[k]["direct"]]
        indirect = [(k, by_src[k]) for k in order if not by_src[k]["direct"]]
        if direct:
            lines.append(u"依赖候选——直接（1跳，{} 个）：".format(len(direct)))
            lines.extend(dep_line(k, e) for k, e in direct)
        if indirect:
            lines.append(u"依赖候选——间接（经依赖链传导，{} 个）：".format(len(indirect)))
            lines.extend(dep_line(k, e) for k, e in indirect)

    # 组成子结构：仅层级证据，列出子项清单（不参与影响传播）
    if comp_rows:
        children = []
        for r in comp_rows:
            s = _text(r.get("src"))
            if s and s not in children:
                children.append(s)
        if children:
            lines.append(u"组成子结构（已确认组成关系，仅层级证据，"
                         u"不参与影响传播，{} 个）：".format(len(children)))
            lines.extend(u"- {}".format(s) for s in children)

    # 实体自身传播入边按角色分组（如：来源子项/承载的业务/主管的业务）
    for rel, role, direction, other_label in role_defs:
        group = [r for r in assoc_rows if _text(r.get("rel")) == rel
                 and r.get("dir") == direction
                 and _text(r.get("role")) == role
                 and (not other_label or _text(r.get("nb_type")) == other_label)]
        if group:
            lines.append(u"{}（{}，{} 个）：".format(role, rel, len(group)))
            for r in group:
                pb = _node_props_brief(_prop_cache, r.get("nb_type"),
                                       r.get("nb_name"), r.get("nb_name"))
                lines.append(u"- {}（{}{}）".format(
                    r.get("nb_name"), r.get("nb_type"),
                    u"；" + pb if pb else u""))

    if lines:
        impact_text = (u"【内部判断材料】以下项目已按系统确认的业务影响方向整理。"
                       u"用于核对事实，不得把标题、关系名或路径术语直接写进面向业务人员的报告；"
                       u"应翻译为自然的业务描述：\n{}").format(u"\n".join(lines))
    elif query_state == u"partial":
        # 查询失败 ≠ 无依赖：部分证据段失败时禁止给出确定性"无影响"
        impact_text = (u"【内部判断材料】目前掌握的信息不完整，无法确认全部相关业务；"
                       u"不得据此判断没有影响，应提醒业务人员进一步核实。")
    elif dep_rels:
        impact_text = (u"【内部判断材料】目前没有明确列出的其他受影响业务；"
                       u"不得据此判断没有影响，应以谨慎措辞说明仍需结合实际业务核实。")
    else:
        impact_text = (u"【内部判断材料】现有信息不足以确认其他业务是否会受到影响；"
                       u"不得据此判断没有影响。应如实列出已确认的相关事项，并说明仍需进一步核实。")

    upstream_text = u"（无）"
    if dep_out:
        ulines = []
        for r in dep_out:
            pb = _node_props_brief(_prop_cache, r.get("nb_type"),
                                   r.get("nb_name"), r.get("nb_name"))
            ulines.append(u"- {}（{}{}）".format(
                r.get("nb_name"), r.get("nb_type"),
                u"；" + pb if pb else u""))
        upstream_text = u"\n".join(ulines)

    # §5.4：查询状态 + 局限必须逐条转述给用户，禁止隐瞒
    state_note = {
        u"completed": u"【信息完整性】本次整理的信息完整。",
        u"partial": (u"【信息完整性】目前掌握的信息不完整。报告须用业务语言说明判断范围有限，"
                     u"不得据此断言没有影响。"),
    }.get(query_state, u"")
    limit_text = u""
    if limitations:
        limit_text = (u"\n【信息完整性补充】本次掌握的信息可能未覆盖全部相关事项。"
                      u"请在报告中提示结论可能不完整，并建议核实相关业务范围；不要复述技术原因。\n")

    rollup_note = u""
    if rollup_bizs:
        qualifier = (u"后沿已确认依赖路径计算影响候选" if dep_rels
                     else u"；当前仅作为结构归属，不推导跨业务影响")
        rollup_note = (u"\n【归属业务】（分析对象经配置的结构路径归并到以下业务{}）：{}\n".format(
                           qualifier, u"、".join(rollup_bizs)))
    # 八-P3：rows 过大会把 analyze_ctx token 爆掉；按 (role, rel, dir, nb_type) 分桶
    # 每桶留 40 条，总共不超过 400 条——impact_text 已经把完整分组摘要给到了
    # LLM，原始行此处只用于复核，抽样后在前面声明"仅代表性样本"
    all_rows = assoc_rows + comp_rows + dep_rows
    # schema 参考按证据瘦身（见 _schema_ref_slim）：只给本轮涉及的关系/类型，
    # 杜绝模型把无关 schema 行（如组织树父类型）读成本轮事实
    schema = _schema_ref_slim(all_rows, ref.get("type"))
    sampled, sample_note = _sample_rows_by_role(all_rows, per_group=40,
                                                total_cap=400)
    rows_block = (u"【原始查询行{tag}】（受影响候选/上游即从中按传播关系提取，"
                  u"复核结果应与上面一致）：\n{body}").format(
                      tag=u"（代表性抽样）" if sample_note else u"",
                      body=json.dumps(sampled, ensure_ascii=False))
    if sample_note:
        rows_block = rows_block + u"\n" + sample_note
    anchor_disp = json.dumps(selected, ensure_ascii=False)
    anchor_brief = _node_props_brief(_prop_cache, ref.get("type"),
                                     ref.get("id_value") or ref.get("name"),
                                     ref.get("name"))
    if anchor_brief:
        anchor_disp += u"\n【分析对象属性】" + anchor_brief
    analyze_ctx = (u"【用户问题】{}\n【分析的业务】{}{}\n{}\n"
                   u"【上游依赖条件】（被分析业务依赖它们，属前置条件，不是受影响方）：\n{}\n"
                   u"{}{}\n"
                   u"{}{}"
                   u"【图结构与关系含义参考】\n{}".format(
                       query, anchor_disp,
                       rollup_note, impact_text, upstream_text,
                       rows_block, bundle.get(u"trunc_hint") or u"",
                       state_note, limit_text, schema))
    # LLM 排版 + 降级兜底：平台挂掉（超时/断流/空流）不报错结束——
    # 改发代码降级报告（_fallback_report，内容 100% 来自查询结果），
    # 分析链路在任何平台状态下都给业务人员真实核查清单
    report_got = []
    trunc_hint = bundle.get(u"trunc_hint") or u""
    try:
        for chunk in _stream(prompts.analyze_prompt(), analyze_ctx,
                             label=u"analyze"):
            report_got.append(chunk)
            yield chunk
    except Exception as e:
        metrics.inc("analyze.llm_fallback")
        runlog.event("error", stage=u"analyze_llm", err=u"{}".format(e))
        try:
            log.warning(u"分析 LLM 失败，改用代码降级报告：%s", e)
        except Exception:
            pass
        report_got = []
    if not u"".join(report_got).strip():
        for chunk in _fallback_report(selected, lines, upstream_text,
                                      rollup_bizs):
            yield chunk
    # A8：代码层保底 —— 回答模型即便没读 trunc_hint，也从流尾补一句免责声明
    if trunc_hint:
        yield (u"\n\n---\n（提示：本次整理的信息可能不完整，以上内容未必涵盖全部相关业务，"
               u"建议结合实际情况进一步核对。）")
    if query_state == u"partial":
        yield (u"\n\n---\n（提示：目前掌握的信息不完整，无法据此确认全部影响范围，"
               u"建议进一步核对相关业务事项。）")

    # §13.3：结构化执行结果（内部事件，供 _analyze / 工作台判断固化与重试）
    dep = None
    if query_state == u"partial" and bundle.get(u"error"):
        dep = u"error"
    elif query_state == u"partial":
        dep = u"truncated" if bundle.get(u"truncated") else u"partial"
    all_empty = not (assoc_rows or comp_rows or dep_rows)
    evidence_status = (u"error" if query_state == u"failed" else
                       u"truncated" if bundle.get(u"truncated") else
                       u"empty" if all_empty and query_state == u"completed"
                       else u"ok")
    yield ("result", {
        u"entity_resolved": True,
        u"analysis_status": u"success" if query_state == u"completed"
        else u"partial",
        u"evidence_status": evidence_status,
        u"selected_valid": True,
        u"can_retry": query_state != u"completed",
        u"query_state": query_state,
        u"selected": selected,
    })


def _stream(instruction, context, label=u"llm"):
    """流式调用 LLM 并逐块剥离 <think>（复用 gateway.py 的状态机）。
    O2：记录首字节延迟（第一次可见 chunk 的到达时间）；
    runlog 落盘输出全文（label 为阶段名，便于在日志面板区分是哪一步的回答）。"""
    import gateway
    import time as _time
    feed, flush = gateway.make_think_filter()
    t0 = _time.time()
    first_shown = False
    acc = []
    metrics.inc("llm.calls")
    for chunk in llm.llm_stream(instruction, context):
        shown = feed(chunk)
        if shown:
            acc.append(shown)
            if not first_shown:
                metrics.observe("llm.first_byte_ms",
                                (_time.time() - t0) * 1000.0)
                first_shown = True
            yield shown
    shown = flush()
    if shown:
        acc.append(shown)
        if not first_shown:
            metrics.observe("llm.first_byte_ms",
                            (_time.time() - t0) * 1000.0)
        yield shown
    # LLM 入参传参全记录：instruction（角色指令/模板，进平台系统提示词）、
    # context（拼好的证据上下文，进平台用户消息）——排查"大模型没收到图查询
    # 结果/不按模板回答"看这里的 instruction/context 字段
    runlog.event("llm", stage=label,
                 platform=config.get("LLM_PLATFORM"),
                 instruction=instruction, context=context,
                 ms=int((_time.time() - t0) * 1000), text=u"".join(acc))
