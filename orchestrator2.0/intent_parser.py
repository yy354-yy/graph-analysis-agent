# -*- coding: utf-8 -*-
"""text3 意图协议解析器（SFM_INTENT_FORMAT=text3 时由 pipeline._intent 调用）。

协议：模型只输出三行 key=value 文本（意图/业务/变更），值域见 INTENT_MAP。
解析原则：按 key 名扫描而非按行序匹配——弱模型常在三行前后附加客套/解释
文字，因此只要求三个字段各出现恰好一次且值合法，其余行一律忽略。
缺失/重复/非法值返回 (None, 错误码)，由调用方统一兜底（绝不猜、绝不默认 chat）。
双兼容 py2.7 / py3.x，无三方依赖。"""
from __future__ import print_function

try:
    unicode     # py2
except NameError:
    unicode = str    # py3

# 语义值 → 内部值（调用方再映射到 action 契约：search/fact/targeted/chat/qa/writing）
INTENT_MAP = {
    u"影响分析": u"impact",
    u"事实查询": u"fact",
    u"普通聊天": u"chat",
    u"智能问答": u"qa",
    u"智能写作": u"writing",
}
# 空值等价写法（模型可能输出"无"以外的空话）
EMPTY_VALUES = frozenset([u"", u"无", u"未提及", u"未知", u"没有", u"空"])
_KEYS = (u"意图", u"业务", u"变更")
_NAME_MAX = 100
_CHANGE_MAX = 300
# 业务名里出现即整体作废的字符（值里带换行/等号说明模型没按协议输出）
_NAME_BAD_CHARS = (u"\n", u"\r", u"=")
# 名称两端容忍的引号与标点（剥完再校验，避免"「证单拟制」"被拒）
_NAME_STRIP = u"「」『』“”‘’\"' ，。,；; "
_CHANGE_STRIP = u" ，。,；; "
# 行首装饰前缀："1. 意图" "【意图】" "一、意图"——剥掉后再匹配 key
_KEY_STRIP = u"0123456789.、()（）[]【】-—:： "


def parse_intent_lines(raw):
    """解析 text3 输出。返回 (parsed, None) 或 (None, 错误码)。
    parsed = {"intent": impact/fact/chat/qa/writing,
              "business_name": unicode（可为空串）,
              "change_desc": unicode（可为空串）}
    错误码：empty_response / encoding_error / missing_field:X / duplicate_field:X
            / intent_invalid / field_too_long / name_malformed"""
    if raw is None:
        return None, u"empty_response"
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            return None, u"encoding_error"
    if not isinstance(raw, unicode):
        raw = u"{}".format(raw)
    found = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # 全角冒号容错（只换第一个，值里出现的冒号不动）
        line = line.replace(u"：", u"=", 1)
        if u"=" not in line:
            continue            # 无分隔符的行 = 噪音（客套/解释），忽略
        key, _, value = line.partition(u"=")
        key = key.strip().strip(_KEY_STRIP)
        if key not in _KEYS:
            continue            # 非协议字段的行，忽略
        if key in found:
            return None, u"duplicate_field:" + key
        found[key] = value.strip()
    missing = [k for k in _KEYS if k not in found]
    if missing:
        return None, u"missing_field:" + u"/".join(missing)
    sem = INTENT_MAP.get(found[u"意图"])
    if sem is None:
        return None, u"intent_invalid"
    name = found[u"业务"].strip(_NAME_STRIP)
    change = found[u"变更"].strip(_CHANGE_STRIP)
    if name in EMPTY_VALUES:
        name = u""
    if change in EMPTY_VALUES:
        change = u""
    if len(name) > _NAME_MAX or len(change) > _CHANGE_MAX:
        return None, u"field_too_long"
    if any(x in name for x in _NAME_BAD_CHARS):
        return None, u"name_malformed"
    return {"intent": sem,
            "business_name": name,
            "change_desc": change}, None
