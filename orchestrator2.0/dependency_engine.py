# -*- coding: utf-8 -*-
"""V4.1 依赖判定与传播控制（改造方案 §5.4 / §7）。

核心纪律：
- 只有 dependency_paths（人工确认）或旧版 core_semantics 人工配置过的依赖关系
  才允许传播影响；使用/协同/包含 等【绝不】自动升级为依赖；
- 「查询失败 ≠ 无依赖」：失败/截断状态由 store.build_evidence_bundle 透传，
  上层禁止输出确定性"无影响"结论；
- 多跳默认上限 2 跳、硬上限 3 跳（config.DEPENDENCY_MAX_DEPTH 可调），
  visited 去重防环。

Python 2/3 双兼容。
"""
from __future__ import print_function

import config
from gateway import _u, safe_ident

# §5.4：多跳默认上限 2，可配置到 3；绝不放开口子
MAX_HOP_DEFAULT = 2
MAX_HOP_HARD = 3


def max_hops():
    """依赖传播深度上限：config.DEPENDENCY_MAX_DEPTH ∈ [1, 3]，默认 2。"""
    try:
        v = int(getattr(config, "DEPENDENCY_MAX_DEPTH", MAX_HOP_DEFAULT))
    except (TypeError, ValueError):
        v = MAX_HOP_DEFAULT
    return max(1, min(MAX_HOP_HARD, v))


def classify_relation(rel):
    """关系名启发式分类（透传 schema_generator）：composition /
    candidate_dependency / association。仅用于判定【哪些关系绝不能当依赖】，
    不用于激活依赖。"""
    import schema_generator
    return schema_generator.classify_relation_name(rel)


def verified_dependency_rules(notes=None):
    """读取已确认依赖规则。优先级：
    仅 notes.dependency_paths（V4.1 已确认方向的规则）；
    旧版 core_semantics 缺少方向证据，不再自动生效。

    返回规则列表 [{"source","relation","target","direction","meaning","depth"}]，
    direction=out 表示 source -[rel]-> target（source 依赖 target，
    target 变更影响 source）。无任何已确认规则时返回 []。"""
    import gateway
    if notes is None:
        notes = gateway.load_schema_notes() or {}
    rules = []
    for p in (notes.get("dependency_paths") or []):
        if not isinstance(p, dict):
            continue
        src = safe_ident(p.get("source"))
        rel = safe_ident(p.get("relation"))
        dst = safe_ident(p.get("target"))
        if not (src and rel and dst):
            continue
        if not (_u(p.get("meaning") or u"")
                and _u(p.get("confirmed_source") or u"")):
            # 缺含义/确认来源的条目不生效（validator 也会拦，这里双保险）
            continue
        try:
            depth = int(p.get("depth") or 1)
        except (TypeError, ValueError):
            depth = 1
        impact = _u(p.get("impact_direction") or u"")
        if impact not in (u"target_to_source", u"source_to_target"):
            continue
        if p.get("dependency_enabled") is False:
            continue
        rules.append({
            u"source": src, u"relation": rel, u"target": dst,
            u"direction": u"out", u"impact_direction": impact,
            u"meaning": _u(p.get("meaning") or u""),
            u"confirmed_source": _u(p.get("confirmed_source") or u""),
            u"depth": max(1, min(MAX_HOP_HARD, depth)),
        })
    if rules:
        return rules
    # 旧版 core_semantics 无法明确起终点和影响方向，禁止无声启用。
    return rules


def propagation_rules_for(rules, entity_label):
    """取适用于某实体类型的依赖规则：规则 target（被依赖方）为该类型，
    或旧版无端点规则。返回 [(rule, hop_cap)]，hop_cap = min(depth, max_hops())。"""
    label = _u(entity_label or u"").strip()
    out = []
    for r in rules:
        changed_label = (r.get(u"target") if r.get(u"impact_direction") == u"target_to_source"
                         else r.get(u"source"))
        if changed_label and changed_label != label:
            continue
        try:
            depth = int(r.get(u"depth") or 1)
        except (TypeError, ValueError):
            depth = 1
        out.append((r, max(1, min(depth, max_hops()))))
    return out


def rule_dependency_rels(rules):
    """已确认依赖的关系名集合（用于把 propagate 配置里的条目分成
    依赖证据 vs 普通关联证据）。"""
    return set(r[u"relation"] for r in rules if r.get(u"relation"))


def dedup_rows(rows, key_fields=(u"src_id", u"src", u"rel", u"dir",
                                 u"nb_type", u"nb_name", u"hop")):
    """visited 去重防环：按关键列组合去重，保留首个（带 role 优先级由
    store 层处理）。"""
    seen = set()
    out = []
    for r in rows or []:
        key = tuple(_u(r.get(f)) for f in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def limitation_text():
    """无已验证依赖规则时的固定 limitation 文案（pipeline 据此保守回答）。"""
    return (u"当前图没有已人工确认的依赖传播关系（dependency_paths 为空）；"
            u"关联证据不构成依赖结论，不能据此断定「无受影响业务」，"
            u"只能回答「现有图谱与配置不足以确认直接依赖」")
