# -*- coding: utf-8 -*-
"""V4.1 唯一 Schema 配置生成器（改造方案 §5.2）。

collect_graph_metadata() 产出扫描元数据后，本模块把元数据转成
schema_notes.<图名>.json 候选配置。原则：

- 结构已发现（getSchema+Cypher 查实）才写入；查询已验证（ID 覆盖=唯一=非空）
  才允许作为主键；依赖语义【绝不自动激活】，dependency_paths 初始为空，
  仅从 previous_notes 按稳定键 (源类型,关系,目标类型,方向) 继承人工确认规则，
  类型或方向变化的一律标记 reconfirm_required，不静默继承。

Python 2/3 双兼容：u"" 字符串、.format()、不用 f-string。
"""
from __future__ import print_function

import hashlib
import time
import datetime

import gateway
from gateway import (
    _u, safe_ident, guess_core_semantics,
    _pick_name_prop, _pick_search_props,
)

SCHEMA_VERSION = u"4.1"

# 组成/层级类关系名提示（§7.1：组成关系不视为跨业务依赖）
_COMPOSITION_HINTS = (u"包含", u"包括", u"组成", u"构成", u"下辖",
                      u"下属", u"隶属", u"属于")
# 疑似依赖类关系名提示（仅作为"候选"，不自动激活）
_DEPENDENCY_HINTS = (u"依赖", u"引用", u"调用", u"消费", u"取数")

_MEANING_SUGGEST = {
    u"composition": u"组成/层级关系：用于归属聚合，不构成跨业务依赖（语义待人工确认）",
    u"candidate_dependency": u"疑似依赖：名称含依赖/调用/消费类词，方向与业务含义未经确认，"
                             u"默认不传播影响（待人工确认）",
    u"association": u"关联关系：仅表明结构关联，不推定业务依赖（待人工确认）",
}


def classify_relation_name(rel):
    """按关系名启发式分类：composition / candidate_dependency / association。
    仅用于生成候选语义与提示排版；不能据此激活依赖传播。"""
    rel = _u(rel or u"")
    for h in _COMPOSITION_HINTS:
        if h in rel:
            return u"composition"
    for h in _DEPENDENCY_HINTS:
        if h in rel:
            return u"candidate_dependency"
    return u"association"


def verified_id_for(node):
    """取该节点类型已验证的主键属性：declared_id 优先，其次 _ID/ID/id~，
    再次任意覆盖=唯一=非空的候选；全部无效返回 None（不猜测）。"""
    iv = node.get("id_validation") or {}
    declared = _u(node.get("declared_id"))
    if declared and (iv.get(declared) or {}).get("valid"):
        return declared
    for cand in (u"_ID", u"ID", u"id~"):
        if (iv.get(cand) or {}).get("valid"):
            return cand
    for k in sorted(iv):
        if (iv.get(k) or {}).get("valid"):
            return k
    return None


def _props_of(node):
    """该类型真实可用的属性名集合：优先实测采样，无采样用声明属性。"""
    observed = node.get("observed_props") or []
    declared = [_u(p.get("name") if isinstance(p, dict) else p)
                for p in (node.get("properties") or []) if p]
    return sorted(set(safe_ident(p) for p in (observed + declared)
                      if safe_ident(p)))


def build_name_and_search_config(metadata):
    """§5.2：name_properties[id 需已验证] + business_search[只含真实存在字段]。
    返回 (name_properties, business_search)。"""
    name_properties = {}
    business_search = []
    for node in metadata.get("node_types") or []:
        lb = safe_ident(node.get("name"))
        if not lb:
            continue
        props = _props_of(node)
        if not props:
            continue
        vid = verified_id_for(node)
        namep = _pick_name_prop(props, _u(node.get("display_name") or u""),
                                vid or u"")
        name_properties[lb] = {u"name": namep, u"id": vid}
        search_props = [p for p in _pick_search_props(props, vid or u"", namep)
                        if p in props]
        business_search.append({
            u"label": lb,
            u"id_prop": vid,
            u"name_prop": namep,
            u"search_props": search_props,
            u"display_props": ([namep] + ([vid] if vid else [])),
            u"observed": node.get("observed_count") is not None,
        })
    return name_properties, business_search


def build_relation_catalog(metadata):
    """§5.2：每条 (源,关系,目标) 组合一条目录项。
    verified=true 仅指结构已查实（数据中实际扫到），不代表依赖语义已确认；
    dependency_enabled 一律 false，依赖只能人工确认后进入 dependency_paths。"""
    declared = {}
    for rt in metadata.get("relationship_types") or []:
        rn = safe_ident(rt.get("name"))
        if not rn:
            continue
        declared[rn] = rt
    observed = {}
    for t in metadata.get("observed_topology") or []:
        key = (safe_ident(t.get("source_type")), safe_ident(t.get("relation")),
               safe_ident(t.get("target_type")))
        if key[0] and key[1] and key[2]:
            observed[key] = t.get("count")

    def _rel_props(rn):
        """该关系的边属性名（扫描 Q07 实测采样），供人工确认语义时参考。"""
        rt = declared.get(rn) or {}
        return [_u(p) for p in (rt.get("properties") or []) if _u(p)]

    catalog = []
    for key in sorted(observed):
        src, rn, dst = key
        catalog.append({
            u"source": src, u"relation": rn, u"target": dst,
            u"observed": True,
            u"observed_count": observed[key],
            u"semantic_type": classify_relation_name(rn),
            u"verified": True,          # 结构已查实（≠依赖语义已确认）
            u"dependency_enabled": False,
            u"rel_props": _rel_props(rn),
        })
    for rn in sorted(declared):
        for pair in declared[rn].get("pairs") or []:
            pair = _u(pair)
            if u"->" not in pair:
                continue
            src, dst = [safe_ident(x.strip()) for x in pair.split(u"->", 1)]
            if not src or not dst or (src, rn, dst) in observed:
                continue
            catalog.append({
                u"source": src, u"relation": rn, u"target": dst,
                u"observed": False,     # 模型声明但当前无数据
                u"observed_count": None,
                u"semantic_type": classify_relation_name(rn),
                u"verified": False,
                u"dependency_enabled": False,
                u"rel_props": _rel_props(rn),
            })
    return catalog


def build_association_paths(catalog):
    """§5.2：每条有效组合生成来源端 out 与目标端 in 的关联查询描述，去重。
    组成类关系不进关联路径（需人工确认后进 composition_paths）。"""
    grouped = {}
    for c in catalog:
        if not c.get("verified") or c.get("semantic_type") == u"composition":
            continue
        src, rn, dst = c[u"source"], c[u"relation"], c[u"target"]
        grouped.setdefault(src, set()).add((rn, u"out", dst))
        grouped.setdefault(dst, set()).add((rn, u"in", src))
    paths = []
    for lb in sorted(grouped):
        steps = [{u"relation": rn, u"direction": d, u"target_label": t}
                 for (rn, d, t) in sorted(grouped[lb])]
        paths.append({u"start_label": lb, u"steps": steps})
    return paths


def generate_entity_roles(catalog, previous_roles=None):
    """V4 兼容 entity_roles：propagate 入/出边骨架 + 人工改过的 role 文案继承。
    compose 仅继承经确认的组成关系（键 (label, rel, child) 仍存在于目录），
    否则置 None 并记入需重新确认清单，绝不猜方向。返回 (roles, reconfirm)。"""
    previous_roles = previous_roles or {}
    reconfirm = []
    edges = {}
    for c in catalog:
        if not c.get("verified"):
            continue
        src, rn, dst = c[u"source"], c[u"relation"], c[u"target"]
        edges.setdefault(src, set()).add((rn, u"out", dst))
        edges.setdefault(dst, set()).add((rn, u"in", src))

    confirmed_compose = {}
    for lb, r in previous_roles.items():
        if not isinstance(r, dict):
            continue
        comp = r.get("compose")
        comps = comp if isinstance(comp, list) else (
            [comp] if isinstance(comp, dict) else [])
        for c in comps:
            rel = safe_ident(c.get("rel"))
            child = safe_ident(c.get("child"))
            if rel and child:
                confirmed_compose.setdefault(lb, []).append(
                    {u"rel": rel, u"child": child})

    catalog_keys = set()
    for c in catalog:
        catalog_keys.add((c[u"source"], c[u"relation"], c[u"target"]))

    roles = {}
    for lb in sorted(set(edges) | set(previous_roles)):
        prev = previous_roles.get(lb) or {}
        prev_map = {}
        for p in (prev.get("propagate") or []):
            if isinstance(p, dict):
                k = (safe_ident(p.get("rel")), _u(p.get("direction")) or u"out",
                     safe_ident(p.get("label")))
                if k[0] and k[2]:
                    prev_map[k] = _u(p.get("role") or u"")
        propagate = []
        seen = set()
        for (rn, d, other) in sorted(edges.get(lb) or set()):
            key = (rn, d, other)
            if key in seen:
                continue
            seen.add(key)
            if key in prev_map:
                role = prev_map[key]            # 人工文案优先
            elif d == u"out":
                role = u"{}{}".format(rn, other)
            else:
                role = (u"所属{}".format(other) if classify_relation_name(rn)
                        == u"composition" else
                        u"{}（来自{}）".format(rn, other))
            propagate.append({
                u"rel": rn, u"direction": d, u"label": other,
                u"depth": 1, u"role": role,
            })
        # 上一版 propagate 中 (rel, direction, label) 已不在新目录 → 需重新确认
        for k, role in sorted(prev_map.items()):
            rn, d, other = k
            exist = ((lb, rn, other) if d == u"out"
                     else (other, rn, lb))
            if exist not in catalog_keys:
                reconfirm.append({
                    u"kind": u"entity_role",
                    u"label": lb, u"rel": rn, u"direction": d,
                    u"other_label": other, u"reason": u"组合在新图中不存在",
                })
        comp_kept = []
        for c in confirmed_compose.get(lb) or []:
            if (lb, c[u"rel"], c[u"child"]) in catalog_keys:
                comp_kept.append(c)
            else:
                reconfirm.append({
                    u"kind": u"compose",
                    u"label": lb, u"rel": c[u"rel"],
                    u"child": c[u"child"], u"reason": u"组成组合在新图中不存在",
                })
        roles[lb] = {
            u"propagate": propagate,
            u"compose": comp_kept or None,
        }
    return roles, reconfirm


def propose_semantics(metadata, catalog):
    """§5.2：核心语义仅生成候选。锚定/承载/显名用既有启发式；
    依赖传播一律不激活（dependency_enabled=False），生效依赖只能来自
    merge_confirmed_rules 继承的人工确认规则。"""
    schema_like = {
        u"node_types": [
            {u"name": n.get("name"),
             u"unique_key": verified_id_for(n),
             u"display_name": n.get("display_name") or u"",
             u"properties": [{u"name": p} for p in _props_of(n)],
             u"count": n.get("observed_count")}
            for n in (metadata.get("node_types") or [])
        ],
        u"relationship_types": [
            {u"name": rt.get("name"),
             u"pairs": rt.get("observed_pairs") or rt.get("pairs") or [],
             u"count": rt.get("observed_count")}
            for rt in (metadata.get("relationship_types") or [])
        ],
    }
    sem, warnings = guess_core_semantics(schema_like)
    labels = set(schema_like[u"node_types"][i][u"name"]
                 for i in range(len(schema_like[u"node_types"])))
    relnames = set(rt[u"name"] for rt in schema_like[u"relationship_types"])
    # 过滤到真实存在的类型/关系；候选 label 同样过滤
    if sem.get(u"anchor_label") not in labels:
        sem[u"anchor_label"] = u""
    sem[u"candidate_labels"] = [x for x in (sem.get(u"candidate_labels") or [])
                                if x in labels]
    if not sem.get(u"candidate_labels") and sem.get(u"anchor_label"):
        sem[u"candidate_labels"] = [sem[u"anchor_label"]]
    if sem.get(u"hosting_rel") not in relnames:
        sem[u"hosting_rel"] = u""
    if sem.get(u"system_label") not in labels:
        sem[u"system_label"] = u""
    sem[u"rollup_exclude_rels"] = [x for x in (sem.get(u"rollup_exclude_rels") or [])
                                   if x in relnames]
    # 保守核心：依赖语义不自动激活
    sem[u"dependency_enabled"] = False
    sem[u"dependency_rel"] = u""
    return sem, warnings


def compute_fingerprint(metadata):
    """节点/边/属性规范化结构指纹（sha256 前 16 位），用于缓存与差异对比。"""
    lines = []
    for node in metadata.get("node_types") or []:
        props = u",".join(sorted(_props_of(node)))
        lines.append(u"N|{}|{}|{}".format(
            _u(node.get("name")), _u(node.get("declared_id")), props))
    for t in metadata.get("observed_topology") or []:
        lines.append(u"R|{}|{}|{}".format(t.get("source_type"),
                                          t.get("relation"),
                                          t.get("target_type")))
    for rt in metadata.get("relationship_types") or []:
        rn = _u(rt.get("name"))
        for pair in (rt.get("pairs") or []):
            lines.append(u"D|{}|{}".format(rn, _u(pair)))
    raw = u"\n".join(sorted(set(lines)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _triple_keys(catalog):
    return set((c[u"source"], c[u"relation"], c[u"target"]) for c in catalog)


def merge_confirmed_rules(notes, previous_notes, catalog):
    """§5.7：人工确认语义按稳定键 (源,关系,目标,方向) 合并；类型或方向变化的
    不静默继承，写入 generation_info.reconfirm_required。
    返回 (dependency_paths, composition_paths, reconfirm, warnings)。"""
    reconfirm = []
    warnings = []
    if not isinstance(previous_notes, dict):
        return [], [], reconfirm, warnings
    keys = _triple_keys(catalog)

    # ---- V4.1 dependency_paths 继承 ----
    dep_paths = []
    for p in (previous_notes.get("dependency_paths") or []):
        if not isinstance(p, dict):
            continue
        src = safe_ident(p.get("source"))
        rn = safe_ident(p.get("relation"))
        dst = safe_ident(p.get("target"))
        if not (src and rn and dst):
            continue
        if (src, rn, dst) in keys:
            if p.get("impact_direction") not in (u"target_to_source", u"source_to_target"):
                reconfirm.append({u"kind": u"dependency", u"source": src,
                                  u"relation": rn, u"target": dst,
                                  u"reason": u"旧依赖规则未明确影响传播方向，需重新确认"})
                continue
            e = dict(p)
            e[u"verified"] = True
            e.setdefault(u"direction", u"out")
            e.setdefault(u"depth", 1)
            e.setdefault(u"meaning", u"")
            e.setdefault(u"confirmed_source", u"人工确认（历史配置继承）")
            dep_paths.append(e)
        else:
            reconfirm.append({
                u"kind": u"dependency", u"source": src, u"relation": rn,
                u"target": dst, u"reason": u"组合在新图中不存在",
            })

    # 旧版只提供关系名称，不能证明方向与端点；不自动继承为已确认依赖。
    prev_sem = previous_notes.get("core_semantics") or {}
    if prev_sem.get("dependency_enabled") and prev_sem.get("dependency_rel"):
        reconfirm.append({u"kind": u"dependency_legacy",
                          u"relation": prev_sem.get("dependency_rel"),
                          u"reason": u"旧配置缺少端点及影响方向，必须重新人工确认"})

    # ---- 组成路径继承（V4.1 composition_paths / entity_roles.compose） ----
    comp_paths = []
    for p in (previous_notes.get("composition_paths") or []):
        if not isinstance(p, dict):
            continue
        start = safe_ident(p.get("start_label"))
        steps = []
        for st in (p.get("steps") or []):
            if not isinstance(st, dict):
                continue
            rn = safe_ident(st.get("relation"))
            tgt = safe_ident(st.get("target_label"))
            if start and rn and tgt and (start, rn, tgt) in keys:
                steps.append({u"relation": rn,
                              u"direction": _u(st.get("direction")) or u"out",
                              u"target_label": tgt})
        if steps:
            comp_paths.append({u"start_label": start, u"steps": steps})
    for lb, r in (previous_notes.get("entity_roles") or {}).items():
        if not isinstance(r, dict):
            continue
        comp = r.get("compose")
        comps = comp if isinstance(comp, list) else (
            [comp] if isinstance(comp, dict) else [])
        for c in comps:
            rel = safe_ident(c.get("rel"))
            child = safe_ident(c.get("child"))
            if rel and child and (lb, rel, child) in keys:
                path = {u"start_label": lb, u"steps": [
                    {u"relation": rel, u"direction": u"out",
                     u"target_label": child}]}
                if path not in comp_paths:
                    comp_paths.append(path)

    # ---- 人工词表类字段：关系仍存在才继承 ----
    relnames = set(c[u"relation"] for c in catalog)
    prev_meanings = previous_notes.get("relationship_meanings") or {}
    meanings = {}
    for rn, m in prev_meanings.items():
        if safe_ident(rn) in relnames:
            meanings[safe_ident(rn)] = m
        else:
            reconfirm.append({
                u"kind": u"relationship_meaning", u"relation": rn,
                u"reason": u"关系在新图中不存在",
            })

    warnings.extend([u"继承人工确认依赖规则 {} 条".format(len(dep_paths))
                     if dep_paths else u"当前尚无已验证的依赖传播关系"])
    return dep_paths, comp_paths, reconfirm, warnings


def generate_schema_notes(metadata, previous_notes=None):
    """扫描元数据 → schema_notes 候选配置（V4 字段全部保留 + V4.1 新字段）。
    previous_notes：当前已发布配置（人工确认规则按稳定键继承）。
    返回 (notes, warnings)。"""
    warnings = list(metadata.get("scan_warnings") or [])
    name_properties, business_search = build_name_and_search_config(metadata)
    catalog = build_relation_catalog(metadata)
    # 已确认的业务语义只能在相同三元组和同一张图内继承；不能按关系名称扩散。
    previous_catalog = (previous_notes or {}).get("relation_catalog") or []
    previous_by_key = {(c.get("source"), c.get("relation"), c.get("target")): c
                       for c in previous_catalog if isinstance(c, dict)}
    for item in catalog:
        old = previous_by_key.get((item["source"], item["relation"], item["target"]))
        if old and old.get("semantics_confirmed"):
            item["semantics_confirmed"] = True
            item["meaning"] = old.get("meaning") or u""
            item["semantic_type"] = old.get("semantic_type") or item["semantic_type"]
            # 依赖传播标识只能通过已确认规则映射，不能直接继承布尔值。
    association_paths = build_association_paths(catalog)
    roles, reconfirm_roles = generate_entity_roles(
        catalog, (previous_notes or {}).get("entity_roles"))
    sem, sem_warnings = propose_semantics(metadata, catalog)
    warnings.extend(sem_warnings)

    dep_paths, comp_paths, reconfirm, merge_warnings = merge_confirmed_rules(
        {u"catalog": True}, previous_notes, catalog)
    warnings.extend(merge_warnings)

    # V4.1 只通过 dependency_paths 传播；禁止把一个旧版 relation 名
    # 写回 core_semantics 并绕过已经确认的端点和影响方向。
    sem[u"dependency_enabled"] = False
    sem[u"dependency_rel"] = u""
    for c in catalog:
        if any((c[u"source"], c[u"relation"], c[u"target"]) ==
               (p[u"source"], p[u"relation"], p[u"target"]) for p in dep_paths):
            c[u"dependency_enabled"] = True
            c[u"semantic_type"] = u"dependency"
            c[u"semantics_confirmed"] = True

    # relationship_meanings：人工已确认的优先，其余按分类给建议（verified=false 语义）
    meanings = {}
    human_meanings = (previous_notes or {}).get("relationship_meanings") or {}
    seen_rels = set()
    for c in catalog:
        rn = c[u"relation"]
        if rn in seen_rels:
            continue
        seen_rels.add(rn)
        if rn in human_meanings:
            meanings[rn] = human_meanings[rn]
        else:
            meanings[rn] = _MEANING_SUGGEST.get(
                c[u"semantic_type"], _MEANING_SUGGEST[u"association"])

    prev = previous_notes if isinstance(previous_notes, dict) else {}
    anchor = sem.get(u"anchor_label") or u""
    display_notes = _u(prev.get("display_notes") or u"")
    if not display_notes:
        display_notes = (u"当前仅支持已确认的结构关系，不代表业务依赖"
                         if not dep_paths else
                         u"依赖传播仅限已确认的「{}」关系".format(
                             sem.get(u"dependency_rel") or u""))

    notes = {
        u"_说明": u"本文件由 V4.1 扫描生成器产出（候选版），依赖语义需人工确认后才生效；"
                  u"人工可编辑 aliases/query_templates/report_headers/influence_rules "
                  u"等词表字段，再次扫描时按稳定键继承",
        u"schema_version": SCHEMA_VERSION,
        u"graph": _u(metadata.get("graph") or u""),
        u"schema_fingerprint": compute_fingerprint(metadata),
        u"core_semantics": sem,
        u"aliases": dict(prev.get("aliases") or {}),
        u"name_properties": name_properties,
        u"business_search": business_search,
        u"entity_roles": roles,
        u"relationship_meanings": meanings,
        u"influence_rules": dict(prev.get("influence_rules") or {}),
        u"relation_catalog": catalog,
        u"association_paths": association_paths,
        u"composition_paths": comp_paths,
        u"dependency_paths": dep_paths,
        u"query_templates": list(prev.get("query_templates") or []),
        u"report_headers": dict(prev.get("report_headers") or {}),
        u"query_examples": list(prev.get("query_examples") or []),
        u"display_notes": display_notes,
        u"generation_info": {
            u"source": _u(metadata.get("schema_source") or u""),
            u"status": u"candidate",
            u"generated_at": datetime.datetime.now().strftime(
                u"%Y-%m-%d %H:%M:%S"),
            u"graph": _u(metadata.get("graph") or u""),
            u"scan_warnings": warnings,
            u"reconfirm_required": reconfirm + reconfirm_roles,
        },
        u"validation_report": {
            u"status": u"pending",
            u"dependency_rules_verified": bool(dep_paths),
        },
    }
    return notes, warnings
