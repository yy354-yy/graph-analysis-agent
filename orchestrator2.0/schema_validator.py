# -*- coding: utf-8 -*-
"""V4.1 Schema 配置验证器（改造方案 §5.3）。

对候选/已发布的 schema_notes 配置做四类校验（全部只读查询）：
1. identifier_check       —— label 存在、主键覆盖=唯一=非空、名称字段可查询
2. relation_structure_check —— 依赖/组成三元组在图中真实存在且方向不颠倒
3. dependency_semantics_check —— 已证实依赖必须注明含义、受影响方向、确认来源
4. query_smoke_check      —— 生成的查询模板在目标图实际执行
                           （区分成功返回0 / 语法失败 / 权限失败 / 超时）

状态取值：passed / needs_review / failed。Python 2/3 双兼容。
"""
from __future__ import print_function

import gateway
from gateway import _u, safe_ident


def _classify_err(err):
    """把查询失败归类：timeout / permission / syntax / unknown（§5.3 第4条）。"""
    err = _u(err or u"")
    low = err.lower()
    if (u"超时" in err or u"timeout" in low or u"timed out" in low):
        return u"timeout"
    if (u"权限" in err or u"forbidden" in low or u"403" in low
            or u"401" in low or u"unauthorized" in low or u"auth" in low):
        return u"permission"
    if (u"语法" in err or u"syntax" in low or u"invalid" in low
            or u"unknown" in low or u"parse" in low):
        return u"syntax"
    return u"unknown"


def _one_graph_run(statement, parameters, errors, what, conn=None):
    """graph_run 包装：失败时记入 errors 并返回 (None, 分类)。"""
    rows, _, err = gateway.graph_run(statement, parameters=parameters,
                                     trusted=True, conn=conn,
                                     stage=u"schema校验·" + _u(what)[:20])
    if err:
        errors.append(u"{}：{}（{}）".format(what, err, _classify_err(err)))
        return None, _classify_err(err)
    return rows, None


def _check_format(notes, errors, warnings):
    """§5.3 第5条：结构字段格式正确，旧版读取逻辑不报错。"""
    ok = True
    if not isinstance(notes, dict):
        errors.append(u"配置根节点必须是对象")
        return False
    cs = notes.get("core_semantics")
    if not isinstance(cs, dict):
        errors.append(u"core_semantics 缺失或不是对象")
        ok = False
    else:
        for k in (u"anchor_label", u"name_prop"):
            v = _u(cs.get(k) or u"")
            if v and not safe_ident(v):
                errors.append(u"core_semantics.{} 含非法字符：{}".format(k, v))
                ok = False
    np = notes.get("name_properties")
    if np is not None and not isinstance(np, dict):
        errors.append(u"name_properties 必须是对象")
        ok = False
    bs = notes.get("business_search")
    if bs is not None:
        if not isinstance(bs, list):
            errors.append(u"business_search 必须是数组")
            ok = False
        else:
            for c in bs:
                if not isinstance(c, dict) or not safe_ident(c.get("label")):
                    errors.append(u"business_search 存在缺少合法 label 的条目")
                    ok = False
                    break
    er = notes.get("entity_roles")
    if er is not None and not isinstance(er, dict):
        errors.append(u"entity_roles 必须是对象")
        ok = False
    else:
        for lb, r in (er or {}).items():
            if not isinstance(r, dict):
                continue
            for p in (r.get("propagate") or []):
                if isinstance(p, dict) and not safe_ident(p.get("rel")):
                    errors.append(u"entity_roles.{}.propagate 存在非法 rel".format(lb))
                    ok = False
                    break
    for p in (notes.get("dependency_paths") or []):
        if isinstance(p, dict) and (p.get("impact_direction") not in
                                    (u"target_to_source", u"source_to_target")):
            errors.append(u"dependency_paths 缺少合法影响传播方向")
            ok = False
            break
    return ok


def validate_notes(notes, conn=None, sample_limit=200):
    """校验 schema_notes 配置。返回报告 dict（§5.3 建议结构）。
    conn：目标图连接上下文（None = 默认连接）。全部只读。"""
    errors = []
    warnings = []
    notes = notes if isinstance(notes, dict) else {}
    if conn is not None and notes.get("graph") and _u(notes.get("graph")) != _u(conn.graph_name):
        errors.append(u"候选配置图名与校验目标图不一致，禁止跨图库校验")

    # ---------- 格式检查（无图依赖） ----------
    format_ok = _check_format(notes, errors, warnings)

    # ---------- 1. 标识检查 ----------
    labels_checked = 0
    labels_empty = []          # 图中 0 节点的类型：属"需复核"而非校验失败
    pk_failed = 0              # 主键实查未通过的数量（真正的失败）
    ids_valid = 0
    ids_configured = 0
    if format_ok:
        seen_labels = set()
        for c in (notes.get("business_search") or []):
            if not isinstance(c, dict):
                continue
            lb = safe_ident(c.get("label"))
            if not lb or lb in seen_labels:
                continue
            seen_labels.add(lb)
            labels_checked += 1
            # 1a. label 存在
            rows, cerr = _one_graph_run(
                u"MATCH (n:`{0}`) RETURN count(n) AS c".format(lb), {},
                errors, u"类型 {} 存在性检查".format(lb), conn=conn)
            if cerr:
                continue
            total = (rows or [{}])[0].get("c") or 0
            if not total:
                labels_empty.append(lb)
                warnings.append(u"类型 {} 在图中无数据（0 节点），搜索配置暂不可用".format(lb))
                continue
            # 1b. 主键覆盖 = 唯一 = 非空
            idp = safe_ident(c.get("id_prop"))
            if idp:
                ids_configured += 1
                rows, cerr = _one_graph_run(
                    u"MATCH (n:`{0}`) RETURN count(n) AS total, "
                    u"count(n.`{1}`) AS nonempty, "
                    u"count(DISTINCT n.`{1}`) AS distinct_n".format(lb, idp),
                    {}, errors, u"类型 {} 主键 {} 验证".format(lb, idp), conn=conn)
                if cerr:
                    continue
                r0 = (rows or [{}])[0]
                ntotal = r0.get("total") or 0
                nonempty = r0.get("nonempty") or 0
                distinct = r0.get("distinct_n") or 0
                if ntotal > 0 and nonempty == ntotal and distinct == ntotal:
                    ids_valid += 1
                else:
                    pk_failed += 1
                    errors.append(
                        u"类型 {} 主键 {} 未通过覆盖验证"
                        u"（总数 {} 非空 {} 去重 {}）".format(
                            lb, idp, ntotal, nonempty, distinct))
            # 1c. 名称/搜索字段可查询（采样存在性）
            sprops = [safe_ident(p) for p in (c.get("search_props") or [])
                      if safe_ident(p)]
            if sprops:
                rows, cerr = _one_graph_run(
                    u"MATCH (n:`{0}`) WITH n LIMIT {1} UNWIND keys(n) AS k "
                    u"RETURN collect(DISTINCT k) AS ks".format(
                        lb, max(1, min(2000, int(sample_limit or 200)))),
                    {}, errors, u"类型 {} 属性采样".format(lb), conn=conn)
                if cerr:
                    continue
                ks = set((rows or [{}])[0].get("ks") or [])
                missing = [p for p in sprops if p not in ks]
                if missing:
                    warnings.append(
                        u"类型 {} 属性采样未见 {}（稀疏属性或配置错误，需人工核对）".format(
                            lb, u"、".join(missing)))

    if labels_checked == 0:
        identifier_status = u"failed"
        errors.append(u"business_search 无可校验条目")
        coverage = 0.0
    else:
        coverage = round(float(ids_valid) / ids_configured, 4) \
            if ids_configured else 1.0
        if pk_failed or (labels_checked == len(labels_empty)):
            # 主键实查未通过，或所有已配置类型在图中均无数据 → 失败
            identifier_status = u"failed"
        elif ids_configured == 0 or ids_valid < ids_configured or labels_empty:
            identifier_status = u"needs_review"
            if labels_empty:
                warnings.append(
                    u"{} 个类型在图中无数据（{}），对应搜索配置暂不可用；"
                    u"若图确实含这些类型请检查绑定图名，否则应从配置移除".format(
                        len(labels_empty), u"、".join(labels_empty)))
            else:
                warnings.append(u"存在未验证或不可用的主键配置，精确锚定将受限")
        else:
            identifier_status = u"passed"

    # ---------- 2. 关系结构检查（三元组存在 + 方向不颠倒） ----------
    # 实查对象 = dependency_paths / composition_paths 里声明的路径（影响传播
    # 的依据）。relation_catalog 是扫描观察到的全量目录，这里只统计不重复实查
    # （扫描时已验证过），避免大目录拖慢校验。
    catalog_entries = [c for c in (notes.get("relation_catalog") or [])
                       if isinstance(c, dict)]
    catalog_total = len(catalog_entries)
    catalog_observed = len([c for c in catalog_entries if c.get("observed")])
    triples = []
    seen_t = set()
    for p in (notes.get("dependency_paths") or []):
        if isinstance(p, dict):
            t = (safe_ident(p.get("source")), safe_ident(p.get("relation")),
                 safe_ident(p.get("target")), u"dep")
            if all(t[:3]) and t not in seen_t:
                seen_t.add(t)
                triples.append(t)
    for p in (notes.get("composition_paths") or []):
        if not isinstance(p, dict):
            continue
        start = safe_ident(p.get("start_label"))
        for st in (p.get("steps") or []):
            if isinstance(st, dict):
                t = (start, safe_ident(st.get("relation")),
                     safe_ident(st.get("target_label")), u"comp")
                if all(t[:3]) and t not in seen_t:
                    seen_t.add(t)
                    triples.append(t)

    rel_checked = 0
    rel_missing = 0
    if format_ok:
        for (src, rn, dst, kind) in triples:
            rows, cerr = _one_graph_run(
                u"MATCH (a:`{0}`)-[r:`{1}`]->(b:`{2}`) WITH r LIMIT 1 "
                u"RETURN count(r) AS c".format(src, rn, dst),
                {}, errors, u"关系 {}-{}->{} 检查".format(src, rn, dst), conn=conn)
            if cerr:
                continue
            rel_checked += 1
            fwd = ((rows or [{}])[0].get("c") or 0) > 0
            if fwd:
                continue
            # 正向 0 条：查反向，判断是否方向颠倒
            rows, _ = _one_graph_run(
                u"MATCH (a:`{2}`)-[r:`{1}`]->(b:`{0}`) WITH r LIMIT 1 "
                u"RETURN count(r) AS c".format(src, rn, dst),
                {}, [], u"关系反向检查", conn=conn)
            rev = bool(rows) and (((rows or [{}])[0].get("c") or 0) > 0)
            if rev:
                errors.append(
                    u"关系 {}-{}->{} 方向疑似颠倒（反向组合存在数据）".format(
                        src, rn, dst))
                rel_missing += 1
            elif kind == u"dep":
                # 降级为提示：允许先配好传播路径、后补数据的场景；
                # 沿无数据关系传播分析结果为空，无害。方向颠倒仍是错误。
                warnings.append(
                    u"依赖关系 {}-{}->{} 在图中当前无数据（声明路径，"
                    u"补数据后自动参与传播）".format(src, rn, dst))
                rel_missing += 1
            else:
                warnings.append(
                    u"组合 {}-{}->{} 当前无数据（声明路径，不算验证通过）".format(
                        src, rn, dst))
                rel_missing += 1

    relation_status = u"passed"
    if not format_ok or errors:
        relation_status = u"failed" if any(
            u"关系" in e or u"依赖关系" in e for e in errors) else relation_status
    if rel_missing:
        relation_status = (u"failed" if any(u"方向疑似颠倒" in e
                                            for e in errors)
                           else u"needs_review")

    # ---------- 3. 依赖语义检查 ----------
    dep_paths = [p for p in (notes.get("dependency_paths") or [])
                 if isinstance(p, dict)]
    verified_paths = 0
    for p in dep_paths:
        meaning = _u(p.get("meaning") or u"")
        src_confirm = _u(p.get("confirmed_source") or u"")
        direction = _u(p.get("direction") or u"")
        if (meaning and src_confirm and direction in (u"in", u"out")
                and p.get("impact_direction") in
                    (u"target_to_source", u"source_to_target")):
            verified_paths += 1
        else:
            errors.append(
                u"依赖 {}-{}->{} 缺少含义/确认来源/方向的完整标注".format(
                    p.get("source"), p.get("relation"), p.get("target")))
    if dep_paths and verified_paths == len(dep_paths):
        dep_status = u"passed"
    elif dep_paths:
        dep_status = u"failed"
    else:
        dep_status = u"needs_review"
        warnings.append(u"当前尚无已验证的依赖传播关系")

    # ---------- 4. 查询冒烟检查 ----------
    smoke_status = u"passed"
    if format_ok:
        cs = notes.get("core_semantics") or {}
        anchor = safe_ident(cs.get("anchor_label"))
        bs = notes.get("business_search") or []
        anchor_cfg = None
        for c in bs:
            if isinstance(c, dict) and safe_ident(c.get("label")) == anchor:
                anchor_cfg = c
                break
        if not anchor_cfg and bs and isinstance(bs[0], dict):
            anchor_cfg = bs[0]
        if anchor_cfg:
            namep = safe_ident(anchor_cfg.get("name_prop"))
            lb = safe_ident(anchor_cfg.get("label"))
            if namep and lb:
                rows, cerr = _one_graph_run(
                    u"MATCH (n:`{0}`) WHERE toLower(n.`{1}`) CONTAINS $kw "
                    u"WITH n LIMIT 1 RETURN count(n) AS c".format(lb, namep),
                    {u"kw": u"一"}, errors, u"锚定搜索冒烟（{}）".format(lb), conn=conn)
                if cerr:
                    smoke_status = u"failed"
                # 成功返回 0 条也算冒烟通过（区分"成功返回0"与"失败"）
        else:
            warnings.append(u"无锚定类型搜索配置，跳过冒烟查询")

    if errors and not format_ok:
        overall = u"failed"
    elif errors:
        overall = u"failed"
    elif any(s == u"needs_review" for s in
             (identifier_status, relation_status, dep_status, smoke_status)):
        overall = u"needs_review"
    else:
        overall = u"passed"

    return {
        u"format_check": {u"status": u"passed" if format_ok else u"failed"},
        u"identifier_check": {u"status": identifier_status,
                              u"coverage": coverage,
                              u"labels_checked": labels_checked,
                              u"labels_empty": labels_empty,
                              u"ids_valid": ids_valid,
                              u"ids_configured": ids_configured},
        u"relation_structure_check": {u"status": relation_status,
                                      u"count": rel_checked,
                                      u"missing": rel_missing,
                                      u"catalog_total": catalog_total,
                                      u"catalog_observed": catalog_observed},
        u"dependency_semantics_check": {u"status": dep_status,
                                        u"verified_paths": verified_paths,
                                        u"total_paths": len(dep_paths)},
        u"query_smoke_check": {u"status": smoke_status},
        u"errors": errors,
        u"warnings": warnings,
        u"overall": overall,
    }
