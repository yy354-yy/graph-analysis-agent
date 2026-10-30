# -*- coding: utf-8 -*-
"""存储与图工具层：复用 gateway.py 的 SQLite（history.db）与 Galaxybase 客户端。

通过 import 使用 gateway 的 db / graph_run / build_schema / make_think_filter
等函数；图数据库连接配置在 gateway.py 配置区（GALAXY_*），可被 settings.py
运行时覆盖。

本文件结构：
    本地会话/状态     get_context / save_state / save_message / history_text
    图工具            business_search（候选搜索，排序收敛）
                      fuzzy_search（2 字滑窗兜底）
                      graph_query（只读校验 + 类型校验 + 强制 LIMIT）
    类型参数表        type_registry / entity_entry
                      结构自动提取（getSchema：标签/主键/入边关系）
                      + schema_notes.json entity_roles 语义标注（传播/组成）
    证据查询          evidence_rows（影响分析：1跳邻接+传播入边变长+子结构
                      +非业务实体归并到业务再跑依赖传播）
                      fact_rows（事实直答：批量 1 跳邻接）
                      dependent_systems（受影响业务的承载系统批量补查）
    已选业务提取      pick_selected（ID > 序号 > 全名 > 关键词唯一 > 旧selected，
                      返回 (selected, source) 供上层识别"新话题退出"）
"""
from __future__ import print_function

import json
import re
import time

import config
import gateway

CN_NUM = {u"一": 1, u"二": 2, u"两": 2, u"三": 3, u"四": 4,
          u"五": 5, u"六": 6, u"七": 7, u"八": 8, u"九": 9, u"十": 10}


# ---------------- 本地会话/状态 ----------------
def get_context(conv_id):
    """读取 最近历史 + 业务状态（对应网关的 /sessions/{id}/context）"""
    with gateway.db() as conn:
        rows = conn.execute(
            "SELECT role, content FROM messages WHERE conv_id = ? "
            "ORDER BY id DESC LIMIT ?", (conv_id, config.HISTORY_MAX)).fetchall()
        srow = conn.execute(
            "SELECT state FROM session_state WHERE conv_id = ?", (conv_id,)
        ).fetchone()
    history = [{"role": r["role"], "content": r["content"]}
               for r in reversed(rows)]
    state = {}
    if srow and srow["state"]:
        try:
            state = json.loads(srow["state"]) or {}
        except ValueError:
            state = {}
    if not isinstance(state, dict):
        state = {}
    return {"history": history, "state": state}


def save_state(conv_id, state):
    """保存/清空业务状态（state=None 清空）"""
    with gateway.db() as conn:
        if state is None:
            conn.execute("DELETE FROM session_state WHERE conv_id = ?", (conv_id,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO session_state(conv_id, state, updated_at) "
                "VALUES(?, ?, ?)",
                (conv_id, json.dumps(state, ensure_ascii=False), time.time()))


def save_message(conv_id, role, content, graph=None):
    """graph: 可选，回答附带的图谱卡片 JSON 字符串（{"entity":{...}}）"""
    with gateway.db() as conn:
        conn.execute(
            "INSERT INTO messages(conv_id, role, content, graph, created_at) "
            "VALUES(?, ?, ?, ?, ?)",
            (conv_id, role, content, graph, time.time()))


def history_text(ctx):
    return u"\n".join(u"{}: {}".format(m.get("role"), m.get("content"))
                      for m in (ctx.get("history") or [])[-10:])


# ---------------- 图工具（进程内直调，不走 HTTP） ----------------
def _search_cfg(labels=None):
    """候选搜索配置：schema_notes.business_search 优先，缺省按名称类属性
    从 schema 自动推导；labels 限定类型。返回有效配置列表。"""
    notes = gateway.load_schema_notes()
    cfg = notes.get("business_search")
    if not cfg:
        schema, err = gateway.build_schema()
        if err:
            return []
        cfg = []
        for nt in schema["node_types"]:
            props = [p for p in nt["properties"]
                     if isinstance(p, dict) and "name" in (p.get("name") or "").lower()]
            if props:
                cfg.append({"label": nt["name"], "search_props":
                            [p["name"] for p in props],
                            "name_prop": props[0]["name"]})
    if labels:
        cfg = [c for c in cfg if c.get("label") in labels]
    return [c for c in (cfg or [])
            if isinstance(c, dict) and c.get("label") and c.get("search_props")]


def keyword_counts(keyword, labels=None):
    """统计类问题的确定性兜底：按类型 count 名称/别名（search_props）包含
    关键词的实体数，不依赖 LLM 生成查询。返回 [(label, count)]（只含 count>0）。"""
    kw = _text(keyword).strip().lower()
    if not kw:
        return []
    out = []
    for c in _search_cfg(labels):
        conds = u" OR ".join(u"toLower(n.`{0}`) CONTAINS $kw".format(p)
                             for p in (c.get("search_props") or []) if p)
        r = graph_query(
            u"MATCH (n:`{l}`) WHERE {w} RETURN count(n) AS c".format(
                l=c["label"], w=conds), {"kw": kw})
        if r.get("success"):
            cnt = (r.get("rows") or [{}])[0].get("c") or 0
            if cnt:
                out.append((c["label"], cnt))
    return out


def business_search(keyword, filters=None, labels=None, limit=20):
    """按关键词搜索候选业务。
    labels: 只搜这些节点类型（None 用全部 business_search 配置）
    limit:  最多返回条数，按 精确名称 > 名称包含 > 别名 > 描述 排序后截取
    """
    kw = (keyword or u"").strip().lower()
    if not kw:
        return []
    filters = filters or {}
    search_cfg = _search_cfg(labels)
    if not search_cfg:
        return []

    results = []
    for ti, cfg in enumerate(search_cfg):
        label = cfg.get("label")
        sprops = [p for p in (cfg.get("search_props") or []) if p]
        if not label or not sprops:
            continue
        name_prop = cfg.get("name_prop") or sprops[0]
        id_prop = cfg.get("id_prop") or name_prop
        display_props = [p for p in (cfg.get("display_props") or [])
                         if p and p not in sprops]
        ret_props = []
        for p in [id_prop, name_prop] + display_props:
            if p and p not in ret_props:
                ret_props.append(p)

        params = {"kw": kw}
        conds = [u"(toLower(n.`{0}`) CONTAINS $kw)".format(p) for p in sprops]
        for k, v in filters.items():
            if k and v and (k in display_props or k in sprops):
                fk = "f_" + re.sub(r"\W", "_", k)
                conds.append(u"(toLower(n.`{0}`) CONTAINS ${1})".format(k, fk))
                params[fk] = u"{}".format(v).lower()
        matched = u"CASE " + u" ".join(
            u"WHEN toLower(n.`{0}`) CONTAINS $kw THEN '{0}'".format(p)
            for p in sprops) + u" ELSE '' END"
        ret_str = u", ".join(u"n.`{0}` AS `{0}`".format(p) for p in ret_props)
        stmt = (u"MATCH (n:`{l}`) WHERE {w} "
                u"RETURN {m} AS matched_on, {r} LIMIT 50").format(
            l=label, w=u" OR ".join(conds), m=matched, r=ret_str)
        rows, _, err = gateway.graph_run(stmt, params)
        if err:
            continue
        for r in rows or []:
            item = {"business_type": label, "id": r.get(id_prop),
                    "name": r.get(name_prop),
                    "matched_on": r.get("matched_on") or None,
                    "_type_pri": ti}
            for p in display_props:
                item[p] = r.get(p)
            results.append(item)

    def rank_key(c):
        name = _text(c.get("name")).lower()
        mo = _text(c.get("matched_on"))
        if name == kw:
            tier = 0          # 名称精确
        elif kw in name:
            tier = 1          # 名称包含
        elif mo == u"别名":
            tier = 2
        else:
            tier = 3          # 描述等其他
        return (tier, c.get("_type_pri", 99), len(name))

    results.sort(key=rank_key)
    for c in results:
        c.pop("_type_pri", None)
    return results[:limit]


def fuzzy_search(keyword, limit=5, labels=None):
    """精确搜索无结果时的分层模糊兜底；结果只作为候选，不直接当结论。"""
    kw = (keyword or u"").strip()
    if len(kw) < 3:
        return []
    grams = []
    for i in range(len(kw) - 1):
        g = kw[i:i + 2]
        if g not in grams:
            grams.append(g)
    merged = {}
    for g in grams:
        for c in business_search(g, labels=labels,
                                 limit=limit * 3):
            key = (_text(c.get("business_type")), _text(c.get("id")))
            if key not in merged:
                c = dict(c)
                c["_grams"] = 1
                merged[key] = c
            else:
                merged[key]["_grams"] += 1
    # 用户口语/错别字可能没有连续二字片段（如“卫证规则”）。仅在二字
    # 窗口完全无命中时，退化为单字交集检索；至少命中两个不同字符才保留，
    # 防止“的/业/务”等高频字把全图候选刷满。
    if not merged and len(set(kw)) >= 2:
        chars = []
        for ch in kw:
            if ch not in chars:
                chars.append(ch)
        for ch in chars:
            for c in business_search(ch, labels=labels, limit=limit * 2):
                key = (_text(c.get("business_type")), _text(c.get("id")))
                if key not in merged:
                    c = dict(c)
                    c["_grams"] = 1
                    c["_char_hits"] = set([ch])
                    merged[key] = c
                else:
                    merged[key].setdefault("_char_hits", set()).add(ch)
                    merged[key]["_grams"] = len(merged[key]["_char_hits"])
        merged = {k: v for k, v in merged.items() if v.get("_grams", 0) >= 2}
    ranked = sorted(merged.values(), key=lambda c: (
        0 if kw.lower() in _text(c.get("name")).lower() else 1,
        -c.get("_grams", 0), len(_text(c.get("name"))),
        _text(c.get("business_type"))))
    for c in ranked:
        c.pop("_grams", None)
        c.pop("_char_hits", None)
    return ranked[:limit]


def graph_query(cypher, parameters=None, validate=True):
    """只读校验并执行 Cypher。
    validate=True 时先校验标签/关系类型是否真实存在（防止 LLM 编造类型白跑一趟），
    校验失败返回带可用类型清单的错误，供上层重试生成。"""
    cypher = (cypher or u"").strip().rstrip(u";").strip()
    if not cypher:
        return {"success": False, "error": "cypher 为空"}
    if gateway.CYPHER_WRITE_RE.search(cypher):
        return {"success": False, "error": u"查询被拒绝：只允许只读查询"}

    if validate:
        schema, serr = gateway.build_schema()
        if not serr:
            known = set([n.get("name") for n in schema.get("node_types") or []] +
                        [r.get("name") for r in schema.get("relationship_types") or []])
            used = set(re.findall(u":`([^`]+)`", cypher) +
                       re.findall(u"\[:`([^`]+)`*?\d*\.\.\d*\]", cypher))
            unknown = [t for t in used if t not in known]
            if unknown:
                return {"success": False,
                        "error": u"Cypher 使用了图中不存在的类型: {}；可用节点类型: {}；"
                                 u"可用关系类型: {}".format(
                                     u"、".join(unknown),
                                     u"、".join(sorted(n.get("name") for n
                                                       in schema["node_types"])),
                                     u"、".join(sorted(r.get("name") for r
                                                       in schema["relationship_types"])))}

    if not gateway.CYPHER_LIMIT_RE.search(cypher):
        cypher = cypher + u" LIMIT {}".format(config.QUERY_LIMIT)
    rows, columns, err = gateway.graph_run(cypher, parameters or {})
    if err:
        return {"success": False, "error": err}
    return {"success": True, "columns": columns,
            "count": len(rows or []), "rows": rows or []}


def data_overview():
    """数据概况（chat 分支的通用问答材料）：图名 + 各点类型数量 + 关系类型清单。
    schema_source == manual 说明 getSchema 和图查询都失败（图库不可达），
    显式提示，避免用户把兜底数据当实时数据。"""
    data, err = gateway.build_schema()
    if err:
        return u"（数据概况暂不可用：{}）".format(err)
    warn = (u"（注意：图数据库当前不可达，以下为人工配置兜底信息，数量非实时数据）\n"
            if data.get("schema_source") == u"manual" else u"")
    nodes = []
    total = 0
    for n in data.get("node_types") or []:
        cnt = n.get("count")
        nodes.append(u"- {}：{} 条".format(n.get("name"),
                    cnt if cnt is not None else u"数量未知"))
        if isinstance(cnt, int):
            total += cnt
    rels = u"、".join(sorted(_text(r.get("name")) for r in
                            data.get("relationship_types") or []))
    return (warn + u"图名称：{}\n节点类型（共{}种，合计约 {} 个节点）：\n{}\n"
            u"关系类型：{}".format(
                (data.get("database") or {}).get("graph") or u"未命名",
                len(nodes), total, u"\n".join(nodes), rels))


def node_type_overview():
    """按官方 Schema 确定性回答节点类型问题。

    Schema 类型数和 MATCH (n) 能观察到的非空标签数是两个口径；必须同时说明，
    避免把尚无实例的合法类型从模型定义中漏掉。
    """
    data, err = gateway.build_schema()
    if err:
        return u"读取图 Schema 失败：{}".format(err)
    types = data.get("node_types") or []
    populated = [n for n in types if isinstance(n.get("count"), int)
                 and n.get("count") > 0]
    empty = [n for n in types if n.get("count") == 0]
    unknown = [n for n in types if n.get("count") is None]
    total_nodes = sum(n.get("count") for n in populated)

    def names(items, with_count=False):
        if with_count:
            return u"、".join(u"{}（{}个）".format(n.get("name"), n.get("count"))
                             for n in items)
        return u"、".join(_text(n.get("name")) for n in items)

    populated_line = (u"- 有数据（{}种）：{}".format(
        len(populated), names(populated, with_count=True))
        if populated else u"- 有数据（0种）：无")
    empty_line = (u"- 暂无实例（{}种）：{}".format(len(empty), names(empty))
                  if empty else u"- 暂无实例（0种）：无")
    lines = [
        u"Galaxybase Schema 共定义 **{} 种点类型**；其中 **{} 种当前有节点数据**，"
        u"合计 **{} 个节点**。".format(len(types), len(populated), total_nodes),
        populated_line,
        empty_line,
    ]
    if unknown:
        lines.append(u"- 数量未获取（{}种）：{}".format(len(unknown), names(unknown)))
    lines.append(u"之前回答的“6种”是只统计了当前非空标签；按 Schema 口径应为 **{}种**。"
                 .format(len(types)))
    return u"\n".join(lines) + u"\n"


# ---------------- 实体类型参数表（结构自动 + 语义标注） ----------------
_reg_cache = {"data": None, "ts": 0}


def core_sem():
    """当前图核心语义（锚定类型/依赖关系/承载关系/名称属性等，见 gateway）"""
    return gateway.core_semantics()


def type_registry():
    """实体类型参数表：
    - 标签/主键/每类入边关系：官方 getSchema 自动提取（含 fromAndToTypes 推导）
    - 传播关系（哪些入边代表影响/关联及其深度）与组成关系（子结构）：
      schema_notes.json 的 entity_roles 人工标注；未标注的类型全部按 1 跳关联处理
    随 build_schema 共享缓存。"""
    import time as _time
    now = _time.time()
    if _reg_cache["data"] is not None and now - _reg_cache["ts"] < 300:
        return _reg_cache["data"]
    data, err = gateway.build_schema()
    reg = {}
    if not err:
        notes = gateway.load_schema_notes()
        roles = notes.get("entity_roles") or {}
        name_props = notes.get("name_properties") or {}
        in_rels = {}
        for rt in data.get("relationship_types") or []:
            name = rt.get("name")
            for pair in rt.get("pairs") or []:
                pair = _text(pair)
                if u"->" in pair:
                    dst = pair.split(u"->", 1)[1].strip()
                    in_rels.setdefault(dst, []).append(name)
        for nt in data.get("node_types") or []:
            label = nt.get("name")
            r = roles.get(label) or {}
            np_cfg = name_props.get(label) or {}
            reg[label] = {
                "label": label,
                "pk": nt.get("unique_key") or u"名称",
                "name_prop": (np_cfg.get("name") if isinstance(np_cfg, dict) else None)
                             or u"名称",
                "propagate": r.get("propagate") or [],
                "compose": r.get("compose"),
                "rollup": r.get("rollup") or [],
                "in_rels": sorted(set(in_rels.get(label) or [])),
            }
    _reg_cache["data"] = reg
    _reg_cache["ts"] = now
    return reg


def entity_entry(entity_type):
    """取某实体类型的参数（未配置时回退锚定类型，再退全表第一个）"""
    reg = type_registry()
    anchor = core_sem().get("anchor_label") or u"业务事项"
    return (reg.get(_text(entity_type)) or
            reg.get(anchor) or
            (reg.values()[0] if reg else {}))


def _run_evidence(segs):
    """执行一组证据查询并去重，返回 (rows, err)"""
    rows = []
    for stmt, params in segs:
        r = graph_query(stmt, params)
        if not r.get("success"):
            return None, u"证据查询失败：{}".format(r.get("error"))
        rows.extend(x for x in (r.get("rows") or []) if _text(x.get("rel")).strip())
    seen = set()
    dedup = []
    for r in rows:
        key = (_text(r.get("src")), _text(r.get("rel")),
               _text(r.get("dir")), _text(r.get("nb_type")),
               _text(r.get("nb_name")), _text(r.get("role")))
        if key not in seen:
            seen.add(key)
            dedup.append(r)
    return dedup, None


def _name_expr(var, reg=None):
    """按图配置生成跨类型名称表达式，避免对所有节点硬套“名称”属性。"""
    reg = reg or type_registry()
    props = []
    for ent in reg.values():
        p = _text(ent.get("name_prop")).strip()
        if p and p not in props:
            props.append(p)
    if u"名称" not in props:
        props.append(u"名称")
    return u"coalesce({})".format(
        u",".join(u"{}.`{}`".format(var, p) for p in props) + u",''")


def _compose_entries(compose):
    if isinstance(compose, dict):
        return [compose]
    return [c for c in (compose or []) if isinstance(c, dict)]


def _rollup_paths(rollup):
    """归一化为路径列表；单条路径是 [{segment}, ...]，也支持多条备选路径。"""
    if not isinstance(rollup, list) or not rollup:
        return []
    if isinstance(rollup[0], dict):
        return [rollup]
    return [p for p in rollup if isinstance(p, list) and p]


def dependent_systems(names):
    """查一批业务各自的承载系统，返回 {业务名: [系统名, ...]}（按核心语义参数化；
    图未配置承载关系/系统类型时返回空，报告里显示"未记录"）"""
    sem = core_sem()
    anchor = sem.get("anchor_label") or u"业务事项"
    hosting = _text(sem.get("hosting_rel")).strip()
    system = _text(sem.get("system_label")).strip()
    np_ = sem.get("name_prop") or u"名称"
    names = [_text(n) for n in names if _text(n).strip()][:60]
    if not names or not hosting or not system:
        return {}
    r = graph_query(
        u"MATCH (b:`{a}`) WHERE b.`{np}` IN $ns "
        u"MATCH (b)-[:`{h}`]->(s:`{sl}`) "
        u"RETURN DISTINCT b.`{np}` AS biz, coalesce(s.`{np}`,'') AS sys LIMIT 400"
        .format(a=anchor, h=hosting, sl=system, np=np_), {"ns": names})
    m = {}
    for row in (r.get("rows") or []):
        m.setdefault(_text(row.get("biz")), []).append(_text(row.get("sys")))
    return m


def evidence_rows(entity_type, biz_id):
    """影响分析的标准证据查询（代码确定性构建，按实体类型参数化）：
    ① 该节点全部 1 跳邻接  ② entity_roles 标注的传播入边（变长）
    ③ 子结构展开  ④ 非锚定实体：无向 1..3 跳归并到锚定类型（core_semantics），
       并对归并出的业务跑依赖入边变长路径（带 直接/间接 hop 标记）。
    返回 (rows, cypher_text, rollup_bizs)；失败时 rows 为 None。"""
    id_ = _text(biz_id).strip()
    if not id_:
        return [], u"", []
    sem = core_sem()
    np_ = sem.get("name_prop") or u"名称"
    anchor = sem.get("anchor_label") or u"业务事项"
    dep_rel = _text(sem.get("dependency_rel")).strip()
    ent = entity_entry(entity_type)
    label = ent.get("label") or anchor
    pk = ent.get("pk") or np_
    current_np = ent.get("name_prop") or np_
    reg = type_registry()
    nb_name_expr = _name_expr(u"nb", reg)

    segs = []
    seg1 = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id "
            u"OPTIONAL MATCH (x)-[r1]-(nb) "
            u"RETURN x.`{np}` AS src, type(r1) AS rel, "
            u"CASE WHEN startNode(r1) = x THEN '出边' ELSE '入边' END AS dir, "
            u"labels(nb)[0] AS nb_type, {nbn} AS nb_name LIMIT 200"
            ).format(l=label, pk=pk, np=current_np, nbn=nb_name_expr)
    segs.append((seg1, {"id": id_}))

    cypher_notes = []
    for p in (ent.get("propagate") or []):
        rel = _text(p.get("rel"))
        depth = p.get("depth") or 1
        if not rel:
            continue
        other_label = _text(p.get("label")).strip()
        other_ent = reg.get(other_label) or {}
        other_np = other_ent.get("name_prop") or np_
        typed_a = u":`{}`".format(other_label) if other_label else u""
        direction = _text(p.get("direction") or u"in").lower()
        if direction == u"out":
            pattern = u"(x)-[:`{}`*1..{}]->(a{})".format(rel, int(depth), typed_a)
            shown_dir = u"出边"
        else:
            pattern = u"(a{})-[:`{}`*1..{}]->(x)".format(typed_a, rel, int(depth))
            shown_dir = u"入边"
        role = _text(p.get("role") or rel).replace(u"'", u"")
        if dep_rel and rel == dep_rel:
            # 兼容既有依赖分析：src 保持为依赖候选，供直接/间接分桶使用。
            src_expr, nb_expr = u"a.`{}`".format(other_np), u"x.`{}`".format(current_np)
        else:
            # 普通关联证据统一按“当前节点 --关系--> 邻居”表达，方向与 src 一致。
            src_expr, nb_expr = u"x.`{}`".format(current_np), u"a.`{}`".format(other_np)
        seg2 = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id MATCH {pattern} "
                u"RETURN DISTINCT {src} AS src, '{r}' AS rel, '{dr}' AS dir, "
                u"labels(a)[0] AS nb_type, {nb} AS nb_name, '{role}' AS role LIMIT 200"
                ).format(l=label, pk=pk, pattern=pattern, src=src_expr,
                         r=rel, dr=shown_dir, nb=nb_expr, role=role)
        segs.append((seg2, {"id": id_}))
        cypher_notes.append(u"-- 关联{} {} *1..{} ({})".format(
            shown_dir, rel, depth, p.get("role") or u""))

    for comp in _compose_entries(ent.get("compose")):
        if not (comp.get("rel") and comp.get("child")):
            continue
        child_ent = reg.get(comp["child"]) or {}
        child_np = child_ent.get("name_prop") or np_
        nb3_name_expr = _name_expr(u"nb3", reg)
        seg3 = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id "
                u"MATCH (x)-[:`{cr}`]->(s:`{cl}`) "
                u"OPTIONAL MATCH (s)-[r3]-(nb3) WHERE NOT (nb3 = x) "
                u"RETURN s.`{snp}` AS src, type(r3) AS rel, "
                u"CASE WHEN startNode(r3) = s THEN '出边' ELSE '入边' END AS dir, "
                u"labels(nb3)[0] AS nb_type, {nbn} AS nb_name LIMIT 200"
                ).format(l=label, pk=pk, snp=child_np, cr=comp["rel"],
                         cl=comp["child"], nbn=nb3_name_expr)
        segs.append((seg3, {"id": id_}))

    rows, err = _run_evidence(segs)
    if err:
        return None, err, []

    # ---- 非锚定实体：先按显式结构路径归并到锚定类型；存在依赖边时再传播 ----
    rollup_bizs = []
    if label != anchor:
        roll_rows = []
        for path in _rollup_paths(ent.get("rollup")):
            pattern = u"(x)"
            valid = True
            for i, step in enumerate(path):
                rel = _text(step.get("rel")).strip()
                target = _text(step.get("label")).strip()
                if not rel or not target:
                    valid = False
                    break
                node = u"v{}".format(i)
                if _text(step.get("direction") or u"out").lower() == u"in":
                    pattern += u"<-[:`{}`]-({}:`{}`)".format(rel, node, target)
                else:
                    pattern += u"-[:`{}`]->({}:`{}`)".format(rel, node, target)
            if not valid or not path:
                continue
            final = u"v{}".format(len(path) - 1)
            anchor_np = (reg.get(anchor) or {}).get("name_prop") or np_
            seg_roll = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id MATCH {path} "
                        u"RETURN DISTINCT x.`{xnp}` AS src, '归属业务' AS rel, "
                        u"'入边' AS dir, '{a}' AS nb_type, {b}.`{anp}` AS nb_name LIMIT 50"
                        ).format(l=label, pk=pk, path=pattern, xnp=current_np,
                                 a=anchor, b=final, anp=anchor_np)
            rr = graph_query(seg_roll, {"id": id_})
            if not rr.get("success"):
                return None, u"归并查询失败：{}".format(rr.get("error")), []
            roll_rows.extend(rr.get("rows") or [])

        # 兼容既有图：没有显式 rollup 时，只有配置了依赖传播才使用旧的无向归并。
        if not _rollup_paths(ent.get("rollup")) and dep_rel:
        # 只走结构性归属链（排除依赖/协同等协作关系，避免把依赖邻居误判为归属）
            excl_rels = sem.get("rollup_exclude_rels") or [dep_rel]
            excl2 = u"[" + u",".join(u"'{}'".format(r) for r in excl_rels) + u"]"
            seg_roll = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id "
                        u"MATCH (x)-[rs*1..3]-(b:`{a}`) "
                        u"WHERE ALL(r IN rs WHERE NOT type(r) IN {ex}) "
                        u"RETURN DISTINCT x.`{xnp}` AS src, '归属业务' AS rel, '入边' AS dir, "
                        u"'{a}' AS nb_type, b.`{anp}` AS nb_name LIMIT 50"
                        ).format(l=label, pk=pk, xnp=current_np, a=anchor,
                                 anp=(reg.get(anchor) or {}).get("name_prop") or np_, ex=excl2)
            rr = graph_query(seg_roll, {"id": id_})
            if not rr.get("success"):
                return None, u"归并查询失败：{}".format(rr.get("error")), []
            roll_rows.extend(rr.get("rows") or [])

        rows.extend(roll_rows)
        seen_biz = []
        for row in roll_rows:
            n = _text(row.get("nb_name"))
            if n and n not in seen_biz:
                seen_biz.append(n)
        rollup_bizs = seen_biz     # 归并出的全部归属业务（seg_roll 已 LIMIT 50）
        if rollup_bizs and dep_rel:
            depth = int(sem.get("dependency_depth") or 6)
            # 批量依赖传播（对全部归属业务一次查完，替代逐业务循环）：
            # 直接（1跳）与变长各一条，按 (依赖方, 归属业务) 对区分直接/间接
            bns = {u"bns": rollup_bizs}
            rd = graph_query(
                u"MATCH (a:`{a}`)-[:`{d}`]->(b:`{a}`) "
                u"WHERE b.`{np}` IN $bns AND a.`{np}` <> b.`{np}` "
                u"RETURN DISTINCT a.`{np}` AS src, '{d}' AS rel, '入边' AS dir, "
                u"'{a}' AS nb_type, b.`{np}` AS nb_name, '直接' AS hop LIMIT 500"
                .format(a=anchor, d=dep_rel, np=np_), bns)
            rv = graph_query(
                u"MATCH (a:`{a}`)-[:`{d}`*1..{dd}]->(b:`{a}`) "
                u"WHERE b.`{np}` IN $bns AND a.`{np}` <> b.`{np}` "
                u"RETURN DISTINCT a.`{np}` AS src, b.`{np}` AS nb_name LIMIT 500"
                .format(a=anchor, d=dep_rel, dd=depth, np=np_), bns)
            if not rd.get("success") or not rv.get("success"):
                return None, u"归并依赖查询失败", []
            direct_pairs = set((_text(x.get("src")), _text(x.get("nb_name")))
                               for x in rd.get("rows") or [])
            rows.extend(rd.get("rows") or [])
            for x in rv.get("rows") or []:
                pair = (_text(x.get("src")), _text(x.get("nb_name")))
                if pair not in direct_pairs:
                    rows.append({u"src": pair[0], u"rel": dep_rel,
                                 u"dir": u"入边", u"nb_type": anchor,
                                 u"nb_name": pair[1], u"hop": u"间接"})
        if rollup_bizs:
            suffix = u"并执行 {} 依赖传播".format(dep_rel) if dep_rel else u"（未配置依赖传播）"
            cypher_notes.append(u"-- 非锚定实体已归并到{}（{}，最多 50 个）{}".format(
                anchor, u"、".join(rollup_bizs), suffix))

    # 变长路径可能按路径展开出重复行（含同名不同ID的节点），按 关系+方向+邻居名 去重
    seen = set()
    dedup = []
    positions = {}
    for r in rows:
        key = (_text(r.get("src")), _text(r.get("rel")),
               _text(r.get("dir")), _text(r.get("nb_type")),
               _text(r.get("nb_name")), _text(r.get("hop")))
        if key in seen:
            # 同一条 1 跳边既来自通用邻接又来自角色查询时，保留带角色标记的行，
            # 供上层稳定分组，同时避免把同一证据重复交给模型。
            pos = positions[key]
            if r.get("role") and not dedup[pos].get("role"):
                dedup[pos] = r
            continue
        seen.add(key)
        positions[key] = len(dedup)
        dedup.append(r)
    return dedup, u"\n".join(cypher_notes), rollup_bizs


def fact_rows(entity_type, ids):
    """事实问答证据：给定某实体类型的 ID 列表，返回它们的 1 跳邻接
    （有组成关系时再加子结构邻接）。按类型分组执行。"""
    ent = entity_entry(entity_type)
    np_ = core_sem().get("name_prop") or u"名称"
    label = ent.get("label") or core_sem().get("anchor_label") or u"业务事项"
    pk = ent.get("pk") or np_
    current_np = ent.get("name_prop") or np_
    reg = type_registry()
    nb_name_expr = _name_expr(u"nb", reg)
    ids = [i for i in ids if _text(i).strip()][:10]
    if not ids:
        return []
    segs = []
    stmt1 = (u"MATCH (x:`{l}`) WHERE x.`{pk}` IN $ids "
             u"OPTIONAL MATCH (x)-[r]-(nb) "
             u"RETURN x.`{np}` AS biz, type(r) AS rel, "
             u"CASE WHEN startNode(r) = x THEN '出边' ELSE '入边' END AS dir, "
             u"labels(nb)[0] AS nb_type, {nbn} AS nb_name "
             u"LIMIT 200").format(l=label, pk=pk, np=current_np, nbn=nb_name_expr)
    segs.append((stmt1, {"ids": ids}))
    for comp in _compose_entries(ent.get("compose")):
        if not (comp.get("rel") and comp.get("child")):
            continue
        nb2_name_expr = _name_expr(u"nb2", reg)
        stmt2 = (u"MATCH (x:`{l}`) WHERE x.`{pk}` IN $ids "
                 u"MATCH (x)-[:`{cr}`]->(s:`{cl}`) "
                 u"OPTIONAL MATCH (s)-[r2]-(nb2) "
                 u"RETURN x.`{np}` AS biz, type(r2) AS rel, "
                 u"CASE WHEN startNode(r2) = s THEN '出边' ELSE '入边' END AS dir, "
                 u"labels(nb2)[0] AS nb_type, {nbn} AS nb_name "
                 u"LIMIT 200").format(l=label, pk=pk, np=current_np,
                                      cr=comp["rel"], cl=comp["child"], nbn=nb2_name_expr)
        segs.append((stmt2, {"ids": ids}))
    rows = []
    for stmt, params in segs:
        r = graph_query(stmt, params)
        if r.get("success"):
            rows.extend(r.get("rows") or [])
    return rows


# ---------------- 图谱工作台（可视化查询） ----------------
def _rel_directed_map():
    """关系类型 -> 是否有向（schema 缺省按有向处理）"""
    dmap = {}
    schema, err = gateway.build_schema()
    if not err:
        for rt in schema.get("relationship_types") or []:
            d = rt.get("directed")
            dmap[_text(rt.get("name"))] = True if d is None else bool(d)
    return dmap


def neighbors_graph(entity_type, entity_id, limit=100):
    """工作台邻接展开：某实体 1 跳邻居，返回归一化图结构。
    节点身份兼容两种来源：主键值（如业务事项的 ID）或名称——
    邻居类型可能没有中心类型的主键属性，取不到时用名称兜底，
    所以中心/邻居都按「主键 OR 名称」匹配。"""
    id_ = _text(entity_id).strip()
    if not id_:
        return {"success": False, "error": u"实体 ID 不能为空"}
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 300))
    ent = entity_entry(entity_type)
    sem = core_sem()
    np_ = sem.get("name_prop") or u"名称"
    label = ent.get("label") or sem.get("anchor_label") or u"业务事项"
    pk = ent.get("pk") or np_
    current_np = ent.get("name_prop") or np_
    nb_name_expr = _name_expr(u"nb")
    stmt = (u"MATCH (x:`{l}`) WHERE x.`{pk}` = $id OR x.`{np}` = $id "
            u"OPTIONAL MATCH (x)-[r]-(nb) "
            u"RETURN x.`{np}` AS cnm, labels(nb)[0] AS t, nb.`{pk}` AS pid, "
            u"{nbn} AS nm, type(r) AS rel, "
            u"CASE WHEN startNode(r) = x THEN '出边' ELSE '入边' END AS dir "
            u"LIMIT {n}").format(l=label, pk=pk, np=current_np,
                                  nbn=nb_name_expr, n=limit + 1)
    r = graph_query(stmt, {"id": id_})
    if not r.get("success"):
        return {"success": False, "error": r.get("error")}
    rows = r.get("rows") or []
    truncated = len(rows) > limit
    if truncated:
        rows = rows[:limit]

    rows = rows or [{}]
    cnm = _text((rows[0] or {}).get("cnm")) or id_
    center_id = u"{}:{}".format(label, id_)
    nodes = [{"id": center_id, "label": label, "name": cnm}]
    node_ids = {center_id}
    dmap = _rel_directed_map()
    edges = []
    edge_ids = set()
    tbl_rows = []          # 邻接表格行（前端"查看表格"用，与边同步去重）
    for row in rows:
        t = _text(row.get("t")).strip()
        pid = _text(row.get("pid")).strip()
        nm = _text(row.get("nm")).strip()
        rel = _text(row.get("rel")).strip()
        ident = pid or nm    # 邻居类型无中心主键属性时（pid 为空）用名称
        if not t or not ident or not rel:   # OPTIONAL MATCH 无邻居的空行
            continue
        nid = u"{}:{}".format(t, ident)
        if nid not in node_ids:
            node_ids.add(nid)
            nodes.append({"id": nid, "label": t, "name": nm or ident})
        if _text(row.get("dir")) == u"出边":
            src, dst = center_id, nid
        else:
            src, dst = nid, center_id
        eid = u"{}|{}|{}".format(src, rel, dst)
        if eid not in edge_ids:
            edge_ids.add(eid)
            edges.append({"id": eid, "type": rel, "source": src,
                          "target": dst, "directed": dmap.get(rel, True)})
            tbl_rows.append({u"src": cnm, u"rel": rel,
                             u"dir": _text(row.get("dir")), u"nb_type": t,
                             u"nb_name": nm or ident})
    return {"success": True, "center": nodes[0], "nodes": nodes,
            "edges": edges, "truncated": truncated,
            "columns": [u"src", u"rel", u"dir", u"nb_type", u"nb_name"],
            "rows": tbl_rows}


def node_props(entity_type, entity_id):
    """工作台节点属性详情：优先 RETURN n（行模式若序列化为属性 map），
    非 dict 时回退为按 schema 属性列表逐属性投影。"""
    id_ = _text(entity_id).strip()
    if not id_:
        return {"success": False, "error": u"实体 ID 不能为空"}
    ent = entity_entry(entity_type)
    sem = core_sem()
    np_ = sem.get("name_prop") or u"名称"
    label = ent.get("label") or sem.get("anchor_label") or u"业务事项"
    pk = ent.get("pk") or np_
    # 「主键 OR 名称」双匹配：节点复合 ID 可能来自主键值，也可能来自
    # 名称（Cypher 结果按名称识别的节点、无主键属性的类型）
    where = u"n.`{pk}` = $id OR n.`{np}` = $id".format(pk=pk, np=np_)
    r = graph_query(u"MATCH (n:`{l}`) WHERE {w} "
                    u"RETURN n AS node LIMIT 1".format(l=label, w=where),
                    {"id": id_})
    if r.get("success"):
        rows = r.get("rows") or []
        if rows:
            node = (rows[0] or {}).get("node")
            if isinstance(node, dict):
                props = {_text(k): v for k, v in node.items()}
                return {"success": True, "label": label,
                        "name": props.get(np_) or id_, "props": props}

    # 回退：按 schema 属性列表逐属性投影（属性访问是已验证能力）
    props_list = []
    schema, serr = gateway.build_schema()
    if not serr:
        for nt in schema.get("node_types") or []:
            if nt.get("name") == label:
                props_list = [p["name"] if isinstance(p, dict) else p
                              for p in (nt.get("properties") or [])]
    if not props_list:
        props_list = [pk, np_]
    ret = u", ".join(u"n.`{p}` AS `{p}`".format(p=p) for p in props_list)
    r2 = graph_query(u"MATCH (n:`{l}`) WHERE {w} "
                     u"RETURN {r} LIMIT 1".format(l=label, w=where, r=ret),
                     {"id": id_})
    if not r2.get("success"):
        return {"success": False, "error": r2.get("error")}
    rows2 = r2.get("rows") or []
    if not rows2:
        return {"success": False,
                "error": u"未找到节点 {}:`{}`".format(label, id_)}
    props = {}
    for p in props_list:
        v = (rows2[0] or {}).get(p)
        if v is not None:
            props[_text(p)] = v
    return {"success": True, "label": label,
            "name": props.get(np_) or id_, "props": props}


# 契约 A 列名后缀：支持英文（a_type/a_name/a_id）和中文（起点类型/起点名称/起点ID）
_TYPE_SUF = (u"_type", u"类型")
_NAME_SUF = (u"_name", u"名称", u"_id", u"ID")
_REL_COLS = (u"rel", u"_rel", u"type", u"关系", u"关系类型")


def _pair_cols(cols):
    """从列名表里找成对前缀：返回 {前缀: {"type": 列名, "name": 列名}}"""
    groups = {}
    for c in cols:
        for s in _TYPE_SUF:
            if c.endswith(s) and len(c) > len(s):
                groups.setdefault(c[:-len(s)], {})["type"] = c
        for s in _NAME_SUF:
            if c.endswith(s) and len(c) > len(s):
                groups.setdefault(c[:-len(s)], {})["name"] = c
    return dict((p, g) for p, g in groups.items()
                if g.get("type") and g.get("name"))


def _pair_node(row, grp, nodes):
    """契约 A 辅助：按前缀列组从行里取类型/名称/ID 建节点。
    返回复合 ID；该前缀无值返回 None。"""
    t = _text(row.get(grp["type"])).strip()
    nv = row.get(grp["name"])
    if not t and _text(nv).strip() == u"":
        return None
    ident = nv if _text(nv).strip() != u"" else t
    nid = u"{}:{}".format(t or u"实体", _text(ident))
    if nid not in nodes:
        nodes[nid] = {"id": nid, "label": t or u"实体",
                      "name": _text(nv).strip() or _text(ident)}
    return nid


def rows_to_graph(rows, columns):
    """把 /graph/query 的原始行分类为可画图或纯表格：
    契约 A（成对列）：存在两组「前缀+类型/名称」列（如 a_type/a_name、
      起点类型/起点名称），可选关系列（rel/关系）→ 有向边 a→b；
    契约 B（证据形状）：src/rel/dir/nb_type/nb_name（evidence_rows 列）
      → 中心-邻居边（按 dir 定方向）；
    都不满足 → render_hint=table，前端按表格渲染。"""
    cols = [_text(c) for c in (columns or [])]
    colset = set(cols)
    rows = rows or []

    if rows and colset >= {u"src", u"rel", u"nb_type", u"nb_name"}:
        anchor = core_sem().get("anchor_label") or u"业务事项"
        nodes = {}
        edges = []
        edge_ids = set()
        for row in rows:
            src = _text(row.get("src")).strip()
            rel = _text(row.get("rel")).strip()
            nb_t = _text(row.get("nb_type")).strip() or u"实体"
            nb_n = _text(row.get("nb_name")).strip()
            if not src or not nb_n:
                continue
            a_t = _text(row.get("src_type")).strip() or anchor
            a_id = u"{}:{}".format(a_t, src)
            b_id = u"{}:{}".format(nb_t, nb_n)
            if a_id not in nodes:
                nodes[a_id] = {"id": a_id, "label": a_t, "name": src}
            if b_id not in nodes:
                nodes[b_id] = {"id": b_id, "label": nb_t, "name": nb_n}
            if _text(row.get("dir")) == u"出边":
                s, d = a_id, b_id
            else:
                s, d = b_id, a_id
            eid = u"{}|{}|{}".format(s, rel, d)
            if eid not in edge_ids:
                edge_ids.add(eid)
                e = {"id": eid, "type": rel, "source": s, "target": d,
                     "directed": True}
                if _text(row.get("hop")):
                    e["hop"] = _text(row.get("hop"))
                edges.append(e)
        if edges:
            return {"render_hint": u"graph",
                    "nodes": list(nodes.values()), "edges": edges}

    pairs = _pair_cols(cols)
    prefixes = sorted(pairs.keys())
    if rows and len(prefixes) >= 2:
        pa, pb = pairs[prefixes[0]], pairs[prefixes[1]]
        relcol = next((c for c in _REL_COLS if c in colset), None)
        nodes = {}
        edges = []
        edge_ids = set()
        for row in rows:
            a_id = _pair_node(row, pa, nodes)
            b_id = _pair_node(row, pb, nodes)
            if not a_id or not b_id or a_id == b_id:
                continue
            rel = _text(row.get(relcol)).strip() if relcol else u""
            eid = u"{}|{}|{}".format(a_id, rel, b_id)
            if eid not in edge_ids:
                edge_ids.add(eid)
                edges.append({"id": eid, "type": rel or u"关联",
                              "source": a_id, "target": b_id,
                              "directed": True})
        if edges:
            return {"render_hint": u"graph",
                    "nodes": list(nodes.values()), "edges": edges}

    return {"render_hint": u"table"}


def schema_text():
    """元数据瘦身文本（逻辑同工作流里的 schema 瘦身代码节点）"""
    data, err = gateway.build_schema()
    if err:
        return u"（图结构读取失败：{}）".format(err)
    nodes = []
    for n in data.get("node_types") or []:
        props = u"、".join(p["name"] if isinstance(p, dict) else p
                           for p in (n.get("properties") or []))
        nodes.append(u"{}（唯一标识: {}，属性: {}，共{}条）".format(
            n.get("name"), n.get("unique_key"), props, n.get("count")))
    rels = []
    for r in data.get("relationship_types") or []:
        rels.append(u"{}: {}（有向）".format(
            r.get("name"), u"、".join(r.get("pairs") or [])))
    parts = [
        u"节点类型：\n" + u"\n".join(nodes),
        u"关系类型：\n" + u"\n".join(rels),
        u"影响规则：" + json.dumps(data.get("influence_rules") or {}, ensure_ascii=False),
        u"关系含义：" + json.dumps(data.get("relationship_meanings") or {}, ensure_ascii=False),
        u"已验证查询示例：\n" + u"\n".join(data.get("query_examples") or []),
    ]
    return u"\n\n".join(parts)


# ---------------- 已选业务提取（候选序号/关键词匹配） ----------------
# 注意：py2 的 re 不支持 \uXXXX 转义（[\u4e00-\u9fff] 会被解析成含 ASCII 数字
# 的乱类，"13"被吃成 3、"10"被吃成 0），所以这里必须写字面量汉字区间 一-龥
_ORD_RE = re.compile(
    u"^[一-龥]{0,4}\\s*(?:第)?([0-9一二两三四五六七八九十]+)\\s*"
    u"(?:个|项|条|号)?[一-龥]{0,2}\\s*[。！!，,]?\\s*$")


def _text(v):
    """任意值转 unicode 文本（py2 下 str(中文) 会炸，统一走这里）"""
    if v is None:
        return u""
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return u"{}".format(v)


def _ordinal(query):
    """从用户回复抠序号："10"、"第3个"、"选2"、"2号"、"第二个" 均支持，返回 int 或 None"""
    m = _ORD_RE.match(query or u"")
    if not m:
        return None
    tok = m.group(1)
    if tok.isdigit():
        return int(tok)
    if tok in CN_NUM:
        return CN_NUM[tok]
    if tok.count(u"十") == 1:  # 十 / 二十三
        left, right = tok.split(u"十")
        if (not left or left in CN_NUM) and (not right or right in CN_NUM):
            return CN_NUM.get(left, 1) * 10 + CN_NUM.get(right, 0)
    return None


def _norm_selected(c):
    if not isinstance(c, dict):
        return None
    kind = _text(c.get("type") or c.get("business_type")).strip()
    cid = c.get("id")
    if not kind or cid is None or not _text(cid).strip():
        return None
    return {"type": kind, "id": cid, "name": _text(c.get("name"))}


def pick_selected(query, business_keyword, state):
    """确定已选业务，返回 (selected, source)。
    source ∈ {"id","ordinal","name","keyword","selected",None}，说明本轮命中方式：
      id/ordinal/name/keyword = 用户本轮消息里有明确选择；
      selected = 只是落在上一轮的旧 selected 上（上层据此判断"用户是否在提新业务"）。
    优先级：ID 精确 > 序号 > 全名精确 > 关键词唯一命中 > 旧 selected 直取（最后）。
    序号越界返回 (None, None)（宁可再确认，不可静默分析错业务）。"""
    cands = state.get("candidates") or []
    q = _text(query).strip()
    # 1) ID 精确匹配——必须在序号解析之前：纯数字业务 ID（如 700002005）
    #    会被序号正则误当越界序号，导致用户回复 ID 永远选不中
    if q:
        for c in cands:
            if _text(c.get("id")).strip() == q:
                return _norm_selected(c), u"id"
    # 2) 序号：带序号永远按序号处理（新序号=换选）；越界不回退旧 selected
    idx = _ordinal(q)
    if idx:
        if 1 <= idx <= len(cands):
            return _norm_selected(cands[idx - 1]), u"ordinal"
        return None, None
    # 3) query 全名精确匹配（唯一时）——不依赖 LLM 提取的关键词
    if q:
        exact = [c for c in cands if _text(c.get("name")).strip() == q]
        if len(exact) == 1:
            return _norm_selected(exact[0]), u"name"
    # 4) 关键词兜底：仅在恰好唯一命中时采用（多候选同名场景不可靠）
    kw = _text(business_keyword).strip()
    if kw and cands:
        hits = [c for c in cands
                if _text(c.get("name")) and
                (kw in _text(c.get("name")) or _text(c.get("name")) in kw)]
        if len(hits) == 1:
            return _norm_selected(hits[0]), u"keyword"
    # 5) 旧 selected 直取（最后，source 标记供上层守卫识别新话题）
    sel = _norm_selected(state.get("selected") or {})
    if sel:
        return sel, u"selected"
    return None, None
