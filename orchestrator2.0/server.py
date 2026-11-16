# -*- coding: utf-8 -*-
"""编排服务入口（端口见 config.PORT，当前 5401）。融合后的唯一服务：聊天页面 + SSE 编排 + 图工具接口。

页面 / SSE 聊天 / 历史会话 / 停止 等前端接口，/api/chat 不转发平台工作流，
而是直接运行 pipeline 编排；另含系统配置接口（/api/settings，读写运行时
配置并实时生效）与图工具接口（/graph/*、/sessions/*，供外部调试图库）。

接口清单：
    GET  /                    聊天页面（templates/index.html，标签"分析智能体"）
    POST /api/chat            SSE 聊天（answer/status/error/done + [DONE]）
    POST /api/stop            停止（编排在本进程内，前端 abort 即中断）
    GET  /api/config          配置状态（前端判断是否可用）
    GET/POST /api/settings    系统配置（读=敏感项掩码；存=实时生效）
    GET  /api/conversations   会话列表
    DELETE /api/conversations/<id>  删除会话（含状态）
    GET  /api/messages        会话消息
    GET  /sessions/<id>/context     最近历史 + 业务状态
    POST /sessions/<id>/turns       保存/清空业务状态（state:null 清空）
    POST /api/schema/scan           §5.6 只读扫描目标图，生成候选配置（不覆盖已发布）
    GET  /api/schema/diff           §5.6 已发布版 vs 候选版结构化差异（只读）
    POST /api/schema/validate       §5.6 候选配置校验（主键/关系/依赖/冒烟，不发布）
    POST /api/schema/publish        §5.6 原子发布候选（旧配置自动备份，可回滚）
    POST /api/schema/rollback       §5.6 回滚到历史版本
    GET  /api/schema/versions       §5.7 版本索引
    GET  /api/schema/relations      §5.6 关系类别与语义确认情况
    POST /api/schema/bind           §5.6 兼容旧绑定入口（不再隐式覆盖）
    GET  /graph/schema              图结构 + 业务语义说明
    POST /graph/business/search     按关键词搜候选业务
    POST /graph/query               只读执行 Cypher（返回原始行证据；mode=graph 附带图结构）
    POST /graph/neighbors           实体 1 跳邻接（工作台展开，归一化图结构 + 邻接表格行）
    POST /graph/node                节点属性详情（工作台属性面板）
    POST /graph/analyze             节点智能分析（影响分析报告，工作台属性面板按钮）

启动：cd orchestrator2.0 && python server.py（py2/py3 均可）
      或项目根目录 ./start.sh（自动挂离线依赖）
"""
from __future__ import print_function

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _force_utf8_stdio():
    """标准输出/错误流强制 UTF-8 + replace：Windows GBK 控制台/任意 locale 下，
    打印中文日志都不会 UnicodeEncodeError（无需手动设 PYTHONIOENCODING）。
    必须在 Flask/logging 初始化前执行。"""
    if sys.version_info[0] == 2:
        import codecs
        sys.stdout = codecs.getwriter(u"utf-8")(sys.stdout, u"replace")
        sys.stderr = codecs.getwriter(u"utf-8")(sys.stderr, u"replace")
    else:
        for name in ("stdout", "stderr"):
            stream = getattr(sys, name, None)
            if stream is not None and hasattr(stream, "reconfigure"):
                try:
                    stream.reconfigure(encoding="utf-8", errors="replace")
                except (ValueError, IOError, OSError):
                    pass


_force_utf8_stdio()

from flask import (Flask, Response, jsonify, render_template, request,
                   stream_with_context)

try:
    from urllib.parse import quote as urllib_quote   # py3
except ImportError:
    from urllib import quote as urllib_quote         # py2

import config
import llm
import pipeline
import prompts
import runlog
import store
import settings

log = config.get_logger("server")

settings.load_and_apply_on_start()  # 启动时应用运行时配置（系统配置界面保存的）
prompts.load_overrides_on_start()   # 启动时应用提示词覆盖（管理页 /admin 保存的）


# ---------------- 可选管理 Token（S1：敏感接口保护） ----------------
# 环境变量 KIRO_ADMIN_TOKEN 不空时，/api/settings、/api/test/*、/api/schema/bind
# 必须带 X-Admin-Token 或 ?admin_token= 才能访问。为空（默认）时保持向后兼容，
# 行为不变，但在服务日志中打印一行提醒。内网专网/反代鉴权场景可直接留空。
_ADMIN_TOKEN = (os.environ.get("KIRO_ADMIN_TOKEN") or u"").strip()
if not _ADMIN_TOKEN:
    log.warning(u"未设置 KIRO_ADMIN_TOKEN，敏感接口（/api/settings、/api/test/*、"
                u"/api/schema/*）对内网开放。生产环境建议通过环境变量 "
                u"KIRO_ADMIN_TOKEN=... 启用保护。")


def _check_admin():
    """未设置 token 时放行；设置了则校验 header/query。
    返回 (ok, response_or_None)——不通过时返回 Flask 响应对象。"""
    if not _ADMIN_TOKEN:
        return True, None
    tok = (request.headers.get("X-Admin-Token") or
           request.args.get("admin_token") or u"").strip()
    if tok and tok == _ADMIN_TOKEN:
        return True, None
    return False, (jsonify({"error": u"未授权：请在请求头 X-Admin-Token 带上管理令牌"}),
                   401)

app = Flask(__name__,
            template_folder=os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "templates"))
# 模板按 mtime 自动重载：改 index.html 后无需重启服务。生产下每次请求都会 stat
# 模板文件，带来不必要的 IO——用环境变量 KIRO_TEMPLATES_AUTO_RELOAD=0 关闭
# （默认开，保持开发期便利；小 bug 清单：server.py:81）
app.config["TEMPLATES_AUTO_RELOAD"] = (
    (os.environ.get("KIRO_TEMPLATES_AUTO_RELOAD") or u"1").strip() != u"0")


def _is_api_path():
    p = request.path or u""
    return p.startswith(u"/api/") or p.startswith(u"/graph/")


# API 路径统一 JSON 错误出口：服务跑旧代码（路由不存在→404）、HTTP 方法不符、
# 未捕获异常时，前端拿到的是可解读的 JSON 而不是 Flask HTML 错误页——
# 否则 r.json() 解析 "<!DOCTYPE..." 报 "Unexpected token '<'"，掩盖真实原因
@app.errorhandler(404)
def _api_not_found(e):
    if _is_api_path():
        return jsonify({"ok": False, "error": u"API 不存在：{}（服务可能在跑"
                        u"旧版代码，请确认文件已更新并重启服务）".format(
                            request.path)}), 404
    return e


@app.errorhandler(405)
def _api_bad_method(e):
    if _is_api_path():
        return jsonify({"ok": False, "error": u"请求方法不允许：{} {}".format(
            request.method, request.path)}), 405
    return e


@app.errorhandler(Exception)
def _api_server_error(e):
    if _is_api_path():
        log.exception(u"API 未捕获异常 {}".format(request.path))
        return jsonify({"ok": False, "error": u"服务内部异常：{}".format(e)}), 500
    return e


import gateway as gw  # noqa: E402  图客户端模块（Flask 实例已占名 app）


def sse(obj):
    data = u"data: {}\n\n".format(json.dumps(obj, ensure_ascii=False))
    if sys.version_info[0] == 2 and isinstance(data, unicode):
        return data.encode("utf-8")
    return data


@app.route("/")
def index():
    return render_template("index.html", mode_label=u"分析智能体2.0")


@app.route("/admin")
def admin():
    """管理控制台（Swagger 风格单页）：全部运行时配置 / 提示词在线覆盖 /
    运行日志 / Schema 绑定与 notes 校对。页面本身不设守卫（与 / 一致），
    敏感操作走各 /api 接口已有的 _check_admin。"""
    return render_template("admin.html")


@app.route("/api/config")
def api_config():
    return jsonify({"configured": bool(config.get("DIFY_API_KEY") if config.get("LLM_PLATFORM") == "dify"
                               else config.get("SFM_API_KEY"))})


@app.route("/api/settings")
def get_settings():
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = settings.load()
    # 敏感项（密码/密钥）回传掩码；留空或 ****** 保存时表示保持原值
    for k in (u"galaxy_pass", u"dify_key", u"sfm_key"):
        if data.get(k):
            data[k] = u"******"
    return jsonify(data)


@app.route("/api/settings", methods=["POST"])
def save_settings():
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    ok, msg = settings.save(data)
    return jsonify({"ok": True, "msg": msg} if ok else {"error": msg}), (200 if ok else 400)


# ---------------- 提示词在线覆盖（管理页 /admin「提示词」节）----------------
@app.route("/api/prompts")
def get_prompts():
    ok, resp = _check_admin()
    if not ok:
        return resp
    return jsonify({"ok": True, "items": prompts.list_prompts()})


@app.route("/api/prompts", methods=["POST"])
def save_prompt():
    """保存单条提示词覆盖：校验（占位符/长度）→ 落盘 → 热补丁即刻生效。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    name = data.get("name") or u""
    text = data.get("text")
    if text is None:
        return jsonify({"error": u"缺少 text"}), 400
    ok, err = prompts.set_override(name, text)
    if not ok:
        return jsonify({"error": err}), 400
    return jsonify({"ok": True, "msg": u"已保存并即时生效"})


@app.route("/api/prompts/reset", methods=["POST"])
def reset_prompt():
    """恢复单条提示词出厂默认：删覆盖条目 + 还原模块属性。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    ok, err = prompts.reset_override(data.get("name") or u"")
    if not ok:
        return jsonify({"error": err}), 400
    return jsonify({"ok": True, "msg": u"已恢复默认"})


# ---------------- 连接测试（系统配置弹窗按钮）----------------
def _conn_from_form(data):
    """从请求体表单值构造显式 GraphConnection（§13.1 连接隔离）：
    非空、非掩码「******」的表单项生效，空值回退当前运行配置。
    跨图扫描/绑定/测试专用——全程不再 setattr 改全局，杜绝并发串图。"""
    def _pick(k):
        v = u"{}".format(data.get(k) or u"").strip()
        if not v or v == u"******":
            return None
        return v
    return gw.GraphConnection(
        base_url=_pick(u"galaxy_base"),
        graph_name=_pick(u"galaxy_graph"),
        user=_pick(u"galaxy_user"),
        password=_pick(u"galaxy_pass"),
        rsa_key=_pick(u"galaxy_rsa_key"))


@app.route("/api/test/graph", methods=["POST"])
def test_graph():
    """图数据库连通性测试：按表单值（未保存也可测）构造显式连接上下文，
    登录 + 一次 count 查询。不触碰全局配置，不影响运行中的服务。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    t0 = time.time()
    try:
        conn = _conn_from_form(data)
        _token, err = gw.gb_token(conn)
        if err:
            return jsonify({"ok": False, "msg": u"登录失败：{}".format(err)})
        rows, _, qerr = gw.graph_run(u"MATCH (n) RETURN count(n) AS c",
                                     trusted=True, conn=conn,
                                     stage=u"连接测试")
        if qerr:
            return jsonify({"ok": False,
                            "msg": u"登录成功但查询失败：{}".format(qerr)})
        cnt = (rows or [{}])[0].get("c")
        return jsonify({"ok": True, "msg": (
            u"连接成功：图「{}」共 {} 个节点（耗时 {:.0f} ms）".format(
                conn.graph_name, cnt, (time.time() - t0) * 1000))})
    except Exception as e:
        return jsonify({"ok": False, "msg": u"测试出错：{}".format(e)})


@app.route("/api/schema/bind", methods=["POST"])
def schema_bind():
    """换图绑定（§5.6 兼容旧入口，「不再隐式覆盖」）：
    - 目标图已有已发布配置（schema_notes.<图名>.json）→ 只做连通性验证，
      原样绑定，人工校对成果不覆盖；要刷新请走「扫描→差异→校验→发布」向导；
    - 无已发布配置 → 内部转完整流程：扫描真实结构 → 生成候选 → 校验 →
      原子发布（启发式猜测，发布后仍需校对 dependency_paths 等）。
    连接全程使用显式 GraphConnection（§13.1），不再 setattr 改全局。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    try:
        conn = _conn_from_form(data)
        import schema_generator
        import schema_manager
        import schema_validator
        published, _perr = schema_manager.load_published(conn.graph_name)
        if isinstance(published, dict):
            # 已有已发布配置：只验证连通性，不覆盖。
            # 统计用 build_schema（getSchema 为空时 Cypher 实扫兜底）——
            # 部分图库侧没有生成图模型元数据，官方 getSchema 会返回空
            schema, err = gw.build_schema(conn, use_cache=False)
            if err:
                return jsonify({"ok": False,
                                "msg": u"读取图结构失败：{}".format(err)})
            sem = published.get("core_semantics") or gw.core_semantics()
            versions = schema_manager.list_versions(conn.graph_name)
            return jsonify({
                "ok": True, "graph": conn.graph_name,
                "mode": u"existing",
                "created": False,
                "version": (versions[0] or {}).get(u"version")
                if versions else u"",
                "notes_file": schema_manager.published_path(conn.graph_name),
                "label_count": len(schema.get("node_types") or []),
                "rel_count": len(schema.get("relationship_types") or []),
                "labels": [n.get("name") for n in
                           (schema.get("node_types") or [])],
                "core_semantics": sem,
                "warnings": [u"已绑定现有图专属配置（未覆盖）。如需按最新图结构"
                             u"刷新，请使用「扫描图结构」向导"],
            })
        # 无已发布配置：扫描 → 候选 → 校验 → 原子发布
        metadata, merr = gw.collect_graph_metadata(conn)
        if merr:
            return jsonify({"ok": False,
                            "msg": u"扫描图结构失败：{}".format(merr)})
        notes, gen_warnings = schema_generator.generate_schema_notes(metadata)
        _path, serr = schema_manager.save_candidate(conn.graph_name, notes)
        if serr:
            return jsonify({"ok": False,
                            "msg": u"保存候选配置失败：{}".format(serr)})
        report = schema_validator.validate_notes(notes, conn=conn)
        # 旧绑定入口不得绕过新向导的校验/确认而静默发布。
        if report.get(u"overall") != u"passed":
            return jsonify({"ok": True, "graph": conn.graph_name,
                            "mode": u"candidate", "created": False,
                            "validation_overall": report.get(u"overall"),
                            "warnings": [u"扫描已生成候选配置，但尚未发布。请进入向导④校验并按需确认后发布。"],
                            "label_count": len(metadata.get("node_types") or []),
                            "rel_count": len(metadata.get("relationship_types") or [])})
        vid, puberr = schema_manager.publish(conn.graph_name, notes=notes,
                                             validation_report=report)
        if puberr:
            return jsonify({"ok": False,
                            "msg": u"发布配置失败：{}".format(puberr)})
        warnings = [w for w in (metadata.get("scan_warnings") or [])
                    if w]
        warnings.extend(w for w in (gen_warnings or []) if w)
        warnings.append(
            u"自动生成的语义是启发式猜测，已直接发布为当前配置；"
            u"请重点校对：锚定类型/依赖路径（dependency_paths 需人工确认后才"
            u"传播影响）、名称与主键（business_search）、关系含义"
            u"（relationship_meanings）")
        return jsonify({
            "ok": True, "graph": conn.graph_name,
            "mode": u"generated",
            "created": True,
            "version": vid,
            "notes_file": schema_manager.published_path(conn.graph_name),
            "label_count": len(metadata.get("node_types") or []),
            "rel_count": len(metadata.get("relationship_types") or []),
            "labels": [t.get("name") for t in (metadata.get("node_types") or [])
                       if t.get("name")],
            "core_semantics": notes.get("core_semantics") or {},
            "validation_overall": report.get(u"overall"),
            "warnings": warnings,
        })
    except Exception as e:
        log.exception(u"绑定出错")
        return jsonify({"ok": False, "msg": u"绑定出错：{}".format(e)})


# ---------------- §5.6 绑定向导：扫描 / 差异 / 校验 / 发布 / 回滚 ----------------

def _graph_param(data=None):
    """目标图名：请求体/查询参数 graph 优先（防路径逃逸由
    schema_manager.safe_graph_dir 统一处理），默认当前运行配置的图。"""
    g = u""
    if isinstance(data, dict):
        g = u"{}".format(data.get("graph") or u"").strip()
    g = (g or request.args.get("graph") or u"").strip() or gw.GALAXY_GRAPH
    return g


@app.route("/api/schema/scan", methods=["POST"])
def schema_scan():
    """§5.6 只读扫描目标图：getSchema + Cypher 交叉验证采集真实结构元数据，
    生成候选配置（不覆盖已发布配置），返回扫描报告与元数据摘要。
    连接信息取请求表单（未保存也可用），全程显式 GraphConnection。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    try:
        conn = _conn_from_form(data)
        metadata, err = gw.collect_graph_metadata(conn)
        if err:
            return jsonify({"ok": False, "msg": u"扫描失败：{}".format(err)})
        import schema_generator
        import schema_manager
        previous = None
        pub, _perr = schema_manager.load_published(conn.graph_name)
        if isinstance(pub, dict):
            previous = pub
        notes, gen_warnings = schema_generator.generate_schema_notes(metadata,
                                                                     previous)
        path, serr = schema_manager.save_candidate(conn.graph_name, notes)
        if serr:
            return jsonify({"ok": False,
                            "msg": u"保存候选配置失败：{}".format(serr)})
        node_types = metadata.get("node_types") or []
        rel_types = metadata.get("relationship_types") or []
        return jsonify({
            "ok": True,
            "graph": metadata.get("graph") or conn.graph_name,
            "schema_source": metadata.get("schema_source"),
            "connection": conn.describe(),
            "candidate_path": path,
            "schema_fingerprint": notes.get("schema_fingerprint") or u"",
            "node_type_count": len(node_types),
            "rel_type_count": len(rel_types),
            "node_types": [{
                "name": t.get("name"),
                "props": [p.get("name") for p in (t.get("properties") or [])
                          if isinstance(p, dict)],
                "observed_count": t.get("observed_count"),
                "declared_id": t.get("declared_id"),
                "id_validation": t.get("id_validation"),
            } for t in node_types],
            "relationship_types": [{
                "name": t.get("name"),
                "pairs": t.get("pairs") or [],
                "observed_count": t.get("observed_count"),
            } for t in rel_types],
            "observed_topology": metadata.get("observed_topology") or [],
            "differences": metadata.get("differences") or {},
            "scan_warnings": [w for w in (metadata.get("scan_warnings") or [])
                              if w] + [w for w in (gen_warnings or []) if w],
            "id_validation": metadata.get("id_validation") or {},
            "reconfirm_required": (notes.get("generation_info") or {}).get(
                u"reconfirm_required") or [],
        })
    except Exception as e:
        log.exception(u"扫描出错")
        return jsonify({"ok": False, "msg": u"扫描出错：{}".format(e)}), 500


@app.route("/api/schema/diff", methods=["GET"])
def schema_diff():
    """§5.6 只读差异：当前已发布版 vs 候选版结构化差异（绑定向导第3步）。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    import schema_manager
    graph = _graph_param()
    candidate, cerr = schema_manager.load_candidate(graph)
    if cerr or not isinstance(candidate, dict):
        return jsonify({"ok": False,
                        "msg": u"无候选配置，请先执行「扫描图结构」"}), 404
    published, perr = schema_manager.load_published(graph)
    diff = schema_manager.diff_notes(published, candidate)
    diff[u"ok"] = True
    diff[u"graph"] = graph
    diff[u"has_published"] = bool(not perr and isinstance(published, dict))
    return jsonify(diff)


@app.route("/api/schema/validate", methods=["POST"])
def schema_validate():
    """§5.6 候选配置校验：格式 / 标识符（主键实查）/ 关系结构 / 依赖语义 /
    查询冒烟。校验报告写回候选配置，不发布。连接信息取请求表单，
    缺省用当前运行配置。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    import schema_manager
    import schema_validator
    graph = _graph_param(data)
    candidate, cerr = schema_manager.load_candidate(graph)
    if cerr or not isinstance(candidate, dict):
        return jsonify({"ok": False, "msg": u"无候选配置，请先执行「扫描图结构」"}), 404
    try:
        conn = _conn_from_form(data)
        report = schema_validator.validate_notes(candidate, conn=conn)
    except Exception as e:
        log.exception(u"校验出错")
        return jsonify({"ok": False, "msg": u"校验出错：{}".format(e)}), 500
    # 报告写回候选（发布时随版本归档进 manifest）
    gen = candidate.get("generation_info") or {}
    gen[u"validation_report"] = report
    candidate[u"generation_info"] = gen
    candidate[u"validation_report"] = report
    schema_manager.save_candidate(graph, candidate)
    return jsonify({"ok": True, "graph": graph, "report": report})


@app.route("/api/schema/publish", methods=["POST"])
def schema_publish():
    """§5.6 管理员确认后原子发布候选配置（旧配置自动备份为历史版本，
    可回滚；发布后清空 gateway/store 各层缓存）。校验未通过时允许发布但
    附警告，由管理员自行判断。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    import schema_manager
    graph = _graph_param(data)
    candidate, cerr = schema_manager.load_candidate(graph)
    if cerr or not isinstance(candidate, dict):
        return jsonify({"ok": False, "msg": u"无候选配置，请先执行「扫描图结构」"}), 404
    report = (candidate.get("generation_info") or {}).get(u"validation_report") \
        or candidate.get(u"validation_report") or {}
    overall = u"{}".format((report or {}).get(u"overall") or u"unknown")
    if candidate.get(u"graph") != graph:
        return jsonify({"ok": False, "msg": u"候选配置图名与目标图不一致"}), 400
    if overall not in (u"passed", u"needs_review"):
        return jsonify({"ok": False, "msg": u"配置尚未成功校验或校验失败，请先执行④校验"}), 400
    if overall == u"needs_review" and not bool(data.get("acknowledge_review")):
        return jsonify({"ok": False, "msg": u"配置尚有待确认项，需明确确认后发布",
                        "requires_review_ack": True}), 409
    vid, err = schema_manager.publish(graph, notes=candidate,
                                      validation_report=report)
    if err:
        return jsonify({"ok": False, "msg": u"发布失败：{}".format(err)}), 500
    warnings = []
    if overall not in (u"passed",):
        warnings.append(u"候选配置校验状态为「{}」，建议先执行校验并处理"
                        u"失败项再发布".format(overall))
    return jsonify({"ok": True, "graph": graph, "version": vid,
                    "validation_overall": overall, "warnings": warnings})


@app.route("/api/schema/rollback", methods=["POST"])
def schema_rollback():
    """§5.6 回滚到已保存的历史版本（当前配置自动备份为新快照，
    回滚动作本身也记录进 manifest）。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    import schema_manager
    graph = _graph_param(data)
    version = u"{}".format(data.get("version") or u"").strip()
    if not version:
        return jsonify({"ok": False, "msg": u"缺少 version 参数"}), 400
    vid, err = schema_manager.rollback(graph, version)
    if err:
        return jsonify({"ok": False, "msg": u"回滚失败：{}".format(err)}), 400
    return jsonify({"ok": True, "graph": graph, "version": vid,
                    "rolled_back_from": version})


@app.route("/api/schema/versions", methods=["GET"])
def schema_versions():
    """§5.7 版本索引（绑定向导回滚下拉用）：版本号 / 时间 / 动作 / 校验状态。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    import schema_manager
    graph = _graph_param()
    return jsonify({"ok": True, "graph": graph,
                    "versions": schema_manager.list_versions(graph)})


@app.route("/api/schema/relations", methods=["GET"])
def schema_relations():
    """§5.6 关系类别与语义确认情况（内网管理页）：关系目录（结构验证标记）
    + 已确认依赖/组成路径 + 每条关系的语义确认状态。
    与向导编辑流一致：优先读【候选配置】（② 确认/扫描生成的内容，
    发布前就能看到），无候选时回退已发布配置。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    import schema_manager
    graph = _graph_param()
    notes, err = schema_manager.load_candidate(graph)
    if err or not isinstance(notes, dict):
        notes, _perr = schema_manager.load_published(graph)
        source = u"published" if isinstance(notes, dict) else u"none"
    else:
        source = u"candidate"
    if not isinstance(notes, dict):
        return jsonify({"ok": False, "msg": u"该图暂无配置，请先执行「扫描图结构」"}), 404
    dep_paths = [x for x in (notes.get(u"dependency_paths") or [])
                 if isinstance(x, dict)]
    def key(x):
        return (x.get(u"source"), x.get(u"relation"), x.get(u"target"))
    dep_map = {key(x): x for x in dep_paths}
    rels = []
    for c in (notes.get(u"relation_catalog") or []):
        if not isinstance(c, dict):
            continue
        k = key(c)
        dep = dep_map.get(k)
        rels.append({
            u"source": c.get(u"source"), u"relation": c.get(u"relation"),
            u"target": c.get(u"target"), u"observed": c.get(u"observed"),
            u"observed_count": c.get(u"observed_count"),
            u"semantic_type": c.get(u"semantic_type"),
            u"meaning": c.get(u"meaning") or (dep or {}).get(u"meaning") or u"",
            u"verified": bool(c.get(u"verified")),
            u"dependency_enabled": bool(dep),
            u"impact_direction": (dep or {}).get(u"impact_direction") or u"none",
            u"rel_props": c.get(u"rel_props") or [],
            u"semantics_confirmed": bool(c.get(u"semantics_confirmed")),
        })
    return jsonify({
        u"ok": True, u"graph": graph, u"source": source,
        u"relations": rels,
        u"relationship_meanings": notes.get(u"relationship_meanings") or {},
        u"dependency_paths": dep_paths,
        u"composition_paths": notes.get(u"composition_paths") or [],
        u"schema_fingerprint": notes.get(u"schema_fingerprint") or u"",
        u"generation_info": notes.get(u"generation_info") or {},
    })


@app.route("/api/schema/confirm_relations", methods=["POST"])
def schema_confirm_relations():
    """§5.6 关系语义人工确认（绑定向导第②步）：把界面上勾选确认的依赖关系
    写入【候选配置】的 dependency_paths（含义/深度/确认来源，方向固定取
    目录三元组的观察方向：source 依赖 target，target 变更影响 source），
    并同步 relationship_meanings 与 relation_catalog 的 dependency_enabled
    标记。只写候选，发布仍走第④步；未提交的条目不动，取消勾选 = 移除对应
    已确认条目。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    import schema_manager
    data = request.get_json(silent=True) or {}
    graph = _graph_param(data)
    rows = data.get(u"relations")
    if not isinstance(rows, list):
        return jsonify({"ok": False, "msg": u"relations 须为数组"}), 400

    notes, err = schema_manager.load_candidate(graph)
    if err or not isinstance(notes, dict):
        notes, perr = schema_manager.load_published(graph)
        if perr or not isinstance(notes, dict):
            # 业务性缺失返回 200 + ok:false（404 会在浏览器控制台产生
            # "Failed to load resource" 红字，误导排障方向）
            return jsonify({"ok": False,
                            "msg": u"该图无候选/已发布配置，请先在向导第①步扫描生成候选"})

    dep_paths = [p for p in (notes.get(u"dependency_paths") or [])
                 if isinstance(p, dict)]
    meanings = notes.get(u"relationship_meanings") or {}
    catalog = [c for c in (notes.get(u"relation_catalog") or [])
               if isinstance(c, dict)]
    stamp = u"人工确认（绑定向导 {}）".format(time.strftime(u"%Y-%m-%d"))
    def _key(item):
        return tuple(gw._u(item.get(x) or u"") for x in
                     (u"source", u"relation", u"target"))
    by_key = {_key(c): c for c in catalog}
    existing = {_key(d): d for d in dep_paths}
    changed = 0
    removed = 0
    for row in rows:
        if not isinstance(row, dict):
            return jsonify({"ok": False, "msg": u"关系配置必须是对象"}), 400
        k = _key(row)
        if k not in by_key:
            return jsonify({"ok": False, "msg": u"关系三元组不在当前扫描目录中：{}".format(k)}), 400
        c = by_key[k]
        confirmed = bool(row.get(u"confirmed"))
        impact = gw._u(row.get(u"impact_direction") or u"none")
        enabled = bool(row.get(u"dependency_enabled"))
        semantic_type = gw._u(row.get(u"semantic_type") or c.get(u"semantic_type") or u"association")
        if semantic_type not in (u"composition", u"association", u"candidate_dependency",
                                 u"dependency", u"organization", u"responsibility",
                                 u"ownership", u"unknown"):
            return jsonify({"ok": False, "msg": u"关系分类无效：{}".format(semantic_type)}), 400
        meaning = gw._u(row.get(u"meaning") or u"").strip()
        if confirmed and not meaning:
            return jsonify({"ok": False, "msg": u"确认关系语义必须填写业务含义"}), 400
        if enabled and (not confirmed or impact not in (u"target_to_source", u"source_to_target")):
            return jsonify({"ok": False, "msg": u"启用依赖必须确认语义并明确影响方向"}), 400
        # 分类（semantic_type）仅作语义标注供报告措辞参考，不限制能否传播；
        # 声明但当前无数据的关系也允许配置传播（校验时仅作提示，
        # 补数据后自动参与传播；沿无数据关系传播结果为空，无害）。
        try:
            depth = max(1, min(3, int(row.get(u"depth") or 1)))
            if enabled and depth > 1 and k[0] != k[2]:
                return jsonify({"ok": False, "msg": u"不同节点类型间的依赖只能配置1跳；多跳需明确逐段路径模板"}), 400
        except (TypeError, ValueError):
            depth = 1
        # 语义确认与依赖传播是两个不同的状态。每条关系按完整三元组保存。
        c[u"semantics_confirmed"] = confirmed
        c[u"semantic_type"] = semantic_type
        c[u"meaning"] = meaning if confirmed else u""
        c[u"dependency_enabled"] = enabled
        c[u"impact_direction"] = impact if enabled else u"none"
        if confirmed:
            meanings[c[u"relation"]] = meaning  # 旧版兼容的关系名建议文本，不作为确认标记
        if k in existing:
            del existing[k]
            removed += 1
        if enabled:
            existing[k] = {
                u"source": k[0], u"relation": k[1], u"target": k[2],
                u"direction": u"out", u"impact_direction": impact,
                u"dependency_enabled": True, u"meaning": meaning,
                u"depth": depth, u"confirmed_source": stamp}
        changed += 1
    notes[u"dependency_paths"] = list(existing.values())
    notes[u"relationship_meanings"] = meanings
    notes[u"relation_catalog"] = catalog
    # 语义修改后旧校验报告失效；必须重新运行 ④ 校验。
    notes.pop(u"validation_report", None)
    (notes.get(u"generation_info") or {}).pop(u"validation_report", None)
    _, werr = schema_manager.save_candidate(graph, notes)
    if werr:
        return jsonify({"ok": False, "msg": u"写入候选配置失败：{}".format(werr)}), 500
    runlog.event("schema_confirm_relations", graph=graph,
                 changed=changed, enabled=len(existing))
    return jsonify({u"ok": True, u"graph": graph,
                    u"confirmed": changed, u"removed": removed,
                    u"dependency_paths": notes[u"dependency_paths"]})


def _read_json(path):
    import io
    import json as _json
    with io.open(path, encoding="utf-8") as f:
        return _json.load(f)


@app.route("/api/schema/notes", methods=["GET"])
def schema_notes_get():
    """读取当前图的 schema_notes；同时返回图的 labels/relationship_types，
    供前端下拉选项。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    try:
        # 当前图配置
        notes = gw.load_schema_notes() or {}
        # 当前图结构（labels / rel 列表供前端下拉）
        schema, err = gw.build_schema()
        labels = []
        rels = []
        if not err and schema:
            labels = [n.get("name") for n in (schema.get("node_types") or [])
                      if n.get("name")]
            rels = [r.get("name") for r in (schema.get("relationship_types") or [])
                    if r.get("name")]
        # 定位实际使用的文件（优先图专属，回退默认）
        path = gw.notes_path_for(gw.GALAXY_GRAPH)
        if not path or not os.path.exists(path):
            path = gw.DEFAULT_NOTES_PATH
        return jsonify({
            "ok": True,
            "graph": gw.GALAXY_GRAPH,
            "notes_file": path,
            "labels": labels,
            "relationships": rels,
            "aliases": notes.get("aliases") or {},
            "relationship_meanings": notes.get("relationship_meanings") or {},
            "report_headers": notes.get("report_headers") or {},
        })
    except Exception as e:
        return jsonify({"ok": False, "msg": u"读取失败：{}".format(e)}), 500


@app.route("/api/schema/notes", methods=["POST"])
def schema_notes_save():
    """保存 schema_notes 的 aliases / relationship_meanings / report_headers。
    合并写入当前图实际使用的 notes 文件（图专属若不存在则落到默认文件）；
    保存后清缓存，立即生效。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    import io as _io
    data = request.get_json(silent=True) or {}
    aliases = data.get("aliases")
    rel_meanings = data.get("relationship_meanings")
    headers = data.get("report_headers")
    # 校验类型
    if aliases is not None and not isinstance(aliases, dict):
        return jsonify({"ok": False, "msg": u"aliases 必须是对象"}), 400
    if rel_meanings is not None and not isinstance(rel_meanings, dict):
        return jsonify({"ok": False, "msg": u"relationship_meanings 必须是对象"}), 400
    if headers is not None and not isinstance(headers, dict):
        return jsonify({"ok": False, "msg": u"report_headers 必须是对象"}), 400
    # 内容合法性
    if aliases is not None:
        for k, v in aliases.items():
            if not isinstance(v, list) or not all(isinstance(x, (str, bytes))
                                                  or (sys.version_info[0] == 2 and isinstance(x, unicode))
                                                  for x in v):
                return jsonify({"ok": False,
                                "msg": u"别名「{}」的值必须是字符串数组".format(k)}), 400
    if rel_meanings is not None:
        for k, v in rel_meanings.items():
            if not isinstance(v, dict):
                return jsonify({"ok": False,
                                "msg": u"关系含义「{}」的值必须是 "
                                       u"{{含义, 方向}} 对象".format(k)}), 400
    if headers is not None:
        for k, v in headers.items():
            if not isinstance(v, list):
                return jsonify({"ok": False,
                                "msg": u"报告表头「{}」的值必须是字符串数组".format(k)}), 400
    # 读 → 合并 → 写
    path = gw.notes_path_for(gw.GALAXY_GRAPH)
    if not path:
        return jsonify({"ok": False, "msg": u"当前图名为空，无法定位配置文件"}), 400
    if not os.path.exists(path):
        # 图专属文件不存在但默认文件绑定在当前图上时，继续写默认文件；
        # 否则按图专属路径新建（避免误改默认模板）
        try:
            default_notes = _read_json(gw.DEFAULT_NOTES_PATH)
        except (IOError, OSError, ValueError):
            default_notes = {}
        if (isinstance(default_notes, dict)
                and default_notes.get("graph") == gw.GALAXY_GRAPH):
            path = gw.DEFAULT_NOTES_PATH
    try:
        if os.path.exists(path):
            with _io.open(path, encoding="utf-8") as f:
                notes = json.load(f)
            notes = notes if isinstance(notes, dict) else {}
        else:
            notes = {u"graph": gw.GALAXY_GRAPH}
    except (IOError, OSError, ValueError) as e:
        return jsonify({"ok": False, "msg": u"读取原文件失败：{}".format(e)}), 500
    if aliases is not None:
        notes[u"aliases"] = aliases
    if rel_meanings is not None:
        notes[u"relationship_meanings"] = rel_meanings
    if headers is not None:
        notes[u"report_headers"] = headers
    try:
        content = json.dumps(notes, ensure_ascii=False, indent=2)
        if sys.version_info[0] == 2 and not isinstance(content, unicode):
            content = content.decode("utf-8")
        with _io.open(path, u"w", encoding="utf-8") as f:
            f.write(content)
    except (IOError, OSError, ValueError) as e:
        return jsonify({"ok": False, "msg": u"写入失败：{}".format(e)}), 500
    # 清缓存，立即生效
    gw._notes_cache = {"key": None, "data": None}
    try:
        import store as _store
        _store._reg_cache = {"data": None, "ts": 0}
    except Exception:
        pass
    return jsonify({"ok": True, "msg": u"已保存并生效", "notes_file": path})


def _form_overrides(data, keys):
    """从请求 JSON 抽取指定键的临时覆盖值（连通性测试用，不落盘）。
    语义与 settings.save 一致：空值跳过；密钥字段的 ****** 掩码视为
    「保留原值」跳过（前端会把已保存密钥回显为掩码）。"""
    out = {}
    if not isinstance(data, dict):
        return out
    for k in keys:
        if k not in data:
            continue
        v = data.get(k)
        if v is None:
            continue
        v = u"{}".format(v).strip()
        if v == u"":
            continue
        if v == settings._MASK and k in settings._SECRET_KEYS:
            continue
        out[k] = v
    return out


@app.route("/api/test/llm", methods=["POST"])
def test_llm():
    """LLM 平台连通性测试：临时应用表单值，发起一次极小补全调用。"""
    ok, resp = _check_admin()
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    keys = (u"llm_platform", u"dify_base", u"dify_key", u"sfm_base",
            u"sfm_key", u"sfm_agent_code", u"sfm_agent_version")
    overrides = _form_overrides(data, keys)
    t0 = time.time()
    saved_runtime = dict(config._runtime)
    try:
        config._runtime.update(overrides)
        # 同 settings.apply：写大写别名，config.get() 才能读到
        for k, v in overrides.items():
            config._runtime[settings._LLM_MAP[k]] = v
        llm.reset_caches()
        reply = llm.llm_complete(
            u"这是连通性测试。请只回复两个字：成功", u"（连通性测试）")
        reply = (reply or u"").strip()
        return jsonify({"ok": True, "msg": (
            u"连接成功（{}/{}，耗时 {:.0f} ms）：模型回复「{}」".format(
                config.get("LLM_PLATFORM"),
                (config.get("DIFY_API_BASE") if config.get("LLM_PLATFORM") == "dify"
                 else config.get("SFM_API_BASE")),
                (time.time() - t0) * 1000, reply[:20] or u"（空）"))})
    except Exception as e:
        return jsonify({"ok": False, "msg": u"调用失败：{}".format(e)})
    finally:
        config._runtime = saved_runtime
        llm.reset_caches()


@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or u"").strip()
    conv_id = data.get("conversation_id") or u""
    mode = (data.get("mode") or u"auto").strip().lower()
    if mode not in (u"auto", u"analysis", u"qa", u"writing"):
        mode = u"auto"
    if not query:
        return jsonify({"error": u"消息不能为空"}), 400

    # 本地会话：不存在则新建，标题取首条消息
    with store.gateway.db() as conn:
        row = None
        if conv_id:
            row = conn.execute(
                "SELECT id FROM conversations WHERE id = ?", (conv_id,)).fetchone()
        if row:
            conv_id = str(row["id"])
        else:
            cur = conn.execute(
                "INSERT INTO conversations(title, created_at) VALUES(?, ?)",
                (query[:24], time.time()))
            conv_id = str(cur.lastrowid)
        conn.execute(
            "INSERT INTO messages(conv_id, role, content, created_at) "
            "VALUES(?, 'user', ?, ?)", (conv_id, query, time.time()))

    t0 = time.time()

    def relay():
        answer = []
        graph_card = {}    # 本条回答的图谱卡片（每条消息存最后一张）
        graph_ev = {}      # 分析报告的内嵌证据图（nodes/edges，与卡片合并落盘）
        has_error = False
        interrupted = True   # 若正常跑到 done 置 False；finally 据此判是否要加中断标记
        try:
            for item in pipeline.run_pipeline(query, conv_id, mode):
                if isinstance(item, tuple):
                    kind, content = item
                else:
                    kind, content = "text", item
                if kind == "text":
                    answer.append(content)
                    yield sse({"type": "answer", "text": content})
                elif kind == "status":
                    yield sse({"type": "status", "text": content})
                elif kind == "graph":
                    graph_card["data"] = content
                    yield sse({"type": "graph_result", "graph": content})
                elif kind == "graph_data":
                    # 分析报告下方的内嵌证据图（stream_analysis 发出）
                    graph_ev["data"] = content
                    yield sse({"type": "graph_evidence", "graph": content})
                elif kind == "result":
                    # §13.3 内部结构化执行结果：已被 _analyze 消费，正常不外漏；
                    # 万一漏出也只静默跳过，不当错误发给前端
                    continue
                else:
                    has_error = True
                    yield sse({"type": "error", "text": content})
            interrupted = False
            yield sse({"type": "done", "conversation_id": conv_id})
            yield "data: [DONE]\n\n"
        finally:
            # C5：无论正常结束/客户端中断/出错，只要已生成文本就落盘，
            # 让消息历史与 pipeline 的 state（task/last_report/draft 等）保持同步。
            # 空内容才真不存（例如还没来得及 yield 任何 text 就被中断）。
            if answer:
                text = u"".join(answer)
                if has_error:
                    text = text + u"\n\n（生成中断，请重试）"
                elif interrupted:
                    text = text + u"\n\n（已中断）"
                gjson = None
                if (graph_card.get("data") is not None
                        or graph_ev.get("data") is not None):
                    # 合并落盘：{entity,...} + {nodes,edges,truncated} 同一个
                    # graph 字段；历史恢复时前端按 nodes 存在性渲染内嵌图，
                    # 旧消息（仅 entity）完全兼容
                    payload = {}
                    if isinstance(graph_card.get("data"), dict):
                        payload.update(graph_card["data"])
                    if isinstance(graph_ev.get("data"), dict):
                        payload.update(graph_ev["data"])
                    try:
                        gjson = json.dumps(payload, ensure_ascii=False)
                    except (TypeError, ValueError):
                        gjson = None
                try:
                    store.save_message(conv_id, "assistant", text,
                                       graph=gjson)
                except Exception:
                    log.exception(u"保存回答失败")
            # 运行日志收尾：完整回答全文 + 中断/错误标记（排障时与 intent/llm/graph 事件对齐时间线）
            runlog.event("turn_end", conv=conv_id,
                         ms=int((time.time() - t0) * 1000),
                         interrupted=interrupted, has_error=has_error,
                         answer=(u"".join(answer) if answer else None))

    return Response(
        stream_with_context(relay()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/stop", methods=["POST"])
def api_stop():
    # 编排在本进程内运行，前端 abort 请求即中断生成器，无需平台侧操作
    return jsonify({"ok": True})


# ---------------- 运行日志（runlog.jsonl，页面侧栏「运行日志」面板） ----------------
@app.route("/api/runlog", methods=["GET"])
def api_runlog():
    """最近日志事件（新的在前）。?limit=N 默认 300，上限 2000。"""
    try:
        limit = int(request.args.get("limit") or 300)
    except (TypeError, ValueError):
        limit = 300
    limit = max(1, min(limit, 2000))
    return jsonify({"ok": True, "items": runlog.tail(limit),
                    "path": runlog.RUNLOG_PATH, "enabled": runlog.enabled(),
                    "size": runlog.size()})


@app.route("/api/runlog/download", methods=["GET"])
def api_runlog_download():
    """下载完整日志文件（含已轮转的内容请另取 runlog.jsonl.1）"""
    if not os.path.exists(runlog.RUNLOG_PATH):
        return jsonify({"error": u"日志文件还不存在（还没有记录过任何事件）"}), 404
    try:
        with open(runlog.RUNLOG_PATH, "rb") as f:
            data = f.read()
    except (IOError, OSError) as e:
        return jsonify({"error": u"读取日志失败：{}".format(e)}), 500
    fname = u"runlog.jsonl"
    return Response(data, mimetype="application/octet-stream",
                    headers={"Content-Disposition":
                             u"attachment; filename=\"{}\"; filename*=UTF-8''{}"
                             .format(fname, urllib_quote(fname.encode("utf-8")))})


@app.route("/api/runlog/clear", methods=["POST"])
def api_runlog_clear():
    """清空日志（排障前"从零开始"用）"""
    ok = runlog.clear()
    return jsonify({"ok": bool(ok),
                    "msg": (u"已清空" if ok else u"清空失败（检查文件权限）")})


_CLIENTLOG_WIN_SEC = 60.0        # 滑动窗口
_CLIENTLOG_MAX_PER_IP = 20       # S4：单 IP 每分钟最多这么多条，超出返回 429
_clientlog_hits = {}             # ip -> [ts, ts, ...]
_clientlog_lock = __import__("threading").Lock()


@app.route("/api/clientlog", methods=["POST"])
def client_log():
    """前端 JS 错误上报（现场排障用）：打印到服务日志。
    S4：按客户端 IP 做滑窗限流，防止被灌日志把磁盘打满。"""
    ip = request.headers.get("X-Forwarded-For", request.remote_addr) or u"unknown"
    ip = (ip.split(",")[0] or u"unknown").strip()
    now = time.time()
    with _clientlog_lock:
        bucket = _clientlog_hits.setdefault(ip, [])
        # 剔除窗口外的旧记录
        cutoff = now - _CLIENTLOG_WIN_SEC
        if bucket and bucket[0] < cutoff:
            _clientlog_hits[ip] = [t for t in bucket if t >= cutoff]
            bucket = _clientlog_hits[ip]
        if len(bucket) >= _CLIENTLOG_MAX_PER_IP:
            return jsonify({"ok": False, "error": u"rate_limited"}), 429
        bucket.append(now)
    data = request.get_json(silent=True) or {}
    log.warning(u"client-error: %s | %s %s | %s",
                data.get(u"message"), data.get(u"source"),
                data.get(u"lineno"), data.get(u"ua"))
    return jsonify({"ok": True})


@app.route("/api/conversations")
def conversations():
    with store.gateway.db() as conn:
        rows = conn.execute(
            "SELECT id, title, created_at FROM conversations "
            "ORDER BY id DESC LIMIT 100").fetchall()
    return jsonify({"items": [{"id": str(r["id"]), "name": r["title"],
                       "created_at": r["created_at"]} for r in rows]})


@app.route("/api/conversations/<cid>", methods=["DELETE"])
def delete_conversation(cid):
    with store.gateway.db() as conn:
        conn.execute("DELETE FROM messages WHERE conv_id = ?", (cid,))
        conn.execute("DELETE FROM conversations WHERE id = ?", (cid,))
        conn.execute("DELETE FROM session_state WHERE conv_id = ?", (cid,))
    return jsonify({"ok": True})


@app.route("/api/messages")
def messages():
    cid = request.args.get("conversation_id", u"")
    if not cid:
        return jsonify({"error": u"conversation_id 不能为空"}), 400
    with store.gateway.db() as conn:
        rows = conn.execute(
            "SELECT role, content, graph FROM messages "
            "WHERE conv_id = ? ORDER BY id", (cid,)).fetchall()
    items = []
    for r in rows:
        item = {
            "role": r["role"],
            "query": r["content"] if r["role"] == "user" else u"",
            "answer": r["content"] if r["role"] == "assistant" else u"",
            "graph": None,
        }
        if r["graph"]:
            try:
                item["graph"] = json.loads(r["graph"])
            except ValueError:
                item["graph"] = None
        items.append(item)
    return jsonify({"items": items})


# ---------------- 会话上下文与图工具接口（外部调试图库用） ----------------
@app.route("/sessions/<cid>/context")
def session_context(cid):
    """读取历史对话（最近 HISTORY_MAX 条）+ 业务状态"""
    ctx = store.get_context(cid)
    ctx["conversation_id"] = cid
    return jsonify(ctx)


@app.route("/sessions/<cid>/turns", methods=["POST"])
def session_turn(cid):
    """保存业务状态（候选列表/已选定业务等）；state 传 null 清空。"""
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": u"请求体须为 JSON 对象"}), 400
    state = data.get("state")
    if state is not None and not isinstance(state, (dict, list)):
        return jsonify({"error": u"state 须为 JSON 对象/数组"}), 400
    store.save_state(cid, state)
    return jsonify({"ok": True, "conversation_id": cid,
                    "state_saved": state is not None})


@app.route("/graph/schema")
def graph_schema():
    """返回图结构 + 查询说明"""
    if not gw.GALAXY_BASE:
        return jsonify({"error": u"未配置 GALAXY_BASE（Galaxybase 地址），"
                                 u"无法读取图结构"}), 503
    data, err = gw.build_schema()
    if err:
        return jsonify({"error": err}), 502
    return jsonify(data)


@app.route("/graph/business/search", methods=["POST"])
def graph_business_search():
    """按业务关键词搜索候选业务。入参：{"keyword": "...", "filters": {"属性": "值"}}"""
    if not gw.GALAXY_BASE:
        return jsonify({"error": u"未配置 GALAXY_BASE，无法搜索"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": u"请求体须为 JSON 对象"}), 400
    keyword = (data.get("keyword") or u"").strip()
    if not keyword:
        return jsonify({"error": u"keyword 不能为空"}), 400
    filters = data.get("filters") or {}
    if not isinstance(filters, dict):
        filters = {}
    candidates = store.business_search(keyword, filters)
    return jsonify({"keyword": keyword, "count": len(candidates),
                    "candidates": candidates})


@app.route("/graph/query", methods=["POST"])
def graph_query():
    """只读执行 Cypher。入参：{"cypher": "MATCH ...", "parameters": {...}}；
    返回证据（原始行），失败 / 被截断明确区分。"""
    if not gw.GALAXY_BASE:
        return jsonify({"error": u"未配置 GALAXY_BASE，无法执行查询"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": u"请求体须为 JSON 对象"}), 400
    cypher = (data.get("cypher") or u"").strip()
    if not cypher:
        return jsonify({"error": u"cypher 不能为空"}), 400
    if gw.CYPHER_WRITE_RE.search(cypher):
        return jsonify({"success": False, "error_type": "forbidden",
                        "error": u"查询被拒绝：只允许只读查询（禁止 CREATE/DELETE/"
                                 u"SET/MERGE/REMOVE/DROP/CALL 等写操作）"}), 403
    result = store.graph_query(cypher, data.get("parameters") or {},
                               validate=False, stage=u"工作台·自由查询")
    if not result.get("success"):
        return jsonify(result), 502
    result["truncated"] = result.get("count", 0) >= config.QUERY_LIMIT
    if (data.get("mode") or u"").strip().lower() == u"graph":
        # 工作台可视化模式：识别行结构能否画图（成对列/证据形状）
        result.update(store.rows_to_graph(result.get("rows"),
                                          result.get("columns")))
    return jsonify(result)


@app.route("/graph/neighbors", methods=["POST"])
def graph_neighbors():
    """工作台邻接展开。入参：{"type": "业务事项", "id": "...", "limit": 100}；
    返回归一化图结构（nodes/edges，复合 ID "Label:pk值"）。"""
    if not gw.GALAXY_BASE:
        return jsonify({"error": u"未配置 GALAXY_BASE，无法查询"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": u"请求体须为 JSON 对象"}), 400
    entity_type = (data.get("type") or u"").strip()
    entity_id = data.get("id")
    if not entity_type or entity_id is None or not u"{}".format(entity_id).strip():
        return jsonify({"error": u"type 和 id 不能为空"}), 400
    result = store.neighbors_graph(entity_type, entity_id, data.get("limit"))
    if not result.get("success"):
        return jsonify(result), 502
    return jsonify(result)


@app.route("/graph/node", methods=["POST"])
def graph_node():
    """工作台节点属性详情。入参：{"type": "业务事项", "id": "..."}。"""
    if not gw.GALAXY_BASE:
        return jsonify({"error": u"未配置 GALAXY_BASE，无法查询"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": u"请求体须为 JSON 对象"}), 400
    entity_type = (data.get("type") or u"").strip()
    entity_id = data.get("id")
    if not entity_type or entity_id is None or not u"{}".format(entity_id).strip():
        return jsonify({"error": u"type 和 id 不能为空"}), 400
    result = store.node_props(entity_type, entity_id)
    if not result.get("success"):
        return jsonify(result), 502
    return jsonify(result)


# ---------------- 报告导出 Word（.doc = Word 兼容 HTML，离线零依赖） ----------------
def _md_escape(s):
    return (s or u"").replace(u"&", u"&amp;").replace(u"<", u"&lt;") \
        .replace(u">", u"&gt;")


def _md_inline(s):
    s = _md_escape(s)
    s = re.sub(u"\\*\\*(.+?)\\*\\*", u"<b>\\1</b>", s)
    s = re.sub(u"`(.+?)`", u"<code>\\1</code>", s)
    return s


def _md_to_html(text):
    """报告 markdown → Word 兼容 HTML（覆盖报告用到的子集：标题/表格/
    列表/加粗/代码/分隔线/段落），离线环境无 markdown 库，自实现。"""
    lines = (text or u"").split(u"\n")
    out, i = [], 0
    while i < len(lines):
        ln = lines[i].rstrip()
        if not ln.strip():
            i += 1
            continue
        # 表格块
        if ln.lstrip().startswith(u"|"):
            cells_rows = []
            while i < len(lines) and lines[i].lstrip().startswith(u"|"):
                r = lines[i].strip()
                i += 1
                if re.match(u"^\\|[\\s:\\-|]+\\|$", r):
                    continue    # 分隔行 |---|---|
                cells_rows.append([c.strip() for c in r.strip(u"|").split(u"|")])
            if cells_rows:
                parts = [u"<table>"]
                for ri, crow in enumerate(cells_rows):
                    tag = u"th" if ri == 0 else u"td"
                    parts.append(u"<tr>" + u"".join(
                        u"<{0}>{1}</{0}>".format(tag, _md_inline(c))
                        for c in crow) + u"</tr>")
                parts.append(u"</table>")
                out.append(u"".join(parts))
            continue
        # 标题
        m = re.match(u"^(#{1,5})\\s+(.*)$", ln)
        if m:
            out.append(u"<h{0}>{1}</h{0}>".format(
                min(len(m.group(1)) + 1, 5), _md_inline(m.group(2))))
            i += 1
            continue
        # 无序列表
        if re.match(u"^\\s*[-*]\\s+", ln):
            items = []
            while i < len(lines) and re.match(u"^\\s*[-*]\\s+", lines[i]):
                items.append(u"<li>" + _md_inline(
                    re.sub(u"^\\s*[-*]\\s+", u"", lines[i])) + u"</li>")
                i += 1
            out.append(u"<ul>" + u"".join(items) + u"</ul>")
            continue
        # 有序列表
        if re.match(u"^\\s*\\d+[.、]\\s+", ln):
            items = []
            while i < len(lines) and re.match(u"^\\s*\\d+[.、]\\s+", lines[i]):
                items.append(u"<li>" + _md_inline(
                    re.sub(u"^\\s*\\d+[.、]\\s+", u"", lines[i])) + u"</li>")
                i += 1
            out.append(u"<ol>" + u"".join(items) + u"</ol>")
            continue
        # 分隔线
        if re.match(u"^\\s*-{3,}\\s*$", ln):
            out.append(u"<hr/>")
            i += 1
            continue
        # 段落（合并连续普通行）
        para = [ln.strip()]
        i += 1
        while i < len(lines) and lines[i].strip() and not re.match(
                u"^(#|\\||\\s*[-*]\\s|\\s*\\d+[.、]|\\s*-{3,}\\s*$)", lines[i]):
            para.append(lines[i].strip())
            i += 1
        out.append(u"<p>" + u"<br/>".join(_md_inline(x) for x in para) + u"</p>")
    return u"".join(out)


@app.route("/api/export/word", methods=["POST"])
def export_word():
    """把分析报告 markdown 导出为 .doc（Word 兼容 HTML：WPS/Word 直接打开
    编辑，表格/标题/加粗均保留；离线环境无 python-docx，此方案零依赖）。
    入参 {"markdown": "...", "name": "可选文件名前缀"}（JSON 或表单 POST 均可，
    表单方式供前端原生 form.submit 下载，避免 blob 兼容问题）。"""
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = {k: v for k, v in request.form.items()}
    md_text = data.get("markdown") or u""
    if not md_text.strip():
        return jsonify({"error": u"报告内容为空"}), 400
    m = re.search(u"^#\\s+(.+)$", md_text, re.M)
    title = (data.get("name") or (m.group(1).strip() if m else u"")).strip()
    fname = u"{}.doc".format(title[:40] if title else u"分析报告")
    html = (u"<!DOCTYPE html><html><head><meta charset='utf-8'>"
            u"<style>body{font-family:'微软雅黑','Microsoft YaHei';font-size:11pt;"
            u"line-height:1.7;color:#222}h1{font-size:16pt}h2{font-size:13.5pt}"
            u"h3{font-size:12pt}table{border-collapse:collapse;width:100%;margin:8pt 0}"
            u"th,td{border:1px solid #999;padding:4pt 8pt;font-size:10.5pt}"
            u"th{background:#eef0fb}code{font-family:Consolas;font-size:10pt}"
            u"li{margin:2pt 0}</style></head><body>"
            + _md_to_html(md_text) + u"</body></html>")
    quoted = urllib_quote(fname.encode("utf-8"))
    return Response(html, mimetype=u"application/msword", headers={
        u"Content-Disposition":
            u"attachment; filename=\"report.doc\"; filename*=UTF-8''{}".format(quoted)})


@app.route("/graph/cypher/generate", methods=["POST"])
def cypher_generate():
    """工作台 Cypher 生成：按用户自然语言描述 + 图结构生成只读查询语句
    （只生成不执行，用户复制到查询框自行执行）。参数内联成字面量，
    复制即可运行。入参 {"question": "..."}；返回 {"cypher": "..."}。"""
    if not gw.GALAXY_BASE:
        return jsonify({"success": False, "error": u"未配置 GALAXY_BASE"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": u"请求体须为 JSON 对象"}), 400
    question = (data.get("question") or u"").strip()
    if not question:
        return jsonify({"success": False, "error": u"请先输入要生成查询的描述"}), 400
    ctx = u"【图结构】\n{}\n【用户问题】{}".format(store.schema_text(), question)
    r = pipeline._extract_json(
        llm.llm_complete(prompts.adhoc_cypher_prompt(), ctx,
                         agent_code=config.get("SFM_ADHOC_AGENT_CODE") or None,
                         timeout=(10, config.get_int("LLM_TIMEOUT_ADHOC_READ", 300))))
    cypher = u""
    params = None
    tpl_name = pipeline._text(r.get("template")).strip()
    if tpl_name:
        # 模板选单填槽优先：模型只"选题+填参"，弱算力下确定性最高；
        # 失败（选单未命中/实例化错）自动回退自由生成
        tpl = pipeline.adhoc_template_by_name(tpl_name)
        if tpl:
            mp = r.get("params")
            cypher, params, _terr = pipeline._build_template_cypher(
                tpl, question, mp if isinstance(mp, dict) else None)
    if not cypher:
        cypher = pipeline._text(r.get("cypher")).strip()
        rp = r.get("parameters")
        if isinstance(rp, dict):
            params = rp
        if cypher:
            # 执行前同类确定性修复（此处执行者是用户本人，修不了的报精确错误）
            cypher, _repairs, rerr = pipeline._repair_cypher(cypher)
            if rerr:
                return jsonify({"success": False, "error": rerr}), 422
    if not cypher:
        return jsonify({"success": False,
                        "error": u"未能生成有效 Cypher，请换个描述试试"}), 502
    if gw.CYPHER_WRITE_RE.search(cypher):
        return jsonify({"success": False,
                        "error": u"生成的语句包含写操作，已被拒绝，请换个描述"}), 400
    # 参数内联成字面量（用户复制后可直接执行）；py2 下 LLM 返回的是 unicode
    if not isinstance(params, dict):
        params = r.get("parameters")
    if isinstance(params, dict):
        for k, v in params.items():
            is_str = isinstance(v, str) or (
                sys.version_info[0] == 2 and isinstance(v, unicode))
            lit = u"'{}'".format(v) if is_str else u"{}".format(v)
            cypher = cypher.replace(u"${}".format(k), lit)
    return jsonify({"success": True, "cypher": cypher})


@app.route("/graph/analyze", methods=["POST"])
def graph_analyze():
    """工作台节点智能分析：对该节点跑影响分析（确定性证据查询 + 分桶预计算
    + LLM 排版报告），复用 pipeline.stream_analysis，不写会话状态。
    入参：{"type": "业务事项", "id": "...", "name": "..."}；返回 {"report": 报告文本}。"""
    if not gw.GALAXY_BASE:
        return jsonify({"success": False, "error": u"未配置 GALAXY_BASE，无法分析"}), 503
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": u"请求体须为 JSON 对象"}), 400
    entity_type = (data.get("type") or u"").strip()
    entity_id = data.get("id")
    if not entity_type or entity_id is None or not u"{}".format(entity_id).strip():
        return jsonify({"success": False, "error": u"type 和 id 不能为空"}), 400
    selected = {"type": entity_type, "id": entity_id,
                "name": u"{}".format(data.get("name") or u"").strip()}
    query = u"分析「{}」发生变更时的影响范围".format(selected["name"] or entity_id)
    parts = []
    try:
        for item in pipeline.stream_analysis(query, {"change_desc": u""}, selected):
            if isinstance(item, tuple):
                kind, content = item
            else:
                kind, content = "text", item
            if kind == "text":
                parts.append(content)
            elif kind == "error":
                return jsonify({"success": False,
                                "error": content}), 502
    except Exception as e:
        return jsonify({"success": False, "error": u"分析出错：{}".format(e)}), 502
    report = u"".join(parts).strip()
    if not report:
        return jsonify({"success": False,
                        "error": u"未能生成分析报告（LLM 未返回内容）"}), 502
    return jsonify({"success": True, "report": report})


@app.route("/metrics", methods=["GET"])
def metrics_endpoint():
    """O2：进程内计数器与耗时直方图。JSON 格式，排障用。"""
    import metrics as _m
    return jsonify(_m.snapshot())


if __name__ == "__main__":
    log.info(u"orchestrator: http://0.0.0.0:%s (LLM platform: %s)",
             config.PORT, config.LLM_PLATFORM)
    app.run(host="0.0.0.0", port=config.PORT, threaded=True)
