# -*- coding: utf-8 -*-
"""图客户端与本地存储层（由旧根目录网关 app.py 融合而来）。

提供：
    本地会话存储    db() / init_db()（SQLite history.db，模块所在目录）
    think 过滤      make_think_filter()（流式剥离 <think>...</think> 状态机）
    Galaxybase 客户端  RSA 登录（纯标准库实现）/ GraphConnection 连接上下文 /
                    graph_run / gb_get_schema / collect_graph_metadata
    图结构          load_schema_notes() / build_schema()（getSchema > Cypher 扫描
                    > schema_notes.json 人工配置，按 连接键+notes指纹 缓存）

连接配置在下方配置区（GALAXY_*），可被 settings.py 运行时覆盖；扫描/绑定等
跨图操作使用显式 GraphConnection 上下文，不再临时改写全局变量（防并发串图）。
兼容 Python 2.7 与 3.x。
"""
from __future__ import print_function

import base64
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time

import requests

try:
    unicode    # py2
except NameError:
    unicode = str   # py3

# ====================== 配置区（直接改这里） ======================
# ------- 图数据库（Galaxybase REST 接口，留空则图相关接口返回未配置） -------
# GALAXY_BASE 填 Galaxybase REST 地址（默认端口 18088），如 http://127.0.0.1:18088
GALAXY_BASE = "http://127.0.0.1:18088"
GALAXY_USER = "admin"
GALAXY_PASS = ""               # 登录密码（真实值放 runtime_config.json，不入库）
GALAXY_PASS_ENCRYPTED = False  # 密码已用官方加密工具处理过则改 True，直接提交
# 登录加密用 RSA 公钥（base64 或 PEM），取自加密工具包 login_password_rsa_public_key.pem；
# 留空使用 REST 使用说明文档中的内置公钥
GALAXY_RSA_KEY = ""
GALAXY_GRAPH = u"业务底账12类图"  # Cypher 执行的图名称（graphName）
GRAPH_TIMEOUT = 30        # 图查询 HTTP 超时（秒）
# 1800s：schema 稳定少变（换图/改配置时 settings.apply 会主动清缓存），
# 缓存太短会导致每轮都全类型重扫一次
SCHEMA_CACHE_TTL = 1800   # 元数据接口内存缓存秒数
# ================================================================

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.db")


# ---------------- 连接上下文（V4.1：显式传参替代临时改写全局变量） ----------------
class GraphConnection(object):
    """图连接上下文：base_url / graph_name / 认证信息 / 超时的显式载体。

    扫描、绑定、连通性测试等跨图操作显式传入本对象，函数内部不再读全局
    GALAXY_*，杜绝"临时 setattr 改全局 + try/finally 恢复"在并发下串图；
    connection_key 用于 token 与 schema 缓存的图级隔离（密码不进 key 明文，
    以其 md5 摘要参与，密码改动即换缓存）。"""

    def __init__(self, base_url=None, graph_name=None, user=None,
                 password=None, rsa_key=None, pass_encrypted=None,
                 timeout=None):
        self.base_url = _u(base_url if base_url else GALAXY_BASE).strip() \
            or GALAXY_BASE
        self.graph_name = _u(graph_name if graph_name is not None
                             else GALAXY_GRAPH).strip()
        self.user = _u(user if user is not None else GALAXY_USER).strip()
        self.password = password if password else GALAXY_PASS
        self.rsa_key = rsa_key if rsa_key else GALAXY_RSA_KEY
        if pass_encrypted is None:
            self.pass_encrypted = GALAXY_PASS_ENCRYPTED
        else:
            self.pass_encrypted = bool(pass_encrypted)
        try:
            self.timeout = int(timeout) if timeout else GRAPH_TIMEOUT
        except (TypeError, ValueError):
            self.timeout = GRAPH_TIMEOUT

    def connection_key(self):
        """连接指纹：同连接共用 token/缓存，不同图/地址/账号/密码互不污染。
        不含明文密码（md5 摘要前 8 位参与）。"""
        pw = hashlib.md5((self.password or u"").encode("utf-8")).hexdigest()[:8]
        raw = u"|".join([self.base_url, self.graph_name, self.user,
                         u"k1" if self.rsa_key else u"k0",
                         u"e1" if self.pass_encrypted else u"e0", pw])
        return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

    def describe(self):
        """日志/前端展示用摘要（不含密码）。"""
        return u"{}@{} 图「{}」".format(self.user, self.base_url,
                                        self.graph_name)


def default_connection():
    """默认连接：在调用时读取模块全局配置（settings.apply 热更新后的值）。
    仅常规查询链路使用；跨图扫描/绑定必须显式构造 GraphConnection。"""
    return GraphConnection()


# ---------------- 本地会话存储 ----------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS conversations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT DEFAULT '新对话',
            created_at REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conv_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at REAL)""")
        # 平台侧会话标识（如 SFM 平台 createSession 返回的 uniqueCode，旧链路遗留列）
        try:
            conn.execute("ALTER TABLE conversations ADD COLUMN platform_session TEXT")
        except sqlite3.OperationalError:
            pass
        # 聊天回答附带的图谱卡片（{"entity":{type,id,name}}，JSON；工作台"展开图谱"用）
        try:
            conn.execute("ALTER TABLE messages ADD COLUMN graph TEXT")
        except sqlite3.OperationalError:
            pass
        # 多轮业务状态（候选业务、已选定业务等，JSON，由 save_state / /sessions/{id}/turns 写入）
        conn.execute("""CREATE TABLE IF NOT EXISTS session_state(
            conv_id TEXT PRIMARY KEY,
            state TEXT,
            updated_at REAL)""")
        # V4.1 多轮写序控制（§13.4）：请求开始递增，回写按 expected_turn 条件更新，
        # 防止旧请求晚完成覆盖新请求的会话状态
        try:
            conn.execute("ALTER TABLE session_state ADD COLUMN turn_seq INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass


init_db()


# ---------------- think 过滤 ----------------
THINK_RE = re.compile(u"<think>.*?</think>", re.S)
THINK_OPEN, THINK_CLOSE = u"<think>", u"</think>"


def make_think_filter():
    """流式剥离 <think>...</think>：标签可能被拆在多个流式块中，
    用状态机 + 尾部保留处理。返回 (feed, flush)。"""
    state = {"in_think": False, "buf": u""}

    def feed(s):
        state["buf"] += s
        out = []
        while True:
            buf = state["buf"]
            if state["in_think"]:
                idx = buf.find(THINK_CLOSE)
                if idx >= 0:
                    state["buf"] = buf[idx + len(THINK_CLOSE):]
                    state["in_think"] = False
                    continue
                # 思考内容丢弃，但保留"最后一个 < 起"的尾部（可能是半个 </think>）
                lt = buf.rfind(u"<")
                state["buf"] = buf[lt:] if lt >= 0 else u""
            else:
                idx = buf.find(THINK_OPEN)
                if idx >= 0:
                    if idx:
                        out.append(buf[:idx])
                    state["buf"] = buf[idx + len(THINK_OPEN):]
                    state["in_think"] = True
                    continue
                # 以"最后一个 <"为分界：前面全是安全内容立即输出，
                # 从 < 起才可能是未完成的 <think>。相较固定长度切割能更早输出，
                # 并避免 "answer<th" 这种末尾恰好一个 < 时整句被阻塞。
                lt = buf.rfind(u"<")
                if lt < 0:
                    if buf:
                        out.append(buf)
                        state["buf"] = u""
                elif lt > 0:
                    out.append(buf[:lt])
                    state["buf"] = buf[lt:]
            break
        return u"".join(out)

    def flush():
        s = state["buf"]
        state["buf"] = u""
        return u"" if state["in_think"] else s

    return feed, flush


# ---------------- 图结构说明与缓存 ----------------
SCHEMA_NOTES_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_NOTES_PATH = os.path.join(SCHEMA_NOTES_DIR, "schema_notes.json")

# 核心语义（跨图复用的"业务范式"参数）：代码不再写死 业务事项/业务依赖 等概念，
# 全部从这里读；换图时在 schema_notes 的 core_semantics 段覆盖即可。
DEFAULT_CORE_SEMANTICS = {
    u"anchor_label": u"业务事项",        # 锚定实体类型（候选搜索/归并/依赖传播中心）
    u"dependency_enabled": True,          # 图中没有明确依赖边时可显式关闭传播推理
    u"dependency_rel": u"业务依赖",      # 影响传播关系（A -[rel]-> B = A 依赖 B）
    u"dependency_depth": 6,              # 依赖传播变长路径深度
    u"hosting_rel": u"承载于",           # 业务 -> 承载系统 关系
    u"system_label": u"信息系统",        # 承载系统实体类型
    u"name_prop": u"名称",               # 通用名称属性
    u"rollup_exclude_rels": [u"业务依赖", u"子项依赖", u"协同事项",
                             u"协同发起", u"协同参与"],   # 归属链排除的协作关系
    u"candidate_labels": [u"业务事项", u"业务子项"],     # 候选搜索限定类型
    u"status_prop": u"办理状态",         # 候选列表展示的状态属性（仅展示用，
                                          # 不拼 Cypher；图里没有则不显示状态）
}

# 图名转文件名安全字符（保留中文/字母/数字/下划线/连字符）
_FILE_SAFE_RE = re.compile(u"[^0-9A-Za-z_一-龥-]+")

# V4.1：缓存全部按图隔离——
#   _token_cache[connection_key] = token
#   _schema_cache[connection_key + notes指纹] = {"ts":…, "data":…}
# A 图分析、B 图扫描互不串缓存；clear_runtime_caches() 一次性全清。
_schema_cache = {}
_token_cache = {}
_notes_cache = {"key": None, "data": None}
# C3：读写 cache 的互斥锁。整体字典重赋 vs 另一线程中间读写，会出现写丢失；
#     锁 + 字段级 clear-in-place 保证引用稳定、读写不交错。
_token_lock = threading.Lock()
_schema_lock = threading.Lock()


def clear_runtime_caches():
    """清空全部图相关缓存（token/schema/notes）。settings.apply 换配置、
    schema 配置发布/回滚后调用，保证立即生效且不残留旧图数据。"""
    with _token_lock:
        _token_cache.clear()
    with _schema_lock:
        _schema_cache.clear()
    _notes_cache["key"] = None
    _notes_cache["data"] = None

# REST 使用说明文档中的 RSA 公钥（若解析失败请把 pem 文件内容填到 GALAXY_RSA_KEY）
GALAXY_DOC_RSA_KEY = (
    "MIGfMA0GCSaGSIb3DQEBAQUAA4GNADCBiQKBgQCXgctF+noMbOXv5c8hVef4vE6eK/mwnzEp"
    "MoHpZJjphuyTcowsD9DBWe+PrqZOEm1PomzD/TxOn9eMhn9O+w3yuuv8fipMs6OjU6Y+rKLrGJ8aCZpzd7Y5eewQS/0hOGmWtQlLd"
    "ayJaUT0B0Fpz3yhR7u7vtVhKwbCYYfHxhg1PQIDAQAB")

# 只读校验：拒绝一切写操作和存储过程调用（内部元数据扫描不经过此校验）
# USING/START/CYPHER 伪指令与 com.galaxybase.* 过程一起拉黑，避免只读账号被绕过。
CYPHER_WRITE_RE = re.compile(
    r"(\b(CREATE|DELETE|DETACH|SET|MERGE|REMOVE|DROP|LOAD\s+CSV|FOREACH|CALL"
    r"|USING|START|CYPHER)\b"
    r"|\bcom\.galaxybase\.[A-Za-z0-9_.]+)",
    re.I)
# LIMIT 识别：先剥掉行注释（-- …）和块注释（/* … */）以及字符串字面量，
# 再查 \bLIMIT\b，避免注释/字符串里的 "LIMIT" 让强制追加被跳过（B2）。
_CYPHER_LIMIT_WORD_RE = re.compile(r"\bLIMIT\b", re.I)
_CYPHER_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_CYPHER_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)
_CYPHER_STRING_LIT_RE = re.compile(r"'(?:\\.|[^'\\])*'")


class _CypherLimitChecker(object):
    def search(self, text):
        s = _CYPHER_LINE_COMMENT_RE.sub(u"", text or u"")
        s = _CYPHER_BLOCK_COMMENT_RE.sub(u"", s)
        s = _CYPHER_STRING_LIT_RE.sub(u"''", s)
        return _CYPHER_LIMIT_WORD_RE.search(s)


CYPHER_LIMIT_RE = _CypherLimitChecker()

# schema_notes 里的标签/属性/关系名会被反引号包进 Cypher。若字符串本身含反引号、
# 引号、分号或换行，就能逃逸出 `..` 闭合去拼接任意语句。此处作为深度防御，
# 拒绝一切含 Cypher 元字符或控制字符的标识符。
_IDENT_FORBID_RE = re.compile(u"[`'\";\\\\\\s]")


def safe_ident(name):
    """校验从 schema_notes 读取的图模型标识符（标签/属性/关系），
    返回安全的字符串；若含 Cypher 元字符或为空，返回 u"" 由上层回退默认。"""
    s = u"" if name is None else u"{}".format(name).strip()
    if not s or _IDENT_FORBID_RE.search(s):
        return u""
    return s


def _u(v):
    return u"" if v is None else u"{}".format(v)


def notes_path_for(graph=None):
    """图专属配置路径 schema_notes.<图名>.json（可能不存在，供绑定/生成用）"""
    g = _FILE_SAFE_RE.sub(u"_", _u(graph or GALAXY_GRAPH)).strip(u"_")
    if not g:
        return None
    return os.path.join(SCHEMA_NOTES_DIR, u"schema_notes.{}.json".format(g))


def load_schema_notes():
    """读取当前图的结构说明：优先 schema_notes.<图名>.json（换图绑定），
    不存在则用默认 schema_notes.json。缓存键含 mtime+文件大小（整数秒 mtime
    会漏掉同一秒内的连续修改，V4.1 修正），改文件即时生效。"""
    path = notes_path_for()
    if not path or not os.path.exists(path):
        path = DEFAULT_NOTES_PATH
    try:
        key = u"{}@{}:{}".format(path, os.path.getmtime(path),
                                 os.path.getsize(path))
    except OSError:
        key = path
    if _notes_cache["key"] == key:
        return _notes_cache["data"]
    try:
        with io.open(path, encoding="utf-8") as f:
            notes = json.load(f)
        notes = notes if isinstance(notes, dict) else {}
    except (IOError, OSError, ValueError):
        notes = {}
    _notes_cache["key"] = key
    _notes_cache["data"] = notes
    return notes


def core_semantics():
    """当前图核心语义 = DEFAULT_CORE_SEMANTICS 被 schema_notes.core_semantics 覆盖。
    会用 safe_ident 过滤被拼进 Cypher 的标识符字段（标签/关系/属性名），
    含 Cypher 元字符的值会回退到默认值，杜绝经 schema_notes 的二次注入。"""
    sem = dict(DEFAULT_CORE_SEMANTICS)
    ov = load_schema_notes().get("core_semantics")
    if isinstance(ov, dict):
        for k, v in ov.items():
            if v is not None and v != u"":
                sem[k] = v
    # 字符串标识符字段：逐个过滤，被拒绝的回退到默认值
    for k in (u"anchor_label", u"dependency_rel", u"hosting_rel",
              u"system_label", u"name_prop"):
        if sem.get(k):
            cleaned = safe_ident(sem[k])
            sem[k] = cleaned if cleaned else DEFAULT_CORE_SEMANTICS.get(k, u"")
    # 列表字段：逐元素过滤
    for k in (u"rollup_exclude_rels", u"candidate_labels"):
        src = sem.get(k) or []
        if isinstance(src, list):
            sem[k] = [x for x in (safe_ident(v) for v in src) if x]
        else:
            sem[k] = DEFAULT_CORE_SEMANTICS.get(k, [])
    # dependency_rel 的空值过去会回退成默认“业务依赖”，导致没有依赖边的图
    # 仍然执行虚假的传播推理。用独立开关表达“此图没有依赖语义”。
    if sem.get("dependency_enabled") is False:
        sem["dependency_rel"] = u""
    return sem


# ---------------- 换图绑定：schema_notes 骨架自动生成 ----------------
def guess_core_semantics(schema):
    """从 build_schema 结果启发式猜核心语义（生成骨架用，需人工校对）。
    返回 (sem, warnings)。

    关键启发式：
    - 锚定类型：优先按命名习惯（业务/事项/流程/活动/服务），同时考虑入边种类数
      （入边 label 多的类型通常是真正枢纽，如"业务子项"入边 5 种 > "业务事项"入边 0 种）
    - 依赖传播关系：仅当名字含「依赖/引用/调用」且【不是多态关系】（两端 label 多样）
      才视为依赖；多态结构关系（如"包括"跨 6 对端点）禁止判定为依赖
    - name_prop：优先 displayName / 常见"名/name"属性；允许单属性顶点"""
    nodes = schema.get("node_types") or []
    rels = schema.get("relationship_types") or []
    labels = [_u(n.get("name")) for n in nodes if n.get("name")]
    relnames = [_u(r.get("name")) for r in rels if r.get("name")]
    warnings = []

    def pick(items, patterns, what, must=True):
        for pat in patterns:
            for it in items:
                if pat in it:
                    return it
        if items:
            warnings.append(u"{}：未按习惯命名匹配到，暂取「{}」，请校对".format(what, items[0]))
            return items[0]
        if must:
            warnings.append(u"{}：图中未找到可选项".format(what))
        return u""

    # 各类型的关系连接度（出现在关系两端类型中的次数）：锚定类型通常是图的枢纽
    degree = {}
    in_rel_kinds = {}
    out_rel_kinds = {}
    pairs_of = {}
    for r in rels:
        rn = _u(r.get("name"))
        prs = [_u(p) for p in (r.get("pairs") or [])]
        pairs_of[rn] = prs
        for p in prs:
            if u"->" not in p:
                continue
            src, dst = p.split(u"->", 1)
            src = src.strip()
            dst = dst.strip()
            if src:
                degree[src] = degree.get(src, 0) + 1
                out_rel_kinds.setdefault(src, set()).add(rn)
            if dst:
                degree[dst] = degree.get(dst, 0) + 1
                in_rel_kinds.setdefault(dst, set()).add(rn)

    # 多态关系检测：若一条关系跨 2+ 对不同端点类型，视为结构多态关系，不作依赖候选
    polymorphic_rels = set()
    for rn, prs in pairs_of.items():
        endpoint_pairs = set()
        for p in prs:
            if u"->" in p:
                endpoint_pairs.add(tuple(x.strip() for x in p.split(u"->", 1)))
        if len(endpoint_pairs) >= 2:
            polymorphic_rels.add(rn)

    # 锚定类型打分：命名匹配 + 入边关系种类数（枢纽特征）+ 出度/入度（备用）
    anchor = u""
    cands = []
    for pat in (u"业务", u"事项", u"流程", u"活动", u"服务"):
        cands = [lb for lb in labels if pat in lb]
        if cands:
            anchor = max(cands,
                         key=lambda lb: (len(in_rel_kinds.get(lb, set())),
                                         degree.get(lb, 0)))
            break
    if not anchor and labels:
        anchor = max(labels,
                     key=lambda lb: (len(in_rel_kinds.get(lb, set())),
                                     degree.get(lb, 0)))
        warnings.append(u"锚定类型：未匹配到业务类命名，按入边种类数+连接度取「{}」，请校对".format(anchor))

    # 依赖关系：名字含依赖语义 且 不是多态关系；优先两端都是锚定类型
    anchor_pair = u"{}->{}".format(anchor, anchor)
    dep = u""
    for rn in relnames:
        if rn in polymorphic_rels:
            continue
        if any(k in rn for k in (u"依赖", u"引用", u"调用")) \
                and anchor_pair in pairs_of.get(rn, []):
            dep = rn
            break
    if not dep:
        dep = next((rn for rn in relnames
                    if rn not in polymorphic_rels
                    and any(k in rn for k in (u"依赖", u"引用", u"调用"))),
                   u"")
    dependency_enabled = bool(dep)
    if not dependency_enabled:
        warnings.append(u"依赖传播关系：未找到名称含“依赖/引用/调用”的非多态关系，"
                        u"已禁用依赖传播；结构关系仍可用于关联分析")
    hosting = pick(relnames, (u"承载", u"部署", u"运行", u"落户"), u"承载关系", must=False)
    system = pick(labels, (u"系统", u"平台", u"应用"), u"承载系统类型", must=False)

    # name_prop：优先锚定类型的 displayName；否则扫全部顶点
    name_prop = u""
    for n in nodes:
        if _u(n.get("name")) == anchor:
            dn = _u(n.get("display_name") or u"")
            if dn:
                name_prop = dn
                break
    if not name_prop:
        for n in nodes:
            dn = _u(n.get("display_name") or u"")
            if dn:
                name_prop = dn
                break
    if not name_prop:
        for n in nodes:
            for p in (n.get("properties") or []):
                pn = p.get("name") if isinstance(p, dict) else p
                if pn == u"名称":
                    name_prop = u"名称"
                    break
            if name_prop:
                break
    if not name_prop:
        for n in nodes:
            for p in (n.get("properties") or []):
                pn = _u(p.get("name") if isinstance(p, dict) else p)
                if pn and (u"名" in pn or u"name" in pn.lower()):
                    name_prop = pn
                    break
            if name_prop:
                break
    if not name_prop:
        name_prop = u"名称"
        warnings.append(u"名称属性：未找到含「名」的属性，暂用「名称」，请校对")
    excl = []
    for r in [dep] + [x for x in relnames if (u"协同" in x or u"协作" in x)]:
        if r and r not in excl:
            excl.append(r)
    # candidate_labels：以锚为首，再按（入边种类数 + 总连接度）排序追加前 2 名；
    # 保证搜索定位时覆盖主要枢纽（如业务事项/信息系统），不仅仅锁定一个锚
    cand_labels = [anchor] if anchor else []
    ranked = sorted(
        [lb for lb in labels if lb and lb != anchor],
        key=lambda lb: (len(in_rel_kinds.get(lb, set())) + len(out_rel_kinds.get(lb, set())),
                        degree.get(lb, 0)),
        reverse=True)
    for lb in ranked[:2]:
        if lb not in cand_labels:
            cand_labels.append(lb)
    sem = {
        u"anchor_label": anchor,
        u"dependency_enabled": dependency_enabled,
        u"dependency_rel": dep,
        u"dependency_depth": 6,
        u"hosting_rel": hosting,
        u"system_label": system,
        u"name_prop": name_prop,
        u"rollup_exclude_rels": excl,
        u"candidate_labels": cand_labels,
    }
    return sem, warnings


# 看起来像 ID / 编号 / 时间戳的属性名 —— 不作为显名或搜索字段
_ID_LIKE_RE = re.compile(
    u"(^|[_/\\-\\s])(id|uuid|guid|key|code|编号|序号|编码|标识|ID|版本号|时间|日期|date|time)"
    u"([_/\\-\\s]|$)",
    re.IGNORECASE | re.UNICODE)


def _is_id_like(prop):
    """粗判属性名像不像 id/编号/时间戳；只对显名/搜索兜底时用。"""
    if not prop:
        return True
    if prop in (u"id~", u"id", u"ID"):
        return True
    return bool(_ID_LIKE_RE.search(prop))


# 显名候选关键词 Tier 1：真正的"名字"属性（含就一定是显名）
_NAME_HINTS_STRONG = (u"名称", u"name", u"标题", u"title")
# 显名候选关键词 Tier 2：仅在 Tier 1 全找不到时的兜底（组织/事项/服务类）
_NAME_HINTS_WEAK = (u"处室", u"部门", u"岗位", u"角色",
                    u"服务对象", u"对象", u"事项",
                    u"司局", u"组织", u"机构")


def _match_any_hint(prop, hints):
    if not prop:
        return False
    pl = prop.lower()
    return any(h in prop or h.lower() in pl for h in hints)


def _pick_name_prop(props, display_name, id_prop):
    """挑一个最像"显名"的属性作 name_prop：
    1) displayName 命中真实属性；2) 恰为"名称"；
    3) 含强显名关键词（名称/name/标题/title），同类里优先不含"编号"；
    4) 含弱显名关键词（司局/部门/对象…）兜底；
    5) displayName / 第一个非 id 属性 / 字面"名称"兜底。"""
    display_name = display_name or u""
    if display_name and display_name in props and not _is_id_like(display_name):
        return display_name
    if u"名称" in props:
        return u"名称"
    # Tier 1：强显名；多个候选时优先不含"编号/code"的、更简短的
    strong = [p for p in props
              if not _is_id_like(p) and _match_any_hint(p, _NAME_HINTS_STRONG)]
    if strong:
        strong.sort(key=lambda p: (int(u"编号" in p or u"code" in p.lower()),
                                   len(p)))
        return strong[0]
    # Tier 2：弱显名兜底，按关键词具体程度排序（处室/部门 > 司局 > 岗位 等）
    weak_hits = []
    for p in props:
        if _is_id_like(p):
            continue
        for idx, h in enumerate(_NAME_HINTS_WEAK):
            if h in p or h.lower() in p.lower():
                weak_hits.append((idx, p))
                break
    if weak_hits:
        weak_hits.sort(key=lambda x: x[0])
        return weak_hits[0][1]
    if display_name:
        return display_name
    non_id = [p for p in props if p and p != id_prop and not _is_id_like(p)]
    if non_id:
        return non_id[0]
    return u"名称"


def _pick_search_props(props, id_prop, name_prop):
    """挑出所有像文本显名/描述/编号的属性放进 search_props，供 WHERE OR 用。
    排除 id 唯一键，但保留业务编号这类对查询有用的 ID-like 字段。"""
    out = []
    seen = set()
    if name_prop and name_prop in props:
        out.append(name_prop)
        seen.add(name_prop)
    # 二类保留：业务编号/应用名称/描述/简介/类别 等文本型属性
    _TEXT_HINTS = (u"名称", u"name", u"标题", u"title", u"编号", u"code",
                   u"描述", u"简介", u"说明", u"类型", u"类别",
                   u"司局", u"处室", u"部门", u"对象", u"岗位")
    for p in props:
        if not p or p in seen:
            continue
        if p == id_prop or p == u"id~":
            continue
        pl = p.lower()
        hit = any(h in p or h.lower() in pl for h in _TEXT_HINTS)
        if hit:
            out.append(p)
            seen.add(p)
    # 最少保留一个（即使只有一个非 id 属性）
    if not out:
        for p in props:
            if p and p != id_prop and p != u"id~":
                out.append(p)
                break
    return out


def write_notes_skeleton(schema, graph=None):
    """为新图生成 schema_notes.<图名>.json 骨架（已存在则原样读回不覆盖）。
    返回 (path, notes, created, warnings)。"""
    path = notes_path_for(graph)
    if not path:
        return None, {}, False, [u"图名为空，无法生成配置文件"]
    if os.path.exists(path):
        try:
            with io.open(path, encoding="utf-8") as f:
                notes = json.load(f)
            return path, (notes if isinstance(notes, dict) else {}), False, []
        except (IOError, OSError, ValueError):
            pass
    sem, warnings = guess_core_semantics(schema)
    # 搜索配置：按名称类属性自动推导；换图后对"没有标准『名称』属性"的类型
    # （如主管部门/海关部门/相对人）必须按业务显名字段兜底，不能回退到字面"名称"
    # —— 否则这些类型的查询会命中不存在的属性而空结果。
    search_cfg = []
    name_properties = {}
    for n in schema.get("node_types") or []:
        lb = _u(n.get("name"))
        if not lb:
            continue
        props = [_u(p.get("name") if isinstance(p, dict) else p)
                 for p in (n.get("properties") or [])]
        props = [p for p in props if p]
        display_name = _u(n.get("display_name") or u"")
        id_prop = _u(n.get("unique_key")) or u"id~"
        namep = _pick_name_prop(props, display_name, id_prop)
        search_props = _pick_search_props(props, id_prop, namep)
        search_cfg.append({
            u"label": lb,
            u"id_prop": id_prop,
            u"name_prop": namep,
            u"search_props": search_props,
        })
        name_properties[lb] = {u"name": namep, u"id": id_prop}

    # entity_roles 完整骨架：按真实 fromAndToTypes 枚举每类型的入边/出边 →
    # propagate 列表（direction/label/role），role 用关系名+方向启发式命名。
    # 换图后无需人工从零填 15 个类型，只需微调 role 名
    rels = schema.get("relationship_types") or []
    entity_roles = {}
    for n in schema.get("node_types") or []:
        lb = _u(n.get("name"))
        if not lb:
            continue
        propagate = []
        seen_keys = set()
        for r in rels:
            rn = _u(r.get("name"))
            if not rn:
                continue
            for p in (r.get("pairs") or []):
                p = _u(p)
                if u"->" not in p:
                    continue
                src, dst = [x.strip() for x in p.split(u"->", 1)]
                # 出边：L 作为 src
                if src == lb and dst:
                    key = (rn, u"out", dst)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    propagate.append({
                        u"rel": rn,
                        u"direction": u"out",
                        u"label": dst,
                        u"depth": 1,
                        u"role": u"{}{}".format(rn, dst),
                    })
                # 入边：L 作为 dst
                if dst == lb and src:
                    key = (rn, u"in", src)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    propagate.append({
                        u"rel": rn,
                        u"direction": u"in",
                        u"label": src,
                        u"depth": 1,
                        u"role": u"所属{}".format(src)
                        if rn == u"包括" else
                        u"{}（来自{}）".format(rn, src),
                    })
        entity_roles[lb] = {u"propagate": propagate, u"compose": None}

    # 锚定类型额外补依赖传播（如果有依赖关系）
    if (sem.get("anchor_label") and sem.get("dependency_enabled")
            and sem.get("dependency_rel")):
        anchor = sem["anchor_label"]
        entity_roles.setdefault(anchor, {u"propagate": [], u"compose": None})
        entity_roles[anchor][u"propagate"].insert(0, {
            u"rel": sem[u"dependency_rel"],
            u"direction": u"in",
            u"depth": sem[u"dependency_depth"],
            u"role": u"受影响候选",
        })

    notes = {
        u"_说明": u"本文件由系统自动生成（换图绑定），核心语义/entity_roles/business_search "
                  u"为启发式填充，请人工校对后使用。aliases/query_templates/report_headers "
                  u"字段按业务词汇和高频问法补充",
        u"graph": _u(graph or GALAXY_GRAPH),
        u"core_semantics": sem,
        u"name_properties": name_properties,
        u"aliases": {},
        u"relationship_meanings": {},
        u"influence_rules": {},
        u"display_notes": u"",
        u"entity_roles": entity_roles,
        u"query_templates": [],
        u"report_headers": {},
        u"query_examples": [],
        u"business_search": search_cfg,
    }
    try:
        content = json.dumps(notes, ensure_ascii=False, indent=2)
        if not isinstance(content, unicode):
            content = content.decode("utf-8")
        with io.open(path, u"w", encoding="utf-8") as f:
            f.write(content)
    except (IOError, OSError, ValueError) as e:
        return path, notes, False, [u"写入配置文件失败：{}".format(e)]
    return path, notes, True, warnings


# ---------------- Galaxybase 客户端（纯标准库实现，py2/py3 兼容） ----------------
def _b2i(b):
    v = 0
    for ch in bytearray(b):
        v = (v << 8) | ch
    return v


def _i2b(v, length):
    out = bytearray(length)
    for i in range(length):
        out[length - 1 - i] = (v >> (8 * i)) & 0xFF
    return bytes(out)


def _der_tlv(buf, pos):
    """读一个 DER TLV，返回 (tag, value, 下一位置)。bytearray 索引在 py2/py3 都返回 int"""
    buf = bytearray(buf)
    tag = buf[pos]
    pos += 1
    ln = buf[pos]
    pos += 1
    if ln & 0x80:
        n = ln & 0x7F
        ln = _b2i(buf[pos:pos + n])
        pos += n
    return tag, bytes(buf[pos:pos + ln]), pos + ln


def _rsa_load_key(b64key):
    b64 = b64key.strip()
    if "-----BEGIN" in b64:
        b64 = "".join(l for l in b64.splitlines()
                      if not l.startswith("-----"))
    der = base64.b64decode(b64)
    _, spki, _ = _der_tlv(der, 0)          # SubjectPublicKeyInfo
    _, _, p = _der_tlv(spki, 0)            # AlgorithmIdentifier（跳过）
    _, bits, _ = _der_tlv(spki, p)         # BIT STRING（首字节为 unused-bits）
    _, rsaseq, _ = _der_tlv(bits[1:], 0)   # RSAPublicKey 序列
    _, n_b, p2 = _der_tlv(rsaseq, 0)
    _, e_b, _ = _der_tlv(rsaseq, p2)
    n, e = _b2i(n_b), _b2i(e_b)
    if n <= 0 or e <= 0:
        raise ValueError("invalid key values")
    return n, e


def rsa_encrypt_password(password, rsa_key=None):
    """RSA PKCS#1 v1.5 加密登录密码（Galaxybase /api/login 要求），返回 base64"""
    key = rsa_key or GALAXY_RSA_KEY or GALAXY_DOC_RSA_KEY
    last_err = None
    cands = [key]
    fixed = key.replace("GCSaG", "GCSqG", 1)  # 文档公钥与标准 OID 头的可能笔误
    if fixed != key:
        cands.append(fixed)
    for cand in cands:
        try:
            n, e = _rsa_load_key(cand)
            msg = password if isinstance(password, bytes) \
                else password.encode("utf-8")
            kb = (n.bit_length() + 7) // 8
            if len(msg) > kb - 11:
                raise ValueError("password too long")
            ps = bytearray()
            while len(ps) < kb - len(msg) - 3:
                b = bytearray(os.urandom(1))[0]
                if b != 0:
                    ps.append(b)
            em = b"\x00\x02" + bytes(ps) + b"\x00" + msg
            c = pow(_b2i(em), e, n)
            return base64.b64encode(_i2b(c, kb))
        except Exception as ex:
            last_err = ex
    raise ValueError(
        u"RSA 公钥解析失败（{}）：请把加密工具包中 login_password_rsa_public_key.pem"
        u" 的内容填到 GALAXY_RSA_KEY".format(last_err))


def gb_token(conn=None, force=False):
    """获取登录 token（按连接键缓存），返回 (token, err)。
    C3：锁内判读 + 落盘，避免并发首次登录重复提交 N 次或清缓存时丢写。"""
    conn = conn or default_connection()
    key = conn.connection_key()
    with _token_lock:
        if not force:
            tok = _token_cache.get(key)
            if tok:
                return tok, None
    if conn.pass_encrypted:
        pwd = conn.password
    else:
        try:
            pwd = rsa_encrypt_password(conn.password, conn.rsa_key)
        except Exception as e:
            return None, u"{}".format(e)
    try:
        r = requests.post(conn.base_url.rstrip("/") + "/api/login",
                          data={"username": conn.user, "password": pwd},
                          timeout=conn.timeout)
    except requests.RequestException as e:
        return None, u"无法连接图数据库登录接口：{}".format(e)
    try:
        resp = r.json()
    except ValueError:
        return None, u"登录接口返回非 JSON（HTTP {}）：{}".format(
            r.status_code, r.text[:300])
    if not resp.get("success"):
        return None, u"图数据库登录失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
    token = (resp.get("data") or {}).get("token")
    if not token:
        return None, u"登录成功但响应中无 token：{}".format(r.text[:300])
    with _token_lock:
        _token_cache[key] = token
    return token, None


def graph_run(statement, parameters=None, trusted=False, conn=None, stage=None):
    """通过 Galaxybase /api/cypher/commit 执行 Cypher，返回 (rows, columns, err)。
    rows 为 list[dict]；err 为 None 或错误描述字符串。token 失效自动重登一次。
    conn：显式连接上下文（None = 默认连接；跨图扫描必须显式传入）。
    stage：阶段标注（如"搜索候选业务"/"证据·一跳邻接"），落到运行日志 graph
    事件的 stage 字段，供日志面板区分这条查询是哪一步发出的。
    O2：记录调用数 / 失败数 / 耗时（图库健康的核心指标）。

    B3：trusted=False（默认）时套只读校验（CYPHER_WRITE_RE），避免某天把用户
       输入拼进常量 Cypher 时直接绕过保护；元数据扫描等已经确定是常量的内部调
       用必须显式传 trusted=True（也是在读/写拆分口上留一次人工复核机会）。"""
    if not trusted and CYPHER_WRITE_RE.search(statement or u""):
        return None, None, u"查询被拒绝：只允许只读查询（禁止 CREATE/DELETE/SET/MERGE/REMOVE/DROP/CALL 等写操作）"
    import metrics
    import runlog
    import time as _time
    conn = conn or default_connection()
    _t0 = _time.time()
    rows, columns, err = _graph_run_inner(statement, parameters, conn)
    metrics.inc("graph.calls")
    if err:
        metrics.inc("graph.failures")
    _ms = int((_time.time() - _t0) * 1000)
    metrics.observe("graph.duration_ms", _ms * 1.0)
    # 运行日志：全部 Cypher 的唯一出口，排查"没查到图数据/查询报错"看这里。
    # 降噪：查到结果或出错才记（0 行成功查询跳过，避免刷屏）；诊断明细模式
    # （debug_log_detail=1，默认开）下全量记录，便于核对"这条查询跑了没"。
    if err or rows or runlog.detail_enabled():
        # 瘦身模式下 event() 自动对 cypher/params 做摘要（cypher_digest/键名），
        # 明细模式原样落盘，这里统一传原文即可
        runlog.event("graph", stage=stage,
                     cypher=statement, params=parameters or {},
                     graph=conn.graph_name,
                     rows=(len(rows) if rows is not None else None),
                     err=err, ms=_ms)
    return rows, columns, err


def _graph_run_inner(statement, parameters, conn):
    payload = {
        "statements": [{
            "statement": statement,
            "parameters": parameters or {},
            "resultDataContents": ["row"],
        }],
        "graphName": conn.graph_name,
    }
    last_err = u""
    for attempt in (1, 2):  # 第二次强制重登（token 过期场景）
        token, err = gb_token(conn, force=(attempt == 2))
        if err:
            return None, None, err
        try:
            r = requests.post(conn.base_url.rstrip("/") + "/api/cypher/commit",
                              json=payload, headers={"Authorization": token},
                              timeout=conn.timeout)
        except requests.RequestException as e:
            return None, None, u"无法连接图数据库：{}".format(e)
        try:
            resp = r.json()
        except ValueError:
            return None, None, u"图数据库返回非 JSON（HTTP {}）：{}".format(
                r.status_code, r.text[:300])
        errors = resp.get("errors") or []
        if errors:
            return None, None, u"图数据库执行错误：{}".format(
                json.dumps(errors, ensure_ascii=False)[:500])
        if not resp.get("success"):
            last_err = u"图数据库返回失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
            if attempt == 1:
                continue  # 可能 token 失效，重登再试
            return None, None, last_err
        results = resp.get("results") or [{}]
        columns = results[0].get("columns") or []
        rows = []
        for d in (results[0].get("data") or []):
            row = d.get("row") or []
            rows.append(dict(zip(columns, row)))
        return rows, columns, None
    return None, None, last_err or u"图数据库认证失败"


def gb_get_schema(conn=None):
    """调官方 GET /api/buildGraph/getSchema，返回 (schema_data, err)"""
    conn = conn or default_connection()
    token, err = gb_token(conn)
    if err:
        return None, err
    try:
        r = requests.get(conn.base_url.rstrip("/") + "/api/buildGraph/getSchema",
                         params={"graphName": conn.graph_name, "isSimple": "true"},
                         headers={"Authorization": token}, timeout=conn.timeout)
    except requests.RequestException as e:
        return None, u"无法连接图数据库 schema 接口：{}".format(e)
    try:
        resp = r.json()
    except ValueError:
        return None, u"schema 接口返回非 JSON（HTTP {}）：{}".format(r.status_code, r.text[:300])
    if not resp.get("success"):
        return None, u"获取图模型失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
    return resp.get("data") or {}, None


def build_schema(conn=None, use_cache=True):
    """读取图结构 + 合并 schema_notes.json 的人工说明，带 TTL 缓存。
    结构来源优先级：官方 getSchema 接口 > Cypher 扫描 > schema_notes.json 人工配置。
    conn：显式连接上下文（None = 默认连接）；缓存按 连接键+notes指纹 隔离，
    换图/换配置互不串缓存；发布新配置后 notes 指纹变化自动失效。
    V4.1：Cypher 兜底不再用 CALL db.labels()（生产版本不支持该过程），
    改为 labels(n)[0] 全表聚合，并保留关系三元组（源类型/方向/目标类型）。"""
    conn = conn or default_connection()
    now = time.time()
    notes = load_schema_notes()
    notes_key = _notes_cache.get("key") or u""
    cache_key = u"{}|{}".format(conn.connection_key(), notes_key)
    if use_cache:
        # C3：读 _schema_cache 要在锁内取快照
        with _schema_lock:
            entry = _schema_cache.get(cache_key) or {}
            cached_ts = entry.get("ts") or 0
            cached_data = entry.get("data")
        if cached_data and now - cached_ts < SCHEMA_CACHE_TTL:
            return cached_data, None

    name_props = notes.get("name_properties") or {}
    node_types = []
    rel_types = []
    source = "manual"

    # ---- 优先：官方 getSchema（带主键、属性类型、边两端类型和方向） ----
    gs, gerr = gb_get_schema(conn)
    if not gerr and ((gs.get("vertexes") or {}) or (gs.get("edges") or {})):
        source = "official"
        for t, v in (gs.get("vertexes") or {}).items():
            t_safe = safe_ident(t)
            if not t_safe:
                continue
            props = [{"name": p.get("name"), "type": p.get("type") or ""}
                     for p in (v.get("property") or [])
                     if isinstance(p, dict) and p.get("name")]
            cnt_rows, _, _ = graph_run(
                u"MATCH (n:`{0}`) RETURN count(n) AS c".format(t_safe),
                trusted=True, conn=conn, stage=u"schema·类型计数")
            node_types.append({
                "name": t_safe,
                "unique_key": v.get("primaryId"),
                "display_name": _u(v.get("displayName") or u""),
                "properties": props,
                "count": (cnt_rows or [{}])[0].get("c"),
            })
        for t, v in (gs.get("edges") or {}).items():
            t_safe = safe_ident(t)
            if not t_safe:
                continue
            pairs = []
            for x in (v.get("fromAndToTypes") or []):
                if not isinstance(x, dict):
                    continue
                ft = safe_ident(x.get("fromType"))
                tt = safe_ident(x.get("toType"))
                if ft and tt:
                    pairs.append(u"{}->{}".format(ft, tt))
            rel_types.append({
                "name": t_safe,
                "directed": v.get("directed"),
                "pairs": pairs,
                "properties": [p_name for p_name in
                               (safe_ident(p.get("name")) or u""
                                for p in (v.get("property") or [])
                                if isinstance(p, dict) and p.get("name"))
                               if p_name],
                "count": None,
            })

    # ---- 回退一：Cypher 扫描（不再依赖 CALL db.labels() 等 Neo4j 过程） ----
    if not node_types and not rel_types:
        label_rows, _, lerr = graph_run(
            u"MATCH (n) RETURN labels(n)[0] AS label, count(*) AS c "
            u"ORDER BY c DESC", trusted=True, conn=conn,
            stage=u"schema扫描·类型统计")
        if not lerr:
            source = "cypher_scan"
            for lr in label_rows:
                lb = safe_ident(lr.get("label"))
                if not lb:
                    continue
                # 采 500 条做属性发现（稀疏属性可能漏；schema 构建低频 + TTL 缓存）
                prop_rows, _, perr = graph_run(
                    u"MATCH (n:`{0}`) WITH n LIMIT 500 UNWIND keys(n) AS k "
                    u"RETURN collect(DISTINCT k) AS ks".format(lb),
                    trusted=True, conn=conn, stage=u"schema扫描·属性发现")
                np_cfg = name_props.get(lb)
                node_types.append({
                    "name": lb,
                    "unique_key": (np_cfg.get("id") if isinstance(np_cfg, dict) else None),
                    "properties": sorted((prop_rows or [{}])[0].get("ks") or []) if not perr else [],
                    "count": lr.get("c"),
                })
            # 关系扫描保留三元组与方向（V4 旧版只查 type(r) 丢端点，此处修正）
            triple_rows, _, terr = graph_run(
                u"MATCH (a)-[r]->(b) RETURN labels(a)[0] AS src, type(r) AS t, "
                u"labels(b)[0] AS dst, count(*) AS c "
                u"ORDER BY src, t, dst", trusted=True, conn=conn,
                stage=u"schema扫描·关系三元组")
            if not terr:
                pairs_by_rel = {}
                counts_by_rel = {}
                for tr in triple_rows:
                    rn = safe_ident(tr.get("t"))
                    src = safe_ident(tr.get("src"))
                    dst = safe_ident(tr.get("dst"))
                    if not rn or not src or not dst:
                        continue
                    pairs_by_rel.setdefault(rn, []).append(
                        u"{}->{}".format(src, dst))
                    counts_by_rel[rn] = counts_by_rel.get(rn, 0) + (tr.get("c") or 0)
                rel_types = [{"name": rn, "directed": True,
                              "pairs": pairs_by_rel[rn],
                              "properties": [],
                              "count": counts_by_rel.get(rn)}
                             for rn in sorted(pairs_by_rel)]
            else:
                # 三元组都查不到时退回只统计边类型（仍无端点信息，记入告警）
                rel_rows, _, _ = graph_run(
                    u"MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c "
                    u"ORDER BY c DESC", trusted=True, conn=conn,
                    stage=u"schema扫描·边类型统计")
                rel_types = [{"name": safe_ident(r.get("t")),
                              "directed": True, "pairs": [],
                              "properties": [], "count": r.get("c")}
                             for r in rel_rows if safe_ident(r.get("t"))]

    # ---- 回退二：人工配置 ----
    if not node_types:
        source = "manual"
        for c in notes.get("business_search") or []:
            if not (isinstance(c, dict) and c.get("label")):
                continue
            manual_props = []
            for pp in ((c.get("search_props") or []) + (c.get("display_props") or [])
                       + [c.get("id_prop"), c.get("name_prop")]):
                if pp and pp not in manual_props:
                    manual_props.append(pp)
            np_cfg = name_props.get(c["label"])
            node_types.append({
                "name": c["label"],
                "unique_key": (np_cfg.get("id") if isinstance(np_cfg, dict) else None),
                "properties": manual_props,
                "count": None,
            })
    if not rel_types:
        for k in (notes.get("relationship_meanings") or {}):
            rel_types.append({"name": k, "count": None})

    data = {
        "database": {
            "product": "galaxybase",
            "rest_base": conn.base_url,
            "graph": conn.graph_name,
            "connection_key": conn.connection_key(),
        },
        "schema_source": source,
        "struct_version": int(now),
        "node_types": node_types,
        "relationship_types": rel_types,
        # 以下为人工补充的业务语义，见 schema_notes.json
        "name_properties": name_props,
        "relationship_meanings": notes.get("relationship_meanings") or {},
        "influence_rules": notes.get("influence_rules") or {},
        "display_notes": notes.get("display_notes") or "",
        "query_examples": notes.get("query_examples") or [],
    }
    with _schema_lock:
        _schema_cache[cache_key] = {"ts": now, "data": data}
    return data, None


def collect_graph_metadata(conn=None, sample_limit=500):
    """V4.1 元数据采集（改造方案 §5.1 / §3）：官方 getSchema 声明结构 +
    Cypher 实际扫描交叉验证。返回 (metadata, err)；两者都失败时返回 err，
    绝不把缓存或人工配置当成"本次扫描成功"。

    metadata 结构：
      graph / schema_source / scan_warnings
      node_types:      name, properties[{name,type}], declared_id, display_name,
                       observed_count(None=未查到，不是0), observed_props
      relationship_types: name, directed, pairs, properties, observed_count
      observed_topology:  Q05 三元组 [{source_type, relation, target_type, count}]
      id_validation:      {label: {prop: {total, nonempty, distinct, valid}}}
      differences:        {nodes: {declared_only, observed_only, matched},
                           edge_pairs: {declared_only, observed_only, matched}}
    """
    conn = conn or default_connection()
    try:
        sample_limit = int(sample_limit)
    except (TypeError, ValueError):
        sample_limit = 500
    if sample_limit < 1:
        sample_limit = 500
    if sample_limit > 2000:
        sample_limit = 2000

    warnings = []
    # ---- 声明结构：官方 getSchema ----
    gs, gerr = gb_get_schema(conn)
    declared_nodes = {}
    declared_edges = {}
    if gerr:
        warnings.append(u"getSchema 获取失败：{}".format(gerr))
    else:
        for t, v in (gs.get("vertexes") or {}).items():
            t_safe = safe_ident(t)
            if not t_safe:
                continue
            props = []
            for p in (v.get("property") or []):
                if isinstance(p, dict) and p.get("name"):
                    pn = safe_ident(p.get("name"))
                    if pn:
                        props.append({"name": pn, "type": _u(p.get("type") or u"")})
            declared_nodes[t_safe] = {
                "primary_id": safe_ident(v.get("primaryId")) or None,
                "display_name": _u(v.get("displayName") or u""),
                "properties": props,
            }
        for t, v in (gs.get("edges") or {}).items():
            t_safe = safe_ident(t)
            if not t_safe:
                continue
            pairs = []
            for x in (v.get("fromAndToTypes") or []):
                if isinstance(x, dict):
                    ft = safe_ident(x.get("fromType"))
                    tt = safe_ident(x.get("toType"))
                    if ft and tt:
                        pairs.append(u"{}->{}".format(ft, tt))
            rprops = []
            for p in (v.get("property") or []):
                if isinstance(p, dict) and p.get("name"):
                    pn = safe_ident(p.get("name"))
                    if pn:
                        rprops.append(pn)
            declared_edges[t_safe] = {
                "directed": bool(v.get("directed")),
                "pairs": pairs,
                "properties": rprops,
            }

    # ---- 实际结构：Cypher 扫描 ----
    # Q01 各 label 数量
    label_rows, _, lerr = graph_run(
        u"MATCH (n) UNWIND labels(n) AS label "
        u"RETURN label AS label, count(*) AS c ORDER BY c DESC",
        trusted=True, conn=conn, stage=u"元数据扫描·类型统计")
    if lerr:
        # UNWIND 不可用或失败时退回 labels(n)[0] 聚合（已验证语句）
        label_rows, _, lerr = graph_run(
            u"MATCH (n) RETURN labels(n)[0] AS label, count(*) AS c "
            u"ORDER BY c DESC", trusted=True, conn=conn,
            stage=u"元数据扫描·类型统计")
        if not lerr:
            warnings.append(u"UNWIND labels(n) 不可用，已回退 labels(n)[0]，"
                            u"多标签节点统计可能偏差")
    observed_labels = {}
    if not lerr:
        for lr in label_rows:
            lb = safe_ident(lr.get("label"))
            if lb:
                observed_labels[lb] = lr.get("c")
    else:
        warnings.append(u"节点类型统计失败：{}".format(lerr))

    # Q02 按 label 采样属性名（只证明"样本出现"，非完整发现）
    sampled_props = {}
    for lb in sorted(observed_labels):
        prop_rows, _, perr = graph_run(
            u"MATCH (n:`{0}`) WITH n LIMIT {1} UNWIND keys(n) AS k "
            u"RETURN collect(DISTINCT k) AS ks".format(lb, sample_limit),
            trusted=True, conn=conn, stage=u"元数据扫描·属性采样")
        if not perr:
            ks = (prop_rows or [{}])[0].get("ks") or []
            sampled_props[lb] = sorted(safe_ident(k) for k in ks if safe_ident(k))
        else:
            sampled_props[lb] = []
            warnings.append(u"类型 {} 属性采样失败：{}".format(lb, perr))

    # Q05 关系三元组（方向）
    triple_rows, _, terr = graph_run(
        u"MATCH (a)-[r]->(b) RETURN labels(a)[0] AS src, type(r) AS t, "
        u"labels(b)[0] AS dst, count(*) AS c ORDER BY src, t, dst",
        trusted=True, conn=conn, stage=u"元数据扫描·关系三元组")
    observed_topology = []
    observed_pairs_by_rel = {}
    if not terr:
        for tr in triple_rows:
            rn = safe_ident(tr.get("t"))
            src = safe_ident(tr.get("src"))
            dst = safe_ident(tr.get("dst"))
            if not rn or not src or not dst:
                continue
            observed_topology.append({
                "source_type": src, "relation": rn, "target_type": dst,
                "count": tr.get("c"),
            })
            observed_pairs_by_rel.setdefault(rn, []).append(u"{}->{}".format(src, dst))
    else:
        warnings.append(u"关系拓扑扫描失败：{}".format(terr))

    if terr:
        warnings.append(u"关系拓扑扫描未完成：不能将未扫描到的关系判定为不存在")

    # Q06 边类型计数（用于 declared-only 关系的 observed_count 与兜底目录）
    edge_rows, _, eerr = graph_run(
        u"MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC",
        trusted=True, conn=conn, stage=u"元数据扫描·边类型统计")
    edge_counts = {}
    if not eerr:
        for er in edge_rows:
            rn = safe_ident(er.get("t"))
            if rn:
                edge_counts[rn] = er.get("c")
    else:
        warnings.append(u"边类型统计失败：{}".format(eerr))

    # Q07 边属性采样（探测方向/接口等辅助证据字段；空结果≠边不存在）
    edge_props = {}
    for rn in sorted(set(observed_pairs_by_rel) | set(edge_counts)):
        prop_rows, _, perr = graph_run(
            u"MATCH ()-[r:`{0}`]() WITH r LIMIT {1} UNWIND keys(r) AS k "
            u"RETURN collect(DISTINCT k) AS ks".format(rn, sample_limit),
            trusted=True, conn=conn, stage=u"元数据扫描·边属性采样")
        if not perr:
            ks = (prop_rows or [{}])[0].get("ks") or []
            edge_props[rn] = sorted(safe_ident(k) for k in ks if safe_ident(k))
        else:
            edge_props[rn] = []

    # Q04 按 label 验证 ID 候选覆盖率/唯一性
    # 候选 = getSchema primaryId（存在的）∪ 常见 [_ID, ID, id~]；
    # 有效判据：total > 0 且 nonempty == total == distinct
    id_candidates = [u"_ID", u"ID", u"id~"]
    for info in declared_nodes.values():
        pid = info.get("primary_id")
        if pid and pid not in id_candidates:
            id_candidates.insert(0, pid)
    id_validation = {}
    for cand in id_candidates:
        if not safe_ident(cand):
            continue
        rows, _, ierr = graph_run(
            u"MATCH (n) UNWIND labels(n) AS label RETURN label AS label, "
            u"count(n) AS total, count(n.`{0}`) AS nonempty, "
            u"count(DISTINCT n.`{0}`) AS distinct_n ORDER BY label".format(cand),
            trusted=True, conn=conn, stage=u"元数据扫描·主键验证")
        if ierr:
            warnings.append(u"ID 候选 {} 验证失败：{}".format(cand, ierr))
            continue
        for r in rows:
            lb = safe_ident(r.get("label"))
            if not lb:
                continue
            total = r.get("total") or 0
            nonempty = r.get("nonempty") or 0
            distinct_n = r.get("distinct_n") or 0
            valid = (total > 0 and nonempty == total and distinct_n == total)
            id_validation.setdefault(lb, {})[cand] = {
                "total": total, "nonempty": nonempty,
                "distinct": distinct_n, "valid": bool(valid),
            }

    # ---- 组装 node_types / relationship_types ----
    node_types = []
    for lb in sorted(set(declared_nodes) | set(observed_labels)):
        d = declared_nodes.get(lb)
        entry = {
            "name": lb,
            "properties": (d or {}).get("properties") or [],
            "declared_id": (d or {}).get("primary_id"),
            "display_name": (d or {}).get("display_name") or u"",
            # observed_count=None 表示未查到/未执行，不是 0
            "observed_count": observed_labels.get(lb),
            "observed_props": sampled_props.get(lb, []),
            "id_validation": id_validation.get(lb) or {},
            "declared": lb in declared_nodes,
        }
        node_types.append(entry)
    relationship_types = []
    for rn in sorted(set(declared_edges) | set(observed_pairs_by_rel)
                     | set(edge_counts)):
        d = declared_edges.get(rn)
        relationship_types.append({
            "name": rn,
            "directed": (d or {}).get("directed", True),
            "pairs": (d or {}).get("pairs") or observed_pairs_by_rel.get(rn) or [],
            "properties": (d or {}).get("properties") or edge_props.get(rn) or [],
            "observed_count": edge_counts.get(rn),
            "observed_pairs": observed_pairs_by_rel.get(rn) or [],
            "declared": rn in declared_edges,
        })

    # ---- Q13 差异计算：declared_only / observed_only / matched ----
    diff_nodes = {
        "declared_only": sorted(set(declared_nodes) - set(observed_labels)),
        "observed_only": sorted(set(observed_labels) - set(declared_nodes)),
        "matched": sorted(set(declared_nodes) & set(observed_labels)),
    }
    declared_pair_set = set()
    for rn, d in declared_edges.items():
        for p in d.get("pairs") or []:
            declared_pair_set.add(u"{}|{}".format(rn, p))
    observed_pair_set = set()
    for t in observed_topology:
        observed_pair_set.add(u"{}|{}->{}".format(
            t["relation"], t["source_type"], t["target_type"]))
    diff_pairs = {
        "declared_only": sorted(declared_pair_set - observed_pair_set),
        "observed_only": sorted(observed_pair_set - declared_pair_set),
        "matched": sorted(declared_pair_set & observed_pair_set),
    }

    if observed_topology and not gerr:
        schema_source = u"official+cypher_scan"
    elif not gerr:
        schema_source = u"official"
    elif observed_topology:
        schema_source = u"cypher_scan"
        warnings.append(u"仅 Cypher 扫描：labels(n)[0] 只取第一个 label，"
                        u"多标签节点需结合 getSchema 补验")
    else:
        return None, u"扫描失败：getSchema 与 Cypher 均不可用（{}；{}）".format(
            gerr, terr or lerr or u"未知错误")

    metadata = {
        "graph": conn.graph_name,
        "connection_key": conn.connection_key(),
        "schema_source": schema_source,
        "scan_warnings": warnings,
        "node_types": node_types,
        "relationship_types": relationship_types,
        "observed_topology": observed_topology,
        "id_validation": id_validation,
        "differences": {"nodes": diff_nodes, "edge_pairs": diff_pairs},
    }
    return metadata, None
