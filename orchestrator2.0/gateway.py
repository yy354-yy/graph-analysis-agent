# -*- coding: utf-8 -*-
"""图客户端与本地存储层（由旧根目录网关 app.py 融合而来）。

提供：
    本地会话存储    db() / init_db()（SQLite history.db，模块所在目录）
    think 过滤      make_think_filter()（流式剥离 <think>...</think> 状态机）
    Galaxybase 客户端  RSA 登录（纯标准库实现）/ graph_run / gb_get_schema
    图结构          load_schema_notes() / build_schema()（getSchema > Cypher 扫描
                    > schema_notes.json 人工配置，TTL 缓存）

连接配置在下方配置区（GALAXY_*），可被 settings.py 运行时覆盖。
兼容 Python 2.7 与 3.x。
"""
from __future__ import print_function

import base64
import io
import json
import os
import re
import sqlite3
import time

import requests

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
SCHEMA_CACHE_TTL = 300    # 元数据接口内存缓存秒数
# ================================================================

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.db")


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
                # 思考内容丢弃，但保留可能是半个闭合标签的尾部
                safe = max(len(buf) - len(THINK_CLOSE) + 1, 0)
                state["buf"] = buf[safe:]
            else:
                idx = buf.find(THINK_OPEN)
                if idx >= 0:
                    if idx:
                        out.append(buf[:idx])
                    state["buf"] = buf[idx + len(THINK_OPEN):]
                    state["in_think"] = True
                    continue
                # 输出确定安全的部分，保留可能是半个开始标签的尾部
                safe = max(len(buf) - len(THINK_OPEN) + 1, 0)
                if safe:
                    out.append(buf[:safe])
                    state["buf"] = buf[safe:]
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
    u"dependency_rel": u"业务依赖",      # 影响传播关系（A -[rel]-> B = A 依赖 B）
    u"dependency_depth": 6,              # 依赖传播变长路径深度
    u"hosting_rel": u"承载于",           # 业务 -> 承载系统 关系
    u"system_label": u"信息系统",        # 承载系统实体类型
    u"name_prop": u"名称",               # 通用名称属性
    u"rollup_exclude_rels": [u"业务依赖", u"子项依赖", u"协同事项",
                             u"协同发起", u"协同参与"],   # 归属链排除的协作关系
    u"candidate_labels": [u"业务事项", u"业务子项"],     # 候选搜索限定类型
}

# 图名转文件名安全字符（保留中文/字母/数字/下划线/连字符）
_FILE_SAFE_RE = re.compile(u"[^0-9A-Za-z_一-龥-]+")

_schema_cache = {"ts": 0, "data": None}
_token_cache = {"token": None}
_notes_cache = {"key": None, "data": None}

# REST 使用说明文档中的 RSA 公钥（若解析失败请把 pem 文件内容填到 GALAXY_RSA_KEY）
GALAXY_DOC_RSA_KEY = (
    "MIGfMA0GCSaGSIb3DQEBAQUAA4GNADCBiQKBgQCXgctF+noMbOXv5c8hVef4vE6eK/mwnzEp"
    "MoHpZJjphuyTcowsD9DBWe+PrqZOEm1PomzD/TxOn9eMhn9O+w3yuuv8fipMs6OjU6Y+rKLrGJ8aCZpzd7Y5eewQS/0hOGmWtQlLd"
    "ayJaUT0B0Fpz3yhR7u7vtVhKwbCYYfHxhg1PQIDAQAB")

# 只读校验：拒绝一切写操作和存储过程调用（内部元数据扫描不经过此校验）
CYPHER_WRITE_RE = re.compile(
    r"\b(CREATE|DELETE|DETACH|SET|MERGE|REMOVE|DROP|LOAD\s+CSV|FOREACH|CALL)\b",
    re.I)
CYPHER_LIMIT_RE = re.compile(r"\bLIMIT\b", re.I)


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
    不存在则用默认 schema_notes.json。带 mtime 缓存，改文件即时生效。"""
    path = notes_path_for()
    if not path or not os.path.exists(path):
        path = DEFAULT_NOTES_PATH
    try:
        key = u"{}@{}".format(path, int(os.path.getmtime(path)))
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
    """当前图核心语义 = DEFAULT_CORE_SEMANTICS 被 schema_notes.core_semantics 覆盖"""
    sem = dict(DEFAULT_CORE_SEMANTICS)
    ov = load_schema_notes().get("core_semantics")
    if isinstance(ov, dict):
        for k, v in ov.items():
            if v is not None and v != u"":
                sem[k] = v
    return sem


# ---------------- 换图绑定：schema_notes 骨架自动生成 ----------------
def guess_core_semantics(schema):
    """从 build_schema 结果启发式猜核心语义（生成骨架用，需人工校对）。
    返回 (sem, warnings)。"""
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
    for r in rels:
        for p in (r.get("pairs") or []):
            for side in _u(p).split(u"->"):
                side = side.strip()
                if side:
                    degree[side] = degree.get(side, 0) + 1
    anchor = u""
    cands = []
    for pat in (u"业务", u"事项", u"流程", u"活动", u"服务"):
        cands = [lb for lb in labels if pat in lb]
        if cands:
            anchor = max(cands, key=lambda lb: degree.get(lb, 0))
            break
    if not anchor and labels:
        anchor = max(labels, key=lambda lb: degree.get(lb, 0))
        warnings.append(u"锚定类型：未匹配到业务类命名，按关系连接度取「{}」，请校对".format(anchor))

    # 依赖关系：优先取"名字含依赖语义 且 两端都是锚定类型"的关系（比纯名字匹配准）
    pairs_of = {}
    for r in rels:
        pairs_of[_u(r.get("name"))] = [_u(p) for p in (r.get("pairs") or [])]
    anchor_pair = u"{}->{}".format(anchor, anchor)
    dep = u""
    for rn in relnames:
        if any(k in rn for k in (u"依赖", u"引用", u"调用")) \
                and anchor_pair in pairs_of.get(rn, []):
            dep = rn
            break
    if not dep:
        dep = pick(relnames, (u"依赖", u"引用", u"调用"), u"依赖传播关系", must=False)
    hosting = pick(relnames, (u"承载", u"部署", u"运行", u"落户"), u"承载关系", must=False)
    system = pick(labels, (u"系统", u"平台", u"应用"), u"承载系统类型", must=False)
    name_prop = u""
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
    sem = {
        u"anchor_label": anchor,
        u"dependency_rel": dep,
        u"dependency_depth": 6,
        u"hosting_rel": hosting,
        u"system_label": system,
        u"name_prop": name_prop,
        u"rollup_exclude_rels": excl,
        u"candidate_labels": [anchor] if anchor else [],
    }
    return sem, warnings


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
    # 搜索配置：按名称类属性自动推导（同 store.business_search 的兜底逻辑）
    search_cfg = []
    for n in schema.get("node_types") or []:
        lb = _u(n.get("name"))
        props = [_u(p.get("name") if isinstance(p, dict) else p)
                 for p in (n.get("properties") or [])]
        namep = next((p for p in props if p == u"名称"), None) or \
            next((p for p in props if p and (u"名" in p or u"name" in p.lower())), None)
        if lb and namep:
            search_cfg.append({
                u"label": lb,
                u"id_prop": _u(n.get("unique_key")) or namep,
                u"name_prop": namep,
                u"search_props": [namep],
            })
    # 锚定类型补依赖传播角色（有依赖关系时），其余类型留给人工标注
    entity_roles = {}
    if sem.get("anchor_label") and sem.get("dependency_rel"):
        entity_roles[sem["anchor_label"]] = {
            u"propagate": [{u"rel": sem[u"dependency_rel"],
                            u"depth": sem[u"dependency_depth"],
                            u"role": u"受影响候选"}],
            u"compose": None,
        }
    notes = {
        u"_说明": u"本文件由系统自动生成（换图绑定），核心语义为启发式猜测，"
                  u"请人工校对 core_semantics / entity_roles / business_search 后使用",
        u"graph": _u(graph or GALAXY_GRAPH),
        u"core_semantics": sem,
        u"name_properties": {},
        u"relationship_meanings": {},
        u"influence_rules": {},
        u"display_notes": u"",
        u"entity_roles": entity_roles,
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


def rsa_encrypt_password(password):
    """RSA PKCS#1 v1.5 加密登录密码（Galaxybase /api/login 要求），返回 base64"""
    key = GALAXY_RSA_KEY or GALAXY_DOC_RSA_KEY
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


def gb_token(force=False):
    """获取登录 token（带缓存），返回 (token, err)"""
    if not force and _token_cache["token"]:
        return _token_cache["token"], None
    if GALAXY_PASS_ENCRYPTED:
        pwd = GALAXY_PASS
    else:
        try:
            pwd = rsa_encrypt_password(GALAXY_PASS)
        except Exception as e:
            return None, u"{}".format(e)
    try:
        r = requests.post(GALAXY_BASE.rstrip("/") + "/api/login",
                          data={"username": GALAXY_USER, "password": pwd},
                          timeout=GRAPH_TIMEOUT)
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
    _token_cache["token"] = token
    return token, None


def graph_run(statement, parameters=None):
    """通过 Galaxybase /api/cypher/commit 执行 Cypher，返回 (rows, columns, err)。
    rows 为 list[dict]；err 为 None 或错误描述字符串。token 失效自动重登一次。"""
    payload = {
        "statements": [{
            "statement": statement,
            "parameters": parameters or {},
            "resultDataContents": ["row"],
        }],
        "graphName": GALAXY_GRAPH,
    }
    last_err = u""
    for attempt in (1, 2):  # 第二次强制重登（token 过期场景）
        token, err = gb_token(force=(attempt == 2))
        if err:
            return None, None, err
        try:
            r = requests.post(GALAXY_BASE.rstrip("/") + "/api/cypher/commit",
                              json=payload, headers={"Authorization": token},
                              timeout=GRAPH_TIMEOUT)
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


def gb_get_schema():
    """调官方 GET /api/buildGraph/getSchema，返回 (schema_data, err)"""
    token, err = gb_token()
    if err:
        return None, err
    try:
        r = requests.get(GALAXY_BASE.rstrip("/") + "/api/buildGraph/getSchema",
                         params={"graphName": GALAXY_GRAPH, "isSimple": "true"},
                         headers={"Authorization": token}, timeout=GRAPH_TIMEOUT)
    except requests.RequestException as e:
        return None, u"无法连接图数据库 schema 接口：{}".format(e)
    try:
        resp = r.json()
    except ValueError:
        return None, u"schema 接口返回非 JSON（HTTP {}）：{}".format(r.status_code, r.text[:300])
    if not resp.get("success"):
        return None, u"获取图模型失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
    return resp.get("data") or {}, None


def build_schema():
    """读取图结构 + 合并 schema_notes.json 的人工说明，带 TTL 缓存。
    结构来源优先级：官方 getSchema 接口 > Cypher 过程扫描 > schema_notes.json 人工配置。"""
    now = time.time()
    if _schema_cache["data"] and now - _schema_cache["ts"] < SCHEMA_CACHE_TTL:
        return _schema_cache["data"], None

    notes = load_schema_notes()
    name_props = notes.get("name_properties") or {}
    node_types = []
    rel_types = []
    source = "manual"

    # ---- 优先：官方 getSchema（带主键、属性类型、边两端类型和方向） ----
    gs, gerr = gb_get_schema()
    if not gerr and ((gs.get("vertexes") or {}) or (gs.get("edges") or {})):
        source = "official"
        for t, v in (gs.get("vertexes") or {}).items():
            props = [{"name": p.get("name"), "type": p.get("type") or ""}
                     for p in (v.get("property") or [])
                     if isinstance(p, dict) and p.get("name")]
            cnt_rows, _, _ = graph_run(
                u"MATCH (n:`{0}`) RETURN count(n) AS c".format(t))
            node_types.append({
                "name": t,
                "unique_key": v.get("primaryId"),
                "properties": props,
                "count": (cnt_rows or [{}])[0].get("c"),
            })
        for t, v in (gs.get("edges") or {}).items():
            pairs = [u"{}->{}".format(x.get("fromType"), x.get("toType"))
                     for x in (v.get("fromAndToTypes") or []) if isinstance(x, dict)]
            rel_types.append({
                "name": t,
                "directed": v.get("directed"),
                "pairs": pairs,
                "properties": [p.get("name") for p in (v.get("property") or [])
                               if isinstance(p, dict) and p.get("name")],
                "count": None,
            })

    # ---- 回退一：Cypher 过程扫描 ----
    if not node_types and not rel_types:
        label_rows, _, lerr = graph_run("CALL db.labels() YIELD label RETURN label")
        if not lerr:
            source = "cypher_scan"
            for lr in label_rows:
                lb = lr.get("label")
                if not lb:
                    continue
                cnt_rows, _, _ = graph_run(
                    u"MATCH (n:`{0}`) RETURN count(n) AS c".format(lb))
                prop_rows, _, perr = graph_run(
                    u"MATCH (n:`{0}`) WITH n LIMIT 50 UNWIND keys(n) AS k "
                    u"RETURN collect(DISTINCT k) AS ks".format(lb))
                np_cfg = name_props.get(lb)
                node_types.append({
                    "name": lb,
                    "unique_key": (np_cfg.get("id") if isinstance(np_cfg, dict) else None),
                    "properties": sorted((prop_rows or [{}])[0].get("ks") or []) if not perr else [],
                    "count": (cnt_rows or [{}])[0].get("c"),
                })
            rel_rows, _, _ = graph_run(
                u"MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC")
            rel_types = [{"name": r.get("t"), "count": r.get("c")} for r in rel_rows]

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
            "rest_base": GALAXY_BASE,
            "graph": GALAXY_GRAPH,
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
    _schema_cache["ts"] = now
    _schema_cache["data"] = data
    return data, None
