# -*- coding: utf-8 -*-
"""V4.1 Schema 配置版本管理（改造方案 §5.7）。

目录布局（不引入额外数据库）：
  orchestrator2.0/
    schema_notes.json                # 默认/旧版兼容（gateway 读取入口不变）
    schema_notes.<图名>.json          # 该图当前【已发布】配置
    schema_versions/<安全图标识>/
      manifest.json                  # 版本索引（版本号/时间/指纹/校验摘要/动作）
      <版本号>.json                  # 历史版本快照
      candidate.json                 # 尚未发布的候选配置

写入纪律：临时文件 → JSON 回读校验 → fsync → 同目录原子 rename；
发布失败保持原文件不动；发布/回滚后清理 gateway 与 store 各层缓存。
Python 2/3 双兼容。
"""
from __future__ import print_function

import io
import json
import os
import re
import tempfile
import time
import shutil

import gateway
from gateway import _u

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VERSIONS_DIR = os.path.join(BASE_DIR, u"schema_versions")

# 目录名白名单：unicode 字母数字 + _-.（其余替换 _），防路径逃逸。
# 注意：\u4e00 必须单反斜杠（字符串字面量层面就转成真字符）——py2 的 re 不支持
# 模式内 \uXXXX 转义，写成 \\u 会让 py2 把中文全部替换成 _（py3 则正常）。
_DIR_UNSAFE_RE = re.compile(u"[^0-9A-Za-z_\u4e00-\u9fff.-]+")


def safe_graph_dir(graph):
    s = _DIR_UNSAFE_RE.sub(u"_", _u(graph or u"").strip())
    s = s.strip(u"._") or u"default"
    return s[:60]


def versions_dir(graph):
    return os.path.join(VERSIONS_DIR, safe_graph_dir(graph))


def candidate_path(graph):
    return os.path.join(versions_dir(graph), u"candidate.json")


def manifest_path(graph):
    return os.path.join(versions_dir(graph), u"manifest.json")


def published_path(graph):
    return gateway.notes_path_for(graph)


# ---------------- 原子写 ----------------
def _atomic_write_json(path, data):
    """临时文件 → JSON 回读校验 → fsync → 同目录原子 rename。"""
    content = json.dumps(data, ensure_ascii=False, indent=2)
    if not isinstance(content, type(u"")):
        content = content.decode(u"utf-8")
    json.loads(content)  # 写前校验
    d = os.path.dirname(path)
    if not os.path.isdir(d):
        os.makedirs(d)
    fd, tmp = tempfile.mkstemp(prefix=u".tmp_schema_", dir=d)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        if hasattr(os, "replace"):
            os.replace(tmp, path)   # py3：POSIX/Windows 都是原子覆盖已存在目标
        else:
            # py2 无 os.replace；Windows 的 os.rename 拒绝覆盖已存在目标
            # （WinError 183），先删旧文件；POSIX 的 rename 本身可覆盖
            if os.path.exists(path):
                os.remove(path)
            os.rename(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _read_json(path):
    """读 JSON 文件；不存在或损坏返回 (None, err)。"""
    if not os.path.exists(path):
        return None, u"文件不存在：{}".format(path)
    try:
        with io.open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data, None
    except (IOError, OSError, ValueError) as e:
        return None, u"读取失败：{}".format(e)


def _content_hash(data):
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True)
    if not isinstance(raw, type(u"")):
        raw = raw.decode(u"utf-8")
    import hashlib
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ---------------- 候选配置 ----------------
def save_candidate(graph, notes):
    """保存候选配置（不覆盖已发布配置）。返回 (path, err)。"""
    if not isinstance(notes, dict):
        return None, u"候选配置必须是 JSON 对象"
    path = candidate_path(graph)
    try:
        _atomic_write_json(path, notes)
    except (OSError, ValueError) as e:
        return None, u"写入候选配置失败：{}".format(e)
    return path, None


def load_candidate(graph):
    return _read_json(candidate_path(graph))


def load_published(graph):
    return _read_json(published_path(graph))


# ---------------- 差异 ----------------
def _label_set(notes):
    out = set()
    for c in (notes.get("business_search") or []):
        if isinstance(c, dict) and c.get("label"):
            out.add(_u(c.get("label")))
    for lb in (notes.get("name_properties") or {}):
        out.add(_u(lb))
    return out


def _catalog_set(notes):
    out = set()
    for c in (notes.get("relation_catalog") or []):
        if isinstance(c, dict):
            out.add((c.get("source"), c.get("relation"), c.get("target")))
    return out


def _dep_set(notes):
    out = set()
    for p in (notes.get("dependency_paths") or []):
        if isinstance(p, dict):
            out.add((p.get("source"), p.get("relation"), p.get("target")))
    return out


def diff_notes(old_notes, new_notes):
    """已发布版 vs 候选版结构化差异（前端绑定向导第3步展示）。只读。"""
    old_notes = old_notes if isinstance(old_notes, dict) else {}
    new_notes = new_notes if isinstance(new_notes, dict) else {}
    old_labels = _label_set(old_notes)
    new_labels = _label_set(new_notes)
    old_cat = _catalog_set(old_notes)
    new_cat = _catalog_set(new_notes)
    old_dep = _dep_set(old_notes)
    new_dep = _dep_set(new_notes)

    id_changes = []
    old_np = old_notes.get("name_properties") or {}
    new_np = new_notes.get("name_properties") or {}
    for lb in sorted(set(old_np) | set(new_np)):
        o = (old_np.get(lb) or {}).get("id")
        n = (new_np.get(lb) or {}).get("id")
        if o != n:
            id_changes.append({u"label": lb, u"old": o, u"new": n})

    sem_changes = []
    old_sem = old_notes.get("core_semantics") or {}
    new_sem = new_notes.get("core_semantics") or {}
    for k in sorted(set(old_sem) | set(new_sem)):
        if old_sem.get(k) != new_sem.get(k):
            sem_changes.append({u"field": k,
                                u"old": old_sem.get(k),
                                u"new": new_sem.get(k)})

    meaning_changes = []
    old_m = old_notes.get("relationship_meanings") or {}
    new_m = new_notes.get("relationship_meanings") or {}
    for rn in sorted(set(old_m) | set(new_m)):
        if old_m.get(rn) != new_m.get(rn):
            meaning_changes.append({u"relation": rn,
                                    u"old": old_m.get(rn),
                                    u"new": new_m.get(rn)})

    gen = new_notes.get("generation_info") or {}
    return {
        u"labels_added": sorted(new_labels - old_labels),
        u"labels_removed": sorted(old_labels - new_labels),
        u"labels_kept": len(old_labels & new_labels),
        u"id_changes": id_changes,
        u"relations_added": [u"{}-{}->{}".format(*t) for t in
                             sorted(new_cat - old_cat)],
        u"relations_removed": [u"{}-{}->{}".format(*t) for t in
                               sorted(old_cat - new_cat)],
        u"dependency_kept": [u"{}-{}->{}".format(*t) for t in
                             sorted(old_dep & new_dep)],
        u"dependency_dropped": [u"{}-{}->{}".format(*t) for t in
                                sorted(old_dep - new_dep)],
        u"dependency_added": [u"{}-{}->{}".format(*t) for t in
                              sorted(new_dep - old_dep)],
        u"core_semantics_changes": sem_changes,
        u"meaning_changes": meaning_changes,
        u"curated_preserved": {
            u"aliases": len(new_notes.get("aliases") or {}),
            u"influence_rules": len(new_notes.get("influence_rules") or {}),
            u"query_templates": len(new_notes.get("query_templates") or []),
            u"report_headers": len(new_notes.get("report_headers") or {}),
            u"query_examples": len(new_notes.get("query_examples") or []),
        },
        u"reconfirm_required": gen.get(u"reconfirm_required") or [],
        u"schema_fingerprint": new_notes.get(u"schema_fingerprint") or u"",
        u"graph": new_notes.get(u"graph") or old_notes.get(u"graph") or u"",
    }


# ---------------- 版本索引 ----------------
def _load_manifest(graph):
    data, err = _read_json(manifest_path(graph))
    if err or not isinstance(data, dict):
        return {u"graph": _u(graph or u""), u"versions": []}
    if not isinstance(data.get("versions"), list):
        data[u"versions"] = []
    return data


def list_versions(graph):
    m = _load_manifest(graph)
    out = []
    for v in m.get(u"versions") or []:
        if isinstance(v, dict):
            out.append({
                u"version": v.get(u"version"),
                u"time": v.get(u"time"),
                u"action": v.get(u"action"),
                u"content_hash": v.get(u"content_hash"),
                u"validation_overall": (v.get(u"validation") or {}).get(
                    u"overall"),
            })
    out.reverse()  # 新版本在前
    return out


def _append_manifest(graph, entry):
    m = _load_manifest(graph)
    m.setdefault(u"versions", []).append(entry)
    try:
        _atomic_write_json(manifest_path(graph), m)
    except (OSError, ValueError):
        pass  # 索引写失败不阻断发布主流程


def _version_id():
    return u"v{}".format(time.strftime(u"%Y%m%d%H%M%S"))


def _unique_version_id(graph):
    """同秒多次发布/回滚的版本号去重：快照文件已存在时追加 _N 后缀。"""
    vid = _version_id()
    d = versions_dir(graph)
    n = 0
    while os.path.exists(os.path.join(d, u"{}.json".format(vid))):
        n += 1
        vid = u"{}_{}".format(_version_id(), n)
    return vid


def _clear_caches():
    """发布/回滚后清理各层缓存（gateway 三缓存 + store 类型表）。"""
    try:
        gateway.clear_runtime_caches()
    except Exception:
        pass
    try:
        import store
        store._reg_cache["data"] = None
        store._reg_cache["ts"] = 0
        store._search_cfg_cache["key"] = None
        store._search_cfg_cache["data"] = None
        store._identity_cache["key"] = None
        store._identity_cache["data"] = {}
        store._identity_ts_cache.clear()
    except Exception:
        pass


# ---------------- 发布 / 回滚 ----------------
def publish(graph, notes=None, validation_report=None):
    """把候选（或显式传入 notes）原子发布为该图当前配置。
    流程：备份当前已发布文件为新版本快照 → 原子写新配置 → 记录 manifest。
    任一步失败保持原文件不动。返回 (version_id, err)。"""
    if notes is None:
        notes, err = load_candidate(graph)
        if err:
            return None, u"无候选配置可发布：{}".format(err)
    if not isinstance(notes, dict):
        return None, u"待发布配置必须是 JSON 对象"

    pub_path = published_path(graph)
    vid = _unique_version_id(graph)
    snap = os.path.join(versions_dir(graph), u"{}.json".format(vid))
    # 1. 备份现有已发布文件（若有）；首次发布则把新配置本身存为快照，
    #    保证该版本也可回滚
    if os.path.exists(pub_path):
        old_data, rerr = _read_json(pub_path)
        if rerr:
            return None, u"现有已发布配置损坏，拒绝覆盖发布：{}".format(rerr)
        try:
            _atomic_write_json(snap, old_data)
        except (OSError, ValueError) as e:
            return None, u"备份当前版本失败：{}".format(e)
    else:
        try:
            _atomic_write_json(snap, notes)
        except (OSError, ValueError) as e:
            return None, u"写入版本快照失败：{}".format(e)
    # 2. 原子写新配置
    try:
        _atomic_write_json(pub_path, notes)
    except (OSError, ValueError) as e:
        return None, u"发布失败（原文件未受影响）：{}".format(e)
    # 3. manifest 记录
    _append_manifest(graph, {
        u"version": vid,
        u"time": time.strftime(u"%Y-%m-%d %H:%M:%S"),
        u"action": u"publish",
        u"content_hash": _content_hash(notes),
        u"validation": validation_report if isinstance(validation_report, dict) else {},
        u"schema_fingerprint": notes.get(u"schema_fingerprint") or u"",
    })
    _clear_caches()
    return vid, None


def rollback(graph, version):
    """回滚到已保存的历史版本。当前配置先自动备份为新快照。"""
    snap = os.path.join(versions_dir(graph),
                        u"{}.json".format(_u(version)))
    data, err = _read_json(snap)
    if err:
        return None, u"历史版本不存在或损坏：{}".format(err)
    cur_vid, perr = publish(graph, notes=data,
                            validation_report={u"overall": u"rollback"})
    if perr:
        return None, u"回滚失败：{}".format(perr)
    # 补记 action=rollback（publish 已写一条 publish 记录，追加说明）
    _append_manifest(graph, {
        u"version": cur_vid,
        u"time": time.strftime(u"%Y-%m-%d %H:%M:%S"),
        u"action": u"rollback",
        u"rolled_back_from": _u(version),
        u"content_hash": _content_hash(data),
    })
    return cur_vid, None


def read_version(graph, version):
    return _read_json(os.path.join(
        versions_dir(graph), u"{}.json".format(_u(version))))


def export_version_backup(graph, target_dir):
    """整图版本目录打包导出（运维备份用）。返回 (zip_path, err)。"""
    src = versions_dir(graph)
    if not os.path.isdir(src):
        return None, u"无版本目录可导出"
    try:
        base = tempfile.mkdtemp(prefix=u"schema_export_")
        name = u"schema_versions_{}_{}".format(
            safe_graph_dir(graph), time.strftime(u"%Y%m%d%H%M%S"))
        out = shutil.make_archive(
            os.path.join(base, name), "zip", src)
        shutil.move(out, os.path.join(target_dir, os.path.basename(out)))
        return os.path.join(target_dir, os.path.basename(out)), None
    except (OSError, ValueError) as e:
        return None, u"导出失败：{}".format(e)
