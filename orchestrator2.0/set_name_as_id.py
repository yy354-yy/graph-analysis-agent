# -*- coding: utf-8 -*-
"""一次性运维工具：把 schema_notes 的唯一标识属性切换为「名称」属性。

背景：图库自带的外部唯一标识 id~ 值晦涩（如 "200ct 26"），未配置主键时
解析链会回退到它。业务名称本身带编码前缀（如 Z0001001001+日2010通关管理），
可读且大概率唯一，可改用名称做唯一标识。

用法（在部署目录，推荐 root 运行——配置文件可能是 root 写的）：
  python2 set_name_as_id.py                 # 处理默认 schema_notes.json
  python2 set_name_as_id.py 海关业务底账-0109_1   # 处理图专属配置
  python2 set_name_as_id.py --revert 海关业务底账-0109_1  # 从备份回滚

做什么：
  1. business_search[*].id_prop   -> 该类型名称属性（name_prop）
  2. name_properties[类型].id     -> 同上
  两处必须一致，否则标识解析判 conflict 拒绝锚定。
  原文件自动备份为 *.bak_时间戳，出错不落盘。
改完重启服务，再走向导④校验：主键实查会验证名称是否真的全类型唯一，
有重复名称的类型会在报告里明确列出（那种类型不应使用名称当主键）。
"""
from __future__ import print_function

import io
import json
import os
import shutil
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))


def _load(path):
    with io.open(path, encoding="utf-8") as f:
        return json.load(f)


def _dump(path, data):
    content = json.dumps(data, ensure_ascii=False, indent=2)
    if not isinstance(content, type(u"")):
        content = content.decode(u"utf-8")
    json.loads(content)  # 写前校验
    with io.open(path, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    args = [a.decode("utf-8") if isinstance(a, bytes) else a
            for a in sys.argv[1:]]
    revert = False
    if args and args[0] == u"--revert":
        revert = True
        args = args[1:]
    graph = args[0] if args else None
    fname = (u"schema_notes.{}.json".format(graph) if graph
             else u"schema_notes.json")
    path = os.path.join(BASE, fname)
    if not os.path.exists(path):
        print(u"配置不存在：{}".format(path))
        sys.exit(1)

    if revert:
        baks = sorted(x for x in os.listdir(BASE)
                      if x.startswith(fname + u".bak_"))
        if not baks:
            print(u"无备份可回滚：{}".format(fname))
            sys.exit(1)
        bak = os.path.join(BASE, baks[-1])
        shutil.copyfile(bak, path)
        print(u"已从 {} 回滚到 {}".format(baks[-1], fname))
        return

    notes = _load(path)
    changed = []
    # 1. business_search：id_prop 换成名称属性
    for c in (notes.get(u"business_search") or []):
        if not isinstance(c, dict):
            continue
        np = (c.get(u"name_prop") or u"名称").strip()
        if np and c.get(u"id_prop") != np:
            changed.append(u"{}.id_prop: {} -> {}".format(
                c.get(u"label"), c.get(u"id_prop"), np))
            c[u"id_prop"] = np
    # 2. name_properties：同步主键字段
    nps = notes.get(u"name_properties") or {}
    if isinstance(nps, dict):
        for lb, cfg in nps.items():
            if not isinstance(cfg, dict):
                continue
            np = (cfg.get(u"name") or u"名称").strip()
            if np and cfg.get(u"id") != np:
                changed.append(u"name_properties.{}.id: {} -> {}".format(
                    lb, cfg.get(u"id"), np))
                cfg[u"id"] = np

    if not changed:
        print(u"无需修改：唯一标识已经是名称属性")
        return
    bak = u"{}.bak_{}".format(
        path, time.strftime(u"%Y%m%d%H%M%S"))
    shutil.copyfile(path, bak)
    _dump(path, notes)
    print(u"已修改 {} 项（备份：{}）".format(len(changed),
                                          os.path.basename(bak)))
    for x in changed:
        print(u"  " + x)
    print(u"完成。请重启服务后走向导④校验（主键实查会验证名称唯一性）。")


if __name__ == "__main__":
    main()
