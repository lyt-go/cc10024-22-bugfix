"""共享文件持久化：跨模块数据产物读写。
data/runs/     → 运行历史JSONL
data/snapshots/ → 页面快照JSON
data/reports/   → 质量报告JSON
data/exports/   → 导出文件
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
import threading
import time
from typing import Any

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_ROOT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "data"))

# 按资源键串行化的锁表：同一锚点/计划的并发确认在进程内排队，避免重复落库。
_KEY_LOCKS: dict[str, threading.RLock] = {}
_KEY_LOCKS_GUARD = threading.Lock()


@contextlib.contextmanager
def locking(key: str):
    """对同一业务键（如锚点编号）加可重入锁，串行化并发确认与编排。"""
    with _KEY_LOCKS_GUARD:
        lock = _KEY_LOCKS.setdefault(key, threading.RLock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def data_path(sub: str, filename: str) -> str:
    p = os.path.join(_DATA_ROOT, sub, filename)
    _ensure_dir(os.path.dirname(p))
    return p


def write_json(sub: str, filename: str, data: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return path


def atomic_write_json(sub: str, filename: str, data: dict[str, Any]) -> str:
    """同目录临时文件 + fsync + 原子替换，保证快照要么完整落库要么不存在。"""
    target = data_path(sub, filename)
    directory = os.path.dirname(target)
    _ensure_dir(directory)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{filename}.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)
        raise
    return target


def read_json(sub: str, filename: str) -> dict[str, Any] | None:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def append_jsonl(sub: str, filename: str, record: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    return path


def read_jsonl(sub: str, filename: str) -> list[dict[str, Any]]:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def list_files(sub: str, suffix: str = "") -> list[str]:
    path = os.path.join(_DATA_ROOT, sub)
    if not os.path.isdir(path):
        return []
    names = sorted(os.listdir(path))
    if suffix:
        names = [n for n in names if n.endswith(suffix)]
    return names


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")