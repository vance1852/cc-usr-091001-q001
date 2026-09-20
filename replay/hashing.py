"""规范化哈希工具。

版本、能力档案与封存指纹都走同一份字节序列，保证
“同输入必同版本、跨进程可复算”。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    """把任意 JSON 可序列化对象压成稳定字节。

    - 键排序、紧凑分隔；
    - 不转义非 ASCII，便于事故记录里直接出现中文；
    - 未知对象退回字符串，避免现场扩展字段导致整批不可入库。
    """
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def sha12(value: Any) -> str:
    """取 SHA-256 前 12 位作为人类可读的内容编号。"""
    return hashlib.sha256(canonical_bytes(value)).hexdigest()[:12]
