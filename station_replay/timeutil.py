"""时间工具：所有调度判断只用 occurred_at（设备侧发生时间）。

received_at 仅用于确定"某个回放版本在重算时知道哪些事实"（知识截止线），
绝不参与发生时间排序与约束计算。
"""
from __future__ import annotations

from datetime import datetime, timezone


def parse_ts(value: str | datetime) -> datetime:
    """解析 ISO-8601 时间戳；结果一定是带时区的 aware datetime。"""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"时间戳缺少时区信息: {value!r}")
    return dt


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def seconds_between(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds()
