"""现场协议边界。

外部系统交换字段：
  event_id / kind(dispatch|telemetry) / sequence / occurred_at
  power_kw（指令与功率遥测；正放负充）/ soc_percent（荷电百分数）
  received_at（接收时间，仅审计与知识截止线使用）
未知扩展字段整体保留在 extras 中，协议升级不丢信息。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .timeutil import parse_ts

DISPATCH = "dispatch"
TELEMETRY = "telemetry"

# kind=telemetry 时用 measure 区分测点
MEASURE_POWER = "power"
MEASURE_SOC = "soc"

_KNOWN_FIELDS = {
    "event_id",
    "kind",
    "sequence",
    "occurred_at",
    "received_at",
    "power_kw",
    "soc_percent",
    "measure",
}


class ProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    event_id: str
    kind: str  # dispatch | telemetry
    sequence: int
    occurred_at: str  # ISO 字符串原样保存
    received_at: str
    measure: str | None  # telemetry: power | soc；dispatch 为 None
    power_kw: float | None
    soc_percent: float | None
    extras: dict[str, Any] = field(default_factory=dict)
    raw_hash: str = ""

    def as_payload(self) -> dict[str, Any]:
        """重建规范化前的业务载荷（含扩展字段与 received_at）。"""
        payload = {
            "event_id": self.event_id,
            "kind": self.kind,
            "sequence": self.sequence,
            "occurred_at": self.occurred_at,
            "received_at": self.received_at,
        }
        if self.power_kw is not None:
            payload["power_kw"] = self.power_kw
        if self.soc_percent is not None:
            payload["soc_percent"] = self.soc_percent
        if self.measure is not None:
            payload["measure"] = self.measure
        payload.update(self.extras)
        return payload

    def content_payload(self) -> dict[str, Any]:
        """用于内容指纹的载荷：不含 received_at。

        同一事件每次重送的接收时间必然不同，这属于审计信息而非业务事实；
        内容是否被篡改只看设备侧字段（序号、发生时间、量测值与扩展字段）。
        """
        payload = self.as_payload()
        payload.pop("received_at", None)
        return payload


def canonical_json(payload: dict[str, Any]) -> str:
    """确定性 JSON：键排序、无空白，作为哈希输入。"""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def raw_hash_of(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def parse_event(raw: dict[str, Any]) -> Event:
    """校验并解析一条现场消息；缺失/非法字段抛 ProtocolError。"""
    if not isinstance(raw, dict):
        raise ProtocolError(f"事件必须是对象: {raw!r}")
    try:
        event_id = str(raw["event_id"])
        kind = str(raw["kind"])
        sequence = int(raw["sequence"])
        occurred_at = str(raw["occurred_at"])
        received_at = str(raw["received_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"事件缺少必需字段或类型错误: {raw!r}") from exc

    if not event_id:
        raise ProtocolError("event_id 不能为空")
    if kind not in (DISPATCH, TELEMETRY):
        raise ProtocolError(f"未知 kind: {kind!r}（仅支持 dispatch/telemetry，未知测点请放扩展字段）")

    # 触发时间格式与时区校验
    try:
        parse_ts(occurred_at)
        parse_ts(received_at)
    except ValueError as exc:
        raise ProtocolError(str(exc)) from exc

    power = raw.get("power_kw")
    soc = raw.get("soc_percent")
    measure_in = raw.get("measure")

    if kind == DISPATCH:
        if power is None:
            raise ProtocolError(f"调度指令 {event_id} 缺少 power_kw")
        measure = None
    else:
        # telemetry 必须能唯一归入一个测点：显式 measure，或按值字段推断
        if measure_in is not None:
            measure = str(measure_in)
            if measure not in (MEASURE_POWER, MEASURE_SOC):
                raise ProtocolError(f"遥测 {event_id} 的 measure 非法: {measure!r}")
        elif power is not None and soc is None:
            measure = MEASURE_POWER
        elif soc is not None and power is None:
            measure = MEASURE_SOC
        else:
            raise ProtocolError(
                f"遥测 {event_id} 必须用 measure 指明 power/soc，"
                "且恰好携带 power_kw 或 soc_percent 之一"
            )
        if measure == MEASURE_POWER and power is None:
            raise ProtocolError(f"功率遥测 {event_id} 缺少 power_kw")
        if measure == MEASURE_SOC and soc is None:
            raise ProtocolError(f"SOC 遥测 {event_id} 缺少 soc_percent")

    power_val = float(power) if power is not None else None
    soc_val = float(soc) if soc is not None else None
    if soc_val is not None and not (0.0 <= soc_val <= 100.0):
        raise ProtocolError(f"遥测 {event_id} soc_percent 越界: {soc_val}")

    extras = {k: v for k, v in raw.items() if k not in _KNOWN_FIELDS}

    event = Event(
        event_id=event_id,
        kind=kind,
        sequence=sequence,
        occurred_at=occurred_at,
        received_at=received_at,
        measure=measure,
        power_kw=power_val,
        soc_percent=soc_val,
        extras=extras,
    )
    object.__setattr__(event, "raw_hash", raw_hash_of(event.content_payload()))
    return event
