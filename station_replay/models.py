"""领域数据结构与设备能力参数。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

RULE_VERSION = "1.0"


@dataclass(frozen=True)
class CapabilityParams:
    """设备能力与安全约束参数；在班次创建时固化，随每个回放版本留快照。"""

    rated_power_kw: float = 1000.0          # PCS 额定功率（充/放对称）
    ramp_kw_per_s: float = 20.0             # 爬坡速率 kW/s
    capacity_kwh: float = 2000.0            # 电池容量
    reserve_soc_pct: float = 20.0           # 放电预留 SOC 下限（%）
    soc_max_pct: float = 95.0               # 充电 SOC 上限（%）
    dispatch_horizon_min: float = 15.0      # 单条指令能量核算区间/生效窗长（分钟）
    power_max_age_s: float = 300.0          # 功率遥测最大允许滞后
    soc_max_age_s: float = 300.0            # SOC 遥测最大允许滞后

    @property
    def horizon_s(self) -> float:
        return self.dispatch_horizon_min * 60.0

    def canonical(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "CapabilityParams":
        if not data:
            return cls()
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"未知能力参数: {sorted(unknown)}")
        return cls(**data)


@dataclass(frozen=True)
class ConstraintHit:
    """一条约束规则的命中证据，所有参与计算的数值都在 numbers 里。"""

    code: str
    passed: bool
    blocking: bool
    message: str
    numbers: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LatestReading:
    """视图中某测点截至 as_of 的最新一条读数。"""

    event_id: str
    sequence: int
    occurred_at: str
    received_at: str
    value: float
    raw_hash: str
    age_s: float | None  # as_of - occurred_at；无读数时为 None


@dataclass(frozen=True)
class Decision:
    version_id: str
    dispatch_event_id: str
    outcome: str  # ACCEPT | REJECT
    effective_at: str
    command_kw: float
    reasons: list[str]
    hits: list[ConstraintHit]
    sources: dict[str, LatestReading | None]  # power / soc
