"""调度回放判定引擎（纯函数，无 I/O）。

两条铁律：

1. 一切排序与判定只用 ``occurred_at``（现场发生时间）；``received_at``
   只决定某条事实在“当时”是否已经到站，绝不参与功率判断。
2. 被拒指令不改变机组状态——后续爬坡、互斥判断都沿上一条**已接受**
   指令继续推演，被拒指令原样进入时间线并附拒绝原因。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# 约束与拒绝原因（稳定字符串，直接落库、对值班员展示）
# ---------------------------------------------------------------------------

C_WINDOW = "effective_window"      # 指令生效窗口
C_RATING = "power_rating"          # 额定功率
C_RAMP = "ramp_rate"               # 爬坡能力
C_EXCLUSIVE = "charge_discharge"   # 充放电互斥（含死区）
C_RESERVE = "reserve_margin"       # 预留容量 / 安全余量
C_TELEMETRY = "soc_available"      # 荷电状态依据

R_OUTSIDE_WINDOW = "OUTSIDE_WINDOW"
R_RATING = "RATING_EXCEEDED"
R_RAMP = "RAMP_EXCEEDED"
R_EXCLUSIVE = "MUTUAL_EXCLUSION"
R_RESERVE = "RESERVE_MARGIN"
R_NO_TELEMETRY = "NO_TELEMETRY"

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"


def parse_dt(value: str | datetime) -> datetime:
    """解析协议时间字符串，保留时区（Python 3.11 fromisoformat 已兼容偏移量）。"""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def epoch(dt: datetime) -> float:
    return dt.timestamp()


def iso(dt: datetime) -> str:
    return dt.isoformat()


@dataclass(frozen=True)
class Capability:
    """设备能力档案。所有数值都在档案里留痕，判定时不接受口头参数。"""

    profile_id: str
    rated_discharge_kw: float
    rated_charge_kw: float
    ramp_kw_per_s: float
    capacity_kwh: float
    soc_floor_percent: float = 0.0
    soc_ceiling_percent: float = 100.0
    safety_margin_percent: float = 0.0
    command_window_s: float = 900.0          # 单条调频指令默认生效时长
    deadband_kw: float = 0.0                 # 充放互斥死区
    discharge_efficiency: float = 1.0        # 放电侧单程效率
    charge_efficiency: float = 1.0           # 充电侧单程效率
    effective_from: datetime | None = None
    effective_to: datetime | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Capability":
        def fnum(key: str) -> float | None:
            return float(data[key]) if data.get(key) is not None else None

        required = (
            "rated_discharge_kw",
            "rated_charge_kw",
            "ramp_kw_per_s",
            "capacity_kwh",
        )
        missing = [k for k in required if data.get(k) is None]
        if missing:
            raise ValueError(f"能力档案缺少必填字段: {', '.join(missing)}")
        return cls(
            profile_id=str(data["profile_id"]),
            rated_discharge_kw=float(data["rated_discharge_kw"]),
            rated_charge_kw=float(data["rated_charge_kw"]),
            ramp_kw_per_s=float(data["ramp_kw_per_s"]),
            capacity_kwh=float(data["capacity_kwh"]),
            soc_floor_percent=float(data.get("soc_floor_percent", 0.0)),
            soc_ceiling_percent=float(data.get("soc_ceiling_percent", 100.0)),
            safety_margin_percent=float(data.get("safety_margin_percent", 0.0)),
            command_window_s=float(data.get("command_window_s", 900.0)),
            deadband_kw=float(data.get("deadband_kw", 0.0)),
            discharge_efficiency=float(data.get("discharge_efficiency", 1.0)),
            charge_efficiency=float(data.get("charge_efficiency", 1.0)),
            effective_from=parse_dt(data["effective_from"]) if data.get("effective_from") else None,
            effective_to=parse_dt(data["effective_to"]) if data.get("effective_to") else None,
        )

    def to_dict(self) -> dict[str, Any]:
        d = {
            "profile_id": self.profile_id,
            "rated_discharge_kw": self.rated_discharge_kw,
            "rated_charge_kw": self.rated_charge_kw,
            "ramp_kw_per_s": self.ramp_kw_per_s,
            "capacity_kwh": self.capacity_kwh,
            "soc_floor_percent": self.soc_floor_percent,
            "soc_ceiling_percent": self.soc_ceiling_percent,
            "safety_margin_percent": self.safety_margin_percent,
            "command_window_s": self.command_window_s,
            "deadband_kw": self.deadband_kw,
            "discharge_efficiency": self.discharge_efficiency,
            "charge_efficiency": self.charge_efficiency,
            "effective_from": iso(self.effective_from) if self.effective_from else None,
            "effective_to": iso(self.effective_to) if self.effective_to else None,
        }
        return d


@dataclass(frozen=True)
class Fact:
    """规范化后的现场事实。extra 保留协议未知扩展字段。"""

    event_id: str
    kind: str
    sequence: int
    occurred_at: datetime
    payload: dict[str, Any]
    received_at: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def received_key(self) -> float:
        """接收排序键；协议允许接收时间缺省，缺省以发生时间兜底（仅影响可见性）。"""
        return epoch(self.received_at) if self.received_at else epoch(self.occurred_at)

    @property
    def occurred_key(self) -> float:
        return epoch(self.occurred_at)


def normalize(event: dict[str, Any]) -> Fact:
    """把外部协议事件转成内部事实。

    必需字段：event_id / kind / sequence / occurred_at；
    dispatch 另需 power_kw，telemetry 另需 soc_percent；
    received_at 可缺省，其余键全部保留为扩展字段。
    """
    for key in ("event_id", "kind", "sequence", "occurred_at"):
        if key not in event:
            raise ValueError(f"事件缺少必填字段 {key}: {event!r}")
    kind = event["kind"]
    if kind == "dispatch" and "power_kw" not in event:
        raise ValueError(f"调度指令缺少 power_kw: {event['event_id']}")
    if kind == "telemetry" and "soc_percent" not in event:
        raise ValueError(f"遥测缺少 soc_percent: {event['event_id']}")
    known = {
        "event_id", "kind", "sequence", "occurred_at", "received_at",
        "power_kw", "soc_percent",
    }
    return Fact(
        event_id=str(event["event_id"]),
        kind=kind,
        sequence=int(event["sequence"]),
        occurred_at=parse_dt(event["occurred_at"]),
        received_at=parse_dt(event["received_at"]) if event.get("received_at") else None,
        payload={k: v for k, v in event.items() if k not in ("event_id", "kind", "sequence")},
        extra={k: v for k, v in event.items() if k not in known},
    )


def _sign(value: float, deadband: float) -> int:
    if value > deadband:
        return 1
    if value < -deadband:
        return -1
    return 0


def reconstruct(
    facts: Iterable[Fact],
    capability: Capability,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """以 as_of（值班员交盘点）重建调度视图。

    返回时间线（含全部事实，迟到者标 visible=False）、逐条指令的判定
    与约束命中项、以及每个判定引用的事实编号，供事后追溯。
    """
    as_of_key = epoch(as_of) if as_of else float("inf")

    commands: list[Fact] = []
    telemetry: list[Fact] = []
    for fact in facts:
        (commands if fact.kind == "dispatch" else telemetry).append(fact)

    commands.sort(key=lambda f: (f.occurred_key, f.sequence))
    telemetry.sort(key=lambda f: (f.occurred_key, f.sequence))

    timeline: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    # 知识截止语义：as_of 之后才到站的事实不进入本版本的判定，
    # 只在时间线中留痕（visible=False）。
    visible_commands = [
        f for f in commands if f.received_key <= as_of_key + 1e-9
    ]
    hidden_commands = [
        f for f in commands if f.received_key > as_of_key + 1e-9
    ]
    visible_telemetry = [
        f for f in telemetry if f.received_key <= as_of_key + 1e-9
    ]

    # 机组当前状态：沿“已接受”指令推进
    last_accepted: Fact | None = None
    last_power = 0.0

    for cmd in visible_commands:
        t = cmd.occurred_key
        power = float(cmd.payload["power_kw"])
        # 命令到站延迟仅作审计：是否进入某版本由 as_of 可见性决定，不参与判定
        arrival_delay_s = round(cmd.received_key - t, 3) if cmd.received_at else None

        hits: list[dict[str, Any]] = []
        basis: dict[str, Any] = {
            "soc_event_id": None,
            "soc_occurred_at": None,
            "soc_percent": None,
            "prev_accepted_event_id": last_accepted.event_id if last_accepted else None,
            "prev_power_kw": last_power,
        }

        # ---- 约束 0：生效窗口（档案班次窗口 + 指令自带 valid_from/to 扩展） ----
        win_from = capability.effective_from
        win_to = capability.effective_to
        if cmd.extra.get("valid_from") is not None:
            win_from = parse_dt(cmd.extra["valid_from"])
        if cmd.extra.get("valid_to") is not None:
            win_to = parse_dt(cmd.extra["valid_to"])
        in_window = True
        win_detail: dict[str, Any] = {}
        if win_from is not None or win_to is not None:
            in_window = (win_from is None or t >= epoch(win_from) - 1e-9) and (
                win_to is None or t <= epoch(win_to) + 1e-9
            )
            win_detail = {
                "window_from": iso(win_from) if win_from else None,
                "window_to": iso(win_to) if win_to else None,
            }
        hits.append({
            "constraint": C_WINDOW, "passed": in_window,
            "detail": win_detail or {"window": "unbounded"},
        })

        # ---- 判定时刻 t 之前发生、且在 as_of 前已经到站的最新遥测 ----
        soc_fact: Fact | None = None
        for tm in visible_telemetry:
            if tm.occurred_key <= t + 1e-9:
                soc_fact = tm  # visible_telemetry 已按发生时间排序
        if soc_fact is not None:
            basis.update(
                soc_event_id=soc_fact.event_id,
                soc_occurred_at=iso(soc_fact.occurred_at),
                soc_percent=float(soc_fact.payload["soc_percent"]),
            )
        hits.append({
            "constraint": C_TELEMETRY, "passed": soc_fact is not None,
            "detail": {
                "latest_visible_telemetry": soc_fact.event_id if soc_fact else None,
            },
        })

        # ---- 约束 1：额定功率（充/放分别有额定值） ----
        rating_ok = (
            -capability.rated_charge_kw - 1e-9 <= power <= capability.rated_discharge_kw + 1e-9
        )
        hits.append({
            "constraint": C_RATING, "passed": rating_ok,
            "detail": {
                "requested_kw": power,
                "charge_limit_kw": -capability.rated_charge_kw,
                "discharge_limit_kw": capability.rated_discharge_kw,
            },
        })

        # ---- 约束 2：爬坡（沿上一条已接受指令） ----
        ramp_ok = True
        ramp_detail: dict[str, Any] = {}
        if last_accepted is not None:
            elapsed = max(t - last_accepted.occurred_key, 0.0)
            allowance = capability.ramp_kw_per_s * elapsed
            delta = abs(power - last_power)
            ramp_ok = delta <= allowance + 1e-9
            ramp_detail = {
                "elapsed_s": round(elapsed, 3),
                "delta_kw": round(delta, 3),
                "allowance_kw": round(allowance, 3),
                "ramp_kw_per_s": capability.ramp_kw_per_s,
            }
        else:
            ramp_detail = {"from_state": "idle", "note": "无前序已接受指令，免爬坡考核"}
        hits.append({"constraint": C_RAMP, "passed": ramp_ok, "detail": ramp_detail})

        # ---- 约束 3：充放电互斥（死区内视为中性，不构成反向） ----
        sign_now = _sign(power, capability.deadband_kw)
        sign_prev = _sign(last_power, capability.deadband_kw)
        exclusive_ok = not (sign_now != 0 and sign_prev != 0 and sign_now != sign_prev)
        hits.append({
            "constraint": C_EXCLUSIVE, "passed": exclusive_ok,
            "detail": {
                "requested_sign": sign_now,
                "prev_sign": sign_prev,
                "deadband_kw": capability.deadband_kw,
            },
        })

        # ---- 约束 4：预留容量 + 安全余量 ----
        duration_s = float(cmd.extra.get("window_s") or capability.command_window_s)
        reserve_ok = True
        reserve_detail: dict[str, Any] = {}
        projected_soc: float | None = None
        floor = capability.soc_floor_percent + capability.safety_margin_percent
        ceiling = capability.soc_ceiling_percent - capability.safety_margin_percent
        if soc_fact is None:
            reserve_ok = False
            reserve_detail = {"reason": "无可见荷电状态，无法核算预留容量"}
        elif sign_now != 0:
            soc = float(soc_fact.payload["soc_percent"])
            cap = capability.capacity_kwh
            if sign_now > 0:  # 放电：从电池取出的能量还要除以效率
                energy_need = power * duration_s / 3600.0 / capability.discharge_efficiency
                projected_soc = soc - energy_need / cap * 100.0
                reserve_ok = projected_soc >= floor - 1e-9
                reserve_detail = {
                    "direction": "discharge",
                    "duration_s": duration_s,
                    "energy_need_kwh": round(energy_need, 3),
                    "soc_floor_with_margin_percent": floor,
                    "projected_soc_percent": round(projected_soc, 3),
                }
            else:  # 充电：进到电池的能量要乘效率
                energy_in = (-power) * duration_s / 3600.0 * capability.charge_efficiency
                projected_soc = soc + energy_in / cap * 100.0
                reserve_ok = projected_soc <= ceiling + 1e-9
                reserve_detail = {
                    "direction": "charge",
                    "duration_s": duration_s,
                    "energy_in_kwh": round(energy_in, 3),
                    "soc_ceiling_with_margin_percent": ceiling,
                    "projected_soc_percent": round(projected_soc, 3),
                }
        else:
            reserve_detail = {"direction": "neutral", "deadband_kw": capability.deadband_kw}
        hits.append({"constraint": C_RESERVE, "passed": reserve_ok, "detail": reserve_detail})

        # ---- 汇总结论：按固定优先级给拒绝原因，全部命中明细仍完整保留 ----
        if not in_window:
            status, reason = REJECTED, R_OUTSIDE_WINDOW
        elif soc_fact is None:
            status, reason = REJECTED, R_NO_TELEMETRY
        elif not rating_ok:
            status, reason = REJECTED, R_RATING
        elif not ramp_ok:
            status, reason = REJECTED, R_RAMP
        elif not exclusive_ok:
            status, reason = REJECTED, R_EXCLUSIVE
        elif not reserve_ok:
            status, reason = REJECTED, R_RESERVE
        else:
            status, reason = ACCEPTED, None
            last_accepted = cmd
            last_power = power

        decision = {
            "event_id": cmd.event_id,
            "kind": "dispatch",
            "sequence": cmd.sequence,
            "occurred_at": iso(cmd.occurred_at),
            "received_at": iso(cmd.received_at) if cmd.received_at else None,
            "power_kw": power,
            "arrival_delay_s": arrival_delay_s,
            "status": status,
            "reason": reason,
            "effective_power_kw": last_power if status == ACCEPTED else None,
            "projected_soc_percent": round(projected_soc, 3) if projected_soc is not None else None,
            "data_basis": basis,
            "constraint_hits": hits,
        }
        decisions.append(decision)
        timeline.append(decision)

    for tm in telemetry:
        visible = tm.received_key <= as_of_key + 1e-9
        timeline.append({
            "event_id": tm.event_id,
            "kind": "telemetry",
            "sequence": tm.sequence,
            "occurred_at": iso(tm.occurred_at),
            "received_at": iso(tm.received_at) if tm.received_at else None,
            "soc_percent": float(tm.payload["soc_percent"]),
            "visible": visible,
            "late": not visible,
        })

    for cmd in hidden_commands:
        timeline.append({
            "event_id": cmd.event_id,
            "kind": "dispatch",
            "sequence": cmd.sequence,
            "occurred_at": iso(cmd.occurred_at),
            "received_at": iso(cmd.received_at) if cmd.received_at else None,
            "power_kw": float(cmd.payload["power_kw"]),
            "visible": False,
            "late": True,
        })

    timeline.sort(key=lambda row: (parse_dt(row["occurred_at"]).timestamp(), row["sequence"]))

    return {
        "as_of": iso(as_of) if as_of else None,
        "profile_id": capability.profile_id,
        "timeline": timeline,
        "decisions": decisions,
    }


def diff_replays(view_then: dict[str, Any], view_now: dict[str, Any]) -> dict[str, Any]:
    """对比同一班次两个回放版本。

    输出结论级差异：指令状态/拒绝原因/依据遥测/投影 SoC 的变化、
    新增可见的迟到事实，以及“交班签认结论是否被改写”的判定。
    """
    then_by_id = {d["event_id"]: d for d in view_then["decisions"]}
    now_by_id = {d["event_id"]: d for d in view_now["decisions"]}

    changed: list[dict[str, Any]] = []
    for event_id, now_d in sorted(now_by_id.items()):
        then_d = then_by_id.get(event_id)
        if then_d is None:
            changed.append({"event_id": event_id, "change": "new_decision", "now": _summary(now_d)})
            continue
        fields = ("status", "reason", "effective_power_kw", "projected_soc_percent")
        diffs = {
            k: {"then": then_d.get(k), "now": now_d.get(k)}
            for k in fields
            if then_d.get(k) != now_d.get(k)
        }
        if then_d["data_basis"]["soc_event_id"] != now_d["data_basis"]["soc_event_id"]:
            diffs["soc_basis"] = {
                "then": then_d["data_basis"]["soc_event_id"],
                "now": now_d["data_basis"]["soc_event_id"],
            }
        if diffs:
            changed.append({"event_id": event_id, "change": "changed", "fields": diffs})

    then_visible = {
        row["event_id"]
        for row in view_then["timeline"]
        if row["kind"] == "telemetry" and row["visible"]
    }
    now_visible = {
        row["event_id"]
        for row in view_now["timeline"]
        if row["kind"] == "telemetry" and row["visible"]
    }

    then_signed = _signed_conclusion(view_then)
    now_signed = _signed_conclusion(view_now)
    return {
        "as_of_then": view_then.get("as_of"),
        "as_of_now": view_now.get("as_of"),
        "changed_decisions": changed,
        "late_telemetry_newly_visible": sorted(now_visible - then_visible),
        "telemetry_no_longer_used": sorted(then_visible - now_visible),
        "signed_conclusion_changed": then_signed != now_signed,
        "signed_conclusion_then": then_signed,
        "signed_conclusion_now": now_signed,
    }


def _summary(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": decision["status"],
        "reason": decision["reason"],
        "effective_power_kw": decision["effective_power_kw"],
    }


def _signed_conclusion(view: dict[str, Any]) -> list[list[Any]]:
    """交班签认口径：每条指令的最终状态与生效功率。"""
    return [
        [d["event_id"], d["status"], d["reason"], d["effective_power_kw"]]
        for d in sorted(view["decisions"], key=lambda d: d["sequence"])
    ]
