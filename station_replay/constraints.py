"""六条调度约束，均为纯函数：无论通过与否都产出带数值的命中证据。

判据时钟只有 occurred_at；功率正值放电、负值充电，0 为中性。
任一 blocking 规则失败 => 指令 REJECT。
"""
from __future__ import annotations

from .models import CapabilityParams, ConstraintHit, LatestReading
from .timeutil import parse_ts, seconds_between

# blocking=False 的规则只留痕（遥测滞后在缺测时不单独拦截，能量余量规则已兜底）
WINDOW = "window"
RATED = "rated"
RAMP = "ramp"
ZERO_CROSSING = "zero_crossing"
RESERVE_HEADROOM = "reserve_headroom"
STALENESS = "staleness"

ALL_CODES = (WINDOW, RATED, RAMP, ZERO_CROSSING, RESERVE_HEADROOM, STALENESS)


def check_window(effective_at: str, as_of: str, horizon_s: float) -> ConstraintHit:
    """生效窗口：effective_at <= as_of < effective_at + horizon。"""
    t0 = parse_ts(effective_at)
    ta = parse_ts(as_of)
    end = t0.timestamp() + horizon_s
    offset = seconds_between(ta, t0)
    passed = 0.0 <= offset < horizon_s
    return ConstraintHit(
        code=WINDOW,
        passed=passed,
        blocking=True,
        message=(
            f"as_of 位于指令生效窗口内（偏移 {offset:.0f}s，窗长 {horizon_s:.0f}s）"
            if passed
            else f"as_of 偏离生效窗口 [{effective_at}, +{horizon_s:.0f}s)，偏移 {offset:.0f}s"
        ),
        numbers={
            "effective_at": effective_at,
            "window_end_epoch": end,
            "as_of": as_of,
            "offset_s": offset,
            "horizon_s": horizon_s,
        },
    )


def check_rated(command_kw: float, params: CapabilityParams) -> ConstraintHit:
    """额定功率：|P_cmd| <= rated_power_kw。"""
    limit = params.rated_power_kw
    magnitude = abs(command_kw)
    passed = magnitude <= limit
    return ConstraintHit(
        code=RATED,
        passed=passed,
        blocking=True,
        message=(
            f"指令功率 {command_kw:g}kW 在额定 ±{limit:g}kW 内"
            if passed
            else f"指令功率 {command_kw:g}kW 超出额定 ±{limit:g}kW（{magnitude:g} > {limit:g}）"
        ),
        numbers={"command_kw": command_kw, "abs_command_kw": magnitude, "rated_power_kw": limit},
    )


def check_ramp(
    command_kw: float,
    effective_at: str,
    power: LatestReading | None,
    horizon_s: float,
    params: CapabilityParams,
) -> ConstraintHit:
    """爬坡：|P_cmd - P0| <= ramp_rate * dt。

    无在充/在放前值时，按 P0=0、t0=effective_at-horizon 处理（站内在窗起点应已停稳）。
    """
    if power is None:
        p0, t0_iso, dt = 0.0, None, horizon_s
        basis = "无前值功率遥测，按 P0=0、dt=horizon 核算"
    else:
        p0 = power.value
        t0_iso = power.occurred_at
        dt = seconds_between(parse_ts(effective_at), parse_ts(t0_iso))
        dt = max(dt, 0.0)
        basis = f"前值功率 {p0:g}kW @ seq {power.sequence}"
    delta = abs(command_kw - p0)
    limit = params.ramp_kw_per_s * dt
    passed = delta <= limit
    return ConstraintHit(
        code=RAMP,
        passed=passed,
        blocking=True,
        message=(
            f"爬坡可行：ΔP {delta:g}kW <= {limit:g}kW（{basis}）"
            if passed
            else f"爬坡能力不足：ΔP {delta:g}kW > 限值 {limit:g}kW，"
            f"超出 {delta - limit:g}kW（{basis}，dt={dt:.0f}s）"
        ),
        numbers={
            "command_kw": command_kw,
            "previous_kw": p0,
            "previous_event_id": power.event_id if power else None,
            "previous_occurred_at": t0_iso,
            "dt_s": dt,
            "delta_kw": delta,
            "ramp_kw_per_s": params.ramp_kw_per_s,
            "ramp_limit_kw": limit,
            "exceed_kw": max(0.0, delta - limit),
        },
    )


def check_zero_crossing(command_kw: float, power: LatestReading | None) -> ConstraintHit:
    """充放电互斥：在充（P0<0）直接要求放电（P_cmd>0），或反之，必须先过零。

    0 为中性：0→正/负、同向加力都允许。无前值视为 0。
    """
    p0 = power.value if power else 0.0
    opposite = (p0 < 0.0 < command_kw) or (command_kw < 0.0 < p0)
    if opposite:
        direction = f"充电 {p0:g}kW -> 放电 {command_kw:+g}kW" if p0 < 0 else f"放电 {p0:g}kW -> 充电 {command_kw:+g}kW"
        msg = f"充放电互斥：{direction}，未经过零台阶"
    else:
        msg = f"无换向冲突（前值 {p0:g}kW，指令 {command_kw:+g}kW，0 为中性）"
    return ConstraintHit(
        code=ZERO_CROSSING,
        passed=not opposite,
        blocking=True,
        message=msg,
        numbers={
            "previous_kw": p0,
            "previous_event_id": power.event_id if power else None,
            "command_kw": command_kw,
            "opposite_signs": opposite,
        },
    )


def check_reserve_headroom(
    command_kw: float,
    soc: LatestReading | None,
    params: CapabilityParams,
) -> ConstraintHit:
    """预留容量 / 安全余量：用区间能量与当前 SOC 核算。

    放电：soc - E/capacity*100 >= reserve_soc_pct
    充电：soc + E/capacity*100 <= soc_max_pct
    """
    horizon_h = params.dispatch_horizon_min / 60.0
    energy_kwh = abs(command_kw) * horizon_h
    soc_pct = soc.value if soc else None
    soc_event = soc.event_id if soc else None

    if soc_pct is None:
        return ConstraintHit(
            code=RESERVE_HEADROOM,
            passed=False,
            blocking=True,
            message=f"无可用 SOC 遥测，无法证明预留容量（区间需能量 {energy_kwh:g}kWh）",
            numbers={
                "soc_percent": None,
                "soc_event_id": None,
                "interval_energy_kwh": energy_kwh,
                "capacity_kwh": params.capacity_kwh,
            },
        )

    numbers = {
        "soc_percent": soc_pct,
        "soc_event_id": soc_event,
        "interval_energy_kwh": energy_kwh,
        "capacity_kwh": params.capacity_kwh,
        "horizon_min": params.dispatch_horizon_min,
    }

    if command_kw > 0:  # 放电，守预留下限
        available = (soc_pct - params.reserve_soc_pct) / 100.0 * params.capacity_kwh
        short = energy_kwh - available
        passed = short <= 0
        numbers.update(
            mode="discharge",
            reserve_soc_pct=params.reserve_soc_pct,
            available_kwh=available,
            short_kwh=max(0.0, short),
        )
        msg = (
            f"放电余量充足：可用 {available:g}kWh >= 区间需 {energy_kwh:g}kWh"
            if passed
            else f"预留容量被突破：可用余量 {available:g}kWh < 区间需 {energy_kwh:g}kWh，"
            f"缺 {short:g}kWh（SOC {soc_pct}%，预留线 {params.reserve_soc_pct}%）"
        )
    elif command_kw < 0:  # 充电，守 SOC 上限
        room = (params.soc_max_pct - soc_pct) / 100.0 * params.capacity_kwh
        over = energy_kwh - room
        passed = over <= 0
        numbers.update(mode="charge", soc_max_pct=params.soc_max_pct, available_kwh=room, over_kwh=max(0.0, over))
        msg = (
            f"充电吸纳空间充足：剩余 {room:g}kWh >= 区间需 {energy_kwh:g}kWh"
            if passed
            else f"SOC 上限将被突破：可吸纳 {room:g}kWh < 区间需 {energy_kwh:g}kWh，"
            f"超 {over:g}kWh（SOC {soc_pct}%，上限 {params.soc_max_pct}%）"
        )
    else:
        passed = True
        numbers.update(mode="idle")
        msg = "零功率指令，不占用预留容量"
    return ConstraintHit(
        code=RESERVE_HEADROOM,
        passed=passed,
        blocking=True,
        message=msg,
        numbers=numbers,
    )


def check_staleness(
    as_of: str,
    power: LatestReading | None,
    soc: LatestReading | None,
    params: CapabilityParams,
) -> ConstraintHit:
    """遥测陈旧度：逐条留痕；超阈值为 blocking 失败。"""
    ages = {}
    stale = []
    for name, reading, limit in (
        ("power_s", power, params.power_max_age_s),
        ("soc_s", soc, params.soc_max_age_s),
    ):
        if reading is None:
            ages[name] = None
            stale.append(f"{name} 缺测")
            continue
        ages[name] = reading.age_s
        ages[name.replace("_s", "_limit_s")] = limit
        if reading.age_s is not None and reading.age_s > limit:
            stale.append(f"{name} 滞后 {reading.age_s:.0f}s > {limit:.0f}s")
    # 缺测由 reserve/ramp 的兜底处理，staleness 仅在"有读数但过期"时拦截
    missing_only = all(s.endswith("缺测") for s in stale)
    passed = not stale or missing_only
    return ConstraintHit(
        code=STALENESS,
        passed=passed,
        blocking=not passed,
        message=(
            "遥测新鲜度满足要求"
            if passed
            else "遥测陈旧：" + "；".join(stale)
        ),
        numbers=ages,
    )


def evaluate_all(
    command_kw: float,
    effective_at: str,
    as_of: str,
    power: LatestReading | None,
    soc: LatestReading | None,
    params: CapabilityParams,
) -> list[ConstraintHit]:
    return [
        check_window(effective_at, as_of, params.horizon_s),
        check_rated(command_kw, params),
        check_ramp(command_kw, effective_at, power, params.horizon_s, params),
        check_zero_crossing(command_kw, power),
        check_reserve_headroom(command_kw, soc, params),
        check_staleness(as_of, power, soc, params),
    ]
