"""按 (as_of, knowledge_cutoff) 从原始事实物化当班视图。

两条不可违背的边界：
  * 只采用 occurred_at <= as_of 的事实（判断时钟是设备发生时间）；
  * 只采用 received_at <= knowledge_cutoff 的事实（该版本"知道"的全集）。
同一测点取 (occurred_at, sequence) 最大者；event_id 重送在入库前已去重。
"""
from __future__ import annotations

from .constraints import evaluate_all
from .models import CapabilityParams, Decision, LatestReading
from .protocol import DISPATCH, MEASURE_POWER, MEASURE_SOC, Event
from .timeutil import parse_ts, seconds_between


class MaterializedView:
    def __init__(
        self,
        version_id: str,
        as_of: str,
        knowledge_cutoff: str,
        params: CapabilityParams,
        events: list[Event],
    ):
        self.version_id = version_id
        self.as_of = as_of
        self.knowledge_cutoff = knowledge_cutoff
        self.params = params
        as_of_dt = parse_ts(as_of)
        cutoff_dt = parse_ts(knowledge_cutoff)

        visible = [
            e
            for e in events
            if parse_ts(e.occurred_at) <= as_of_dt
            and parse_ts(e.received_at) <= cutoff_dt
        ]
        self.visible_events = visible

        self._dispatch: list[Event] = sorted(
            (e for e in visible if e.kind == DISPATCH),
            key=lambda e: (parse_ts(e.occurred_at), e.sequence),
        )
        self.power = self._latest(MEASURE_POWER, as_of_dt)
        self.soc = self._latest(MEASURE_SOC, as_of_dt)

        # role 记录每条被采用事实的身份，供版本溯源与差异
        self.adopted: dict[str, str] = {}
        for e in visible:
            if e.kind == DISPATCH:
                self.adopted[e.event_id] = "dispatch"
            else:
                self.adopted[e.event_id] = f"telemetry.{e.measure}"

    def _latest(self, measure: str, as_of_dt) -> LatestReading | None:
        candidates = [
            e for e in self.visible_events if e.kind == "telemetry" and e.measure == measure
        ]
        if not candidates:
            return None
        chosen = max(candidates, key=lambda e: (parse_ts(e.occurred_at), e.sequence))
        value = chosen.power_kw if measure == MEASURE_POWER else chosen.soc_percent
        return LatestReading(
            event_id=chosen.event_id,
            sequence=chosen.sequence,
            occurred_at=chosen.occurred_at,
            received_at=chosen.received_at,
            value=float(value),
            raw_hash=chosen.raw_hash,
            age_s=seconds_between(as_of_dt, parse_ts(chosen.occurred_at)),
        )

    def decisions(self) -> list[Decision]:
        """对窗口相关的每条指令出决定；窗口外指令也留痕（REJECT/原因在时间线可见）。"""
        result: list[Decision] = []
        for cmd in self._dispatch:
            hits = evaluate_all(
                command_kw=cmd.power_kw,
                effective_at=cmd.occurred_at,
                as_of=self.as_of,
                power=self.power,
                soc=self.soc,
                params=self.params,
            )
            failed = [h for h in hits if h.blocking and not h.passed]
            result.append(
                Decision(
                    version_id=self.version_id,
                    dispatch_event_id=cmd.event_id,
                    outcome="REJECT" if failed else "ACCEPT",
                    effective_at=cmd.occurred_at,
                    command_kw=cmd.power_kw,
                    reasons=[f"{h.code}: {h.message}" for h in failed],
                    hits=hits,
                    sources={"power": self.power, "soc": self.soc},
                )
            )
        return result


def materialize(
    version_id: str,
    as_of: str,
    knowledge_cutoff: str,
    params: CapabilityParams,
    events: list[Event],
) -> MaterializedView:
    return MaterializedView(version_id, as_of, knowledge_cutoff, params, events)
