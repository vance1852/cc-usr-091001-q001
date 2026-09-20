"""事故回放演示：值班员输入事故片段，重建当班视图并完成追责核对。

时间线：
  v0  as_of=23:58:00  cutoff=23:58:30  soc-41 尚未到达
  v1  as_of=23:58:00  cutoff=00:02:00  soc-41 已到但更旧，不被采用 -> 结论哈希与 v0 相同
  v_s as_of=00:00:00  cutoff=00:02:00  交班封存版本（签认结论）
  v2  as_of=23:58:00  cutoff=00:05:00  p-59 补传后的最新视图（指令时刻）
  v3  as_of=00:00:00  cutoff=00:05:00  封存视图的补传后对照版（同 as_of，仅截止线不同）

用法：python -m station_replay.demo [--db PATH] [--json incident.json]
不指定 --db 时使用内存库（纯演示）；指定文件可跨进程验证持久性。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .models import CapabilityParams
from .storage import Storage
from .timeutil import parse_ts

T_ASK = "2026-08-18T23:58:00+08:00"          # 临时调频指令发生时刻
T_PRE_SOC41 = "2026-08-18T23:58:30+08:00"    # soc-41（23:59:05 到）之前的截止线
T_SHIFT_END = "2026-08-19T00:00:00+08:00"
T_SEAL = "2026-08-19T00:02:00+08:00"         # 交班封存时刻
T_LATEST = "2026-08-19T00:05:00+08:00"       # 补传后的最新知识截止线


def _hr(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _show_decision(timeline: dict, dispatch_id: str) -> None:
    dec = next(d for d in timeline["decisions"] if d["dispatch_event_id"] == dispatch_id)
    print(f"  指令 {dispatch_id} @ {dec['effective_at']}  P={dec['command_kw']:g}kW"
          f"  => {dec['outcome']}")
    for hit in dec["hits"]:
        mark = "PASS" if hit["passed"] else "FAIL"
        print(f"    [{mark}] {hit['code']:<17} {hit['message']}")
    if dec["reasons"]:
        print("  拒绝原因：")
        for r in dec["reasons"]:
            print(f"    - {r}")


def run(db_path: str = ":memory:", incident: str | Path | None = None) -> Storage:
    incident = incident or Path(__file__).parents[1] / "reference" / "incident_night_20260818.json"
    pack = json.loads(Path(incident).read_text(encoding="utf-8"))
    shift = pack["shift"]
    sid = shift["shift_id"]

    store = Storage.open(db_path)

    _hr("1. 建立班次（设备能力参数在此时固化）")
    params = CapabilityParams.from_dict(pack["capability"])
    print(json.dumps(store.create_shift(
        sid, shift["start_at"], shift["end_at"], params
    ), ensure_ascii=False, indent=2))

    seal_dt = parse_ts(T_SEAL)
    on_shift_events = [e for e in pack["events"] if parse_ts(e["received_at"]) <= seal_dt]
    late_events = [e for e in pack["events"] if parse_ts(e["received_at"]) > seal_dt]

    _hr("2. 录入封存前可见事实（含 cmd-17 重送；soc-41 乱序晚到、p-59 封存后才到）")
    for r in store.add_events(on_shift_events):
        print(f"  {r['event_id']:<8} {r['status']}")

    _hr("3. 视图 v0：soc-41 到达前（cutoff=23:58:30），系统在指令时刻看到什么")
    v0 = store.create_version(sid, T_ASK, T_PRE_SOC41)
    tl0 = store.get_timeline(v0["version_id"])
    print(f"  {v0['version_id']} 采用事实：{sorted(tl0['adopted_events'])}")
    _show_decision(tl0, "cmd-17")

    _hr("4. 迟到的 soc-41（occurred 23:56:30，比 soc-42 更旧）到达，重算视图 v1")
    v1 = store.create_version(sid, T_ASK, T_SEAL)
    tl1 = store.get_timeline(v1["version_id"])
    print(f"  {v1['version_id']} 采用事实：{sorted(tl1['adopted_events'])}")
    print(f"  soc-41 可见但未被采用（测点取 occurred_at+sequence 最新者，仍是 soc-42）")
    same_conclusion = v0["decisions_hash"] == v1["decisions_hash"]
    print(f"  decisions_hash v0={v0['decisions_hash'][:16]}… v1={v1['decisions_hash'][:16]}…")
    print(f"  >> 补传更旧遥测未改变任何结论：{same_conclusion}")
    _show_decision(tl1, "cmd-17")

    _hr("5. 同一事件重送：再次录入 cmd-17，不得重复改变结果")
    redeliver = next(e for e in pack["events"] if e["event_id"] == "cmd-17")
    print(" ", store.add_events([redeliver]))
    v1_again = store.create_version(sid, T_ASK, T_SEAL)
    print(f"  同一切片仍返回同一版本 {v1_again['version_id']}"
          f"（幂等：{v1_again['version_id'] == v1['version_id']}）")

    _hr("6. 交班封存：以班次结束为 as_of 固化当班结论（WORM + 触发器冻结）")
    seal = store.seal_shift(sid, T_SEAL)
    sealed_vid = seal["version_id"]
    print(json.dumps(seal, ensure_ascii=False, indent=2))
    print(f"  WORM 自检（尝试 UPDATE events）：{store.worm_attempt('events')}")
    print(f"  WORM 自检（尝试 DELETE decisions）：{store.worm_attempt('decisions')}")

    _hr("7. 封存后补传 p-59（功率 0kW @23:57:30，00:03:10 才收到）")
    # 先证明封存冻结：试图用一个封存前不存在、但 cutoff 不晚于封存时刻的切片顶替当班视图
    from .storage import StorageError
    try:
        store.create_version(sid, "2026-08-18T23:59:00+08:00", T_SEAL)
        print("  异常：竞争版本竟被接受！")
    except StorageError as exc:
        print(f"  竞争版本被封存触发器拒绝：{exc}")
    for r in store.add_events(late_events):
        print(f"  {r['event_id']:<8} {r['status']}")
    v2 = store.create_version(sid, T_ASK, T_LATEST)
    v3 = store.create_version(sid, T_SHIFT_END, T_LATEST)
    tl2 = store.get_timeline(v2["version_id"])
    print(f"  派生新版本 {v2['version_id']}（父 {v2['parent_version_id']}）与"
          f" {v3['version_id']}（父 {v3['parent_version_id']}），封存版本保持原样")
    print(f"  {v2['version_id']} 采用事实：{sorted(tl2['adopted_events'])}")
    _show_decision(tl2, "cmd-17")

    _hr("8. 当班视图 v1 vs 最新视图 v2 差异（同一指令时刻）")
    diff = store.diff_versions(v1["version_id"], v2["version_id"])
    print(f"  新增采用事实：{diff['events_added']}")
    print(f"  移除采用事实：{diff['events_removed']}")
    for ch in diff["decisions_changed"]:
        print(f"  指令 {ch['dispatch_event_id']}：{ch['from_outcome']} -> {ch['to_outcome']}")
        print(f"    当班拒绝原因：")
        for r in ch["from_reasons"]:
            print(f"      - {r}")
        print(f"    最新拒绝原因：")
        for r in ch["to_reasons"]:
            print(f"      - {r}")
        for flip in ch["constraint_flips"]:
            print(f"    约束翻转 {flip['code']}: passed {flip['from_passed']} -> {flip['to_passed']}")

    _hr("9. 沿 cmd-17 功率决定溯源：数据版本、原始读数、约束命中项")
    trace = store.trace_decision(v2["version_id"], "cmd-17")
    for kind in ("power", "soc"):
        r = trace["used_readings"][kind]
        if r:
            print(f"  {kind:<5} 采用 {r['event_id']}(seq {r['sequence']})"
                  f" occurred={r['occurred_at']} value={r['value']}"
                  f" raw_hash={r['raw_hash'][:12]}…")
        else:
            print(f"  {kind:<5} 无可用读数")
    print("  全部命中项：", [h["code"] for h in trace["decision"]["hits"]])

    _hr("10. 未改写证明：封存版本 vs 补传后同 as_of 新版本")
    proof = store.proof_untampered(sealed_vid, v3["version_id"])
    print(f"  封存版本：{proof['seal']['version_id']}  封存于 {proof['seal']['sealed_at']}")
    print(f"  封存 summary_hash：{proof['sealed_version_summary_hash']}")
    print(f"  哈希链校验：ok={proof['chain']['ok']} {proof['chain']['problems']}")
    cmp_ = proof["comparison"]
    print(f"  对照版本新增事实：{cmp_['events_added_in_later']}；移除：{cmp_['events_removed_in_later']}")
    for ch in cmp_["decisions_changed"]:
        print(f"  指令 {ch['dispatch_event_id']} 结论 {ch['from_outcome']} -> {ch['to_outcome']}（新版本内容，封存版本未动）")
    print(f"  >> {proof['conclusion']}")

    _hr("11. 全链重算校验（重启后打开数据库会自动执行同一检查）")
    report = store.verify_all_chains()
    print(json.dumps(report, ensure_ascii=False, indent=2))

    return store


def main() -> None:
    ap = argparse.ArgumentParser(description="储能站事故调度回放演示")
    ap.add_argument("--db", default=":memory:", help="SQLite 路径（默认内存库）")
    ap.add_argument("--json", dest="incident", default=None, help="事故片段 JSON")
    args = ap.parse_args()
    run(args.db, args.incident)


if __name__ == "__main__":
    main()
