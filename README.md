# 储能电站调度回放

共享储能站调度追责回放后端：从现场原始事实（调度指令、功率/SOC 遥测）重建指定时刻的决策视图，逐条核对**指令生效窗口、额定功率、爬坡能力、充放电互斥（必须过零）、预留容量/SOC 安全余量、遥测陈旧度**，越界指令带着具体拒绝原因（含全部计算数值）进入时间线。

现场消息都带有 `event_id`、设备侧序号 `sequence` 与发生时间 `occurred_at`；`received_at` 仅用于审计与"知识截止线"，**不能替代事件发生时间参与调度判断**。功率正值表示放电，负值表示充电，荷电状态使用百分数。未知扩展字段原样保留。

## 设计要点

- **同一事件重送幂等**：`event_id` 相同即同一事实，重复录入只进 `event_redeliveries` 审计表，不重复生成决策；内容指纹不含 `received_at`（重送时间本来就不同），载荷被改则记 `payload_conflict` 并保留首条。
- **迟到遥测只派生新版本**：版本由 `(shift, as_of, knowledge_cutoff)` 唯一确定，写入后不可变；更晚的知识截止线产生新版本并以 `prev_hash` 挂接哈希链，永不覆盖旧版本。
- **视图物化只用当时知道的事实**：`occurred_at <= as_of` 且 `received_at <= knowledge_cutoff`；每个测点取 `(occurred_at, sequence)` 最大者——乱序到达但更旧的遥测（如 soc-41）可见而不被采用，**结论哈希不变**。
- **班次封存冻结**：封存版本以班次结束为 `as_of`、封存时刻为知识截止线固化签认结论；SQLite 触发器禁止再插入 cutoff 不晚于封存时刻的竞争版本。
- **WORM 由数据库强制**：`events / shifts / shift_seals / replay_versions / version_events / decisions / constraint_hits` 八张表均有 `BEFORE UPDATE/DELETE` 触发器，绕过应用层直接改库也会被 ABORT。
- **哈希链可重启校验**：每版 `summary_hash = sha256(prev_hash + 采用事实集 + 全部决策与命中项)`，封存哈希单独入 `shift_seals`；打开数据库即自动重算全链，`verify` 可随时复检。
- **可溯源**：任一功率决定可追到版本所用的 power/SOC 原始事件（event_id、序号、发生时间、值、raw_hash）与六条约束命中项。

仅依赖 Python 3.11+ 标准库（`sqlite3` / `http.server` / `zoneinfo`）。

## 快速开始

```bash
# 一条命令看完事故追责全流程（内存库）
python3 -m station_replay.demo

# 文件库：可跨进程重启验证持久性
python3 -m station_replay.demo --db station.db
```

事故样例 `reference/incident_night_20260818.json`（2026-08-18 夜班）演示：

| 视图 | as_of | 知识截止线 | 系统看到 | cmd-17 (+850kW) |
|---|---|---|---|---|
| v0 | 23:58:00 | 23:58:30 | p-57=−300kW, soc-42=24.5% | REJECT：充放电互斥 + 预留容量缺 122.5kWh |
| v1 | 23:58:00 | 00:02:00 | soc-41 乱序到达但更旧，仍取 soc-42 | 同 v0，**decisions_hash 完全一致** |
| 封存 | 00:00:00 | 00:02:00 | 交班签认结论 | WORM + 触发器冻结 |
| 最新 | 23:58:00 | 00:05:00 | p-59=0kW（00:03 补传） | REJECT：改判为**爬坡不足**（30s 限 600kW，超 250kW），预留仍破 |

## CLI

```bash
python3 -m station_replay.cli --db station.db shift create night-20260818 \
    --start 2026-08-18T22:00:00+08:00 --end 2026-08-19T00:00:00+08:00
python3 -m station_replay.cli --db station.db ingest reference/incident_night_20260818.json
python3 -m station_replay.cli --db station.db replay night-20260818 \
    --as-of 2026-08-18T23:58:00+08:00 --cutoff 2026-08-19T00:02:00+08:00
python3 -m station_replay.cli --db station.db seal night-20260818 --at 2026-08-19T00:02:00+08:00
python3 -m station_replay.cli --db station.db timeline v-night-20260818-002
python3 -m station_replay.cli --db station.db diff v-night-20260818-002 v-night-20260818-004
python3 -m station_replay.cli --db station.db trace cmd-17 --version v-night-20260818-004
python3 -m station_replay.cli --db station.db prove v-night-20260818-003 --to v-night-20260818-005
python3 -m station_replay.cli --db station.db verify
python3 -m station_replay.cli --db station.db serve --port 8080
```

## HTTP API

```
POST /api/events                          批量录入（按 event_id 幂等）
GET  /api/events/{event_id}               原始事实 + raw_hash + 重送记录
POST /api/shifts                          建班并固化能力参数
GET  /api/shifts/{shift_id}               班次与封存信息
POST /api/shifts/{shift_id}/seal          封存当班结论
POST /api/replays                         {"shift_id","as_of","knowledge_cutoff"}
GET  /api/replays?shift_id=               版本列表（父子链、哈希、封存标记）
GET  /api/replays/{vid}                   版本详情
GET  /api/replays/{vid}/timeline          决策时间线（拒绝原因 + 全部命中数值）
GET  /api/diff?from=&to=                  两版视图差异（采用事实 + 约束翻转）
GET  /api/decisions/{cid}?version=        决定溯源（所用读数版本 + 约束命中项）
GET  /api/proof/{sealed_vid}?to_version=  封存未改写证明
GET  /api/verify?shift_id=                哈希链重算校验
```

## 约束规则（每条都出证，数值随命中项落库）

1. `window` 生效窗口：`effective_at <= as_of < effective_at + 15min`
2. `rated` 额定功率：`|P| <= 1000kW`
3. `ramp` 爬坡：`|P_cmd − P0| <= 20kW/s × dt`（无前值按 P0=0、dt=窗长）
4. `zero_crossing` 充放电互斥：在充直接要求放电（或反向）即失败，0 为中性必须先过零
5. `reserve_headroom` 预留容量：放电后 SOC 不得低于 20%（充电不得高于 95%），按区间能量 `|P|×0.25h` 核算
6. `staleness` 遥测陈旧：功率/SOC 读数滞后超 300s 拦截（缺测由能量/爬坡规则兜底）

能力参数在**建班时固化**并随每个版本留快照（`CapabilityParams`，可按站配置）。

## 代码结构

```
station_replay/
  protocol.py     现场协议解析、扩展字段保留、内容指纹
  timeutil.py     ISO 时间（occurred_at 为唯一判据时钟）
  models.py       CapabilityParams / ConstraintHit / Decision
  constraints.py  六条纯函数约束（带数值证据）
  reconstruct.py  (as_of, knowledge_cutoff) 视图物化
  storage.py      SQLite DDL、WORM/封存触发器、幂等录入、哈希链、封存与校验
  api.py          stdlib HTTP JSON API
  cli.py          命令行
  demo.py         事故片段完整追责回放
reference/
  events.json                    协议边界样例（原有）
  incident_night_20260818.json   完整事故片段
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：六条约束数值、协议解析与指纹、重送幂等/载荷冲突、乱序遥测不改结论、迟到功率派生新版本与原因改判、封存触发器冻结、WORM 直改库拦截、跨进程重启哈希链一致、HTTP API 端到端。
