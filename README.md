# 储能电站调度回放后端

回答凌晨偏差追责的两个核心问题——**临时调频指令到达时系统看到了哪些遥测**、
**预留容量在哪一步被突破**——并把结论建立在可重启后复算的 SQLite 版本台账上。

纯 Python 3.11 标准库，零第三方依赖。

## 设计原则（现场口径）

1. **一切判定只用 `occurred_at`（发生时间）**。`received_at`（接收时间）仅决定
   某条事实在某个知识截止点（as_of）是否可见，绝不替代发生时间参与功率判断。
2. **被拒指令不改变机组状态**：爬坡、互斥的前态永远沿上一条*已接受*指令。
   越界指令带具体拒绝原因进入时间线，而不是被丢弃。
3. **内容寻址、只追加**：同一份输入集永远得到同一个 `version_id`；同一事件
   重送（event_id + 内容哈希相同）只累加 `resend_count`，不产生新版本。
4. **迟到遥测只派生新版本**：知识截止于 as_of。交班视图只认交班前到站的事实；
   补传后另派新版本，已签认版本逐字节不变（库内触发器 + 内容哈希双重保证）。
5. **封存即冻结**：班次封存后事实/版本/班次台账禁改禁删（由 SQLite 触发器
   在库内强制执行，不是应用层"自觉"）；补录仍可追加，派生的版本打 `post_seal`。

## 目录结构

```
replay/                    核心包
  engine.py                纯函数判定引擎（无 I/O，易测）
  service.py               幂等接入、版本化回放、封存、追溯、对账
  db.py                    SQLite schema 与不可变触发器
  cli.py / __main__.py     值班员命令行
  http_server.py           标准库 HTTP 后端（每线程一个 SQLite 连接）
  hashing.py               规范哈希
data/
  capability_bess_a.json   设备能力档案
  fragments/               事故片段（按到站批次拆分，含重送与迟到）
tests/test_replay.py       15 项全链路测试
reference/events.json      协议边界样例（外部系统交换边界，支持其全部字段）
```

## 快速开始

```bash
# 1. 注册能力档案、开班次
python3 -m replay --db station.db capability register data/capability_bess_a.json
python3 -m replay --db station.db shift create night-20260818 --profile BESS-A-2026 \
  --starts-at 2026-08-18T22:00:00+08:00 --ends-at 2026-08-19T06:00:00+08:00 \
  --handover-at 2026-08-19T00:00:00+08:00

# 2. 接入交班前片段（乱序、批内重送 cmd-17 自动去重）
python3 -m replay --db station.db ingest night-20260818 data/fragments/01_before_handover.json

# 3. 重建交班时刻的决策
python3 -m replay --db station.db replay night-20260818 \
  --as-of 2026-08-19T00:00:00+08:00 --note "交班视图"

# 4. 封存（自动把交班视图钉为签认版本）
python3 -m replay --db station.db seal night-20260818

# 5. 交班后迟到遥测与第三次重送入库（重送依旧不产生新版本）
python3 -m replay --db station.db ingest night-20260818 data/fragments/02_late_after_handover.json

# 6. 最新视图
python3 -m replay --db station.db replay night-20260818 --note "补传后最新视图"

# 7. 当时视图 vs 最新视图（直接回答"补传有没有改写签认结论"）
python3 -m replay --db station.db diff-signed night-20260818

# 8. 沿任一功率决定追数据版本与约束命中
python3 -m replay --db station.db trace night-20260818 cmd-17

# 9. 用版本钉住的事实集重算，与封存结果逐字节对账
python3 -m replay --db station.db verify ver_xxxxxxxxxxxx
```

HTTP 后端：

```bash
python3 -m replay --db station.db serve --host 0.0.0.0 --port 8080
```

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/capabilities` | 注册设备能力 |
| POST | `/shifts` | 开班 |
| POST | `/shifts/{id}/ingest` | 幂等接入事件 |
| POST | `/shifts/{id}/replay` | 按 `as_of` 重建视图（内容寻址） |
| POST | `/shifts/{id}/seal` | 封存并签认 |
| GET  | `/shifts/{id}/versions` | 版本列表 |
| GET  | `/shifts/{id}/signed-vs-latest` | 签认 vs 最新差异 |
| GET  | `/shifts/{id}/decisions/{event_id}/trace` | 决策血缘 |
| GET  | `/versions/{id}` / `/versions/{id}/verify` | 取版本 / 复算对账 |
| GET  | `/diff?then=&now=` | 任意两版本对比 |

## 判定模型

按发生时间排序后逐条核算，拒绝原因按固定优先级给出（全部约束的命中/未命中
明细始终完整保留在时间线里，不只是首要原因）：

1. **生效窗口** `effective_window`：档案班次窗口，或指令自带
   `valid_from`/`valid_to`（如 cmd-13）→ `OUTSIDE_WINDOW`
2. **遥测依据** `soc_available`：判定时刻最新的、已到站的 SoC → `NO_TELEMETRY`
3. **额定功率** `power_rating`：充/放额定分别考核 → `RATING_EXCEEDED`
4. **爬坡能力** `ramp_rate`：距上一条*已接受*指令的时间 × 爬坡率 → `RAMP_EXCEEDED`
5. **充放电互斥** `charge_discharge`：正=放、负=充，跨零（死区外）反向 →
   `MUTUAL_EXCLUSION`
6. **预留容量/安全余量** `reserve_margin`：按生效时长积分能量并考虑效率，
   投影 SoC 不得越过 `floor+margin` / `ceiling-margin` → `RESERVE_MARGIN`

功率符号沿用 `reference/events.json` 约定：**正值放电、负值充电**，SoC 为百分数。

## 版本与不可变性如何经受重启

* `facts` 按 `(shift_id, event_id)` 唯一，业务列由内容哈希钉住；
  触发器只允许刷新 `last_seen_at`/`resend_count` 审计列。
* `replay_versions` 与 `version_facts`（版本采用的每条事实 + 内容哈希 + 当时
  是否可见）写入即不可改、不可删；版本结果自带 `result_hash`。
* 同一 event_id 业务内容不同的"补传"按 `CONTENT_CONFLICT` **整批拒绝**。
* `verify` 用版本钉住的事实集重算，逐字节比对，并列出版本之后新增的事实。
* 所有台账在一个 SQLite 文件内（WAL 模式），进程重启后原样可读、可复算。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

15 项覆盖：引擎全约束链、被拒指令不改变状态、扩展字段保留、幂等接入与重送去重、
迟到数据派生新版本且不改写签认结论、同号冲突整批拒绝、封存冻结与 post_seal
补录、进程重启后版本/签认可恢复、决策血缘、以及完整 HTTP 端到端流程。

## 事故回放结论摘要（data/ 片段）

| 指令 | 发生时间 | 请求功率 | 结论 | 首要原因 | 当时 SoC 依据 |
|---|---|---:|---|---|---|
| cmd-13 | 23:52:00 | +100 | 拒绝 | OUTSIDE_WINDOW（自带窗口 23:50–23:51:30） | — |
| cmd-15 | 23:55:30 | +200 | 接受 | — | soc-40 (26.0%) |
| cmd-16 | 23:57:00 | +400 | 接受 | — | soc-41 (25.1%) |
| cmd-17 | 23:58:00 | +850 | 拒绝 | RAMP_EXCEEDED（450 > 420 kW）；**同时**预留容量被突破（投影 13.92% < 15% 下限+余量） | soc-41 |
| cmd-18 | 23:58:30 | −100 | 拒绝 | MUTUAL_EXCLUSION（前态仍为 cmd-16 的 +400 放电） | soc-43 |
| cmd-19 | 23:59:30 | +1500 | 拒绝 | RATING_EXCEEDED（> 1000 kW 额定） | soc-43 |

补传的 soc-42（23:57:30 发生、00:02:10 才到站）让最新视图中 cmd-17 的依据变为
soc-42、投影 SoC 变为 13.32%，但**拒绝结论与全部签认口径不变**——补传旧数据
没有、也无法改写交班时已签认的结论。

## 协议兼容性

* 现场消息的 `event_id` / `kind` / `sequence` / `occurred_at` / `received_at`
  / `power_kw` / `soc_percent` 全部支持；`reference/events.json` 可直接投喂。
* 未知扩展字段（如 `valid_from`、`valid_to`、`window_s`、现场自定义键、中文备注）
  原样保留并参与内容哈希——协议升级不破坏旧版本复算。
