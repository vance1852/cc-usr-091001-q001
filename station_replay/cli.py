"""命令行入口（与 HTTP API 对应）。

示例：
  python -m station_replay.cli --db station.db shift create night-20260818 \
      --start 2026-08-18T22:00:00+08:00 --end 2026-08-19T00:00:00+08:00
  python -m station_replay.cli --db station.db ingest reference/incident_night_20260818.json
  python -m station_replay.cli --db station.db replay night-20260818 \
      --as-of 2026-08-18T23:58:00+08:00 --cutoff 2026-08-19T00:02:00+08:00
  python -m station_replay.cli --db station.db seal night-20260818 --at 2026-08-19T00:02:00+08:00
  python -m station_replay.cli --db station.db timeline v-night-20260818-001
  python -m station_replay.cli --db station.db diff v-night-20260818-001 v-night-20260818-002
  python -m station_replay.cli --db station.db trace cmd-17 --version v-night-20260818-002
  python -m station_replay.cli --db station.db prove v-night-20260818-003 --to v-night-20260818-004
  python -m station_replay.cli --db station.db verify
  python -m station_replay.cli --db station.db serve --port 8080
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import demo as demo_mod
from .api import serve
from .models import CapabilityParams
from .storage import NotFound, Storage, StorageError


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="station_replay", description="储能站调度回放后端")
    ap.add_argument("--db", default="station_replay.db", help="SQLite 路径")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("ingest", help="录入事故片段/事件 JSON（文件可为事件数组或含 events 键的包）")
    p.add_argument("file")

    p = sub.add_parser("shift", help="班次操作")
    ss = p.add_subparsers(dest="subcmd", required=True)
    pc = ss.add_parser("create")
    pc.add_argument("shift_id")
    pc.add_argument("--start", required=True)
    pc.add_argument("--end", required=True)
    pc.add_argument("--params", help="能力参数 JSON 文件")
    ps = ss.add_parser("seal")
    ps.add_argument("shift_id")
    ps.add_argument("--at", default=None, help="封存时刻（默认现在）")
    pg = ss.add_parser("get")
    pg.add_argument("shift_id")

    p = sub.add_parser("replay", help="派生（或取回）指定 as_of/cutoff 的不可变回放版本")
    p.add_argument("shift_id")
    p.add_argument("--as-of", required=True)
    p.add_argument("--cutoff", required=True)

    p = sub.add_parser("versions", help="列出版本")
    p.add_argument("shift_id")

    p = sub.add_parser("timeline")
    p.add_argument("version_id")

    p = sub.add_parser("diff")
    p.add_argument("from_version")
    p.add_argument("to_version")

    p = sub.add_parser("trace", help="沿一条指令追到所用读数与约束命中项")
    p.add_argument("dispatch_event_id")
    p.add_argument("--version", required=True)

    p = sub.add_parser("event", help="查看原始事实与重送记录")
    p.add_argument("event_id")

    p = sub.add_parser("prove", help="封存版本未改写证明")
    p.add_argument("sealed_version")
    p.add_argument("--to", required=True)

    p = sub.add_parser("verify", help="全链重算校验")
    p.add_argument("--shift-id", default=None)

    p = sub.add_parser("serve", help="启动 HTTP 服务")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)

    p = sub.add_parser("demo", help="用内置事故片段跑完整追责演示")
    p.add_argument("--json", default=None)

    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "demo":
        demo_mod.run(args.db, args.json)
        return 0
    if args.cmd == "serve":
        serve(args.db, args.host, args.port)
        return 0

    store = Storage.open(args.db)
    try:
        if args.cmd == "ingest":
            data = json.loads(Path(args.file).read_text(encoding="utf-8"))
            events = data["events"] if isinstance(data, dict) and "events" in data else data
            _print({"results": store.add_events(events)})
        elif args.cmd == "shift":
            if args.subcmd == "create":
                params = CapabilityParams()
                if args.params:
                    raw = json.loads(Path(args.params).read_text(encoding="utf-8"))
                    params = CapabilityParams.from_dict(
                        raw.get("capability", raw.get("params", raw))
                    )
                _print(store.create_shift(args.shift_id, args.start, args.end, params))
            elif args.subcmd == "seal":
                _print(store.seal_shift(args.shift_id, args.at))
            else:
                _print(store.get_shift(args.shift_id))
        elif args.cmd == "replay":
            _print(store.create_version(args.shift_id, args.as_of, args.cutoff))
        elif args.cmd == "versions":
            _print({"versions": store.list_versions(args.shift_id)})
        elif args.cmd == "timeline":
            _print(store.get_timeline(args.version_id))
        elif args.cmd == "diff":
            _print(store.diff_versions(args.from_version, args.to_version))
        elif args.cmd == "trace":
            _print(store.trace_decision(args.version, args.dispatch_event_id))
        elif args.cmd == "event":
            _print(store.get_event(args.event_id))
        elif args.cmd == "prove":
            _print(store.proof_untampered(args.sealed_version, args.to))
        elif args.cmd == "verify":
            _print(store.verify_chain(args.shift_id) if args.shift_id else store.verify_all_chains())
        return 0
    except NotFound as exc:
        print(f"未找到: {exc}", file=sys.stderr)
        return 404
    except (StorageError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 400
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
