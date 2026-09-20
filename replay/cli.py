"""命令行入口：值班员直接操作回放台账。

示例：
    python -m replay capability register data/capability.json --db station.db
    python -m replay shift create night-0818 --profile BESS-A ...
    python -m replay ingest night-0818 data/part1.json
    python -m replay replay night-0818 --as-of 2026-08-19T00:00:00+08:00
    python -m replay seal night-0818
    python -m replay ingest night-0818 data/late_fragments.json
    python -m replay diff-signed night-0818
    python -m replay trace night-0818 cmd-17
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .service import ReplayError, ReplayService


def _load_json(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _events_from(payload: Any) -> list[dict[str, Any]]:
    """兼容三种投喂格式：裸事件列表、reference/events.json 信封、批量信封。"""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("events"), list):
            return payload["events"]
        if isinstance(payload.get("items"), list):
            return payload["items"]
    raise SystemExit("输入文件需为事件列表或含 events 字段的信封")


def _print(obj: Any) -> None:
    json.dump(obj, sys.stdout, ensure_ascii=False, indent=2, default=str)
    sys.stdout.write("\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m replay", description="储能电站调度回放后端")
    p.add_argument("--db", default="station.db", help="SQLite 台账路径（默认 station.db）")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("capability", help="注册设备能力档案（同内容幂等）")
    cap = sp.add_subparsers(dest="action", required=True)
    cap_add = cap.add_parser("register")
    cap_add.add_argument("file")

    sp = sub.add_parser("shift", help="班次台账")
    sh = sp.add_subparsers(dest="action", required=True)
    sh_new = sh.add_parser("create")
    sh_new.add_argument("shift_id")
    sh_new.add_argument("--profile", required=True)
    sh_new.add_argument("--starts-at", required=True)
    sh_new.add_argument("--ends-at", required=True)
    sh_new.add_argument("--handover-at", required=True)
    sh_get = sh.add_parser("show")
    sh_get.add_argument("shift_id")

    ing = sub.add_parser("ingest", help="接入一批事件（乱序、重复均可）")
    ing.add_argument("shift_id")
    ing.add_argument("file")

    rp = sub.add_parser("replay", help="按指定交盘点重建视图")
    rp.add_argument("shift_id")
    rp.add_argument("--as-of", dest="as_of", default=None, help="缺省=以全部已到事实重建最新视图")
    rp.add_argument("--note", default=None)

    sub.add_parser("versions", help="（需接 shift_id）").add_argument("shift_id")

    seal = sub.add_parser("seal", help="封存班次并钉死签认版本")
    seal.add_argument("shift_id")
    seal.add_argument("--handover-at", default=None)

    df = sub.add_parser("diff", help="对比两个回放版本")
    df.add_argument("then_version_id")
    df.add_argument("now_version_id")

    dfs = sub.add_parser("diff-signed", help="签认版本 vs 最新版本（补传是否改写结论）")
    dfs.add_argument("shift_id")

    tr = sub.add_parser("trace", help="沿一条功率指令追溯数据版本与约束命中")
    tr.add_argument("shift_id")
    tr.add_argument("event_id")

    vf = sub.add_parser("verify", help="重算版本并与封存结果逐字节对账")
    vf.add_argument("version_id")

    gv = sub.add_parser("show-version", help="查看某版本完整时间线")
    gv.add_argument("version_id")

    sv = sub.add_parser("serve", help="启动 HTTP 后端")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8080)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with ReplayService(args.db) as svc:
        try:
            if args.cmd == "capability" and args.action == "register":
                pid = svc.register_capability(_load_json(args.file))
                _print({"registered": pid})

            elif args.cmd == "shift" and args.action == "create":
                _print(svc.create_shift(args.shift_id, args.profile,
                                        args.starts_at, args.ends_at, args.handover_at))
            elif args.cmd == "shift" and args.action == "show":
                _print(svc.get_shift(args.shift_id))

            elif args.cmd == "ingest":
                _print(svc.ingest(args.shift_id, _events_from(_load_json(args.file))))

            elif args.cmd == "replay":
                _print(svc.replay(args.shift_id, as_of=args.as_of, note=args.note))

            elif args.cmd == "versions":
                _print(svc.list_versions(args.shift_id))

            elif args.cmd == "seal":
                _print(svc.seal_shift(args.shift_id, args.handover_at))

            elif args.cmd == "diff":
                _print(svc.diff_versions(args.then_version_id, args.now_version_id))

            elif args.cmd == "diff-signed":
                _print(svc.signed_vs_latest(args.shift_id))

            elif args.cmd == "trace":
                _print(svc.trace_decision(args.shift_id, args.event_id))

            elif args.cmd == "verify":
                _print(svc.verify_version(args.version_id))

            elif args.cmd == "show-version":
                _print(svc.get_version(args.version_id))

            elif args.cmd == "serve":
                from .http_server import serve
                serve(svc, args.host, args.port)
                return 0

        except ReplayError as exc:
            _print({"error": exc.code, "message": str(exc)})
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
