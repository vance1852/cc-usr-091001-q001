"""JSON HTTP API（仅依赖标准库），让值班终端可直接查询回放结果。

路由：
  POST /api/events                         批量录入（按 event_id 幂等）
  GET  /api/events/{event_id}              原始事实、raw_hash 与重送记录
  POST /api/shifts                         建班并固化能力参数
  GET  /api/shifts/{shift_id}              班次与封存信息
  POST /api/shifts/{shift_id}/seal         封存当班结论
  POST /api/replays                        {"shift_id","as_of","knowledge_cutoff"} 派生版本
  GET  /api/replays?shift_id=              版本列表
  GET  /api/replays/{vid}                  版本详情（哈希、参数、父版本）
  GET  /api/replays/{vid}/timeline         决策时间线（含拒绝原因与命中数值）
  GET  /api/diff?from=&to=                 两版视图差异
  GET  /api/decisions/{cid}?version=       决定溯源（所用读数 + 约束命中项）
  GET  /api/proof/{sealed_vid}?to_version= 封存未改写证明
  GET  /api/verify?shift_id=               哈希链重算校验
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .storage import NotFound, Storage, StorageError


class _Handler(BaseHTTPRequestHandler):
    server_version = "StationReplay/1.0"

    # ---- 工具 ----
    def _send(self, status: int, body) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _query(self, key: str, required: bool = True) -> str | None:
        qs = parse_qs(urlparse(self.path).query)
        values = qs.get(key)
        if not values:
            if required:
                self._send(400, {"error": f"缺少查询参数 {key}"})
                return None
            return None
        return values[0]

    def log_message(self, fmt, *args) -> None:  # 静默访问日志
        return

    # ---- 路由 ----
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        store: Storage = self.server.store  # type: ignore[attr-defined]
        try:
            if path == "/health":
                self._send(200, {"status": "ok"})
            elif path == "/api/verify":
                sid = self._query("shift_id", required=False)
                self._send(200, store.verify_chain(sid) if sid else store.verify_all_chains())
            elif path == "/api/replays":
                sid = self._query("shift_id")
                if sid is None:
                    return
                self._send(200, {"versions": store.list_versions(sid)})
            elif path == "/api/diff":
                a, b = self._query("from"), self._query("to")
                if a is None or b is None:
                    return
                self._send(200, store.diff_versions(a, b))
            elif m := re.fullmatch(r"/api/events/(?P<eid>[^/]+)", path):
                self._send(200, store.get_event(m["eid"]))
            elif m := re.fullmatch(r"/api/shifts/(?P<sid>[^/]+)", path):
                self._send(200, store.get_shift(m["sid"]))
            elif m := re.fullmatch(r"/api/replays/(?P<vid>[^/]+)/timeline", path):
                self._send(200, store.get_timeline(m["vid"]))
            elif m := re.fullmatch(r"/api/replays/(?P<vid>[^/]+)", path):
                self._send(200, store.get_version(m["vid"]))
            elif m := re.fullmatch(r"/api/decisions/(?P<cid>[^/]+)", path):
                vid = self._query("version")
                if vid is None:
                    return
                self._send(200, store.trace_decision(vid, m["cid"]))
            elif m := re.fullmatch(r"/api/proof/(?P<vid>[^/]+)", path):
                to = self._query("to_version")
                if to is None:
                    return
                self._send(200, store.proof_untampered(m["vid"], to))
            else:
                self._send(404, {"error": f"无此路由: {path}"})
        except NotFound as exc:
            self._send(404, {"error": str(exc)})
        except (StorageError, ValueError) as exc:
            self._send(400, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        store: Storage = self.server.store  # type: ignore[attr-defined]
        try:
            body = self._body()
            if path == "/api/events":
                events = body.get("events", body if isinstance(body, list) else None)
                if events is None:
                    self._send(400, {"error": "请求体应为事件数组或 {'events': [...]}"})
                    return
                self._send(200, {"results": store.add_events(events)})
            elif path == "/api/shifts":
                from .models import CapabilityParams
                params = CapabilityParams.from_dict(body.get("params"))
                self._send(201, store.create_shift(
                    body["shift_id"], body["start_at"], body["end_at"], params
                ))
            elif m := re.fullmatch(r"/api/shifts/(?P<sid>[^/]+)/seal", path):
                self._send(200, store.seal_shift(m["sid"], body.get("sealed_at")))
            elif path == "/api/replays":
                self._send(201, store.create_version(
                    body["shift_id"], body["as_of"], body["knowledge_cutoff"]
                ))
            else:
                self._send(404, {"error": f"无此路由: {path}"})
        except KeyError as exc:
            self._send(400, {"error": f"缺少字段: {exc.args[0]}"})
        except (StorageError, ValueError) as exc:
            self._send(400, {"error": str(exc)})


def create_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.store = Storage.open(db_path)  # type: ignore[attr-defined]
    return server


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    httpd = create_server(db_path, host, port)
    print(f"调度回放后端监听 http://{host}:{port}（数据库 {db_path}）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.store.close()  # type: ignore[attr-defined]
