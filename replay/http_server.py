"""HTTP 后端（标准库，零依赖）。

路由：
    GET  /health
    POST /capabilities                      注册能力档案
    POST /shifts                            开班次
    GET  /shifts/{shift_id}
    POST /shifts/{shift_id}/ingest          接入事件（幂等）
    POST /shifts/{shift_id}/replay          {"as_of": "..."} 重建视图
    GET  /shifts/{shift_id}/versions
    POST /shifts/{shift_id}/seal
    GET  /shifts/{shift_id}/signed-vs-latest
    GET  /shifts/{shift_id}/decisions/{event_id}/trace
    GET  /versions/{version_id}
    GET  /versions/{version_id}/verify
    GET  /diff?then=...&now=...
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .db import open_db
from .service import ReplayError, ReplayService


class _PerThreadService:
    """每线程一个 SQLite 连接，避免跨线程使用同一连接。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()

    def get(self) -> ReplayService:
        if not hasattr(self._local, "svc"):
            svc = ReplayService.__new__(ReplayService)
            svc.db_path = self.db_path
            svc.conn = open_db(self.db_path)
            self._local.svc = svc
        return self._local.svc


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: object) -> None:
    raw = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def make_handler(holder: _PerThreadService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "DispatchReplay/1.0"

        def log_message(self, fmt: str, *args: object) -> None:  # 安静一点
            return

        def _read_json(self) -> object:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _ok(self, body: object, status: int = 200) -> None:
            _json_response(self, status, body)

        def _err(self, status: int, code: str, message: str) -> None:
            _json_response(self, status, {"error": code, "message": message})

        def do_GET(self) -> None:  # noqa: N802
            svc = holder.get()
            u = urlparse(self.path)
            p, q = u.path.strip("/"), parse_qs(u.query)
            try:
                if p == "health":
                    return self._ok({"status": "ok"})
                parts = p.split("/")
                if len(parts) == 2 and parts[0] == "shifts":
                    return self._ok(svc.get_shift(parts[1]))
                if len(parts) == 3 and parts[0] == "shifts" and parts[2] == "versions":
                    return self._ok({"versions": svc.list_versions(parts[1])})
                if len(parts) == 3 and parts[0] == "shifts" and parts[2] == "signed-vs-latest":
                    return self._ok(svc.signed_vs_latest(parts[1]))
                if len(parts) == 5 and parts[0] == "shifts" and parts[2] == "decisions" \
                        and parts[4] == "trace":
                    return self._ok(svc.trace_decision(parts[1], parts[3]))
                if len(parts) == 2 and parts[0] == "versions":
                    return self._ok(svc.get_version(parts[1]))
                if len(parts) == 3 and parts[0] == "versions" and parts[2] == "verify":
                    return self._ok(svc.verify_version(parts[1]))
                if p == "diff" and "then" in q and "now" in q:
                    return self._ok(svc.diff_versions(q["then"][0], q["now"][0]))
                self._err(404, "NOT_FOUND", f"无此路由: {self.path}")
            except ReplayError as exc:
                self._err(409 if exc.code == "SHIFT_SEALED" else 404, exc.code, str(exc))
            except json.JSONDecodeError:
                self._err(400, "BAD_JSON", "请求体不是合法 JSON")

        def do_POST(self) -> None:  # noqa: N802
            svc = holder.get()
            p = urlparse(self.path).path.strip("/")
            parts = p.split("/")
            try:
                body = self._read_json()
                if not isinstance(body, dict):
                    return self._err(400, "BAD_BODY", "请求体需为 JSON 对象")

                if p == "capabilities":
                    pid = svc.register_capability(body)
                    return self._ok({"registered": pid})
                if p == "shifts":
                    return self._ok(svc.create_shift(
                        body["shift_id"], body["profile_id"],
                        body["starts_at"], body["ends_at"], body["handover_at"]), 201)
                if len(parts) == 3 and parts[0] == "shifts" and parts[2] == "ingest":
                    events = body.get("events") if isinstance(body, dict) else body
                    if not isinstance(events, list):
                        return self._err(400, "BAD_BODY", "需要 events 列表")
                    return self._ok(svc.ingest(parts[1], events))
                if len(parts) == 3 and parts[0] == "shifts" and parts[2] == "replay":
                    return self._ok(svc.replay(parts[1], as_of=body.get("as_of"),
                                               note=body.get("note")), 201)
                if len(parts) == 3 and parts[0] == "shifts" and parts[2] == "seal":
                    return self._ok(svc.seal_shift(parts[1], body.get("handover_at")))
                self._err(404, "NOT_FOUND", f"无此路由: {self.path}")
            except ReplayError as exc:
                status = 409 if exc.code in ("CONTENT_CONFLICT", "PROFILE_CONFLICT") else 404
                if exc.code in ("SHIFT_SEALED",):
                    status = 409
                self._err(status, exc.code, str(exc))
            except (json.JSONDecodeError, KeyError) as exc:
                self._err(400, "BAD_REQUEST", f"请求解析失败: {exc}")

    return Handler


def serve(svc: ReplayService, host: str = "127.0.0.1", port: int = 8080) -> None:
    holder = _PerThreadService(svc.db_path)
    httpd = ThreadingHTTPServer((host, port), make_handler(holder))
    print(f"调度回放后端监听 http://{host}:{port}（台账 {svc.db_path}）", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
