"""调度回放后端测试：以 reference 样例与 data/ 事故片段驱动全链路。"""
from __future__ import annotations

import json
import threading
import tempfile
import unittest
import urllib.request
from datetime import datetime
from http.server import ThreadingHTTPServer
from pathlib import Path

from replay.engine import (
    Capability, Fact, normalize, reconstruct, diff_replays, parse_dt,
    R_OUTSIDE_WINDOW, R_RAMP, R_RATING, R_EXCLUSIVE, R_RESERVE, R_NO_TELEMETRY,
    ACCEPTED, REJECTED,
)
from replay.service import ReplayError, ReplayService
from replay.http_server import _PerThreadService, make_handler

ROOT = Path(__file__).parents[1]
HANDOVER = "2026-08-19T00:00:00+08:00"


def load_json(rel: str):
    return json.loads((ROOT / rel).read_text(encoding="utf-8"))


class FreshDB:
    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "station.db")
        self.svc = ReplayService(self.path)
        return self.svc

    def __exit__(self, *exc):
        self.svc.close()
        self.tmp.cleanup()


def bootstrap(svc: ReplayService) -> tuple[str, list[dict], list[dict]]:
    """注册能力+开班+接入交班前片段，返回 shift 与两批事件。"""
    cap = load_json("data/capability_bess_a.json")
    batch1 = load_json("data/fragments/01_before_handover.json")
    batch2 = load_json("data/fragments/02_late_after_handover.json")
    svc.register_capability(cap)
    svc.create_shift("night", cap["profile_id"], "2026-08-18T22:00:00+08:00",
                     "2026-08-19T06:00:00+08:00", HANDOVER)
    return "night", batch1["events"], batch2["events"]


class EngineScenarioTest(unittest.TestCase):
    """直接验证事故片段在交班时刻的判定链。"""

    def setUp(self):
        self.cap = Capability.from_dict(load_json("data/capability_bess_a.json"))
        b1 = load_json("data/fragments/01_before_handover.json")["events"]
        self.facts = [normalize(e) for e in b1]
        self.view = reconstruct(self.facts, self.cap, as_of=parse_dt(HANDOVER))
        self.by_id = {d["event_id"]: d for d in self.view["decisions"]}

    def test_full_constraint_chain(self):
        d = self.by_id
        self.assertEqual(d["cmd-13"]["status"], REJECTED)
        self.assertEqual(d["cmd-13"]["reason"], R_OUTSIDE_WINDOW)
        self.assertFalse(self._hit(d["cmd-13"], "effective_window")["passed"])

        self.assertEqual(d["cmd-15"]["status"], ACCEPTED)
        self.assertEqual(d["cmd-15"]["data_basis"]["soc_event_id"], "soc-40")

        self.assertEqual(d["cmd-16"]["status"], ACCEPTED)
        self.assertEqual(d["cmd-16"]["data_basis"]["soc_event_id"], "soc-41")
        ramp16 = self._hit(d["cmd-16"], "ramp_rate")
        self.assertEqual(ramp16["detail"]["allowance_kw"], 630.0)

        # cmd-17：爬坡与预留容量同时越界，按优先级报爬坡，但两条命中都在时间线里
        self.assertEqual(d["cmd-17"]["status"], REJECTED)
        self.assertEqual(d["cmd-17"]["reason"], R_RAMP)
        ramp17 = self._hit(d["cmd-17"], "ramp_rate")
        self.assertGreater(ramp17["detail"]["delta_kw"], ramp17["detail"]["allowance_kw"])
        reserve17 = self._hit(d["cmd-17"], "reserve_margin")
        self.assertFalse(reserve17["passed"])
        self.assertLess(reserve17["detail"]["projected_soc_percent"],
                        reserve17["detail"]["soc_floor_with_margin_percent"])

        # cmd-18：爬坡其实够，但沿用上一条“已接受”的 400kW 放电 → 充放互斥
        self.assertEqual(d["cmd-18"]["status"], REJECTED)
        self.assertEqual(d["cmd-18"]["reason"], R_EXCLUSIVE)
        self.assertTrue(self._hit(d["cmd-18"], "ramp_rate")["passed"])

        # cmd-19：超额定
        self.assertEqual(d["cmd-19"]["status"], REJECTED)
        self.assertEqual(d["cmd-19"]["reason"], R_RATING)

    @staticmethod
    def _hit(decision, name):
        return next(h for h in decision["constraint_hits"] if h["constraint"] == name)

    def test_rejected_command_does_not_change_state(self):
        # cmd-17 被拒后，cmd-18 的互斥前态仍是 cmd-16 的 400kW，而不是 850
        d18 = self.by_id["cmd-18"]
        self.assertEqual(d18["data_basis"]["prev_accepted_event_id"], "cmd-16")
        self.assertEqual(d18["data_basis"]["prev_power_kw"], 400.0)

    def test_timeline_marks_late_and_orders_by_occurred_time(self):
        late = {r["event_id"] for r in self.view["timeline"] if r.get("late")}
        self.assertEqual(late, set())  # 本批只含交班前到站数据
        occurred = [parse_dt(r["occurred_at"]) for r in self.view["timeline"]]
        self.assertEqual(occurred, sorted(occurred))

    def test_unknown_extra_fields_survive_normalize(self):
        fact = normalize({
            "event_id": "x", "kind": "dispatch", "sequence": 1,
            "occurred_at": "2026-08-18T23:00:00+08:00", "power_kw": 0,
            "grid_feeder": "F-07", "operator_note": "中文备注保留",
        })
        self.assertEqual(fact.extra["grid_feeder"], "F-07")
        self.assertIn("operator_note", fact.extra)

    def test_no_telemetry_rejects_with_reason(self):
        cap = self.cap
        cmd = normalize({"event_id": "c1", "kind": "dispatch", "sequence": 1,
                         "occurred_at": "2026-08-18T23:30:00+08:00", "power_kw": 100})
        view = reconstruct([cmd], cap)
        self.assertEqual(view["decisions"][0]["reason"], R_NO_TELEMETRY)

    def test_charge_ceiling_margin(self):
        # 接近上限时充电指令应被预留/安全余量拒绝
        cap = Capability.from_dict({
            **self.cap.to_dict(),
            "profile_id": "test-ceiling",
            "effective_from": None, "effective_to": None,
        })
        facts = [
            normalize({"event_id": "s1", "kind": "telemetry", "sequence": 1,
                       "occurred_at": "2026-08-18T23:00:00+08:00", "soc_percent": 98.0}),
            normalize({"event_id": "c1", "kind": "dispatch", "sequence": 2,
                       "occurred_at": "2026-08-18T23:00:10+08:00", "power_kw": -500}),
        ]
        view = reconstruct(facts, cap)
        d = view["decisions"][0]
        self.assertEqual(d["reason"], R_RESERVE)
        self.assertGreater(d["constraint_hits"][-1]["detail"]["projected_soc_percent"],
                           d["constraint_hits"][-1]["detail"]["soc_ceiling_with_margin_percent"])


class ServiceVersioningTest(unittest.TestCase):
    def test_idempotent_replay_and_resend_dedup(self):
        with FreshDB() as svc:
            shift, b1, b2 = bootstrap(svc)
            r1 = svc.ingest(shift, b1)
            self.assertEqual(r1["new"].count("cmd-17"), 1)
            self.assertIn("cmd-17", r1["duplicates"])  # 批内重送

            v0 = svc.replay(shift, as_of=HANDOVER, note="交班视图")
            v0_again = svc.replay(shift, as_of=HANDOVER)
            self.assertEqual(v0["version_id"], v0_again["version_id"])
            self.assertTrue(v0_again["deduped"])

            # 重放同一批：全部记为重送，不产生新版本
            r2 = svc.ingest(shift, b1)
            self.assertEqual(r2["new"], [])
            self.assertEqual(len(r2["duplicates"]), len(b1))
            v0_after = svc.replay(shift, as_of=HANDOVER)
            self.assertEqual(v0_after["version_id"], v0["version_id"])

    def test_late_telemetry_creates_new_version_without_rewriting(self):
        with FreshDB() as svc:
            shift, b1, b2 = bootstrap(svc)
            svc.ingest(shift, b1)
            v_then = svc.replay(shift, as_of=HANDOVER, note="交班签认")

            svc.ingest(shift, b2)  # soc-42 / soc-44 迟到 + cmd-17 三送
            v_now = svc.replay(shift)  # 最新视图

            self.assertNotEqual(v_then["version_id"], v_now["version_id"])

            # 关键性质：迟到数据入库后用同一交盘点重建，只派生新版本——
            # 时间线多出迟到标注，但逐条判定与签认口径一致
            v_then_rebuilt = svc.replay(shift, as_of=HANDOVER, note="补传后重建交班视图")
            self.assertNotEqual(v_then_rebuilt["version_id"], v_then["version_id"])
            then_tl = {r["event_id"]: r for r in v_then_rebuilt["timeline"]
                       if r["kind"] == "telemetry"}
            self.assertTrue(then_tl["soc-42"]["late"])
            now_tl = {r["event_id"]: r for r in v_now["timeline"]
                      if r["kind"] == "telemetry"}
            self.assertFalse(now_tl["soc-42"]["late"])
            self.assertEqual(
                [(d["event_id"], d["status"], d["effective_power_kw"])
                 for d in v_then_rebuilt["decisions"]],
                [(d["event_id"], d["status"], d["effective_power_kw"])
                 for d in v_then["decisions"]],
            )

            # 补传后的同交盘点视图再重放：输入指纹相同，必须幂等回同一版本
            again = svc.replay(shift, as_of=HANDOVER, note="补传后重建交班视图")
            self.assertEqual(again["version_id"], v_then_rebuilt["version_id"])
            self.assertTrue(again["deduped"])

            diff = svc.diff_versions(v_then["version_id"], v_now["version_id"])

            # 迟到遥测清单
            self.assertEqual(set(diff["late_telemetry_newly_visible"]), {"soc-42", "soc-44"})

            # cmd-17 的数据依据从 soc-41 改为迟到的 soc-42，但被拒结论不变
            changed = {c["event_id"]: c for c in diff["changed_decisions"]}
            self.assertIn("cmd-17", changed)
            self.assertEqual(changed["cmd-17"]["fields"]["soc_basis"],
                             {"then": "soc-41", "now": "soc-42"})
            self.assertNotIn("status", changed["cmd-17"]["fields"])

            # 交班签认口径（状态/生效功率）逐条不变
            self.assertFalse(diff["signed_conclusion_changed"])

    def test_conflicting_event_id_rejects_whole_batch(self):
        with FreshDB() as svc:
            shift, b1, _ = bootstrap(svc)
            svc.ingest(shift, b1)
            evil = [{"event_id": "cmd-17", "kind": "dispatch", "sequence": 17,
                     "occurred_at": "2026-08-18T23:58:00+08:00", "power_kw": 999}]
            with self.assertRaises(ReplayError) as ctx:
                svc.ingest(shift, evil)
            self.assertEqual(ctx.exception.code, "CONTENT_CONFLICT")
            # 库里的 cmd-17 仍是 850
            facts = {f["event_id"]: f for f in svc.list_facts(shift)}
            self.assertEqual(facts["cmd-17"]["payload"]["power_kw"], 850)

    def test_seal_freezes_signed_version_but_allows_post_seal_revision(self):
        with FreshDB() as svc:
            shift, b1, b2 = bootstrap(svc)
            svc.ingest(shift, b1)
            v_signed = svc.replay(shift, as_of=HANDOVER, note="交班签认版本")
            seal = svc.seal_shift(shift)
            self.assertEqual(seal["signed_version_id"], v_signed["version_id"])

            # 封存后迟到数据仍可接入并派生新版本
            svc.ingest(shift, b2)
            v_post = svc.replay(shift, note="封存后补传视图")
            self.assertTrue(v_post["post_seal"])
            self.assertNotEqual(v_post["version_id"], v_signed["version_id"])

            # 签认版本逐字节可复算
            check = svc.verify_version(v_signed["version_id"])
            self.assertTrue(check["matches"])
            self.assertEqual(set(check["facts_added_after_version"]), {"soc-42", "soc-44"})

            # 一键对比：补传没有改写签认结论
            report = svc.signed_vs_latest(shift)
            self.assertTrue(report["signed_conclusion_intact"])
            self.assertFalse(report["signed_conclusion_changed"])

            # 库内触发器：版本与事实禁改禁删、班次禁改
            import sqlite3
            with self.assertRaises(sqlite3.IntegrityError):
                svc.conn.execute("DELETE FROM replay_versions WHERE version_id=?",
                                 (v_signed["version_id"],))
            with self.assertRaises(sqlite3.IntegrityError):
                svc.conn.execute("UPDATE facts SET payload_json='{}' WHERE event_id='cmd-17'")
            with self.assertRaises(sqlite3.IntegrityError):
                svc.conn.execute("UPDATE shifts SET handover_at=? WHERE shift_id=?",
                                 ("2026-08-19T01:00:00+08:00", shift))

    def test_persistence_across_process_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "station.db")
            with ReplayService(path) as svc:
                shift, b1, b2 = bootstrap(svc)
                svc.ingest(shift, b1)
                v0 = svc.replay(shift, as_of=HANDOVER)
                svc.seal_shift(shift)
                svc.ingest(shift, b2)
                v1 = svc.replay(shift)
                ids = (v0["version_id"], v1["version_id"])
            # “重启”：新进程、新连接
            with ReplayService(path) as svc2:
                self.assertEqual(svc2.signed_version(shift)["version_id"], ids[0])
                self.assertEqual(svc2.latest_version(shift)["version_id"], ids[1])
                self.assertTrue(svc2.verify_version(ids[0])["matches"])
                trace = svc2.trace_decision(shift, "cmd-17")
                self.assertEqual(len(trace["across_versions"]), 2)
                for row in trace["across_versions"]:
                    self.assertIn("constraint_hits", row)
                    self.assertIn("data_version", row)

    def test_trace_links_power_decision_to_data_and_constraints(self):
        with FreshDB() as svc:
            shift, b1, b2 = bootstrap(svc)
            svc.ingest(shift, b1)
            svc.replay(shift, as_of=HANDOVER)
            svc.ingest(shift, b2)
            svc.replay(shift)
            trace = svc.trace_decision(shift, "cmd-17")
            rows = trace["across_versions"]
            self.assertEqual([r["soc_basis_event_id"] for r in rows], ["soc-41", "soc-42"])
            for r in rows:
                names = {h["constraint"] for h in r["constraint_hits"]}
                self.assertEqual(
                    names,
                    {"effective_window", "soc_available", "power_rating",
                     "ramp_rate", "charge_discharge", "reserve_margin"},
                )
                hashes = {x["event_id"]: x["content_hash"] for x in r["data_version"]}
                self.assertIn(r["soc_basis_event_id"], hashes)


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "station.db")
        holder = _PerThreadService(self.db_path)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(holder))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data,
            headers={"Content-Type": "application/json"}, method=method)
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_end_to_end_http(self):
        cap = load_json("data/capability_bess_a.json")
        b1 = load_json("data/fragments/01_before_handover.json")
        b2 = load_json("data/fragments/02_late_after_handover.json")

        self._call("POST", "/capabilities", cap)
        status, _ = self._call("POST", "/shifts", {
            "shift_id": "night", "profile_id": cap["profile_id"],
            "starts_at": "2026-08-18T22:00:00+08:00",
            "ends_at": "2026-08-19T06:00:00+08:00",
            "handover_at": HANDOVER,
        })
        self.assertEqual(status, 201)
        self._call("POST", "/shifts/night/ingest", {"events": b1["events"]})
        _, v0 = self._call("POST", "/shifts/night/replay", {"as_of": HANDOVER})
        self._call("POST", "/shifts/night/seal", {})
        self._call("POST", "/shifts/night/ingest", {"events": b2["events"]})
        _, v1 = self._call("POST", "/shifts/night/replay", {})

        _, diff = self._call(
            "GET", f"/diff?then={v0['version_id']}&now={v1['version_id']}")
        self.assertFalse(diff["signed_conclusion_changed"])

        _, report = self._call("GET", "/shifts/night/signed-vs-latest")
        self.assertTrue(report["signed_conclusion_intact"])

        _, trace = self._call("GET", "/shifts/night/decisions/cmd-17/trace")
        self.assertEqual(len(trace["across_versions"]), 2)

        _, verify = self._call("GET", f"/versions/{v0['version_id']}/verify")
        self.assertTrue(verify["matches"])


class ReferenceSampleCompatTest(unittest.TestCase):
    """reference/events.json 的字段边界必须被完整支持。"""

    def test_reference_events_normalize(self):
        data = load_json("reference/events.json")
        facts = [normalize(e) for e in data["events"]]
        self.assertEqual(len(facts), 4)
        # 同 event_id 两条重送内容哈希一致
        h1 = ReplayService._content_hash(data["events"][2])
        h2 = ReplayService._content_hash(data["events"][0])
        self.assertEqual(h1, h2)
        # 时间解析带时区
        self.assertIsInstance(facts[0].occurred_at, datetime)


if __name__ == "__main__":
    unittest.main()
