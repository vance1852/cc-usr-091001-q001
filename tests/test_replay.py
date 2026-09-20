"""端到端：事故片段回放、版本不可变、封存冻结、跨进程重启、WORM。"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from station_replay.models import CapabilityParams
from station_replay.storage import Storage, StorageError

T_ASK = "2026-08-18T23:58:00+08:00"
T_PRE_SOC41 = "2026-08-18T23:58:30+08:00"
T_END = "2026-08-19T00:00:00+08:00"
T_SEAL = "2026-08-19T00:02:00+08:00"
T_LATEST = "2026-08-19T00:05:00+08:00"

P57 = {"event_id": "p-57", "kind": "telemetry", "measure": "power", "sequence": 57,
       "occurred_at": "2026-08-18T23:55:00+08:00", "received_at": "2026-08-18T23:55:01+08:00",
       "power_kw": -300.0}
SOC42 = {"event_id": "soc-42", "kind": "telemetry", "measure": "soc", "sequence": 42,
         "occurred_at": "2026-08-18T23:57:30+08:00", "received_at": "2026-08-18T23:57:31+08:00",
         "soc_percent": 24.5}
CMD = {"event_id": "cmd-17", "kind": "dispatch", "sequence": 17,
       "occurred_at": T_ASK, "received_at": "2026-08-18T23:58:00+08:00", "power_kw": 850.0}
CMD_RESEND = {**CMD, "received_at": "2026-08-18T23:58:01+08:00"}
SOC41 = {"event_id": "soc-41", "kind": "telemetry", "measure": "soc", "sequence": 41,
         "occurred_at": "2026-08-18T23:56:30+08:00", "received_at": "2026-08-18T23:59:05+08:00",
         "soc_percent": 25.1}
P59 = {"event_id": "p-59", "kind": "telemetry", "measure": "power", "sequence": 59,
       "occurred_at": "2026-08-18T23:57:30+08:00", "received_at": "2026-08-19T00:03:10+08:00",
       "power_kw": 0.0}


class ReplayEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "station.db")
        self.store = Storage.open(self.db)
        self.store.create_shift("night", "2026-08-18T22:00:00+08:00", T_END, CapabilityParams())

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _decision(self, timeline, cid="cmd-17"):
        return next(d for d in timeline["decisions"] if d["dispatch_event_id"] == cid)

    def _codes(self, dec, passed=None):
        return [h["code"] for h in dec["hits"] if passed is None or h["passed"] is passed]

    def test_redelivery_is_idempotent(self):
        res = self.store.add_events([P57, SOC42, CMD])
        self.assertEqual({r["status"] for r in res}, {"new"})
        res2 = self.store.add_events([CMD_RESEND, CMD_RESEND])
        self.assertTrue(all(r["status"] == "duplicate" for r in res2))
        v = self.store.create_version("night", T_ASK, T_SEAL)
        timeline = self.store.get_timeline(v["version_id"])
        self.assertEqual(len(timeline["decisions"]), 1)  # 只有一条 cmd-17
        detail = self.store.get_event("cmd-17")
        self.assertEqual(len(detail["redeliveries"]), 2)

    def test_on_shift_view_rejects_for_zero_crossing_and_reserve(self):
        self.store.add_events([P57, SOC42, CMD])
        v = self.store.create_version("night", T_ASK, T_SEAL)
        dec = self._decision(self.store.get_timeline(v["version_id"]))
        self.assertEqual(dec["outcome"], "REJECT")
        failed = self._codes(dec, passed=False)
        self.assertIn("zero_crossing", failed)
        self.assertIn("reserve_headroom", failed)
        # ramp 在 -300 前值下通过（180s 限额 3600kW）
        ramp = next(h for h in dec["hits"] if h["code"] == "ramp")
        self.assertTrue(ramp["passed"])
        reserve = next(h for h in dec["hits"] if h["code"] == "reserve_headroom")
        self.assertAlmostEqual(reserve["numbers"]["short_kwh"], 122.5)

    def test_late_but_older_telemetry_does_not_change_conclusion(self):
        self.store.add_events([P57, SOC42, CMD])
        v0 = self.store.create_version("night", T_ASK, T_PRE_SOC41)
        self.store.add_events([SOC41])  # 乱序晚到，但 occurred 更旧
        v1 = self.store.create_version("night", T_ASK, T_SEAL)
        self.assertNotEqual(v0["version_id"], v1["version_id"])
        # 新版本能看到 soc-41，但测点最新值仍是 soc-42
        self.assertIn("soc-41", self.store.get_timeline(v1["version_id"])["adopted_events"])
        self.assertEqual(v0["decisions_hash"], v1["decisions_hash"])
        trace = self.store.trace_decision(v1["version_id"], "cmd-17")
        self.assertEqual(trace["used_readings"]["soc"]["event_id"], "soc-42")

    def test_late_power_forks_new_view_and_reason_changes(self):
        self.store.add_events([P57, SOC42, CMD, SOC41])
        v1 = self.store.create_version("night", T_ASK, T_SEAL)
        self.store.add_events([P59])
        v2 = self.store.create_version("night", T_ASK, T_LATEST)
        self.assertEqual(v2["parent_version_id"], v1["version_id"])

        dec2 = self._decision(self.store.get_timeline(v2["version_id"]))
        failed = self._codes(dec2, passed=False)
        self.assertNotIn("zero_crossing", failed)
        self.assertIn("ramp", failed)
        self.assertIn("reserve_headroom", failed)

        diff = self.store.diff_versions(v1["version_id"], v2["version_id"])
        self.assertEqual(diff["events_added"], ["p-59"])
        flips = {f["code"] for ch in diff["decisions_changed"] for f in ch["constraint_flips"]}
        self.assertEqual(flips, {"ramp", "zero_crossing"})
        self.assertTrue(diff["conclusions_hash_changed"])

    def test_seal_freezes_on_shift_view_and_survives_restart(self):
        self.store.add_events([P57, SOC42, CMD, SOC41])
        sealed = self.store.seal_shift("night", T_SEAL)
        sealed_vid = sealed["version_id"]
        before = self.store.get_timeline(sealed_vid)
        seal_hash = sealed["summary_hash"]

        # 封存后不得再插入 as_of<=班次结束、cutoff<=封存时刻 的竞争版本
        with self.assertRaises(StorageError):
            self.store.create_version("night", T_ASK, T_PRE_SOC41)

        # 封存后补传：可以生成更晚 cutoff 的新版本
        self.store.add_events([P59])
        later = self.store.create_version("night", T_END, T_LATEST)
        self.assertNotEqual(later["version_id"], sealed_vid)

        # 模拟进程重启：关闭后重新打开同一文件
        self.store.close()
        self.store = Storage.open(self.db)
        after = self.store.get_timeline(sealed_vid)
        self.assertEqual(json.dumps(before, sort_keys=True), json.dumps(after, sort_keys=True))
        report = self.store.verify_chain("night")
        self.assertTrue(report["ok"], report["problems"])
        seal_row = self.store.get_shift("night")["sealed"]
        self.assertEqual(seal_row["summary_hash"], seal_hash)

        proof = self.store.proof_untampered(sealed_vid, later["version_id"])
        self.assertTrue(proof["chain"]["ok"])
        self.assertIn("p-59", proof["comparison"]["events_added_in_later"])
        self.assertIn("未改写", proof["conclusion"])

    def test_worm_triggers_block_mutation(self):
        self.store.add_events([CMD])
        raw = sqlite3.connect(self.db)
        for table, stmt in (
            ("events", "UPDATE events SET kind='x' WHERE event_id='cmd-17'"),
            ("decisions", None),  # 决策行在版本生成后测
        ):
            if stmt is not None:
                with self.assertRaises(sqlite3.IntegrityError):
                    raw.execute(stmt)
                raw.rollback()
        with self.assertRaises(sqlite3.IntegrityError):
            raw.execute("DELETE FROM events WHERE event_id='cmd-17'")
        raw.rollback()
        self.store.create_version("night", T_ASK, T_SEAL)
        with self.assertRaises(sqlite3.IntegrityError):
            raw.execute("UPDATE replay_versions SET as_of=as_of WHERE 1=1")
        raw.rollback()
        with self.assertRaises(sqlite3.IntegrityError):
            raw.execute("DELETE FROM constraint_hits WHERE 1=1")
        raw.close()

    def test_conflicting_redelivery_kept_but_not_applied(self):
        self.store.add_events([CMD])
        conflict = {**CMD, "power_kw": 999.0, "received_at": "2026-08-18T23:58:30+08:00"}
        res = self.store.add_events([conflict])
        self.assertEqual(res[0]["status"], "payload_conflict")
        detail = self.store.get_event("cmd-17")
        self.assertEqual(detail["event"]["power_kw"], 850.0)
        self.assertEqual(detail["redeliveries"][0]["same_as_first"], 0)

    def test_accepted_command_passes_all(self):
        # 在停稳且高 SOC 状态下的小指令应全部通过
        self.store.add_events([
            {"event_id": "p0", "kind": "telemetry", "measure": "power", "sequence": 9,
             "occurred_at": "2026-08-18T23:57:30+08:00", "received_at": "2026-08-18T23:57:31+08:00",
             "power_kw": 0.0},
            {"event_id": "s0", "kind": "telemetry", "measure": "soc", "sequence": 9,
             "occurred_at": "2026-08-18T23:57:30+08:00", "received_at": "2026-08-18T23:57:31+08:00",
             "soc_percent": 60.0},
            {"event_id": "c1", "kind": "dispatch", "sequence": 10,
             "occurred_at": T_ASK, "received_at": T_ASK, "power_kw": 100.0},
        ])
        v = self.store.create_version("night", T_ASK, T_SEAL)
        dec = self._decision(self.store.get_timeline(v["version_id"]), "c1")
        self.assertEqual(dec["outcome"], "ACCEPT")
        self.assertEqual(dec["reasons"], [])


if __name__ == "__main__":
    unittest.main()
