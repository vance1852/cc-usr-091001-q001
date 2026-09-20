"""HTTP API 端到端测试（标准库 urllib，不依赖第三方框架）。"""
import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from station_replay.api import create_server

INCIDENT = json.loads(
    (Path(__file__).parents[1] / "reference" / "incident_night_20260818.json").read_text("utf-8")
)
BASE = None  # 测试启动时设置


def request(method: str, path: str, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class ApiTest(unittest.TestCase):
    server = None
    thread = None
    db_path = None

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "api.db")
        cls.server = create_server(cls.db_path, "127.0.0.1", 0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        global BASE
        BASE = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.store.close()
        cls.tmp.cleanup()

    def test_full_incident_flow(self):
        s, _ = request("POST", "/api/shifts", {
            "shift_id": "night-20260818",
            "start_at": INCIDENT["shift"]["start_at"],
            "end_at": INCIDENT["shift"]["end_at"],
            "params": INCIDENT["capability"],
        })
        self.assertEqual(s, 201)

        seal_dt = "2026-08-19T00:02:00+08:00"
        on_shift = [e for e in INCIDENT["events"] if e["received_at"] <= seal_dt]
        late = [e for e in INCIDENT["events"] if e["received_at"] > seal_dt]

        _, ing = request("POST", "/api/events", {"events": on_shift})
        cmd_statuses = [r["status"] for r in ing["results"] if r["event_id"] == "cmd-17"]
        self.assertEqual(sorted(cmd_statuses), ["duplicate", "new"])  # 同批重送：一条新增一条重复

        _, v1 = request("POST", "/api/replays", {
            "shift_id": "night-20260818",
            "as_of": "2026-08-18T23:58:00+08:00",
            "knowledge_cutoff": seal_dt,
        })
        self.assertEqual(v1["sealed"], False)
        _, tl1 = request("GET", f"/api/replays/{v1['version_id']}/timeline")
        dec = tl1["decisions"][0]
        self.assertEqual(dec["outcome"], "REJECT")
        self.assertIn("zero_crossing", dec["reasons"][0])

        _, seal = request("POST", "/api/shifts/night-20260818/seal", {"sealed_at": seal_dt})
        sealed_vid = seal["version_id"]

        _, ing2 = request("POST", "/api/events", {"events": late})
        self.assertEqual(ing2["results"][0]["status"], "new")

        _, v2 = request("POST", "/api/replays", {
            "shift_id": "night-20260818",
            "as_of": "2026-08-18T23:58:00+08:00",
            "knowledge_cutoff": "2026-08-19T00:05:00+08:00",
        })
        _, diff = request("GET", f"/api/diff?from={v1['version_id']}&to={v2['version_id']}")
        self.assertEqual(diff["events_added"], ["p-59"])

        _, trace = request("GET", f"/api/decisions/cmd-17?version={v2['version_id']}")
        self.assertEqual(trace["used_readings"]["power"]["event_id"], "p-59")

        _, ev = request("GET", "/api/events/cmd-17")
        self.assertEqual(ev["event"]["power_kw"], 850.0)
        self.assertEqual(len(ev["redeliveries"]), 1)

        _, proof = request(
            "GET", f"/api/proof/{sealed_vid}?to_version="
                   f"{request('POST', '/api/replays', {'shift_id': 'night-20260818', 'as_of': '2026-08-19T00:00:00+08:00', 'knowledge_cutoff': '2026-08-19T00:05:00+08:00'})[1]['version_id']}"
        )
        self.assertTrue(proof["chain"]["ok"])
        self.assertIn("p-59", proof["comparison"]["events_added_in_later"])

        _, verify = request("GET", "/api/verify?shift_id=night-20260818")
        self.assertTrue(verify["ok"])

    def test_404_and_bad_request(self):
        s, body = request("GET", "/api/events/nope")
        self.assertEqual(s, 404)
        self.assertIn("error", body)
        s, _ = request("POST", "/api/events", {"events": [{"event_id": "bad"}]})
        self.assertEqual(s, 400)


if __name__ == "__main__":
    unittest.main()
