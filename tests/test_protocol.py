import json
import unittest

from station_replay.protocol import parse_event, raw_hash_of, ProtocolError


class ProtocolTest(unittest.TestCase):
    def test_dispatch_parse_and_extras_preserved(self):
        raw = {
            "event_id": "cmd-1",
            "kind": "dispatch",
            "sequence": 1,
            "occurred_at": "2026-08-18T23:58:00+08:00",
            "received_at": "2026-08-18T23:58:01+08:00",
            "power_kw": 850,
            "source": "agc-v3",
        }
        ev = parse_event(raw)
        self.assertEqual(ev.extras, {"source": "agc-v3"})
        self.assertEqual(ev.as_payload()["source"], "agc-v3")
        self.assertEqual(ev.raw_hash, raw_hash_of(ev.content_payload()))
        # 重送只改 received_at：内容哈希不变（接收时间仅审计用）
        resent = dict(raw, received_at="2026-08-18T23:59:00+08:00")
        self.assertEqual(parse_event(resent).raw_hash, ev.raw_hash)

    def test_telemetry_measure_inference(self):
        ev = parse_event({
            "event_id": "soc-42", "kind": "telemetry", "sequence": 42,
            "occurred_at": "2026-08-18T23:57:30+08:00",
            "received_at": "2026-08-18T23:57:31+08:00",
            "soc_percent": 24.5,
        })
        self.assertEqual(ev.measure, "soc")

    def test_duplicate_payload_same_hash(self):
        raw = {"event_id": "cmd-17", "kind": "dispatch", "sequence": 17,
               "occurred_at": "2026-08-18T23:58:00+08:00",
               "received_at": "2026-08-18T23:58:00+08:00", "power_kw": 850}
        self.assertEqual(raw_hash_of(raw), raw_hash_of(dict(raw)))

    def test_payload_change_changes_hash(self):
        a = {"event_id": "cmd-17", "kind": "dispatch", "sequence": 17,
             "occurred_at": "2026-08-18T23:58:00+08:00",
             "received_at": "2026-08-18T23:58:00+08:00", "power_kw": 850}
        b = dict(a, power_kw=851)
        self.assertNotEqual(raw_hash_of(a), raw_hash_of(b))

    def test_rejects_naive_timestamp(self):
        with self.assertRaises(ProtocolError):
            parse_event({"event_id": "x", "kind": "dispatch", "sequence": 1,
                         "occurred_at": "2026-08-18T23:58:00",
                         "received_at": "2026-08-18T23:58:01+08:00", "power_kw": 1})

    def test_soc_range(self):
        with self.assertRaises(ProtocolError):
            parse_event({"event_id": "x", "kind": "telemetry", "measure": "soc",
                         "sequence": 1, "occurred_at": "2026-08-18T23:58:00+08:00",
                         "received_at": "2026-08-18T23:58:00+08:00", "soc_percent": 120})


if __name__ == "__main__":
    unittest.main()
