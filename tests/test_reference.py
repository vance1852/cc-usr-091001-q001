import json
import unittest
from pathlib import Path


class DispatchReferenceTest(unittest.TestCase):
    def test_event_sample_contains_duplicate_and_late_telemetry(self):
        data = json.loads((Path(__file__).parents[1] / "reference" / "events.json").read_text(encoding="utf-8"))
        ids = [event["event_id"] for event in data["events"]]
        telemetry = [event["sequence"] for event in data["events"] if event["kind"] == "telemetry"]
        self.assertEqual(data["station_timezone"], "Asia/Shanghai")
        self.assertLess(telemetry[-1], telemetry[0])
        self.assertLess(len(set(ids)), len(ids))


if __name__ == "__main__":
    unittest.main()
