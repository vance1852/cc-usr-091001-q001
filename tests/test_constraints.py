import unittest

from station_replay.constraints import (
    check_ramp,
    check_rated,
    check_reserve_headroom,
    check_staleness,
    check_window,
    check_zero_crossing,
)
from station_replay.models import CapabilityParams, LatestReading

T0 = "2026-08-18T23:58:00+08:00"


def reading(event_id, seq, occurred, value, age=None):
    return LatestReading(event_id, seq, occurred, occurred + "Z", value, "hash", age)


class ConstraintTest(unittest.TestCase):
    def setUp(self):
        self.p = CapabilityParams()

    def test_window_boundaries(self):
        h = self.p.horizon_s
        self.assertTrue(check_window(T0, T0, h).passed)
        self.assertFalse(
            check_window(T0, "2026-08-18T23:57:59+08:00", h).passed
        )
        self.assertFalse(
            check_window(T0, "2026-08-19T00:13:00+08:00", h).passed
        )
        self.assertTrue(
            check_window(T0, "2026-08-19T00:12:59+08:00", h).passed
        )

    def test_rated(self):
        self.assertTrue(check_rated(1000, self.p).passed)
        self.assertTrue(check_rated(-1000, self.p).passed)
        hit = check_rated(1001, self.p)
        self.assertFalse(hit.passed)
        self.assertEqual(hit.numbers["abs_command_kw"], 1001)

    def test_ramp_within_and_exceed(self):
        # 30s 内 0 -> 850，限值 600，超出 250
        prev = reading("p-59", 59, "2026-08-18T23:57:30+08:00", 0.0)
        hit = check_ramp(850, T0, prev, self.p.horizon_s, self.p)
        self.assertFalse(hit.passed)
        self.assertEqual(hit.numbers["dt_s"], 30.0)
        self.assertEqual(hit.numbers["ramp_limit_kw"], 600.0)
        self.assertEqual(hit.numbers["exceed_kw"], 250.0)

        # 180s 内 -300 -> 850，限值 3600，可行
        prev2 = reading("p-57", 57, "2026-08-18T23:55:00+08:00", -300.0)
        hit2 = check_ramp(850, T0, prev2, self.p.horizon_s, self.p)
        self.assertTrue(hit2.passed)
        self.assertEqual(hit2.numbers["delta_kw"], 1150.0)

    def test_ramp_without_previous_uses_zero_full_horizon(self):
        hit = check_ramp(850, T0, None, self.p.horizon_s, self.p)
        self.assertTrue(hit.passed)  # 850 <= 20*900=18000
        self.assertEqual(hit.numbers["dt_s"], 900.0)

    def test_zero_crossing(self):
        charging = reading("p", 1, T0, -300.0)
        self.assertFalse(check_zero_crossing(850, charging).passed)
        idle = reading("p", 2, T0, 0.0)
        self.assertTrue(check_zero_crossing(850, idle).passed)
        discharging = reading("p", 3, T0, 300.0)
        self.assertTrue(check_zero_crossing(850, discharging).passed)
        self.assertFalse(check_zero_crossing(-850, discharging).passed)
        self.assertTrue(check_zero_crossing(850, None).passed)

    def test_reserve_headroom_discharge_break(self):
        # SOC 24.5%，预留 20%，容量 2000 -> 可用 90kWh，需 212.5kWh，缺 122.5
        soc = reading("soc-42", 42, T0, 24.5)
        hit = check_reserve_headroom(850, soc, self.p)
        self.assertFalse(hit.passed)
        self.assertAlmostEqual(hit.numbers["available_kwh"], 90.0)
        self.assertAlmostEqual(hit.numbers["interval_energy_kwh"], 212.5)
        self.assertAlmostEqual(hit.numbers["short_kwh"], 122.5)

    def test_reserve_headroom_ok(self):
        soc = reading("soc", 1, T0, 40.0)
        hit = check_reserve_headroom(850, soc, self.p)
        self.assertTrue(hit.passed)

    def test_reserve_headroom_charge_ceiling(self):
        soc = reading("soc", 1, T0, 94.0)
        hit = check_reserve_headroom(-800, soc, self.p)  # 需 200kWh，只剩 1%*2000=20
        self.assertFalse(hit.passed)
        self.assertAlmostEqual(hit.numbers["over_kwh"], 180.0)

    def test_reserve_without_soc_fails(self):
        hit = check_reserve_headroom(850, None, self.p)
        self.assertFalse(hit.passed)

    def test_staleness(self):
        fresh = reading("p", 1, "2026-08-18T23:57:30+08:00", 0.0, age=30.0)
        soc = reading("s", 1, "2026-08-18T23:57:30+08:00", 24.0, age=30.0)
        self.assertTrue(check_staleness(T0, fresh, soc, self.p).passed)
        stale = reading("p", 1, "2026-08-18T23:50:00+08:00", 0.0, age=480.0)
        hit = check_staleness(T0, stale, soc, self.p)
        self.assertFalse(hit.passed)
        self.assertTrue(hit.blocking)
        # 缺测不单独拦截（能量/爬坡规则兜底）
        missing = check_staleness(T0, None, None, self.p)
        self.assertTrue(missing.passed)


if __name__ == "__main__":
    unittest.main()
