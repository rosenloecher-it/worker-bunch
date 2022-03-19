import unittest
from datetime import timezone, timedelta, datetime

from tzlocal import get_localzone

from worker_bunch.astral_times.astral_times_config import AstralTime, AstralTimesConfKey
from worker_bunch.astral_times.astral_times_manager import AstralTimesManager, AstralParsed


class TestAstralTimeManager(unittest.TestCase):

    DUMMY_CONFIG = {
        AstralTimesConfKey.LATITUDE: 51.051873,
        AstralTimesConfKey.LONGITUDE: 13.741522,
        AstralTimesConfKey.ELEVATION: 125,
    }

    def test_round_time_to_minute(self):
        tz = get_localzone()

        self.assertEqual(
            AstralTimesManager.round_time_to_minute(datetime(2022, 3, 19, 9, 55, 15, tzinfo=tz)),
            datetime(2022, 3, 19, 9, 55, 0, tzinfo=tz)
        )
        self.assertEqual(
            AstralTimesManager.round_time_to_minute(datetime(2022, 3, 19, 9, 55, 31, 12345, tzinfo=tz)),
            datetime(2022, 3, 19, 9, 56, 0, tzinfo=tz)
        )
        self.assertEqual(
            AstralTimesManager.round_time_to_minute(datetime(2022, 3, 19, 23, 59, 31, tzinfo=tz)),
            datetime(2022, 3, 20, 0, 0, 0, tzinfo=tz)
        )

    def test_astral_parse_type(self):
        a1 = AstralParsed(predefined=AstralTime.SUNRISE, is_dawn=True, is_dusk=True, depression=6)
        self.assertEqual(a1.is_valid(), False)

        # Check AstralParse to see if the prop initialisation is working properly
        a2 = AstralParsed()
        self.assertEqual(a2.predefined, None)
        self.assertEqual(a2.is_dawn, None)
        self.assertEqual(a2.is_dusk, None)
        self.assertEqual(a2.depression, None)
        self.assertEqual(a2.is_valid(), False)

        self.assertEqual(a1.predefined, AstralTime.SUNRISE)
        self.assertEqual(a1.is_dawn, True)
        self.assertEqual(a1.is_dusk, True)
        self.assertEqual(a1.depression, 6)

        self.assertEqual(AstralParsed(predefined=AstralTime.SUNRISE).is_valid(), True)
        self.assertEqual(AstralParsed(is_dawn=True, depression=6).is_valid(), True)
        self.assertEqual(AstralParsed(is_dusk=True, depression=6).is_valid(), True)
        self.assertEqual(AstralParsed(is_dawn=True, is_dusk=True, depression=6).is_valid(), False)
        self.assertEqual(AstralParsed(is_dusk=True).is_valid(), False)
        self.assertEqual(AstralParsed(is_dusk=True, depression=0).is_valid(), False)

    def test_parse_astral_time_key(self):

        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawn")
        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawnnovalid")
        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawn_22")

        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dawn_18"),
            AstralParsed(is_dawn=True, depression=18)
        )
        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dusk_2"),
            AstralParsed(is_dusk=True, depression=2)
        )
        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dusk_02"),
            AstralParsed(is_dusk=True, depression=2)
        )

        for astral_time in AstralTime:
            expected = AstralParsed(predefined=astral_time)
            got = AstralTimesManager.parse_astral_time_key(astral_time.value)
            self.assertEqual(got, expected)
            self.assertEqual(got.is_valid(), True)

        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("  Sunrise "),
            AstralParsed(predefined=AstralTime.SUNRISE)
        )

    def test_get_astral_times(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        now = datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))
        astral_times = m.get_astral_times(now)

        expected_times = {
            "timestamp": datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600))),
            "dawn_astro": datetime(2022, 3, 19, 4, 56, tzinfo=timezone(timedelta(seconds=3600))),
            "dawn_nautical": datetime(2022, 3, 19, 4, 16, tzinfo=timezone(timedelta(seconds=3600))),
            "dawn_civil": datetime(2022, 3, 19, 5, 35, tzinfo=timezone(timedelta(seconds=3600))),
            "sunrise": datetime(2022, 3, 19, 6, 8, tzinfo=timezone(timedelta(seconds=3600))),
            "noon": datetime(2022, 3, 19, 12, 13, tzinfo=timezone(timedelta(seconds=3600))),
            "sunset": datetime(2022, 3, 19, 18, 18, tzinfo=timezone(timedelta(seconds=3600))),
            "dusk_civil": datetime(2022, 3, 19, 18, 52, tzinfo=timezone(timedelta(seconds=3600))),
            "dusk_nautical": datetime(2022, 3, 19, 20, 11, tzinfo=timezone(timedelta(seconds=3600))),
            "dusk_astro": datetime(2022, 3, 19, 19, 31, tzinfo=timezone(timedelta(seconds=3600))),
            "midnight": datetime(2022, 3, 19, 0, 13, tzinfo=timezone(timedelta(seconds=3600)))
        }
        self.assertEqual(astral_times, expected_times)

    def test_hits(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        t_no_hit = datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))
        t_sunrise = datetime(2022, 3, 19, 6, 8, tzinfo=timezone(timedelta(seconds=3600)))

        self.assertEqual(len(m._cached_values), 0)
        hit = m.hits(" Sunrise ", t_no_hit)
        self.assertEqual(hit, False)
        self.assertEqual(len(m._cached_values), 1)

        hit = m.hits(" Sunrise ", t_sunrise)
        self.assertEqual(hit, True)
        self.assertEqual(len(m._cached_values), 1)

        hit = m.hits(" sunrisE ", t_sunrise)
        self.assertEqual(hit, True)
        self.assertEqual(len(m._cached_values), 2)

        # times differ more than AstralTimeManager.RECALC_SPAN
        hit = m.hits("       Sunrise ", t_no_hit)
        self.assertEqual(hit, False)
        self.assertEqual(len(m._cached_values), 1)  # cache was reset
