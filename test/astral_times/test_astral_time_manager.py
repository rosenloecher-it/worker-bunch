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

    # noinspection SpellCheckingInspection
    def test_parse_astral_time_key(self):

        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawn")
        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawnnovalid")
        with self.assertRaises(ValueError):
            AstralTimesManager.parse_astral_time_key("dawn22")

        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dawn18"),
            AstralParsed(is_dawn=True, depression=18)
        )
        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dusk2"),
            AstralParsed(is_dusk=True, depression=2)
        )
        self.assertEqual(
            AstralTimesManager.parse_astral_time_key("dusk02"),
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

    def test_get_location(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        location = m.get_location()
        self.assertEqual(location.latitude, self.DUMMY_CONFIG[AstralTimesConfKey.LATITUDE])
        self.assertEqual(location.longitude, self.DUMMY_CONFIG[AstralTimesConfKey.LONGITUDE])
        self.assertEqual(location.elevation, self.DUMMY_CONFIG[AstralTimesConfKey.ELEVATION])

    def test_get_astral_time(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        now = datetime(2022, 3, 19, 7, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))

        sunset = m.get_astral_time("sunset", now)
        self.assertEqual(sunset, datetime(2022, 3, 19, 18, 18, tzinfo=timezone(timedelta(seconds=3600))))

        astral_times = m.get_astral_times(now)
        self.assertEqual(sunset, astral_times["sunset"])

        with self.assertRaises(ValueError):
            m.get_astral_time("does not exists", now)

    def test_get_astral_times(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        now = datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))
        astral_times = m.get_astral_times(now)

        expected_times = {
            "timestamp": datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600))),
            "dawnAstro": datetime(2022, 3, 19, 4, 56, tzinfo=timezone(timedelta(seconds=3600))),
            "dawnNautical": datetime(2022, 3, 19, 4, 16, tzinfo=timezone(timedelta(seconds=3600))),
            "dawnCivil": datetime(2022, 3, 19, 5, 35, tzinfo=timezone(timedelta(seconds=3600))),
            "sunrise": datetime(2022, 3, 19, 6, 8, tzinfo=timezone(timedelta(seconds=3600))),
            "noon": datetime(2022, 3, 19, 12, 13, tzinfo=timezone(timedelta(seconds=3600))),
            "sunset": datetime(2022, 3, 19, 18, 18, tzinfo=timezone(timedelta(seconds=3600))),
            "duskCivil": datetime(2022, 3, 19, 18, 52, tzinfo=timezone(timedelta(seconds=3600))),
            "duskNautical": datetime(2022, 3, 19, 20, 11, tzinfo=timezone(timedelta(seconds=3600))),
            "duskAstro": datetime(2022, 3, 19, 19, 31, tzinfo=timezone(timedelta(seconds=3600))),
            "midnight": datetime(2022, 3, 19, 0, 13, tzinfo=timezone(timedelta(seconds=3600)))
        }
        self.assertEqual(astral_times, expected_times)

    def test_get_astral_times_invalid(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        now = datetime(2022, 6, 21, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))
        astral_times = m.get_astral_times(now)

        expected_times = {
            'dawnAstro': datetime(2022, 6, 21, 1, 46, tzinfo=timezone(timedelta(seconds=3600))),
            'dawnCivil': datetime(2022, 6, 21, 2, 59, tzinfo=timezone(timedelta(seconds=3600))),
            'dawnNautical': None,
            'duskAstro': datetime(2022, 6, 21, 22, 28, tzinfo=timezone(timedelta(seconds=3600))),
            'duskCivil': datetime(2022, 6, 21, 21, 14, tzinfo=timezone(timedelta(seconds=3600))),
            'duskNautical': None,
            'midnight': datetime(2022, 6, 21, 0, 7, tzinfo=timezone(timedelta(seconds=3600))),
            'noon': datetime(2022, 6, 21, 12, 7, tzinfo=timezone(timedelta(seconds=3600))),
            'sunrise': datetime(2022, 6, 21, 3, 47, tzinfo=timezone(timedelta(seconds=3600))),
            'sunset': datetime(2022, 6, 21, 20, 26, tzinfo=timezone(timedelta(seconds=3600))),
            'timestamp': datetime(2022, 6, 21, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600)))
        }
        self.assertEqual(astral_times, expected_times)

    def test_get_astral_times_extented_invalid(self):
        m = AstralTimesManager(self.DUMMY_CONFIG)
        now = datetime(2022, 6, 21, 9, 55, 15, tzinfo=timezone(timedelta(seconds=7200)))

        keys = AstralTime.extended_values()

        astral_times = {}

        for key in keys:
            astral_times[key] = m.get_astral_time(key, now)

        self.assertEqual({
            'dawn10': datetime(2022, 6, 21, 3, 14, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn11': datetime(2022, 6, 21, 3, 1, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn12': datetime(2022, 6, 21, 2, 46, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn13': datetime(2022, 6, 21, 2, 28, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn14': datetime(2022, 6, 21, 2, 6, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn15': datetime(2022, 6, 21, 1, 27, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn16': None,
            'dawn17': None,
            'dawn18': None,
            'dawn2': datetime(2022, 6, 21, 4, 36, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn3': datetime(2022, 6, 21, 4, 27, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn4': datetime(2022, 6, 21, 4, 18, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn5': datetime(2022, 6, 21, 4, 9, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn6': datetime(2022, 6, 21, 3, 59, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn7': datetime(2022, 6, 21, 3, 49, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn8': datetime(2022, 6, 21, 3, 38, tzinfo=timezone(timedelta(seconds=7200))),
            'dawn9': datetime(2022, 6, 21, 3, 27, tzinfo=timezone(timedelta(seconds=7200))),
            'dawnAstro': datetime(2022, 6, 21, 2, 46, tzinfo=timezone(timedelta(seconds=7200))),
            'dawnCivil': datetime(2022, 6, 21, 3, 59, tzinfo=timezone(timedelta(seconds=7200))),
            'dawnNautical': None,
            'dusk10': datetime(2022, 6, 21, 22, 59, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk11': datetime(2022, 6, 21, 23, 13, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk12': datetime(2022, 6, 21, 23, 28, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk13': datetime(2022, 6, 21, 23, 46, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk14': datetime(2022, 6, 22, 0, 8, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk15': datetime(2022, 6, 22, 0, 47, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk16': None,
            'dusk17': None,
            'dusk18': None,
            'dusk2': datetime(2022, 6, 21, 21, 38, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk3': datetime(2022, 6, 21, 21, 46, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk4': datetime(2022, 6, 21, 21, 55, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk5': datetime(2022, 6, 21, 22, 5, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk6': datetime(2022, 6, 21, 22, 14, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk7': datetime(2022, 6, 21, 22, 25, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk8': datetime(2022, 6, 21, 22, 36, tzinfo=timezone(timedelta(seconds=7200))),
            'dusk9': datetime(2022, 6, 21, 22, 47, tzinfo=timezone(timedelta(seconds=7200))),
            'duskAstro': datetime(2022, 6, 21, 23, 28, tzinfo=timezone(timedelta(seconds=7200))),
            'duskCivil': datetime(2022, 6, 21, 22, 14, tzinfo=timezone(timedelta(seconds=7200))),
            'duskNautical': None,
            'midnight': datetime(2022, 6, 21, 1, 7, tzinfo=timezone(timedelta(seconds=7200))),
            'noon': datetime(2022, 6, 21, 13, 7, tzinfo=timezone(timedelta(seconds=7200))),
            'sunrise': datetime(2022, 6, 21, 4, 47, tzinfo=timezone(timedelta(seconds=7200))),
            'sunset': datetime(2022, 6, 21, 21, 26, tzinfo=timezone(timedelta(seconds=7200))),
        }, astral_times)

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

        # noinspection SpellCheckingInspection
        hit = m.hits(" sunrisE ", t_sunrise)
        self.assertEqual(hit, True)
        self.assertEqual(len(m._cached_values), 2)

        # another hour => cache ist reset
        hit = m.hits("       Sunrise ", t_no_hit)
        self.assertEqual(hit, False)
        self.assertEqual(len(m._cached_values), 1)  # cache was reset
