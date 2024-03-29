import unittest
from datetime import datetime, timezone, timedelta

from worker_bunch.utils.json_utils import JsonUtils


class TestJsonUtils(unittest.TestCase):

    def test_dumps(self):

        original_data = {
            "timestamp": datetime(2022, 3, 19, 9, 55, 15, tzinfo=timezone(timedelta(seconds=3600))),
            "boolean": True,
            "integer": 456,
            "float": 1.234,
            "text": "text123",
        }
        expected_data = '{"boolean": true, "float": 1.234, "integer": 456, "text": "text123", "timestamp": "2022-03-19T09:55:15+01:00"}'

        result_data = JsonUtils.dumps(original_data)

        self.assertEqual(result_data, expected_data)

    def test_loads_dict(self):
        self.assertEqual(JsonUtils.loads_dict('{"s": 1}'), {"s": 1})

        with self.assertRaises(ValueError):
            JsonUtils.loads_dict("4")
