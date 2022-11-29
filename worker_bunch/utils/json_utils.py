import datetime
import json

from worker_bunch.utils.time_utils import TimeUtils


class JsonUtils:

    @classmethod
    def _default_json_serial(cls, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime.datetime):
            return TimeUtils.iso_tz(obj)
        elif isinstance(obj, datetime.date):
            return obj.isoformat()

        raise TypeError(f"Type '{type(obj)}' is not JSON serializable!")

    @classmethod
    def dumps(cls, data, sort_keys=True, indent=None) -> str:
        return json.dumps(data, indent=indent, sort_keys=sort_keys, default=cls._default_json_serial)
