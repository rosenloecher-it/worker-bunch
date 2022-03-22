import datetime
import json


class JsonUtils:

    @classmethod
    def _default_json_serial(cls, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()

        raise TypeError(f"Type '{type(obj)}' is not JSON serializable!")

    @classmethod
    def dumps(cls, data) -> str:
        return json.dumps(data, sort_keys=True, default=cls._default_json_serial)
