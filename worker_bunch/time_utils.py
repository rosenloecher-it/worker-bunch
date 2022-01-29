import datetime
import time

import pycron
from tzlocal import get_localzone


class TimeUtils:

    @classmethod
    def now(cls, no_ms=False) -> datetime.datetime:
        """overwrite/mock in test"""
        now = datetime.datetime.now(tz=get_localzone())
        if no_ms:
            now = now.replace(microsecond=0)
        return now

    @classmethod
    def sleep(cls, millis) -> int:
        time.sleep(millis)
        return millis

    @classmethod
    def cron_trigger(cls, cron: str, since: datetime.datetime, now: datetime.datetime = None):
        now = now if now is not None else cls.now()
        return pycron.has_been(cron, since, now)
