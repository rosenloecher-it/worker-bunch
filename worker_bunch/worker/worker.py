import abc
import copy
import logging
import threading
from logging import Logger
from typing import Dict, List, Optional, Set

from worker_bunch.database_manager import DatabaseManager
from worker_bunch.dispatcher import Dispatcher, DispatcherListener
from worker_bunch.exceptions import ShutdownException
from worker_bunch.mqtt.mqtt_proxy import MqttProxy
from worker_bunch.service_logging import ServiceLogging
from worker_bunch.notification import Notification
from worker_bunch.time_utils import TimeUtils


class Worker(threading.Thread, DispatcherListener):

    def __init__(self, name: str):
        threading.Thread.__init__(self, name=name)

        self._lock = threading.Lock()
        self._closing = False  # shutdown in process

        self.__logger = None  # type: Optional[Logger]

        self._notifications = set()  # type: Set[Notification]

        self._database_manager = None  # type: Optional[DatabaseManager]
        self._extra_settings = {}  # type: Dict[str, any]
        self._mqtt_proxy = None

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, self.name)

    def __repr__(self) -> str:
        return '{}({})'.format(self.__class__.__name__, self.name)

    def set_extra_settings(self, extra_settings: Optional[Dict[str, any]]):
        with self._lock:
            self._extra_settings = copy.deepcopy(extra_settings) if extra_settings else {}

    def set_database_manager(self, database_manager: DatabaseManager):
        with self._lock:
            self._database_manager = database_manager

    def set_mqtt_proxy(self, mqtt_proxy: MqttProxy):
        with self._lock:
            self._mqtt_proxy = mqtt_proxy

    def get_partial_settings_schema(self) -> Optional[Dict[str, any]]:
        """returns a partial JSON schema for extra setting if needed. overwrite..."""
        return None

    def make_partial_settings_required(self) -> bool:
        """return True to force existence via JSON schema validation. overwrite..."""
        return False

    def set_last_will(self):
        """Set the last will at mqtt client. Explicitly triggered before the mqtt client connects. Kind of:
        ```
        if self._topic and self._last_will:
            self._mqtt_proxy.set_last_will(self._topic, self._last_will)
        ```
        """

    def stop(self):
        """
        Just the notification to finish and stop the thread. A last will may be better send within `_final_work`.
        """
        with self._lock:
            self._closing = True

    def proceed(self) -> bool:
        with self._lock:
            if self._closing:
                raise ShutdownException()
            return True

    @property
    def _logger(self):
        if self.__logger is None:
            log_name = ServiceLogging.get_log_name(self, self.name)
            self.__logger = logging.getLogger(log_name)
        return self.__logger

    # noinspection PyMethodMayBeStatic
    def _sleep(self):
        TimeUtils.sleep(0.5)

    @abc.abstractmethod
    def subscribe_notifications(self, dispatcher: Dispatcher):
        """
        subscribe for notifications via:

        timer_job = schedule.every(14 * 60).to(16 * 60).seconds  # type: schedule.Job
        dispatcher.subscribe_timer(self, timer_job, "keyword-timer")

        self._dispatcher.subscribe_cron("* * * * *", self, "cron-every-minute")

        self._dispatcher.subscribe_topics(self, ["mqtt/topic"],
        """
        raise NotImplementedError()

    def add_notifications(self, notifications: Set[Notification]):
        with self._lock:
            self._notifications |= notifications

    def _get_and_reset_notifications(self) -> List[Notification]:
        with self._lock:
            notifications = list(self._notifications)
            self._notifications.clear()
            return notifications

    def _reset_notifications(self):
        with self._lock:
            return self._notifications.clear()

    def _should_handle_pending_notifications(self) -> bool:
        with self._lock:
            return bool(self._notifications)

    def run(self):
        try:
            while self.proceed():
                self._process_notifications()
                self._sleep()

        except ShutdownException:
            self._logger.debug('shutdown')
        except Exception as ex:
            self._logger.exception(ex)
        finally:
            self._final_work()

    def run_single(self):
        try:
            if self.proceed():
                self._process_notifications()

        except ShutdownException:
            self._logger.debug('shutdown')
        except Exception as ex:
            self._logger.exception(ex)
        finally:
            self._final_work()

    def _process_notifications(self):
        if self._should_handle_pending_notifications():
            notifications = self._get_and_reset_notifications()
            if notifications:
                self._work(notifications)

    @abc.abstractmethod
    def _work(self, notifications: List[Notification]):
        """the task "main" function."""
        raise NotImplementedError()

    def _final_work(self):
        """
        E.g.: You may send explicitly the last will(s) here (as a normal message). Kind of:
        ```
        if self._topic and self._last_will:
            self._mqtt_proxy.publish(self._topic, self._last_will)
        ```
        """
