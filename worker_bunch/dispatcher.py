import abc
import logging
from typing import Dict, List, Optional, Set

import pycron
import rx
import attr
import schedule
from rx import operators as rx_ops
from paho.mqtt.client import MQTTMessage
from rx.core import Observer
from rx.disposable import Disposable

from worker_bunch.notification import Notification, NotificationType, NotificationBucket
from worker_bunch.time_utils import TimeUtils


_logger = logging.getLogger(__name__)


class DispatcherListener:

    @abc.abstractmethod
    def add_notifications(self, notifications: Set[Notification]):
        raise NotImplementedError()

    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError()


@attr.frozen
class TopicMatch:

    topic: str
    search_pattern: str = None
    listeners: Set[DispatcherListener] = attr.Factory(set)


class Dispatcher:
    """
    Runs only in one thread context.
    """

    DEFAULT_DEBOUNCE_TIME = 0.1

    def __init__(self):

        self._shutdown = False

        self._last_cron_time = TimeUtils.now()

        self._max_debounce_time = 0

        # separation of notifications and trigger, notifications are overwritten by newer ones
        self._notifications: Dict[DispatcherListener, NotificationBucket] = {}

        self._exact_topic_matches: Dict[str, TopicMatch] = {}
        self._wildcard_topic_matches: List[TopicMatch] = []

        self._cron_subscriptions: Dict[str, TopicMatch] = {}
        self._observers: Dict[DispatcherListener, Optional[Observer]] = {}

        # only simple values can be pushed through pipelines. So an "listener-id" instead if the listener reference gets pushed.
        self._observer_listener: Dict[int, DispatcherListener] = {}

        self._disposables: List[Disposable] = []

    def close(self):
        self._shutdown = True

        for observable in self._observers.values():
            observable.on_completed()
        self._observers = {}

        wait_for_emptied_queues = min(self._max_debounce_time + 0.05, 1.0)
        TimeUtils.sleep(wait_for_emptied_queues)

        for disposable in self._disposables:
            disposable.dispose()
        self._disposables = []

    def get_mqtt_topics(self) -> List[str]:
        topics = [m.topic for m in self._wildcard_topic_matches]
        topics.extend([m.topic for m in self._exact_topic_matches.values()])
        return topics

    @classmethod
    def check_and_extract_wildcard_topic(cls, topic):
        if topic.endswith("#"):
            return topic.strip("#")
        else:
            return None

    def _register_mqtt_topic(self, listener: DispatcherListener, topic: str):
        wildcard_search_pattern = self.check_and_extract_wildcard_topic(topic)
        if wildcard_search_pattern:
            topic_match = next((m for m in self._wildcard_topic_matches if m.topic == topic), None)
            if topic_match is None:
                topic_match = TopicMatch(topic=topic, search_pattern=wildcard_search_pattern)
                self._wildcard_topic_matches.append(topic_match)
            topic_match.listeners.add(listener)

        else:
            topic_match = self._exact_topic_matches.get(topic)
            if topic_match is None:
                topic_match = TopicMatch(topic=topic)
                self._exact_topic_matches[topic] = topic_match
            topic_match.listeners.add(listener)

    def subscribe_mqtt_topics(self, listener: DispatcherListener, topics: List[str],
                              debounce_time: float = DEFAULT_DEBOUNCE_TIME) -> None:
        if listener in self._observer_listener:
            raise RuntimeError(f"Listener ({listener.name}) has already subscribed (only one pipeline per listener)!")
        self._observer_listener[id(listener)] = listener

        self._max_debounce_time = max(self._max_debounce_time, debounce_time)

        for topic in topics:
            self._register_mqtt_topic(listener, topic)

        def creating_observer_callback(observer, _):
            self._observers[listener] = observer

        observable = rx.create(creating_observer_callback)

        instance = self
        disposable = observable.pipe(
            rx_ops.debounce(debounce_time)
        ).subscribe(lambda listener_id: instance._send_notifications_by_id(listener_id))

        self._disposables.append(disposable)

    def subscribe_cron(self, listener: DispatcherListener, cron: str, topic: str):
        topic_match = self._cron_subscriptions.get(cron)
        if topic_match is None:
            topic_match = TopicMatch(topic=topic)
            self._cron_subscriptions[cron] = topic_match
        topic_match.listeners.add(listener)

    def subscribe_timer(self, listener: DispatcherListener, timer_job: schedule.Job, topic: str):
        """
        :param listener:
        :param timer_job:
        :param topic:
        :return:
        """
        instance = self

        def timer_closure():
            notification = Notification(
                type=NotificationType.TIMER,
                topic=topic,
                payload=None
            )
            instance._store_notification(listener, notification)
            instance._send_notifications(listener)

        timer_job.do(timer_closure)

    def trigger_timers(self):
        """Triggers timer and cron notification,"""
        if self._shutdown:
            return

        schedule.run_pending()

        now = TimeUtils.now().replace(second=0, microsecond=0)

        if self._last_cron_time < now:  # next minute
            send_to: Set[DispatcherListener] = set()

            for cron, topic_match in self._cron_subscriptions.items():
                if pycron.has_been(cron, self._last_cron_time, now):
                    notification = Notification(type=NotificationType.CRON, topic=topic_match.topic, payload=None)
                    for listener in topic_match.listeners:
                        self._store_notification(listener, notification)
                        send_to.add(listener)
            self._last_cron_time = now

            for listener in send_to:
                self._send_notifications(listener)

    def push_mqtt_messages(self, messages: List[MQTTMessage]):
        if self._shutdown:
            return

        for message in messages:
            notification = Notification.create_from_mqtt(message)

            listeners: Set[DispatcherListener] = set()

            match = self._exact_topic_matches.get(notification.topic)
            if match:
                listeners.update(match.listeners)

            for match in self._wildcard_topic_matches:
                if notification.topic.startswith(match.search_pattern):
                    listeners.update(match.listeners)

            for listener in list(listeners):
                self._store_notification(listener, notification)
                self._queue_notification(listener)

    def _queue_notification(self, listener: DispatcherListener):
        observer = self._observers[listener]  # must exists
        observer.on_next(id(listener))

    def _store_notification(self, listener: DispatcherListener, notification: Notification):
        bucket: Optional[NotificationBucket] = self._notifications.get(listener)
        if bucket is None:
            bucket = NotificationBucket()
            self._notifications[listener] = bucket
        bucket.add(notification)

    def _send_notifications_by_id(self, listener_id: int):
        listener = self._observer_listener.get(listener_id)
        self._send_notifications(listener)

    def _send_notifications(self, listener: DispatcherListener):
        bucket: Optional[NotificationBucket] = self._notifications.get(listener)
        if bucket:
            listener.add_notifications(bucket.get_set())
            bucket.clear()
