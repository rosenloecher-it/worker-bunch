from enum import Enum

from attr import frozen
from paho.mqtt.client import MQTTMessage


class NotificationType(Enum):
    CRON = "CRON"
    TIMER = "TIMER"
    MQTT_MESSAGE = "MQTT_MESSAGE"


@frozen
class Notification:

    type: NotificationType

    # contains the MQTT topic or timer key
    topic: str

    # payload is not part of key. it will be skipped if new notification for the same type an topic arrive.
    payload: str

    def __key(self):
        return self.type, self.topic

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__key() == other.__key()
        return NotImplemented

    @classmethod
    def ensure_string(cls, value_in) -> str:
        if isinstance(value_in, bytes):
            return value_in.decode("utf-8")

        return value_in

    @classmethod
    def create_from_mqtt(cls, mqtt_message: MQTTMessage):

        return Notification(
            type=NotificationType.MQTT_MESSAGE,
            topic=cls.ensure_string(mqtt_message.topic),
            payload=cls.ensure_string(mqtt_message.payload),
        )
