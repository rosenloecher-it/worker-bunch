from worker_bunch.service_logging import LOGGING_JSONSCHEMA
from worker_bunch.database.database_config import DATABASE_CONNECTIONS_JSONSCHEMA
from worker_bunch.mqtt.mqtt_config import MQTT_JSONSCHEMA
from worker_bunch.worker.worker_config import WORKER_INSTANCES_JSONSCHEMA


class ConfigException(Exception):
    pass


class MainConfKey:
    LOGGING = "logging"
    MQTT_BROKER = "mqtt_broker"
    DATABASE_CONNECTIONS = "database_connections"
    WORKER_INSTANCES = "worker_instances"
    WORKER_SETTINGS = "worker_settings"


CONFIG_JSONSCHEMA = {
    "type": "object",
    "properties": {
        MainConfKey.LOGGING: LOGGING_JSONSCHEMA,
        MainConfKey.MQTT_BROKER: MQTT_JSONSCHEMA,
        MainConfKey.WORKER_INSTANCES: WORKER_INSTANCES_JSONSCHEMA,
        MainConfKey.WORKER_SETTINGS: {"type": "object"},  # dummy
        MainConfKey.DATABASE_CONNECTIONS: DATABASE_CONNECTIONS_JSONSCHEMA,
    },
    "additionalProperties": False,
    "required": [MainConfKey.WORKER_INSTANCES],
}
