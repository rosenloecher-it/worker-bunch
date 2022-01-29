import copy
import json
import os
from typing import Dict, Optional

import yaml
from jsonschema import validate


from worker_bunch.service_logging import LOGGING_JSONSCHEMA
from worker_bunch.database_config import DATABASE_CONNECTIONS_JSONSCHEMA
from worker_bunch.mqtt.mqtt_config import MQTT_JSONSCHEMA
from worker_bunch.worker.worker_config import WorkerSettingsDeclaration, WORKER_INSTANCES_JSONSCHEMA


class _MainConfKey:
    LOGGING = "logging"
    MQTT_BROKER = "mqtt_broker"
    DATABASE_CONNECTIONS = "database_connections"
    WORKER_INSTANCES = "worker_instances"
    WORKER_SETTINGS = "worker_settings"


CONFIG_JSONSCHEMA = {
    "type": "object",
    "properties": {
        _MainConfKey.LOGGING: LOGGING_JSONSCHEMA,
        _MainConfKey.MQTT_BROKER: MQTT_JSONSCHEMA,
        _MainConfKey.WORKER_INSTANCES: WORKER_INSTANCES_JSONSCHEMA,
        _MainConfKey.WORKER_SETTINGS: {"type": "object"},  # dummy
        _MainConfKey.DATABASE_CONNECTIONS: DATABASE_CONNECTIONS_JSONSCHEMA,
    },
    "additionalProperties": False,
    "required": [_MainConfKey.WORKER_INSTANCES],
}


class ServiceConfig:

    def __init__(self):
        self._config_data = {}

    def read_config_file(self, config_file, skip_file_access_check=False):

        if not skip_file_access_check:
            self.check_config_file_access(config_file)

        with open(config_file, 'r') as stream:
            file_data = yaml.unsafe_load(stream)

        # first validation without worker config
        validate(file_data, CONFIG_JSONSCHEMA)

        self._config_data = file_data

    @classmethod
    def create_extended_json_schema(cls, declarations: Dict[str, WorkerSettingsDeclaration]) -> Dict[str, any]:
        schema = copy.deepcopy(CONFIG_JSONSCHEMA)

        worker_settings_schema = {"type": "object"}
        worker_settings_schema["additionalProperties"] = False

        settings_entries = {}
        worker_settings_schema["properties"] = settings_entries
        settings_required = []
        worker_settings_schema["required"] = settings_required

        for worker_name, extra_config in declarations.items():
            if extra_config.settings_schema:
                settings_entries[worker_name] = extra_config.settings_schema
                if extra_config.required:
                    settings_required.append(worker_name)

        schema[_MainConfKey.WORKER_SETTINGS] = worker_settings_schema

        return schema

    def revalidate_worker_extra_settings(self, declarations: Dict[str, WorkerSettingsDeclaration]):
        extended_schema = self.create_extended_json_schema(declarations)
        validate(self._config_data, extended_schema)

    def get_worker_instances_config(self):
        return self._config_data.get(_MainConfKey.WORKER_INSTANCES, {})

    def get_worker_settings(self):
        return self._config_data.get(_MainConfKey.WORKER_SETTINGS, {})

    def get_logging_config(self):
        return self._config_data.get(_MainConfKey.LOGGING, {})

    def get_mqtt_config(self):
        return self._config_data.get(_MainConfKey.MQTT_BROKER, {})

    def get_database_config(self):
        return self._config_data.get(_MainConfKey.DATABASE_CONNECTIONS, {})

    @classmethod
    def check_config_file_access(cls, config_file):
        if not os.path.isfile(config_file):
            raise FileNotFoundError('config file ({}) does not exist!'.format(config_file))

        permissions = oct(os.stat(config_file).st_mode & 0o777)[2:]
        if permissions != "600":
            extra = "change via 'chmod'. this config file may contain sensitive information."
            raise PermissionError(f"wrong config file permissions ({config_file}: expected 600, got {permissions})! {extra}")

    @classmethod
    def print_config_file_json_schema(cls, workers_extra_configs: Optional[Dict[str, WorkerSettingsDeclaration]]):

        if workers_extra_configs:
            schema = cls.create_extended_json_schema(workers_extra_configs)
        else:
            schema = CONFIG_JSONSCHEMA

        print(json.dumps(schema, indent=4, sort_keys=True))
