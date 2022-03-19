import copy
import os
from typing import Dict, Optional


import yaml
from jsonschema import validate

from worker_bunch.service_config import MainConfKey, CONFIG_JSONSCHEMA
from worker_bunch.worker.worker_config import WorkerSettingsDeclaration


class ServiceConfigurator:

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

        schema["properties"][MainConfKey.WORKER_SETTINGS] = worker_settings_schema
        return schema

    def revalidate_worker_extra_settings(self, declarations: Dict[str, WorkerSettingsDeclaration]):
        extended_schema = self.create_extended_json_schema(declarations)
        validate(self._config_data, extended_schema)

    def get_worker_instances_config(self):
        return self._config_data.get(MainConfKey.WORKER_INSTANCES, {})

    def get_worker_settings(self):
        return self._config_data.get(MainConfKey.WORKER_SETTINGS, {})

    def get_logging_config(self):
        return self._config_data.get(MainConfKey.LOGGING, {})

    def get_mqtt_config(self):
        return self._config_data.get(MainConfKey.MQTT_BROKER, {})

    def get_astral_config(self):
        return self._config_data.get(MainConfKey.ASTRAL_TIMES, {})

    def get_database_config(self):
        return self._config_data.get(MainConfKey.DATABASE_CONNECTIONS, {})

    @classmethod
    def check_config_file_access(cls, config_file):
        if not os.path.isfile(config_file):
            raise FileNotFoundError('config file ({}) does not exist!'.format(config_file))

        permissions = oct(os.stat(config_file).st_mode & 0o777)[2:]
        if permissions != "600":
            extra = "change via 'chmod'. this config file may contain sensitive information."
            raise PermissionError(f"wrong config file permissions ({config_file}: expected 600, got {permissions})! {extra}")

    @classmethod
    def generate_config_file_json_schema(cls, workers_extra_configs: Optional[Dict[str, WorkerSettingsDeclaration]]):
        if workers_extra_configs:
            schema = cls.create_extended_json_schema(workers_extra_configs)
        else:
            schema = CONFIG_JSONSCHEMA
        return schema
