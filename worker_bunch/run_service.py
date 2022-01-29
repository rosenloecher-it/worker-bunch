import logging
import sys
from typing import List, Optional

import click
from jsonschema import ValidationError

from worker_bunch.service_config import ServiceConfig
from worker_bunch.service_logging import LOGGING_CHOICES, ServiceLogging
from worker_bunch.database_manager import DatabaseManager
from worker_bunch.dispatcher import Dispatcher
from worker_bunch.mqtt.mqtt_client import MqttClient
from worker_bunch.mqtt.mqtt_proxy import MqttProxy
from worker_bunch.runner import Runner
from worker_bunch.time_utils import TimeUtils
from worker_bunch.worker.worker import Worker
from worker_bunch.worker.worker_factory import WorkerFactory

_logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--json-schema",
    is_flag=True,
    help="Prints the config file JSON schema and exits."
)
@click.option(
    "--config-file",
    help="Config file",
)
@click.option(
    "--log-file",
    help="Log file (if stated journal logging is disabled)"
)
@click.option(
    "--log-level",
    help="Log level",
    type=click.Choice(LOGGING_CHOICES, case_sensitive=False),
)
@click.option(
    "--print-log-console",
    is_flag=True,
    help="Print log output to console too."
)
@click.option(
    "--skip-log-times",
    is_flag=True,
    help="Skip log timestamp (systemd/journald logs get their own timestamp)."
)
def worker_bunch_main(config_file, json_schema, log_file, log_level, print_log_console, skip_log_times):
    """A (smart)home task/rule engine where configurable workers (threads) get notified about MQTT messages or timer events."""

    try:
        if json_schema:
            print_json_schema(config_file)
        else:
            run_service(config_file, log_file, log_level, print_log_console, skip_log_times)

    except KeyboardInterrupt:
        pass  # exits 0 by default

    except Exception as ex:
        _logger.exception(ex)
        sys.exit(1)  # a simple return is not understood by click


def _shutdown_workers(workers: List[Worker]):
    for worker in workers:
        try:
            worker.stop()
        except Exception as ex:
            _logger.exception(ex)

    time_to_wait = 10.0

    while True:
        time_to_wait -= TimeUtils.sleep(0.02)
        alive_workers = [w for w in workers if w.is_alive()]
        if not alive_workers:
            break
        if time_to_wait < 0:
            _logger.warn("workers doesn't closing properly: %s", ",".join([w.name for w in workers]))
            break


def run_service(config_file, log_file, log_level, print_log_console, skip_log_times):
    dispatcher: Optional[Dispatcher] = None
    mqtt_client: Optional[MqttClient] = None
    mqtt_proxy: Optional[MqttProxy] = None
    workers: List[Worker] = []

    try:
        # configuring
        service_config = ServiceConfig()
        service_config.read_config_file(config_file)

        ServiceLogging.configure(
            service_config.get_logging_config(),
            log_file, log_level, print_log_console, skip_log_times
        )
        _logger.debug("start")

        worker_instances_config = service_config.get_worker_instances_config()
        worker_dict = WorkerFactory.create_workers(worker_instances_config)
        workers = list(worker_dict.values())
        workers_settings_declarations = WorkerFactory.extract_workers_settings_declarations(worker_dict)

        service_config.revalidate_worker_extra_settings(workers_settings_declarations)

        # bootstrapping
        dispatcher = Dispatcher()
        database_manager = DatabaseManager(service_config.get_database_config())

        mqtt_config = service_config.get_mqtt_config()
        mqtt_client = MqttClient(mqtt_config) if mqtt_config else None
        mqtt_proxy = MqttProxy(mqtt_client)

        workers_settings = service_config.get_worker_settings()

        for worker in workers:
            # before set_mqtt_proxy (last will depends on config)!
            worker.set_extra_settings(workers_settings.get(worker.name))

            worker.set_database_manager(database_manager)
            worker.set_mqtt_proxy(mqtt_proxy)
            worker.set_last_will()

        # start
        runner = Runner(dispatcher, mqtt_proxy, workers)
        runner.run()

    finally:
        _logger.info("shutdown")

        if dispatcher is not None:  # first: stop
            try:
                dispatcher.close()
            except Exception as ex:
                _logger.exception(ex)

        _shutdown_workers(workers)

        if mqtt_proxy is not None:
            try:
                mqtt_proxy.close()
            except Exception as ex:
                _logger.exception(ex)

        if mqtt_client is not None:
            try:
                mqtt_client.close()
            except Exception as ex:
                _logger.exception(ex)


def print_json_schema(config_file: str):
    try:
        if not config_file:
            print("The JSON schema output is supposed to contain also the worker extra settings, but therefore it needs a specific "
                  "configuration. So please provide the configuration file.\n")
            ServiceConfig.print_config_file_json_schema(None)
            return

        service_config = ServiceConfig()
        service_config.read_config_file(config_file, skip_file_access_check=True)

        worker_instances_config = service_config.get_worker_instances_config()
        workers = WorkerFactory.create_workers(worker_instances_config)
        workers_extra_configs = WorkerFactory.extract_workers_settings_declarations(workers)

        ServiceConfig.print_config_file_json_schema(workers_extra_configs)

    except ValidationError as ex:
        print("The JSON schema output is supposed to contain also the worker extra settings, but that is only possible "
              "if the base settings can be read flawlessly. Actually there is a problem reading the config file, so only "
              "the base JSON schema is shown.\n\nActual problem: {}\n".format(str(ex.message)))
        ServiceConfig.print_config_file_json_schema(None)


if __name__ == '__main__':
    worker_bunch_main()  # exit codes must be handled by click!
