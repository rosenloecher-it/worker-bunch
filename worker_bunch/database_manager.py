import copy


class DatabaseManager:

    def __init__(self, config):

        self._config = copy.deepcopy(config)

    def get_database_connection(self):
        pass

    def free_database_connection(self):
        pass

    def get_mqtt_connector(self):
        pass

    def create_mqtt_client(self):
        pass
