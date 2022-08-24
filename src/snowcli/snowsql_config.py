import os
import configparser

class SnowsqlConfig():
    def __init__(self, path='~/.snowsql/config'):
        self.path = path
        self.config = configparser.ConfigParser()
        self.config.read(os.path.expanduser(path))

    def get_connection(self, connection_name):
        connection = self.config['connections.'+connection_name]
        # Remap names to appropriate args in Python Connector API
        connection = dict((k.replace('name', ''), v.strip('"')) for k, v in connection.items())
        return connection

    def add_connection(self, connection_name, entry):
        connection = self.config[f"connections.{connection_name}"]
        for (k, v) in entry.items():
            connection.set(k, v)

        with open(self.path, 'wb') as f:
            self.config.write(f)

