import os
import configparser

class SnowsqlConfig():
    def __init__(self, path='~/.snowsql/config'):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.expanduser(path))

    def getConnection(self, connection_name):
        connection = self.config['connections.'+connection_name]
        # Remap names to appropriate args in Python Connector API
        connection = dict((k.replace('name', ''), v.strip('"')) for k, v in connection.items())
        return connection

