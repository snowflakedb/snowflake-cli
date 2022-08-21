import configparser
import click
import os
from pathlib import Path

from snowcli.snow_connector import SnowflakeConnector

# XXX: This is quite ugly, but we store in .snowcli/credentials either:
# 1. The actual creds for your Snowflake connection
# 2. The path to the snowsql-formatted config and the connection name you want to use
# TODO: Also, maybe look at what click offers https://click.palletsprojects.com/en/8.1.x/utils/#finding-application-folders 
config_file_path = os.path.expanduser('~/.snowcli/credentials')

auth_config = configparser.ConfigParser()
auth_config.read(config_file_path)
snowflake_connection: SnowflakeConnector

def connectToSnowflake():
    global snowflake_connection
    if 'snowsql_config_path' in auth_config['default']:
        snowflake_connection = SnowflakeConnector.fromConfig(
                auth_config.get('default', 'snowsql_config_path'), auth_config.get('default', 'snowsql_connection'))
    else:
        snowflake_connection = SnowflakeConnector(auth_config.get(
            'default', 'account'), auth_config.get('default', 'username'), auth_config.get('default', 'password'))


def isAuth():
    if not auth_config.has_section('default') and not auth_config.has_option('default', 'account'):
        click.echo('You must login first with `snowcli login`')
        return False
    return True
