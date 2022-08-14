import configparser
import snow_connector
import click

auth_config = configparser.ConfigParser()
auth_config.read('credentials')
snowflake_connection: snow_connector.SnowflakeConnector

def connectToSnowflake():
    global snowflake_connection
    snowflake_connection = snow_connector.SnowflakeConnector(auth_config.get('default', 'account'), auth_config.get('default', 'username'), auth_config.get('default', 'password'))

def isAuth():
    if not auth_config.has_option('default', 'account'):
        click.echo('You must login first with `snowcli login`')
        return False
    return True