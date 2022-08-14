import click
import configparser
import snow_connector

CONFIG = configparser.ConfigParser()
CONFIG.read('credentials')
SNOWFLAKE_CONN: snow_connector.SnowflakeConnector

@click.command()
def create():
    if isAuth():
        connectToSnowflake()
        click.echo(SNOWFLAKE_CONN.getVersion())

@click.command()
def deploy():
    if isAuth():
        click.echo('Deploying...')

@click.command()
def build():
    if isAuth():
        click.echo(f'Building... with {CONFIG.get("default", "account")}')

@click.command()
@click.option('--account', prompt=True, help='Snowflake account')
@click.option('--username', prompt=True, help='Snowflake username')
@click.option('--password', prompt=True, hide_input=True, help='Snowflake password')
def login(account, username, password):
    global CONFIG
    CONFIG['default'] = {
        'account': account,
        'username': username,
        'password': password
    }
    with open('credentials', 'w') as configfile:
        CONFIG.write(configfile)

@click.group()
def cli():
    pass

cli.add_command(create)
cli.add_command(build)
cli.add_command(deploy)
cli.add_command(login)

def isAuth():
    if not CONFIG.has_option('default', 'account'):
        click.echo('You must login first with `snowcli login`')
        return False
    return True

def connectToSnowflake():
    global SNOWFLAKE_CONN
    SNOWFLAKE_CONN = snow_connector.SnowflakeConnector(CONFIG.get('default', 'account'), CONFIG.get('default', 'username'), CONFIG.get('default', 'password'))

def main():
    cli()