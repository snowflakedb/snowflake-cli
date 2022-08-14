import click
import configparser
import snow_connector

CONFIG = configparser.ConfigParser()
CONFIG.read('credentials')
SNOWFLAKE_CONN: snow_connector.SnowflakeConnector

@click.group()
def function():
    pass

@click.command()
@click.option('--name', '-n', help='Name of the function')
@click.option('--database', '-d', help='Database name')
@click.option('--schema', '-s', help='Schema name')
def function_create(name, database, schema):
    if isAuth():
        connectToSnowflake()
        click.echo(SNOWFLAKE_CONN.createFunction(name='testFunction', inputParameters='', returnType='string', handler='app.run', imports='@~/my-deployment-package.zip', database='JEFFHOLLAN_DEMO', schema='PUBLIC', role='ACCOUNTADMIN', warehouse='SRI'))

@click.command()
def function_deploy():
    if isAuth():
        click.echo('Deploying...')

@click.command()
def function_build():
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

@click.command()
def procedure():
    pass

@click.command()
def streamlit():
    pass

function.add_command(function_create, 'create')
function.add_command(function_deploy, 'deploy')
function.add_command(function_build, 'build')
cli.add_command(function)
cli.add_command(procedure)
cli.add_command(streamlit)
cli.add_command(login)
