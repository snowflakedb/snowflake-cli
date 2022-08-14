import click
import config

@click.group()
def function():
    pass

@click.command()
@click.option('--name', '-n', help='Name of the function')
@click.option('--database', '-d', help='Database name')
@click.option('--schema', '-s', help='Schema name')
def function_create(name, database, schema):
    if config.isAuth():
        config.connectToSnowflake()
        click.echo(config.snowflake_connection.createFunction(name='testFunction', inputParameters='', returnType='string', handler='app.run', imports='@~/my-deployment-package.zip', database='JEFFHOLLAN_DEMO', schema='PUBLIC', role='ACCOUNTADMIN', warehouse='SRI'))

@click.command()
def function_deploy():
    if config.isAuth():
        click.echo('Deploying...')

@click.command()
def function_build():
    if config.isAuth():
        click.echo(f'Building... with {config.auth_config.get("default", "account")}')

@click.command()
@click.option('--account', prompt=True, help='Snowflake account')
@click.option('--username', prompt=True, help='Snowflake username')
@click.option('--password', prompt=True, hide_input=True, help='Snowflake password')
def login(account, username, password):
    config.auth_config['default'] = {
        'account': account,
        'username': username,
        'password': password
    }
    with open('credentials', 'w') as configfile:
        config.auth_config.write(configfile)

@click.group()
def cli():
    pass

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
