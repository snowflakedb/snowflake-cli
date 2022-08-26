import click

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig
from .function import function
from .streamlit import streamlit

@click.group()
def cli():
    pass

def main():
    cli()

@cli.command()
@click.option('--account', prompt='Snowflake account name', default='', help='Snowflake database', required=True)
@click.option('--username', prompt='Snowflake username', default='', help='Snowflake schema', required=True)
@click.option('--password', prompt='Snowflake password', default='', help='Snowflake role', required=True)
def connection_add(account, username, password):
    cfg = SnowsqlConfig(snowsql_config_path)
    connection_entry = {
        'account': account,
        'username': username,
        'password': password,
    }
    cfg.add_connection(snowsql_connection_name, connection_entry)

@cli.command()
@click.option('--config', '-c', 'snowsql_config_path', prompt='Path to Snowsql config', default='~/.snowsql/config', help='snowsql config file')
@click.option('--connection', '-C', 'snowsql_connection_name', prompt='Connection name (for entry in snowsql config)', help='connection name from snowsql config file')
def login(snowsql_config_path, snowsql_connection_name):
    # TODO: Validate that the path exists and the connection does too
    cfg = AppConfig()
    cfg.config['snowsql_config_path'] = snowsql_config_path
    cfg.config['snowsql_connection_name'] = snowsql_connection_name
    cfg.save()
    click.echo(f"Using connection name {snowsql_connection_name} in {snowsql_config_path}")
    click.echo(f"Wrote {cfg.path}")

@cli.command()
@click.option('--environment', '-e', default='dev', help='Name of environment (e.g. dev, prod, staging)', required=True)
@click.option('--database', prompt='Snowflake database', default='', help='Snowflake database', required=True)
@click.option('--schema', prompt='Snowflake schema', default='', help='Snowflake schema', required=True)
@click.option('--role', prompt='Snowflake role', default='', help='Snowflake role', required=True)
@click.option('--warehouse', prompt='Snowflake warehouse', default='', help='Snowflake warehouse', required=True)
def configure(environment, database, schema, role, warehouse):
    # TODO: Let user know if we're overwriting an existing environment
    cfg = AppConfig()
    cfg.config[environment] = {}
    cfg.config[environment]['database'] = database
    cfg.config[environment]['schema'] = schema
    cfg.config[environment]['role'] = role
    cfg.config[environment]['warehouse'] = warehouse
    cfg.save()

cli.add_command(function)
cli.add_command(streamlit, "streamlit")
cli.add_command(login)
cli.add_command(configure)

#cli.add_command(notebooks)
#cli.add_command(procedure)

if __name__ == '__main__':
    main()
