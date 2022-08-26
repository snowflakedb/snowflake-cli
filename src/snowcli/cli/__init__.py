import click
from pathlib import Path
from prettytable import PrettyTable
import sys

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

@click.group()
def connection():
    pass

@click.command("list")
def connection_list():
    app_cfg = AppConfig().config
    if 'snowsql_config_path' not in app_cfg:
        click.echo("No snowsql config path set. Please run snowcli login first.")

    click.echo(f"Using {app_cfg['snowsql_config_path']}...")

    cfg = SnowsqlConfig(app_cfg['snowsql_config_path'])
    table = PrettyTable()
    table.field_names = ["Connection", "Account", "Username"]
    for (connection_name, v) in cfg.config.items():
        if connection_name.startswith("connections."):
            connection_name = connection_name.replace("connections.", "")
            if 'account' in v:
                table.add_row([connection_name, v['account'], v['username']])
            if 'accountname' in v:
                table.add_row([connection_name, v['accountname'], v['username']])
    click.echo(table)

@click.command("add")
@click.option('--connection', prompt='Name for this connection', default='', help='Snowflake connection name', required=True)
@click.option('--account', prompt='Snowflake account name', default='', help='Snowflake database', required=True)
@click.option('--username', prompt='Snowflake username', default='', help='Snowflake schema', required=True)
@click.option('--password', prompt='Snowflake password', default='', hide_input=True, help='Snowflake password', required=True)
def connection_add(connection, account, username, password):
    app_cfg = AppConfig().config
    cfg = SnowsqlConfig(app_cfg['snowsql_config_path'])
    connection_entry = {
        'account': account,
        'username': username,
        'password': password,
    }
    cfg.add_connection(connection, connection_entry)
    click.echo(f"Wrote new connection {connection} to {cfg.path}")

@click.command()
@click.option('--config', '-c', 'snowsql_config_path', prompt='Path to Snowsql config', default='~/.snowsql/config', help='snowsql config file')
@click.option('--connection', '-C', 'snowsql_connection_name', prompt='Connection name (for entry in snowsql config)', help='connection name from snowsql config file')
def login(snowsql_config_path, snowsql_connection_name):
    if not Path(snowsql_config_path).expanduser().exists():
        click.echo(f"Path to snowsql config does not exist: {snowsql_config_path}")
        sys.exit(1)

    cfg = SnowsqlConfig(snowsql_config_path)
    if f"connections.{snowsql_connection_name}" not in cfg.config:
        click.echo(f"Connection not found in {snowsql_config_path}: {snowsql_connection_name}")
        sys.exit(1)

    cfg = AppConfig()
    cfg.config['snowsql_config_path'] = snowsql_config_path
    cfg.config['snowsql_connection_name'] = snowsql_connection_name
    cfg.save()
    click.echo(f"Using connection name {snowsql_connection_name} in {snowsql_config_path}")
    click.echo(f"Wrote {cfg.path}")

@click.command()
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
    click.echo(f"Wrote new environment {environment} to {cfg.path}")

connection.add_command(connection_list)
connection.add_command(connection_add)

cli.add_command(function)
cli.add_command(streamlit, "streamlit")
cli.add_command(login)
cli.add_command(configure)
cli.add_command(connection)

#cli.add_command(notebooks)
#cli.add_command(procedure)

if __name__ == '__main__':
    main()
