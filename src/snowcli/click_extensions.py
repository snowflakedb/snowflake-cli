import click
from snowcli.config import AppConfig

def require_environment(function):
    app_config = AppConfig()

    function = click.option(
        '--environment', '-e', help='Environment name', default=app_config.config.get('environment'))(function)
    return function
