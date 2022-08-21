from lib2to3.pgen2.token import STRING
import click
import yaml
import configparser
from snowcli.snowsql_config import SnowsqlConfig

def CommandWithConfigOverload(config_file_param_name: str, auth_config: configparser.ConfigParser):
    class CustomCommandClass(click.Command):
        def invoke(self, ctx):
            if auth_config is not None:
                conf = auth_config['default']

                if 'snowsql_config_path' in auth_config['default']:
                    conf = SnowsqlConfig(auth_config['default']['snowsql_config_path'])
                    conf = conf.getConnection(auth_config['default']['snowsql_connection'])

                for param, value in ctx.params.items():
                    if value is None and param in conf:
                        ctx.params[param] = conf[param]

            return super(CustomCommandClass, self).invoke(ctx)
    return CustomCommandClass
