from lib2to3.pgen2.token import STRING
from tkinter.tix import INTEGER
import click
import yaml
import configparser


def CommandWithConfigOverload(config_file_param_name: str, auth_config: configparser.ConfigParser):
    class CustomCommandClass(click.Command):
        def invoke(self, ctx):
            if auth_config is not None:
                for param, value in ctx.params.items():
                    if value is None and param in auth_config['default']:
                        ctx.params[param] = auth_config['default'][param]

            return super(CustomCommandClass, self).invoke(ctx)
    return CustomCommandClass
