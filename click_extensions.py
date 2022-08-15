from lib2to3.pgen2.token import STRING
from tkinter.tix import INTEGER
import click
import yaml
import configparser


def CommandWithConfigOverload(config_file_param_name: str, auth_config: configparser.ConfigParser):
    class CustomCommandClass(click.Command):
        def invoke(self, ctx):
            # YAML file gets prescendence over config file
            config_file = ctx.params[config_file_param_name]
            if config_file is not None:
                with open(config_file) as f:
                    config_data = yaml.safe_load(f)
                    for param, value in ctx.params.items():
                        print(param, value)
                        if value is None and param in config_data:
                            ctx.params[param] = config_data[param]

            if auth_config is not None:
                for param, value in ctx.params.items():
                    if value is None and param in auth_config['default']:
                        ctx.params[param] = auth_config['default'][param]

            return super(CustomCommandClass, self).invoke(ctx)
    return CustomCommandClass
