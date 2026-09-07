from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflake.cli.api.plugins.command.bridge import build_command_spec
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
)


class ReportingHandler:
    def run(self, *args, **kwargs):
        return None


REPORTING_SPEC = CommandGroupSpec(
    name="reporting",
    help="Generate and manage reports.",
    parent_path=(),
    commands=(
        CommandDef(
            name="run",
            help="Run a report.",
            handler_method="run",
            requires_connection=True,
            output_type="QueryResult",
        ),
    ),
)


@plugin_hook_impl
def command_spec():
    return build_command_spec(REPORTING_SPEC, ReportingHandler())


inventory_app = SnowTyperFactory(
    name="inventory",
    help="Manages inventory objects.",
)


@plugin_hook_impl
def inventory_command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=inventory_app.create_instance(),
    )


def get_builtin_plugin_name_to_plugin_spec():
    from snowflake.cli._plugins.git import plugin_spec as git_plugin_spec
    from snowflake.cli._plugins.inventory import plugin_spec as inventory_plugin_spec
    from snowflake.cli._plugins.reporting import plugin_spec as reporting_plugin_spec

    return {
        "git": git_plugin_spec,
        "reporting": reporting_plugin_spec,
        "inventory": inventory_plugin_spec,
    }
