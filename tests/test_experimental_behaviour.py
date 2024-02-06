import json

import typer
from snowflake.cli.api.commands.decorators import (
    global_options,
    with_experimental_behaviour,
    with_output,
)
from snowflake.cli.api.commands.experimental_behaviour import (
    experimental_behaviour_enabled,
)
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)

from tests.testing_utils.fixtures import *

_test_experimental_typer = typer.Typer(name="test")


@_test_experimental_typer.command("hello")
@with_output
@with_experimental_behaviour()
@global_options
def _test_experimental_command(username: str, **options):
    if experimental_behaviour_enabled():
        return MessageResult(f"Experimental: Hello {username}")
    else:
        return MessageResult(f"Hello {username}")


@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=_test_experimental_typer,
    )


def _mock_test_plugin():
    from . import test_experimental_behaviour as plugin_spec

    return mock.patch(
        "snowflake.cli.app.commands_registration.command_plugins_loader.builtin_plugin_name_to_plugin_spec",
        {"test-plugin": plugin_spec},
    )


@_mock_test_plugin()
def test_experimental_invocation(runner):
    result_of_help = runner.invoke(["test", "hello", "--help"])
    assert "username" in result_of_help.output
    assert "--experimental" not in result_of_help.output
    assert "--format" in result_of_help.output

    result_of_execution = runner.invoke(
        ["test", "hello", "John", "--format", "JSON", "--experimental"]
    )
    assert result_of_execution.exit_code == 0
    assert json.loads(result_of_execution.output) == {
        "message": "Experimental: Hello John"
    }


@_mock_test_plugin()
def test_not_experimental_invocation(runner):
    result_of_help = runner.invoke(["test", "hello", "--help"])
    assert "username" in result_of_help.output
    assert "--experimental" not in result_of_help.output
    assert "--format" in result_of_help.output

    result_of_execution = runner.invoke(["test", "hello", "John", "--format", "JSON"])
    assert result_of_execution.exit_code == 0
    assert json.loads(result_of_execution.output) == {"message": "Hello John"}
