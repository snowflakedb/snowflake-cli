import json
from tests.testing_utils.fixtures import *

import typer

from snowcli.api.plugin.command import (
    CommandSpec,
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandType,
    plugin_hook_impl,
)
from snowcli.cli.common.decorators import with_experimental_behaviour, global_options
from snowcli.cli.common.experimental_behaviour import experimental_behaviour_enabled
from snowcli.output.decorators import with_output
from snowcli.output.types import MessageResult

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
        "snowcli.app.commands_registration.command_plugins_loader.builtin_plugin_name_to_plugin_spec",
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
