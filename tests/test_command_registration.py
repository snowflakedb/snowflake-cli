from typer import Typer

from snowcli.api.plugin.command import (
    CommandSpec,
    CommandPath,
    CommandType,
    SNOWCLI_ROOT_COMMAND_PATH,
)
from snowcli.cli.connection import plugin_spec as connection_plugin_spec
from snowcli.cli.streamlit import plugin_spec as streamlit_plugin_spec
from tests.testing_utils.fixtures import *


def test_builtin_plugins_registration(runner):
    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages Streamlit in Snowflake") == 1


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_auto_empty_callback_for_new_groups_with_single_command(
    connection_command_spec_mock, runner
):
    typer_instance = Typer(name="connection", help="Manages connections to Snowflake")
    typer_instance.command("test-command", help="Test command help")(lambda: None)
    connection_command_spec_mock.return_value = CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=typer_instance,
    )

    result1 = runner.invoke(["-h"])
    assert result1.exit_code == 0
    assert result1.output.count("Manages connections to Snowflake") == 1

    result2 = runner.invoke(["connection", "-h"])
    assert result2.exit_code == 0
    assert result2.output.count("test-command") == 1
    assert result2.output.count("Test command help") == 1

    result3 = runner.invoke(["connection", "test-command", "-h"])
    assert result3.exit_code == 0
    assert result3.output.count("Test command help") == 1


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_exception_handling_if_single_command_has_callback(
    connection_command_spec_mock, runner
):
    typer_instance = Typer(
        name="connection",
        help="Manages connections to Snowflake",
        callback=lambda: None,
    )
    typer_instance.command("test-command", help="Test command help")(lambda: None)
    connection_command_spec_mock.return_value = CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=typer_instance,
    )

    result = runner.invoke(["-h"])
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages Streamlit in Snowflake") == 1


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_exception_handling_if_single_command_has_multiple_commands(
    connection_command_spec_mock, runner
):
    typer_instance = Typer(name="connection", help="Manages connections to Snowflake")
    typer_instance.command("test-command1", help="Test command1 help")(lambda: None)
    typer_instance.command("test-command2", help="Test command2 help")(lambda: None)
    connection_command_spec_mock.return_value = CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.SINGLE_COMMAND,
        typer_instance=typer_instance,
    )

    result = runner.invoke(["-h"])
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages Streamlit in Snowflake") == 1


@mock.patch(
    "snowcli.app.commands_registration.command_plugins_loader.builtin_plugin_name_to_plugin_spec",
    {
        "connection": connection_plugin_spec,
        "connection2": connection_plugin_spec,
    },
)
def test_duplicated_plugin_handling(runner):
    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages Streamlit in Snowflake") == 0


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_conflicting_command_plugin_paths_handling(
    connection_command_spec_mock, runner
):
    connection_command_spec_mock.return_value = streamlit_plugin_spec.command_spec()

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages Streamlit in Snowflake") == 1


@mock.patch("snowcli.cli.streamlit.plugin_spec.command_spec")
def test_conflicting_commands_handling(streamlit_command_spec_mock, runner):
    streamlit_command_spec_mock.return_value = CommandSpec(
        parent_command_path=CommandPath(["connection"]),
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=Typer(name="list", callback=lambda: None),
    )

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1

    result2 = runner.invoke(["connection", "-h"])
    assert result2.exit_code == 0
    assert result2.output.count("Lists configured connections") == 1

    result3 = runner.invoke(["connection", "list", "-h"])
    assert result3.exit_code == 0
    assert result3.output.count("Lists configured connections") == 1


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_not_existing_command_group_handling(connection_command_spec_mock, runner):
    connection_command_spec_mock.return_value = CommandSpec(
        parent_command_path=CommandPath(["xyz123"]),
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=Typer(name="list", callback=lambda: None),
    )

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("xyz123") == 0
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages Streamlit in Snowflake") == 1


@mock.patch("snowcli.cli.connection.plugin_spec.command_spec")
def test_broken_command_spec_handling(connection_command_spec_mock, runner):
    connection_command_spec_mock.side_effect = RuntimeError("Test exception")

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages Streamlit in Snowflake") == 1
