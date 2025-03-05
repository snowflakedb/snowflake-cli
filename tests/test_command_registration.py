# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

from snowflake.cli._plugins.connection import plugin_spec as connection_plugin_spec
from snowflake.cli._plugins.streamlit import plugin_spec as streamlit_plugin_spec
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandPath,
    CommandSpec,
    CommandType,
)
from typer import Typer


def test_builtin_plugins_registration(runner):
    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1
    assert result.output.count("Executes Snowflake query") == 1


def test_multiple_use_of_test_runner(runner):
    def assert_result_is_correct(result):
        assert result.exit_code == 0
        assert result.output.count("Manages connections to Snowflake") == 1
        assert result.output.count("Manages a Streamlit app in Snowflake") == 1
        assert result.output.count("Manages Snowflake objects") == 1
        assert result.output.count("Executes Snowflake query") == 1

    assert_result_is_correct(runner.invoke(["-h"]))
    assert_result_is_correct(runner.invoke(["-h"]))


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
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


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
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
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
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
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch(
    "snowflake.cli._app.commands_registration.command_plugins_loader.get_builtin_plugin_name_to_plugin_spec",
    lambda: {
        "connection": connection_plugin_spec,
        "connection2": connection_plugin_spec,
    },
)
def test_duplicated_plugin_handling(runner):
    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages Streamlit in Snowflake") == 0


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
def test_conflicting_command_plugin_paths_handling(
    connection_command_spec_mock, runner
):
    connection_command_spec_mock.return_value = streamlit_plugin_spec.command_spec()

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch("snowflake.cli._plugins.streamlit.plugin_spec.command_spec")
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


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
def test_not_existing_command_group_handling(
    connection_command_spec_mock, runner, caplog
):
    connection_command_spec_mock.return_value = CommandSpec(
        parent_command_path=CommandPath(["xyz123"]),
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=Typer(name="list", callback=lambda: None),
    )

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert (
        "Cannot register plugin [connection]: Invalid command path [snow xyz123 list]. Command group [xyz123] does not exist."
        in caplog.messages
    )
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
def test_broken_command_spec_handling(connection_command_spec_mock, runner):
    connection_command_spec_mock.side_effect = RuntimeError("Test exception")

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 0
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch(
    "snowflake.cli.api.plugins.plugin_config.PluginConfigProvider.get_enabled_plugin_names"
)
def test_not_existing_external_entrypoint_handling(enabled_plugin_names_mock, runner):
    enabled_plugin_names_mock.return_value = ["xyz123"]

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1


@mock.patch("pluggy.PluginManager.load_setuptools_entrypoints")
@mock.patch(
    "snowflake.cli.api.plugins.plugin_config.PluginConfigProvider.get_enabled_plugin_names"
)
def test_broken_external_entrypoint_handling(
    enabled_plugin_names_mock, load_setuptools_entrypoints_mock, runner
):
    enabled_plugin_names_mock.return_value = ["xyz123"]
    load_setuptools_entrypoints_mock.side_effect = RuntimeError("Test exception")

    result = runner.invoke(["-h"])
    assert result.exit_code == 0
    assert result.output.count("Manages connections to Snowflake") == 1
    assert result.output.count("Manages a Streamlit app in Snowflake") == 1
