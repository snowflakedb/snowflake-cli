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

import os
from unittest import mock

import pytest


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "show": "warehouseValue",
}


@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("snow sql", "SNOWCLI.SQL"),
        ("snow show warehouses", "SNOWCLI.SHOW.WAREHOUSES"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.app.snow_connector.command_info")
def test_command_context_is_passed_to_snowflake_connection(
    mock_command_info, mock_connect, cmd, expected, test_snowcli_config
):
    from snowflake.cli.api.config import config_init
    from snowflake.cli.app.snow_connector import connect_to_snowflake

    config_init(test_snowcli_config)

    mock_ctx = mock.Mock()
    mock_ctx.command_path = cmd
    mock_command_info.return_value = expected

    connect_to_snowflake()

    mock_connect.assert_called_once_with(
        application=expected,
        database="db_for_test",
        schema="test_public",
        role="test_role",
        warehouse="xs",
        password="dummy_password",
        application_name="snowcli",
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_connectivity_error(runner):
    result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 1
    assert "Invalid connection configuration" in result.output
    assert "User is empty" in result.output


@mock.patch("snowflake.connector")
def test_no_output_from_connection(mock_connect, runner):
    funny_text = "what's taters, my precious?"

    def _mock(*args, **kwargs):
        print(funny_text)
        return mock.MagicMock()

    mock_connect.connect = _mock

    result = runner.invoke(["sql", "-q", "select 1"])
    assert funny_text not in result.output


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_master_token_without_temporary_connection(
    runner,
):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--master-token", "dummy-master-token"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session or master token, you must use a temporary connection"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_session_token_without_temporary_connection(
    runner,
):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--session-token", "dummy-session-token"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session or master token, you must use a temporary connection"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_missing_session_token(runner):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--master-token", "dummy-master-token", "-x"]
    )
    assert result.exit_code == 1
    assert (
        "When using a master token, you must provide the corresponding session token"
        in result.output
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_missing_master_token(runner):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--session-token", "dummy-session-token", "-x"]
    )
    assert result.exit_code == 1
    assert (
        "When using a session token, you must provide the corresponding master token"
        in result.output
    )
