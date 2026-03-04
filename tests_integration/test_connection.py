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
from textwrap import dedent
from unittest import mock

import pytest

from tests_common import IS_WINDOWS
from tests_integration.snowflake_connector import (
    setup_test_database,
    setup_test_schema,
    add_uuid_to_name,
    mock_single_env_var,
    SCHEMA_ENV_PARAMETER,
)


@pytest.mark.integration
def test_connection_test_simple(runner):
    result = runner.invoke_with_connection_json(["connection", "test"])
    assert result.exit_code == 0, result.output
    assert result.json["Connection name"] == "integration"
    assert result.json["Status"] == "OK"


@pytest.mark.integration
def test_connection_dashed_database(runner, snowflake_session):
    database = add_uuid_to_name("dashed-database")
    with setup_test_database(snowflake_session, database):
        result = runner.invoke_with_connection_json(["connection", "test"])
        assert result.exit_code == 0, result.output
        assert result.json["Database"] == database


@pytest.mark.integration
def test_connection_dashed_schema(
    runner, test_database, snowflake_session, snowflake_home
):
    schema = "dashed-schema-name"
    with setup_test_schema(snowflake_session, schema):
        result = runner.invoke_with_connection(["connection", "test", "--debug"])
        assert result.exit_code == 0, result.output
        assert f'use schema "{schema}"' in result.output


@pytest.mark.integration
def test_connection_not_existing_schema(
    runner, test_database, snowflake_session, snowflake_home
):
    schema = "schema_which_does_not_exist"
    with mock_single_env_var(SCHEMA_ENV_PARAMETER, value=schema):
        result = runner.invoke_with_connection(["connection", "test"])
        assert result.exit_code == 1, result.output
        assert (
            f'Could not use schema "{schema.upper()}". Object does not exist'
            in result.output
        )


@pytest.mark.parametrize("report_dir", [".", "custom/report/dir"])
@pytest.mark.integration
def test_connection_diagnostic_report(runner, report_dir, temporary_working_directory):
    from pathlib import Path

    command = ["connection", "test", "--enable-diag", "--diag-log-path", report_dir]
    result = runner.invoke_with_connection(command)
    assert result.exit_code == 0, result.output
    expected_report_location = Path(report_dir) / "SnowflakeConnectionTestReport.txt"
    assert expected_report_location.exists(), result.output


@pytest.mark.integration
@pytest.mark.skipif(IS_WINDOWS, reason="Unix-based permission system test")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.connector.connect")
def test_custom_config_does_not_fail_on_wide_default_permissions(
    mock_connect, _, runner, snowflake_home, tmp_path
):
    default_config = snowflake_home / "config.toml"
    default_connections = snowflake_home / "connections.toml"
    default_config.write_text("")
    default_connections.write_text(
        dedent(
            """
        [my_connection]
        account = "my_account"
        user = "my_user"
        password = "my_password"
        """
        )
    )
    default_config.chmod(0o777)
    default_connections.chmod(0o777)

    custom_config = tmp_path / "config600.toml"
    custom_config.write_text(
        dedent(
            """
        [connections.my_custom_connection]
        account = "my_custom_account"
        user = "my_custom_user"
        password = "my_custom_password"
        """
        )
    )
    custom_config.chmod(0o600)

    mock_connect.return_value = mock.MagicMock(
        host="test.snowflakecomputing.com",
        account="my_account",
        user="my_user",
        role=None,
        database=None,
        schema=None,
        warehouse=None,
    )

    result = runner.invoke(
        [
            "--config-file",
            custom_config,
            "connection",
            "test",
            "--connection",
            "my_connection",
        ]
    )

    assert result.exit_code == 0, result.output
    _, kwargs = mock_connect.call_args
    assert kwargs["unsafe_skip_file_permissions_check"] is True
