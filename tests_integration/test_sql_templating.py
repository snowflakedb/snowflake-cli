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

import pytest


@pytest.mark.integration
def test_sql_env_value_from_cli_param(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select '&{ctx.env.test}'", "--env", "test=value_from_cli"]
    )

    assert result.exit_code == 0
    assert result.json == [{"'VALUE_FROM_CLI'": "value_from_cli"}]


@pytest.mark.integration
def test_sql_env_value_from_cli_param_that_is_blank(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select '&{ctx.env.test}'", "--env", "test="]
    )

    assert result.exit_code == 0
    assert result.json == [{"''": ""}]


@pytest.mark.integration
def test_sql_undefined_env_causing_error(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select '&{ctx.env.test}'"]
    )

    assert result.exit_code == 1
    assert "SQL template rendering error" in result.output


@pytest.mark.integration
def test_sql_env_value_from_os_env(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select '&{ctx.env.test}'"], env={"test": "value_from_os_env"}
    )

    assert result.exit_code == 0
    assert result.json == [{"'VALUE_FROM_OS_ENV'": "value_from_os_env"}]


@pytest.mark.integration
def test_sql_env_value_from_cli_param_overriding_os_env(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select '&{ctx.env.test}'", "--env", "test=value_from_cli"],
        env={"test": "value_from_os_env"},
    )

    assert result.exit_code == 0
    assert result.json == [{"'VALUE_FROM_CLI'": "value_from_cli"}]


@pytest.mark.integration
def test_sql_env_value_from_cli_duplicate_arg(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            "select '&{ctx.env.Test}'",
            "--env",
            "Test=firstArg",
            "--env",
            "Test=secondArg",
        ]
    )

    assert result.exit_code == 0
    assert result.json == [{"'SECONDARG'": "secondArg"}]


@pytest.mark.integration
def test_sql_env_value_from_cli_multiple_args(runner, snowflake_session):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            "select '&{ctx.env.Test1}-&{ctx.env.Test2}'",
            "--env",
            "Test1=test1",
            "--env",
            "Test2=test2",
        ]
    )

    assert result.exit_code == 0
    assert result.json == [{"'TEST1-TEST2'": "test1-test2"}]
