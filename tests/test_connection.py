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

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock

import pytest
import tomlkit
from snowflake.cli.api.constants import ObjectType

from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Requires further refactor to work on Windows", allow_module_level=True)


def test_new_connection_can_be_added(
    runner, os_agnostic_snapshot, named_temporary_file
):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
                "--port",
                "8080",
            ],
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == os_agnostic_snapshot


def test_new_connection_can_be_added_as_default(runner, os_agnostic_snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "default-conn",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
                "--default",
            ],
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == os_agnostic_snapshot


def test_new_connection_with_jwt_auth(runner, os_agnostic_snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn2",
                "--username",
                "user2",
                "--account",
                "account1",
                "--authenticator",
                "SNOWFLAKE_JWT",
                "--private-key",
                "~/private_key",
            ],
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == os_agnostic_snapshot


def test_port_has_cannot_be_string(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--account",
                "account1",
                "--port",
                "portValue",
            ],
        )
    assert result.exit_code == 1, result.output
    assert "Value of port must be integer" in result.output


def test_port_has_cannot_be_float(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn1",
                "--username",
                "user1",
                "--account",
                "account1",
                "--port",
                "123.45",
            ],
        )
    assert result.exit_code == 1, result.output
    assert "Value of port must be integer" in result.output


def test_new_connection_add_prompt_handles_default_values(runner, os_agnostic_snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName",
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == os_agnostic_snapshot


def test_new_connection_add_prompt_handles_prompt_override(
    runner, os_agnostic_snapshot
):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName\ndbName",
        )
        content = tmp_file.read()
    assert result.exit_code == 0, result.output
    assert content == os_agnostic_snapshot


def test_fails_if_existing_connection(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        tmp_file.write(
            dedent(
                """\
        [connections]
        [connections.conn2]
        username = "foo"
        """
            )
        )
        tmp_file.flush()
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn2",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ],
        )
    assert result.exit_code == 1, result.output
    assert "Connection conn2 already exists  " in result.output


@mock.patch("snowflake.cli.plugins.connection.commands.get_default_connection_name")
def test_lists_connection_information(mock_get_default_conn_name, runner):
    mock_get_default_conn_name.return_value = "empty"
    result = runner.invoke(["connection", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {
            "connection_name": "full",
            "parameters": {
                "account": "dev_account",
                "database": "dev_database",
                "host": "dev_host",
                "port": 8000,
                "role": "dev_role",
                "schema": "dev_schema",
                "user": "dev_user",
                "warehouse": "dev_warehouse",
            },
            "is_default": False,
        },
        {
            "connection_name": "default",
            "parameters": {
                "database": "db_for_test",
                "password": "****",  # masked
                "role": "test_role",
                "schema": "test_public",
                "warehouse": "xs",
            },
            "is_default": False,
        },
        {"connection_name": "empty", "parameters": {}, "is_default": True},
        {
            "connection_name": "test_connections",
            "parameters": {"user": "python"},
            "is_default": False,
        },
    ]


@mock.patch.dict(
    os.environ,
    {
        # connection not existing in config.toml but with a name starting with connection from config.toml ("empty")
        "SNOWFLAKE_CONNECTIONS_EMPTYABC_PASSWORD": "abc123",
        # connection existing in config.toml but key not used by CLI
        "SNOWFLAKE_CONNECTIONS_EMPTY_PW": "abc123",
    },
    clear=True,
)
@mock.patch("snowflake.cli.plugins.connection.commands.get_default_connection_name")
def test_connection_list_does_not_print_too_many_env_variables(
    mock_get_default_conn_name, runner
):
    mock_get_default_conn_name.return_value = "empty"
    result = runner.invoke(["connection", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {
            "connection_name": "full",
            "parameters": {
                "account": "dev_account",
                "database": "dev_database",
                "host": "dev_host",
                "port": 8000,
                "role": "dev_role",
                "schema": "dev_schema",
                "user": "dev_user",
                "warehouse": "dev_warehouse",
            },
            "is_default": False,
        },
        {
            "connection_name": "default",
            "parameters": {
                "database": "db_for_test",
                "password": "****",  # masked
                "role": "test_role",
                "schema": "test_public",
                "warehouse": "xs",
            },
            "is_default": False,
        },
        {"connection_name": "empty", "parameters": {}, "is_default": True},
        {
            "connection_name": "test_connections",
            "parameters": {"user": "python"},
            "is_default": False,
        },
    ]


def test_second_connection_not_update_default_connection(runner, os_agnostic_snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        tmp_file.write(
            dedent(
                """\
        [connections]
        [connections.conn]
        username = "foo"
        
        [options]
        default_connection = "conn"
        """
            )
        )
        tmp_file.flush()
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "conn2",
                "--username",
                "user1",
                "--password",
                "password1",
                "--account",
                "account1",
            ],
        )
        tmp_file.seek(0)
        content = tmp_file.read()

        assert result.exit_code == 0, result.output
        assert content == os_agnostic_snapshot


@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_connection_test(mock_connect, mock_om, runner):
    result = runner.invoke(
        ["connection", "test", "-c", "full", "--diag-log-path", "/tmp"]
    )
    assert result.exit_code == 0, result.output
    assert "Host" in result.output
    assert "Password" not in result.output
    assert "password" not in result.output

    mock_connect.assert_called_with(
        temporary_connection=False,
        mfa_passcode=None,
        enable_diag=False,
        diag_log_path="/tmp",
        diag_allowlist_path=None,
        connection_name="full",
        account=None,
        user=None,
        password=None,
        authenticator=None,
        private_key_path=None,
        token_file_path=None,
        session_token=None,
        master_token=None,
        database=None,
        schema=None,
        role=None,
        warehouse=None,
    )

    conn = mock_connect.return_value
    assert mock_om.return_value.use.mock_calls == [
        mock.call(object_type=ObjectType.ROLE, name=f'"{conn.role}"'),
        mock.call(object_type=ObjectType.DATABASE, name=f'"{conn.database}"'),
        mock.call(object_type=ObjectType.SCHEMA, name=f'"{conn.schema}"'),
        mock.call(object_type=ObjectType.WAREHOUSE, name=f'"{conn.warehouse}"'),
    ]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("option", ["--temporary-connection", "-x"])
def test_temporary_connection(mock_connector, mock_ctx, option, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        [
            "object",
            "list",
            "warehouse",
            option,
            "--account",
            "test_account",
            "--user",
            "snowcli_test",
            "--password",
            "top_secret",
            "--warehouse",
            "xsmall",
            "--database",
            "test_dv",
            "--schema",
            "PUBLIC",
        ],
    )

    assert result.exit_code == 0
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        account="test_account",
        user="snowcli_test",
        password="top_secret",
        database="test_dv",
        schema="PUBLIC",
        warehouse="xsmall",
        application_name="snowcli",
    )


@mock.patch.dict(
    os.environ,
    {
        "PRIVATE_KEY_PASSPHRASE": "password",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
def test_key_pair_authentication(mock_connector, mock_ctx, runner):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    ctx = mock_ctx()
    mock_connector.return_value = ctx

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    encrypted_pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            os.getenv("PRIVATE_KEY_PASSPHRASE").encode("utf-8")
        ),
    )

    private_key = serialization.load_pem_private_key(
        encrypted_pem_private_key,
        str.encode(os.getenv("PRIVATE_KEY_PASSPHRASE")),
        default_backend(),
    )

    private_key = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    with NamedTemporaryFile("w+", suffix=".p8") as tmp_file:
        tmp_file.write(
            dedent("\n".join(encrypted_pem_private_key.decode().splitlines()))
        )
        tmp_file.flush()
        result = runner.invoke(
            [
                "object",
                "list",
                "warehouse",
                "--temporary-connection",
                "--account",
                "test_account",
                "--user",
                "snowcli_test",
                "--authenticator",
                "SNOWFLAKE_JWT",
                "--private-key-path",
                tmp_file.name,
                "--warehouse",
                "xsmall",
                "--database",
                "test_dv",
                "--schema",
                "PUBLIC",
            ]
        )

    assert result.exit_code == 0
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        private_key=private_key,
        account="test_account",
        user="snowcli_test",
        authenticator="SNOWFLAKE_JWT",
        database="test_dv",
        schema="PUBLIC",
        warehouse="xsmall",
        application_name="snowcli",
    )


@mock.patch("snowflake.connector.connect")
def test_session_and_master_tokens(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    session_token = "dummy-session-token"
    master_token = "dummy-master-token"
    result = runner.invoke(
        [
            "object",
            "list",
            "warehouse",
            "--temporary-connection",
            "--account",
            "test_account",
            "--user",
            "snowcli_test",
            "--authenticator",
            "SNOWFLAKE_JWT",
            "--session-token",
            session_token,
            "--master-token",
            master_token,
            "--warehouse",
            "xsmall",
            "--database",
            "test_dv",
            "--schema",
            "PUBLIC",
        ]
    )

    assert result.exit_code == 0
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        session_token=session_token,
        master_token=master_token,
        account="test_account",
        user="snowcli_test",
        authenticator="SNOWFLAKE_JWT",
        database="test_dv",
        schema="PUBLIC",
        warehouse="xsmall",
        server_session_keep_alive=True,
        application_name="snowcli",
    )


@mock.patch("snowflake.connector.connect")
def test_token_file_path_tokens(mock_connector, mock_ctx, runner, temp_dir):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    token_file = Path(temp_dir) / "token.file"
    token_file.touch()

    result = runner.invoke(
        [
            "object",
            "list",
            "warehouse",
            "--temporary-connection",
            "--token-file-path",
            token_file,
        ]
    )

    assert result.exit_code == 0
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        token_file_path=str(token_file),
        application_name="snowcli",
    )


@mock.patch.dict(
    os.environ,
    {
        "PRIVATE_KEY_PASSPHRASE": "password",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.app.snow_connector._load_pem_to_der")
def test_key_pair_authentication_from_config(
    mock_load, mock_connector, mock_ctx, temp_dir, runner
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_load.return_value = "secret value"

    with NamedTemporaryFile("w+", suffix="toml") as tmp_file:
        tmp_file.write(
            dedent(
                """
               [connections.jwt]
               account = "my_account"
               user = "jdoe"
               authenticator = "SNOWFLAKE_JWT"
               private_key_path = "~/sf_private_key.p8"
            """
            )
        )
        tmp_file.flush()

        result = runner.invoke_with_config_file(
            tmp_file.name,
            ["object", "list", "warehouse", "-c", "jwt"],
        )

    assert result.exit_code == 0, result.output
    mock_load.assert_called_once_with("~/sf_private_key.p8")
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        account="my_account",
        user="jdoe",
        authenticator="SNOWFLAKE_JWT",
        private_key="secret value",
        application_name="snowcli",
    )


@pytest.mark.parametrize(
    "command",
    [
        ["sql", "-q", "select 1"],
        ["connection", "test"],
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
def test_mfa_passcode(_, mock_connect, runner, command):
    command.extend(["--mfa-passcode", "123"])
    result = runner.invoke(command)

    assert result.exit_code == 0, result.output
    args, kwargs = mock_connect.call_args
    assert kwargs["passcode"] == "123"


def test_if_password_callback_is_called_only_once_from_prompt(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName\npassword",
        )

    assert result.exit_code == 0
    assert result.output.count("WARNING!") == 0


def test_if_password_callback_is_called_only_once_from_arguments(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "-n",
                "test_conn",
                "-a",
                "test_conn",
                "-u",
                "test_conn",
                "-p",
                "test_conn",
            ],
        )

    assert result.exit_code == 0
    assert result.output.count("WARNING!") == 1


@pytest.mark.parametrize(
    "command",
    [
        ["sql", "-q", "select 1"],
        ["connection", "test"],
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
def test_mfa_passcode_from_prompt(_, mock_connect, runner, command):
    command.append("--mfa-passcode")
    result = runner.invoke(command, input="123")

    assert result.exit_code == 0, result.output
    args, kwargs = mock_connect.call_args
    assert kwargs["passcode"] == "123"


@mock.patch("snowflake.connector.connect")
def test_no_mfa_passcode(mock_connect, runner):
    result = runner.invoke(["sql", "-q", "select 1"])

    assert result.exit_code == 0, result.output
    args, kwargs = mock_connect.call_args
    assert kwargs.get("passcode") is None


@mock.patch("snowflake.connector.connect")
def test_mfa_cache(mock_connect, runner):
    result = runner.invoke(
        ["sql", "-q", "select 1", "--authenticator", "username_password_mfa"]
    )

    assert result.exit_code == 0, result.output
    args, kwargs = mock_connect.call_args
    assert kwargs["authenticator"] == "username_password_mfa"
    assert kwargs["client_request_mfa_token"]


@pytest.mark.parametrize(
    "env",
    [
        {
            "SNOWFLAKE_CONNECTIONS_EMPTY_ACCOUNT": "some_account",
            "SNOWFLAKE_CONNECTIONS_EMPTY_DATABASE": "test_database",
            "SNOWFLAKE_CONNECTIONS_EMPTY_WAREHOUSE": "large",
            "SNOWFLAKE_CONNECTIONS_EMPTY_ROLE": "role",
            "SNOWFLAKE_CONNECTIONS_EMPTY_SCHEMA": "my_schema",
            "SNOWFLAKE_CONNECTIONS_EMPTY_PASSWORD": "dummy",
        },
        {
            "SNOWFLAKE_ACCOUNT": "some_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "large",
            "SNOWFLAKE_ROLE": "role",
            "SNOWFLAKE_SCHEMA": "my_schema",
            "SNOWFLAKE_PASSWORD": "dummy",
        },
    ],
)
@mock.patch("snowflake.connector.connect")
def test_connection_details_are_resolved_using_environment_variables(
    mock_connect, env, test_snowcli_config, runner
):
    with mock.patch.dict(os.environ, env, clear=True):

        result = runner.invoke(["sql", "-q", "select 1", "-c", "empty"])

        assert result.exit_code == 0, result.output
        _, kwargs = mock_connect.call_args
        assert kwargs == {
            "account": "some_account",
            "application": "SNOWCLI.SQL",
            "database": "test_database",
            "warehouse": "large",
            "schema": "my_schema",
            "role": "role",
            "password": "dummy",
            "application_name": "snowcli",
        }


@pytest.mark.parametrize(
    "env",
    [
        {
            "SNOWFLAKE_CONNECTIONS_EMPTY_ACCOUNT": "some_account",
            "SNOWFLAKE_CONNECTIONS_EMPTY_DATABASE": "test_database",
            "SNOWFLAKE_CONNECTIONS_EMPTY_WAREHOUSE": "large",
            "SNOWFLAKE_CONNECTIONS_EMPTY_ROLE": "role",
            "SNOWFLAKE_CONNECTIONS_EMPTY_SCHEMA": "my_schema",
        },
        {
            "SNOWFLAKE_ACCOUNT": "some_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "large",
            "SNOWFLAKE_ROLE": "role",
            "SNOWFLAKE_SCHEMA": "my_schema",
        },
    ],
)
@mock.patch("snowflake.connector.connect")
def test_flags_take_precedence_before_environment_variables(
    mock_connect, env, test_snowcli_config, runner
):
    with mock.patch.dict(os.environ, env, clear=True):

        result = runner.invoke(
            [
                "sql",
                "-q",
                "select 1",
                "-c",
                "empty",
                "--account",
                "account_from_flag",
                "--database",
                "database_from_flag",
                "--schema",
                "schema_from_flag",
                "--password",
                "password_from_flag",
                "--role",
                "role_from_flag",
            ]
        )

        assert result.exit_code == 0, result.output
        _, kwargs = mock_connect.call_args
        assert kwargs == {
            "account": "account_from_flag",
            "application": "SNOWCLI.SQL",
            "database": "database_from_flag",
            "warehouse": "large",
            "schema": "schema_from_flag",
            "password": "password_from_flag",
            "role": "role_from_flag",
            "application_name": "snowcli",
        }


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_TEST_CONNECTIONS_ACCOUNT": "account_from_connection_env",
        "SNOWFLAKE_ACCOUNT": "account_from_global_env",
        "SNOWFLAKE_CONNECTIONS_TEST_CONNECTIONS_DATABASE": "database_from_connection_env",
        "SNOWFLAKE_DATABASE": "database_from_global_env",
        "SNOWFLAKE_ROLE": "role_from_global_env",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
def test_source_precedence(mock_connect, runner):
    result = runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "-c",
            "test_connections",
            "--account",
            "account_from_flag",
        ]
    )

    assert result.exit_code == 0, result.output
    _, kwargs = mock_connect.call_args
    assert kwargs == {
        "user": "python",  # from config
        "account": "account_from_flag",
        "application": "SNOWCLI.SQL",
        "database": "database_from_connection_env",
        "role": "role_from_global_env",
        "application_name": "snowcli",
    }


def test_set_default_connection_fails_if_no_connection(runner):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name, ["connection", "set-default", "foo"]
        )

    assert result.exit_code == 1
    assert "Connection foo is not configured" in result.output


def test_set_default_connection(runner):
    def _change_connection(config_file, conn_name):
        result = runner.invoke_with_config_file(
            config_file.name, ["connection", "set-default", conn_name]
        )
        assert result.exit_code == 0, result.output
        return tomlkit.loads(Path(tmp_file.name).read_text()).value

    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        tmp_file.write("[connections.conn1]\n[connections.conn2]")
        tmp_file.flush()

        config = _change_connection(tmp_file, "conn1")
        assert config["default_connection_name"] == "conn1"

        config = _change_connection(tmp_file, "conn2")
        assert config["default_connection_name"] == "conn2"


@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_connection_test_diag_report(mock_connect, mock_om, runner):
    result = runner.invoke(
        ["connection", "test", "-c", "full", "--enable-diag", "--diag-log-path", "/tmp"]
    )
    assert result.exit_code == 0, result.output
    print(result.output)
    assert "Host" in result.output
    assert "Diag Report" in result.output
    mock_connect.assert_called_once_with(
        temporary_connection=False,
        mfa_passcode=None,
        enable_diag=True,
        diag_log_path="/tmp",
        diag_allowlist_path=None,
        connection_name="full",
        account=None,
        user=None,
        password=None,
        authenticator=None,
        private_key_path=None,
        token_file_path=None,
        session_token=None,
        master_token=None,
        database=None,
        schema=None,
        role=None,
        warehouse=None,
    )
