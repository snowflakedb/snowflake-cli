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
from snowflake.cli.api.config import ConnectionConfig
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_utils import file_permissions_are_strict

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


@mock.patch("os.path.exists")
@mock.patch("snowflake.cli._plugins.connection.commands.Path")
def test_new_connection_with_jwt_auth(mock_path, mock_os, runner, os_agnostic_snapshot):
    mock_os.return_value = True
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


def test_if_whitespaces_are_stripped_from_connection_name(runner, os_agnostic_snapshot):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
                "--connection-name",
                "       whitespaceTest     ",
                "--username",
                "userName            ",
                "--account",
                "             accName",
            ],
            input="123\n some role    \n some warehouse\n foo \n bar     \n baz \n    12345 \n Kaszuby \n foo   \n \n",
        )
        content = tmp_file.read()

        assert result.exit_code == 0, result.output
        assert content == os_agnostic_snapshot

        connections_list = runner.invoke_with_config_file(
            tmp_file.name, ["connection", "list", "--format", "json"]
        )
        assert connections_list.exit_code == 0
        assert connections_list.output == os_agnostic_snapshot

        set_as_default = runner.invoke_with_config_file(
            tmp_file.name, ["connection", "set-default", "whitespaceTest"]
        )
        assert set_as_default.exit_code == 0
        assert "Default connection set to: whitespaceTest" in set_as_default.output


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
    assert result.exit_code == 2, result.output
    assert "'portValue' is not a valid integer" in result.output


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
    assert result.exit_code == 2, result.output
    assert "'123.45' is not a valid integer. " in result.output


@pytest.mark.parametrize(
    "selected_option",
    [9, 10],  # 9 - private_key_file prompt, 10 - token_file_path prompt
)
def test_file_paths_have_to_exist_when_given_in_prompt(selected_option, runner):
    result = _run_connection_add_with_path_provided_as_prompt(
        "~/path/to/file", selected_option, runner
    )

    assert result.exit_code == 2, result.output
    assert "Path ~/path/to/file does not exist." in result.output


@pytest.mark.parametrize(
    "selected_option", [9, 10]
)  # 9 - private_key_file prompt, 10 - token_file_path prompt
def test_connection_can_be_added_with_existing_paths_in_prompt(selected_option, runner):
    with NamedTemporaryFile("w+") as tmp_path:
        result = _run_connection_add_with_path_provided_as_prompt(
            tmp_path.name, selected_option, runner
        )
    assert result.exit_code == 0, result.output
    assert "Wrote new connection connName to" in result.output


@pytest.mark.parametrize("selected_option", ["-k", "-t"])
def test_file_paths_have_to_exist_when_given_in_arguments(selected_option, runner):
    result = _run_connection_add_with_path_provided_as_argument(
        "~/path/to/file", selected_option, runner
    )
    assert result.exit_code == 2, result.output
    assert "Path ~/path/to/file does not exist." in result.output


@pytest.mark.parametrize("selected_option", ["-k", "-t"])
def test_connection_can_be_added_with_existing_paths_in_arguments(
    selected_option, runner
):
    with NamedTemporaryFile("w+") as tmp_path:
        result = _run_connection_add_with_path_provided_as_argument(
            tmp_path.name, selected_option, runner
        )
    assert result.exit_code == 0, result.output
    assert "Wrote new connection conn1 to" in result.output


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
    assert result.exit_code == 2, result.output
    assert "Connection conn2 already exists  " in result.output


@mock.patch("snowflake.cli._plugins.connection.commands.get_default_connection_name")
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
        {
            "connection_name": "private_key_file",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": "/private/key",
            },
        },
        {
            "connection_name": "private_key_path",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
            },
        },
        {
            "connection_name": "no_private_key",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
            },
        },
        {
            "connection_name": "jwt",
            "is_default": False,
            "parameters": {
                "account": "testing_account",
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": "/private/key",
                "user": "jdoe",
            },
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
@mock.patch("snowflake.cli._plugins.connection.commands.get_default_connection_name")
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
        {
            "connection_name": "private_key_file",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": "/private/key",
            },
        },
        {
            "connection_name": "private_key_path",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
            },
        },
        {
            "connection_name": "no_private_key",
            "is_default": False,
            "parameters": {
                "authenticator": "SNOWFLAKE_JWT",
            },
        },
        {
            "connection_name": "jwt",
            "is_default": False,
            "parameters": {
                "account": "testing_account",
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": "/private/key",
                "user": "jdoe",
            },
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


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
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
        enable_diag=False,
        diag_log_path=Path("/tmp"),
        connection_name="full",
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
            "--host",
            "snowcli_test_host",
            "--port",
            "123456789",
        ],
    )

    assert result.exit_code == 0
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        host="snowcli_test_host",
        port=123456789,
        account="test_account",
        user="snowcli_test",
        password="top_secret",
        database="test_dv",
        schema="PUBLIC",
        warehouse="xsmall",
        application_name="snowcli",
        using_session_keep_alive=True,
    )


@mock.patch.dict(
    os.environ,
    {
        "PRIVATE_KEY_PASSPHRASE": "password",
    },
    clear=True,
)
@pytest.mark.parametrize(
    "private_key_flag_name", ["--private-key-file", "--private-key-path"]
)
@mock.patch("snowflake.connector.connect")
def test_key_pair_authentication(
    mock_connector, mock_ctx, runner, private_key_flag_name
):
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
                private_key_flag_name,
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
        using_session_keep_alive=True,
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
        using_session_keep_alive=True,
    )


@mock.patch("snowflake.connector.connect")
def test_token_file_path_tokens(mock_connector, mock_ctx, runner, temporary_directory):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    token_file = Path(temporary_directory) / "token.file"
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
        using_session_keep_alive=True,
    )


@mock.patch.dict(
    os.environ,
    {
        "PRIVATE_KEY_PASSPHRASE": "password",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector._load_pem_from_file")
@mock.patch("snowflake.cli._app.snow_connector._load_pem_to_der")
def test_key_pair_authentication_from_config(
    mock_convert, mock_load_file, mock_connector, mock_ctx, temporary_directory, runner
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_convert.return_value = SecretType("secret value")

    with NamedTemporaryFile("w+", suffix="toml") as tmp_file:
        tmp_file.write(
            dedent(
                """
               [connections.jwt]
               account = "my_account"
               user = "jdoe"
               authenticator = "SNOWFLAKE_JWT"
               private_key_file = "~/sf_private_key.p8"
            """
            )
        )
        tmp_file.flush()

        result = runner.invoke_with_config_file(
            tmp_file.name,
            ["object", "list", "warehouse", "-c", "jwt"],
        )

    assert result.exit_code == 0, result.output
    mock_load_file.assert_called_once_with("~/sf_private_key.p8")
    mock_connector.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
        account="my_account",
        user="jdoe",
        authenticator="SNOWFLAKE_JWT",
        private_key="secret value",
        application_name="snowcli",
        using_session_keep_alive=True,
    )


@pytest.mark.parametrize(
    "command",
    [
        ["sql", "-q", "select 1"],
        ["connection", "test"],
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
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
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
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
            "using_session_keep_alive": True,
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
            "using_session_keep_alive": True,
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
        "using_session_keep_alive": True,
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


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
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
        enable_diag=True,
        diag_log_path=Path("/tmp"),
        connection_name="full",
    )


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
def test_diag_log_path_default_is_actual_tempdir(mock_connect, mock_om, runner):
    from snowflake.cli.api.commands.flags import _DIAG_LOG_DEFAULT_VALUE

    result = runner.invoke(["connection", "test", "-c", "full", "--enable-diag"])
    assert result.exit_code == 0, result.output
    assert mock_connect.call_args.kwargs["diag_log_path"] not in [
        _DIAG_LOG_DEFAULT_VALUE,
        Path(_DIAG_LOG_DEFAULT_VALUE),
    ]


def _run_connection_add_with_path_provided_as_argument(
    path: str, selected_option: str, runner
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
                "--account",
                "account1",
                "--port",
                "12378",
                selected_option,
                path,
            ],
        )
    return result


def _run_connection_add_with_path_provided_as_prompt(
    path: str, selected_option: int, runner
):
    with NamedTemporaryFile("w+", suffix=".toml") as tmp_file:
        result = runner.invoke_with_config_file(
            tmp_file.name,
            [
                "connection",
                "add",
            ],
            input="connName\naccName\nuserName\npassword{}{}".format(
                selected_option * "\n", path
            ),
        )

    return result


def test_new_connection_is_added_to_connections_toml(
    runner, os_agnostic_snapshot, named_temporary_file, snowflake_home
):
    connections_toml = Path(snowflake_home) / "connections.toml"
    connections_toml.touch()
    connections_toml.write_text(
        dedent(
            """
        [a]
        account = "A"
        
        [b]
        account = "B"
        """
        )
    )

    result = runner.super_invoke(
        [
            "connection",
            "add",
            "--connection-name",
            "new_one",
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

    assert result.exit_code == 0, result.output
    assert f"Wrote new connection new_one to {connections_toml}" in result.output

    assert connections_toml.read_text() == dedent(
        """\
        [a]
        account = "A"
        
        [b]
        account = "B"
        
        [new_one]
        account = "account1"
        user = "user1"
        password = "password1"
        port = 8080
    """
    )


@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
@mock.patch.dict(os.environ, {}, clear=True)
def test_generate_jwt_without_passphrase(
    mocked_get_token, runner, named_temporary_file
):
    mocked_get_token.return_value = "funny token"

    with named_temporary_file() as f:
        f.write_text("secret from file")
        result = runner.invoke(
            [
                "connection",
                "generate-jwt",
                "--user",
                "FooBar",
                "--account",
                "account1",
                "--private-key-path",
                f,
            ],
        )

    assert result.exit_code == 0, result.output
    assert result.output == "funny token\n"
    mocked_get_token.assert_called_once_with(
        user="FooBar", account="account1", privatekey_path=str(f), key_password=None
    )


@pytest.mark.parametrize("passphrase", ["", "pass123"])
@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
@mock.patch.dict(os.environ, {}, clear=True)
def test_generate_jwt_with_passphrase(
    mocked_get_token, runner, named_temporary_file, passphrase
):
    mocked_get_token.side_effect = [TypeError("foo"), "funny token"]

    with named_temporary_file() as f:
        f.write_text("secret from file")
        result = runner.invoke(
            [
                "connection",
                "generate-jwt",
                "--user",
                "FooBar",
                "--account",
                "account1",
                "--private-key-path",
                f,
            ],
            input=passphrase,
        )

    assert result.exit_code == 0, result.output
    assert (
        result.output
        == "Enter private key file password (press enter for empty) []: \nfunny token\n"
    )
    mocked_get_token.assert_has_calls(
        [
            mock.call(
                user="FooBar",
                account="account1",
                privatekey_path=str(f),
                key_password=None,
            ),
            mock.call(
                user="FooBar",
                account="account1",
                privatekey_path=str(f),
                key_password=passphrase,
            ),
        ],
        any_order=True,
    )


@mock.patch.dict(os.environ, {"PRIVATE_KEY_PASSPHRASE": "123"}, clear=True)
@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
def test_generate_jwt_with_pass_phrase_from_env(
    mocked_get_token, runner, named_temporary_file
):
    mocked_get_token.return_value = "funny token"

    with named_temporary_file() as f:
        f.write_text("secret from file")
        result = runner.invoke(
            [
                "connection",
                "generate-jwt",
                "--user",
                "FooBar",
                "--account",
                "account1",
                "--private-key-path",
                f,
            ]
        )

    assert result.exit_code == 0, result.output
    assert result.output == "funny token\n"
    mocked_get_token.assert_called_once_with(
        user="FooBar", account="account1", privatekey_path=str(f), key_password="123"
    )


@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
@mock.patch.dict(os.environ, {}, clear=True)
def test_generate_jwt_uses_config(mocked_get_token, runner, named_temporary_file):
    mocked_get_token.return_value = "funny token"

    with named_temporary_file() as f:
        f.write_text("secret from file")
        result = runner.invoke(
            ["connection", "generate-jwt", "--connection", "jwt"],
        )

    assert result.exit_code == 0, result.output
    assert result.output == "funny token\n"
    mocked_get_token.assert_called_once_with(
        user="jdoe",
        account="testing_account",
        privatekey_path="/private/key",
        key_password=None,
    )


@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
@pytest.mark.parametrize(
    "cmd_line_params, expected",
    (
        pytest.param(
            ("--user", "jdoe2"),
            {"user": "jdoe2"},
            id="--user flag",
        ),
        pytest.param(
            ("--account", "account2"),
            {"account": "account2"},
            id="--account flag",
        ),
    ),
)
def test_generate_jwt_honors_params(
    mocked_get_token, runner, cmd_line_params, expected
):
    mocked_get_token.return_value = "funny token"

    result = runner.invoke(
        ["connection", "generate-jwt", "--connection", "jwt", *cmd_line_params],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "funny token\n"
    expected_params = {
        "user": "jdoe",
        "account": "testing_account",
        "privatekey_path": "/private/key",
        "key_password": None,
    } | expected
    mocked_get_token.assert_called_once_with(**expected_params)


@pytest.mark.parametrize("attribute", ["account", "user", "private_key_file"])
@mock.patch(
    "snowflake.cli._plugins.connection.commands.connector.auth.get_token_from_private_key"
)
def test_generate_jwt_raises_error_if_required_parameter_is_missing(
    mocked_get_token, attribute, runner, named_temporary_file
):
    connection_details = {
        "account": "account1",
        "user": "FooBar",
        "private_key_file": "/private/key",
    }
    del connection_details[attribute]
    data = tomlkit.dumps({"connections": {"jwt": connection_details}})

    with NamedTemporaryFile("w+", suffix="toml") as tmp_file:
        tmp_file.write(data)
        tmp_file.flush()

        result = runner.invoke_with_config_file(
            tmp_file.name,
            ["connection", "generate-jwt", "-c", "jwt"],
        )
        assert (
            f"{attribute.capitalize().replace('_', ' ')} is not set in the connection context"
            in result.output
        )


@mock.patch("snowflake.cli._plugins.connection.commands.add_connection_to_proper_file")
def test_connection_add_no_interactive(mock_add, runner):
    mock_add.return_value = "file_name"
    result = runner.invoke(
        [
            "connection",
            "add",
            "--connection-name",
            "conn1",
            "--username",
            "user1",
            "--account",
            "account1",
            "--no-interactive",
        ]
    )

    assert result.exit_code == 0
    # Assert no prompts in the output
    assert "Wrote new connection conn1 to file_name\n" == result.output

    mock_add.assert_called_once_with(
        "conn1",
        ConnectionConfig(
            account="account1",
            user="user1",
            host=None,
            region=None,
            port=None,
            database=None,
            schema=None,
            warehouse=None,
            role=None,
            authenticator=None,
            private_key_file=None,
            token_file_path=None,
            _other_settings={},
        ),
    )


@mock.patch("snowflake.cli._plugins.auth.keypair.manager.AuthManager.execute_query")
@mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
@mock.patch("snowflake.connector.connect")
def test_connection_add_with_key_pair(
    mock_connect,
    mock_object_execute_query,
    mock_auth_execute_query,
    runner,
    tmp_path,
    mock_cursor,
    test_snowcli_config,
    enable_auth_keypair_feature_flag,
):
    mock_connect.return_value.user = "user"
    mock_object_execute_query.return_value = mock_cursor(
        rows=[
            {"property": "RSA_PUBLIC_KEY", "value": None},
            {"property": "RSA_PUBLIC_KEY_2", "value": None},
        ],
        columns=[],
    )

    result = runner.invoke(
        [
            "connection",
            "add",
        ],
        input="conn\n"  # connection name: zz
        "test\n"  # account:
        "user\n"  # user:
        "123\n"  # password:
        "\n"  # role:
        "\n"  # warehouse:
        "\n"  # database:
        "\n"  # schema:
        "\n"  # host:
        "\n"  # port:
        "\n"  # region:
        "\n"  # authenticator:
        "\n"  # private key file:
        "\n"  # token file path:
        "y\n"  #
        "4096\n"  # key_length
        f"{tmp_path}\n"  # output_path
        "123\n",  # passphrase
    )

    private_key_path = tmp_path / "conn.p8"
    public_key_path = tmp_path / "conn.pub"
    assert result.exit_code == 0, result.output
    assert result.output == dedent(
        f"""\
        Enter connection name: conn
        Enter account: test
        Enter user: user
        Enter password: 
        Enter role: 
        Enter warehouse: 
        Enter database: 
        Enter schema: 
        Enter host: 
        Enter port: 
        Enter region: 
        Enter authenticator: 
        Enter private key file: 
        Enter token file path: 
        Do you want to configure key pair authentication? [y/N]: y
        Key length [2048]: 4096
        Output path [~/.ssh]: {tmp_path}
        Private key passphrase: 
        Set the `PRIVATE_KEY_PASSPHRASE` environment variable before using the connection.
        Wrote new connection conn to {test_snowcli_config}
        """
    )
    assert private_key_path.exists()
    assert "BEGIN ENCRYPTED PRIVATE KEY" in private_key_path.read_text()
    assert file_permissions_are_strict(private_key_path)
    assert public_key_path.exists()
    assert file_permissions_are_strict(public_key_path)


def test_connection_add_no_key_pair_setup_if_private_key_provided(
    runner, test_snowcli_config, tmp_path
):
    key = tmp_path / "key.p8"
    key.touch()

    result = runner.invoke(
        [
            "connection",
            "add",
        ],
        input="conn\n"  # connection name: zz
        "test\n"  # account:
        "user\n"  # user:
        "\n"  # password:
        "\n"  # role:
        "\n"  # warehouse:
        "\n"  # database:
        "\n"  # schema:
        "\n"  # host:
        "\n"  # port:
        "\n"  # region:
        "\n"  # authenticator:
        f"{key}\n"  # private key file:
        "\n",  # token file path:
    )
    assert result.exit_code == 0, result.output
    assert result.output == dedent(
        f"""\
        Enter connection name: conn
        Enter account: test
        Enter user: user
        Enter password: 
        Enter role: 
        Enter warehouse: 
        Enter database: 
        Enter schema: 
        Enter host: 
        Enter port: 
        Enter region: 
        Enter authenticator: 
        Enter private key file: {key.resolve()}
        Enter token file path: 
        Wrote new connection conn to {test_snowcli_config}
        """
    )


def test_connection_add_no_key_pair_setup_if_no_interactive(
    runner, tmp_path, test_snowcli_config
):
    result = runner.invoke(
        [
            "connection",
            "add",
            "--connection-name",
            "conn",
            "--user",
            "user",
            "--account",
            "account",
            "--no-interactive",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (
        result.output.strip() == f"Wrote new connection conn to {test_snowcli_config}"
    )


@mock.patch("snowflake.cli._plugins.auth.keypair.manager.AuthManager.execute_query")
@mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
@mock.patch("snowflake.connector.connect")
def test_connection_add_with_key_pair_saves_password_if_keypair_is_set(
    mock_connect,
    mock_object_execute_query,
    mock_auth_execute_query,
    runner,
    tmp_path,
    mock_cursor,
    test_snowcli_config,
    enable_auth_keypair_feature_flag,
):
    mock_connect.return_value.user = "user"
    mock_object_execute_query.return_value = mock_cursor(
        rows=[
            {"property": "RSA_PUBLIC_KEY", "value": None},
            {"property": "RSA_PUBLIC_KEY_2", "value": "public key..."},
        ],
        columns=[],
    )

    result = runner.invoke(
        [
            "connection",
            "add",
        ],
        input="conn\n"  # connection name: zz
        "test\n"  # account:
        "user\n"  # user:
        "123\n"  # password:
        "\n"  # role:
        "\n"  # warehouse:
        "\n"  # database:
        "\n"  # schema:
        "\n"  # host:
        "\n"  # port:
        "\n"  # region:
        "\n"  # authenticator:
        "\n"  # private key file:
        "\n"  # token file path:
        "y\n"  #
        "\n"  # key_length
        f"{tmp_path}\n"  # output_path
        "123\n",  # passphrase
    )

    private_key_path = tmp_path / "conn.p8"
    public_key_path = tmp_path / "conn.pub"
    assert result.exit_code == 0, result.output
    assert result.output == dedent(
        f"""\
        Enter connection name: conn
        Enter account: test
        Enter user: user
        Enter password: 
        Enter role: 
        Enter warehouse: 
        Enter database: 
        Enter schema: 
        Enter host: 
        Enter port: 
        Enter region: 
        Enter authenticator: 
        Enter private key file: 
        Enter token file path: 
        Do you want to configure key pair authentication? [y/N]: y
        Key length [2048]: 
        Output path [~/.ssh]: {tmp_path}
        Private key passphrase: 
        Wrote new password-based connection conn to {test_snowcli_config}, however there were some issues during key pair setup. Review the following error and check 'snow auth keypair' commands to setup key pair authentication:
         * The public key is set already.
        """
    )
    assert not private_key_path.exists()
    assert not public_key_path.exists()
    with open(test_snowcli_config, "r") as f:
        connections = tomlkit.load(f)
        assert connections["connections"]["conn"]["password"] == "123"


@pytest.fixture
def enable_auth_keypair_feature_flag():
    with mock.patch(
        f"snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_AUTH_KEYPAIR.is_enabled",
        return_value=True,
    ):
        yield
